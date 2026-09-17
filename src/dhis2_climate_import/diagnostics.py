from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .dhis2_auth import auth_headers
from .errors import PipelineError


COMMON_CONTEXTS = ("dhis", "dhis2", "hmis", "snis")
DIAGNOSTIC_PATHS = ("/api/system/info", "/api/me")


def diagnose_dhis2_access(
    env: dict[str, str],
    *,
    contexts: list[str],
    include_common_contexts: bool,
    timeout_seconds: float,
) -> dict[str, Any]:
    base_url = _normalise_base_url(env.get("DHIS2_BASE_URL", "").strip())
    if not base_url:
        raise PipelineError("DHIS2_BASE_URL is required in .env or environment.")

    auth_error: str | None = None
    headers = {
        "Accept": "application/json",
        "User-Agent": "dhis2-climate-import/diagnose",
    }
    try:
        headers.update(auth_headers(env))
    except PipelineError as exc:
        auth_error = str(exc)

    candidates = candidate_base_urls(
        base_url,
        contexts=contexts,
        include_common_contexts=include_common_contexts,
    )
    probes = [
        probe_url(base_url=candidate, path=path, headers=headers, timeout_seconds=timeout_seconds)
        for candidate in candidates
        for path in DIAGNOSTIC_PATHS
    ]
    return {
        "configuration": {
            "baseUrl": base_url,
            "transport": env.get("DHIS2_TRANSPORT", "dhis2-client"),
            "authMethod": env.get("DHIS2_AUTH_METHOD", "auto"),
            "hasPat": bool(env.get("DHIS2_PAT", "").strip()),
            "hasBasicCredentials": bool(
                env.get("DHIS2_USERNAME", "").strip()
                and env.get("DHIS2_PASSWORD", "").strip()
            ),
            "authUsable": auth_error is None,
            "authError": auth_error,
        },
        "candidates": candidates,
        "probes": probes,
        "summary": summarise_probes(probes),
    }


def candidate_base_urls(
    base_url: str,
    *,
    contexts: list[str],
    include_common_contexts: bool,
) -> list[str]:
    result = [_normalise_base_url(base_url)]
    context_values = list(contexts)
    if include_common_contexts:
        context_values.extend(COMMON_CONTEXTS)
    for context in context_values:
        clean_context = context.strip("/")
        if not clean_context:
            continue
        candidate = f"{result[0]}/{clean_context}"
        if candidate not in result:
            result.append(candidate)
    return result


def probe_url(
    *,
    base_url: str,
    path: str,
    headers: dict[str, str],
    timeout_seconds: float,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    try:
        request = Request(url, headers=headers, method="GET")
        with urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            return _probe_result(
                url=url,
                status_code=response.status,
                headers=dict(response.headers.items()),
                body=body,
                error=None,
            )
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return _probe_result(
            url=url,
            status_code=exc.code,
            headers=dict(exc.headers.items()),
            body=body,
            error=None,
        )
    except URLError as exc:
        return {
            "url": url,
            "statusCode": None,
            "contentType": None,
            "server": None,
            "looksLikeDhis2Json": False,
            "looksLikeNginxHtml": False,
            "message": str(exc),
            "hint": "Network/TLS/DNS error before an HTTP response was received.",
        }


def summarise_probes(probes: list[dict[str, Any]]) -> str:
    ok = [probe for probe in probes if probe.get("looksLikeDhis2Json") and probe.get("statusCode") == 200]
    if ok:
        return f"DHIS2 API reachable at {ok[0]['url']}."
    nginx = [probe for probe in probes if probe.get("looksLikeNginxHtml")]
    if nginx:
        return (
            "nginx returned HTML before a DHIS2 JSON response. Check DHIS2_BASE_URL, "
            "context path, reverse-proxy API rules, Authorization header forwarding, "
            "or IP/VPN allow-listing."
        )
    auth = [probe for probe in probes if probe.get("statusCode") in {401, 403}]
    if auth:
        return "API route exists but authentication or authorities are not sufficient."
    return "No reachable DHIS2 JSON API candidate was found."


def _probe_result(
    *,
    url: str,
    status_code: int,
    headers: dict[str, str],
    body: str,
    error: str | None,
) -> dict[str, Any]:
    content_type = headers.get("Content-Type") or headers.get("content-type")
    server = headers.get("Server") or headers.get("server")
    parsed = _parse_json(body)
    looks_like_dhis2_json = isinstance(parsed, dict) and bool(
        {"version", "dhis2Version", "username", "id", "status", "httpStatus"} & set(parsed)
    )
    looks_like_nginx_html = _looks_like_nginx_html(body, parsed)
    return {
        "url": url,
        "statusCode": status_code,
        "contentType": content_type,
        "server": server,
        "looksLikeDhis2Json": looks_like_dhis2_json,
        "looksLikeNginxHtml": looks_like_nginx_html,
        "message": _safe_message(body, parsed, error),
        "hint": _hint(status_code, looks_like_dhis2_json, looks_like_nginx_html),
    }


def _hint(status_code: int, looks_like_dhis2_json: bool, looks_like_nginx_html: bool) -> str:
    if looks_like_dhis2_json and status_code == 200:
        return "This endpoint is reachable and returns DHIS2 JSON."
    if looks_like_nginx_html:
        return "This response did not reach DHIS2 correctly; nginx returned an HTML error page."
    if status_code == 401:
        return "Authentication was rejected. Check PAT/basic credentials."
    if status_code == 403:
        return "Authentication worked but the account may lack authority, or the proxy denied the request."
    if status_code == 404:
        return "This context path does not expose the DHIS2 API."
    return "Inspect this candidate before using it as DHIS2_BASE_URL."


def _safe_message(body: str, parsed: Any, error: str | None) -> str:
    if error:
        return error[:300]
    if isinstance(parsed, dict):
        for key in ("message", "httpStatus", "status"):
            value = parsed.get(key)
            if isinstance(value, str) and value:
                return " ".join(value.split())[:300]
    return " ".join(body.split())[:300]


def _looks_like_nginx_html(body: str, parsed: Any) -> bool:
    values = [body]
    if isinstance(parsed, dict):
        values.extend(str(value) for value in parsed.values() if isinstance(value, str))
    compact = " ".join(values).lower()
    return "<html" in compact and "nginx" in compact


def _parse_json(body: str) -> Any:
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def _normalise_base_url(base_url: str) -> str:
    clean = base_url.rstrip("/")
    if clean.endswith("/api"):
        return clean[:-4]
    return clean
