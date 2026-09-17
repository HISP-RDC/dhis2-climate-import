from __future__ import annotations

import base64

from .errors import PipelineError


def auth_options(env: dict[str, str]) -> dict[str, str]:
    method = _auth_method(env)
    pat = env.get("DHIS2_PAT", "").strip()
    username = env.get("DHIS2_USERNAME", "").strip()
    password = env.get("DHIS2_PASSWORD", "").strip()

    if method in {"auto", "pat"} and pat:
        return {"token": _strip_pat_scheme(pat)}
    if method in {"auto", "basic"} and username and password:
        return {"username": username, "password": password}
    _raise_missing_auth(method)


def auth_headers(env: dict[str, str]) -> dict[str, str]:
    method = _auth_method(env)
    pat = env.get("DHIS2_PAT", "").strip()
    if method in {"auto", "pat"} and pat:
        return {"Authorization": _pat_authorization_value(pat)}

    username = env.get("DHIS2_USERNAME", "").strip()
    password = env.get("DHIS2_PASSWORD", "").strip()
    if method in {"auto", "basic"} and username and password:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}
    _raise_missing_auth(method)


def _auth_method(env: dict[str, str]) -> str:
    method = env.get("DHIS2_AUTH_METHOD", "auto").strip().lower() or "auto"
    if method not in {"auto", "pat", "basic"}:
        raise PipelineError("DHIS2_AUTH_METHOD must be one of: auto, pat, basic.")
    return method


def _raise_missing_auth(method: str) -> None:
    if method == "basic":
        raise PipelineError(
            "DHIS2_AUTH_METHOD=basic requires DHIS2_USERNAME and DHIS2_PASSWORD."
        )
    if method == "pat":
        raise PipelineError("DHIS2_AUTH_METHOD=pat requires DHIS2_PAT.")
    raise PipelineError("DHIS2_PAT or DHIS2_USERNAME/DHIS2_PASSWORD is required.")


def _strip_pat_scheme(pat: str) -> str:
    for scheme in ("ApiToken ", "Bearer "):
        if pat.startswith(scheme):
            return pat[len(scheme) :]
    return pat


def _pat_authorization_value(pat: str) -> str:
    first_token = pat.split(None, 1)[0] if pat else ""
    if first_token in {"ApiToken", "Bearer", "Basic"}:
        return pat
    return f"ApiToken {pat}"
