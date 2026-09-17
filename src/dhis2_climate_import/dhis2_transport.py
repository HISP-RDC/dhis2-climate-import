from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode

from .dhis2_auth import auth_options
from .http import HttpRequestError


class Dhis2PythonClientTransport:
    """Adapter from dhis2-python-client to this project's small request_json protocol."""

    def __init__(self, base_url: str, *, env: dict[str, str]):
        try:
            from dhis2_client import DHIS2Client as UpstreamDHIS2Client
            from dhis2_client.errors import DHIS2HTTPError
        except ImportError as exc:
            raise RuntimeError(
                "dhis2-client is not installed. Run `make install` to install the pinned "
                "dhis2-python-client dependency."
            ) from exc

        self.base_url = base_url.rstrip("/")
        self._error_type = DHIS2HTTPError
        self._client = UpstreamDHIS2Client(
            base_url=self.base_url,
            **auth_options(env),
            default_page_size=int(env.get("DHIS2_DEFAULT_PAGE_SIZE") or 50),
            timeout=float(env.get("DHIS2_TIMEOUT_SECONDS") or 60),
            connect_timeout=float(env.get("DHIS2_CONNECT_TIMEOUT_SECONDS") or 60),
            retries=int(env.get("DHIS2_RETRIES") or 3),
            log_level=env.get("DHIS2_CLIENT_LOG_LEVEL") or "CRITICAL",
        )

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        method_upper = method.upper()
        try:
            if method_upper == "GET":
                return self._client.get(path, params=params)
            if method_upper == "POST":
                return self._client.post(path, params=params, json=json_body)
            if method_upper == "PUT":
                return self._client.put(path, params=params, json=json_body)
            if method_upper == "PATCH":
                return self._client.patch(path, params=params, json=json_body)
            if method_upper == "DELETE":
                return self._client.delete(path, params=params)
        except self._error_type as exc:
            payload = getattr(exc, "payload", {})
            raise HttpRequestError(
                method_upper,
                _url(self.base_url, path, params),
                getattr(exc, "status_code", None),
                json.dumps(payload, sort_keys=True),
            ) from exc
        except Exception as exc:
            raise HttpRequestError(
                method_upper,
                _url(self.base_url, path, params),
                None,
                str(exc),
            ) from exc
        raise ValueError(f"Unsupported HTTP method: {method}")


def _url(base_url: str, path: str, params: dict[str, Any] | None) -> str:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if not params:
        return url
    encoded = urlencode(
        {key: _query_value(value) for key, value in params.items() if value is not None},
        doseq=True,
    )
    return f"{url}?{encoded}"


def _query_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return [_query_value(item) for item in value]
    return value
