from __future__ import annotations

from dataclasses import dataclass
import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass
class HttpRequestError(RuntimeError):
    method: str
    url: str
    status_code: int | None
    body: str

    def __str__(self) -> str:
        status = self.status_code if self.status_code is not None else "network"
        body = f": {_short_body(self.body)}" if self.body else ""
        return f"{self.method} {self.url} failed with {status}{body}"


class JsonHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout_seconds: float = 60,
        retries: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self.backoff_seconds = backoff_seconds

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        body = self.request(method, path, params=params, json_body=json_body, headers=headers)
        if body == "":
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise HttpRequestError(method, self._url(path, params), None, body) from exc

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        url = self._url(path, params)
        request_headers = {
            "Accept": "application/json",
            "User-Agent": "dhis2-climate-import/0.1",
            **self.headers,
            **(headers or {}),
        }
        data: bytes | None = None
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"

        for attempt in range(self.retries + 1):
            try:
                request = Request(url, data=data, headers=request_headers, method=method.upper())
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    return response.read().decode("utf-8")
            except HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if not _is_retriable(exc.code) or attempt >= self.retries:
                    raise HttpRequestError(method.upper(), url, exc.code, body) from exc
                time.sleep(self._sleep_seconds(attempt))
            except (URLError, TimeoutError, OSError) as exc:
                if attempt >= self.retries:
                    raise HttpRequestError(method.upper(), url, None, str(exc)) from exc
                time.sleep(self._sleep_seconds(attempt))
        raise AssertionError("unreachable")

    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{self.base_url}/{path.lstrip('/')}"
        if params:
            encoded_params = {
                key: _query_value(value)
                for key, value in params.items()
                if value is not None
            }
            url = f"{url}?{urlencode(encoded_params, doseq=True)}"
        return url

    def _sleep_seconds(self, attempt: int) -> float:
        return self.backoff_seconds * (2**attempt)


def _is_retriable(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _query_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return [_query_value(item) for item in value]
    return value


def _short_body(body: str) -> str:
    compact = " ".join(body.split())
    return compact[:500]
