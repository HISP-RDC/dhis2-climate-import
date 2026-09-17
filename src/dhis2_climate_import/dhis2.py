from __future__ import annotations

from dataclasses import dataclass
import re
import time
from typing import Any

from .dhis2_auth import auth_headers
from .dhis2_transport import Dhis2PythonClientTransport
from .errors import PipelineError
from .http import HttpRequestError, JsonHttpClient


@dataclass(frozen=True)
class Dhis2Check:
    base_url: str
    version: str
    user: str
    display_name: str


@dataclass(frozen=True)
class DataSetRef:
    id: str
    code: str | None
    name: str
    period_type: str | None


class DHIS2Client:
    def __init__(self, base_url: str, *, http: Any | None = None, headers: dict[str, str] | None = None):
        self.base_url = base_url.rstrip("/")
        self.http = http or JsonHttpClient(self.base_url, headers=headers)

    @classmethod
    def from_env(cls, env: dict[str, str]) -> "DHIS2Client":
        base_url = _normalise_base_url(env.get("DHIS2_BASE_URL", "").strip())
        if not base_url:
            raise PipelineError("DHIS2_BASE_URL is required in .env or environment.")
        transport = env.get("DHIS2_TRANSPORT", "dhis2-client").strip().lower()
        if transport == "dhis2-client":
            return cls(base_url, http=Dhis2PythonClientTransport(base_url, env=env))
        if transport == "stdlib":
            return cls(base_url, headers=auth_headers(env))
        raise PipelineError("DHIS2_TRANSPORT must be one of: dhis2-client, stdlib.")

    def system_info(self) -> dict[str, Any]:
        return self.http.request_json("GET", "/api/system/info")

    def me(self) -> dict[str, Any]:
        return self.http.request_json("GET", "/api/me", params={"fields": "id,username,name"})

    def check(self, min_version: tuple[int, int] = (2, 37)) -> Dhis2Check:
        info = self.system_info()
        me = self.me()
        version = str(info.get("version") or info.get("dhis2Version") or "")
        if not version:
            raise PipelineError("Could not read DHIS2 version from /api/system/info.")
        parsed = parse_dhis2_version(version)
        if parsed < min_version:
            target = ".".join(str(part) for part in min_version)
            raise PipelineError(f"DHIS2 {version} is below the required minimum {target}.")
        return Dhis2Check(
            base_url=self.base_url,
            version=version,
            user=str(me.get("username") or me.get("id") or ""),
            display_name=str(me.get("name") or me.get("displayName") or ""),
        )

    def organisation_units_level(self, *, level: int, page_size: int) -> list[dict[str, Any]]:
        return self._paged(
            "/api/organisationUnits",
            root_key="organisationUnits",
            page_size=page_size,
            params={
                "filter": f"level:eq:{level}",
                "fields": "id,code,name,path,level,geometry",
                "paging": "true",
            },
        )

    def data_elements(self, *, page_size: int) -> list[dict[str, Any]]:
        return self._paged(
            "/api/dataElements",
            root_key="dataElements",
            page_size=page_size,
            params={
                "fields": "id,code,name,displayName,valueType,domainType,aggregationType,periodType",
                "paging": "true",
            },
        )

    def resolve_data_set(self, selector: str) -> DataSetRef:
        if _looks_like_uid(selector):
            try:
                return self._data_set_ref_from_id(selector)
            except HttpRequestError as exc:
                if exc.status_code != 404:
                    raise

        response = self.http.request_json(
            "GET",
            "/api/dataSets",
            params={
                "filter": [f"id:eq:{selector}", f"code:eq:{selector}"],
                "rootJunction": "OR",
                "fields": "id,code,name,periodType",
                "pageSize": 2,
                "paging": "true",
            },
        )
        data_sets = response.get("dataSets")
        if not isinstance(data_sets, list):
            raise PipelineError("DHIS2 response did not contain a 'dataSets' list.")
        if not data_sets:
            raise PipelineError(f"DHIS2 data set '{selector}' was not found by UID or code.")
        if len(data_sets) > 1:
            raise PipelineError(
                f"DHIS2 data set selector '{selector}' matched more than one data set."
            )
        data_set = data_sets[0]
        return DataSetRef(
            id=str(data_set["id"]),
            code=data_set.get("code"),
            name=str(data_set.get("name") or data_set["id"]),
            period_type=data_set.get("periodType"),
        )

    def data_elements_for_data_set(
        self, *, data_set_id: str, page_size: int
    ) -> list[dict[str, Any]]:
        data_set = self.http.request_json(
            "GET",
            f"/api/dataSets/{data_set_id}",
            params={
                "fields": (
                    "id,code,name,periodType,"
                    "dataSetElements[dataElement[id,code,name,displayName,valueType,domainType,"
                    "aggregationType,periodType]]"
                )
            },
        )
        data_set_elements = data_set.get("dataSetElements")
        if isinstance(data_set_elements, list):
            elements = [
                item["dataElement"]
                for item in data_set_elements
                if isinstance(item, dict) and isinstance(item.get("dataElement"), dict)
            ]
            return _unique_by_id(elements)

        return self._paged(
            "/api/dataElements",
            root_key="dataElements",
            page_size=page_size,
            params={
                "filter": f"dataSetElements.dataSet.id:eq:{data_set_id}",
                "fields": "id,code,name,displayName,valueType,domainType,aggregationType,periodType",
                "paging": "true",
            },
        )

    def import_data_value_set(
        self,
        payload: dict[str, Any],
        *,
        dry_run: bool,
        import_strategy: str,
        preheat_cache: bool,
        async_mode: bool,
    ) -> dict[str, Any]:
        return self.http.request_json(
            "POST",
            "/api/dataValueSets",
            params={
                "dryRun": dry_run,
                "importStrategy": import_strategy,
                "preheatCache": preheat_cache,
                "async": async_mode,
            },
            json_body=payload,
        )

    def task_status(self, task_id_or_path: str) -> Any:
        path = task_id_or_path
        if not path.startswith("/api/"):
            path = f"/api/system/tasks/DATAVALUE_IMPORT/{task_id_or_path}"
        return self.http.request_json("GET", path)

    def wait_for_data_value_import(
        self, task_id_or_path: str, *, timeout_seconds: int = 600, poll_seconds: int = 5
    ) -> Any:
        deadline = time.time() + timeout_seconds
        last_status: Any = None
        while time.time() < deadline:
            last_status = self.task_status(task_id_or_path)
            if _task_finished(last_status):
                return last_status
            time.sleep(poll_seconds)
        raise PipelineError(f"Timed out waiting for DHIS2 data value import task: {last_status}")

    def trigger_analytics_tables(self) -> dict[str, Any]:
        return self.http.request_json("POST", "/api/resourceTables/analytics")

    def analytics(self, *, dimensions: list[str], params: dict[str, Any] | None = None) -> dict[str, Any]:
        query_params: dict[str, Any] = {"dimension": dimensions, "skipMeta": "false"}
        if params:
            query_params.update(params)
        return self.http.request_json("GET", "/api/analytics", params=query_params)

    def _data_set_ref_from_id(self, data_set_id: str) -> DataSetRef:
        data_set = self.http.request_json(
            "GET",
            f"/api/dataSets/{data_set_id}",
            params={"fields": "id,code,name,periodType"},
        )
        return DataSetRef(
            id=str(data_set["id"]),
            code=data_set.get("code"),
            name=str(data_set.get("name") or data_set["id"]),
            period_type=data_set.get("periodType"),
        )

    def _paged(
        self,
        path: str,
        *,
        root_key: str,
        page_size: int,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        page = 1
        items: list[dict[str, Any]] = []
        while True:
            response = self.http.request_json(
                "GET",
                path,
                params={**params, "page": page, "pageSize": page_size},
            )
            page_items = response.get(root_key)
            if not isinstance(page_items, list):
                raise PipelineError(f"DHIS2 response did not contain a '{root_key}' list.")
            items.extend(page_items)
            pager = response.get("pager") or {}
            page_count = int(pager.get("pageCount") or page)
            if page >= page_count:
                return items
            page += 1


def parse_dhis2_version(value: str) -> tuple[int, int]:
    match = re.search(r"(\d+)\.(\d+)", value)
    if not match:
        raise PipelineError(f"Could not parse DHIS2 version: {value}")
    return int(match.group(1)), int(match.group(2))


def _looks_like_uid(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9]{10}", value))


def _unique_by_id(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        item_id = item.get("id")
        if not isinstance(item_id, str) or item_id in seen:
            continue
        seen.add(item_id)
        unique.append(item)
    return unique


def _normalise_base_url(base_url: str) -> str:
    clean = base_url.rstrip("/")
    if clean.endswith("/api"):
        return clean[:-4]
    return clean


def _task_finished(status: Any) -> bool:
    if isinstance(status, list):
        return any(_task_finished(item) for item in status)
    if not isinstance(status, dict):
        return False
    if status.get("completed") is True or status.get("status") in {"SUCCESS", "ERROR", "DONE"}:
        return True
    children = status.get("tasks")
    if isinstance(children, list):
        return any(_task_finished(item) for item in children)
    return False
