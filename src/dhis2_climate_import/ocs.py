from __future__ import annotations

from pathlib import Path
from typing import Any

from .http import JsonHttpClient


class OCSClient:
    def __init__(self, base_url: str, *, http: Any | None = None):
        self.base_url = base_url.rstrip("/")
        self.http = http or JsonHttpClient(self.base_url)

    @classmethod
    def from_env(cls, env: dict[str, str]) -> "OCSClient":
        base_url = env.get("CLIMATE_SERVICE_BASE_URL", "http://127.0.0.1:8000")
        return cls(base_url)

    def extent(self) -> dict[str, Any]:
        return self.http.request_json("GET", "/extent")

    def ingest_dataset(
        self,
        dataset_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        overwrite: bool = False,
        publish: bool = True,
    ) -> dict[str, Any]:
        return self.http.request_json(
            "POST",
            "/ingestions",
            json_body={
                "dataset_id": dataset_id,
                "start": start,
                "end": end,
                "overwrite": overwrite,
                "publish": publish,
            },
        )

    def sync_plan(self, dataset_id: str, *, end: str | None = None) -> dict[str, Any]:
        return self.http.request_json("GET", f"/sync/{dataset_id}/plan", params={"end": end})

    def sync_apply(
        self, dataset_id: str, *, end: str | None = None, publish: bool = True
    ) -> dict[str, Any]:
        return self.http.request_json(
            "POST",
            f"/sync/{dataset_id}",
            params={"end": end, "publish": publish},
        )

    def aggregate_to_dhis2_json(
        self,
        *,
        process_id: str,
        dataset_id: str,
        org_units_geojson_path: Path,
        data_element_id: str,
        period_type: str,
        start: str,
        end: str,
        method: str,
    ) -> dict[str, Any]:
        process_graph = {
            "aggregate": {
                "process_id": process_id,
                "arguments": {
                    "dataset_id": dataset_id,
                    "org_units_geojson": str(org_units_geojson_path),
                    "data_element_id": data_element_id,
                    "period_type": period_type,
                    "start": start,
                    "end": end,
                    "method": method,
                },
                "result": True,
            }
        }
        return self.http.request_json("POST", "/result", json_body={"process_graph": process_graph})
