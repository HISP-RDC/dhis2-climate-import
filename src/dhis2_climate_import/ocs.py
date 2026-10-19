from __future__ import annotations

import time
from typing import Any, Callable

from .errors import PipelineError
from .http import JsonHttpClient

ProgressCallback = Callable[[str, dict[str, Any]], None]


class OCSClient:
    def __init__(
        self,
        base_url: str,
        *,
        http: Any | None = None,
        poll_seconds: float = 5.0,
        job_timeout_seconds: float = 86400.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.http = http or JsonHttpClient(self.base_url)
        self.poll_seconds = poll_seconds
        self.job_timeout_seconds = job_timeout_seconds

    @classmethod
    def from_env(cls, env: dict[str, str]) -> "OCSClient":
        base_url = env.get("CLIMATE_SERVICE_BASE_URL", "http://127.0.0.1:9000")
        http_timeout = _positive_float(env, "CLIMATE_SERVICE_HTTP_TIMEOUT_SECONDS", 60.0)
        poll_seconds = _positive_float(env, "CLIMATE_SERVICE_POLL_SECONDS", 5.0)
        job_timeout = _positive_float(env, "CLIMATE_SERVICE_JOB_TIMEOUT_SECONDS", 86400.0)
        return cls(
            base_url,
            http=JsonHttpClient(base_url, timeout_seconds=http_timeout, retries=0),
            poll_seconds=poll_seconds,
            job_timeout_seconds=job_timeout,
        )

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
        progress: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        response = self.http.request_json(
            "POST",
            "/ingestions",
            json_body={
                "dataset_id": dataset_id,
                "start": start,
                "end": end,
                "overwrite": overwrite,
                "publish": publish,
            },
            headers={"Prefer": "respond-async"},
        )
        status = str(response.get("status", "")).lower()
        if status not in {"accepted", "running", "retrying"}:
            return response
        job_id = str(response.get("ingestion_id", "")).strip()
        if not job_id:
            raise PipelineError("OCS accepted the asynchronous ingestion without returning a job id")
        _emit(progress, "ocs.ingestion.queued", job=job_id, status=status)
        return self.wait_for_ingestion(job_id, progress=progress)

    def wait_for_ingestion(
        self,
        job_id: str,
        *,
        progress: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """Poll one native OCS ingestion job until it reaches a terminal state."""
        deadline = time.monotonic() + self.job_timeout_seconds
        last_snapshot: tuple[Any, ...] | None = None
        while True:
            record = self.http.request_json("GET", f"/ingestions/jobs/{job_id}")
            status = str(record.get("status", "")).lower()
            job_progress = record.get("progress") or {}
            snapshot = (
                status,
                job_progress.get("done"),
                job_progress.get("total"),
                job_progress.get("percent"),
                job_progress.get("message"),
            )
            if snapshot != last_snapshot:
                _emit(
                    progress,
                    "ocs.ingestion.progress",
                    job=job_id,
                    status=status,
                    done=job_progress.get("done"),
                    total=job_progress.get("total"),
                    percent=job_progress.get("percent"),
                    message=job_progress.get("message"),
                )
                last_snapshot = snapshot

            if status == "successful":
                _emit(progress, "ocs.ingestion.done", job=job_id, status=status)
                result = record.get("result")
                return result if isinstance(result, dict) else record
            if status in {"failed", "cancelled"}:
                error = record.get("error") or {}
                message = error.get("message") or job_progress.get("message") or status
                raise PipelineError(f"OCS ingestion job {job_id} {status}: {message}")
            if time.monotonic() >= deadline:
                raise PipelineError(
                    f"OCS ingestion job {job_id} is still {status or 'running'} after "
                    f"{self.job_timeout_seconds:g} seconds; the background job was not cancelled"
                )
            time.sleep(self.poll_seconds)

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
        geometries: dict[str, Any],
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
                    "temporal_extent": [start, end],
                    "geometries": geometries,
                    "data_element_id": data_element_id,
                    "period_type": period_type,
                    "method": method,
                },
                "result": True,
            }
        }
        return self.http.request_json("POST", "/result", json_body={"process_graph": process_graph})


def _positive_float(env: dict[str, str], name: str, default: float) -> float:
    raw = (env.get(name) or "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError as exc:
        raise PipelineError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise PipelineError(f"{name} must be greater than zero")
    return value


def _emit(progress: ProgressCallback | None, event: str, **details: Any) -> None:
    if progress is not None:
        progress(event, {key: value for key, value in details.items() if value is not None})
