from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from .errors import PipelineError
from .ocs import OCSClient


_ACTIVE_STATUSES = frozenset({"accepted", "running", "retrying"})


def cleanup_stale_ocs_jobs(
    ocs: OCSClient,
    *,
    job_store: Path,
    dataset_ids: set[str],
    older_than_hours: float,
    wait_seconds: float,
    apply: bool,
    now: datetime | None = None,
    progress: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Plan or cancel stale active native OCS jobs for selected datasets."""
    if older_than_hours <= 0:
        raise PipelineError("--older-than-hours must be greater than zero.")
    if wait_seconds <= 0:
        raise PipelineError("--wait-seconds must be greater than zero.")
    if not job_store.is_file():
        raise PipelineError(f"OCS native job store not found: {job_store}")

    records = _load_records(job_store)
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)

    candidates: list[dict[str, Any]] = []
    ignored_without_timestamp = 0
    for record in records:
        status = str(record.get("status") or "").lower()
        request = record.get("request") if isinstance(record.get("request"), dict) else {}
        dataset_id = str(request.get("dataset_id") or "").strip()
        if status not in _ACTIVE_STATUSES or dataset_id not in dataset_ids:
            continue
        timestamp = _record_timestamp(record)
        if timestamp is None:
            ignored_without_timestamp += 1
            continue
        age_hours = max(0.0, (reference - timestamp).total_seconds() / 3600)
        if age_hours < older_than_hours:
            continue
        job_id = str(record.get("job_id") or record.get("jobID") or "").strip()
        if not job_id:
            ignored_without_timestamp += 1
            continue
        candidates.append(
            {
                "jobID": job_id,
                "datasetID": dataset_id,
                "status": status,
                "ageHours": round(age_hours, 2),
                "cancelRequested": bool(
                    record.get("cancel_requested", record.get("cancelRequested", False))
                ),
            }
        )

    cancelled: list[dict[str, Any]] = []
    skipped_after_recheck: list[dict[str, Any]] = []
    if apply:
        for candidate in candidates:
            current = ocs.ingestion_job(candidate["jobID"])
            current_status = str(current.get("status") or "").lower()
            request = current.get("request") if isinstance(current.get("request"), dict) else {}
            current_dataset = str(request.get("dataset_id") or "").strip()
            if current_status not in _ACTIVE_STATUSES or current_dataset != candidate["datasetID"]:
                skipped_after_recheck.append(
                    {
                        **candidate,
                        "currentStatus": current_status,
                        "reason": "job changed before cancellation",
                    }
                )
                continue
            terminal = ocs.cancel_ingestion_job(
                candidate["jobID"],
                wait_seconds=wait_seconds,
                progress=progress,
            )
            cancelled.append(
                {
                    **candidate,
                    "terminalStatus": str(terminal.get("status") or "").lower(),
                }
            )

    return {
        "status": "applied" if apply else "planned",
        "jobStore": str(job_store),
        "datasetIDs": sorted(dataset_ids),
        "olderThanHours": older_than_hours,
        "activeJobCount": sum(
            1
            for record in records
            if str(record.get("status") or "").lower() in _ACTIVE_STATUSES
        ),
        "candidateCount": len(candidates),
        "candidates": candidates,
        "cancelledCount": len(cancelled),
        "cancelled": cancelled,
        "skippedAfterRecheck": skipped_after_recheck,
        "ignoredWithoutTimestamp": ignored_without_timestamp,
    }


def _load_records(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"Could not read OCS native job store {path}: {exc}") from exc
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise PipelineError(f"OCS native job store must contain a JSON array of objects: {path}")
    return payload


def _record_timestamp(record: dict[str, Any]) -> datetime | None:
    for key in ("started_at", "startedAt", "created_at", "createdAt"):
        raw = record.get(key)
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            value = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        except ValueError:
            continue
        return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)
    return None
