from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.ocs_cleanup import cleanup_stale_ocs_jobs


class FakeOCS:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    def ingestion_job(self, job_id: str):
        return {
            "jobID": job_id,
            "status": "running",
            "request": {"dataset_id": "modis_ndvi_weekly"},
        }

    def cancel_ingestion_job(self, job_id: str, *, wait_seconds: float, progress=None):
        del wait_seconds, progress
        self.cancelled.append(job_id)
        return {"jobID": job_id, "status": "cancelled"}


class OCSCleanupTests(unittest.TestCase):
    def test_apply_cancels_only_old_active_jobs_for_selected_datasets(self) -> None:
        records = [
            {
                "job_id": "stale-selected",
                "status": "running",
                "created_at": "2026-11-15T00:00:00+00:00",
                "request": {"dataset_id": "modis_ndvi_weekly"},
            },
            {
                "job_id": "recent-selected",
                "status": "running",
                "created_at": "2026-11-18T11:00:00+00:00",
                "request": {"dataset_id": "modis_ndvi_weekly"},
            },
            {
                "job_id": "stale-other",
                "status": "running",
                "created_at": "2026-11-15T00:00:00+00:00",
                "request": {"dataset_id": "unrelated_dataset"},
            },
            {
                "job_id": "completed-selected",
                "status": "successful",
                "created_at": "2026-11-15T00:00:00+00:00",
                "request": {"dataset_id": "modis_ndvi_weekly"},
            },
        ]
        ocs = FakeOCS()

        with TemporaryDirectory() as temporary_directory:
            store = Path(temporary_directory) / "jobs.json"
            store.write_text(json.dumps(records), encoding="utf-8")
            result = cleanup_stale_ocs_jobs(
                ocs,
                job_store=store,
                dataset_ids={"modis_ndvi_weekly"},
                older_than_hours=48,
                wait_seconds=120,
                apply=True,
                now=datetime(2026, 11, 18, 12, tzinfo=timezone.utc),
            )

        self.assertEqual(result["candidateCount"], 1)
        self.assertEqual(result["cancelledCount"], 1)
        self.assertEqual(ocs.cancelled, ["stale-selected"])

    def test_dry_run_never_requests_cancellation(self) -> None:
        records = [
            {
                "job_id": "stale-selected",
                "status": "retrying",
                "created_at": "2026-11-15T00:00:00Z",
                "request": {"dataset_id": "modis_ndvi_weekly"},
                "cancel_requested": True,
            }
        ]
        ocs = FakeOCS()

        with TemporaryDirectory() as temporary_directory:
            store = Path(temporary_directory) / "jobs.json"
            store.write_text(json.dumps(records), encoding="utf-8")
            result = cleanup_stale_ocs_jobs(
                ocs,
                job_store=store,
                dataset_ids={"modis_ndvi_weekly"},
                older_than_hours=48,
                wait_seconds=120,
                apply=False,
                now=datetime(2026, 11, 18, 12, tzinfo=timezone.utc),
            )

        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["candidateCount"], 1)
        self.assertEqual(ocs.cancelled, [])


if __name__ == "__main__":
    unittest.main()
