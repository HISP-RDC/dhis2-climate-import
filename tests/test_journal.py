from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.journal import (
    ImportFingerprint,
    RunJournal,
    TransferFingerprint,
    TransferRunJournal,
)


class JournalTests(unittest.TestCase):
    def test_records_success_for_idempotence(self) -> None:
        fingerprint = ImportFingerprint(
            dataset_id="chirps3_precipitation_daily",
            start="2024-01-01",
            end="2024-01-31",
            period_type="day",
            data_element_id="DataEl00001",
            org_unit_level=3,
            org_unit_cache_hash="abc",
        )
        with TemporaryDirectory() as tmp:
            journal = RunJournal(Path(tmp) / "runs.sqlite3")
            self.assertFalse(journal.has_successful_import(fingerprint))
            journal.record(
                run_id="run-1",
                command="import-dhis2",
                fingerprint=fingerprint,
                dry_run=False,
                status="success",
                counts={"imported": 1, "updated": 0, "ignored": 0, "deleted": 0},
            )
            self.assertTrue(journal.has_successful_import(fingerprint))

    def test_tracks_transfer_windows_and_marks_interrupted_work(self) -> None:
        fingerprint = TransferFingerprint(
            scenario_id="x-to-y",
            job_id="hospital",
            source_instance="x@https://x.example.org",
            target_instance="y@https://y.example.org",
            source_data_set="DataSet0001",
            target_data_set="DataSet0001",
            start="2026-01-01",
            end="2026-03-31",
            config_hash="abc",
        )
        with TemporaryDirectory() as tmp:
            journal = TransferRunJournal(Path(tmp) / "transfer.sqlite3")
            journal.start_parent_run(
                run_id="parent-1",
                fingerprint=fingerprint,
                dry_run=False,
                window_count=1,
            )
            journal.start_window(
                window_id="window-1",
                run_id="parent-1",
                fingerprint=fingerprint,
                dry_run=False,
                window_index=1,
                window_count=1,
                attempt=1,
            )
            self.assertEqual(
                journal.mark_running_windows_interrupted(job_id="hospital"), 1
            )
            journal.start_window(
                window_id="window-2",
                run_id="parent-1",
                fingerprint=fingerprint,
                dry_run=False,
                window_index=1,
                window_count=1,
                attempt=2,
            )
            journal.finish_window(window_id="window-2", status="success")

            self.assertTrue(
                journal.has_successful_window(
                    fingerprint=fingerprint,
                    dry_run=False,
                )
            )
            rows = journal.window_rows(run_id="parent-1")
            self.assertEqual([row["status"] for row in rows], ["interrupted", "success"])


if __name__ == "__main__":
    unittest.main()
