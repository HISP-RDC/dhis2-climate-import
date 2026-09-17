from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.journal import ImportFingerprint, RunJournal


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


if __name__ == "__main__":
    unittest.main()
