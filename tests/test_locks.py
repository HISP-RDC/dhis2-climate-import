from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.errors import TransientPipelineError
from dhis2_climate_import.locks import concurrency_slot, exclusive_lock


class LockTests(unittest.TestCase):
    def test_exclusive_job_lock_blocks_a_second_holder(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "job.lock"
            with exclusive_lock(
                path,
                purpose="first",
                transient_when_busy=True,
            ):
                with self.assertRaises(TransientPipelineError):
                    with exclusive_lock(
                        path,
                        purpose="second",
                        transient_when_busy=True,
                    ):
                        self.fail("second lock holder must not enter")

    def test_global_concurrency_limit_blocks_an_extra_job(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with concurrency_slot(root, maximum=1):
                with self.assertRaises(TransientPipelineError):
                    with concurrency_slot(root, maximum=1):
                        self.fail("second concurrency slot must not exist")


if __name__ == "__main__":
    unittest.main()
