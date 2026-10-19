from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
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

    def test_waiting_concurrency_slot_enters_after_release(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            holder_entered = threading.Event()

            def hold_slot() -> None:
                with concurrency_slot(root, maximum=1):
                    holder_entered.set()
                    time.sleep(0.05)

            holder = threading.Thread(target=hold_slot)
            holder.start()
            self.assertTrue(holder_entered.wait(timeout=1))

            started_at = time.monotonic()
            with concurrency_slot(
                root,
                maximum=1,
                wait_when_busy=True,
                poll_interval_seconds=0.01,
            ) as slot:
                self.assertEqual(slot, 0)
            elapsed = time.monotonic() - started_at

            holder.join(timeout=1)
            self.assertFalse(holder.is_alive())
            self.assertGreaterEqual(elapsed, 0.03)

    def test_waiting_exclusive_lock_enters_after_release(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "metadata.lock"
            holder_entered = threading.Event()

            def hold_lock() -> None:
                with exclusive_lock(path, purpose="first"):
                    holder_entered.set()
                    time.sleep(0.05)

            holder = threading.Thread(target=hold_lock)
            holder.start()
            self.assertTrue(holder_entered.wait(timeout=1))

            started_at = time.monotonic()
            with exclusive_lock(
                path,
                purpose="second",
                wait_when_busy=True,
                poll_interval_seconds=0.01,
            ):
                pass
            elapsed = time.monotonic() - started_at

            holder.join(timeout=1)
            self.assertFalse(holder.is_alive())
            self.assertGreaterEqual(elapsed, 0.03)


if __name__ == "__main__":
    unittest.main()
