from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SCRIPT = PROJECT_ROOT / "deploy" / "systemd" / "dhis2-climate-pipeline.sh"
OCS_SERVICE = PROJECT_ROOT / "deploy" / "systemd" / "open-climate-service.service"
PIPELINE_SERVICE = PROJECT_ROOT / "deploy" / "systemd" / "dhis2-climate-pipeline.service"


class SystemdClimatePipelineTests(unittest.TestCase):
    def _run_pipeline(
        self,
        temporary_path: Path,
        *,
        jobs: str = "precipitation_weekly_chirps vegetation_ndvi_weekly",
        failed_job: str = "",
        failed_status: str = "2",
        duplicate_job: str = "",
        mode: str = "fixed",
        continue_on_failure: str = "true",
        start_period: str = "2026-W38",
        end_period: str = "2026-W39",
        period_batch_size: str = "13",
    ) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        calls_path = temporary_path / "calls.log"
        fake_python = temporary_path / "python"
        fake_python.write_text(
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                printf '%s\\n' "$*" >> "$FAKE_CLI_CALLS"
                if [[ -n "$FAKE_FAILED_JOB" && "$*" == *"ingest-dataset $FAKE_FAILED_JOB"* ]]; then
                  printf 'simulated failure for %s\\n' "$FAKE_FAILED_JOB" >&2
                  exit "$FAKE_FAILED_STATUS"
                fi
                if [[ -n "$FAKE_DUPLICATE_JOB" && "$*" == *"import-dhis2 $FAKE_DUPLICATE_JOB"* && "$*" == *"--apply"* ]]; then
                  printf 'ERROR: This import already succeeded with the same dataset, period, data element, and org unit cache hash. Use --force to rerun it.\\n' >&2
                  exit 2
                fi
                printf '{}\\n'
                """
            ),
            encoding="utf-8",
        )
        fake_python.chmod(0o755)

        env_file = temporary_path / ".env"
        config_file = temporary_path / "import-pipeline.yaml"
        env_file.write_text("CLIMATE_SERVICE_BASE_URL=http://127.0.0.1:9000\n", encoding="utf-8")
        config_file.write_text("imports: []\n", encoding="utf-8")

        environment = os.environ.copy()
        environment.update(
            {
                "DHIS2_CLIMATE_PROJECT_DIR": str(temporary_path),
                "DHIS2_CLIMATE_PYTHON_BIN": str(fake_python),
                "DHIS2_CLIMATE_DATE_PYTHON_BIN": sys.executable,
                "DHIS2_CLIMATE_ENV_FILE": str(env_file),
                "DHIS2_CLIMATE_CONFIG_FILE": str(config_file),
                "DHIS2_CLIMATE_STATE_DIR": str(temporary_path / "state"),
                "DHIS2_CLIMATE_JOBS": jobs,
                "DHIS2_CLIMATE_WINDOW_MODE": mode,
                "DHIS2_CLIMATE_START_PERIOD": start_period,
                "DHIS2_CLIMATE_END_PERIOD": end_period,
                "DHIS2_CLIMATE_LOOKBACK_WEEKS": "2",
                "DHIS2_CLIMATE_LAG_WEEKS": "3",
                "DHIS2_CLIMATE_PERIOD_BATCH_SIZE": period_batch_size,
                "DHIS2_CLIMATE_AS_OF": "2026-10-19",
                "DHIS2_CLIMATE_CONTINUE_ON_FAILURE": continue_on_failure,
                "DHIS2_CLIMATE_REFRESH_CACHES": "true",
                "DHIS2_CLIMATE_SKIP_MISSING_GEOMETRY": "true",
                "FAKE_CLI_CALLS": str(calls_path),
                "FAKE_FAILED_JOB": failed_job,
                "FAKE_FAILED_STATUS": failed_status,
                "FAKE_DUPLICATE_JOB": duplicate_job,
            }
        )
        result = subprocess.run(
            ["bash", str(PIPELINE_SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        calls = calls_path.read_text(encoding="utf-8").splitlines() if calls_path.exists() else []
        return result, calls

    def test_runs_ingest_review_dry_run_and_apply_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            result, calls = self._run_pipeline(Path(temporary_directory))

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        joined = "\n".join(calls)
        self.assertLess(joined.index("check-dhis2"), joined.index("cache-org-units"))
        self.assertLess(joined.index("cache-org-units"), joined.index("cache-data-elements"))
        self.assertLess(
            joined.index("ingest-dataset precipitation_weekly_chirps"),
            joined.index("review-import precipitation_weekly_chirps"),
        )
        self.assertLess(
            joined.index("--dry-run"),
            joined.index("--apply"),
        )
        self.assertLess(
            joined.index("import-dhis2 precipitation_weekly_chirps"),
            joined.index("ingest-dataset vegetation_ndvi_weekly"),
        )
        self.assertIn("--start-period 2026-W38 --end-period 2026-W39", joined)

    def test_checkpoint_skips_completed_jobs_on_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            first, first_calls = self._run_pipeline(temporary_path, jobs="vegetation_ndvi_weekly")
            second, second_calls = self._run_pipeline(temporary_path, jobs="vegetation_ndvi_weekly")

        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        new_calls = second_calls[len(first_calls) :]
        self.assertEqual(len(new_calls), 3)
        self.assertTrue(any("check-dhis2" in call for call in new_calls))
        self.assertFalse(any("ingest-dataset" in call for call in new_calls))
        self.assertIn("checkpoint réutilisé", second.stdout)

    def test_splits_historical_period_for_ocs_and_dhis2_apply(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            result, calls = self._run_pipeline(
                temporary_path,
                jobs="vegetation_ndvi_weekly",
                start_period="2025-W01",
                end_period="2025-W27",
                period_batch_size="13",
            )
            checkpoints = list((temporary_path / "state" / "checkpoints").glob("*.json"))

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        ingestion_calls = [call for call in calls if "ingest-dataset" in call]
        apply_calls = [
            call for call in calls if "import-dhis2" in call and "--apply" in call
        ]
        self.assertEqual(len(ingestion_calls), 3)
        self.assertEqual(len(apply_calls), 3)
        self.assertEqual(len(checkpoints), 3)
        self.assertIn("--start-period 2025-W01 --end-period 2025-W13", ingestion_calls[0])
        self.assertIn("--start-period 2025-W14 --end-period 2025-W26", ingestion_calls[1])
        self.assertIn("--start-period 2025-W27 --end-period 2025-W27", ingestion_calls[2])

    def test_continues_after_permanent_job_failure_and_exits_two(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            result, calls = self._run_pipeline(
                Path(temporary_directory),
                jobs="bad_job vegetation_ndvi_weekly",
                failed_job="bad_job",
                failed_status="2",
            )

        self.assertEqual(result.returncode, 2)
        joined = "\n".join(calls)
        self.assertIn("ingest-dataset bad_job", joined)
        self.assertIn("ingest-dataset vegetation_ndvi_weekly", joined)
        self.assertIn("failedJobs=bad_job", result.stdout)

    def test_returns_temporary_failure_status_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            result, _calls = self._run_pipeline(
                Path(temporary_directory),
                jobs="bad_job",
                failed_job="bad_job",
                failed_status="75",
            )

        self.assertEqual(result.returncode, 75)

    def test_journal_duplicate_is_checkpointed_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            result, calls = self._run_pipeline(
                temporary_path,
                jobs="vegetation_ndvi_weekly",
                duplicate_job="vegetation_ndvi_weekly",
            )
            checkpoints = list((temporary_path / "state" / "checkpoints").glob("*.json"))

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(checkpoints), 1)
        self.assertTrue(any("--apply" in call for call in calls))
        self.assertNotIn("--force", "\n".join(calls))
        self.assertIn("déjà confirmé par le journal local", result.stdout)

    def test_rolling_window_uses_latest_complete_week_and_lag(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            result, calls = self._run_pipeline(
                Path(temporary_directory),
                jobs="vegetation_ndvi_weekly",
                mode="rolling",
            )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("start=2026-W38 | end=2026-W39", result.stdout)
        self.assertTrue(
            any("--start-period 2026-W38 --end-period 2026-W39" in call for call in calls)
        )

    def test_units_keep_ocs_single_worker_and_order_pipeline_after_it(self) -> None:
        ocs = OCS_SERVICE.read_text(encoding="utf-8")
        pipeline = PIPELINE_SERVICE.read_text(encoding="utf-8")

        self.assertIn("--workers 1", ocs)
        self.assertNotIn("--reload", ocs)
        self.assertIn("Requires=open-climate-service.service", pipeline)
        self.assertIn("RestartPreventExitStatus=2", pipeline)


if __name__ == "__main__":
    unittest.main()
