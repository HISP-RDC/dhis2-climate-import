from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
QUEUE_SCRIPT = PROJECT_ROOT / "deploy" / "systemd" / "dhis2-transfer-queue.sh"


class SystemdTransferQueueTests(unittest.TestCase):
    def _run_queue(
        self,
        *,
        jobs: str,
        failed_job: str = "",
        continue_on_failure: str = "true",
    ) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            calls_path = temporary_path / "started-units.log"
            systemctl_path = temporary_path / "systemctl"
            systemctl_path.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -u

                    action="$1"
                    shift
                    case "$action" in
                      reset-failed)
                        exit 0
                        ;;
                      start)
                        unit="${@: -1}"
                        printf '%s\\n' "$unit" >> "$FAKE_SYSTEMCTL_CALLS"
                        exit 0
                        ;;
                      show)
                        unit="$1"
                        property=""
                        for argument in "$@"; do
                          case "$argument" in
                            --property=*) property="${argument#--property=}" ;;
                          esac
                        done
                        failed_unit="dhis2-transfer@${FAKE_FAILED_JOB}.service"
                        failed=false
                        if [[ -n "$FAKE_FAILED_JOB" && "$unit" == "$failed_unit" ]]; then
                          failed=true
                        fi
                        case "$property" in
                          ActiveState)
                            [[ "$failed" == true ]] && printf 'failed\\n' || printf 'inactive\\n'
                            ;;
                          SubState)
                            [[ "$failed" == true ]] && printf 'failed\\n' || printf 'dead\\n'
                            ;;
                          Result)
                            [[ "$failed" == true ]] && printf 'exit-code\\n' || printf 'success\\n'
                            ;;
                          ExecMainStatus)
                            [[ "$failed" == true ]] && printf '2\\n' || printf '0\\n'
                            ;;
                          *)
                            exit 2
                            ;;
                        esac
                        ;;
                      *)
                        exit 2
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            systemctl_path.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "SYSTEMCTL_BIN": str(systemctl_path),
                    "FAKE_SYSTEMCTL_CALLS": str(calls_path),
                    "FAKE_FAILED_JOB": failed_job,
                    "DHIS2_TRANSFER_JOBS": jobs,
                    "DHIS2_TRANSFER_CONTINUE_ON_FAILURE": continue_on_failure,
                    "DHIS2_TRANSFER_QUEUE_POLL_SECONDS": "1",
                }
            )
            result = subprocess.run(
                ["bash", str(QUEUE_SCRIPT)],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            calls = (
                calls_path.read_text(encoding="utf-8").splitlines()
                if calls_path.exists()
                else []
            )
            return result, calls

    def test_runs_jobs_in_declared_order(self) -> None:
        result, calls = self._run_queue(
            jobs="hospital_service sigl_fosa weekly_climate"
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(
            calls,
            [
                "dhis2-transfer@hospital_service.service",
                "dhis2-transfer@sigl_fosa.service",
                "dhis2-transfer@weekly_climate.service",
            ],
        )

    def test_continues_after_failure_and_reports_failed_queue(self) -> None:
        result, calls = self._run_queue(
            jobs="hospital_service sigl_fosa weekly_climate",
            failed_job="sigl_fosa",
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            calls,
            [
                "dhis2-transfer@hospital_service.service",
                "dhis2-transfer@sigl_fosa.service",
                "dhis2-transfer@weekly_climate.service",
            ],
        )
        self.assertIn("failedJobs=sigl_fosa", result.stdout)

    def test_can_stop_at_first_failure(self) -> None:
        result, calls = self._run_queue(
            jobs="hospital_service sigl_fosa weekly_climate",
            failed_job="sigl_fosa",
            continue_on_failure="false",
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            calls,
            [
                "dhis2-transfer@hospital_service.service",
                "dhis2-transfer@sigl_fosa.service",
            ],
        )


if __name__ == "__main__":
    unittest.main()
