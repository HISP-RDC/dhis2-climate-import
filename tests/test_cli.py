import json
import unittest
from unittest.mock import patch

from dhis2_climate_import.cli import (
    _dhis2_error_details,
    _check_transfer_instance,
    _format_http_error,
    build_parser,
    cmd_transfer_reconcile_organisation_units,
    cmd_transfer_reconcile_metadata,
    cmd_transfer_repair_metadata,
    cmd_transfer_run_window,
)
from dhis2_climate_import.http import HttpRequestError


class CliErrorFormattingTests(unittest.TestCase):
    def test_retries_transient_transfer_connection_check(self) -> None:
        class Client:
            def __init__(self) -> None:
                self.calls = 0

            def check(self, *, min_version):
                self.calls += 1
                if self.calls == 1:
                    raise HttpRequestError(
                        "GET",
                        "https://example.org/api/system/info",
                        503,
                        "temporarily unavailable",
                    )

        client = Client()
        with patch("dhis2_climate_import.cli.time.sleep") as sleep:
            _check_transfer_instance(
                client,
                label="target",
                attempts=2,
                initial_delay=1,
                max_delay=10,
                progress=None,
            )

        self.assertEqual(client.calls, 2)
        sleep.assert_called_once_with(1)

    def test_parses_explicit_metadata_repair_dry_run(self) -> None:
        args = build_parser().parse_args(
            [
                "transfer",
                "--scenario",
                "config/scenarios/x-to-y.yaml",
                "repair-metadata",
                "hospital_service",
                "--dry-run",
            ]
        )

        self.assertEqual(args.job_id, "hospital_service")
        self.assertTrue(args.dry_run)
        self.assertIs(args.func, cmd_transfer_repair_metadata)

    def test_formats_nested_dhis2_conflicts_without_hiding_error_reports(self) -> None:
        body = json.dumps(
            {
                "response": {
                    "typeReports": [
                        {
                            "objectReports": [
                                {
                                    "errorReports": [
                                        {
                                            "errorCode": "E5000",
                                            "message": "Object already exists for UID Abc12345678",
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
        )
        error = HttpRequestError("POST", "https://example.org/api/metadata", 409, body)

        self.assertEqual(
            _dhis2_error_details(body),
            ["E5000: Object already exists for UID Abc12345678"],
        )
        self.assertIn(
            "E5000: Object already exists for UID Abc12345678",
            _format_http_error(error),
        )

    def test_compacts_e8024_indexes_and_keeps_conflict_arguments(self) -> None:
        body = json.dumps(
            {
                "response": {
                    "conflicts": [
                        {
                            "errorCode": "E8024",
                            "indexes": list(range(5000)),
                            "args": [
                                "DataSet0001",
                                "DataEl00001",
                                ["HistoricC01"],
                            ],
                        }
                    ]
                }
            }
        )
        error = HttpRequestError("POST", "https://example.org/api/dataValueSets", 409, body)

        formatted = _format_http_error(error)

        self.assertIn("E8024", formatted)
        self.assertIn("affectedValues=5000", formatted)
        self.assertIn("HistoricC01", formatted)
        self.assertNotIn("4999", formatted)

    def test_parses_reconcile_and_internal_window_commands(self) -> None:
        reconcile = build_parser().parse_args(
            [
                "transfer",
                "--scenario",
                "config/scenarios/x-to-y.yaml",
                "reconcile-metadata",
                "hospital_service",
                "--dry-run",
            ]
        )
        window = build_parser().parse_args(
            [
                "transfer",
                "--scenario",
                "config/scenarios/x-to-y.yaml",
                "run-window",
                "hospital_service",
                "--start",
                "2026-01-01",
                "--end",
                "2026-01-31",
                "--apply",
                "--parent-run-id",
                "parent-1",
                "--window-id",
                "window-1",
            ]
        )
        organisation_units = build_parser().parse_args(
            [
                "transfer",
                "--scenario",
                "config/scenarios/x-to-y.yaml",
                "reconcile-org-units",
                "hospital_service",
                "--dry-run",
            ]
        )

        self.assertIs(reconcile.func, cmd_transfer_reconcile_metadata)
        self.assertIs(
            organisation_units.func,
            cmd_transfer_reconcile_organisation_units,
        )
        self.assertIs(window.func, cmd_transfer_run_window)


if __name__ == "__main__":
    unittest.main()
