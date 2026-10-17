import json
import unittest

from dhis2_climate_import.cli import (
    _dhis2_error_details,
    _format_http_error,
    build_parser,
    cmd_transfer_repair_metadata,
)
from dhis2_climate_import.http import HttpRequestError


class CliErrorFormattingTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
