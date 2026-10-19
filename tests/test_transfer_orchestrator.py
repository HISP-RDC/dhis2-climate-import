from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dhis2_climate_import.dhis2 import DataSetRef
from dhis2_climate_import.errors import PipelineError, TransientPipelineError
from dhis2_climate_import.transfer_config import load_transfer_scenario
from dhis2_climate_import.transfer_orchestrator import orchestrate_transfer


class TransferOrchestratorTests(unittest.TestCase):
    def test_splits_36_months_into_12_windows_then_resumes_without_reimport(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(Path(tmp))
            context = SimpleNamespace(
                job=scenario.get_job("hospital"),
                source_data_set=DataSetRef(
                    id="DataSet0001",
                    code="HOSPITAL",
                    name="Hospital",
                    period_type="Monthly",
                ),
                start="2017-01-01",
                end="2019-12-31",
            )
            source = SimpleNamespace(base_url="https://x.example.org")
            target = SimpleNamespace(base_url="https://y.example.org")
            calls: list[tuple[str, str]] = []

            def runner(
                start: str,
                end: str,
                _index: int,
                _count: int,
                _window_id: str,
                _parent_run_id: str,
            ):
                calls.append((start, end))
                return {
                    "sourceDataValueCount": 10,
                    "counts": {
                        "imported": 10,
                        "updated": 0,
                        "ignored": 0,
                        "deleted": 0,
                    },
                    "reportPath": f"{start}.json",
                }

            with (
                patch(
                    "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                    return_value={"requiresChanges": False, "committed": True},
                ),
                patch(
                    "dhis2_climate_import.transfer_orchestrator.resolve_transfer_context",
                    return_value=context,
                ),
            ):
                first = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=True,
                    window_runner=runner,
                )
                second = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=True,
                    window_runner=runner,
                )

            self.assertEqual(first["windowCount"], 12)
            self.assertEqual(first["completedWindows"], 12)
            self.assertEqual(first["sourceDataValueCount"], 120)
            self.assertEqual(len(calls), 12)
            self.assertEqual(calls[0], ("2017-01-01", "2017-03-31"))
            self.assertEqual(calls[-1], ("2019-10-01", "2019-12-31"))
            self.assertEqual(second["completedWindows"], 0)
            self.assertEqual(second["skippedWindows"], 12)

    def test_aggregates_historical_metadata_quarantine_without_failing_windows(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(Path(tmp))
            context = SimpleNamespace(
                job=scenario.get_job("hospital"),
                source_data_set=DataSetRef(
                    id="DataSet0001",
                    code="HOSPITAL",
                    name="Hospital",
                    period_type="Monthly",
                ),
                start="2017-01-01",
                end="2019-12-31",
            )
            source = SimpleNamespace(base_url="https://x.example.org")
            target = SimpleNamespace(base_url="https://y.example.org")

            def runner(*_args):
                return {
                    "sourceDataValueCount": 10,
                    "counts": {"imported": 9},
                    "historicalDataElements": {
                        "quarantinedDataValueCount": 1,
                        "quarantine": {
                            "policy": "unsafe_historical_category_metadata",
                            "quarantinedDataValueCount": 1,
                            "artifact": "quarantine.json",
                        },
                    },
                    "reportPath": "window.json",
                }

            with (
                patch(
                    "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                    return_value={"requiresChanges": False, "committed": True},
                ),
                patch(
                    "dhis2_climate_import.transfer_orchestrator.resolve_transfer_context",
                    return_value=context,
                ),
            ):
                result = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=True,
                    window_runner=runner,
                )

            self.assertEqual(result["status"], "success_with_quarantine")
            self.assertEqual(result["completedWindows"], 12)
            self.assertEqual(result["failedWindows"], 0)
            self.assertEqual(result["sourceDataValueCount"], 120)
            self.assertEqual(result["targetDataValueCount"], 108)
            self.assertEqual(
                result["historicalDataValueQuarantine"][
                    "quarantinedDataValueCount"
                ],
                12,
            )

    def test_metadata_dry_run_stops_before_value_export_when_changes_are_required(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(Path(tmp))
            source = SimpleNamespace(base_url="https://x.example.org")
            target = SimpleNamespace(base_url="https://y.example.org")
            with patch(
                "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                return_value={"requiresChanges": True, "committed": False},
            ):
                result = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=False,
                    window_runner=lambda *_args: self.fail("value export must not run"),
                )

            self.assertEqual(result["status"], "metadata_changes_required")
            self.assertEqual(result["windowCount"], 0)

    def test_organisation_unit_dry_run_runs_automatically_and_stops_before_export(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(
                Path(tmp),
                run_org_unit_reconciliation=True,
            )
            source = SimpleNamespace(base_url="https://x.example.org")
            target = SimpleNamespace(base_url="https://y.example.org")
            with (
                patch(
                    "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                    return_value={"requiresChanges": False, "committed": False},
                ),
                patch(
                    "dhis2_climate_import.transfer_orchestrator."
                    "reconcile_target_organisation_units",
                    return_value={
                        "requiresChanges": True,
                        "committed": False,
                        "status": "validated",
                    },
                ) as reconcile_units,
            ):
                result = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=False,
                    window_runner=lambda *_args: self.fail(
                        "value export must not run"
                    ),
                )

            reconcile_units.assert_called_once()
            self.assertEqual(result["status"], "metadata_changes_required")
            self.assertEqual(result["windowCount"], 0)
            self.assertTrue(result["organisationUnits"]["requiresChanges"])

    def test_retries_transient_error_but_not_permanent_error(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(Path(tmp))
            context = SimpleNamespace(
                job=scenario.get_job("hospital"),
                source_data_set=DataSetRef(
                    id="DataSet0001",
                    code="HOSPITAL",
                    name="Hospital",
                    period_type="Monthly",
                ),
                start="2017-01-01",
                end="2017-01-31",
            )
            source = SimpleNamespace(base_url="https://x.example.org")
            target = SimpleNamespace(base_url="https://y.example.org")
            attempts = 0

            def transient_then_success(*_args):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise TransientPipelineError("temporary reset")
                return {
                    "sourceDataValueCount": 1,
                    "counts": {"imported": 1},
                    "reportPath": "window.json",
                }

            common_patches = (
                patch(
                    "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                    return_value={"requiresChanges": False, "committed": True},
                ),
                patch(
                    "dhis2_climate_import.transfer_orchestrator.resolve_transfer_context",
                    return_value=context,
                ),
                patch("dhis2_climate_import.transfer_orchestrator.time.sleep"),
            )
            with common_patches[0], common_patches[1], common_patches[2]:
                result = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=True,
                    window_runner=transient_then_success,
                )
            self.assertEqual(attempts, 2)
            self.assertEqual(result["completedWindows"], 1)

        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(Path(tmp))
            context.job = scenario.get_job("hospital")
            permanent_attempts = 0

            def permanent(*_args):
                nonlocal permanent_attempts
                permanent_attempts += 1
                raise PipelineError("metadata collision")

            with (
                patch(
                    "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                    return_value={"requiresChanges": False, "committed": True},
                ),
                patch(
                    "dhis2_climate_import.transfer_orchestrator.resolve_transfer_context",
                    return_value=context,
                ),
                self.assertRaises(PipelineError),
            ):
                orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=True,
                    window_runner=permanent,
                )
            self.assertEqual(permanent_attempts, 1)

    def test_historical_metadata_dry_run_is_not_checkpointed_as_data_success(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(Path(tmp))
            context = SimpleNamespace(
                job=scenario.get_job("hospital"),
                source_data_set=DataSetRef(
                    id="DataSet0001",
                    code="HOSPITAL",
                    name="Hospital",
                    period_type="Monthly",
                ),
                start="2017-01-01",
                end="2017-01-31",
            )
            source = SimpleNamespace(base_url="https://x.example.org")
            target = SimpleNamespace(base_url="https://y.example.org")

            def pending(*_args):
                return {
                    "sourceDataValueCount": 9,
                    "counts": {},
                    "dataValidationPendingHistoricalMetadataCommit": True,
                    "historicalDataElements": {"mappings": [{"targetDataElement": "HistDE00001"}]},
                    "reportPath": "pending.json",
                }

            with (
                patch(
                    "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                    return_value={"requiresChanges": False, "committed": False},
                ),
                patch(
                    "dhis2_climate_import.transfer_orchestrator.resolve_transfer_context",
                    return_value=context,
                ),
            ):
                result = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=False,
                    window_runner=pending,
                )

            self.assertEqual(result["status"], "metadata_changes_required")
            self.assertEqual(result["metadataPendingWindows"], 1)
            self.assertEqual(result["completedWindows"], 0)
            self.assertEqual(result["sourceDataValueCount"], 9)

    def test_parent_summary_aggregates_e8031_quarantine(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _monthly_scenario(Path(tmp))
            context = SimpleNamespace(
                job=scenario.get_job("hospital"),
                source_data_set=DataSetRef(
                    id="DataSet0001",
                    code="HOSPITAL",
                    name="Hospital",
                    period_type="Monthly",
                ),
                start="2017-01-01",
                end="2017-03-31",
            )
            source = SimpleNamespace(base_url="https://x.example.org")
            target = SimpleNamespace(base_url="https://y.example.org")

            def quarantined(*_args):
                return {
                    "status": "success_with_quarantine",
                    "sourceDataValueCount": 10,
                    "counts": {"imported": 5},
                    "invalidDataValueQuarantine": {
                        "errorCode": "E8120",
                        "quarantinedDataValueCount": 2,
                        "artifact": "e8120.json",
                    },
                    "timelinessQuarantine": {
                        "errorCode": "E8031",
                        "quarantinedDataValueCount": 3,
                        "organisationUnitCount": 1,
                        "periodCount": 2,
                        "groups": [],
                        "artifact": "e8031.json",
                    },
                    "reportPath": "window.json",
                }

            with (
                patch(
                    "dhis2_climate_import.transfer_orchestrator.reconcile_target_metadata",
                    return_value={"requiresChanges": False, "committed": True},
                ),
                patch(
                    "dhis2_climate_import.transfer_orchestrator.resolve_transfer_context",
                    return_value=context,
                ),
            ):
                result = orchestrate_transfer(
                    scenario,
                    source,
                    target,
                    job_id="hospital",
                    apply=True,
                    window_runner=quarantined,
                )

            self.assertEqual(result["status"], "success_with_quarantine")
            self.assertEqual(result["sourceDataValueCount"], 10)
            self.assertEqual(result["targetDataValueCount"], 5)
            self.assertEqual(
                result["invalidDataValueQuarantine"]["quarantinedDataValueCount"],
                2,
            )
            self.assertEqual(
                result["timelinessQuarantine"]["quarantinedDataValueCount"],
                3,
            )
            self.assertEqual(
                result["windows"][0]["status"],
                "success_with_quarantine",
            )


def _monthly_scenario(
    root: Path,
    *,
    run_org_unit_reconciliation: bool = False,
):
    path = root / "config" / "scenarios" / "x-to-y.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        f"""
schema_version: 2
id: x-to-y
source:
  instance: x
target:
  instance: y
  metadata:
    mode: create_if_missing
    identity_strategy: preserve_uid
    import:
      strategy: CREATE
      atomic: true
  organisation_units:
    synchronization:
      enabled: {str(run_org_unit_reconciliation).lower()}
      run_before_transfer: {str(run_org_unit_reconciliation).lower()}
transfer:
  retry:
    attempts: 2
    initial_delay_seconds: 1
    max_delay_seconds: 1
state:
  directory: state
jobs:
  - id: hospital
    source_data_set: DataSet0001
    target_data_set: DataSet0001
    period_type: month
    default_start_period: 2017-01
    default_end_period: 2019-12
    transfer:
      execution:
        period_batch_size: 3
        process_isolation: false
        resume: true
        stop_on_error: true
""",
        encoding="utf-8",
    )
    return load_transfer_scenario(path)


if __name__ == "__main__":
    unittest.main()
