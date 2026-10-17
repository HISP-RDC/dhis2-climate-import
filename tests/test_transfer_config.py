from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.errors import PipelineError
from dhis2_climate_import.transfer_config import (
    load_instance_registry,
    load_transfer_scenario,
)


class TransferConfigTests(unittest.TestCase):
    def test_automatic_org_unit_reconciliation_requires_enabled_policy(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config" / "scenarios" / "invalid.yaml"
            path.parent.mkdir(parents=True)
            path.write_text(
                """
schema_version: 2
id: invalid
source:
  instance: x
target:
  instance: y
  organisation_units:
    synchronization:
      enabled: false
      run_before_transfer: true
jobs:
  - id: monthly
    source_data_set: DataSet0001
    target_data_set: DataSet0001
    period_type: month
    default_start_period: 2026-01
    default_end_period: 2026-12
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(PipelineError, "requires synchronization.enabled"):
                load_transfer_scenario(path)

    def test_job_execution_override_deep_merges_global_retry_and_flags(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config" / "scenarios" / "merge.yaml"
            path.parent.mkdir(parents=True)
            path.write_text(
                """
schema_version: 2
id: merge
source:
  instance: x
target:
  instance: y
transfer:
  retry:
    attempts: 7
    initial_delay_seconds: 2
    max_delay_seconds: 10
  execution:
    process_isolation: false
    resume: false
    stop_on_error: false
jobs:
  - id: monthly
    source_data_set: DataSet0001
    target_data_set: DataSet0001
    period_type: month
    default_start_period: 2026-01
    default_end_period: 2026-12
    transfer:
      execution:
        period_batch_size: 6
""",
                encoding="utf-8",
            )
            job = load_transfer_scenario(path).get_job("monthly")

        self.assertEqual(job.transfer.retry.attempts, 7)
        self.assertEqual(job.transfer.execution.period_batch_size, 6)
        self.assertFalse(job.transfer.execution.process_isolation)
        self.assertFalse(job.transfer.execution.resume)
        self.assertFalse(job.transfer.execution.stop_on_error)

    def test_loads_basic_and_pat_instance_profiles_without_secrets_in_yaml(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "instances.yaml"
            path.write_text(
                """
schema_version: 2
instances:
  x:
    type: dhis2
    base_url_env: DHIS2_X_BASE_URL
    auth:
      method: basic
      username_env: DHIS2_X_USERNAME
      password_env: DHIS2_X_PASSWORD
  y:
    type: dhis2
    base_url_env: DHIS2_Y_BASE_URL
    auth:
      method: pat
      token_env: DHIS2_Y_PAT
""",
                encoding="utf-8",
            )
            registry = load_instance_registry(path)

        source_env = registry.get("x").client_env(
            {
                "DHIS2_X_BASE_URL": "https://x.example.org",
                "DHIS2_X_USERNAME": "reader",
                "DHIS2_X_PASSWORD": "secret",
            }
        )
        target_env = registry.get("y").client_env(
            {
                "DHIS2_Y_BASE_URL": "https://y.example.org",
                "DHIS2_Y_PAT": "d2pat_target",
            }
        )

        self.assertEqual(source_env["DHIS2_AUTH_METHOD"], "basic")
        self.assertEqual(source_env["DHIS2_USERNAME"], "reader")
        self.assertEqual(target_env["DHIS2_AUTH_METHOD"], "pat")
        self.assertEqual(target_env["DHIS2_PAT"], "d2pat_target")

    def test_loads_create_if_missing_scenario_and_linked_dataset_discovery(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config" / "scenarios" / "x-to-y.yaml"
            path.parent.mkdir(parents=True)
            path.write_text(
                """
schema_version: 2
id: x-to-y
source:
  instance: x
target:
  instance: y
  metadata:
    mode: create_if_missing
    identity_strategy: preserve_uid
    reconcile_existing: true
    import:
      strategy: CREATE
      atomic: true
    synchronization:
      historical_data_elements:
        uid_collision_strategy: clone
  organisation_units:
    discovery: linked_to_dataset
    mapping_strategy: uid
    exclude:
      - uid: BlockedOu01
        reason: Temporary hierarchy review
    synchronization:
      enabled: true
      create_missing: true
      require_same_identity: true
      allow_root_creation: false
      attach_to_target_data_set: true
      preserve_geometry: true
transfer:
  chunk_size: 100
jobs:
  - id: climate_weekly
    source_data_set: CCH_WEEKLY
    target_data_set: CCH_WEEKLY
    period_type: Weekly
    default_start_period: 2026-W01
    default_end_period: 2026-W02
""",
                encoding="utf-8",
            )
            scenario = load_transfer_scenario(path)

        job = scenario.get_job("climate_weekly")
        self.assertEqual(scenario.metadata.mode, "create_if_missing")
        self.assertTrue(scenario.metadata.reconcile_existing)
        self.assertEqual(job.period_type, "week")
        self.assertEqual(job.organisation_units.discovery, "linked_to_dataset")
        self.assertEqual(len(job.organisation_units.exclusions), 1)
        self.assertEqual(job.organisation_units.exclusions[0].uid, "BlockedOu01")
        self.assertEqual(
            job.organisation_units.exclusions[0].reason,
            "Temporary hierarchy review",
        )
        self.assertTrue(job.organisation_units.synchronization.enabled)
        self.assertFalse(
            job.organisation_units.synchronization.run_before_transfer
        )
        self.assertEqual(
            job.organisation_units.synchronization.conflict_policy,
            "block",
        )
        self.assertTrue(job.organisation_units.synchronization.create_missing)
        self.assertTrue(job.organisation_units.synchronization.require_same_name)
        self.assertTrue(job.organisation_units.synchronization.require_same_code)
        self.assertFalse(job.organisation_units.synchronization.allow_root_creation)
        self.assertTrue(
            job.organisation_units.synchronization.attach_to_target_data_set
        )
        self.assertEqual(job.transfer.chunk_size, 100)
        self.assertEqual(job.transfer.source_period_batch_size, 13)
        self.assertEqual(job.transfer.execution.period_batch_size, 13)
        self.assertTrue(job.transfer.execution.process_isolation)
        self.assertTrue(job.transfer.execution.resume)
        self.assertEqual(job.transfer.retry.attempts, 5)
        self.assertEqual(scenario.max_parallel_jobs, 1)
        self.assertTrue(
            scenario.metadata.synchronization.historical_data_elements.enabled
        )
        self.assertEqual(
            scenario.metadata.synchronization.historical_data_elements.uid_collision_strategy,
            "clone",
        )
        self.assertEqual(scenario.project_root, path.parents[2])

    def test_global_dynamic_org_unit_conflict_policy_applies_to_every_job(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config" / "scenarios" / "dynamic.yaml"
            path.parent.mkdir(parents=True)
            path.write_text(
                """
schema_version: 2
id: dynamic
source:
  instance: x
target:
  instance: y
  organisation_units:
    mapping_strategy: uid
    synchronization:
      enabled: true
      run_before_transfer: true
      conflict_policy: exclude
jobs:
  - id: first
    source_data_set: DataSet0001
    target_data_set: DataSet0001
    period_type: month
    default_start_period: 2026-01
    default_end_period: 2026-01
  - id: second
    source_data_set: DataSet0002
    target_data_set: DataSet0002
    period_type: month
    default_start_period: 2026-01
    default_end_period: 2026-01
""",
                encoding="utf-8",
            )

            scenario = load_transfer_scenario(path)

        self.assertEqual(
            [
                job.organisation_units.synchronization.conflict_policy
                for job in scenario.jobs
            ],
            ["exclude", "exclude"],
        )

    def test_dynamic_org_unit_conflict_policy_requires_automatic_reconciliation(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config" / "scenarios" / "invalid-dynamic.yaml"
            path.parent.mkdir(parents=True)
            path.write_text(
                """
schema_version: 2
id: invalid-dynamic
source:
  instance: x
target:
  instance: y
  organisation_units:
    mapping_strategy: uid
    synchronization:
      enabled: true
      run_before_transfer: false
      conflict_policy: exclude
jobs:
  - id: monthly
    source_data_set: DataSet0001
    target_data_set: DataSet0001
    period_type: month
    default_start_period: 2026-01
    default_end_period: 2026-01
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                PipelineError,
                "requires run_before_transfer: true",
            ):
                load_transfer_scenario(path)

    def test_rejects_duplicate_or_unexplained_org_unit_exclusions(self) -> None:
        template = """
schema_version: 2
id: invalid-exclusions
source:
  instance: x
target:
  instance: y
jobs:
  - id: monthly
    source_data_set: DataSet0001
    target_data_set: DataSet0001
    period_type: month
    default_start_period: 2026-01
    default_end_period: 2026-01
    organisation_units:
      exclude:
{entries}
"""
        cases = (
            (
                "        - uid: BlockedOu01\n"
                "          reason: first\n"
                "        - uid: BlockedOu01\n"
                "          reason: duplicate",
                "Duplicate organisation unit exclusion UID",
            ),
            (
                "        - uid: BlockedOu01",
                r"organisation_units.exclude\[0\].reason.*required",
            ),
        )
        for entries, expected_error in cases:
            with self.subTest(expected_error=expected_error), TemporaryDirectory() as tmp:
                path = Path(tmp) / "config" / "scenarios" / "invalid.yaml"
                path.parent.mkdir(parents=True)
                path.write_text(template.format(entries=entries), encoding="utf-8")

                with self.assertRaisesRegex(PipelineError, expected_error):
                    load_transfer_scenario(path)


if __name__ == "__main__":
    unittest.main()
