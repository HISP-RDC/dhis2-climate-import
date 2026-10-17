from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.transfer_config import (
    load_instance_registry,
    load_transfer_scenario,
)


class TransferConfigTests(unittest.TestCase):
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
  organisation_units:
    discovery: linked_to_dataset
    mapping_strategy: uid
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
        self.assertEqual(job.transfer.chunk_size, 100)
        self.assertEqual(job.transfer.source_period_batch_size, 13)
        self.assertEqual(scenario.project_root, path.parents[2])


if __name__ == "__main__":
    unittest.main()
