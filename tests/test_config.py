from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.config import load_pipeline_config


class ConfigTests(unittest.TestCase):
    def test_loads_project_yaml_subset(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "config" / "import-pipeline.yaml"
            path.parent.mkdir()
            path.write_text(
                """
dhis2:
  org_unit_level: 3
  climate_data_set: CCH_DAILY
  climate_data_sets:
    - CCH_DAILY
    - CCH_WEEKLY
  preheat_cache: true
open_climate_service:
  aggregate_process_id: aggregate_to_dhis2_json
imports:
  - id: precipitation_daily
    label: Precipitation
    dataset_id: chirps3_precipitation_daily
    data_set: CCH_DAILY
    data_element: CCH_PRECIP
    period_type: day
    method: mean
    expected_units: mm
    default_start: 2024-01-01
    default_end: 2024-01-31
""",
                encoding="utf-8",
            )
            config = load_pipeline_config(path)

        self.assertEqual(config.dhis2.org_unit_level, 3)
        self.assertEqual(config.dhis2.climate_data_set, "CCH_DAILY")
        self.assertEqual(config.dhis2.climate_data_sets, ("CCH_DAILY", "CCH_WEEKLY"))
        self.assertTrue(config.dhis2.preheat_cache)
        self.assertEqual(config.get_job("precipitation_daily").dataset_id, "chirps3_precipitation_daily")
        self.assertEqual(config.get_job("precipitation_daily").data_set, "CCH_DAILY")


if __name__ == "__main__":
    unittest.main()
