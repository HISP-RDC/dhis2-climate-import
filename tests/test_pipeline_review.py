from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.cache import write_data_elements_cache, write_org_units_cache
from dhis2_climate_import.config import load_pipeline_config
from dhis2_climate_import.errors import PipelineError
from dhis2_climate_import.periods import periods_between
from dhis2_climate_import.pipeline import review_import


class PipelineReviewTests(unittest.TestCase):
    def test_reviews_weekly_climate_app_style_import(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config" / "import-pipeline.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                """
dhis2:
  org_unit_level: 3
  cache_max_age_hours: 168
open_climate_service:
  aggregate_process_id: aggregate_to_dhis2_json
imports:
  - id: temperature_max_weekly_era5_land
    label: Max air temperature (ERA5-Land)
    dataset_id: era5land_temperature_max_weekly
    data_set: CCH_WEEKLY
    data_element: ERA5_LAND_TEMPERATURE_MAX
    period_type: week
    method: max
    expected_units: celsius
    default_start_period: 2026-W09
    default_end_period: 2026-W31
""",
                encoding="utf-8",
            )
            write_data_elements_cache(
                root_dir=root,
                source_base_url="https://dhis2.example.org",
                data_elements=[
                    {
                        "id": "BDIuhHdQEDI",
                        "code": "ERA5_LAND_TEMPERATURE_MAX",
                        "name": "CCH - Temperature maximale de l'air (ERA5-Land)",
                    }
                ],
                data_sets=[{"id": "gKA14xjAFiw", "code": "CCH_WEEKLY", "periodType": "Weekly"}],
            )
            write_org_units_cache(
                root_dir=root,
                level=2,
                source_base_url="https://dhis2.example.org",
                organisation_units=[
                    {
                        "id": f"OuLevel2{i:04d}",
                        "level": 2,
                        "name": f"DPS {i}",
                        "path": f"/RDC/OuLevel2{i:04d}",
                        "geometry": {"type": "Point", "coordinates": [20.0, -2.0]},
                    }
                    for i in range(26)
                ],
            )

            review = review_import(
                load_pipeline_config(config_path),
                job_id="temperature_max_weekly_era5_land",
                org_unit_level=2,
                data_set="CCH_WEEKLY",
                expected_org_units=26,
                expected_data_values=598,
            )

        self.assertEqual(review["periodStart"], "2026-W09")
        self.assertEqual(review["periodEnd"], "2026-W31")
        self.assertEqual(review["dateStart"], "2026-02-23")
        self.assertEqual(review["dateEnd"], "2026-08-02")
        self.assertEqual(review["orgUnitsWithGeometry"], 26)
        self.assertEqual(review["computedDataValues"], 598)
        self.assertEqual(review["expectedDataValues"], 598)
        self.assertEqual(review["dataSet"], "CCH_WEEKLY")

    def test_review_can_use_default_dynamic_level_without_hardcoded_counts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config" / "import-pipeline.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                """
dhis2:
  org_unit_level: 3
  cache_max_age_hours: 168
open_climate_service:
  aggregate_process_id: aggregate_to_dhis2_json
imports:
  - id: temperature_max_weekly_era5_land
    label: Max air temperature (ERA5-Land)
    dataset_id: era5land_temperature_max_weekly
    data_set: CCH_WEEKLY
    data_element: ERA5_LAND_TEMPERATURE_MAX
    period_type: week
    method: max
    expected_units: celsius
    default_start_period: 2026-W09
    default_end_period: 2026-W31
""",
                encoding="utf-8",
            )
            write_data_elements_cache(
                root_dir=root,
                source_base_url="https://dhis2.example.org",
                data_elements=[
                    {
                        "id": "BDIuhHdQEDI",
                        "code": "ERA5_LAND_TEMPERATURE_MAX",
                        "name": "CCH - Temperature maximale de l'air (ERA5-Land)",
                    }
                ],
                data_sets=[{"id": "gKA14xjAFiw", "code": "CCH_WEEKLY", "periodType": "Weekly"}],
            )
            write_org_units_cache(
                root_dir=root,
                level=3,
                source_base_url="https://dhis2.example.org",
                organisation_units=[
                    {
                        "id": f"OuLevel3{i:04d}",
                        "level": 3,
                        "name": f"Zone {i}",
                        "path": f"/RDC/OuLevel3{i:04d}",
                        "geometry": {"type": "Point", "coordinates": [20.0, -2.0]},
                    }
                    for i in range(2)
                ],
            )

            review = review_import(
                load_pipeline_config(config_path),
                job_id="temperature_max_weekly_era5_land",
            )

        self.assertEqual(review["orgUnitLevel"], 3)
        self.assertEqual(review["orgUnitsWithGeometry"], 2)
        self.assertEqual(review["computedDataValues"], 46)
        self.assertIsNone(review["expectedOrgUnitsWithGeometry"])
        self.assertIsNone(review["expectedDataValues"])

    def test_review_rejects_wrong_data_set_period_type(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config" / "import-pipeline.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                """
dhis2:
  org_unit_level: 3
  cache_max_age_hours: 168
open_climate_service:
  aggregate_process_id: aggregate_to_dhis2_json
imports:
  - id: temperature_max_weekly_era5_land
    label: Max air temperature (ERA5-Land)
    dataset_id: era5land_temperature_max_weekly
    data_set: CCH_DAILY
    data_element: ERA5_LAND_TEMPERATURE_MAX
    period_type: week
    method: max
    expected_units: celsius
    default_start_period: 2026-W09
    default_end_period: 2026-W31
""",
                encoding="utf-8",
            )
            write_data_elements_cache(
                root_dir=root,
                source_base_url="https://dhis2.example.org",
                data_elements=[
                    {
                        "id": "BDIuhHdQEDI",
                        "code": "ERA5_LAND_TEMPERATURE_MAX",
                        "name": "CCH - Temperature maximale de l'air (ERA5-Land)",
                    }
                ],
                data_sets=[{"id": "DailyDataSet", "code": "CCH_DAILY", "periodType": "Daily"}],
            )

            with self.assertRaisesRegex(PipelineError, "periodType 'Daily'"):
                review_import(
                    load_pipeline_config(config_path),
                    job_id="temperature_max_weekly_era5_land",
                )

    def test_review_can_use_years_back_without_configured_default_period(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config" / "import-pipeline.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                """
dhis2:
  org_unit_level: 3
  cache_max_age_hours: 168
open_climate_service:
  aggregate_process_id: aggregate_to_dhis2_json
imports:
  - id: temperature_max_weekly_era5_land
    label: Max air temperature (ERA5-Land)
    dataset_id: era5land_temperature_max_weekly
    data_set: CCH_WEEKLY
    data_element: ERA5_LAND_TEMPERATURE_MAX
    period_type: week
    method: max
    expected_units: celsius
""",
                encoding="utf-8",
            )
            write_data_elements_cache(
                root_dir=root,
                source_base_url="https://dhis2.example.org",
                data_elements=[
                    {
                        "id": "BDIuhHdQEDI",
                        "code": "ERA5_LAND_TEMPERATURE_MAX",
                        "name": "CCH - Temperature maximale de l'air (ERA5-Land)",
                    }
                ],
                data_sets=[{"id": "gKA14xjAFiw", "code": "CCH_WEEKLY", "periodType": "Weekly"}],
            )
            write_org_units_cache(
                root_dir=root,
                level=3,
                source_base_url="https://dhis2.example.org",
                organisation_units=[
                    {
                        "id": "OuLevel30001",
                        "level": 3,
                        "name": "Zone 1",
                        "path": "/RDC/OuLevel30001",
                        "geometry": {"type": "Point", "coordinates": [20.0, -2.0]},
                    }
                ],
            )

            review = review_import(
                load_pipeline_config(config_path),
                job_id="temperature_max_weekly_era5_land",
                years_back=10,
                as_of="2026-08-27",
            )

        self.assertEqual(review["dateStart"], "2016-08-29")
        self.assertEqual(review["dateEnd"], "2026-08-23")
        self.assertEqual(review["periodStart"], "2016-W35")
        self.assertEqual(review["periodEnd"], "2026-W34")
        self.assertEqual(
            review["computedDataValues"],
            len(periods_between("2016-08-29", "2026-08-23", "week")),
        )


if __name__ == "__main__":
    unittest.main()
