from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.cache import (
    load_data_elements_cache,
    load_org_units_cache,
    resolve_data_element,
    write_data_elements_cache,
    write_org_units_cache,
)
from dhis2_climate_import.errors import PipelineError


class CacheTests(unittest.TestCase):
    def test_writes_and_loads_level_3_geojson_cache(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_org_units_cache(
                root_dir=root,
                level=3,
                source_base_url="https://dhis2.example.org",
                organisation_units=[
                    {
                        "id": "OuLevel3001",
                        "code": "OU-1",
                        "name": "Health Zone A",
                        "path": "/Root/OuLevel3001",
                        "level": 3,
                        "geometry": {"type": "Point", "coordinates": [12.0, -3.0]},
                    }
                ],
            )
            geojson, manifest = load_org_units_cache(root_dir=root, level=3, max_age_hours=1)

        self.assertEqual(manifest["count"], 1)
        self.assertEqual(geojson["features"][0]["id"], "OuLevel3001")
        self.assertEqual(geojson["features"][0]["properties"]["level"], 3)

    def test_rejects_missing_geometry(self) -> None:
        with TemporaryDirectory() as tmp:
            with self.assertRaises(PipelineError):
                write_org_units_cache(
                    root_dir=Path(tmp),
                    level=3,
                    source_base_url="https://dhis2.example.org",
                    organisation_units=[{"id": "OuLevel3001", "level": 3}],
                )

    def test_can_audit_and_skip_units_without_geometry(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_org_units_cache(
                root_dir=root,
                level=3,
                source_base_url="https://dhis2.example.org",
                organisation_units=[
                    {
                        "id": "OuWithGeom1",
                        "name": "With geometry",
                        "level": 3,
                        "geometry": {"type": "Point", "coordinates": [12.0, -3.0]},
                    },
                    {"id": "OuWithout01", "name": "Without geometry", "level": 3},
                ],
                skip_missing_geometry=True,
            )
            geojson, manifest = load_org_units_cache(root_dir=root, level=3, max_age_hours=1)

        self.assertEqual(manifest["count"], 1)
        self.assertEqual(manifest["sourceCount"], 2)
        self.assertEqual(manifest["excludedWithoutGeometry"], 1)
        self.assertEqual(manifest["excludedOrganisationUnitIds"], ["OuWithout01"])
        self.assertEqual([feature["id"] for feature in geojson["features"]], ["OuWithGeom1"])

    def test_resolves_data_element_by_code_or_uid(self) -> None:
        elements = [{"id": "DataEl00001", "code": "CCH_PRECIP", "name": "Rain"}]
        self.assertEqual(resolve_data_element(elements, "CCH_PRECIP")["id"], "DataEl00001")
        self.assertEqual(resolve_data_element(elements, "DataEl00001")["code"], "CCH_PRECIP")
        self.assertEqual(resolve_data_element(elements, "Rain")["id"], "DataEl00001")

    def test_writes_data_elements_cache_with_dataset_scope(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_data_elements_cache(
                root_dir=root,
                source_base_url="https://dhis2.example.org",
                data_elements=[{"id": "DataEl00001", "code": "CCH_PRECIP"}],
                data_sets=[
                    {"id": "DataSet0001", "code": "CCH_DAILY", "name": "Climate Daily"},
                    {"id": "DataSet0002", "code": "CCH_WEEKLY", "name": "Climate Weekly"},
                ],
            )
            elements = load_data_elements_cache(root)

        self.assertEqual(elements[0]["id"], "DataEl00001")


if __name__ == "__main__":
    unittest.main()
