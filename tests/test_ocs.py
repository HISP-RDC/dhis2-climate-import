from pathlib import Path
import unittest

from dhis2_climate_import.ocs import OCSClient


class FakeHttp:
    def __init__(self) -> None:
        self.last_call = None

    def request_json(self, method, path, *, params=None, json_body=None, headers=None):
        self.last_call = (method, path, params, json_body)
        return {"dataValues": []}


class OCSTests(unittest.TestCase):
    def test_aggregate_builds_openeo_process_graph(self) -> None:
        http = FakeHttp()
        response = OCSClient("http://ocs.example.org", http=http).aggregate_to_dhis2_json(
            process_id="aggregate_to_dhis2_json",
            dataset_id="chirps3_precipitation_daily",
            org_units_geojson_path=Path("cache/org-units-level-3.geojson"),
            data_element_id="DataEl00001",
            period_type="day",
            start="2024-01-01",
            end="2024-01-31",
            method="mean",
        )
        method, path, _params, body = http.last_call
        self.assertEqual(response, {"dataValues": []})
        self.assertEqual(method, "POST")
        self.assertEqual(path, "/result")
        self.assertEqual(
            body["process_graph"]["aggregate"]["arguments"]["dataset_id"],
            "chirps3_precipitation_daily",
        )


if __name__ == "__main__":
    unittest.main()
