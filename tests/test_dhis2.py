import unittest

from dhis2_climate_import.dhis2 import DHIS2Client, parse_dhis2_version


class FakeHttp:
    def __init__(self) -> None:
        self.calls = []

    def request_json(self, method, path, *, params=None, json_body=None, headers=None):
        self.calls.append((method, path, params, json_body))
        if path == "/api/system/info":
            return {"version": "2.42.1"}
        if path == "/api/me":
            return {"username": "climate-bot", "name": "Climate Bot"}
        if path == "/api/organisationUnits" and params["page"] == 1:
            return {
                "organisationUnits": [{"id": "OU1"}],
                "pager": {"page": 1, "pageCount": 2},
            }
        if path == "/api/organisationUnits" and params["page"] == 2:
            return {
                "organisationUnits": [{"id": "OU2"}],
                "pager": {"page": 2, "pageCount": 2},
            }
        if path == "/api/dataSets":
            return {
                "dataSets": [
                    {
                        "id": "DataSet0001",
                        "code": "CCH_DAILY",
                        "name": "Climate Daily",
                        "periodType": "Daily",
                    }
                ],
                "pager": {"page": 1, "pageCount": 1},
            }
        if path == "/api/dataSets/DataSet0001":
            return {
                "id": "DataSet0001",
                "code": "CCH_DAILY",
                "name": "Climate Daily",
                "periodType": "Daily",
                "dataSetElements": [
                    {
                        "dataElement": {
                            "id": "DataEl00001",
                            "code": "CCH_PRECIP",
                            "displayName": "Rain",
                        }
                    }
                ],
            }
        raise AssertionError((method, path, params, json_body))


class Dhis2Tests(unittest.TestCase):
    def test_parse_dhis2_version(self) -> None:
        self.assertEqual(parse_dhis2_version("2.42.1"), (2, 42))

    def test_check_reads_system_info_and_me(self) -> None:
        result = DHIS2Client("https://dhis2.example.org", http=FakeHttp()).check()
        self.assertEqual(result.version, "2.42.1")
        self.assertEqual(result.user, "climate-bot")

    def test_paginates_org_units(self) -> None:
        client = DHIS2Client("https://dhis2.example.org", http=FakeHttp())
        units = client.organisation_units_level(level=3, page_size=1)
        self.assertEqual([unit["id"] for unit in units], ["OU1", "OU2"])

    def test_resolves_dataset_then_fetches_linked_data_elements(self) -> None:
        client = DHIS2Client("https://dhis2.example.org", http=FakeHttp())
        data_set = client.resolve_data_set("CCH_DAILY")
        elements = client.data_elements_for_data_set(data_set_id=data_set.id, page_size=100)

        self.assertEqual(data_set.id, "DataSet0001")
        self.assertEqual(elements[0]["code"], "CCH_PRECIP")


if __name__ == "__main__":
    unittest.main()
