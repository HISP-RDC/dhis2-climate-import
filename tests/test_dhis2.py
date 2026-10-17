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
                "shortName": "Climate Daily",
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


class FakeTransferHttp:
    def __init__(self) -> None:
        self.calls = []

    def request_json(self, method, path, *, params=None, json_body=None, headers=None):
        self.calls.append((method, path, params, json_body))
        if method == "GET" and path.endswith("/metadata.json"):
            return {"dataSets": [{"id": "DataSet0001"}]}
        if method == "POST" and path == "/api/metadata":
            return {"status": "OK"}
        if method == "GET" and path == "/api/dataValueSets.json":
            return {"dataValues": [{"dataElement": "DataEl00001"}]}
        if method == "POST" and path == "/api/dataValueSets":
            return {"status": "SUCCESS", "importCount": {"imported": 1}}
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

    def test_paginates_all_org_units_for_transfer_matching(self) -> None:
        client = DHIS2Client("https://dhis2.example.org", http=FakeHttp())
        units = client.organisation_units(page_size=1)
        self.assertEqual([unit["id"] for unit in units], ["OU1", "OU2"])

    def test_resolves_dataset_then_fetches_linked_data_elements(self) -> None:
        client = DHIS2Client("https://dhis2.example.org", http=FakeHttp())
        data_set = client.resolve_data_set("CCH_DAILY")
        elements = client.data_elements_for_data_set(data_set_id=data_set.id, page_size=100)

        self.assertEqual(data_set.id, "DataSet0001")
        self.assertEqual(elements[0]["code"], "CCH_PRECIP")

    def test_data_set_details_requests_required_update_identity(self) -> None:
        http = FakeHttp()
        client = DHIS2Client("https://dhis2.example.org", http=http)

        details = client.data_set_details("DataSet0001")

        self.assertEqual(details["shortName"], "Climate Daily")
        self.assertIn("shortName", http.calls[-1][2]["fields"])

    def test_metadata_and_data_transfer_endpoints(self) -> None:
        http = FakeTransferHttp()
        client = DHIS2Client("https://dhis2.example.org", http=http)

        metadata = client.export_data_set_metadata("DataSet0001", skip_sharing=True)
        validation = client.import_metadata(
            metadata,
            commit=False,
            import_strategy="CREATE",
            atomic_mode="ALL",
        )
        exported = client.export_data_values(
            data_set_id="DataSet0001",
            start="2026-01-01",
            end="2026-01-31",
            organisation_unit_ids=["OrgUnit00001"],
            include_children=False,
            include_deleted=False,
            id_schemes={"dataElementIdScheme": "UID"},
        )
        imported = client.import_data_value_set(
            exported,
            dry_run=True,
            import_strategy="CREATE_AND_UPDATE",
            preheat_cache=True,
            async_mode=False,
            id_schemes={"dataElementIdScheme": "UID"},
        )

        self.assertEqual(validation["status"], "OK")
        self.assertEqual(imported["importCount"]["imported"], 1)
        metadata_call = http.calls[1]
        self.assertEqual(metadata_call[2]["importMode"], "VALIDATE")
        self.assertEqual(metadata_call[2]["atomicMode"], "ALL")
        export_call = http.calls[2]
        self.assertEqual(export_call[2]["orgUnit"], ["OrgUnit00001"])


if __name__ == "__main__":
    unittest.main()
