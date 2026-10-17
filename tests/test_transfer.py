from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dhis2_climate_import.dhis2 import DataSetRef
from dhis2_climate_import.errors import PipelineError
from dhis2_climate_import.http import HttpRequestError
from dhis2_climate_import.transfer import (
    _prepare_metadata_package,
    _remove_existing_metadata_objects,
    _validate_metadata_package,
    repair_target_metadata,
    review_transfer,
    run_transfer,
)
from dhis2_climate_import.transfer_config import load_transfer_scenario


class FakeTransferClient:
    def __init__(
        self,
        base_url: str,
        *,
        target_has_dataset: bool = False,
        target_dataset_by_uid_only: bool = False,
        target_units: list[dict] | None = None,
        global_target_elements: list[dict] | None = None,
    ) -> None:
        self.base_url = base_url
        self.target_has_dataset = target_has_dataset
        self.target_dataset_by_uid_only = target_dataset_by_uid_only
        self.metadata_calls: list[bool] = []
        self.data_import_calls: list[bool] = []
        self.export_calls: list[dict] = []
        self.organisation_unit_list_calls = 0
        self.data_set = DataSetRef(
            id="DataSet0001",
            code="CCH_WEEKLY",
            name="Climate Weekly",
            period_type="Weekly",
        )
        self.elements = [
            {
                "id": "DataEl00001",
                "code": "CCH_RAIN",
                "name": "Rainfall",
            },
            {
                "id": "DataEl00002",
                "code": "CCH_TEMP",
                "name": "Temperature",
            },
        ]
        self.target_units = target_units if target_units is not None else [
            {"id": "OrgUnit00001", "code": "OU-1", "name": "Zone 1"}
        ]
        self.global_target_elements = global_target_elements or []

    def resolve_data_set(self, selector):
        return self.data_set

    def find_data_set(self, selector):
        if not self.target_has_dataset:
            return None
        if self.target_dataset_by_uid_only and selector != self.data_set.id:
            return None
        return self.data_set

    def data_elements_for_data_set(self, *, data_set_id, page_size):
        return self.elements

    def data_elements(self, *, page_size):
        return self.global_target_elements

    def organisation_units_for_data_set(self, data_set_id):
        return [{"id": "OrgUnit00001", "code": "OU-1", "name": "Zone 1"}]

    def organisation_units(self, *, page_size=1000):
        self.organisation_unit_list_calls += 1
        return self.target_units

    def resolve_organisation_unit(self, selector):
        matches = [
            unit
            for unit in self.target_units
            if selector in {unit.get("id"), unit.get("code")}
        ]
        if len(matches) != 1:
            raise PipelineError(f"Organisation unit '{selector}' was not found uniquely.")
        return matches[0]

    def export_data_set_metadata(self, data_set_id, *, skip_sharing):
        return {
            "dataSets": [{"id": "DataSet0001", "code": "CCH_WEEKLY"}],
            "dataElements": self.elements,
        }

    def import_metadata(
        self,
        payload,
        *,
        commit,
        import_strategy,
        atomic_mode,
        identifier,
        skip_sharing,
    ):
        self.metadata_calls.append(commit)
        if commit:
            self.target_has_dataset = True
        return {"status": "OK", "importCount": {"imported": len(payload)}}

    def export_data_values(
        self,
        *,
        data_set_id,
        start,
        end,
        organisation_unit_ids,
        include_children,
        include_deleted,
        id_schemes,
    ):
        self.export_calls.append(
            {
                "start": start,
                "end": end,
                "organisation_unit_ids": organisation_unit_ids,
            }
        )
        return {
            "dataValues": [
                {
                    "dataElement": "DataEl00001",
                    "period": "2026W01",
                    "orgUnit": "OrgUnit00001",
                    "categoryOptionCombo": "CatCombo001",
                    "attributeOptionCombo": "CatCombo001",
                    "value": "12.5",
                },
                {
                    "dataElement": "DataEl00002",
                    "period": "2026W01",
                    "orgUnit": "OrgUnit00001",
                    "categoryOptionCombo": "CatCombo001",
                    "attributeOptionCombo": "CatCombo001",
                    "value": "24.1",
                },
            ]
        }

    def import_data_value_set(
        self,
        payload,
        *,
        dry_run,
        import_strategy,
        preheat_cache,
        async_mode,
        id_schemes,
    ):
        self.data_import_calls.append(dry_run)
        return {
            "status": "SUCCESS",
            "importCount": {
                "imported": len(payload["dataValues"]),
                "updated": 0,
                "ignored": 0,
                "deleted": 0,
            },
        }


class TransferTests(unittest.TestCase):
    def test_review_discovers_data_elements_and_requires_bootstrap(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = FakeTransferClient("https://x.example.org")
            target = FakeTransferClient("https://y.example.org")

            review = review_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
            )

            self.assertTrue(review["metadataBootstrapRequired"])
            self.assertEqual(review["sourceDataElementCount"], 2)
            self.assertEqual(review["sourceOrganisationUnitCount"], 1)
            self.assertEqual(review["targetMatchedOrganisationUnitCount"], 1)
            self.assertEqual(target.organisation_unit_list_calls, 1)
            self.assertTrue(
                (scenario.state_dir / "cache" / "source" / "weekly_climate.json").exists()
            )

    def test_dry_run_validates_missing_metadata_without_committing(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=1)
            source = FakeTransferClient("https://x.example.org")
            target = FakeTransferClient("https://y.example.org")

            report = run_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
            )

            self.assertEqual(target.metadata_calls, [False])
            self.assertEqual(target.data_import_calls, [])
            self.assertTrue(report["metadata"]["validated"])
            self.assertFalse(report["metadata"]["committed"])
            self.assertTrue(report["dataValidationPendingMetadataCommit"])
            self.assertEqual(report["sourceDataValueCount"], 2)

    def test_source_export_is_split_by_configured_dhis2_period_batch(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                period_type="month",
                default_start_period="2026-01",
                default_end_period="2026-03",
                source_period_batch_size=1,
            )
            source = FakeTransferClient("https://x.example.org")
            source.data_set = DataSetRef(
                id="DataSet0001",
                code="CCH_WEEKLY",
                name="Hospital",
                period_type="Monthly",
            )
            target = FakeTransferClient(
                "https://y.example.org",
                target_has_dataset=True,
            )
            target.data_set = source.data_set

            run_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
            )

            self.assertEqual(
                [(item["start"], item["end"]) for item in source.export_calls],
                [
                    ("2026-01-01", "2026-01-31"),
                    ("2026-02-01", "2026-02-28"),
                    ("2026-03-01", "2026-03-31"),
                ],
            )

    def test_source_export_timeout_subdivides_organisation_unit_batch(self) -> None:
        class TimeoutSource(FakeTransferClient):
            def organisation_units_for_data_set(self, data_set_id):
                return [
                    {"id": f"OrgUnit0000{index}", "code": f"OU-{index}"}
                    for index in range(1, 5)
                ]

            def export_data_values(self, **kwargs):
                unit_ids = kwargs["organisation_unit_ids"]
                self.export_calls.append(kwargs)
                if len(unit_ids) > 1:
                    raise HttpRequestError(
                        "GET",
                        "https://x.example.org/api/dataValueSets.json",
                        None,
                        "The read operation timed out",
                    )
                return {
                    "dataValues": [
                        {
                            "dataElement": "DataEl00001",
                            "period": "2026W01",
                            "orgUnit": unit_ids[0],
                            "categoryOptionCombo": "CatCombo001",
                            "attributeOptionCombo": "CatCombo001",
                            "value": "12.5",
                        }
                    ]
                }

        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = TimeoutSource("https://x.example.org")
            target_units = [
                {"id": f"OrgUnit0000{index}", "code": f"OU-{index}"}
                for index in range(1, 5)
            ]
            target = FakeTransferClient(
                "https://y.example.org",
                target_has_dataset=True,
                target_units=target_units,
            )
            events = []

            report = run_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
                progress=lambda event, details: events.append((event, details)),
            )

            self.assertEqual(report["sourceDataValueCount"], 4)
            self.assertEqual(len(source.export_calls), 15)
            self.assertEqual(
                sum(event == "source.export.request.split" for event, _ in events),
                7,
            )

    def test_review_rejects_incompatible_existing_target_data_element(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = FakeTransferClient("https://x.example.org")
            source.elements[0]["valueType"] = "NUMBER"
            target = FakeTransferClient(
                "https://y.example.org", target_has_dataset=True
            )
            target.elements[0]["valueType"] = "TEXT"

            with self.assertRaises(PipelineError):
                review_transfer(
                    scenario,
                    source,
                    target,
                    job_id="weekly_climate",
                )

    def test_review_falls_back_to_source_uid_when_target_code_is_missing(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = FakeTransferClient("https://x.example.org")
            target = FakeTransferClient(
                "https://y.example.org",
                target_has_dataset=True,
                target_dataset_by_uid_only=True,
            )

            review = review_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
            )

            self.assertFalse(review["metadataBootstrapRequired"])
            self.assertEqual(review["targetDataSet"]["id"], "DataSet0001")

    def test_review_blocks_when_source_organisation_unit_is_missing_on_target(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = FakeTransferClient("https://x.example.org")
            target = FakeTransferClient(
                "https://y.example.org",
                target_units=[],
            )

            with self.assertRaisesRegex(
                PipelineError, "Organisation units differ between source and target"
            ):
                review_transfer(
                    scenario,
                    source,
                    target,
                    job_id="weekly_climate",
                )

    def test_review_accepts_different_uid_when_mapping_organisation_units_by_code(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_mapping="code",
            )
            source = FakeTransferClient("https://x.example.org")
            target = FakeTransferClient(
                "https://y.example.org",
                target_units=[
                    {"id": "Different001", "code": "OU-1", "name": "Zone 1"}
                ],
            )

            review = review_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
            )

            self.assertEqual(review["organisationUnitMappingStrategy"], "code")
            self.assertEqual(review["targetMatchedOrganisationUnitCount"], 1)

    def test_apply_bootstraps_metadata_then_dry_runs_and_imports_values(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=1)
            source = FakeTransferClient("https://x.example.org")
            target = FakeTransferClient("https://y.example.org")

            report = run_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=True,
            )

            self.assertEqual(target.metadata_calls, [False, True])
            self.assertEqual(target.data_import_calls, [True, True, False, False])
            self.assertTrue(report["metadata"]["committed"])
            self.assertEqual(report["counts"]["imported"], 2)
            self.assertTrue(Path(report["reportPath"]).exists())

            with self.assertRaises(PipelineError):
                run_transfer(
                    scenario,
                    source,
                    target,
                    job_id="weekly_climate",
                    apply=True,
                )

    def test_prepares_minimal_metadata_without_invalid_source_dependencies(self) -> None:
        source_package = {
            "categories": [
                {"id": "GLevLNI9wkl", "code": "default"},
                {"id": "Category001", "name": "Age"},
            ],
            "categoryOptions": [
                {"id": "xYerKDKCefk", "code": "default"},
                {"id": "Option00001", "name": "Under five"},
            ],
            "categoryCombos": [
                {"id": "bjDvmb4bfuf", "code": "default"},
                {"id": "Combo000001", "categories": [{"id": "Category001"}]},
            ],
            "categoryOptionCombos": [
                {
                    "id": "HllvX50cXC0",
                    "categoryCombo": {"id": "bjDvmb4bfuf"},
                    "categoryOptions": [{"id": "xYerKDKCefk"}],
                },
                {
                    "id": "OptionCom001",
                    "categoryCombo": {"id": "Combo000001"},
                    "categoryOptions": [{"id": "Option00001"}],
                },
                {
                    "id": "OrphanCo001",
                    "categoryCombo": {"id": "Missing0001"},
                    "categoryOptions": [{"id": "Option00001"}],
                },
            ],
            "dataElements": [
                {
                    "id": "DataEl00001",
                    "categoryCombo": {"id": "Combo000001"},
                }
            ],
            "dataSets": [
                {
                    "id": "DataSet0001",
                    "compulsoryDataElementOperands": [
                        {
                            "dataElement": {"id": "DataEl00001"},
                            "categoryOptionCombo": {"id": "OrphanCo001"},
                        }
                    ],
                }
            ],
        }

        prepared, report = _prepare_metadata_package(source_package)

        self.assertEqual(report["removedIncompatibleCompulsoryOperands"], 1)
        self.assertEqual(report["removedOrphanCategoryOptionCombos"], 2)
        self.assertEqual(
            [item["id"] for item in prepared["categoryOptionCombos"]],
            ["OptionCom001"],
        )
        self.assertEqual(
            prepared["dataSets"][0]["compulsoryDataElementOperands"], []
        )

        filtered, removed = _remove_existing_metadata_objects(
            prepared, {"Combo000001", "DataEl00001"}
        )
        self.assertEqual(removed["dataElements"], 1)
        self.assertEqual(removed["categoryCombos"], 1)
        self.assertEqual(removed["categoryOptionCombos"], 1)
        self.assertEqual(filtered["dataSets"][0]["id"], "DataSet0001")

    def test_metadata_validation_reuses_e5000_objects_without_updating_them(self) -> None:
        class MetadataTarget:
            def __init__(self):
                self.payloads = []

            def import_metadata(self, payload, **kwargs):
                self.payloads.append(payload)
                if len(self.payloads) == 1:
                    body = (
                        '{"response":{"typeReports":[{"objectReports":['
                        '{"errorReports":[{"errorCode":"E5000",'
                        '"mainId":"DataEl00001"}]}]}]}}'
                    )
                    raise HttpRequestError("POST", "https://y/api/metadata", 409, body)
                return {"status": "OK"}

        target = MetadataTarget()
        package = {
            "dataElements": [{"id": "DataEl00001"}],
            "dataSets": [{"id": "DataSet0001"}],
        }

        filtered, response, reused = _validate_metadata_package(
            target,
            package,
            import_strategy="CREATE",
            atomic_mode="ALL",
            skip_sharing=True,
        )

        self.assertEqual(response["status"], "OK")
        self.assertEqual(reused, {"dataElements": 1})
        self.assertEqual(filtered["dataElements"], [])
        self.assertEqual(filtered["dataSets"], [{"id": "DataSet0001"}])

    def test_metadata_repair_dry_run_audits_and_validates_only_structure_stage(self) -> None:
        class RepairSource(FakeTransferClient):
            def __init__(self):
                super().__init__("https://x.example.org")
                self.elements = [
                    {
                        "id": "DataEl00001",
                        "code": "DE_1",
                        "name": "Consultations",
                        "shortName": "Consultations",
                        "valueType": "INTEGER",
                        "domainType": "AGGREGATE",
                        "aggregationType": "SUM",
                        "categoryCombo": {"id": "Combo000001"},
                    }
                ]

            def export_data_set_metadata(self, data_set_id, *, skip_sharing):
                return {
                    "categories": [
                        {
                            "id": "Category001",
                            "name": "Sexe",
                            "shortName": "Sexe",
                            "categoryOptions": [{"id": "Option00001"}],
                        }
                    ],
                    "categoryOptions": [
                        {
                            "id": "Option00001",
                            "name": "Féminin",
                            "shortName": "Féminin",
                        }
                    ],
                    "categoryCombos": [
                        {
                            "id": "Combo000001",
                            "name": "Sexe",
                            "categories": [{"id": "Category001"}],
                        }
                    ],
                    "categoryOptionCombos": [
                        {
                            "id": "OptCombo001",
                            "name": "Féminin",
                            "categoryCombo": {"id": "Combo000001"},
                            "categoryOptions": [{"id": "Option00001"}],
                        }
                    ],
                    "dataElements": self.elements,
                    "dataSets": [{"id": "DataSet0001", "code": "CCH_WEEKLY"}],
                }

        class RepairTarget(FakeTransferClient):
            def __init__(self, *, raw_values=None):
                target_element = {
                    "id": "DataEl00001",
                    "code": "DE_1",
                    "name": "Consultations",
                    "shortName": "Consultations",
                    "valueType": "INTEGER",
                    "domainType": "AGGREGATE",
                    "aggregationType": "SUM",
                    "categoryCombo": {"id": "bjDvmb4bfuf", "code": "default"},
                    "dataSetElements": [],
                }
                super().__init__(
                    "https://y.example.org",
                    global_target_elements=[target_element],
                )
                self.raw_values = raw_values or []
                self.objects = {
                    "categoryCombos": [
                        {"id": "Combo000001", "name": "Sexe", "categories": []}
                    ],
                    "categories": [
                        {
                            "id": "Category001",
                            "name": "Sexe",
                            "shortName": "Sexe",
                            "categoryOptions": [{"id": "Option00001"}],
                        }
                    ],
                    "categoryOptions": [
                        {
                            "id": "Option00001",
                            "name": "Féminin",
                            "shortName": "Féminin",
                        }
                    ],
                }

            def metadata_objects_by_ids(
                self, *, resource, root_key, object_ids, fields, batch_size=40
            ):
                return [
                    item
                    for item in self.objects.get(root_key, [])
                    if item["id"] in object_ids
                ]

            def user_organisation_unit_roots(self):
                return [{"id": "OrgUnit00001", "name": "Root"}]

            def find_data_values(self, **kwargs):
                return self.raw_values

            def category_option_combos_for_category_combos(
                self, category_combo_ids, *, page_size=500
            ):
                return []

            def import_metadata(self, payload, *, commit, **kwargs):
                self.metadata_calls.append(commit)
                if commit and payload.get("categoryCombos"):
                    by_id = {item["id"]: item for item in self.objects["categoryCombos"]}
                    for item in payload["categoryCombos"]:
                        by_id[item["id"]]["categories"] = item.get("categories", [])
                if commit and payload.get("dataElements"):
                    by_id = {item["id"]: item for item in self.global_target_elements}
                    for item in payload["dataElements"]:
                        by_id[item["id"]]["categoryCombo"] = item["categoryCombo"]
                return {"status": "OK"}

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                reconcile_existing=True,
            )
            target = RepairTarget()
            report = repair_target_metadata(
                scenario,
                RepairSource(),
                target,
                job_id="weekly_climate",
                apply=False,
            )

            self.assertEqual(report["status"], "validated")
            self.assertEqual(report["repair"]["categoryCombosToUpdate"], 1)
            self.assertEqual(report["repair"]["categoryOptionCombosToCreate"], 1)
            self.assertEqual(report["repair"]["dataElementsToUpdate"], 1)
            self.assertTrue(
                report["assignments"]["validationPendingStructureCommit"]
            )
            self.assertEqual(target.metadata_calls, [False])

            apply_target = RepairTarget()
            apply_report = repair_target_metadata(
                scenario,
                RepairSource(),
                apply_target,
                job_id="weekly_climate",
                apply=True,
            )
            self.assertEqual(apply_report["status"], "repaired")
            self.assertEqual(
                apply_target.metadata_calls,
                [False, True, False, True],
            )
            self.assertTrue(apply_report["assignments"]["validated"])
            self.assertTrue(apply_report["assignments"]["committed"])

            with self.assertRaisesRegex(PipelineError, "raw or deleted target data value"):
                repair_target_metadata(
                    scenario,
                    RepairSource(),
                    RepairTarget(
                        raw_values=[
                            {
                                "dataElement": "DataEl00001",
                                "period": "202601",
                                "orgUnit": "OrgUnit00001",
                            }
                        ]
                    ),
                    job_id="weekly_climate",
                    apply=False,
                )

    def test_metadata_repair_requires_explicit_scenario_opt_in(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                reconcile_existing=False,
            )
            with self.assertRaisesRegex(PipelineError, "Metadata repair is disabled"):
                repair_target_metadata(
                    scenario,
                    FakeTransferClient("https://x.example.org"),
                    FakeTransferClient("https://y.example.org"),
                    job_id="weekly_climate",
                    apply=False,
                )


def _scenario(
    root: Path,
    *,
    chunk_size: int,
    organisation_unit_mapping: str = "uid",
    reconcile_existing: bool = False,
    period_type: str = "week",
    default_start_period: str = "2026-W01",
    default_end_period: str = "2026-W02",
    source_period_batch_size: int | None = None,
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
    reconcile_existing: {str(reconcile_existing).lower()}
    skip_sharing: true
    import:
      strategy: CREATE
      atomic: true
  organisation_units:
    discovery: linked_to_dataset
    mapping_strategy: {organisation_unit_mapping}
mapping:
  data_elements: uid
  organisation_units: {organisation_unit_mapping}
  category_option_combos: uid
  attribute_option_combos: uid
  data_sets: uid
transfer:
  import_strategy: CREATE_AND_UPDATE
  chunk_size: {chunk_size}
  {f"source_period_batch_size: {source_period_batch_size}" if source_period_batch_size else ""}
  include_deleted: false
state:
  directory: state
jobs:
  - id: weekly_climate
    source_data_set: CCH_WEEKLY
    target_data_set: CCH_WEEKLY
    period_type: {period_type}
    default_start_period: {default_start_period}
    default_end_period: {default_end_period}
""",
        encoding="utf-8",
    )
    return load_transfer_scenario(path)


if __name__ == "__main__":
    unittest.main()
