import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from dhis2_climate_import.dhis2 import DataSetRef
from dhis2_climate_import.errors import PipelineError
from dhis2_climate_import.http import HttpRequestError
from dhis2_climate_import.transfer import (
    TransferContext,
    _data_set_membership_package,
    _import_target_chunks,
    _normalise_source_category_option_combos,
    _prepare_metadata_package,
    _remove_existing_metadata_objects,
    _restrict_data_set_organisation_units,
    _validate_target_chunks,
    _validate_target_category_option_combos,
    _validate_metadata_package,
    _version_unmappable_historical_values,
    reconcile_target_organisation_units,
    reconcile_target_metadata,
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

    def data_set_details(self, data_set_id):
        linked = self.data_elements_for_data_set(
            data_set_id=data_set_id,
            page_size=500,
        )
        return {
            "id": data_set_id,
            "code": self.data_set.code,
            "name": self.data_set.name,
            "shortName": "Climate Weekly",
            "periodType": self.data_set.period_type,
            "categoryCombo": {"id": "CatCombo001"},
            "dataSetElements": [
                {
                    "dataSet": {"id": data_set_id},
                    "dataElement": {"id": item["id"]},
                }
                for item in linked
            ],
            "organisationUnits": self.target_units,
        }

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
    def test_target_validation_timeout_splits_only_the_failing_chunk(self) -> None:
        class SlowTarget:
            def __init__(self) -> None:
                self.calls: list[int] = []

            def import_data_value_set(self, payload, **kwargs):
                size = len(payload["dataValues"])
                self.calls.append(size)
                if size > 2:
                    raise HttpRequestError(
                        "POST",
                        "https://y.example.org/api/dataValueSets",
                        None,
                        "The read operation timed out",
                    )
                return {
                    "status": "SUCCESS",
                    "importCount": {"imported": size},
                }

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=4,
                adaptive_minimum_chunk_size=2,
                adaptive_retry_before_split=0,
            )
            context = SimpleNamespace(job=scenario.get_job("weekly_climate"))
            target = SlowTarget()
            values = [
                {"orgUnit": "OpenOrg0001", "period": "201701", "value": str(i)}
                for i in range(4)
            ]

            responses, accepted, report = _validate_target_chunks(
                target,
                [{"dataSet": "DataSet0001", "dataValues": values}],
                context=context,
                state_dir=scenario.job_state_dir("weekly_climate"),
                current_run_id="adaptive-validation",
            )

            self.assertEqual(target.calls, [4, 2, 2])
            self.assertEqual(len(responses), 2)
            self.assertEqual([len(item["dataValues"]) for item in accepted], [2, 2])
            self.assertEqual(report["adaptiveChunking"]["splits"], 1)
            self.assertEqual(report["adaptiveChunking"]["smallestChunkSize"], 2)

    def test_target_import_timeout_splits_and_checkpoints_children(self) -> None:
        class SlowTarget:
            base_url = "https://y.example.org"

            def __init__(self) -> None:
                self.calls: list[int] = []

            def import_data_value_set(self, payload, **kwargs):
                size = len(payload["dataValues"])
                self.calls.append(size)
                if size > 2:
                    raise HttpRequestError(
                        "POST",
                        "https://y.example.org/api/dataValueSets",
                        None,
                        "The read operation timed out",
                    )
                return {
                    "status": "SUCCESS",
                    "importCount": {"imported": size},
                }

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=4,
                adaptive_minimum_chunk_size=2,
                adaptive_retry_before_split=0,
            )
            context = SimpleNamespace(
                job=scenario.get_job("weekly_climate"),
                target_data_set=DataSetRef(
                    id="DataSet0001",
                    code="CCH_WEEKLY",
                    name="Climate Weekly",
                    period_type="Weekly",
                ),
                start="2017-01-01",
                end="2017-01-31",
            )
            target = SlowTarget()
            values = [
                {"orgUnit": "OpenOrg0001", "period": "201701", "value": str(i)}
                for i in range(4)
            ]

            responses, report = _import_target_chunks(
                target,
                [{"dataSet": "DataSet0001", "dataValues": values}],
                context=context,
                state_dir=scenario.job_state_dir("weekly_climate"),
                resume_checkpoints=True,
                progress=None,
            )

            self.assertEqual(target.calls, [4, 2, 2])
            self.assertEqual(len(responses), 2)
            self.assertEqual(report["splits"], 1)
            checkpoint = Path(report["checkpoint"])
            self.assertTrue(checkpoint.exists())
            payload = json.loads(checkpoint.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["completedChunks"]), 2)

    def test_confirmed_target_chunks_resume_without_another_post(self) -> None:
        class Target:
            base_url = "https://y.example.org"

            def __init__(self, *, reject_calls: bool = False) -> None:
                self.calls = 0
                self.reject_calls = reject_calls

            def import_data_value_set(self, payload, **kwargs):
                self.calls += 1
                if self.reject_calls:
                    raise AssertionError("checkpointed chunk must not be posted again")
                return {
                    "status": "SUCCESS",
                    "importCount": {"imported": len(payload["dataValues"])},
                }

        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=2)
            context = SimpleNamespace(
                job=scenario.get_job("weekly_climate"),
                target_data_set=DataSetRef(
                    id="DataSet0001",
                    code="CCH_WEEKLY",
                    name="Climate Weekly",
                    period_type="Weekly",
                ),
                start="2017-01-01",
                end="2017-01-31",
            )
            chunks = [
                {
                    "dataSet": "DataSet0001",
                    "dataValues": [
                        {
                            "dataElement": "DataEl00001",
                            "orgUnit": "OpenOrg0001",
                            "period": "201701",
                            "value": "1",
                        }
                    ],
                }
            ]
            first = Target()
            _import_target_chunks(
                first,
                chunks,
                context=context,
                state_dir=scenario.job_state_dir("weekly_climate"),
                resume_checkpoints=True,
                progress=None,
            )
            resumed = Target(reject_calls=True)
            responses, report = _import_target_chunks(
                resumed,
                chunks,
                context=context,
                state_dir=scenario.job_state_dir("weekly_climate"),
                resume_checkpoints=True,
                progress=None,
            )

            self.assertEqual(first.calls, 1)
            self.assertEqual(resumed.calls, 0)
            self.assertTrue(responses[0]["checkpoint"])
            self.assertEqual(report["resumedChunks"], 1)
            self.assertEqual(report["resumedDataValues"], 1)

    def test_e8031_values_are_quarantined_and_remaining_values_are_revalidated(
        self,
    ) -> None:
        class TimelinessTarget:
            def __init__(self) -> None:
                self.calls: list[list[str]] = []

            def import_data_value_set(self, payload, **kwargs):
                units = [item["orgUnit"] for item in payload["dataValues"]]
                self.calls.append(units)
                rejected = [
                    index
                    for index, item in enumerate(payload["dataValues"])
                    if item["orgUnit"] == "ClosedOu001"
                ]
                if rejected:
                    raise HttpRequestError(
                        "POST",
                        "https://y.example.org/api/dataValueSets",
                        409,
                        json.dumps(
                            {
                                "response": {
                                    "conflicts": [
                                        {"errorCode": "E8031", "indexes": rejected}
                                    ]
                                }
                            }
                        ),
                    )
                return {
                    "status": "SUCCESS",
                    "importCount": {"imported": len(payload["dataValues"])},
                }

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
                organisation_unit_run_before_transfer=True,
                organisation_unit_conflict_policy="exclude",
            )
            context = SimpleNamespace(job=scenario.get_job("weekly_climate"))
            target = TimelinessTarget()
            chunks = [
                {
                    "dataSet": "DataSet0001",
                    "dataValues": [
                        {
                            "dataElement": "DataEl00001",
                            "period": "201701",
                            "orgUnit": "ClosedOu001",
                            "value": "1",
                        },
                        {
                            "dataElement": "DataEl00001",
                            "period": "201701",
                            "orgUnit": "OpenOrg0001",
                            "value": "2",
                        },
                        {
                            "dataElement": "DataEl00002",
                            "period": "201702",
                            "orgUnit": "ClosedOu001",
                            "value": "3",
                        },
                    ],
                }
            ]

            responses, accepted, report = _validate_target_chunks(
                target,
                chunks,
                context=context,
                state_dir=scenario.job_state_dir("weekly_climate"),
                current_run_id="transfer-test",
            )

            self.assertEqual(
                target.calls,
                [
                    ["ClosedOu001", "OpenOrg0001", "ClosedOu001"],
                    ["OpenOrg0001"],
                ],
            )
            self.assertEqual(len(responses), 1)
            self.assertEqual(
                [item["orgUnit"] for item in accepted[0]["dataValues"]],
                ["OpenOrg0001"],
            )
            self.assertEqual(report["quarantinedDataValueCount"], 2)
            self.assertEqual(report["organisationUnitCount"], 1)
            self.assertEqual(report["periodCount"], 2)
            artifact = Path(report["artifact"])
            self.assertTrue(artifact.exists())
            artifact_payload = json.loads(artifact.read_text(encoding="utf-8"))
            self.assertEqual(len(artifact_payload["values"]), 2)

    def test_e8031_is_not_silently_quarantined_with_another_conflict(self) -> None:
        class MixedConflictTarget:
            def import_data_value_set(self, payload, **kwargs):
                raise HttpRequestError(
                    "POST",
                    "https://y.example.org/api/dataValueSets",
                    409,
                    json.dumps(
                        {
                            "response": {
                                "conflicts": [
                                    {"errorCode": "E8031", "indexes": [0]},
                                    {"errorCode": "E8024", "indexes": [0]},
                                ]
                            }
                        }
                    ),
                )

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
                organisation_unit_run_before_transfer=True,
                organisation_unit_conflict_policy="exclude",
            )
            context = SimpleNamespace(job=scenario.get_job("weekly_climate"))
            with self.assertRaises(HttpRequestError):
                _validate_target_chunks(
                    MixedConflictTarget(),
                    [{"dataValues": [{"orgUnit": "ClosedOu001"}]}],
                    context=context,
                    state_dir=scenario.job_state_dir("weekly_climate"),
                    current_run_id="transfer-test",
                )

    def test_e8031_quarantine_blocks_an_empty_execution_window(self) -> None:
        class ClosedTarget:
            def import_data_value_set(self, payload, **kwargs):
                raise HttpRequestError(
                    "POST",
                    "https://y.example.org/api/dataValueSets",
                    409,
                    json.dumps(
                        {
                            "response": {
                                "conflicts": [
                                    {"errorCode": "E8031", "indexes": [0]}
                                ]
                            }
                        }
                    ),
                )

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
                organisation_unit_run_before_transfer=True,
                organisation_unit_conflict_policy="exclude",
            )
            context = SimpleNamespace(job=scenario.get_job("weekly_climate"))
            with self.assertRaisesRegex(PipelineError, "remove every source data value"):
                _validate_target_chunks(
                    ClosedTarget(),
                    [
                        {
                            "dataValues": [
                                {
                                    "orgUnit": "ClosedOu001",
                                    "period": "201701",
                                }
                            ]
                        }
                    ],
                    context=context,
                    state_dir=scenario.job_state_dir("weekly_climate"),
                    current_run_id="transfer-test",
                )

    def test_e8031_audit_survives_a_later_validation_conflict(self) -> None:
        class LaterConflictTarget:
            def __init__(self) -> None:
                self.calls = 0

            def import_data_value_set(self, payload, **kwargs):
                self.calls += 1
                error_code = "E8031" if self.calls == 1 else "E8120"
                raise HttpRequestError(
                    "POST",
                    "https://y.example.org/api/dataValueSets",
                    409,
                    json.dumps(
                        {
                            "response": {
                                "conflicts": [
                                    {"errorCode": error_code, "indexes": [0]}
                                ]
                            }
                        }
                    ),
                )

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
                organisation_unit_run_before_transfer=True,
                organisation_unit_conflict_policy="exclude",
            )
            state_dir = scenario.job_state_dir("weekly_climate")
            context = SimpleNamespace(job=scenario.get_job("weekly_climate"))
            with self.assertRaises(HttpRequestError):
                _validate_target_chunks(
                    LaterConflictTarget(),
                    [
                        {
                            "dataValues": [
                                {"orgUnit": "ClosedOu001", "period": "201701"},
                                {"orgUnit": "OpenOrg0001", "period": "201701"},
                            ]
                        }
                    ],
                    context=context,
                    state_dir=state_dir,
                    current_run_id="transfer-test",
                )

            artifact = (
                state_dir
                / "runs"
                / "transfer-test"
                / "data-value-quarantine"
                / "e8031.json"
            )
            self.assertTrue(artifact.exists())
            payload = json.loads(artifact.read_text(encoding="utf-8"))
            self.assertEqual(payload["quarantinedDataValueCount"], 1)

    def test_run_quarantines_empty_values_but_preserves_numeric_zero(self) -> None:
        class Source(FakeTransferClient):
            def export_data_values(self, **kwargs):
                return {
                    "dataValues": [
                        {
                            "dataElement": "DataEl00001",
                            "period": "2026W01",
                            "orgUnit": "OrgUnit00001",
                            "value": "   ",
                        },
                        {
                            "dataElement": "DataEl00002",
                            "period": "2026W01",
                            "orgUnit": "OrgUnit00001",
                            "value": 0,
                        },
                    ]
                }

        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            report = run_transfer(
                scenario,
                Source("https://x.example.org"),
                FakeTransferClient(
                    "https://y.example.org",
                    target_has_dataset=True,
                ),
                job_id="weekly_climate",
                apply=False,
            )

            quarantine = report["invalidDataValueQuarantine"]
            self.assertEqual(report["status"], "success_with_quarantine")
            self.assertEqual(report["sourceDataValueCount"], 2)
            self.assertEqual(report["targetDataValueCount"], 1)
            self.assertEqual(quarantine["quarantinedDataValueCount"], 1)
            self.assertTrue(Path(quarantine["artifact"]).exists())

    def test_metadata_bootstrap_keeps_only_effective_org_unit_selection(self) -> None:
        package = {
            "dataSets": [
                {
                    "id": "DataSet0001",
                    "organisationUnits": [
                        {"id": "IncludedOu1"},
                        {"id": "BlockedOu01"},
                    ],
                },
                {
                    "id": "OtherData01",
                    "organisationUnits": [{"id": "BlockedOu01"}],
                },
            ]
        }

        _restrict_data_set_organisation_units(
            package,
            data_set_id="DataSet0001",
            allowed_uids={"IncludedOu1"},
        )

        self.assertEqual(
            package["dataSets"][0]["organisationUnits"],
            [{"id": "IncludedOu1"}],
        )
        self.assertEqual(
            package["dataSets"][1]["organisationUnits"],
            [{"id": "BlockedOu01"}],
        )

    def test_reconcile_org_units_creates_missing_hierarchy_parent_first(self) -> None:
        root = {
            "id": "RootUnit001",
            "code": "ROOT",
            "name": "Country",
            "shortName": "Country",
            "openingDate": "2000-01-01",
            "path": "/RootUnit001",
            "level": 1,
        }
        parent = {
            "id": "ParentUnit1",
            "code": "PARENT",
            "name": "Province",
            "shortName": "Province",
            "openingDate": "2000-01-01",
            "path": "/RootUnit001/ParentUnit1",
            "level": 2,
            "parent": {"id": "RootUnit001"},
        }
        child = {
            "id": "ChildUnit01",
            "code": "CHILD",
            "name": "Zone",
            "shortName": "Zone",
            "openingDate": "2000-01-01",
            "path": "/RootUnit001/ParentUnit1/ChildUnit01",
            "level": 3,
            "parent": {"id": "ParentUnit1"},
        }

        class OrgUnitClient:
            def __init__(self, base_url, units, *, direct_ids=(), target=False):
                self.base_url = base_url
                self.units = {item["id"]: dict(item) for item in units}
                self.direct_ids = set(direct_ids)
                self.target = target
                self.assignments = {"RootUnit001"} if target else set()
                self.metadata_payloads = []
                self.data_set = DataSetRef(
                    id="DataSet0001",
                    code="CCH_WEEKLY",
                    name="Climate Weekly",
                    period_type="Weekly",
                )

            def resolve_data_set(self, selector):
                return self.data_set

            def find_data_set(self, selector):
                return self.data_set if self.target else None

            def organisation_units_for_data_set(self, data_set_id):
                return [self.units[uid] for uid in sorted(self.direct_ids)]

            def organisation_units(self, *, page_size=1000):
                return list(self.units.values())

            def metadata_objects_by_ids(
                self, *, resource, root_key, object_ids, fields, batch_size=40
            ):
                return [self.units[uid] for uid in object_ids if uid in self.units]

            def data_set_details(self, data_set_id):
                return {
                    "id": data_set_id,
                    "code": "CCH_WEEKLY",
                    "name": "Climate Weekly",
                    "shortName": "Climate Weekly",
                    "periodType": "Weekly",
                    "dataSetElements": [],
                    "organisationUnits": [
                        {"id": uid} for uid in sorted(self.assignments)
                    ],
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
                self.metadata_payloads.append((commit, payload))
                if commit:
                    for item in payload.get("organisationUnits", []):
                        stored = dict(item)
                        parent_uid = (stored.get("parent") or {}).get("id")
                        parent_path = (
                            self.units[parent_uid]["path"] if parent_uid else ""
                        )
                        stored["path"] = f"{parent_path}/{stored['id']}"
                        stored["level"] = len(
                            [part for part in stored["path"].split("/") if part]
                        )
                        self.units[stored["id"]] = stored
                    for data_set in payload.get("dataSets", []):
                        self.assignments = {
                            item["id"] for item in data_set.get("organisationUnits", [])
                        }
                return {"status": "OK"}

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
            )
            source = OrgUnitClient(
                "https://x.example.org",
                [root, parent, child],
                direct_ids=["ChildUnit01"],
            )
            target = OrgUnitClient(
                "https://y.example.org",
                [root],
                target=True,
            )

            dry_run = reconcile_target_organisation_units(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
            )
            package = target.metadata_payloads[0][1]

            self.assertEqual(dry_run["status"], "validated")
            self.assertEqual(
                [item["id"] for item in package["organisationUnits"]],
                ["ParentUnit1", "ChildUnit01"],
            )
            self.assertEqual(
                package["organisationUnits"][1]["parent"]["id"],
                "ParentUnit1",
            )
            self.assertEqual(target.metadata_payloads[0][0], False)

            applied = reconcile_target_organisation_units(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=True,
            )

            self.assertEqual(applied["status"], "created")
            self.assertIn("ChildUnit01", target.units)
            self.assertIn("ChildUnit01", target.assignments)

    def test_reconcile_org_units_blocks_existing_unit_under_different_parent(self) -> None:
        source_units = [
            {
                "id": "RootUnit001",
                "code": "ROOT",
                "name": "Country",
                "path": "/RootUnit001",
                "level": 1,
            },
            {
                "id": "ChildUnit01",
                "code": "CHILD",
                "name": "Zone",
                "path": "/RootUnit001/ChildUnit01",
                "level": 2,
                "parent": {"id": "RootUnit001"},
            },
        ]

        class Source(FakeTransferClient):
            def organisation_units_for_data_set(self, data_set_id):
                return [source_units[1]]

            def metadata_objects_by_ids(self, **kwargs):
                wanted = set(kwargs["object_ids"])
                return [item for item in source_units if item["id"] in wanted]

        class Target(FakeTransferClient):
            def __init__(self):
                super().__init__(
                    "https://y.example.org",
                    target_has_dataset=True,
                    target_units=[
                        source_units[0],
                        {
                            **source_units[1],
                            "path": "/OtherRoot01/ChildUnit01",
                            "parent": {"id": "OtherRoot01"},
                        },
                    ],
                )

            def metadata_objects_by_ids(self, **kwargs):
                wanted = set(kwargs["object_ids"])
                return [item for item in self.target_units if item["id"] in wanted]

            def data_set_details(self, data_set_id):
                return {
                    "id": data_set_id,
                    "code": "CCH_WEEKLY",
                    "name": "Climate Weekly",
                    "shortName": "Climate Weekly",
                    "periodType": "Weekly",
                    "organisationUnits": [{"id": "ChildUnit01"}],
                }

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
            )
            with self.assertRaisesRegex(PipelineError, "parent: source=RootUnit001"):
                reconcile_target_organisation_units(
                    scenario,
                    Source("https://x.example.org"),
                    Target(),
                    job_id="weekly_climate",
                    apply=False,
                )

    def test_dynamic_conflict_policy_quarantines_only_the_affected_subtree(self) -> None:
        root = {
            "id": "RootUnit001",
            "code": "ROOT",
            "name": "Country",
            "path": "/RootUnit001",
            "level": 1,
        }
        blocked_source = {
            "id": "BlockedOu01",
            "code": "BLOCKED",
            "name": "Source facility name",
            "path": "/RootUnit001/BlockedOu01",
            "level": 2,
            "parent": {"id": "RootUnit001"},
        }
        included = {
            "id": "IncludedOu1",
            "code": "INCLUDED",
            "name": "Included facility",
            "path": "/RootUnit001/IncludedOu1",
            "level": 2,
            "parent": {"id": "RootUnit001"},
        }

        class Source(FakeTransferClient):
            def organisation_units_for_data_set(self, data_set_id):
                return [blocked_source, included]

            def metadata_objects_by_ids(self, **kwargs):
                wanted = set(kwargs["object_ids"])
                return [
                    item
                    for item in (root, blocked_source, included)
                    if item["id"] in wanted
                ]

        class Target(FakeTransferClient):
            def __init__(self):
                target_blocked = {
                    **blocked_source,
                    "name": "Different target facility",
                }
                super().__init__(
                    "https://y.example.org",
                    target_has_dataset=True,
                    target_units=[root, target_blocked, included],
                )

            def metadata_objects_by_ids(self, **kwargs):
                wanted = set(kwargs["object_ids"])
                return [
                    item for item in self.target_units if item["id"] in wanted
                ]

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
                organisation_unit_run_before_transfer=True,
                organisation_unit_conflict_policy="exclude",
            )
            source = Source("https://x.example.org")
            target = Target()

            result = reconcile_target_organisation_units(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
            )

            self.assertEqual(result["status"], "already_aligned_with_quarantine")
            self.assertTrue(result["detectedOrganisationUnitConflicts"])
            self.assertEqual(result["blocked"], [])
            self.assertEqual(result["organisationUnitConflictPolicy"], "exclude")
            self.assertEqual(
                result["detectedOrganisationUnitConflicts"][0]["uid"],
                "BlockedOu01",
            )
            self.assertEqual(result["excludedSourceOrganisationUnitCount"], 1)
            self.assertEqual(
                result["excludedSourceOrganisationUnits"][0]["id"],
                "BlockedOu01",
            )
            self.assertTrue(
                Path(result["organisationUnitQuarantineArtifact"]).exists()
            )

            review = review_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
            )
            self.assertEqual(review["sourceOrganisationUnitCount"], 1)
            self.assertEqual(review["excludedSourceOrganisationUnitCount"], 1)
            self.assertEqual(
                review["detectedOrganisationUnitConflicts"][0]["uid"],
                "BlockedOu01",
            )

            class OnlyBlockedSource(Source):
                def organisation_units_for_data_set(self, data_set_id):
                    return [blocked_source]

            with self.assertRaisesRegex(
                PipelineError,
                "would exclude the complete source selection",
            ):
                reconcile_target_organisation_units(
                    scenario,
                    OnlyBlockedSource("https://x.example.org"),
                    target,
                    job_id="weekly_climate",
                    apply=False,
                )

    def test_reconcile_org_units_warns_on_allowed_code_difference(self) -> None:
        source_root = {
            "id": "RootUnit001",
            "code": "100000",
            "name": "Country",
            "shortName": "Country",
            "openingDate": "2000-01-01",
            "path": "/RootUnit001",
            "level": 1,
        }
        target_root = {
            **source_root,
            "code": "RDC-100000",
        }

        class HierarchyClient(FakeTransferClient):
            def __init__(self, base_url, unit, *, target=False):
                super().__init__(
                    base_url,
                    target_has_dataset=target,
                    target_units=[unit],
                )
                self.unit = unit

            def organisation_units_for_data_set(self, data_set_id):
                return [self.unit]

            def metadata_objects_by_ids(self, **kwargs):
                return (
                    [self.unit]
                    if self.unit["id"] in set(kwargs["object_ids"])
                    else []
                )

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
                organisation_unit_require_same_code=False,
            )
            result = reconcile_target_organisation_units(
                scenario,
                HierarchyClient("https://x.example.org", source_root),
                HierarchyClient(
                    "https://y.example.org",
                    target_root,
                    target=True,
                ),
                job_id="weekly_climate",
                apply=False,
            )

            self.assertEqual(result["status"], "already_aligned")
            self.assertEqual(result["blocked"], [])
            self.assertEqual(len(result["warnings"]), 1)
            self.assertIn("code", result["warnings"][0])

    def test_membership_update_preserves_existing_dataset_element_category_combo(self) -> None:
        class Target:
            def data_set_details(self, data_set_id):
                return {
                    "id": data_set_id,
                    "code": "CCH_WEEKLY",
                    "name": "Climate Weekly on Y",
                    "shortName": "Climate Weekly Y",
                    "periodType": "Weekly",
                    "categoryCombo": {"id": "CatCombo001"},
                    "organisationUnits": [
                        {"id": "OrgUnit00001"},
                        {"id": "OrgUnit00002"},
                    ],
                    "dataSetElements": [
                        {
                            "dataSet": {"id": data_set_id},
                            "dataElement": {"id": "DataEl00001"},
                            "categoryCombo": {"id": "OverrideCC1"},
                        }
                    ]
                }

        package = _data_set_membership_package(
            Target(),
            data_set_id="DataSet0001",
            data_element_ids={"DataEl00001", "DataEl00002"},
        )

        elements = package["dataSets"][0]["dataSetElements"]
        data_set = package["dataSets"][0]
        by_id = {item["dataElement"]["id"]: item for item in elements}
        self.assertEqual(data_set["name"], "Climate Weekly on Y")
        self.assertEqual(data_set["shortName"], "Climate Weekly Y")
        self.assertEqual(data_set["periodType"], "Weekly")
        self.assertEqual(data_set["code"], "CCH_WEEKLY")
        self.assertEqual(data_set["categoryCombo"], {"id": "CatCombo001"})
        self.assertEqual(
            data_set["organisationUnits"],
            [{"id": "OrgUnit00001"}, {"id": "OrgUnit00002"}],
        )
        self.assertEqual(
            by_id["DataEl00001"]["categoryCombo"]["id"],
            "OverrideCC1",
        )
        self.assertNotIn("categoryCombo", by_id["DataEl00002"])

    def test_membership_update_blocks_incomplete_target_dataset_identity(self) -> None:
        class Target:
            def data_set_details(self, data_set_id):
                return {
                    "id": data_set_id,
                    "name": "Climate Weekly",
                    "dataSetElements": [],
                    "organisationUnits": [],
                }

        with self.assertRaisesRegex(
            PipelineError,
            "required target properties are missing: shortName, periodType",
        ):
            _data_set_membership_package(
                Target(),
                data_set_id="DataSet0001",
                data_element_ids={"DataEl00001"},
            )

    def test_membership_update_blocks_when_org_unit_assignments_are_unreadable(self) -> None:
        class Target:
            def data_set_details(self, data_set_id):
                return {
                    "id": data_set_id,
                    "name": "Climate Weekly",
                    "shortName": "Climate Weekly",
                    "periodType": "Weekly",
                    "dataSetElements": [],
                }

        with self.assertRaisesRegex(
            PipelineError,
            "organisationUnits collection was not returned",
        ):
            _data_set_membership_package(
                Target(),
                data_set_id="DataSet0001",
                data_element_ids={"DataEl00001"},
            )

    def test_reconcile_plans_attachment_of_compatible_unlinked_data_element(self) -> None:
        class PartialTarget(FakeTransferClient):
            def data_elements_for_data_set(self, *, data_set_id, page_size):
                return self.elements[:1]

            def data_elements(self, *, page_size):
                return self.elements

        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = FakeTransferClient("https://x.example.org")
            target = PartialTarget(
                "https://y.example.org",
                target_has_dataset=True,
            )

            result = reconcile_target_metadata(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
            )

            self.assertTrue(result["requiresChanges"])
            self.assertEqual(result["createDataElements"], [])
            self.assertEqual(result["attachDataElements"], ["DataEl00002"])
            self.assertEqual(target.metadata_calls, [False])
            self.assertTrue(Path(result["targetSnapshotArtifact"]).exists())

    def test_reconcile_blocks_same_code_with_different_uid(self) -> None:
        class CollisionTarget(FakeTransferClient):
            def data_elements_for_data_set(self, *, data_set_id, page_size):
                return self.elements[:1]

            def data_elements(self, *, page_size):
                return [
                    self.elements[0],
                    {
                        "id": "OtherDE0001",
                        "code": "CCH_TEMP",
                        "name": "Another object",
                    },
                ]

        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            with self.assertRaisesRegex(PipelineError, "code CCH_TEMP"):
                reconcile_target_metadata(
                    scenario,
                    FakeTransferClient("https://x.example.org"),
                    CollisionTarget(
                        "https://y.example.org",
                        target_has_dataset=True,
                    ),
                    job_id="weekly_climate",
                    apply=False,
                )

    def test_reconcile_warns_when_uid_matches_but_target_codes_are_absent(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = FakeTransferClient("https://x.example.org")
            target = FakeTransferClient(
                "https://y.example.org",
                target_has_dataset=True,
            )
            target.data_set = DataSetRef(
                id="DataSet0001",
                code=None,
                name="Climate Weekly",
                period_type="Weekly",
            )
            for item in target.elements:
                item["code"] = None
            target.global_target_elements = target.elements

            result = reconcile_target_metadata(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
            )

            self.assertEqual(result["blocked"], [])
            self.assertEqual(len(result["identityWarnings"]), 3)
            self.assertIn("UID identity remains authoritative", result["identityWarnings"][0])

    def test_reconcile_validates_creation_of_missing_data_element(self) -> None:
        class MissingTarget(FakeTransferClient):
            def data_elements_for_data_set(self, *, data_set_id, page_size):
                return self.elements[:1]

            def data_elements(self, *, page_size):
                return self.elements[:1]

        with TemporaryDirectory() as tmp:
            result = reconcile_target_metadata(
                _scenario(Path(tmp), chunk_size=5000),
                FakeTransferClient("https://x.example.org"),
                MissingTarget(
                    "https://y.example.org",
                    target_has_dataset=True,
                ),
                job_id="weekly_climate",
                apply=False,
            )

            self.assertEqual(result["createDataElements"], ["DataEl00002"])
            self.assertTrue(result["attachmentValidationPendingCreateCommit"])
            self.assertTrue(result["validated"])

    def test_versions_historical_data_element_without_losing_old_category_semantics(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                period_type="month",
                default_start_period="2026-01",
                default_end_period="2026-01",
            )
            source = _HistoricalSource()
            target = _HistoricalTarget()
            canonical = {
                "id": "DataEl00001",
                "code": "HOSP_ADM",
                "name": "Admissions",
                "valueType": "INTEGER",
                "domainType": "AGGREGATE",
                "aggregationType": "SUM",
                "categoryCombo": {"id": "CurrentCC01"},
            }
            context = TransferContext(
                job=scenario.get_job("weekly_climate"),
                source_data_set=DataSetRef(
                    id="DataSet0001",
                    code="CCH_WEEKLY",
                    name="Hospital",
                    period_type="Monthly",
                ),
                target_data_set=DataSetRef(
                    id="DataSet0001",
                    code="CCH_WEEKLY",
                    name="Hospital",
                    period_type="Monthly",
                ),
                source_data_elements=(canonical,),
                target_data_elements=(canonical,),
                source_organisation_units=(),
                target_organisation_units=(),
                start="2026-01-01",
                end="2026-01-31",
                metadata_bootstrap_required=False,
            )
            target.linked = [canonical]
            values = [
                {
                    "dataElement": "DataEl00001",
                    "period": "202601",
                    "orgUnit": "OrgUnit00001",
                    "categoryOptionCombo": "HistoricC01",
                    "value": "7",
                }
            ]

            rewritten, report = _version_unmappable_historical_values(
                source,
                target,
                scenario,
                context,
                values,
                apply=True,
            )

            historical_id = report["mappings"][0]["targetDataElement"]
            self.assertNotEqual(historical_id, "DataEl00001")
            self.assertEqual(rewritten[0]["dataElement"], historical_id)
            self.assertEqual(rewritten[0]["categoryOptionCombo"], "HistoricC01")
            self.assertEqual(report["rewrittenValueCount"], 1)
            self.assertIn(historical_id, {item["id"] for item in target.linked})

    def test_clones_historical_category_graph_when_target_uid_is_incompatible(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                period_type="month",
                default_start_period="2026-01",
                default_end_period="2026-01",
                historical_uid_collision_strategy="clone",
            )
            source = _HistoricalSource()
            target = _HistoricalTarget()
            canonical = {
                "id": "DataEl00001",
                "code": "HOSP_ADM",
                "name": "Admissions",
                "valueType": "INTEGER",
                "domainType": "AGGREGATE",
                "aggregationType": "SUM",
                "categoryCombo": {"id": "CurrentCC01"},
            }
            context = TransferContext(
                job=scenario.get_job("weekly_climate"),
                source_data_set=DataSetRef(
                    id="DataSet0001",
                    code="CCH_WEEKLY",
                    name="Hospital",
                    period_type="Monthly",
                ),
                target_data_set=DataSetRef(
                    id="DataSet0001",
                    code="CCH_WEEKLY",
                    name="Hospital",
                    period_type="Monthly",
                ),
                source_data_elements=(canonical,),
                target_data_elements=(canonical,),
                source_organisation_units=(),
                target_organisation_units=(),
                start="2026-01-01",
                end="2026-01-31",
                metadata_bootstrap_required=False,
            )
            target.linked = [canonical]
            target.objects["categoryCombos"]["HistoricCC1"] = {
                "id": "HistoricCC1",
                "code": "HIST_CC",
                "name": "Old speciality",
                "dataDimensionType": "DISAGGREGATION",
                "categories": [],
            }
            values = [
                {
                    "dataElement": "DataEl00001",
                    "period": "202601",
                    "orgUnit": "OrgUnit00001",
                    "categoryOptionCombo": "HistoricC01",
                    "value": "7",
                }
            ]

            blocking_scenario = _scenario(
                Path(tmp) / "blocking",
                chunk_size=5000,
                period_type="month",
                default_start_period="2026-01",
                default_end_period="2026-01",
            )
            with self.assertRaisesRegex(
                PipelineError,
                "categoryCombos HistoricCC1 categories differ",
            ):
                _version_unmappable_historical_values(
                    source,
                    target,
                    blocking_scenario,
                    context,
                    [dict(values[0])],
                    apply=True,
                )

            rewritten, report = _version_unmappable_historical_values(
                source,
                target,
                scenario,
                context,
                values,
                apply=True,
            )

            clone = report["clonedHistoricalGraphs"][0]
            cloned_parent = clone["targetCategoryCombo"]
            cloned_combination = clone["uidMappings"]["categoryOptionCombos"][
                "HistoricC01"
            ]
            historical_element = report["mappings"][0]["targetDataElement"]
            self.assertNotEqual(cloned_parent, "HistoricCC1")
            self.assertNotEqual(cloned_combination, "HistoricC01")
            self.assertEqual(rewritten[0]["dataElement"], historical_element)
            self.assertEqual(
                rewritten[0]["categoryOptionCombo"], cloned_combination
            )
            self.assertEqual(
                target.objects["categoryCombos"]["HistoricCC1"]["categories"],
                [],
            )
            self.assertEqual(
                target.objects["dataElements"][historical_element]["categoryCombo"],
                {"id": cloned_parent},
            )
            self.assertEqual(
                target.objects["categoryOptionCombos"][cloned_combination][
                    "categoryCombo"
                ],
                {"id": cloned_parent},
            )
            self.assertNotIn("HistoricCat", target.objects["categories"])
            self.assertNotIn("HistoricOp1", target.objects["categoryOptions"])

    def test_remaps_historical_category_option_combo_by_exact_options(self) -> None:
        client = _CategoryOptionComboClient(
            current=[
                {
                    "id": "CurrentC001",
                    "categoryCombo": {"id": "CurrentCC01"},
                    "categoryOptions": [{"id": "CategoryO01"}],
                }
            ],
            historical=[
                {
                    "id": "HistoricC01",
                    "name": "Medicine",
                    "categoryCombo": {"id": "HistoricCC1"},
                    "categoryOptions": [
                        {"id": "CategoryO01", "name": "Medicine"}
                    ],
                }
            ],
        )
        context = _category_context()
        source_values = [
            {
                "dataElement": "DataEl00001",
                "period": "202601",
                "orgUnit": "OrgUnit00001",
                "categoryOptionCombo": "HistoricC01",
                "value": "12",
            }
        ]

        normalised, report = _normalise_source_category_option_combos(
            client,
            context,
            source_values,
        )

        self.assertEqual(
            normalised[0]["categoryOptionCombo"],
            "CurrentC001",
        )
        self.assertIs(normalised, source_values)
        self.assertEqual(report["remappedValueCount"], 1)
        self.assertEqual(report["historicalCombinationCount"], 1)

    def test_blocks_historical_category_option_combo_without_exact_equivalent(self) -> None:
        client = _CategoryOptionComboClient(
            current=[
                {
                    "id": "CurrentC001",
                    "categoryCombo": {"id": "CurrentCC01"},
                    "categoryOptions": [{"id": "CategoryO01"}],
                }
            ],
            historical=[
                {
                    "id": "HistoricC01",
                    "name": "Speciality",
                    "categoryCombo": {"id": "HistoricCC1"},
                    "categoryOptions": [
                        {"id": "RemovedOp01", "name": "Speciality"}
                    ],
                }
            ],
        )

        with self.assertRaisesRegex(PipelineError, "cannot be mapped losslessly"):
            _normalise_source_category_option_combos(
                client,
                _category_context(),
                [
                    {
                        "dataElement": "DataEl00001",
                        "period": "202601",
                        "orgUnit": "OrgUnit00001",
                        "categoryOptionCombo": "HistoricC01",
                        "value": "12",
                    }
                ],
            )

    def test_blocks_historical_remap_that_would_create_duplicate_value_key(self) -> None:
        client = _CategoryOptionComboClient(
            current=[
                {
                    "id": "CurrentC001",
                    "categoryCombo": {"id": "CurrentCC01"},
                    "categoryOptions": [{"id": "CategoryO01"}],
                }
            ],
            historical=[
                {
                    "id": "HistoricC01",
                    "categoryCombo": {"id": "HistoricCC1"},
                    "categoryOptions": [{"id": "CategoryO01"}],
                }
            ],
        )
        values = [
            {
                "dataElement": "DataEl00001",
                "period": "202601",
                "orgUnit": "OrgUnit00001",
                "categoryOptionCombo": combo,
                "attributeOptionCombo": "AttrOpt0001",
                "value": value,
            }
            for combo, value in (
                ("HistoricC01", "12"),
                ("CurrentC001", "14"),
            )
        ]

        with self.assertRaisesRegex(PipelineError, "duplicate data value keys"):
            _normalise_source_category_option_combos(
                client,
                _category_context(),
                values,
            )

        self.assertEqual(values[0]["categoryOptionCombo"], "HistoricC01")

    def test_target_category_option_combo_preflight_explains_e8024(self) -> None:
        target = _CategoryOptionComboClient(current=[], historical=[])

        with self.assertRaisesRegex(PipelineError, "E8024"):
            _validate_target_category_option_combos(
                target,
                _category_context(target=True),
                [
                    {
                        "dataElement": "DataEl00001",
                        "period": "202601",
                        "orgUnit": "OrgUnit00001",
                        "categoryOptionCombo": "MissingC001",
                        "value": "12",
                    }
                ],
            )

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
                (
                    scenario.job_state_dir("weekly_climate")
                    / "cache"
                    / "source"
                    / "weekly_climate.json"
                ).exists()
            )

    def test_review_excludes_an_org_unit_subtree_and_reports_affected_units(self) -> None:
        class SourceWithExcludedSubtree(FakeTransferClient):
            def organisation_units_for_data_set(self, data_set_id):
                return [
                    {
                        "id": "OrgUnit00001",
                        "code": "OU-1",
                        "name": "Included facility",
                        "path": "/RootUnit001/IncludedPar/OrgUnit00001",
                        "level": 3,
                    },
                    {
                        "id": "OrgUnit00002",
                        "code": "OU-2",
                        "name": "Quarantined facility",
                        "path": "/RootUnit001/BlockedOu01/OrgUnit00002",
                        "level": 3,
                    },
                ]

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_exclusions=(
                    ("BlockedOu01", "Temporary hierarchy review"),
                ),
            )
            source = SourceWithExcludedSubtree("https://x.example.org")
            target = FakeTransferClient(
                "https://y.example.org",
                target_has_dataset=True,
                target_units=[
                    {
                        "id": "OrgUnit00001",
                        "code": "OU-1",
                        "name": "Included facility",
                    }
                ],
            )

            review = review_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
            )

            self.assertEqual(review["sourceOrganisationUnitCount"], 1)
            self.assertEqual(review["targetMatchedOrganisationUnitCount"], 1)
            self.assertEqual(review["excludedSourceOrganisationUnitCount"], 1)
            self.assertEqual(
                review["excludedSourceOrganisationUnits"][0]["id"],
                "OrgUnit00002",
            )
            self.assertEqual(
                review["organisationUnitExclusions"],
                [
                    {
                        "uid": "BlockedOu01",
                        "reason": "Temporary hierarchy review",
                    }
                ],
            )

            report = run_transfer(
                scenario,
                source,
                target,
                job_id="weekly_climate",
                apply=False,
            )

            self.assertTrue(source.export_calls)
            self.assertTrue(
                all(
                    call["organisation_unit_ids"] == ["OrgUnit00001"]
                    for call in source.export_calls
                )
            )
            self.assertEqual(report["excludedSourceOrganisationUnitCount"], 1)

    def test_review_rejects_an_exclusion_outside_the_source_selection(self) -> None:
        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_exclusions=(
                    ("UnknownOu01", "Temporary hierarchy review"),
                ),
            )

            with self.assertRaisesRegex(
                PipelineError,
                "neither selected source units nor ancestors",
            ):
                review_transfer(
                    scenario,
                    FakeTransferClient("https://x.example.org"),
                    FakeTransferClient("https://y.example.org"),
                    job_id="weekly_climate",
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

    def test_review_reports_a_safe_missing_org_unit_plan_instead_of_blocking(self) -> None:
        root = {
            "id": "RootUnit001",
            "code": "ROOT",
            "name": "Country",
            "shortName": "Country",
            "openingDate": "2000-01-01",
            "path": "/RootUnit001",
            "level": 1,
        }
        child = {
            "id": "OrgUnit00001",
            "code": "OU-1",
            "name": "Zone 1",
            "shortName": "Zone 1",
            "openingDate": "2000-01-01",
            "parent": {"id": "RootUnit001"},
            "path": "/RootUnit001/OrgUnit00001",
            "level": 2,
        }

        class Source(FakeTransferClient):
            def organisation_units_for_data_set(self, data_set_id):
                return [child]

            def metadata_objects_by_ids(self, **kwargs):
                wanted = set(kwargs["object_ids"])
                return [item for item in (root, child) if item["id"] in wanted]

        class Target(FakeTransferClient):
            def __init__(self):
                super().__init__("https://y.example.org", target_units=[root])

            def metadata_objects_by_ids(self, **kwargs):
                if kwargs["resource"] != "organisationUnits":
                    return []
                wanted = set(kwargs["object_ids"])
                return [item for item in self.target_units if item["id"] in wanted]

        with TemporaryDirectory() as tmp:
            scenario = _scenario(
                Path(tmp),
                chunk_size=5000,
                organisation_unit_synchronization=True,
                organisation_unit_run_before_transfer=True,
                organisation_unit_conflict_policy="exclude",
            )
            result = review_transfer(
                scenario,
                Source("https://x.example.org"),
                Target(),
                job_id="weekly_climate",
            )

            self.assertEqual(result["status"], "organisation_unit_changes_required")
            self.assertEqual(
                result["organisationUnits"]["organisationUnitsToCreate"],
                ["OrgUnit00001"],
            )
            self.assertIn("--apply", result["nextStep"])

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

    def test_transfer_blocks_before_import_when_dataset_is_not_assigned_to_value_ou(
        self,
    ) -> None:
        class UnassignedTarget(FakeTransferClient):
            def data_set_details(self, data_set_id):
                details = super().data_set_details(data_set_id)
                details["organisationUnits"] = []
                return details

        with TemporaryDirectory() as tmp:
            scenario = _scenario(Path(tmp), chunk_size=5000)
            source = FakeTransferClient("https://x.example.org")
            target = UnassignedTarget(
                "https://y.example.org",
                target_has_dataset=True,
            )

            with self.assertRaisesRegex(PipelineError, "E8022"):
                run_transfer(
                    scenario,
                    source,
                    target,
                    job_id="weekly_climate",
                    apply=False,
                )

            self.assertEqual(target.data_import_calls, [])

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
                        "optionSet": {"id": "OptionSet001"},
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
                    "options": [
                        {
                            "id": "SetOption001",
                            "code": "OBS",
                            "name": "Observation",
                            "sortOrder": 1,
                        }
                    ],
                    "optionSets": [
                        {
                            "id": "OptionSet001",
                            "code": "OBS_SET",
                            "name": "Observations",
                            "valueType": "TEXT",
                            "options": [{"id": "SetOption001"}],
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
            self.assertEqual(report["repair"]["optionSets"]["referenced"], 1)
            self.assertEqual(report["repair"]["dataElementsToUpdate"], 1)
            self.assertTrue(
                report["assignments"]["validationPendingStructureCommit"]
            )
            self.assertEqual(target.metadata_calls, [False])
            assignment = json.loads(
                Path(report["assignments"]["artifact"]).read_text(encoding="utf-8")
            )
            self.assertEqual(assignment["optionSets"][0]["id"], "OptionSet001")
            self.assertEqual(assignment["options"][0]["id"], "SetOption001")

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

            resumed_target = RepairTarget()
            resumed_target.objects["categoryCombos"][0]["categories"] = [
                {"id": "Category001"}
            ]
            resumed_report = repair_target_metadata(
                scenario,
                RepairSource(),
                resumed_target,
                job_id="weekly_climate",
                apply=False,
            )
            self.assertFalse(resumed_report["structure"]["required"])
            self.assertTrue(resumed_report["assignments"]["validated"])
            self.assertFalse(
                resumed_report["assignments"]["validationPendingStructureCommit"]
            )
            self.assertEqual(resumed_target.metadata_calls, [False])

            incompatible_option_set_target = RepairTarget()
            incompatible_option_set_target.objects["categoryCombos"][0]["categories"] = [
                {"id": "Category001"}
            ]
            incompatible_option_set_target.objects["optionSets"] = [
                {
                    "id": "OptionSet001",
                    "code": "OBS_SET",
                    "name": "Observations",
                    "valueType": "NUMBER",
                    "options": [{"id": "SetOption001"}],
                }
            ]
            incompatible_option_set_target.objects["options"] = [
                {
                    "id": "SetOption001",
                    "code": "OBS",
                    "name": "Observation",
                    "sortOrder": 1,
                }
            ]
            with self.assertRaisesRegex(
                PipelineError,
                "incompatible OptionSet dependencies",
            ):
                repair_target_metadata(
                    scenario,
                    RepairSource(),
                    incompatible_option_set_target,
                    job_id="weekly_climate",
                    apply=False,
                )

            class FailingAssignmentTarget(RepairTarget):
                def __init__(self):
                    super().__init__()
                    self.objects["categoryCombos"][0]["categories"] = [
                        {"id": "Category001"}
                    ]

                def import_metadata(self, payload, *, commit, **kwargs):
                    if payload.get("dataElements"):
                        raise HttpRequestError(
                            "POST",
                            "https://y.example.org/api/metadata",
                            409,
                            '{"response":{"errorCode":"E5002"}}',
                        )
                    return super().import_metadata(payload, commit=commit, **kwargs)

            with self.assertRaises(HttpRequestError):
                repair_target_metadata(
                    scenario,
                    RepairSource(),
                    FailingAssignmentTarget(),
                    job_id="weekly_climate",
                    apply=False,
                )
            progress_path = (
                scenario.job_state_dir("weekly_climate")
                / "artifacts"
                / "metadata"
                / "weekly_climate.repair.progress.json"
            )
            progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
            self.assertEqual(progress_payload["lastStage"], "assignment_validation")
            self.assertEqual(progress_payload["status"], "failed")
            self.assertIn("E5002", progress_payload["stages"][-1]["error"])

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
    historical_uid_collision_strategy: str = "block",
    organisation_unit_synchronization: bool = False,
    organisation_unit_run_before_transfer: bool = False,
    organisation_unit_conflict_policy: str = "block",
    organisation_unit_require_same_code: bool = True,
    organisation_unit_exclusions: tuple[tuple[str, str], ...] = (),
    adaptive_minimum_chunk_size: int | None = None,
    adaptive_retry_before_split: int | None = None,
):
    path = root / "config" / "scenarios" / "x-to-y.yaml"
    path.parent.mkdir(parents=True)
    exclusion_config = ""
    if organisation_unit_exclusions:
        lines = ["    organisation_units:", "      exclude:"]
        for uid, reason in organisation_unit_exclusions:
            lines.extend(
                (
                    f"        - uid: {uid}",
                    f'          reason: "{reason}"',
                )
            )
        exclusion_config = "\n" + "\n".join(lines)
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
    synchronization:
      historical_data_elements:
        uid_collision_strategy: {historical_uid_collision_strategy}
  organisation_units:
    discovery: linked_to_dataset
    mapping_strategy: {organisation_unit_mapping}
    synchronization:
      enabled: {str(organisation_unit_synchronization).lower()}
      run_before_transfer: {str(organisation_unit_run_before_transfer).lower()}
      conflict_policy: {organisation_unit_conflict_policy}
      require_same_code: {str(organisation_unit_require_same_code).lower()}
mapping:
  data_elements: uid
  organisation_units: {organisation_unit_mapping}
  category_option_combos: uid
  attribute_option_combos: uid
  data_sets: uid
transfer:
  import_strategy: CREATE_AND_UPDATE
  chunk_size: {chunk_size}
  adaptive_chunking:
    minimum_chunk_size: {adaptive_minimum_chunk_size or min(500, chunk_size)}
    retry_before_split: {
        adaptive_retry_before_split if adaptive_retry_before_split is not None else 1
    }
    backoff_seconds: 0.001
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
    default_end_period: {default_end_period}{exclusion_config}
""",
        encoding="utf-8",
    )
    return load_transfer_scenario(path)


class _CategoryOptionComboClient:
    def __init__(self, *, current: list[dict], historical: list[dict]) -> None:
        self.current = current
        self.historical = historical

    def category_option_combos_for_category_combos(
        self, category_combo_ids: list[str]
    ) -> list[dict]:
        return [
            item
            for item in self.current
            if (item.get("categoryCombo") or {}).get("id") in category_combo_ids
        ]

    def metadata_objects_by_ids(
        self,
        *,
        resource: str,
        root_key: str,
        object_ids: list[str],
        fields: str,
    ) -> list[dict]:
        return [item for item in self.historical if item.get("id") in object_ids]


class _HistoricalSource:
    def category_option_combos_for_category_combos(
        self, category_combo_ids: list[str]
    ) -> list[dict]:
        items = {
            "CurrentCC01": [
                {
                    "id": "CurrentC001",
                    "name": "Current",
                    "categoryCombo": {"id": "CurrentCC01"},
                    "categoryOptions": [{"id": "CurrentOp01"}],
                }
            ],
            "HistoricCC1": [
                {
                    "id": "HistoricC01",
                    "name": "Speciality",
                    "categoryCombo": {"id": "HistoricCC1"},
                    "categoryOptions": [{"id": "HistoricOp1"}],
                }
            ],
        }
        return [item for uid in category_combo_ids for item in items.get(uid, [])]

    def metadata_objects_by_ids(
        self,
        *,
        resource: str,
        root_key: str,
        object_ids: list[str],
        fields: str,
    ) -> list[dict]:
        objects = {
            "categoryOptionCombos": self.category_option_combos_for_category_combos(
                ["HistoricCC1"]
            ),
            "categoryCombos": [
                {
                    "id": "HistoricCC1",
                    "code": "HIST_CC",
                    "name": "Old speciality",
                    "dataDimensionType": "DISAGGREGATION",
                    "categories": [{"id": "HistoricCat"}],
                }
            ],
            "categories": [
                {
                    "id": "HistoricCat",
                    "code": "HIST_CAT",
                    "name": "Old speciality",
                    "shortName": "Old speciality",
                    "dataDimensionType": "DISAGGREGATION",
                    "categoryOptions": [{"id": "HistoricOp1"}],
                }
            ],
            "categoryOptions": [
                {
                    "id": "HistoricOp1",
                    "code": "HIST_OPT",
                    "name": "Speciality",
                    "shortName": "Speciality",
                }
            ],
            "dataElements": [
                {
                    "id": "DataEl00001",
                    "code": "HOSP_ADM",
                    "name": "Admissions",
                    "shortName": "Admissions",
                    "valueType": "INTEGER",
                    "domainType": "AGGREGATE",
                    "aggregationType": "SUM",
                }
            ],
        }
        return [
            item
            for item in objects.get(resource, [])
            if item.get("id") in object_ids
        ]


class _HistoricalTarget:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, dict]] = {
            key: {}
            for key in (
                "categoryOptions",
                "categories",
                "categoryCombos",
                "categoryOptionCombos",
                "dataElements",
            )
        }
        self.linked: list[dict] = []

    def data_set_details(self, data_set_id: str) -> dict:
        return {
            "id": data_set_id,
            "code": "CCH_WEEKLY",
            "name": "Hospital service",
            "shortName": "Hospital service",
            "periodType": "Monthly",
            "categoryCombo": {"id": "CatCombo001"},
            "organisationUnits": [{"id": "OrgUnit00001"}],
            "dataSetElements": [
                {
                    "dataSet": {"id": data_set_id},
                    "dataElement": {"id": item["id"]},
                }
                for item in self.linked
            ],
        }

    def metadata_objects_by_ids(
        self,
        *,
        resource: str,
        root_key: str,
        object_ids: list[str],
        fields: str,
    ) -> list[dict]:
        return [
            self.objects.get(resource, {})[uid]
            for uid in object_ids
            if uid in self.objects.get(resource, {})
        ]

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
        if commit:
            for collection, items in payload.items():
                if collection == "dataSets":
                    memberships = items[0].get("dataSetElements", [])
                    linked_ids = {
                        item["dataElement"]["id"] for item in memberships
                    }
                    by_id = {item["id"]: item for item in self.linked}
                    by_id.update(self.objects["dataElements"])
                    self.linked = [by_id[uid] for uid in linked_ids]
                    continue
                if collection in self.objects:
                    for item in items:
                        self.objects[collection][item["id"]] = item
        return {"status": "OK"}

    def data_elements_for_data_set(self, *, data_set_id: str, page_size: int):
        return self.linked


def _category_context(*, target: bool = False) -> SimpleNamespace:
    element = {
        "id": "DataEl00001",
        "categoryCombo": {"id": "CurrentCC01"},
    }
    return SimpleNamespace(
        job=SimpleNamespace(
            id_schemes=SimpleNamespace(
                data_elements="uid",
                category_option_combos="uid",
            )
        ),
        source_data_elements=(element,),
        target_data_elements=(element,),
        target_data_set=(
            DataSetRef(
                id="DataSet0001",
                code="DATA_SET",
                name="Data Set",
                period_type="Monthly",
            )
            if target
            else None
        ),
    )


if __name__ == "__main__":
    unittest.main()
