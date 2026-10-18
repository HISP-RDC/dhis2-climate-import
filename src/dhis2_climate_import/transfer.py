from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
import hashlib
import json
from pathlib import Path
import time
from typing import Any

from .dhis2 import DHIS2Client, DataSetRef
from .errors import PipelineError, TransientPipelineError
from .http import HttpRequestError
from .journal import TransferFingerprint, TransferRunJournal
from .locks import exclusive_lock
from .periods import (
    date_window_period_bounds,
    period_date_windows,
    period_range_bounds,
    periods_between,
    relative_year_window,
)
from .reports import extract_conflicts, import_counts, run_id, write_report
from .transfer_config import TransferJobConfig, TransferScenario


@dataclass(frozen=True)
class TransferContext:
    job: TransferJobConfig
    source_data_set: DataSetRef
    target_data_set: DataSetRef | None
    source_data_elements: tuple[dict[str, Any], ...]
    target_data_elements: tuple[dict[str, Any], ...]
    source_organisation_units: tuple[dict[str, Any], ...]
    target_organisation_units: tuple[dict[str, Any], ...]
    start: str
    end: str
    metadata_bootstrap_required: bool
    category_combo_repairs: tuple[dict[str, Any], ...] = ()
    excluded_source_organisation_units: tuple[dict[str, Any], ...] = ()
    detected_organisation_unit_conflicts: tuple[dict[str, Any], ...] = ()


ProgressCallback = Callable[[str, dict[str, Any]], None]


def review_transfer(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    start: str | None = None,
    end: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    job = scenario.get_job(job_id)
    organisation_unit_report: dict[str, Any] | None = None
    if job.organisation_units.synchronization.conflict_policy == "exclude":
        organisation_unit_report = reconcile_target_organisation_units(
            scenario,
            source,
            target,
            job_id=job_id,
            apply=False,
        )
        if organisation_unit_report.get("requiresChanges"):
            resolved_start, resolved_end = _resolve_window(
                job,
                start=start,
                end=end,
                start_period=start_period,
                end_period=end_period,
                years_back=years_back,
                as_of=as_of,
            )
            if not resolved_start or not resolved_end:
                raise PipelineError(
                    f"Transfer job '{job.id}' needs a date window. Configure defaults or "
                    "pass --start/--end, --start-period/--end-period, or --years-back."
                )
            source_data_set = source.resolve_data_set(job.source_data_set)
            source_elements = source.data_elements_for_data_set(
                data_set_id=source_data_set.id,
                page_size=500,
            )
            return {
                "scenario": scenario.id,
                "job": job.id,
                "status": "organisation_unit_changes_required",
                "start": resolved_start,
                "end": resolved_end,
                "sourceInstance": {
                    "profile": scenario.source_instance,
                    "baseUrl": source.base_url,
                },
                "targetInstance": {
                    "profile": scenario.target_instance,
                    "baseUrl": target.base_url,
                },
                "sourceDataSet": asdict(source_data_set),
                "targetDataSet": organisation_unit_report.get("targetDataSet"),
                "sourceDataElementCount": len(source_elements),
                "sourceOrganisationUnitCount": organisation_unit_report.get(
                    "directSourceOrganisationUnits", 0
                ),
                "metadataBootstrapRequired": (
                    organisation_unit_report.get("targetDataSet") is None
                ),
                "organisationUnits": organisation_unit_report,
                "nextStep": (
                    "Run the transfer with --apply; the validated organisation-unit "
                    "plan will be committed before metadata and data values."
                ),
            }
    context = resolve_transfer_context(
        scenario,
        source,
        target,
        job_id=job_id,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
    )
    _write_metadata_caches(scenario.job_state_dir(context.job.id), source, target, context)
    return _review_payload(scenario, source, target, context)


def reconcile_target_organisation_units(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    apply: bool,
    progress: ProgressCallback | None = None,
    _dynamic_conflicts: dict[str, tuple[str, ...]] | None = None,
    _quarantine_artifact: str | None = None,
) -> dict[str, Any]:
    """Create a missing source OU hierarchy on the target without moving existing OUs."""
    job = scenario.get_job(job_id)
    policy = job.organisation_units.synchronization
    if not policy.enabled:
        raise PipelineError(
            "Organisation unit synchronization is disabled for this job. Enable "
            "target.organisation_units.synchronization.enabled first."
        )
    if job.id_schemes.organisation_units != "uid":
        raise PipelineError(
            "Organisation unit synchronization requires UID mapping so hierarchy "
            "identity can be preserved safely."
        )

    _emit_progress(
        progress,
        "organisation_units.reconcile.start",
        job=job_id,
        apply=apply,
    )
    source_data_set = source.resolve_data_set(job.source_data_set)
    selected_source_units, excluded_source_units = _resolve_source_unit_selection(
        source,
        source_data_set,
        job,
        additional_excluded_ids=set(_dynamic_conflicts or {}),
    )
    direct_source_units = tuple(selected_source_units)
    if not direct_source_units:
        raise PipelineError(
            f"Source Data Set '{job.source_data_set}' has no organisation units to reconcile."
        )

    direct_source_ids = _metadata_keys(
        direct_source_units,
        "uid",
        "source organisation units",
    )
    hierarchy_ids: set[str] = set()
    path_errors: list[str] = []
    for unit in direct_source_units:
        uid = str(unit.get("id") or "")
        path_ids = _organisation_unit_path_ids(unit)
        if not path_ids or path_ids[-1] != uid:
            path_errors.append(f"{uid or '<sans UID>'} has invalid path {unit.get('path')!r}")
            continue
        hierarchy_ids.update(path_ids)
    if path_errors:
        raise PipelineError(
            "Source organisation unit hierarchy is incomplete: "
            + "; ".join(path_errors[:10])
        )

    source_hierarchy = source.metadata_objects_by_ids(
        resource="organisationUnits",
        root_key="organisationUnits",
        object_ids=sorted(hierarchy_ids),
        fields=(
            "id,code,name,shortName,description,openingDate,closedDate,comment,"
            "geometry,parent[id],path,level"
        ),
        batch_size=job.organisation_units.batch_size,
    )
    source_by_uid = {
        str(item["id"]): item for item in source_hierarchy if item.get("id")
    }
    unresolved_source_ids = sorted(hierarchy_ids - set(source_by_uid))
    if unresolved_source_ids:
        raise PipelineError(
            "Could not read the complete source organisation unit hierarchy: "
            + ", ".join(unresolved_source_ids[:10])
        )

    target_inventory = target.organisation_units(page_size=1000)
    target_inventory_by_uid: dict[str, dict[str, Any]] = {}
    duplicate_target_uids: set[str] = set()
    target_by_code: dict[str, dict[str, Any]] = {}
    duplicate_target_codes: set[str] = set()
    for unit in target_inventory:
        uid = str(unit.get("id") or "")
        if uid:
            if uid in target_inventory_by_uid:
                duplicate_target_uids.add(uid)
            else:
                target_inventory_by_uid[uid] = unit
        code = str(unit.get("code") or "")
        if code:
            if code in target_by_code and target_by_code[code].get("id") != uid:
                duplicate_target_codes.add(code)
            else:
                target_by_code[code] = unit
    target_root_ids = {
        str(unit["id"])
        for unit in target_inventory
        if unit.get("id")
        and (
            unit.get("level") == 1
            or len(_organisation_unit_path_ids(unit)) == 1
        )
    }

    existing_hierarchy_ids = sorted(hierarchy_ids & set(target_inventory_by_uid))
    target_hierarchy = target.metadata_objects_by_ids(
        resource="organisationUnits",
        root_key="organisationUnits",
        object_ids=existing_hierarchy_ids,
        fields="id,code,name,parent[id],path,level",
        batch_size=job.organisation_units.batch_size,
    )
    target_by_uid = {
        str(item["id"]): item for item in target_hierarchy if item.get("id")
    }
    for uid in existing_hierarchy_ids:
        target_by_uid.setdefault(uid, target_inventory_by_uid[uid])

    state_dir = scenario.job_state_dir(job.id)
    current_run_id = run_id(f"organisation-units-{scenario.id}-{job.id}")
    artifact_dir = state_dir / "runs" / current_run_id / "organisation-units"
    source_path = _write_json(
        artifact_dir / "source.json",
        {
            "dataSet": asdict(source_data_set),
            "directOrganisationUnitIds": sorted(direct_source_ids),
            "organisationUnitExclusions": _organisation_unit_exclusion_payload(job),
            "excludedSourceOrganisationUnits": _organisation_unit_summary_payload(
                excluded_source_units
            ),
            "hierarchy": source_hierarchy,
        },
    )
    target_snapshot_path = _write_json(
        artifact_dir / "target-before.json",
        {
            "matchingHierarchy": [target_by_uid[uid] for uid in sorted(target_by_uid)],
            "duplicateUids": sorted(duplicate_target_uids),
            "duplicateCodes": sorted(duplicate_target_codes),
        },
    )

    blocked: list[str] = []
    warnings: list[str] = []
    quarantinable_conflicts: dict[str, list[str]] = defaultdict(list)

    def record_unit_conflict(uid: str, message: str) -> None:
        if policy.conflict_policy == "exclude":
            quarantinable_conflicts[uid].append(message)
        else:
            blocked.append(message)

    if duplicate_target_uids:
        blocked.append(
            "duplicate target UIDs: " + ", ".join(sorted(duplicate_target_uids)[:10])
        )
    source_codes = {
        str(item.get("code"))
        for item in source_hierarchy
        if item.get("code")
    }
    relevant_duplicate_codes = sorted(source_codes & duplicate_target_codes)
    if relevant_duplicate_codes:
        for source_unit in source_hierarchy:
            source_uid = str(source_unit.get("id") or "")
            source_code = str(source_unit.get("code") or "")
            if source_uid and source_code in duplicate_target_codes:
                record_unit_conflict(
                    source_uid,
                    f"source OU {source_uid} code {source_code!r} is duplicated on target",
                )

    ordered_source_ids = sorted(
        hierarchy_ids,
        key=lambda uid: (
            len(_organisation_unit_path_ids(source_by_uid[uid])),
            str(source_by_uid[uid].get("path") or ""),
            uid,
        ),
    )
    source_parents: dict[str, str | None] = {}
    for uid in ordered_source_ids:
        source_unit = source_by_uid[uid]
        path_ids = _organisation_unit_path_ids(source_unit)
        if not path_ids or path_ids[-1] != uid:
            blocked.append(f"source OU {uid} has invalid path {source_unit.get('path')!r}")
            continue
        expected_parent = path_ids[-2] if len(path_ids) > 1 else None
        explicit_parent = _reference_uid(source_unit.get("parent"))
        if explicit_parent and explicit_parent != expected_parent:
            blocked.append(
                f"source OU {uid} parent/path mismatch: parent={explicit_parent}, "
                f"pathParent={expected_parent}"
            )
        source_parents[uid] = expected_parent
        if expected_parent and expected_parent not in source_by_uid:
            blocked.append(f"source OU {uid} parent {expected_parent} was not retrieved")

        target_unit = target_by_uid.get(uid)
        source_code = str(source_unit.get("code") or "")
        if source_code:
            code_match = target_by_code.get(source_code)
            if code_match and str(code_match.get("id") or "") != uid:
                record_unit_conflict(
                    uid,
                    f"source OU {uid} code {source_code!r} is already used on target by "
                    f"UID {code_match.get('id')}",
                )
        if target_unit is None:
            if expected_parent is None:
                if not policy.allow_root_creation:
                    record_unit_conflict(
                        uid,
                        f"source root OU {uid} is missing on target while "
                        "allow_root_creation=false",
                    )
                elif target_root_ids:
                    record_unit_conflict(
                        uid,
                        f"source root OU {uid} cannot be created because target already has "
                        f"root(s): {', '.join(sorted(target_root_ids)[:10])}",
                    )
            continue

        identity_checks = (
            ("code", policy.require_same_code),
            ("name", policy.require_same_name),
        )
        for field, required in identity_checks:
            source_value = source_unit.get(field)
            target_value = target_unit.get(field)
            if source_value and source_value != target_value:
                message = (
                    f"OU {uid} {field}: source={source_value!r}, "
                    f"target={target_value!r}"
                )
                if required:
                    record_unit_conflict(uid, message)
                else:
                    warnings.append(message)
        target_parent = _organisation_unit_parent_uid(target_unit)
        if target_parent != expected_parent:
            record_unit_conflict(
                uid,
                f"OU {uid} parent: source={expected_parent}, target={target_parent}",
            )

    missing_ids = [uid for uid in ordered_source_ids if uid not in target_by_uid]
    if missing_ids and not policy.create_missing:
        for uid in missing_ids:
            record_unit_conflict(
                uid,
                f"OU {uid} is missing while create_missing=false",
            )

    quarantine_path: Path | None = (
        Path(_quarantine_artifact) if _quarantine_artifact else None
    )
    if policy.conflict_policy == "exclude" and quarantinable_conflicts:
        conflict_ids = set(quarantinable_conflicts)
        affected_units = [
            unit
            for unit in direct_source_units
            if conflict_ids
            & (set(_organisation_unit_path_ids(unit)) | {str(unit.get("id") or "")})
        ]
        if not affected_units:
            blocked.append(
                "Detected organisation-unit conflicts could not be mapped to any "
                "selected source subtree: "
                + ", ".join(sorted(conflict_ids)[:10])
            )
        elif len(affected_units) == len(direct_source_units):
            blocked.append(
                "Dynamic organisation-unit quarantine would exclude the complete "
                f"source selection ({len(affected_units)} units); transfer blocked"
            )
        elif not blocked:
            merged_conflicts: dict[str, tuple[str, ...]] = {
                uid: tuple(reasons)
                for uid, reasons in (_dynamic_conflicts or {}).items()
            }
            for uid, reasons in quarantinable_conflicts.items():
                merged_conflicts[uid] = tuple(
                    dict.fromkeys((*merged_conflicts.get(uid, ()), *reasons))
                )
            all_excluded_units = [*excluded_source_units, *affected_units]
            quarantine_path = _write_organisation_unit_quarantine(
                scenario,
                source,
                target,
                job,
                source_data_set,
                conflicts=merged_conflicts,
                excluded_source_units=all_excluded_units,
            )
            _write_json(
                artifact_dir / "quarantine.json",
                {
                    "conflictPolicy": policy.conflict_policy,
                    "detectedConflicts": _organisation_unit_conflict_payload(
                        merged_conflicts
                    ),
                    "excludedSourceOrganisationUnits": (
                        _organisation_unit_summary_payload(all_excluded_units)
                    ),
                    "manifest": str(quarantine_path),
                },
            )
            _emit_progress(
                progress,
                "organisation_units.reconcile.quarantine",
                conflicts=len(merged_conflicts),
                excluded=len(all_excluded_units),
                manifest=str(quarantine_path),
            )
            return reconcile_target_organisation_units(
                scenario,
                source,
                target,
                job_id=job_id,
                apply=apply,
                progress=progress,
                _dynamic_conflicts=merged_conflicts,
                _quarantine_artifact=str(quarantine_path),
            )
    elif policy.conflict_policy == "exclude" and _dynamic_conflicts is None:
        quarantine_path = _write_organisation_unit_quarantine(
            scenario,
            source,
            target,
            job,
            source_data_set,
            conflicts={},
            excluded_source_units=excluded_source_units,
        )

    target_data_set = target.find_data_set(job.target_data_set)
    if (
        target_data_set is None
        and scenario.metadata.identity_strategy == "preserve_uid"
        and job.target_data_set != source_data_set.id
    ):
        target_data_set = target.find_data_set(source_data_set.id)
    target_data_set_details = _target_data_set_details(
        target,
        target_data_set.id if target_data_set else None,
    )
    existing_assignments = {
        uid
        for uid in _reference_ids(
            (target_data_set_details or {}).get("organisationUnits")
        )
    }
    assignments_to_add: list[str] = []
    data_set_package: list[dict[str, Any]] = []
    if policy.attach_to_target_data_set and target_data_set is not None:
        assignments_to_add = sorted(direct_source_ids - existing_assignments)
        if assignments_to_add:
            data_set_update = _target_data_set_identity_payload(
                target_data_set_details,
                target_data_set.id,
            )
            data_set_update["dataSetElements"] = (
                _target_data_set_element_memberships(
                    target_data_set_details,
                    target_data_set.id,
                )
            )
            data_set_update["organisationUnits"] = [
                {"id": uid}
                for uid in sorted(existing_assignments | direct_source_ids)
            ]
            data_set_package = [data_set_update]

    organisation_units_package = [
        _organisation_unit_create_payload(
            source_by_uid[uid],
            parent_uid=source_parents.get(uid),
            preserve_geometry=policy.preserve_geometry,
        )
        for uid in missing_ids
    ]
    package: dict[str, Any] = {}
    if organisation_units_package:
        package["organisationUnits"] = organisation_units_package
    if data_set_package:
        package["dataSets"] = data_set_package

    plan = {
        "scenario": scenario.id,
        "job": job.id,
        "dryRun": not apply,
        "sourceDataSet": asdict(source_data_set),
        "targetDataSet": asdict(target_data_set) if target_data_set else None,
        "directSourceOrganisationUnits": len(direct_source_ids),
        "hierarchyOrganisationUnits": len(hierarchy_ids),
        "existingHierarchyOrganisationUnits": len(target_by_uid),
        "targetRootOrganisationUnits": sorted(target_root_ids),
        "missingDirectOrganisationUnits": sorted(direct_source_ids - set(target_by_uid)),
        "organisationUnitsToCreate": missing_ids,
        "organisationUnitConflictPolicy": policy.conflict_policy,
        "organisationUnitExclusions": _organisation_unit_exclusion_payload(job),
        "detectedOrganisationUnitConflicts": _organisation_unit_conflict_payload(
            _dynamic_conflicts or quarantinable_conflicts
        ),
        "organisationUnitQuarantineArtifact": (
            str(quarantine_path) if quarantine_path else None
        ),
        "excludedSourceOrganisationUnitCount": len(excluded_source_units),
        "excludedSourceOrganisationUnits": _organisation_unit_summary_payload(
            excluded_source_units
        ),
        "dataSetAssignmentsToAdd": assignments_to_add,
        "dataSetAssignmentPendingMetadataCreation": bool(
            policy.attach_to_target_data_set and target_data_set is None
        ),
        "warnings": warnings,
        "blocked": blocked,
        "sourceArtifact": str(source_path),
        "targetSnapshotArtifact": str(target_snapshot_path),
    }
    plan["requiresChanges"] = bool(package)
    plan_path = _write_json(artifact_dir / "plan.json", plan)
    package_path = _write_json(artifact_dir / "package.json", package)
    _emit_progress(
        progress,
        "organisation_units.reconcile.plan",
        hierarchy=len(hierarchy_ids),
        excluded=len(excluded_source_units),
        quarantinedConflicts=len(_dynamic_conflicts or quarantinable_conflicts),
        create=len(missing_ids),
        attach=len(assignments_to_add),
        warnings=len(warnings),
        blocked=len(blocked),
    )
    if blocked:
        blocked_path = _write_json(artifact_dir / "blocked.json", {"blocked": blocked})
        raise PipelineError(
            "Organisation unit synchronization blocked: "
            + "; ".join(blocked[:10])
            + f". Audit: {blocked_path}"
        )

    validation: dict[str, Any] | None = None
    commit: dict[str, Any] | None = None
    if package:
        _emit_progress(
            progress,
            "organisation_units.validation.start",
            create=len(missing_ids),
            attach=len(assignments_to_add),
        )
        validation = target.import_metadata(
            package,
            commit=False,
            import_strategy="CREATE_AND_UPDATE" if data_set_package else "CREATE",
            atomic_mode="ALL",
            identifier="UID",
            skip_sharing=scenario.metadata.skip_sharing,
        )
        _assert_success_response(validation, "Organisation unit hierarchy validation")
        _emit_progress(progress, "organisation_units.validation.done")
        if apply:
            _emit_progress(progress, "organisation_units.commit.start")
            commit = target.import_metadata(
                package,
                commit=True,
                import_strategy="CREATE_AND_UPDATE" if data_set_package else "CREATE",
                atomic_mode="ALL",
                identifier="UID",
                skip_sharing=scenario.metadata.skip_sharing,
            )
            _assert_success_response(commit, "Organisation unit hierarchy import")
            _emit_progress(progress, "organisation_units.commit.done")

    _write_json(artifact_dir / "validation.json", validation or {})
    _write_json(artifact_dir / "commit.json", commit or {})
    status = "already_aligned" if not package else ("created" if apply else "validated")
    if _dynamic_conflicts:
        status = f"{status}_with_quarantine"
    if apply and package:
        verification_conflicts: list[str] = []
        if missing_ids:
            verified_units = target.metadata_objects_by_ids(
                resource="organisationUnits",
                root_key="organisationUnits",
                object_ids=missing_ids,
                fields="id,code,name,parent[id],path,level",
                batch_size=job.organisation_units.batch_size,
            )
            verified_by_uid = {
                str(item["id"]): item for item in verified_units if item.get("id")
            }
            missing_after_commit = sorted(set(missing_ids) - set(verified_by_uid))
            if missing_after_commit:
                verification_conflicts.append(
                    "missing after import: " + ", ".join(missing_after_commit[:10])
                )
            for uid in sorted(set(missing_ids) & set(verified_by_uid)):
                source_unit = source_by_uid[uid]
                target_unit = verified_by_uid[uid]
                for field in ("code", "name"):
                    if source_unit.get(field) != target_unit.get(field):
                        verification_conflicts.append(
                            f"OU {uid} {field} differs after import"
                        )
                if _organisation_unit_parent_uid(target_unit) != source_parents.get(uid):
                    verification_conflicts.append(f"OU {uid} parent differs after import")
        if assignments_to_add and target_data_set is not None:
            verified_data_set = _target_data_set_details(target, target_data_set.id) or {}
            verified_assignments = set(
                _reference_ids(verified_data_set.get("organisationUnits"))
            )
            missing_assignments = sorted(set(assignments_to_add) - verified_assignments)
            if missing_assignments:
                verification_conflicts.append(
                    "target Data Set assignments missing: "
                    + ", ".join(missing_assignments[:10])
                )
        verification_path = _write_json(
            artifact_dir / "verification.json",
            {
                "status": "ERROR" if verification_conflicts else "OK",
                "conflicts": verification_conflicts,
                "createdOrganisationUnits": missing_ids,
                "dataSetAssignments": assignments_to_add,
            },
        )
        if verification_conflicts:
            raise PipelineError(
                "Organisation unit verification failed: "
                + "; ".join(verification_conflicts[:10])
                + f". Audit: {verification_path}"
            )
        _emit_progress(
            progress,
            "organisation_units.verify.done",
            created=len(missing_ids),
            attached=len(assignments_to_add),
        )

    report = {
        **plan,
        "runId": current_run_id,
        "status": status,
        "validated": validation is not None or not package,
        "committed": commit is not None,
        "planArtifact": str(plan_path),
        "packageArtifact": str(package_path),
        "validationArtifact": str(artifact_dir / "validation.json"),
        "commitArtifact": str(artifact_dir / "commit.json"),
        "verificationArtifact": (
            str(artifact_dir / "verification.json") if apply and package else None
        ),
    }
    report_path = write_report(state_dir, current_run_id, report)
    report["reportPath"] = str(report_path)
    _emit_progress(
        progress,
        "organisation_units.reconcile.done",
        status=status,
        created=len(missing_ids) if apply else 0,
        attached=len(assignments_to_add) if apply else 0,
    )
    return report


def run_transfer(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    apply: bool,
    force: bool = False,
    keep_payload: bool = False,
    start: str | None = None,
    end: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
    progress: ProgressCallback | None = None,
    resume_chunk_checkpoints: bool | None = None,
) -> dict[str, Any]:
    _emit_progress(progress, "transfer.start", job=job_id, apply=apply)
    _emit_progress(progress, "context.resolve.start")
    context = resolve_transfer_context(
        scenario,
        source,
        target,
        job_id=job_id,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
    )
    _emit_progress(
        progress,
        "context.resolve.done",
        sourceDataElements=len(context.source_data_elements),
        organisationUnits=len(context.source_organisation_units),
        excludedOrganisationUnits=len(context.excluded_source_organisation_units),
        start=context.start,
        end=context.end,
        metadataBootstrap=context.metadata_bootstrap_required,
    )
    if context.job.transfer.include_deleted:
        raise PipelineError(
            "Transfer of deleted data values is intentionally disabled in this version. "
            "Set transfer.include_deleted to false."
        )

    state_dir = scenario.job_state_dir(context.job.id)
    _write_metadata_caches(state_dir, source, target, context)
    _emit_progress(progress, "cache.metadata.written", directory=str(state_dir))
    config_hash = _sha256_file(scenario.path)
    fingerprint = TransferFingerprint(
        scenario_id=scenario.id,
        job_id=context.job.id,
        source_instance=f"{scenario.source_instance}@{source.base_url}",
        target_instance=f"{scenario.target_instance}@{target.base_url}",
        source_data_set=context.source_data_set.id,
        target_data_set=context.job.target_data_set,
        start=context.start,
        end=context.end,
        config_hash=config_hash,
    )
    journal = TransferRunJournal(state_dir / "journal.sqlite3")
    if apply and not force and journal.has_successful_transfer(fingerprint):
        raise PipelineError(
            "This transfer already succeeded for the same source, target, data set, period "
            "window and configuration. Use --force to run it again."
        )

    current_run_id = run_id(f"transfer-{scenario.id}-{context.job.id}")
    source_count = 0
    empty_counts = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
    chunk_resume_enabled = context.job.transfer.execution.resume_chunks and (
        not force
        if resume_chunk_checkpoints is None
        else resume_chunk_checkpoints
    )
    try:
        metadata_report: dict[str, Any] = {
            "required": context.metadata_bootstrap_required,
            "validated": False,
            "committed": False,
        }
        metadata_package: dict[str, Any] | None = None
        if context.metadata_bootstrap_required:
            _emit_progress(progress, "metadata.source_export.start")
            source_metadata_package = source.export_data_set_metadata(
                context.source_data_set.id,
                skip_sharing=scenario.metadata.skip_sharing,
            )
            source_metadata_path = _write_json(
                state_dir
                / "artifacts"
                / "metadata"
                / f"{context.job.id}.metadata.source.json",
                source_metadata_package,
            )
            _emit_progress(progress, "metadata.source_export.done")
            metadata_package, preparation_report = _prepare_metadata_package(
                source_metadata_package
            )
            metadata_path = _write_json(
                state_dir / "artifacts" / "metadata" / f"{context.job.id}.metadata.json",
                metadata_package,
            )
            _emit_progress(progress, "metadata.validation.start")
            metadata_package, validation, reused_objects = _validate_metadata_package(
                target,
                metadata_package,
                import_strategy=scenario.metadata.import_strategy,
                atomic_mode=scenario.metadata.atomic_mode,
                skip_sharing=scenario.metadata.skip_sharing,
            )
            _write_json(metadata_path, metadata_package)
            _assert_success_response(validation, "Target metadata validation")
            _emit_progress(
                progress,
                "metadata.validation.done",
                reusedObjects=sum(reused_objects.values()),
            )
            metadata_report.update(
                {
                    "validated": True,
                    "artifact": str(metadata_path),
                    "sourceArtifact": str(source_metadata_path),
                    "preparation": preparation_report,
                    "reusedExistingObjects": reused_objects,
                    "validationResponse": validation,
                }
            )

        # Commit and re-read canonical metadata before exporting a potentially
        # very large value window.  Historical value handling needs the target
        # Data Set to exist so it can attach versioned Data Elements safely.
        if metadata_package is not None and apply:
            _emit_progress(progress, "metadata.commit.start")
            commit_response = target.import_metadata(
                metadata_package,
                commit=True,
                import_strategy=scenario.metadata.import_strategy,
                atomic_mode=scenario.metadata.atomic_mode,
                identifier="UID",
                skip_sharing=scenario.metadata.skip_sharing,
            )
            _assert_success_response(commit_response, "Target metadata import")
            _emit_progress(progress, "metadata.commit.done")
            metadata_report.update(
                {"committed": True, "commitResponse": commit_response}
            )
            target_data_set = target.find_data_set(context.source_data_set.id)
            if target_data_set is None:
                target_data_set = target.find_data_set(context.job.target_data_set)
            if target_data_set is None:
                raise PipelineError(
                    "Metadata import completed but the target Data Set cannot be resolved."
                )
            target_elements = tuple(
                target.data_elements_for_data_set(
                    data_set_id=target_data_set.id, page_size=500
                )
            )
            context = replace(
                context,
                target_data_set=target_data_set,
                target_data_elements=target_elements,
                metadata_bootstrap_required=True,
            )
            _validate_data_set_compatibility(context)
            _write_metadata_caches(state_dir, source, target, context)

        source_payload = _export_source_values(source, context, progress=progress)
        source_values = source_payload["dataValues"]
        source_count = len(source_values)
        if not context.job.transfer.preserve_comments:
            source_values = [
                {key: value for key, value in item.items() if key != "comment"}
                for item in source_values
            ]
        source_values, invalid_data_value_report = _quarantine_invalid_data_values(
            source_values,
            state_dir=state_dir,
            current_run_id=current_run_id,
        )
        invalid_data_value_count = int(
            invalid_data_value_report["quarantinedDataValueCount"]
        )
        if source_count and invalid_data_value_count == source_count:
            raise PipelineError(
                "Invalid-value quarantine would remove every source data value in this "
                f"execution window. Transfer blocked. Audit: "
                f"{invalid_data_value_report['artifact']}"
            )
        if invalid_data_value_count:
            _emit_progress(
                progress,
                "source.invalid_values.quarantine",
                errorCode="E8120",
                quarantinedDataValues=invalid_data_value_count,
                remainingDataValues=len(source_values),
                artifact=invalid_data_value_report["artifact"],
            )
        source_values, historical_data_element_report = (
            _version_unmappable_historical_values(
                source,
                target,
                scenario,
                context,
                source_values,
                apply=apply,
                progress=progress,
            )
        )
        historical_pending = bool(
            historical_data_element_report.get("validationPendingMetadataCommit")
        )
        if historical_pending:
            category_option_combo_report = {
                "checked": False,
                "historicalCombinationCount": 0,
                "remappedValueCount": 0,
                "mappings": [],
            }
        else:
            historical_target_ids = {
                str(item["targetDataElement"])
                for item in historical_data_element_report.get("mappings", [])
                if item.get("targetDataElement")
            }
            source_values, category_option_combo_report = (
                _normalise_source_category_option_combos(
                    source,
                    context,
                    source_values,
                    ignored_data_elements=historical_target_ids,
                    progress=progress,
                )
            )
        source_payload = {"dataValues": source_values}
        if keep_payload:
            _write_json(
                state_dir / "artifacts" / "data" / f"{current_run_id}.values.json",
                source_payload,
            )

        if apply and historical_data_element_report.get("mappings"):
            if context.target_data_set is None:
                raise PipelineError(
                    "Historical Data Elements were created but the target Data Set is unavailable."
                )
            context = replace(
                context,
                target_data_elements=tuple(
                    target.data_elements_for_data_set(
                        data_set_id=context.target_data_set.id,
                        page_size=500,
                    )
                ),
            )

        dry_run_responses: list[dict[str, Any]] = []
        apply_responses: list[dict[str, Any]] = []
        validated_chunk_count = 0
        adaptive_validation_report: dict[str, Any] = {
            "enabled": context.job.transfer.adaptive_chunking.enabled,
            "retries": 0,
            "splits": 0,
            "smallestChunkSize": None,
        }
        adaptive_import_report: dict[str, Any] = {
            "enabled": context.job.transfer.adaptive_chunking.enabled,
            "retries": 0,
            "splits": 0,
            "smallestChunkSize": None,
            "resumedChunks": 0,
            "resumedDataValues": 0,
            "checkpoint": None,
        }
        timeliness_quarantine_report: dict[str, Any] = {
            "enabled": (
                context.job.organisation_units.synchronization.conflict_policy
                == "exclude"
            ),
            "errorCode": "E8031",
            "quarantinedDataValueCount": 0,
            "organisationUnitCount": 0,
            "periodCount": 0,
            "groups": [],
            "artifact": None,
        }
        if context.target_data_set is not None and not historical_pending:
            _validate_target_data_set_organisation_units(
                target,
                context,
                source_values,
                progress=progress,
            )
            _validate_target_category_option_combos(
                target,
                context,
                source_values,
                progress=progress,
            )
            target_payload = _target_payload(context, source_values)
            chunks = _chunk_payload(target_payload, context.job.transfer.chunk_size)
            (
                dry_run_responses,
                chunks,
                timeliness_quarantine_report,
            ) = _validate_target_chunks(
                target,
                chunks,
                context=context,
                state_dir=state_dir,
                current_run_id=current_run_id,
                progress=progress,
            )
            validated_chunk_count = len(chunks)
            adaptive_validation_report = dict(
                timeliness_quarantine_report.get("adaptiveChunking") or {}
            )
            if apply:
                target_value_count = sum(
                    len(chunk["dataValues"]) for chunk in chunks
                )
                _emit_progress(
                    progress,
                    "target.import.start",
                    chunks=len(chunks),
                    dataValues=target_value_count,
                )
                apply_responses, adaptive_import_report = _import_target_chunks(
                    target,
                    chunks,
                    context=context,
                    state_dir=state_dir,
                    resume_checkpoints=chunk_resume_enabled,
                    progress=progress,
                )
                _emit_progress(
                    progress,
                    "target.import.done",
                    chunks=len(chunks),
                    requestChunks=len(apply_responses),
                    resumedChunks=adaptive_import_report["resumedChunks"],
                )
        elif apply:
            raise PipelineError("Target Data Set is unavailable after metadata bootstrap.")

        counts = import_counts(apply_responses if apply else dry_run_responses)
        quarantined_data_value_count = int(
            timeliness_quarantine_report.get("quarantinedDataValueCount") or 0
        )
        status = (
            "success_with_quarantine"
            if context.detected_organisation_unit_conflicts
            or quarantined_data_value_count
            or invalid_data_value_count
            else "success"
        )
        report = {
            "runId": current_run_id,
            "scenario": scenario.id,
            "job": context.job.id,
            "dryRun": not apply,
            "status": status,
            "sourceInstance": {
                "profile": scenario.source_instance,
                "baseUrl": source.base_url,
            },
            "targetInstance": {
                "profile": scenario.target_instance,
                "baseUrl": target.base_url,
            },
            "sourceDataSet": asdict(context.source_data_set),
            "targetDataSet": asdict(context.target_data_set)
            if context.target_data_set
            else None,
            "start": context.start,
            "end": context.end,
            "sourceDataValueCount": source_count,
            "targetDataValueCount": (
                source_count
                - invalid_data_value_count
                - quarantined_data_value_count
            ),
            "chunkCount": validated_chunk_count,
            "metadata": metadata_report,
            "historicalDataElements": historical_data_element_report,
            "categoryOptionCombos": category_option_combo_report,
            "invalidDataValueQuarantine": invalid_data_value_report,
            "timelinessQuarantine": timeliness_quarantine_report,
            "adaptiveChunking": {
                "configuration": asdict(
                    context.job.transfer.adaptive_chunking
                ),
                "validation": adaptive_validation_report,
                "import": adaptive_import_report,
            },
            "organisationUnitExclusions": _organisation_unit_exclusion_payload(
                context.job
            ),
            "organisationUnitConflictPolicy": (
                context.job.organisation_units.synchronization.conflict_policy
            ),
            "detectedOrganisationUnitConflicts": list(
                context.detected_organisation_unit_conflicts
            ),
            "organisationUnitQuarantineActive": bool(
                context.detected_organisation_unit_conflicts
            ),
            "excludedSourceOrganisationUnitCount": len(
                context.excluded_source_organisation_units
            ),
            "excludedSourceOrganisationUnits": _organisation_unit_summary_payload(
                context.excluded_source_organisation_units
            ),
            "dataValidationPendingMetadataCommit": bool(
                context.target_data_set is None and not apply
            ),
            "dataValidationPendingHistoricalMetadataCommit": historical_pending,
            "dryRunResponses": dry_run_responses,
            "applyResponses": apply_responses,
            "counts": counts,
            "configHash": config_hash,
        }
        report_path = write_report(state_dir, current_run_id, report)
        journal.record(
            run_id=current_run_id,
            fingerprint=fingerprint,
            dry_run=not apply,
            status="success",
            source_count=source_count,
            counts=counts,
            report_path=str(report_path),
        )
        report["reportPath"] = str(report_path)
        _emit_progress(
            progress,
            "transfer.done",
            sourceDataValues=source_count,
            reportPath=str(report_path),
        )
        return report
    except Exception as exc:
        _emit_progress(progress, "transfer.failed", error=str(exc)[:500])
        journal.record(
            run_id=current_run_id,
            fingerprint=fingerprint,
            dry_run=not apply,
            status="failed",
            source_count=source_count,
            counts=empty_counts,
            error=str(exc),
        )
        raise


_RECONCILIATION_AUDIT_START = "1900-01-01"
_RECONCILIATION_AUDIT_END = "2100-12-31"


def repair_target_metadata(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    apply: bool,
    start: str | None = None,
    end: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Repair an incomplete target category model before bootstrapping a Data Set.

    The operation is deliberately separate from the data transfer.  It only accepts the
    narrowly observed failure mode where an existing Data Element has the source UID and
    identity fields but points to the default CategoryCombo on the target.
    """
    if not scenario.metadata.reconcile_existing:
        raise PipelineError(
            "Metadata repair is disabled. Set target.metadata.reconcile_existing: true "
            "in the scenario after reviewing the target audit."
        )
    context = resolve_transfer_context(
        scenario,
        source,
        target,
        job_id=job_id,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
        allow_metadata_repair=True,
    )
    if context.target_data_set is not None:
        raise PipelineError(
            "Metadata repair is only allowed while the target Data Set is missing. "
            "An existing Data Set requires a separate migration plan."
        )

    current_run_id = run_id(f"repair-{scenario.id}-{context.job.id}")
    state_dir = scenario.job_state_dir(context.job.id)
    source_package = source.export_data_set_metadata(
        context.source_data_set.id,
        skip_sharing=scenario.metadata.skip_sharing,
    )
    source_path = _write_json(
        state_dir
        / "artifacts"
        / "metadata"
        / f"{context.job.id}.repair.metadata.source.json",
        source_package,
    )
    prepared, preparation_report = _prepare_metadata_package(source_package)
    _restrict_data_set_organisation_units(
        prepared,
        data_set_id=context.source_data_set.id,
        allowed_uids={
            str(unit["id"])
            for unit in context.source_organisation_units
            if unit.get("id")
        },
    )

    if not context.category_combo_repairs:
        report = {
            "runId": current_run_id,
            "scenario": scenario.id,
            "job": context.job.id,
            "dryRun": not apply,
            "status": "not_required",
            "sourceArtifact": str(source_path),
            "message": "No repairable CategoryCombo mismatch remains on the target.",
        }
        report_path = write_report(state_dir, current_run_id, report)
        report["reportPath"] = str(report_path)
        return report

    structure_package, assignment_package, repair_report, rollback = (
        _build_metadata_repair_plan(target, prepared, context)
    )
    structure_path = _write_json(
        state_dir
        / "artifacts"
        / "metadata"
        / f"{context.job.id}.repair.structure.json",
        structure_package,
    )
    assignment_path = _write_json(
        state_dir
        / "artifacts"
        / "metadata"
        / f"{context.job.id}.repair.assignments.json",
        assignment_package,
    )
    rollback_path = _write_json(
        state_dir
        / "artifacts"
        / "metadata"
        / f"{context.job.id}.repair.rollback.json",
        rollback,
    )
    progress_path = (
        state_dir
        / "artifacts"
        / "metadata"
        / f"{context.job.id}.repair.progress.json"
    )
    repair_progress: dict[str, Any] = {
        "runId": current_run_id,
        "job": context.job.id,
        "dryRun": not apply,
        "stages": [],
    }

    def record_repair_stage(
        stage: str,
        status: str,
        *,
        response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        entry: dict[str, Any] = {"stage": stage, "status": status}
        if response is not None:
            entry["response"] = response
        if error is not None:
            entry["error"] = str(error)
        repair_progress["stages"].append(entry)
        repair_progress["lastStage"] = stage
        repair_progress["status"] = status
        _write_json(progress_path, repair_progress)

    def import_repair_stage(
        stage: str,
        payload: dict[str, Any],
        *,
        commit: bool,
        import_strategy: str,
        label: str,
    ) -> dict[str, Any]:
        record_repair_stage(stage, "running")
        try:
            response = target.import_metadata(
                payload,
                commit=commit,
                import_strategy=import_strategy,
                atomic_mode="ALL",
                identifier="UID",
                skip_sharing=scenario.metadata.skip_sharing,
            )
            _assert_success_response(response, label)
        except Exception as exc:
            record_repair_stage(stage, "failed", error=exc)
            raise
        record_repair_stage(stage, "success", response=response)
        return response

    record_repair_stage("plan", "success")

    structure_required = bool(structure_package.get("categoryCombos"))
    structure_validation: dict[str, Any] | None = None
    structure_commit: dict[str, Any] | None = None
    assignment_validation: dict[str, Any] | None = None
    assignment_commit: dict[str, Any] | None = None
    assignment_validation_pending = structure_required and not apply
    if structure_required:
        structure_validation = import_repair_stage(
            "structure_validation",
            structure_package,
            commit=False,
            import_strategy="UPDATE",
            label="Target CategoryCombo repair validation",
        )
        if apply:
            structure_commit = import_repair_stage(
                "structure_commit",
                structure_package,
                commit=True,
                import_strategy="UPDATE",
                label="Target CategoryCombo repair",
            )

    if not structure_required or apply:
        record_repair_stage("assignment_preparation", "running")
        try:
            assignment_package, category_option_combo_report = (
                _prepare_assignment_package_for_target(
                    target,
                    assignment_package,
                    repair_report["categoryComboIds"],
                )
            )
            repair_report["categoryOptionCombosAfterStructureCommit"] = (
                category_option_combo_report
            )
            assignment_package, option_set_target_report = (
                _prepare_option_set_dependencies_for_target(
                    target,
                    assignment_package,
                )
            )
        except Exception as exc:
            record_repair_stage("assignment_preparation", "failed", error=exc)
            raise
        record_repair_stage("assignment_preparation", "success")
        repair_report["optionSetsAfterTargetAudit"] = option_set_target_report
        _write_json(assignment_path, assignment_package)
        assignment_validation = import_repair_stage(
            "assignment_validation",
            assignment_package,
            commit=False,
            import_strategy="CREATE_AND_UPDATE",
            label="Target CategoryOptionCombo/DataElement repair validation",
        )
        assignment_validation_pending = False
        if apply:
            assignment_commit = import_repair_stage(
                "assignment_commit",
                assignment_package,
                commit=True,
                import_strategy="CREATE_AND_UPDATE",
                label="Target CategoryOptionCombo/DataElement repair",
            )
    if apply:
        refreshed_target_elements = tuple(
            _matching_metadata_objects(
                context.source_data_elements,
                target.data_elements(page_size=1000),
                context.job.id_schemes.data_elements,
                "target Data Elements",
            )
        )
        verified_context = replace(
            context,
            target_data_elements=refreshed_target_elements,
            category_combo_repairs=(),
        )
        _validate_data_set_compatibility(verified_context)

    report = {
        "runId": current_run_id,
        "scenario": scenario.id,
        "job": context.job.id,
        "dryRun": not apply,
        "status": "repaired" if apply else "validated",
        "sourceInstance": {
            "profile": scenario.source_instance,
            "baseUrl": source.base_url,
        },
        "targetInstance": {
            "profile": scenario.target_instance,
            "baseUrl": target.base_url,
        },
        "preparation": preparation_report,
        "repair": repair_report,
        "structure": {
            "artifact": str(structure_path),
            "required": structure_required,
            "validated": structure_validation is not None or not structure_required,
            "committed": structure_commit is not None,
            "validationResponse": structure_validation,
            "commitResponse": structure_commit,
        },
        "assignments": {
            "artifact": str(assignment_path),
            "validationPendingStructureCommit": assignment_validation_pending,
            "validated": assignment_validation is not None,
            "committed": assignment_commit is not None,
            "validationResponse": assignment_validation,
            "commitResponse": assignment_commit,
        },
        "sourceArtifact": str(source_path),
        "rollbackEvidenceArtifact": str(rollback_path),
        "progressArtifact": str(progress_path),
        "nextStep": (
            "Run repair-metadata with --apply, then rerun the normal transfer dry-run."
            if not apply
            else "Rerun the normal transfer with --dry-run before importing data."
        ),
    }
    report_path = write_report(state_dir, current_run_id, report)
    report["reportPath"] = str(report_path)
    return report


def reconcile_target_metadata(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    apply: bool,
    start: str | None = None,
    end: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Create or complete the canonical target Data Set before exporting values."""
    if not scenario.metadata.synchronization.enabled:
        raise PipelineError(
            "Metadata synchronization is disabled for this scenario."
        )
    _emit_progress(progress, "metadata.reconcile.start", job=job_id, apply=apply)
    context = resolve_transfer_context(
        scenario,
        source,
        target,
        job_id=job_id,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
        allow_metadata_repair=(
            scenario.metadata.synchronization.incompatible_policy == "safe_repair"
        ),
        allow_incomplete_target=True,
    )
    if context.category_combo_repairs:
        repair = repair_target_metadata(
            scenario,
            source,
            target,
            job_id=job_id,
            apply=apply,
            start=context.start,
            end=context.end,
        )
        if not apply:
            return {
                "scenario": scenario.id,
                "job": context.job.id,
                "dryRun": True,
                "status": "safe_repair_required",
                "requiresChanges": True,
                "committed": False,
                "repair": repair,
                "nextStep": (
                    "Run reconcile-metadata with --apply, then rerun the transfer."
                ),
            }
        return reconcile_target_metadata(
            scenario,
            source,
            target,
            job_id=job_id,
            apply=True,
            start=context.start,
            end=context.end,
            progress=progress,
        )
    state_dir = scenario.job_state_dir(context.job.id)
    current_run_id = run_id(f"metadata-{scenario.id}-{context.job.id}")
    artifact_dir = state_dir / "runs" / current_run_id / "metadata"

    source_package = source.export_data_set_metadata(
        context.source_data_set.id,
        skip_sharing=scenario.metadata.skip_sharing,
    )
    source_path = _write_json(artifact_dir / "source.json", source_package)
    prepared, preparation_report = _prepare_metadata_package(source_package)
    _restrict_data_set_organisation_units(
        prepared,
        data_set_id=context.source_data_set.id,
        allowed_uids={
            str(unit["id"])
            for unit in context.source_organisation_units
            if unit.get("id")
        },
    )

    all_target_elements = target.data_elements(page_size=1000)
    source_by_uid = {
        str(item["id"]): item
        for item in context.source_data_elements
        if item.get("id")
    }
    target_by_uid = {
        str(item["id"]): item for item in all_target_elements if item.get("id")
    }
    target_by_code = {
        str(item["code"]): item for item in all_target_elements if item.get("code")
    }
    source_codes = {
        str(item["code"])
        for item in context.source_data_elements
        if item.get("code")
    }
    target_snapshot_path = _write_json(
        artifact_dir / "target-before.json",
        {
            "dataSet": (
                asdict(context.target_data_set)
                if context.target_data_set is not None
                else None
            ),
            "linkedDataElements": list(context.target_data_elements),
            "dataSetDetails": _target_data_set_details(
                target,
                context.target_data_set.id if context.target_data_set else None,
            ),
            "affectedGlobalDataElements": [
                item
                for item in all_target_elements
                if item.get("id") in source_by_uid
                or (item.get("code") and str(item["code"]) in source_codes)
            ],
        },
    )
    linked_uids = {
        str(item["id"]) for item in context.target_data_elements if item.get("id")
    }

    collisions: list[str] = []
    identity_warnings: list[str] = []
    if (
        context.target_data_set is not None
        and scenario.metadata.synchronization.require_same_identity
    ):
        source_code = context.source_data_set.code
        target_code = context.target_data_set.code
        if source_code and target_code and source_code != target_code:
            collisions.append(
                f"Data Set code: source={source_code!r}, target={target_code!r}"
            )
        elif source_code and not target_code:
            identity_warnings.append(
                f"Data Set code is absent on target; UID identity remains authoritative "
                f"(source={source_code!r})"
            )
        source_name = context.source_data_set.name
        target_name = context.target_data_set.name
        if source_name and source_name != target_name:
            collisions.append(
                f"Data Set name: source={source_name!r}, target={target_name!r}"
            )
    missing_uids: list[str] = []
    attach_uids: list[str] = []
    compatible_uids: list[str] = []
    incompatible: list[str] = []
    for uid, source_element in sorted(source_by_uid.items()):
        target_element = target_by_uid.get(uid)
        source_code = source_element.get("code")
        if target_element is None:
            if source_code and source_code in target_by_code:
                other = target_by_code[str(source_code)]
                collisions.append(
                    f"code {source_code} uses source UID {uid} but target UID "
                    f"{other.get('id')}"
                )
            else:
                missing_uids.append(uid)
            continue
        element_conflicts = _data_element_conflicts(
            source_element,
            target_element,
            category_scheme=context.job.id_schemes.category_option_combos,
        )
        if scenario.metadata.synchronization.require_same_identity:
            source_code = source_element.get("code")
            target_code = target_element.get("code")
            if source_code and target_code and source_code != target_code:
                element_conflicts.append(
                    f"code: source={source_code!r}, target={target_code!r}"
                )
            elif source_code and not target_code:
                identity_warnings.append(
                    f"Data Element {uid} code is absent on target; UID identity remains "
                    f"authoritative (source={source_code!r})"
                )
            source_name = source_element.get("name")
            target_name = target_element.get("name")
            if source_name and source_name != target_name:
                element_conflicts.append(
                    f"name: source={source_name!r}, target={target_name!r}"
                )
        if element_conflicts:
            incompatible.extend(f"{uid} {item}" for item in element_conflicts)
            continue
        compatible_uids.append(uid)
        if context.target_data_set is not None and uid not in linked_uids:
            attach_uids.append(uid)

    blocked = collisions + incompatible
    if missing_uids and not scenario.metadata.synchronization.create_missing_objects:
        blocked.append(
            f"{len(missing_uids)} missing Data Elements while create_missing_objects=false"
        )
    if attach_uids and not scenario.metadata.synchronization.attach_compatible_data_elements:
        blocked.append(
            f"{len(attach_uids)} compatible Data Elements are not attached while "
            "attach_compatible_data_elements=false"
        )

    plan = {
        "scenario": scenario.id,
        "job": context.job.id,
        "dryRun": not apply,
        "targetDataSetExists": context.target_data_set is not None,
        "createDataSet": context.target_data_set is None,
        "sourceDataElements": len(source_by_uid),
        "compatibleDataElements": len(compatible_uids),
        "createDataElements": missing_uids,
        "attachDataElements": attach_uids,
        "identityWarnings": identity_warnings,
        "blocked": blocked,
        "preparation": preparation_report,
        "sourceArtifact": str(source_path),
        "targetSnapshotArtifact": str(target_snapshot_path),
    }
    plan["requiresChanges"] = bool(
        context.target_data_set is None or missing_uids or attach_uids
    )
    plan_path = _write_json(artifact_dir / "plan.json", plan)
    if blocked:
        _write_json(artifact_dir / "blocked.json", {"blocked": blocked})
        raise PipelineError(
            "Metadata reconciliation blocked: " + "; ".join(blocked[:10])
        )

    validations: list[dict[str, Any]] = []
    commits: list[dict[str, Any]] = []
    if context.target_data_set is None:
        candidate, validation, reused = _validate_metadata_package(
            target,
            prepared,
            import_strategy=scenario.metadata.import_strategy,
            atomic_mode=scenario.metadata.atomic_mode,
            skip_sharing=scenario.metadata.skip_sharing,
        )
        _assert_success_response(validation, "Target metadata reconciliation validation")
        _write_json(artifact_dir / "canonical-package.json", candidate)
        validations.append({"stage": "create_data_set", "response": validation})
        plan["reusedExistingObjects"] = reused
        if apply:
            response = target.import_metadata(
                candidate,
                commit=True,
                import_strategy=scenario.metadata.import_strategy,
                atomic_mode=scenario.metadata.atomic_mode,
                identifier="UID",
                skip_sharing=scenario.metadata.skip_sharing,
            )
            _assert_success_response(response, "Target metadata reconciliation")
            commits.append({"stage": "create_data_set", "response": response})
    else:
        if missing_uids:
            create_package = _metadata_package_for_missing_data_elements(
                prepared,
                set(missing_uids),
            )
            candidate, validation, reused = _validate_metadata_package(
                target,
                create_package,
                import_strategy="CREATE",
                atomic_mode=scenario.metadata.atomic_mode,
                skip_sharing=scenario.metadata.skip_sharing,
            )
            _assert_success_response(validation, "Missing target metadata validation")
            _write_json(artifact_dir / "create-missing.json", candidate)
            validations.append({"stage": "create_missing", "response": validation})
            plan["reusedExistingObjects"] = reused
            if apply:
                response = target.import_metadata(
                    candidate,
                    commit=True,
                    import_strategy="CREATE",
                    atomic_mode=scenario.metadata.atomic_mode,
                    identifier="UID",
                    skip_sharing=scenario.metadata.skip_sharing,
                )
                _assert_success_response(response, "Create missing target metadata")
                commits.append({"stage": "create_missing", "response": response})

        membership_uids = sorted(set(missing_uids) | set(attach_uids))
        if membership_uids:
            membership_package = _data_set_membership_package(
                target,
                data_set_id=context.target_data_set.id,
                data_element_ids=set(linked_uids) | set(membership_uids),
            )
            _write_json(artifact_dir / "attach.json", membership_package)
            if not missing_uids or apply:
                validation = target.import_metadata(
                    membership_package,
                    commit=False,
                    import_strategy="CREATE_AND_UPDATE",
                    atomic_mode=scenario.metadata.atomic_mode,
                    identifier="UID",
                    skip_sharing=scenario.metadata.skip_sharing,
                )
                _assert_success_response(validation, "Data Set membership validation")
                validations.append({"stage": "attach", "response": validation})
                if apply:
                    response = target.import_metadata(
                        membership_package,
                        commit=True,
                        import_strategy="CREATE_AND_UPDATE",
                        atomic_mode=scenario.metadata.atomic_mode,
                        identifier="UID",
                        skip_sharing=scenario.metadata.skip_sharing,
                    )
                    _assert_success_response(response, "Attach target Data Elements")
                    commits.append({"stage": "attach", "response": response})
            else:
                plan["attachmentValidationPendingCreateCommit"] = True

    _write_json(artifact_dir / "validation.json", validations)
    _write_json(artifact_dir / "commit.json", commits)
    plan["validated"] = True
    plan["committed"] = apply
    plan["planArtifact"] = str(plan_path)

    if apply:
        verified = resolve_transfer_context(
            scenario,
            source,
            target,
            job_id=job_id,
            start=context.start,
            end=context.end,
            start_period=None,
            end_period=None,
            years_back=None,
            as_of=None,
        )
        plan["verifiedDataElements"] = len(verified.target_data_elements)
        plan["targetDataSet"] = asdict(verified.target_data_set)
        _write_metadata_caches(state_dir, source, target, verified)

    report_path = _write_json(artifact_dir.parent / "metadata-summary.json", plan)
    plan["reportPath"] = str(report_path)
    _emit_progress(
        progress,
        "metadata.reconcile.done",
        created=len(missing_uids) + int(context.target_data_set is None),
        attached=len(attach_uids),
        committed=apply,
    )
    return plan


def _data_element_conflicts(
    source: dict[str, Any],
    target: dict[str, Any],
    *,
    category_scheme: str,
) -> list[str]:
    conflicts: list[str] = []
    for field in ("valueType", "domainType"):
        source_value = source.get(field)
        target_value = target.get(field)
        if source_value and target_value and source_value != target_value:
            conflicts.append(f"{field}: source={source_value}, target={target_value}")
    source_category = _reference_key(source.get("categoryCombo"), category_scheme)
    target_category = _reference_key(target.get("categoryCombo"), category_scheme)
    if source_category and target_category and source_category != target_category:
        conflicts.append(
            f"categoryCombo: source={source_category}, target={target_category}"
        )
    return conflicts


def _metadata_package_for_missing_data_elements(
    package: dict[str, Any], missing_uids: set[str]
) -> dict[str, Any]:
    result = deepcopy(package)
    result.pop("dataSets", None)
    result.pop("dataEntryForms", None)
    elements = result.get("dataElements")
    if isinstance(elements, list):
        result["dataElements"] = [
            item
            for item in elements
            if isinstance(item, dict) and item.get("id") in missing_uids
        ]
    return result


def _target_data_set_details(
    target: DHIS2Client,
    data_set_id: str | None,
) -> dict[str, Any] | None:
    if not data_set_id:
        return None
    reader = getattr(target, "data_set_details", None)
    if not callable(reader):
        return None
    details = reader(data_set_id)
    return details if isinstance(details, dict) else None


def _data_set_membership_package(
    target: DHIS2Client,
    *,
    data_set_id: str,
    data_element_ids: set[str],
) -> dict[str, Any]:
    """Build an additive, valid Data Set update from the target's own identity."""
    details = _target_data_set_details(target, data_set_id)
    if details is None:
        raise PipelineError(
            f"Cannot update target Data Set {data_set_id}: its metadata could not be read."
        )
    memberships = {
        str(item["dataElement"]["id"]): item
        for item in _target_data_set_element_memberships(details, data_set_id)
    }
    for element_id in data_element_ids:
        memberships.setdefault(
            element_id,
            {
                "dataSet": {"id": data_set_id},
                "dataElement": {"id": element_id},
            },
        )
    data_set_payload = _target_data_set_identity_payload(details, data_set_id)
    data_set_payload["dataSetElements"] = [
        memberships[element_id]
        for element_id in sorted(memberships)
    ]
    data_set_payload["organisationUnits"] = (
        _target_data_set_organisation_unit_references(details, data_set_id)
    )
    return {"dataSets": [data_set_payload]}


def _target_data_set_element_memberships(
    details: dict[str, Any] | None,
    data_set_id: str,
) -> list[dict[str, Any]]:
    if details is None or not isinstance(details.get("dataSetElements"), list):
        raise PipelineError(
            f"Cannot update target Data Set {data_set_id}: its dataSetElements "
            "collection was not returned, so existing memberships cannot be preserved."
        )
    memberships: dict[str, dict[str, Any]] = {}
    for item in details["dataSetElements"]:
        if not isinstance(item, dict):
            continue
        element_id = _reference_uid(item.get("dataElement"))
        if not element_id:
            continue
        preserved = deepcopy(item)
        preserved["dataSet"] = {"id": data_set_id}
        preserved["dataElement"] = {"id": element_id}
        memberships[element_id] = preserved
    return [memberships[element_id] for element_id in sorted(memberships)]


def _target_data_set_organisation_unit_references(
    details: dict[str, Any] | None,
    data_set_id: str,
) -> list[dict[str, str]]:
    if details is None or not isinstance(details.get("organisationUnits"), list):
        raise PipelineError(
            f"Cannot update target Data Set {data_set_id}: its organisationUnits "
            "collection was not returned, so existing assignments cannot be preserved."
        )
    return [
        {"id": uid}
        for uid in sorted(_reference_ids(details["organisationUnits"]))
    ]


def _target_data_set_identity_payload(
    details: dict[str, Any] | None,
    data_set_id: str,
) -> dict[str, Any]:
    """Copy only the target identity fields required for a safe metadata update."""
    if details is None:
        raise PipelineError(
            f"Cannot update target Data Set {data_set_id}: its metadata could not be read."
        )
    returned_id = str(details.get("id") or data_set_id)
    if returned_id != data_set_id:
        raise PipelineError(
            f"Target Data Set lookup returned UID {returned_id}, expected {data_set_id}."
        )
    required_identity = ("name", "shortName", "periodType")
    missing_identity = [
        field for field in required_identity if not details.get(field)
    ]
    if missing_identity:
        raise PipelineError(
            f"Cannot update target Data Set {data_set_id}: required target properties are "
            f"missing: {', '.join(missing_identity)}."
        )
    payload: dict[str, Any] = {
        "id": data_set_id,
        "name": details["name"],
        "shortName": details["shortName"],
        "periodType": details["periodType"],
    }
    if details.get("code"):
        payload["code"] = details["code"]
    category_combo_id = _reference_uid(details.get("categoryCombo"))
    if category_combo_id:
        payload["categoryCombo"] = {"id": category_combo_id}
    return payload


def resolve_transfer_context(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    start: str | None,
    end: str | None,
    start_period: str | None,
    end_period: str | None,
    years_back: int | None,
    as_of: str | None,
    allow_metadata_repair: bool = False,
    allow_incomplete_target: bool = False,
) -> TransferContext:
    job = scenario.get_job(job_id)
    resolved_start, resolved_end = _resolve_window(
        job,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
    )
    if not resolved_start or not resolved_end:
        raise PipelineError(
            f"Transfer job '{job.id}' needs a date window. Configure defaults or pass "
            "--start/--end, --start-period/--end-period, or --years-back."
        )

    source_data_set = source.resolve_data_set(job.source_data_set)
    source_elements = tuple(
        source.data_elements_for_data_set(data_set_id=source_data_set.id, page_size=500)
    )
    if not source_elements:
        raise PipelineError(
            f"Source Data Set '{job.source_data_set}' has no linked Data Elements."
        )
    file_elements = [
        str(item.get("code") or item.get("id"))
        for item in source_elements
        if item.get("valueType") == "FILE_RESOURCE"
    ]
    if file_elements:
        raise PipelineError(
            "Data Set transfer does not copy DHIS2 file resources. Unsupported Data Elements: "
            + ", ".join(file_elements[:10])
        )
    dynamic_conflicts: dict[str, tuple[str, ...]] = {}
    if job.organisation_units.synchronization.conflict_policy == "exclude":
        try:
            dynamic_conflicts = _load_organisation_unit_quarantine(
                scenario,
                source,
                target,
                job,
                source_data_set,
            )
        except PipelineError as exc:
            if "quarantine manifest" not in str(exc):
                raise
            reconcile_target_organisation_units(
                scenario,
                source,
                target,
                job_id=job.id,
                apply=False,
            )
            dynamic_conflicts = _load_organisation_unit_quarantine(
                scenario,
                source,
                target,
                job,
                source_data_set,
            )
    selected_source_units, excluded_source_units = _resolve_source_unit_selection(
        source,
        source_data_set,
        job,
        additional_excluded_ids=set(dynamic_conflicts),
    )
    source_units = tuple(selected_source_units)
    if not source_units:
        raise PipelineError(
            f"Source Data Set '{job.source_data_set}' has no organisation units available "
            "for export. Configure organisation_units.source_roots if needed."
        )
    target_units = tuple(_resolve_target_units(target, source_units, job))

    target_data_set = target.find_data_set(job.target_data_set)
    if (
        target_data_set is None
        and scenario.metadata.identity_strategy == "preserve_uid"
        and job.target_data_set != source_data_set.id
    ):
        target_data_set = target.find_data_set(source_data_set.id)
    target_elements: tuple[dict[str, Any], ...] = ()
    bootstrap_required = target_data_set is None
    if bootstrap_required:
        if scenario.metadata.mode != "create_if_missing":
            raise PipelineError(
                f"Target Data Set '{job.target_data_set}' does not exist and target.metadata.mode "
                "is require_existing."
            )
        if job.target_data_set not in {
            source_data_set.id,
            source_data_set.code,
            job.source_data_set,
        }:
            raise PipelineError(
                "create_if_missing with preserve_uid requires target_data_set to use the same "
                "UID or code as the source Data Set."
            )
    else:
        target_elements = tuple(
            target.data_elements_for_data_set(data_set_id=target_data_set.id, page_size=500)
        )
    if bootstrap_required:
        target_elements = tuple(
            _matching_metadata_objects(
                source_elements,
                target.data_elements(page_size=1000),
                job.id_schemes.data_elements,
                "target Data Elements",
            )
        )

    context = TransferContext(
        job=job,
        source_data_set=source_data_set,
        target_data_set=target_data_set,
        source_data_elements=source_elements,
        target_data_elements=target_elements,
        source_organisation_units=source_units,
        target_organisation_units=target_units,
        start=resolved_start,
        end=resolved_end,
        metadata_bootstrap_required=bootstrap_required,
        excluded_source_organisation_units=tuple(excluded_source_units),
        detected_organisation_unit_conflicts=tuple(
            _organisation_unit_conflict_payload(dynamic_conflicts)
        ),
    )
    repairs = _validate_data_set_compatibility(
        context,
        allow_metadata_repair=allow_metadata_repair,
        allow_missing_target_elements=allow_incomplete_target,
    )
    return replace(context, category_combo_repairs=repairs)


def _validate_data_set_compatibility(
    context: TransferContext,
    *,
    allow_metadata_repair: bool = False,
    allow_missing_target_elements: bool = False,
) -> tuple[dict[str, Any], ...]:
    expected = {
        "day": "Daily",
        "week": "Weekly",
        "month": "Monthly",
        "year": "Yearly",
    }[context.job.period_type]
    if context.source_data_set.period_type and context.source_data_set.period_type != expected:
        raise PipelineError(
            f"Source Data Set periodType is '{context.source_data_set.period_type}', expected "
            f"'{expected}' for transfer job '{context.job.id}'."
        )
    if context.target_data_set and context.target_data_set.period_type:
        if context.target_data_set.period_type != expected:
            raise PipelineError(
                f"Target Data Set periodType is '{context.target_data_set.period_type}', expected "
                f"'{expected}' for transfer job '{context.job.id}'."
            )
    if not context.target_data_elements:
        return ()
    scheme = context.job.id_schemes.data_elements
    source_keys = _metadata_keys(context.source_data_elements, scheme, "source Data Elements")
    target_keys = _metadata_keys(context.target_data_elements, scheme, "target Data Elements")
    missing = sorted(source_keys - target_keys)
    if context.target_data_set and missing and not allow_missing_target_elements:
        preview = ", ".join(missing[:10])
        raise PipelineError(
            f"Target Data Set is missing {len(missing)} Data Elements using {scheme.upper()} "
            f"mapping: {preview}"
        )
    source_by_key = _metadata_by_key(context.source_data_elements, scheme)
    target_by_key = _metadata_by_key(context.target_data_elements, scheme)
    conflicts: list[str] = []
    repairs: list[dict[str, Any]] = []
    for key in sorted(source_keys & target_keys):
        source_element = source_by_key[key]
        target_element = target_by_key[key]
        for field in ("valueType", "domainType"):
            source_value = source_element.get(field)
            target_value = target_element.get(field)
            if source_value and target_value and source_value != target_value:
                conflicts.append(
                    f"{key} {field}: source={source_value}, target={target_value}"
                )
        category_scheme = context.job.id_schemes.category_option_combos
        source_category = _reference_key(source_element.get("categoryCombo"), category_scheme)
        target_category = _reference_key(target_element.get("categoryCombo"), category_scheme)
        if source_category and target_category and source_category != target_category:
            if (
                allow_metadata_repair
                and context.metadata_bootstrap_required
                and category_scheme == "uid"
                and _is_default_metadata_object(target_element.get("categoryCombo"))
                and not _is_default_metadata_object(source_element.get("categoryCombo"))
            ):
                repairs.append(
                    {
                        "key": key,
                        "dataElement": str(source_element.get("id") or key),
                        "name": source_element.get("name"),
                        "sourceCategoryCombo": source_category,
                        "targetCategoryCombo": target_category,
                    }
                )
            else:
                conflicts.append(
                    f"{key} categoryCombo: source={source_category}, target={target_category}"
                )
    if conflicts:
        raise PipelineError(
            "Source and target Data Elements are incompatible: " + "; ".join(conflicts[:10])
        )
    return tuple(repairs)


def _resolve_source_units(
    source: DHIS2Client, data_set: DataSetRef, job: TransferJobConfig
) -> list[dict[str, Any]]:
    selected, _ = _resolve_source_unit_selection(source, data_set, job)
    return selected


def _resolve_source_unit_selection(
    source: DHIS2Client,
    data_set: DataSetRef,
    job: TransferJobConfig,
    *,
    additional_excluded_ids: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    policy = job.organisation_units
    if policy.discovery == "configured_roots":
        units = [source.resolve_organisation_unit(item) for item in policy.source_roots]
    else:
        units = source.organisation_units_for_data_set(data_set.id)
    excluded_ids = {item.uid for item in policy.exclusions} | set(
        additional_excluded_ids or ()
    )
    if not excluded_ids:
        return units, []

    available_hierarchy_ids: set[str] = set()
    for unit in units:
        if not isinstance(unit, dict):
            continue
        uid = str(unit.get("id") or "")
        if uid:
            available_hierarchy_ids.add(uid)
        available_hierarchy_ids.update(_organisation_unit_path_ids(unit))
    unknown_ids = sorted(excluded_ids - available_hierarchy_ids)
    if unknown_ids:
        raise PipelineError(
            "Configured organisation unit exclusions are neither selected source "
            "units nor ancestors of the source selection for job "
            f"{job.id}: {', '.join(unknown_ids[:10])}"
        )

    selected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for unit in units:
        if not isinstance(unit, dict):
            continue
        unit_hierarchy_ids = set(_organisation_unit_path_ids(unit))
        uid = str(unit.get("id") or "")
        if uid:
            unit_hierarchy_ids.add(uid)
        if unit_hierarchy_ids & excluded_ids:
            excluded.append(unit)
        else:
            selected.append(unit)
    return selected, excluded


def _organisation_unit_path_ids(unit: dict[str, Any]) -> list[str]:
    path = unit.get("path")
    if not isinstance(path, str):
        return []
    return [part for part in path.split("/") if part]


def _organisation_unit_parent_uid(unit: dict[str, Any]) -> str | None:
    explicit = _reference_uid(unit.get("parent"))
    if explicit:
        return explicit
    path_ids = _organisation_unit_path_ids(unit)
    return path_ids[-2] if len(path_ids) > 1 else None


def _organisation_unit_create_payload(
    source: dict[str, Any],
    *,
    parent_uid: str | None,
    preserve_geometry: bool,
) -> dict[str, Any]:
    uid = str(source.get("id") or "")
    name = str(source.get("name") or "")
    if not uid or not name:
        raise PipelineError("A source organisation unit is missing its UID or name.")
    payload: dict[str, Any] = {
        "id": uid,
        "name": name,
        "shortName": str(source.get("shortName") or _limited_text(name, 50)),
        "openingDate": source.get("openingDate") or "1970-01-01",
    }
    for field in (
        "code",
        "description",
        "closedDate",
        "comment",
    ):
        if source.get(field) not in {None, ""}:
            payload[field] = source[field]
    if preserve_geometry and isinstance(source.get("geometry"), dict):
        payload["geometry"] = deepcopy(source["geometry"])
    if parent_uid:
        payload["parent"] = {"id": parent_uid}
    return payload


def _resolve_target_units(
    target: DHIS2Client,
    source_units: tuple[dict[str, Any], ...],
    job: TransferJobConfig,
) -> list[dict[str, Any]]:
    scheme = job.id_schemes.organisation_units
    field = "id" if scheme == "uid" else "code"
    source_keys = _metadata_keys(source_units, scheme, "source organisation units")
    target_units = target.organisation_units(page_size=1000)
    target_by_key: dict[str, dict[str, Any]] = {}
    duplicate_keys: set[str] = set()
    for target_unit in target_units:
        target_key = target_unit.get(field)
        if not isinstance(target_key, str) or target_key not in source_keys:
            continue
        if target_key in target_by_key:
            duplicate_keys.add(target_key)
            continue
        target_by_key[target_key] = target_unit

    missing = sorted(source_keys - target_by_key.keys())
    incompatible = [f"duplicate {field} {key}" for key in sorted(duplicate_keys)]

    if missing or incompatible:
        details: list[str] = []
        if missing:
            details.append(
                f"missing on target ({len(missing)}): {', '.join(missing[:10])}"
            )
        if incompatible:
            details.append(
                f"different on target ({len(incompatible)}): "
                + ", ".join(incompatible[:10])
            )
        raise PipelineError(
            "Organisation units differ between source and target using "
            f"{scheme.upper()} mapping; transfer blocked. " + "; ".join(details)
        )
    return [target_by_key[str(source_unit[field])] for source_unit in source_units]


def _matching_metadata_objects(
    source_items: tuple[dict[str, Any], ...],
    target_items: list[dict[str, Any]],
    scheme: str,
    label: str,
) -> list[dict[str, Any]]:
    field = "id" if scheme == "uid" else "code"
    source_keys = _metadata_keys(source_items, scheme, "source Data Elements")
    matches: dict[str, dict[str, Any]] = {}
    duplicates: set[str] = set()
    for item in target_items:
        key = item.get(field)
        if not isinstance(key, str) or key not in source_keys:
            continue
        if key in matches:
            duplicates.add(key)
        else:
            matches[key] = item
    if duplicates:
        raise PipelineError(
            f"{label} contains duplicate {field} values: "
            + ", ".join(sorted(duplicates)[:10])
        )
    return [matches[key] for key in sorted(matches)]


def _build_metadata_repair_plan(
    target: DHIS2Client,
    package: dict[str, Any],
    context: TransferContext,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    if context.job.id_schemes.data_elements != "uid":
        raise PipelineError("Metadata repair requires Data Element UID mapping.")
    if context.job.id_schemes.category_option_combos != "uid":
        raise PipelineError("Metadata repair requires CategoryOptionCombo UID mapping.")

    repair_ids = {
        str(item["dataElement"])
        for item in context.category_combo_repairs
        if item.get("dataElement")
    }
    source_elements = {
        str(item["id"]): item
        for item in package.get("dataElements", [])
        if isinstance(item, dict) and item.get("id")
    }
    target_elements = {
        str(item["id"]): item
        for item in context.target_data_elements
        if isinstance(item, dict) and item.get("id")
    }
    missing_source = sorted(repair_ids - source_elements.keys())
    missing_target = sorted(repair_ids - target_elements.keys())
    if missing_source or missing_target:
        raise PipelineError(
            "Metadata repair cannot resolve every affected Data Element. "
            f"Missing source={missing_source[:10]}, target={missing_target[:10]}."
        )

    assigned: list[str] = []
    identity_conflicts: list[str] = []
    for data_element_id in sorted(repair_ids):
        source_element = source_elements[data_element_id]
        target_element = target_elements[data_element_id]
        memberships = target_element.get("dataSetElements")
        if isinstance(memberships, list) and memberships:
            names = [
                str((item.get("dataSet") or {}).get("id") or "unknown")
                for item in memberships
                if isinstance(item, dict)
            ]
            assigned.append(f"{data_element_id} ({', '.join(names[:3])})")
        for field in (
            "name",
            "shortName",
            "valueType",
            "domainType",
            "aggregationType",
        ):
            source_value = source_element.get(field)
            target_value = target_element.get(field)
            if source_value != target_value:
                identity_conflicts.append(
                    f"{data_element_id} {field}: source={source_value!r}, "
                    f"target={target_value!r}"
                )
        source_code = source_element.get("code")
        target_code = target_element.get("code")
        if source_code and target_code and source_code != target_code:
            identity_conflicts.append(
                f"{data_element_id} code: source={source_code!r}, target={target_code!r}"
            )
    if assigned:
        raise PipelineError(
            "Metadata repair blocked: affected target Data Elements are already assigned "
            "to Data Sets: " + "; ".join(assigned[:10])
        )
    if identity_conflicts:
        raise PipelineError(
            "Metadata repair blocked: source and target Data Element identities differ: "
            + "; ".join(identity_conflicts[:10])
        )

    category_combo_ids = sorted(
        {
            str(item["sourceCategoryCombo"])
            for item in context.category_combo_repairs
            if item.get("sourceCategoryCombo")
        }
    )
    source_category_combos = {
        str(item["id"]): item
        for item in package.get("categoryCombos", [])
        if isinstance(item, dict) and item.get("id") in category_combo_ids
    }
    if set(category_combo_ids) != source_category_combos.keys():
        missing = sorted(set(category_combo_ids) - source_category_combos.keys())
        raise PipelineError(
            "Metadata repair package is missing source CategoryCombos: "
            + ", ".join(missing)
        )
    target_category_combos = {
        str(item["id"]): item
        for item in target.metadata_objects_by_ids(
            resource="categoryCombos",
            root_key="categoryCombos",
            object_ids=category_combo_ids,
            fields="id,code,name,dataDimensionType,categories[id]",
        )
        if item.get("id")
    }
    missing_target_combos = sorted(
        set(category_combo_ids) - target_category_combos.keys()
    )
    if missing_target_combos:
        raise PipelineError(
            "Metadata repair only supports incomplete existing CategoryCombos. Missing on "
            "target: " + ", ".join(missing_target_combos)
        )

    category_ids: set[str] = set()
    combo_conflicts: list[str] = []
    repaired_combo_ids: list[str] = []
    reused_combo_ids: list[str] = []
    for combo_id in category_combo_ids:
        source_combo = source_category_combos[combo_id]
        target_combo = target_category_combos[combo_id]
        _assert_same_metadata_label(
            source_combo,
            target_combo,
            object_type="CategoryCombo",
        )
        source_categories = _reference_ids(source_combo.get("categories"))
        target_categories = _reference_ids(target_combo.get("categories"))
        category_ids.update(source_categories)
        if target_categories == source_categories:
            reused_combo_ids.append(combo_id)
        elif not target_categories and source_categories:
            repaired_combo_ids.append(combo_id)
        else:
            combo_conflicts.append(
                f"{combo_id}: source={sorted(source_categories)}, "
                f"target={sorted(target_categories)}"
            )
    if combo_conflicts:
        raise PipelineError(
            "Metadata repair blocked: CategoryCombo structures differ instead of being "
            "empty on target: " + "; ".join(combo_conflicts[:10])
        )

    _validate_reused_categories(target, package, category_ids)

    roots = target.user_organisation_unit_roots()
    root_ids = [str(item["id"]) for item in roots if item.get("id")]
    if not root_ids:
        raise PipelineError(
            "Metadata repair blocked: the target account has no organisation-unit root "
            "available for the raw data audit."
        )
    raw_values = target.find_data_values(
        data_element_ids=sorted(repair_ids),
        organisation_unit_ids=root_ids,
        start=_RECONCILIATION_AUDIT_START,
        end=_RECONCILIATION_AUDIT_END,
        include_deleted=True,
        limit=1,
    )
    if raw_values:
        sample = raw_values[0]
        raise PipelineError(
            "Metadata repair blocked: at least one raw or deleted target data value exists "
            "for an affected Data Element. Sample key: "
            f"dataElement={sample.get('dataElement')}, period={sample.get('period')}, "
            f"orgUnit={sample.get('orgUnit')}."
        )

    source_option_combos = [
        deepcopy(item)
        for item in package.get("categoryOptionCombos", [])
        if isinstance(item, dict)
        and _reference_uid(item.get("categoryCombo")) in category_combo_ids
    ]
    if not source_option_combos:
        raise PipelineError(
            "Metadata repair package has no valid CategoryOptionCombos for the affected "
            "CategoryCombos."
        )

    structure_package = {
        "categoryCombos": [
            deepcopy(source_category_combos[item]) for item in repaired_combo_ids
        ]
    }
    selected_source_elements = [
        deepcopy(source_elements[item]) for item in sorted(repair_ids)
    ]
    option_set_dependencies, option_set_report = (
        _option_set_dependencies_for_data_elements(
            package,
            selected_source_elements,
        )
    )
    assignment_package = {
        **option_set_dependencies,
        "categoryOptionCombos": source_option_combos,
        "dataElements": selected_source_elements,
    }
    rollback = {
        "warning": (
            "Evidence only. Do not import this file directly; use the recorded references "
            "to construct reviewed JSON Patch rollback requests."
        ),
        "target": target.base_url,
        "categoryCombos": [
            {
                "id": item,
                "categories": [
                    {"id": child}
                    for child in sorted(
                        _reference_ids(target_category_combos[item].get("categories"))
                    )
                ],
            }
            for item in repaired_combo_ids
        ],
        "dataElements": [
            {
                "id": item,
                "categoryCombo": deepcopy(target_elements[item].get("categoryCombo")),
            }
            for item in sorted(repair_ids)
        ],
    }
    report = {
        "categoryComboIds": category_combo_ids,
        "categoryCombosToUpdate": len(repaired_combo_ids),
        "categoryCombosAlreadyCompatible": len(reused_combo_ids),
        "categoryOptionCombosToCreate": len(source_option_combos),
        "optionSets": option_set_report,
        "dataElementsToUpdate": len(repair_ids),
        "dataSetMembershipConflicts": 0,
        "rawDataAudit": {
            "start": _RECONCILIATION_AUDIT_START,
            "end": _RECONCILIATION_AUDIT_END,
            "includeDeleted": True,
            "organisationUnitRoots": [
                {"id": item.get("id"), "name": item.get("name")} for item in roots
            ],
            "matchingValueCount": 0,
        },
    }
    return structure_package, assignment_package, report, rollback


def _option_set_dependencies_for_data_elements(
    package: dict[str, Any],
    data_elements: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, int]]:
    """Select the complete OptionSet dependency graph for affected Data Elements."""
    option_set_ids = {
        option_set_id
        for item in data_elements
        if (option_set_id := _reference_uid(item.get("optionSet")))
    }
    if not option_set_ids:
        return {"options": [], "optionSets": []}, {
            "referenced": 0,
            "optionsReferenced": 0,
        }

    source_option_sets = {
        str(item["id"]): item
        for item in package.get("optionSets", [])
        if isinstance(item, dict) and item.get("id") in option_set_ids
    }
    missing_option_sets = sorted(option_set_ids - source_option_sets.keys())
    if missing_option_sets:
        raise PipelineError(
            "Metadata repair package is missing source OptionSets referenced by "
            "affected Data Elements: " + ", ".join(missing_option_sets)
        )

    option_ids: set[str] = set()
    for option_set in source_option_sets.values():
        option_ids.update(_reference_ids(option_set.get("options")))
    source_options = {
        str(item["id"]): item
        for item in package.get("options", [])
        if isinstance(item, dict) and item.get("id") in option_ids
    }
    missing_options = sorted(option_ids - source_options.keys())
    if missing_options:
        raise PipelineError(
            "Metadata repair package is missing Options referenced by affected "
            "OptionSets: " + ", ".join(missing_options[:10])
        )
    return {
        "options": [deepcopy(source_options[item]) for item in sorted(option_ids)],
        "optionSets": [
            deepcopy(source_option_sets[item]) for item in sorted(option_set_ids)
        ],
    }, {
        "referenced": len(option_set_ids),
        "optionsReferenced": len(option_ids),
    }


def _validate_reused_categories(
    target: DHIS2Client,
    package: dict[str, Any],
    category_ids: set[str],
) -> None:
    source_categories = {
        str(item["id"]): item
        for item in package.get("categories", [])
        if isinstance(item, dict) and item.get("id") in category_ids
    }
    if source_categories.keys() != category_ids:
        missing = sorted(category_ids - source_categories.keys())
        raise PipelineError(
            "Metadata repair package is missing Categories: " + ", ".join(missing)
        )
    target_categories = {
        str(item["id"]): item
        for item in target.metadata_objects_by_ids(
            resource="categories",
            root_key="categories",
            object_ids=sorted(category_ids),
            fields=(
                "id,code,name,shortName,dataDimension,dataDimensionType,"
                "categoryOptions[id]"
            ),
        )
        if item.get("id")
    }
    if target_categories.keys() != category_ids:
        missing = sorted(category_ids - target_categories.keys())
        raise PipelineError(
            "Metadata repair requires existing target Categories: " + ", ".join(missing)
        )

    option_ids: set[str] = set()
    for category_id in sorted(category_ids):
        source_category = source_categories[category_id]
        target_category = target_categories[category_id]
        _assert_same_metadata_label(
            source_category,
            target_category,
            object_type="Category",
        )
        source_options = _reference_ids(source_category.get("categoryOptions"))
        target_options = _reference_ids(target_category.get("categoryOptions"))
        if source_options != target_options:
            raise PipelineError(
                f"Metadata repair blocked: Category {category_id} options differ: "
                f"source={sorted(source_options)}, target={sorted(target_options)}."
            )
        option_ids.update(source_options)

    source_options = {
        str(item["id"]): item
        for item in package.get("categoryOptions", [])
        if isinstance(item, dict) and item.get("id") in option_ids
    }
    target_options = {
        str(item["id"]): item
        for item in target.metadata_objects_by_ids(
            resource="categoryOptions",
            root_key="categoryOptions",
            object_ids=sorted(option_ids),
            fields="id,code,name,shortName",
        )
        if item.get("id")
    }
    if source_options.keys() != option_ids or target_options.keys() != option_ids:
        missing_source = sorted(option_ids - source_options.keys())
        missing_target = sorted(option_ids - target_options.keys())
        raise PipelineError(
            "Metadata repair requires matching CategoryOptions. "
            f"Missing source={missing_source}, target={missing_target}."
        )
    for option_id in sorted(option_ids):
        _assert_same_metadata_label(
            source_options[option_id],
            target_options[option_id],
            object_type="CategoryOption",
        )


def _prepare_assignment_package_for_target(
    target: DHIS2Client,
    assignment_package: dict[str, Any],
    category_combo_ids: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    result = deepcopy(assignment_package)
    source_combos = [
        item
        for item in result.get("categoryOptionCombos", [])
        if isinstance(item, dict) and item.get("id")
    ]
    source_by_uid = {str(item["id"]): item for item in source_combos}
    source_by_semantics = {
        _category_option_combo_key(item): str(item["id"]) for item in source_combos
    }
    if len(source_by_semantics) != len(source_combos):
        raise PipelineError(
            "Source repair package contains duplicate CategoryOptionCombo semantics."
        )

    target_combos = target.category_option_combos_for_category_combos(
        category_combo_ids
    )
    reused: set[str] = set()
    conflicts: list[str] = []
    for target_combo in target_combos:
        target_uid = str(target_combo.get("id") or "")
        semantics = _category_option_combo_key(target_combo)
        source_uid = source_by_semantics.get(semantics)
        if source_uid is None:
            conflicts.append(
                f"unexpected target CategoryOptionCombo {target_uid} with key={semantics}"
            )
        elif source_uid != target_uid:
            conflicts.append(
                f"same category/options use different UID: source={source_uid}, "
                f"target={target_uid}"
            )
        else:
            reused.add(source_uid)
    for uid, source_combo in source_by_uid.items():
        same_uid = next(
            (item for item in target_combos if str(item.get("id")) == uid),
            None,
        )
        if same_uid is not None and _category_option_combo_key(same_uid) != (
            _category_option_combo_key(source_combo)
        ):
            conflicts.append(f"CategoryOptionCombo {uid} has different semantics on target")
    if conflicts:
        raise PipelineError(
            "Metadata repair blocked after CategoryCombo update: "
            + "; ".join(conflicts[:10])
        )

    result["categoryOptionCombos"] = [
        item for item in source_combos if str(item["id"]) not in reused
    ]
    return result, {
        "existingCompatible": len(reused),
        "toCreate": len(result["categoryOptionCombos"]),
    }


def _prepare_option_set_dependencies_for_target(
    target: DHIS2Client,
    assignment_package: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, int]]:
    """Reuse only semantically compatible OptionSets and Options already on Y."""
    result = deepcopy(assignment_package)
    source_option_sets = {
        str(item["id"]): item
        for item in result.get("optionSets", [])
        if isinstance(item, dict) and item.get("id")
    }
    source_options = {
        str(item["id"]): item
        for item in result.get("options", [])
        if isinstance(item, dict) and item.get("id")
    }
    if not source_option_sets:
        return result, {
            "existingCompatibleOptionSets": 0,
            "optionSetsToCreate": 0,
            "existingCompatibleOptions": 0,
            "optionsToCreate": 0,
        }

    target_option_sets = {
        str(item["id"]): item
        for item in target.metadata_objects_by_ids(
            resource="optionSets",
            root_key="optionSets",
            object_ids=sorted(source_option_sets),
            fields="id,code,name,valueType,options[id]",
        )
        if item.get("id")
    }
    conflicts: list[str] = []
    compatible_option_sets: set[str] = set()
    for uid in sorted(source_option_sets.keys() & target_option_sets.keys()):
        source_item = source_option_sets[uid]
        target_item = target_option_sets[uid]
        item_conflicts: list[str] = []
        for field in ("name", "valueType"):
            if source_item.get(field) != target_item.get(field):
                item_conflicts.append(
                    f"OptionSet {uid} {field}: source={source_item.get(field)!r}, "
                    f"target={target_item.get(field)!r}"
                )
        source_code = source_item.get("code")
        target_code = target_item.get("code")
        if source_code and target_code and source_code != target_code:
            item_conflicts.append(
                f"OptionSet {uid} code: source={source_code!r}, target={target_code!r}"
            )
        source_children = _reference_ids(source_item.get("options"))
        target_children = _reference_ids(target_item.get("options"))
        if source_children != target_children:
            item_conflicts.append(
                f"OptionSet {uid} options: source={sorted(source_children)}, "
                f"target={sorted(target_children)}"
            )
        if item_conflicts:
            conflicts.extend(item_conflicts)
        else:
            compatible_option_sets.add(uid)

    target_options = {
        str(item["id"]): item
        for item in target.metadata_objects_by_ids(
            resource="options",
            root_key="options",
            object_ids=sorted(source_options),
            fields="id,code,name,sortOrder",
        )
        if item.get("id")
    }
    compatible_options: set[str] = set()
    for uid in sorted(source_options.keys() & target_options.keys()):
        source_item = source_options[uid]
        target_item = target_options[uid]
        item_conflicts: list[str] = []
        if source_item.get("name") != target_item.get("name"):
            item_conflicts.append(
                f"Option {uid} name: source={source_item.get('name')!r}, "
                f"target={target_item.get('name')!r}"
            )
        source_sort_order = source_item.get("sortOrder")
        target_sort_order = target_item.get("sortOrder")
        if (
            source_sort_order is not None
            and target_sort_order is not None
            and source_sort_order != target_sort_order
        ):
            item_conflicts.append(
                f"Option {uid} sortOrder: source={source_sort_order!r}, "
                f"target={target_sort_order!r}"
            )
        source_code = source_item.get("code")
        target_code = target_item.get("code")
        if source_code and target_code and source_code != target_code:
            item_conflicts.append(
                f"Option {uid} code: source={source_code!r}, target={target_code!r}"
            )
        if item_conflicts:
            conflicts.extend(item_conflicts)
        else:
            compatible_options.add(uid)
    if conflicts:
        raise PipelineError(
            "Metadata repair blocked by incompatible OptionSet dependencies on target: "
            + "; ".join(conflicts[:10])
        )

    result["optionSets"] = [
        item
        for uid, item in source_option_sets.items()
        if uid not in compatible_option_sets
    ]
    result["options"] = [
        item for uid, item in source_options.items() if uid not in compatible_options
    ]
    return result, {
        "existingCompatibleOptionSets": len(compatible_option_sets),
        "optionSetsToCreate": len(result["optionSets"]),
        "existingCompatibleOptions": len(compatible_options),
        "optionsToCreate": len(result["options"]),
    }


def _assert_same_metadata_label(
    source: dict[str, Any],
    target: dict[str, Any],
    *,
    object_type: str,
) -> None:
    object_id = source.get("id")
    for field in ("name", "shortName"):
        source_value = source.get(field)
        target_value = target.get(field)
        if source_value is not None and target_value is not None and source_value != target_value:
            raise PipelineError(
                f"Metadata repair blocked: {object_type} {object_id} {field} differs: "
                f"source={source_value!r}, target={target_value!r}."
            )
    source_code = source.get("code")
    target_code = target.get("code")
    if source_code and target_code and source_code != target_code:
        raise PipelineError(
            f"Metadata repair blocked: {object_type} {object_id} code differs: "
            f"source={source_code!r}, target={target_code!r}."
        )


def _reference_ids(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {
        str(item["id"])
        for item in value
        if isinstance(item, dict) and item.get("id")
    }


def _category_option_combo_key(value: dict[str, Any]) -> tuple[str, tuple[str, ...]]:
    return (
        str(_reference_uid(value.get("categoryCombo")) or ""),
        tuple(sorted(_reference_ids(value.get("categoryOptions")))),
    )


def _restrict_data_set_organisation_units(
    package: dict[str, Any],
    *,
    data_set_id: str,
    allowed_uids: set[str],
) -> None:
    """Keep a bootstrap package aligned with the effective transfer selection."""
    data_sets = package.get("dataSets")
    if not isinstance(data_sets, list):
        return
    for data_set in data_sets:
        if not isinstance(data_set, dict) or str(data_set.get("id") or "") != data_set_id:
            continue
        data_set["organisationUnits"] = [
            {"id": uid} for uid in sorted(allowed_uids)
        ]


def _prepare_metadata_package(
    source_package: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    package = deepcopy(source_package)
    removed_defaults: dict[str, int] = {}
    default_collections = (
        "categories",
        "categoryOptions",
        "categoryCombos",
    )
    for collection in default_collections:
        items = package.get(collection)
        if not isinstance(items, list):
            continue
        kept = [item for item in items if not _is_default_metadata_object(item)]
        removed_defaults[collection] = len(items) - len(kept)
        package[collection] = kept

    data_element_combos = {
        str(item.get("id")): _reference_uid(item.get("categoryCombo"))
        for item in package.get("dataElements", [])
        if isinstance(item, dict) and item.get("id")
    }
    option_combo_parents = {
        str(item.get("id")): _reference_uid(item.get("categoryCombo"))
        for item in package.get("categoryOptionCombos", [])
        if isinstance(item, dict) and item.get("id")
    }
    removed_operands = 0
    for data_set in package.get("dataSets", []):
        if not isinstance(data_set, dict):
            continue
        operands = data_set.get("compulsoryDataElementOperands")
        if not isinstance(operands, list):
            continue
        valid_operands: list[dict[str, Any]] = []
        for operand in operands:
            if not isinstance(operand, dict):
                continue
            data_element_id = _reference_uid(operand.get("dataElement"))
            option_combo_id = _reference_uid(operand.get("categoryOptionCombo"))
            element_combo = data_element_combos.get(data_element_id or "")
            option_combo = option_combo_parents.get(option_combo_id or "")
            if element_combo and option_combo and element_combo != option_combo:
                removed_operands += 1
                continue
            valid_operands.append(operand)
        data_set["compulsoryDataElementOperands"] = valid_operands

    removed_orphan_combos = _prune_category_option_combos(package)
    return package, {
        "removedDefaultObjects": {
            key: count for key, count in removed_defaults.items() if count
        },
        "removedIncompatibleCompulsoryOperands": removed_operands,
        "removedOrphanCategoryOptionCombos": removed_orphan_combos,
    }


def _validate_metadata_package(
    target: DHIS2Client,
    package: dict[str, Any],
    *,
    import_strategy: str,
    atomic_mode: str,
    skip_sharing: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, int]]:
    candidate = deepcopy(package)
    reused: dict[str, int] = {}
    for _attempt in range(3):
        try:
            validation = target.import_metadata(
                candidate,
                commit=False,
                import_strategy=import_strategy,
                atomic_mode=atomic_mode,
                identifier="UID",
                skip_sharing=skip_sharing,
            )
            return candidate, validation, reused
        except HttpRequestError as exc:
            if import_strategy != "CREATE" or exc.status_code != 409:
                raise
            existing_uids = _existing_metadata_uids(exc.body)
            if not existing_uids:
                raise
            candidate, removed = _remove_existing_metadata_objects(
                candidate, existing_uids
            )
            if not removed:
                raise
            for collection, count in removed.items():
                reused[collection] = reused.get(collection, 0) + count
    raise PipelineError("Metadata validation did not converge after reusing existing objects.")


def _existing_metadata_uids(body: str) -> set[str]:
    try:
        payload = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        return set()
    uids: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("errorCode") == "E5000":
                main_id = value.get("mainId")
                if isinstance(main_id, str):
                    uids.add(main_id)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return uids


def _remove_existing_metadata_objects(
    package: dict[str, Any], existing_uids: set[str]
) -> tuple[dict[str, Any], dict[str, int]]:
    filtered = deepcopy(package)
    removed: dict[str, int] = {}
    for collection, items in list(filtered.items()):
        if not isinstance(items, list):
            continue
        kept = [
            item
            for item in items
            if not (
                isinstance(item, dict)
                and isinstance(item.get("id"), str)
                and item["id"] in existing_uids
            )
        ]
        if len(kept) != len(items):
            removed[collection] = len(items) - len(kept)
            filtered[collection] = kept
    pruned = _prune_category_option_combos(filtered)
    if pruned:
        removed["categoryOptionCombos"] = (
            removed.get("categoryOptionCombos", 0) + pruned
        )
    return filtered, removed


def _prune_category_option_combos(package: dict[str, Any]) -> int:
    option_combos = package.get("categoryOptionCombos")
    category_combos = package.get("categoryCombos")
    if not isinstance(option_combos, list) or not isinstance(category_combos, list):
        return 0
    available = {
        str(item["id"])
        for item in category_combos
        if isinstance(item, dict) and item.get("id")
    }
    kept = [
        item
        for item in option_combos
        if isinstance(item, dict)
        and _reference_uid(item.get("categoryCombo")) in available
    ]
    removed = len(option_combos) - len(kept)
    package["categoryOptionCombos"] = kept
    return removed


def _is_default_metadata_object(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return value.get("code") == "default" or value.get("id") in {
        "GLevLNI9wkl",
        "xYerKDKCefk",
        "bjDvmb4bfuf",
    }


def _reference_uid(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    uid = value.get("id")
    return str(uid) if uid else None


def _export_source_values(
    source: DHIS2Client,
    context: TransferContext,
    *,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    units = [str(item["id"]) for item in context.source_organisation_units]
    unit_batch_size = context.job.organisation_units.batch_size
    unit_batches = [
        units[offset : offset + unit_batch_size]
        for offset in range(0, len(units), unit_batch_size)
    ]
    windows = period_date_windows(
        context.start,
        context.end,
        context.job.period_type,
        context.job.transfer.source_period_batch_size,
    )
    planned_requests = len(unit_batches) * len(windows)
    export_state = {"attempts": 0, "completed": 0, "valuesFetched": 0}
    values_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    export_started = time.monotonic()
    _emit_progress(
        progress,
        "source.export.start",
        periodWindows=len(windows),
        organisationUnitBatches=len(unit_batches),
        plannedRequests=planned_requests,
        periodBatchSize=context.job.transfer.source_period_batch_size,
        organisationUnitBatchSize=unit_batch_size,
    )
    for window_start, window_end in windows:
        for unit_batch in unit_batches:
            batch_values = _export_source_batch(
                source,
                context,
                start=window_start,
                end=window_end,
                organisation_unit_ids=unit_batch,
                planned_requests=planned_requests,
                export_state=export_state,
                progress=progress,
            )
            for value in batch_values:
                if not isinstance(value, dict):
                    raise PipelineError(
                        "Source data export contains a non-object data value."
                    )
                value = _normalise_data_value(value)
                key = _data_value_key(value)
                existing = values_by_key.get(key)
                if existing is not None and existing != value:
                    raise PipelineError(
                        "Source export returned conflicting duplicate data values for "
                        f"{key}. Check overlapping organisation-unit roots."
                    )
                values_by_key[key] = value
    _emit_progress(
        progress,
        "source.export.done",
        requestsCompleted=export_state["completed"],
        attempts=export_state["attempts"],
        dataValues=len(values_by_key),
        durationSeconds=round(time.monotonic() - export_started, 2),
    )
    return {"dataValues": list(values_by_key.values())}


def _export_source_batch(
    source: DHIS2Client,
    context: TransferContext,
    *,
    start: str,
    end: str,
    organisation_unit_ids: list[str],
    planned_requests: int,
    export_state: dict[str, int],
    progress: ProgressCallback | None,
    split_depth: int = 0,
) -> list[dict[str, Any]]:
    export_state["attempts"] += 1
    attempt = export_state["attempts"]
    request_started = time.monotonic()
    _emit_progress(
        progress,
        "source.export.request.start",
        attempt=attempt,
        plannedRequests=planned_requests,
        completed=export_state["completed"],
        start=start,
        end=end,
        organisationUnits=len(organisation_unit_ids),
        splitDepth=split_depth,
    )
    try:
        payload = source.export_data_values(
            data_set_id=context.source_data_set.id,
            start=start,
            end=end,
            organisation_unit_ids=organisation_unit_ids,
            include_children=context.job.organisation_units.include_children,
            include_deleted=context.job.transfer.include_deleted,
            id_schemes=context.job.id_schemes.api_params(),
        )
    except HttpRequestError as exc:
        if not _is_timeout_or_gateway_error(exc):
            raise
        child_requests = _split_source_export_request(
            start,
            end,
            organisation_unit_ids,
            context.job.period_type,
        )
        if not child_requests:
            raise TransientPipelineError(
                "Source export timed out even for one DHIS2 period and one organisation "
                f"unit ({start} to {end}, {organisation_unit_ids[0]}). Increase "
                "DHIS2_TIMEOUT_SECONDS or inspect the source DHIS2 performance."
            ) from exc
        _emit_progress(
            progress,
            "source.export.request.split",
            attempt=attempt,
            reason=f"HTTP {exc.status_code or 'timeout'}",
            childRequests=len(child_requests),
            start=start,
            end=end,
            organisationUnits=len(organisation_unit_ids),
        )
        values: list[dict[str, Any]] = []
        for child_start, child_end, child_units in child_requests:
            values.extend(
                _export_source_batch(
                    source,
                    context,
                    start=child_start,
                    end=child_end,
                    organisation_unit_ids=child_units,
                    planned_requests=planned_requests,
                    export_state=export_state,
                    progress=progress,
                    split_depth=split_depth + 1,
                )
            )
        return values

    batch_values = payload["dataValues"]
    export_state["completed"] += 1
    export_state["valuesFetched"] += len(batch_values)
    _emit_progress(
        progress,
        "source.export.request.done",
        attempt=attempt,
        completed=export_state["completed"],
        plannedRequests=planned_requests,
        start=start,
        end=end,
        organisationUnits=len(organisation_unit_ids),
        dataValues=len(batch_values),
        valuesFetched=export_state["valuesFetched"],
        durationSeconds=round(time.monotonic() - request_started, 2),
    )
    return batch_values


def _split_source_export_request(
    start: str,
    end: str,
    organisation_unit_ids: list[str],
    period_type: str,
) -> list[tuple[str, str, list[str]]]:
    request_periods = periods_between(start, end, period_type)
    if len(request_periods) > 1:
        midpoint = len(request_periods) // 2
        left_start, left_end = period_range_bounds(
            request_periods[0],
            request_periods[midpoint - 1],
            period_type,
        )
        right_start, right_end = period_range_bounds(
            request_periods[midpoint],
            request_periods[-1],
            period_type,
        )
        return [
            (left_start, left_end, organisation_unit_ids),
            (right_start, right_end, organisation_unit_ids),
        ]
    if len(organisation_unit_ids) > 1:
        midpoint = len(organisation_unit_ids) // 2
        return [
            (start, end, organisation_unit_ids[:midpoint]),
            (start, end, organisation_unit_ids[midpoint:]),
        ]
    return []


def _is_timeout_or_gateway_error(exc: HttpRequestError) -> bool:
    text = f"{exc.body} {exc}".lower()
    return exc.status_code in {502, 503, 504, 520, 522, 524} or any(
        marker in text
        for marker in (
            "timed out",
            "timeout",
            "cloudflare",
            "connection reset",
        )
    )


def is_transient_transfer_http_error(exc: HttpRequestError) -> bool:
    text = f"{exc.body} {exc}".lower()
    return exc.status_code in {429, 502, 503, 504, 520, 522, 524} or any(
        marker in text
        for marker in (
            "timed out",
            "timeout",
            "cloudflare",
            "connection reset",
            "temporary failure in name resolution",
            "name or service not known",
            "nodename nor servname provided",
        )
    )


def _version_unmappable_historical_values(
    source: DHIS2Client,
    target: DHIS2Client,
    scenario: TransferScenario,
    context: TransferContext,
    data_values: list[dict[str, Any]],
    *,
    apply: bool,
    progress: ProgressCallback | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Preserve non-remappable historical category semantics on versioned DEs.

    A source Data Element may have changed CategoryCombo while old values still point
    at the former CategoryOptionCombos.  When no exact option-set equivalent exists in
    the current combo, the value is moved to a deterministic historical Data Element
    on Y.  Its CategoryCombo and CategoryOptionCombo UIDs are preserved from X.
    """
    report: dict[str, Any] = {
        "enabled": scenario.metadata.synchronization.historical_data_elements.enabled,
        "checked": False,
        "created": False,
        "rewrittenValueCount": 0,
        "mappings": [],
        "validationPendingMetadataCommit": False,
    }
    policy = scenario.metadata.synchronization.historical_data_elements
    if not policy.enabled or not data_values:
        return data_values, report
    if (
        context.job.id_schemes.data_elements != "uid"
        or context.job.id_schemes.category_option_combos != "uid"
    ):
        raise PipelineError(
            "Versioned historical Data Elements require UID mapping for Data Elements "
            "and CategoryOptionCombos."
        )

    source_elements = {
        str(item["id"]): item
        for item in context.source_data_elements
        if item.get("id")
    }
    current_combo_ids = sorted(
        {
            combo_id
            for item in context.source_data_elements
            if (combo_id := _reference_uid(item.get("categoryCombo")))
        }
    )
    # Older/simple Data Sets without category metadata need no historical audit.
    if not current_combo_ids:
        return data_values, report

    _emit_progress(
        progress,
        "historical_data_elements.audit.start",
        dataValues=len(data_values),
    )
    current_combos = source.category_option_combos_for_category_combos(
        current_combo_ids
    )
    current_by_id = {
        str(item["id"]): item for item in current_combos if item.get("id")
    }
    stale_usage: Counter[tuple[str, str, str]] = Counter()
    missing_elements: Counter[str] = Counter()
    for value in data_values:
        element_id = str(value.get("dataElement") or "")
        element = source_elements.get(element_id)
        if element is None:
            missing_elements[element_id or "<missing>"] += 1
            continue
        expected_combo = _reference_uid(element.get("categoryCombo"))
        value_combo = str(value.get("categoryOptionCombo") or "")
        current = current_by_id.get(value_combo)
        if (
            not expected_combo
            or not value_combo
            or (
                current is not None
                and _reference_uid(current.get("categoryCombo")) == expected_combo
            )
        ):
            continue
        stale_usage[(element_id, value_combo, expected_combo)] += 1

    if missing_elements:
        preview = ", ".join(
            f"{key} ({count} values)"
            for key, count in missing_elements.most_common(10)
        )
        raise PipelineError(
            "Source values reference Data Elements outside the configured Data Set: "
            + preview
        )
    report["checked"] = True
    if not stale_usage:
        _emit_progress(
            progress,
            "historical_data_elements.audit.done",
            versions=0,
            rewrittenValues=0,
        )
        return data_values, report

    historical_ids = sorted({combo_id for _, combo_id, _ in stale_usage})
    historical_combos = source.metadata_objects_by_ids(
        resource="categoryOptionCombos",
        root_key="categoryOptionCombos",
        object_ids=historical_ids,
        fields=(
            "id,code,name,categoryCombo[id,code,name],"
            "categoryOptions[id,code,name]"
        ),
    )
    historical_by_id = {
        str(item["id"]): item for item in historical_combos if item.get("id")
    }
    missing_combos = sorted(set(historical_ids) - set(historical_by_id))
    if missing_combos:
        raise PipelineError(
            "Historical CategoryOptionCombo objects no longer exist on X: "
            + ", ".join(missing_combos[:10])
        )

    current_by_semantics: dict[str, set[tuple[str, ...]]] = defaultdict(set)
    for combo in current_combos:
        parent_id = _reference_uid(combo.get("categoryCombo"))
        if parent_id:
            current_by_semantics[parent_id].add(
                tuple(sorted(_reference_ids(combo.get("categoryOptions"))))
            )

    version_usage: Counter[tuple[str, str]] = Counter()
    combo_to_version: dict[tuple[str, str], str] = {}
    exact_equivalents = 0
    for (element_id, combo_id, expected_combo), count in stale_usage.items():
        historical = historical_by_id[combo_id]
        semantics = tuple(sorted(_reference_ids(historical.get("categoryOptions"))))
        if semantics in current_by_semantics.get(expected_combo, set()):
            exact_equivalents += count
            continue
        historical_parent = _reference_uid(historical.get("categoryCombo"))
        if not historical_parent:
            raise PipelineError(
                f"Historical CategoryOptionCombo {combo_id} has no parent CategoryCombo."
            )
        version_usage[(element_id, historical_parent)] += count
        combo_to_version[(element_id, combo_id)] = historical_parent

    # Exact equivalents are deliberately left untouched here and are handled by
    # _normalise_source_category_option_combos after this phase.
    if not version_usage:
        report["exactEquivalentValueCount"] = exact_equivalents
        _emit_progress(
            progress,
            "historical_data_elements.audit.done",
            versions=0,
            rewrittenValues=0,
        )
        return data_values, report
    if context.target_data_set is None:
        report.update(
            {
                "validationPendingMetadataCommit": True,
                "reason": "canonical target Data Set must be committed first",
                "plannedVersions": len(version_usage),
            }
        )
        return data_values, report

    parent_ids = sorted({parent for _, parent in version_usage})
    parent_combos = source.metadata_objects_by_ids(
        resource="categoryCombos",
        root_key="categoryCombos",
        object_ids=parent_ids,
        fields="id,code,name,dataDimensionType,categories[id]",
    )
    parents_by_id = {
        str(item["id"]): item for item in parent_combos if item.get("id")
    }
    if set(parent_ids) != set(parents_by_id):
        missing = sorted(set(parent_ids) - set(parents_by_id))
        raise PipelineError(
            "Historical CategoryCombos no longer exist on X: "
            + ", ".join(missing[:10])
        )
    category_ids = sorted(
        {
            category_id
            for combo in parent_combos
            for category_id in _reference_ids(combo.get("categories"))
        }
    )
    categories = source.metadata_objects_by_ids(
        resource="categories",
        root_key="categories",
        object_ids=category_ids,
        fields="id,code,name,shortName,dataDimensionType,categoryOptions[id]",
    )
    option_ids = sorted(
        {
            option_id
            for category in categories
            for option_id in _reference_ids(category.get("categoryOptions"))
        }
        | {
            option_id
            for combo in historical_combos
            for option_id in _reference_ids(combo.get("categoryOptions"))
        }
    )
    category_options = source.metadata_objects_by_ids(
        resource="categoryOptions",
        root_key="categoryOptions",
        object_ids=option_ids,
        fields="id,code,name,shortName",
    )
    all_parent_combinations = source.category_option_combos_for_category_combos(
        parent_ids
    )
    full_elements = source.metadata_objects_by_ids(
        resource="dataElements",
        root_key="dataElements",
        object_ids=sorted({element for element, _ in version_usage}),
        fields=(
            "id,code,name,shortName,description,valueType,domainType,"
            "aggregationType,zeroIsSignificant,optionSet[id]"
        ),
    )
    full_elements_by_id = {
        str(item["id"]): item for item in full_elements if item.get("id")
    }

    categories_by_id = {
        str(item["id"]): item for item in categories if item.get("id")
    }
    options_by_id = {
        str(item["id"]): item for item in category_options if item.get("id")
    }
    combinations_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for combination in all_parent_combinations:
        parent_id = _reference_uid(combination.get("categoryCombo"))
        if parent_id:
            combinations_by_parent[parent_id].append(combination)

    effective_collections: dict[str, list[dict[str, Any]]] = {
        "categoryOptions": [],
        "categories": [],
        "categoryCombos": [],
        "categoryOptionCombos": [],
    }
    effective_parent_ids: dict[str, str] = {}
    effective_combination_ids: dict[tuple[str, str], str] = {}
    cloned_graphs: list[dict[str, Any]] = []
    for parent_id in parent_ids:
        source_parent = parents_by_id[parent_id]
        graph_category_ids = _reference_ids(source_parent.get("categories"))
        graph_categories = [
            categories_by_id[item]
            for item in sorted(graph_category_ids)
            if item in categories_by_id
        ]
        graph_combinations = combinations_by_parent.get(parent_id, [])
        graph_option_ids = {
            item
            for category in graph_categories
            for item in _reference_ids(category.get("categoryOptions"))
        } | {
            item
            for combination in graph_combinations
            for item in _reference_ids(combination.get("categoryOptions"))
        }
        graph: dict[str, list[dict[str, Any]]] = {
            "categoryOptions": [
                options_by_id[item]
                for item in sorted(graph_option_ids)
                if item in options_by_id
            ],
            "categories": graph_categories,
            "categoryCombos": [source_parent],
            "categoryOptionCombos": graph_combinations,
        }
        conflicts = _historical_target_conflicts(target, graph)
        if conflicts and policy.uid_collision_strategy == "block":
            raise PipelineError(
                "Historical metadata synchronization blocked by incompatible objects on Y: "
                + "; ".join(conflicts[:10])
            )
        if conflicts:
            graph, clone_report = _clone_historical_category_graph(
                graph,
                source_parent_id=parent_id,
                name_suffix=policy.name_suffix,
            )
            clone_report["conflicts"] = conflicts
            cloned_graphs.append(clone_report)
            effective_parent_ids[parent_id] = str(
                graph["categoryCombos"][0]["id"]
            )
            _emit_progress(
                progress,
                "historical_data_elements.uid_collision.clone",
                sourceCategoryCombo=parent_id,
                targetCategoryCombo=effective_parent_ids[parent_id],
                conflicts=len(conflicts),
            )
            cloned_combination_ids = clone_report["uidMappings"][
                "categoryOptionCombos"
            ]
            for source_id, target_id in cloned_combination_ids.items():
                effective_combination_ids[(parent_id, source_id)] = target_id
        else:
            effective_parent_ids[parent_id] = parent_id
            for item in graph_combinations:
                if item.get("id"):
                    item_id = str(item["id"])
                    effective_combination_ids[(parent_id, item_id)] = item_id
        for collection, items in graph.items():
            effective_collections[collection].extend(items)

    for collection, items in effective_collections.items():
        effective_collections[collection] = _unique_metadata_items(items)

    version_definitions: list[dict[str, Any]] = []
    for (element_id, parent_id), count in sorted(version_usage.items()):
        source_element = full_elements_by_id.get(element_id) or source_elements[element_id]
        parent = parents_by_id[parent_id]
        target_parent_id = effective_parent_ids[parent_id]
        target_id = _stable_dhis2_uid("historical-data-element", element_id, parent_id)
        parent_label = str(parent.get("name") or parent_id)
        base_name = str(source_element.get("name") or element_id)
        name = _limited_text(
            f"{base_name}{policy.name_suffix} ({parent_label})", 230
        )
        short_name = _limited_text(name, 50)
        source_code = str(source_element.get("code") or element_id)
        code_suffix = f"_H_{hashlib.sha256(parent_id.encode()).hexdigest()[:8]}"
        code = source_code[: 50 - len(code_suffix)] + code_suffix
        version_definitions.append(
            {
                "sourceDataElement": element_id,
                "historicalCategoryCombo": parent_id,
                "targetCategoryCombo": target_parent_id,
                "targetDataElement": target_id,
                "dataValues": count,
                "name": name,
                "code": code,
                "metadata": _historical_data_element_payload(
                    source_element,
                    target_id=target_id,
                    target_code=code,
                    target_name=name,
                    target_short_name=short_name,
                    category_combo_id=target_parent_id,
                ),
            }
        )

    collections: dict[str, list[dict[str, Any]]] = {
        **effective_collections,
        "dataElements": [item["metadata"] for item in version_definitions],
    }
    _assert_historical_target_compatibility(target, collections)
    package = _missing_historical_metadata(target, collections)
    existing_target_ids = {
        str(item["id"])
        for item in context.target_data_elements
        if item.get("id")
    }
    version_ids = {str(item["targetDataElement"]) for item in version_definitions}
    missing_memberships = version_ids - existing_target_ids
    artifact_run = run_id(f"historical-{scenario.id}-{context.job.id}")
    artifact_dir = (
        scenario.job_state_dir(context.job.id)
        / "runs"
        / artifact_run
        / "historical-metadata"
    )
    plan = {
        "scenario": scenario.id,
        "job": context.job.id,
        "dryRun": not apply,
        "versions": [
            {key: value for key, value in item.items() if key != "metadata"}
            for item in version_definitions
        ],
        "objectsToCreate": {
            key: len(value) for key, value in package.items() if value
        },
        "dataSetMembershipsToAdd": sorted(missing_memberships),
        "uidCollisionStrategy": policy.uid_collision_strategy,
        "clonedHistoricalGraphs": cloned_graphs,
    }
    _write_json(artifact_dir / "plan.json", plan)
    validations: list[dict[str, Any]] = []
    commits: list[dict[str, Any]] = []
    validation_path = artifact_dir / "validation.json"
    commit_path = artifact_dir / "commit.json"
    _write_json(validation_path, validations)
    _write_json(commit_path, commits)

    with exclusive_lock(
        scenario.state_dir / "locks" / "metadata.lock",
        purpose=f"historical metadata for {scenario.id}/{context.job.id}",
        transient_when_busy=True,
    ):
        non_empty_package = {key: value for key, value in package.items() if value}
        if non_empty_package:
            _emit_progress(
                progress,
                "historical_data_elements.metadata.validate.start",
                versions=len(version_definitions),
            )
            validation = target.import_metadata(
                non_empty_package,
                commit=False,
                import_strategy="CREATE",
                atomic_mode=scenario.metadata.atomic_mode,
                identifier="UID",
                skip_sharing=scenario.metadata.skip_sharing,
            )
            _assert_success_response(validation, "Historical metadata validation")
            validations.append({"stage": "create", "response": validation})
            _write_json(validation_path, validations)
            if apply:
                commit = target.import_metadata(
                    non_empty_package,
                    commit=True,
                    import_strategy="CREATE",
                    atomic_mode=scenario.metadata.atomic_mode,
                    identifier="UID",
                    skip_sharing=scenario.metadata.skip_sharing,
                )
                _assert_success_response(commit, "Historical metadata creation")
                commits.append({"stage": "create", "response": commit})
                _write_json(commit_path, commits)

        if missing_memberships:
            membership_package = _data_set_membership_package(
                target,
                data_set_id=context.target_data_set.id,
                data_element_ids=existing_target_ids | version_ids,
            )
            _write_json(artifact_dir / "membership.json", membership_package)
            if not non_empty_package or apply:
                validation = target.import_metadata(
                    membership_package,
                    commit=False,
                    import_strategy="CREATE_AND_UPDATE",
                    atomic_mode=scenario.metadata.atomic_mode,
                    identifier="UID",
                    skip_sharing=scenario.metadata.skip_sharing,
                )
                _assert_success_response(
                    validation, "Historical Data Set membership validation"
                )
                validations.append({"stage": "attach", "response": validation})
                _write_json(validation_path, validations)
                if apply:
                    commit = target.import_metadata(
                        membership_package,
                        commit=True,
                        import_strategy="CREATE_AND_UPDATE",
                        atomic_mode=scenario.metadata.atomic_mode,
                        identifier="UID",
                        skip_sharing=scenario.metadata.skip_sharing,
                    )
                    _assert_success_response(
                        commit, "Historical Data Set membership update"
                    )
                    commits.append({"stage": "attach", "response": commit})
                    _write_json(commit_path, commits)
            else:
                plan["membershipValidationPendingCreateCommit"] = True

    pending = bool((non_empty_package or missing_memberships) and not apply)
    if pending:
        report.update(
            {
                "mappings": plan["versions"],
                "clonedHistoricalGraphs": cloned_graphs,
                "validationPendingMetadataCommit": True,
                "planArtifact": str(artifact_dir / "plan.json"),
            }
        )
        return data_values, report

    target_elements = target.data_elements_for_data_set(
        data_set_id=context.target_data_set.id,
        page_size=500,
    )
    linked_ids = {str(item["id"]) for item in target_elements if item.get("id")}
    missing_after_commit = sorted(version_ids - linked_ids)
    if missing_after_commit:
        raise PipelineError(
            "Historical metadata import completed but these Data Elements are not linked "
            "to the target Data Set: " + ", ".join(missing_after_commit)
        )

    target_for_pair = {
        (str(item["sourceDataElement"]), str(item["historicalCategoryCombo"])):
        str(item["targetDataElement"])
        for item in version_definitions
    }
    rewritten = 0
    for value in data_values:
        element_id = str(value.get("dataElement") or "")
        combo_id = str(value.get("categoryOptionCombo") or "")
        parent_id = combo_to_version.get((element_id, combo_id))
        target_id = target_for_pair.get((element_id, parent_id or ""))
        if target_id:
            value["dataElement"] = target_id
            value["categoryOptionCombo"] = effective_combination_ids.get(
                (parent_id or "", combo_id),
                combo_id,
            )
            rewritten += 1

    report.update(
        {
            "created": bool(commits),
            "rewrittenValueCount": rewritten,
            "mappings": plan["versions"],
            "clonedHistoricalGraphs": cloned_graphs,
            "validationPendingMetadataCommit": False,
            "planArtifact": str(artifact_dir / "plan.json"),
            "validationArtifact": str(artifact_dir / "validation.json"),
            "commitArtifact": str(commit_path),
            "exactEquivalentValueCount": exact_equivalents,
        }
    )
    _emit_progress(
        progress,
        "historical_data_elements.audit.done",
        versions=len(version_definitions),
        rewrittenValues=rewritten,
    )
    return data_values, report


def _historical_data_element_payload(
    source: dict[str, Any],
    *,
    target_id: str,
    target_code: str,
    target_name: str,
    target_short_name: str,
    category_combo_id: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": target_id,
        "code": target_code,
        "name": target_name,
        "shortName": target_short_name,
        "valueType": source.get("valueType") or "NUMBER",
        "domainType": source.get("domainType") or "AGGREGATE",
        "aggregationType": source.get("aggregationType") or "SUM",
        "categoryCombo": {"id": category_combo_id},
    }
    for field in ("description", "zeroIsSignificant"):
        if field in source and source[field] is not None:
            payload[field] = source[field]
    option_set = _reference_uid(source.get("optionSet"))
    if option_set:
        payload["optionSet"] = {"id": option_set}
    return payload


def _stable_dhis2_uid(*parts: str) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).digest()
    number = int.from_bytes(digest, "big")
    encoded = ""
    for _ in range(10):
        number, index = divmod(number, len(alphabet))
        encoded += alphabet[index]
    first = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[digest[0] % 26]
    return first + encoded


def _limited_text(value: str, maximum: int) -> str:
    return value if len(value) <= maximum else value[: maximum - 1].rstrip() + "…"


def _unique_metadata_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for item in items:
        uid = str(item.get("id") or "")
        if not uid:
            continue
        existing = by_id.get(uid)
        if existing is not None and existing != item:
            raise PipelineError(
                f"Historical metadata contains conflicting definitions for UID {uid}."
            )
        by_id[uid] = item
    return [by_id[uid] for uid in sorted(by_id)]


def _clone_historical_category_graph(
    graph: dict[str, list[dict[str, Any]]],
    *,
    source_parent_id: str,
    name_suffix: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Clone one historical category graph into an isolated deterministic namespace."""
    parent_items = [
        item
        for item in graph.get("categoryCombos", [])
        if str(item.get("id") or "") == source_parent_id
    ]
    if len(parent_items) != 1:
        raise PipelineError(
            f"Historical CategoryCombo graph {source_parent_id} is incomplete or ambiguous."
        )

    mappings: dict[str, dict[str, str]] = {
        "categoryOptions": {
            str(item["id"]): _stable_dhis2_uid(
                "historical-category-option-clone",
                source_parent_id,
                str(item["id"]),
            )
            for item in graph.get("categoryOptions", [])
            if item.get("id")
        },
        "categories": {
            str(item["id"]): _stable_dhis2_uid(
                "historical-category-clone",
                source_parent_id,
                str(item["id"]),
            )
            for item in graph.get("categories", [])
            if item.get("id")
        },
        "categoryCombos": {
            source_parent_id: _stable_dhis2_uid(
                "historical-category-combo-clone",
                source_parent_id,
            )
        },
        "categoryOptionCombos": {
            str(item["id"]): _stable_dhis2_uid(
                "historical-category-option-combo-clone",
                source_parent_id,
                str(item["id"]),
            )
            for item in graph.get("categoryOptionCombos", [])
            if item.get("id")
        },
    }
    label_suffix = (
        name_suffix
        + " "
        + hashlib.sha256(source_parent_id.encode("utf-8")).hexdigest()[:8]
    )

    def clone_identity(
        source: dict[str, Any],
        *,
        target_id: str,
    ) -> dict[str, Any]:
        result = deepcopy(source)
        result["id"] = target_id
        code = source.get("code")
        if code:
            suffix = (
                "_H_"
                + hashlib.sha256(target_id.encode("utf-8")).hexdigest()[:8]
            )
            result["code"] = str(code)[: 50 - len(suffix)] + suffix
        name = source.get("name")
        if name:
            result["name"] = _limited_text(str(name) + label_suffix, 230)
        short_name = source.get("shortName")
        if short_name:
            result["shortName"] = _limited_text(
                str(short_name) + label_suffix,
                50,
            )
        return result

    def mapped_references(
        value: Any,
        mapping: dict[str, str],
        *,
        label: str,
    ) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        result: list[dict[str, str]] = []
        missing: list[str] = []
        for reference in value:
            source_id = _reference_uid(reference)
            if not source_id:
                continue
            target_id = mapping.get(source_id)
            if target_id is None:
                missing.append(source_id)
            else:
                result.append({"id": target_id})
        if missing:
            raise PipelineError(
                f"Historical clone {source_parent_id} is missing {label}: "
                + ", ".join(sorted(missing)[:10])
            )
        return result

    cloned_options = [
        clone_identity(
            item,
            target_id=mappings["categoryOptions"][str(item["id"])],
        )
        for item in graph.get("categoryOptions", [])
        if item.get("id")
    ]
    cloned_categories: list[dict[str, Any]] = []
    for item in graph.get("categories", []):
        if not item.get("id"):
            continue
        cloned = clone_identity(
            item,
            target_id=mappings["categories"][str(item["id"])],
        )
        cloned["categoryOptions"] = mapped_references(
            item.get("categoryOptions"),
            mappings["categoryOptions"],
            label="CategoryOptions",
        )
        cloned_categories.append(cloned)

    source_parent = parent_items[0]
    cloned_parent = clone_identity(
        source_parent,
        target_id=mappings["categoryCombos"][source_parent_id],
    )
    cloned_parent["categories"] = mapped_references(
        source_parent.get("categories"),
        mappings["categories"],
        label="Categories",
    )

    cloned_combinations: list[dict[str, Any]] = []
    for item in graph.get("categoryOptionCombos", []):
        if not item.get("id"):
            continue
        cloned = clone_identity(
            item,
            target_id=mappings["categoryOptionCombos"][str(item["id"])],
        )
        cloned["categoryCombo"] = {"id": cloned_parent["id"]}
        cloned["categoryOptions"] = mapped_references(
            item.get("categoryOptions"),
            mappings["categoryOptions"],
            label="CategoryOptions",
        )
        cloned_combinations.append(cloned)

    return (
        {
            "categoryOptions": cloned_options,
            "categories": cloned_categories,
            "categoryCombos": [cloned_parent],
            "categoryOptionCombos": cloned_combinations,
        },
        {
            "sourceCategoryCombo": source_parent_id,
            "targetCategoryCombo": str(cloned_parent["id"]),
            "uidMappings": mappings,
        },
    )


def _historical_target_conflicts(
    target: DHIS2Client,
    collections: dict[str, list[dict[str, Any]]],
) -> list[str]:
    endpoints = {
        "categoryOptions": "categoryOptions",
        "categories": "categories",
        "categoryCombos": "categoryCombos",
        "categoryOptionCombos": "categoryOptionCombos",
        "dataElements": "dataElements",
    }
    fields = {
        "categoryOptions": "id,code,name,shortName",
        "categories": "id,code,name,shortName,dataDimensionType,categoryOptions[id]",
        "categoryCombos": "id,code,name,dataDimensionType,categories[id]",
        "categoryOptionCombos": "id,code,name,categoryCombo[id],categoryOptions[id]",
        "dataElements": (
            "id,code,name,shortName,valueType,domainType,aggregationType,"
            "categoryCombo[id]"
        ),
    }
    conflicts: list[str] = []
    for collection, source_items in collections.items():
        if collection not in endpoints:
            continue
        ids = [str(item["id"]) for item in source_items if item.get("id")]
        if not ids:
            continue
        existing = target.metadata_objects_by_ids(
            resource=endpoints[collection],
            root_key=collection,
            object_ids=ids,
            fields=fields[collection],
        )
        expected_by_id = {
            str(item["id"]): item for item in source_items if item.get("id")
        }
        for item in existing:
            uid = str(item.get("id") or "")
            expected = expected_by_id.get(uid)
            if expected is None:
                continue
            if collection in {"categories", "categoryOptionCombos"}:
                relation = "categoryOptions"
                if _reference_ids(expected.get(relation)) != _reference_ids(
                    item.get(relation)
                ):
                    conflicts.append(f"{collection} {uid} {relation} differ")
            if collection == "categoryCombos" and _reference_ids(
                expected.get("categories")
            ) != _reference_ids(item.get("categories")):
                conflicts.append(f"categoryCombos {uid} categories differ")
            if collection == "categoryOptionCombos" and _reference_uid(
                expected.get("categoryCombo")
            ) != _reference_uid(item.get("categoryCombo")):
                conflicts.append(f"categoryOptionCombos {uid} parent differs")
            if collection == "dataElements":
                conflicts.extend(
                    f"dataElements {uid} {conflict}"
                    for conflict in _data_element_conflicts(
                        expected, item, category_scheme="uid"
                    )
                )
            # DHIS2 derives CategoryOptionCombo names from their option set. The
            # parent and complete option UID set above are the semantic identity;
            # comparing a generated label would create false conflicts across versions.
            identity_fields = (
                ("code",)
                if collection == "categoryOptionCombos"
                else ("code", "name")
            )
            for field in identity_fields:
                left = expected.get(field)
                right = item.get(field)
                if left and right and left != right:
                    conflicts.append(
                        f"{collection} {uid} {field}: source={left!r}, target={right!r}"
                    )
    return conflicts


def _assert_historical_target_compatibility(
    target: DHIS2Client,
    collections: dict[str, list[dict[str, Any]]],
) -> None:
    conflicts = _historical_target_conflicts(target, collections)
    if conflicts:
        raise PipelineError(
            "Historical metadata synchronization blocked by incompatible objects on Y: "
            + "; ".join(conflicts[:10])
        )


def _missing_historical_metadata(
    target: DHIS2Client,
    collections: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for collection, items in collections.items():
        ids = [str(item["id"]) for item in items if item.get("id")]
        existing = target.metadata_objects_by_ids(
            resource=collection,
            root_key=collection,
            object_ids=ids,
            fields="id",
        )
        existing_ids = {str(item["id"]) for item in existing if item.get("id")}
        result[collection] = [
            deepcopy(item)
            for item in items
            if item.get("id") and str(item["id"]) not in existing_ids
        ]
    return result


def _target_payload(
    context: TransferContext, data_values: list[dict[str, Any]]
) -> dict[str, Any]:
    if context.target_data_set is None:
        raise PipelineError("Cannot build target payload without a target Data Set.")
    scheme = context.job.id_schemes.data_sets
    selector = (
        context.target_data_set.id
        if scheme == "uid"
        else context.target_data_set.code
    )
    if not selector:
        raise PipelineError(
            f"Target Data Set has no {scheme.upper()} for the configured mapping strategy."
        )
    return {"dataSet": selector, "dataValues": data_values}


def _normalise_source_category_option_combos(
    source: DHIS2Client,
    context: TransferContext,
    data_values: list[dict[str, Any]],
    *,
    ignored_data_elements: set[str] | None = None,
    progress: ProgressCallback | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Replace stale source COC UIDs only when an exact current equivalent exists.

    DHIS2 can retain historical data values after a Data Element CategoryCombo was
    replaced.  The export API still returns those old CategoryOptionCombo UIDs, while
    a current Data Set import rejects them with E8024.  Matching on the complete set of
    CategoryOption UIDs preserves the value semantics without changing metadata.
    """
    report: dict[str, Any] = {
        "checked": False,
        "historicalCombinationCount": 0,
        "remappedValueCount": 0,
        "mappings": [],
    }
    if not data_values:
        return data_values, report

    data_element_scheme = context.job.id_schemes.data_elements
    combo_scheme = context.job.id_schemes.category_option_combos
    data_elements = _metadata_by_key(
        context.source_data_elements,
        data_element_scheme,
    )
    current_combo_ids = sorted(
        {
            combo_id
            for item in context.source_data_elements
            if (combo_id := _reference_uid(item.get("categoryCombo")))
        }
    )
    if not current_combo_ids:
        return data_values, report

    _emit_progress(
        progress,
        "source.category_option_combos.audit.start",
        dataValues=len(data_values),
    )
    current_combos = source.category_option_combos_for_category_combos(
        current_combo_ids
    )
    identifier_field = "id" if combo_scheme == "uid" else "code"
    current_by_identifier = {
        str(item[identifier_field]): item
        for item in current_combos
        if item.get(identifier_field)
    }

    historical_usage: Counter[tuple[str, str, str]] = Counter()
    missing_data_elements: Counter[str] = Counter()
    ignored = ignored_data_elements or set()
    for value in data_values:
        data_element_key = str(value.get("dataElement") or "")
        if data_element_key in ignored:
            continue
        element = data_elements.get(data_element_key)
        if element is None:
            missing_data_elements[data_element_key or "<missing>"] += 1
            continue
        expected_combo_id = _reference_uid(element.get("categoryCombo"))
        value_combo = value.get("categoryOptionCombo")
        if not expected_combo_id or not value_combo:
            continue
        value_combo_key = str(value_combo)
        current = current_by_identifier.get(value_combo_key)
        if current is not None and _reference_uid(
            current.get("categoryCombo")
        ) == expected_combo_id:
            continue
        historical_usage[
            (data_element_key, value_combo_key, expected_combo_id)
        ] += 1

    if missing_data_elements:
        preview = ", ".join(
            f"{key} ({count} values)"
            for key, count in missing_data_elements.most_common(10)
        )
        raise PipelineError(
            "Source values reference Data Elements outside the configured source Data Set: "
            + preview
        )

    report["checked"] = True
    if not historical_usage:
        _emit_progress(
            progress,
            "source.category_option_combos.audit.done",
            historicalCombinations=0,
            remappedValues=0,
        )
        return data_values, report

    if combo_scheme != "uid":
        raise PipelineError(
            "Historical CategoryOptionCombo reconciliation currently requires "
            "mapping.category_option_combos: uid."
        )

    historical_ids = sorted({item[1] for item in historical_usage})
    historical_objects = source.metadata_objects_by_ids(
        resource="categoryOptionCombos",
        root_key="categoryOptionCombos",
        object_ids=historical_ids,
        fields=(
            "id,code,name,categoryCombo[id,code,name],"
            "categoryOptions[id,code,name]"
        ),
    )
    historical_by_id = {
        str(item["id"]): item for item in historical_objects if item.get("id")
    }

    current_by_semantics: dict[
        str, dict[tuple[str, ...], dict[str, Any]]
    ] = defaultdict(dict)
    for item in current_combos:
        parent_id = _reference_uid(item.get("categoryCombo"))
        identifier = item.get(identifier_field)
        if not parent_id or not identifier:
            continue
        semantics = tuple(sorted(_reference_ids(item.get("categoryOptions"))))
        existing = current_by_semantics[parent_id].get(semantics)
        if existing is not None and existing.get(identifier_field) != identifier:
            raise PipelineError(
                "Source metadata contains duplicate current CategoryOptionCombos for "
                f"CategoryCombo {parent_id} and options {semantics}."
            )
        current_by_semantics[parent_id][semantics] = item

    replacements: dict[tuple[str, str], str] = {}
    replacement_counts: Counter[tuple[str, str, tuple[str, ...]]] = Counter()
    unresolved: list[str] = []
    for (data_element_key, historical_id, expected_combo_id), count in sorted(
        historical_usage.items()
    ):
        historical = historical_by_id.get(historical_id)
        if historical is None:
            unresolved.append(
                f"{historical_id} for Data Element {data_element_key}: object not found on X"
            )
            continue
        semantics = tuple(sorted(_reference_ids(historical.get("categoryOptions"))))
        replacement = current_by_semantics.get(expected_combo_id, {}).get(semantics)
        replacement_id = replacement.get(identifier_field) if replacement else None
        if not replacement_id:
            option_names = sorted(
                str(item.get("name") or item.get("id"))
                for item in historical.get("categoryOptions", [])
                if isinstance(item, dict)
            )
            label = ", ".join(option_names) or "no category options"
            unresolved.append(
                f"{historical_id} ({label}) for Data Element {data_element_key}: "
                f"no exact equivalent in current CategoryCombo {expected_combo_id} "
                f"({count} values)"
            )
            continue
        replacement_key = str(replacement_id)
        replacements[(data_element_key, historical_id)] = replacement_key
        replacement_counts[(historical_id, replacement_key, semantics)] += count

    if unresolved:
        raise PipelineError(
            "Source values use historical CategoryOptionCombos that cannot be mapped "
            "losslessly to the Data Elements' current CategoryCombos. Transfer stopped "
            "before metadata or data commit: "
            + "; ".join(unresolved[:10])
        )

    destination_pairs = {
        (data_element, replacement)
        for (data_element, _historical), replacement in replacements.items()
    }
    seen_destination_keys: dict[tuple[str, ...], str] = {}
    collisions: Counter[tuple[str, ...]] = Counter()
    for value in data_values:
        data_element_key = str(value.get("dataElement") or "")
        source_combo_key = str(value.get("categoryOptionCombo") or "")
        effective_combo_key = replacements.get(
            (data_element_key, source_combo_key),
            source_combo_key,
        )
        if (data_element_key, effective_combo_key) not in destination_pairs:
            continue
        effective_key = tuple(
            str(child or "")
            for child in (
                data_element_key,
                value.get("period"),
                value.get("orgUnit"),
                effective_combo_key,
                value.get("attributeOptionCombo"),
            )
        )
        previous_combo = seen_destination_keys.get(effective_key)
        if previous_combo is not None:
            collisions[effective_key] += 1
        else:
            seen_destination_keys[effective_key] = source_combo_key

    if collisions:
        preview = "; ".join(
            f"dataElement={key[0]}, period={key[1]}, orgUnit={key[2]}, "
            f"categoryOptionCombo={key[3]} ({count + 1} values)"
            for key, count in collisions.most_common(10)
        )
        raise PipelineError(
            "Historical CategoryOptionCombo remapping would create duplicate data value "
            f"keys, so the transfer was blocked before commit: {preview}"
        )

    for value in data_values:
        key = (
            str(value.get("dataElement") or ""),
            str(value.get("categoryOptionCombo") or ""),
        )
        replacement = replacements.get(key)
        if replacement is not None:
            value["categoryOptionCombo"] = replacement

    mappings = [
        {
            "source": source_id,
            "target": target_id,
            "categoryOptions": list(options),
            "dataValues": count,
        }
        for (source_id, target_id, options), count in sorted(
            replacement_counts.items()
        )
    ]
    remapped_count = sum(replacement_counts.values())
    report.update(
        {
            "historicalCombinationCount": len(replacement_counts),
            "remappedValueCount": remapped_count,
            "mappings": mappings,
        }
    )
    _emit_progress(
        progress,
        "source.category_option_combos.audit.done",
        historicalCombinations=len(replacement_counts),
        remappedValues=remapped_count,
    )
    return data_values, report


def _validate_target_data_set_organisation_units(
    target: DHIS2Client,
    context: TransferContext,
    data_values: list[dict[str, Any]],
    *,
    progress: ProgressCallback | None = None,
) -> None:
    """Fail before DHIS2 E8022 when the Data Set is not assigned to value OUs."""
    if context.target_data_set is None:
        raise PipelineError(
            "Cannot audit target Data Set organisation units without a target Data Set."
        )
    _emit_progress(progress, "target.organisation_units.audit.start")
    details = _target_data_set_details(target, context.target_data_set.id)
    assigned_units = _target_data_set_organisation_unit_references(
        details,
        context.target_data_set.id,
    )
    scheme = context.job.id_schemes.organisation_units
    field = "id" if scheme == "uid" else "code"
    assigned_keys = {
        str(item.get(field))
        for item in (details or {}).get("organisationUnits", [])
        if isinstance(item, dict) and item.get(field)
    }
    used_keys = {
        str(item.get("orgUnit"))
        for item in data_values
        if isinstance(item, dict) and item.get("orgUnit")
    }
    missing = sorted(used_keys - assigned_keys)
    if missing:
        raise PipelineError(
            f"Target Data Set {context.target_data_set.id} is not assigned to "
            f"{len(missing)} organisation unit(s) used by the exported values; DHIS2 "
            "would reject them with E8022. Missing assignments: "
            + ", ".join(missing[:10])
            + ". Run reconcile-org-units --apply or enable "
            "target.organisation_units.synchronization.run_before_transfer."
        )
    _emit_progress(
        progress,
        "target.organisation_units.audit.done",
        assigned=len(assigned_units),
        used=len(used_keys),
    )


def _validate_target_category_option_combos(
    target: DHIS2Client,
    context: TransferContext,
    data_values: list[dict[str, Any]],
    *,
    progress: ProgressCallback | None = None,
) -> None:
    """Ensure every value COC exists under the target Data Element current combo."""
    if not data_values or context.target_data_set is None:
        return
    data_element_scheme = context.job.id_schemes.data_elements
    combo_scheme = context.job.id_schemes.category_option_combos
    data_elements = _metadata_by_key(
        context.target_data_elements,
        data_element_scheme,
    )
    current_combo_ids = sorted(
        {
            combo_id
            for item in context.target_data_elements
            if (combo_id := _reference_uid(item.get("categoryCombo")))
        }
    )
    if not current_combo_ids:
        return

    _emit_progress(progress, "target.category_option_combos.audit.start")
    target_combos = target.category_option_combos_for_category_combos(
        current_combo_ids
    )
    identifier_field = "id" if combo_scheme == "uid" else "code"
    target_by_identifier = {
        str(item[identifier_field]): item
        for item in target_combos
        if item.get(identifier_field)
    }
    conflicts: Counter[tuple[str, str, str]] = Counter()
    for value in data_values:
        data_element_key = str(value.get("dataElement") or "")
        value_combo_key = str(value.get("categoryOptionCombo") or "")
        if not value_combo_key:
            continue
        element = data_elements.get(data_element_key)
        expected_combo_id = (
            _reference_uid(element.get("categoryCombo")) if element else None
        )
        target_combo = target_by_identifier.get(value_combo_key)
        actual_combo_id = (
            _reference_uid(target_combo.get("categoryCombo"))
            if target_combo
            else None
        )
        if not expected_combo_id or actual_combo_id != expected_combo_id:
            conflicts[
                (
                    data_element_key or "<missing>",
                    value_combo_key,
                    expected_combo_id or "<missing>",
                )
            ] += 1

    if conflicts:
        preview = "; ".join(
            f"Data Element {data_element}, CategoryOptionCombo {combo}, "
            f"expected CategoryCombo {expected} ({count} values)"
            for (data_element, combo, expected), count in conflicts.most_common(10)
        )
        raise PipelineError(
            "Target CategoryOptionCombo preflight failed; DHIS2 would reject these values "
            f"with E8024: {preview}"
        )
    _emit_progress(
        progress,
        "target.category_option_combos.audit.done",
        categoryOptionCombinations=len(target_by_identifier),
    )


def _validate_target_chunks(
    target: DHIS2Client,
    chunks: list[dict[str, Any]],
    *,
    context: TransferContext,
    state_dir: Path,
    current_run_id: str,
    progress: ProgressCallback | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Dry-run chunks, adaptively split transient failures, and quarantine E8031.

    E8031 is returned when an organisation unit is not active for the supplied
    period.  DHIS2 is authoritative here: using the indexes from its validation
    response avoids reimplementing version-specific opening/closing date rules.
    """
    exclusion_enabled = (
        context.job.organisation_units.synchronization.conflict_policy == "exclude"
    )
    responses: list[dict[str, Any]] = []
    accepted_chunks: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    adaptive = context.job.transfer.adaptive_chunking
    adaptive_stats: dict[str, Any] = {
        "enabled": adaptive.enabled,
        "minimumChunkSize": adaptive.minimum_chunk_size,
        "retryBeforeSplit": adaptive.retry_before_split,
        "retries": 0,
        "splits": 0,
        "smallestChunkSize": None,
    }
    original_chunk_count = len(chunks)
    original_value_count = sum(
        len(chunk.get("dataValues", []))
        for chunk in chunks
        if isinstance(chunk.get("dataValues"), list)
    )
    _emit_progress(
        progress,
        "target.dry_run.start",
        chunks=original_chunk_count,
        dataValues=original_value_count,
    )

    for index, original_chunk in enumerate(chunks, start=1):
        chunk_started = time.monotonic()
        original_values = original_chunk.get("dataValues")
        if not isinstance(original_values, list):
            raise PipelineError(f"Target data dry-run chunk {index} has no dataValues list.")
        values = list(original_values)
        original_indexes = list(range(len(values)))
        quarantined_in_chunk = 0
        _emit_progress(
            progress,
            "target.dry_run.chunk.start",
            chunk=f"{index}/{original_chunk_count}",
            dataValues=len(values),
        )
        quarantined_before = len(quarantined)
        chunk_responses, chunk_candidates = _validate_target_chunk_adaptive(
            target,
            original_chunk,
            values=values,
            original_indexes=original_indexes,
            original_chunk_index=index,
            original_chunk_count=original_chunk_count,
            context=context,
            exclusion_enabled=exclusion_enabled,
            quarantined=quarantined,
            state_dir=state_dir,
            current_run_id=current_run_id,
            original_value_count=original_value_count,
            adaptive_stats=adaptive_stats,
            progress=progress,
        )
        responses.extend(chunk_responses)
        accepted_chunks.extend(chunk_candidates)
        quarantined_in_chunk = len(quarantined) - quarantined_before
        accepted_in_chunk = sum(
            len(candidate.get("dataValues", [])) for candidate in chunk_candidates
        )

        _emit_progress(
            progress,
            "target.dry_run.chunk.done",
            chunk=f"{index}/{original_chunk_count}",
            acceptedDataValues=accepted_in_chunk,
            quarantinedDataValues=quarantined_in_chunk,
            requestChunks=len(chunk_candidates),
            durationSeconds=round(time.monotonic() - chunk_started, 2),
        )

    report = _timeliness_quarantine_report(
        quarantined,
        enabled=exclusion_enabled,
        state_dir=state_dir,
        current_run_id=current_run_id,
        source_data_value_count=original_value_count,
    )
    if (
        original_value_count
        and int(report["quarantinedDataValueCount"]) == original_value_count
    ):
        raise PipelineError(
            "E8031 quarantine would remove every source data value in this execution "
            f"window. Transfer blocked. Audit: {report['artifact']}"
        )
    report["adaptiveChunking"] = adaptive_stats
    _emit_progress(
        progress,
        "target.dry_run.done",
        chunks=original_chunk_count,
        acceptedChunks=len(accepted_chunks),
        acceptedDataValues=original_value_count
        - int(report["quarantinedDataValueCount"]),
        quarantinedDataValues=report["quarantinedDataValueCount"],
    )
    return responses, accepted_chunks, report


def _validate_target_chunk_adaptive(
    target: DHIS2Client,
    payload: dict[str, Any],
    *,
    values: list[dict[str, Any]],
    original_indexes: list[int],
    original_chunk_index: int,
    original_chunk_count: int,
    context: TransferContext,
    exclusion_enabled: bool,
    quarantined: list[dict[str, Any]],
    state_dir: Path,
    current_run_id: str,
    original_value_count: int,
    adaptive_stats: dict[str, Any],
    progress: ProgressCallback | None,
    split_depth: int = 0,
    part: str = "root",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not values:
        return [], []
    retry = 0
    adaptive = context.job.transfer.adaptive_chunking
    while values:
        candidate = {**payload, "dataValues": values}
        try:
            response = target.import_data_value_set(
                candidate,
                dry_run=True,
                import_strategy=context.job.transfer.import_strategy,
                preheat_cache=context.job.transfer.preheat_cache,
                async_mode=False,
                id_schemes=context.job.id_schemes.api_params(),
            )
        except HttpRequestError as exc:
            rejected_indexes = _exclusive_e8031_indexes(
                exc.body,
                value_count=len(values),
            )
            if (
                exclusion_enabled
                and exc.status_code == 409
                and rejected_indexes is not None
            ):
                rejected = set(rejected_indexes)
                for candidate_index in rejected_indexes:
                    quarantined.append(
                        {
                            "chunk": original_chunk_index,
                            "index": original_indexes[candidate_index],
                            "reason": (
                                "Target organisation unit is not active for the "
                                "data value period"
                            ),
                            "dataValue": values[candidate_index],
                        }
                    )
                values = [
                    value
                    for candidate_index, value in enumerate(values)
                    if candidate_index not in rejected
                ]
                original_indexes = [
                    value
                    for candidate_index, value in enumerate(original_indexes)
                    if candidate_index not in rejected
                ]
                partial_report = _timeliness_quarantine_report(
                    quarantined,
                    enabled=exclusion_enabled,
                    state_dir=state_dir,
                    current_run_id=current_run_id,
                    source_data_value_count=original_value_count,
                )
                _emit_progress(
                    progress,
                    "target.timeliness.quarantine",
                    chunk=f"{original_chunk_index}/{original_chunk_count}",
                    part=part,
                    errorCode="E8031",
                    quarantinedDataValues=len(rejected_indexes),
                    remainingDataValues=len(values),
                    artifact=partial_report["artifact"],
                )
                retry = 0
                continue
            if not adaptive.enabled or not _is_adaptive_chunk_error(exc):
                raise
            if retry < adaptive.retry_before_split:
                retry += 1
                adaptive_stats["retries"] += 1
                delay = adaptive.backoff_seconds * retry
                _emit_progress(
                    progress,
                    "target.dry_run.chunk.retry",
                    chunk=f"{original_chunk_index}/{original_chunk_count}",
                    part=part,
                    attempt=retry,
                    dataValues=len(values),
                    delaySeconds=delay,
                )
                time.sleep(delay)
                continue
            if len(values) // 2 < adaptive.minimum_chunk_size:
                _emit_progress(
                    progress,
                    "target.dry_run.chunk.minimum_failed",
                    chunk=f"{original_chunk_index}/{original_chunk_count}",
                    part=part,
                    dataValues=len(values),
                    minimumChunkSize=adaptive.minimum_chunk_size,
                )
                raise
            midpoint = len(values) // 2
            adaptive_stats["splits"] += 1
            _emit_progress(
                progress,
                "target.dry_run.chunk.split",
                chunk=f"{original_chunk_index}/{original_chunk_count}",
                part=part,
                dataValues=len(values),
                leftDataValues=midpoint,
                rightDataValues=len(values) - midpoint,
                splitDepth=split_depth + 1,
            )
            child_responses: list[dict[str, Any]] = []
            child_candidates: list[dict[str, Any]] = []
            for suffix, child_values, child_indexes in (
                ("L", values[:midpoint], original_indexes[:midpoint]),
                ("R", values[midpoint:], original_indexes[midpoint:]),
            ):
                responses, candidates = _validate_target_chunk_adaptive(
                    target,
                    payload,
                    values=child_values,
                    original_indexes=child_indexes,
                    original_chunk_index=original_chunk_index,
                    original_chunk_count=original_chunk_count,
                    context=context,
                    exclusion_enabled=exclusion_enabled,
                    quarantined=quarantined,
                    state_dir=state_dir,
                    current_run_id=current_run_id,
                    original_value_count=original_value_count,
                    adaptive_stats=adaptive_stats,
                    progress=progress,
                    split_depth=split_depth + 1,
                    part=f"{part}.{suffix}",
                )
                child_responses.extend(responses)
                child_candidates.extend(candidates)
            return child_responses, child_candidates

        _assert_success_response(
            response,
            f"Target data dry-run chunk {original_chunk_index} ({part})",
            allow_warning=True,
        )
        conflicts = extract_conflicts(response)
        if conflicts:
            raise PipelineError(
                f"Target data dry-run chunk {original_chunk_index} ({part}) reported "
                f"conflicts: {json.dumps(conflicts, sort_keys=True)[:1000]}"
            )
        smallest = adaptive_stats.get("smallestChunkSize")
        adaptive_stats["smallestChunkSize"] = (
            len(values) if smallest is None else min(int(smallest), len(values))
        )
        return [
            {
                "chunk": original_chunk_index,
                "part": part,
                "response": response,
            }
        ], [candidate]
    return [], []


def _import_target_chunks(
    target: DHIS2Client,
    chunks: list[dict[str, Any]],
    *,
    context: TransferContext,
    state_dir: Path,
    resume_checkpoints: bool,
    progress: ProgressCallback | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    adaptive = context.job.transfer.adaptive_chunking
    checkpoint_path, checkpoint = _load_chunk_checkpoint(
        state_dir,
        target,
        context,
        enabled=resume_checkpoints,
    )
    stats: dict[str, Any] = {
        "enabled": adaptive.enabled,
        "minimumChunkSize": adaptive.minimum_chunk_size,
        "retryBeforeSplit": adaptive.retry_before_split,
        "retries": 0,
        "splits": 0,
        "smallestChunkSize": None,
        "resumedChunks": 0,
        "resumedDataValues": 0,
        "checkpoint": str(checkpoint_path) if checkpoint_path else None,
    }
    responses: list[dict[str, Any]] = []
    for index, chunk in enumerate(chunks, start=1):
        chunk_started = time.monotonic()
        values = chunk.get("dataValues")
        if not isinstance(values, list):
            raise PipelineError(f"Target data import chunk {index} has no dataValues list.")
        _emit_progress(
            progress,
            "target.import.chunk.start",
            chunk=f"{index}/{len(chunks)}",
            dataValues=len(values),
        )
        chunk_responses = _import_target_chunk_adaptive(
            target,
            chunk,
            original_chunk_index=index,
            original_chunk_count=len(chunks),
            context=context,
            checkpoint_path=checkpoint_path,
            checkpoint=checkpoint,
            stats=stats,
            progress=progress,
        )
        responses.extend(chunk_responses)
        _emit_progress(
            progress,
            "target.import.chunk.done",
            chunk=f"{index}/{len(chunks)}",
            requestChunks=len(chunk_responses),
            dataValues=len(values),
            durationSeconds=round(time.monotonic() - chunk_started, 2),
        )
    return responses, stats


def _import_target_chunk_adaptive(
    target: DHIS2Client,
    payload: dict[str, Any],
    *,
    original_chunk_index: int,
    original_chunk_count: int,
    context: TransferContext,
    checkpoint_path: Path | None,
    checkpoint: dict[str, Any],
    stats: dict[str, Any],
    progress: ProgressCallback | None,
    split_depth: int = 0,
    part: str = "root",
) -> list[dict[str, Any]]:
    values = payload.get("dataValues")
    if not isinstance(values, list):
        raise PipelineError(
            f"Target data import chunk {original_chunk_index} ({part}) has no "
            "dataValues list."
        )
    if not values:
        return []
    chunk_hash = _data_value_chunk_hash(payload)
    completed = checkpoint.get("completedChunks") or {}
    saved = completed.get(chunk_hash) if isinstance(completed, dict) else None
    if isinstance(saved, dict):
        stats["resumedChunks"] += 1
        stats["resumedDataValues"] += len(values)
        _emit_progress(
            progress,
            "target.import.chunk.resume",
            chunk=f"{original_chunk_index}/{original_chunk_count}",
            part=part,
            dataValues=len(values),
            checkpoint=str(checkpoint_path),
        )
        response = saved.get("response")
        if not isinstance(response, dict):
            response = {
                "status": "SUCCESS",
                "importCount": {
                    "imported": 0,
                    "updated": 0,
                    "ignored": len(values),
                    "deleted": 0,
                },
            }
        return [
            {
                "chunk": original_chunk_index,
                "part": part,
                "checkpoint": True,
                "response": response,
            }
        ]

    adaptive = context.job.transfer.adaptive_chunking
    retry = 0
    while True:
        try:
            response = target.import_data_value_set(
                payload,
                dry_run=False,
                import_strategy=context.job.transfer.import_strategy,
                preheat_cache=context.job.transfer.preheat_cache,
                async_mode=False,
                id_schemes=context.job.id_schemes.api_params(),
            )
        except HttpRequestError as exc:
            if not adaptive.enabled or not _is_adaptive_chunk_error(exc):
                raise
            if retry < adaptive.retry_before_split:
                retry += 1
                stats["retries"] += 1
                delay = adaptive.backoff_seconds * retry
                _emit_progress(
                    progress,
                    "target.import.chunk.retry",
                    chunk=f"{original_chunk_index}/{original_chunk_count}",
                    part=part,
                    attempt=retry,
                    dataValues=len(values),
                    delaySeconds=delay,
                    outcome="unknown",
                )
                time.sleep(delay)
                continue
            if len(values) // 2 < adaptive.minimum_chunk_size:
                _emit_progress(
                    progress,
                    "target.import.chunk.minimum_failed",
                    chunk=f"{original_chunk_index}/{original_chunk_count}",
                    part=part,
                    dataValues=len(values),
                    minimumChunkSize=adaptive.minimum_chunk_size,
                    outcome="unknown",
                )
                raise
            midpoint = len(values) // 2
            stats["splits"] += 1
            _emit_progress(
                progress,
                "target.import.chunk.split",
                chunk=f"{original_chunk_index}/{original_chunk_count}",
                part=part,
                dataValues=len(values),
                leftDataValues=midpoint,
                rightDataValues=len(values) - midpoint,
                splitDepth=split_depth + 1,
                outcome="unknown_replayed_with_create_and_update",
            )
            responses: list[dict[str, Any]] = []
            for suffix, child_values in (
                ("L", values[:midpoint]),
                ("R", values[midpoint:]),
            ):
                child_payload = {**payload, "dataValues": child_values}
                responses.extend(
                    _import_target_chunk_adaptive(
                        target,
                        child_payload,
                        original_chunk_index=original_chunk_index,
                        original_chunk_count=original_chunk_count,
                        context=context,
                        checkpoint_path=checkpoint_path,
                        checkpoint=checkpoint,
                        stats=stats,
                        progress=progress,
                        split_depth=split_depth + 1,
                        part=f"{part}.{suffix}",
                    )
                )
            return responses

        _assert_success_response(
            response,
            f"Target data import chunk {original_chunk_index} ({part})",
            allow_warning=True,
        )
        conflicts = extract_conflicts(response)
        if conflicts:
            raise PipelineError(
                f"Target data import chunk {original_chunk_index} ({part}) reported "
                f"conflicts: {json.dumps(conflicts, sort_keys=True)[:1000]}"
            )
        smallest = stats.get("smallestChunkSize")
        stats["smallestChunkSize"] = (
            len(values) if smallest is None else min(int(smallest), len(values))
        )
        if checkpoint_path is not None:
            completed = checkpoint.setdefault("completedChunks", {})
            completed[chunk_hash] = {
                "dataValueCount": len(values),
                "completedAtEpoch": int(time.time()),
                "response": response,
            }
            _write_json(checkpoint_path, checkpoint)
            _emit_progress(
                progress,
                "target.import.chunk.checkpoint",
                chunk=f"{original_chunk_index}/{original_chunk_count}",
                part=part,
                dataValues=len(values),
                checkpoint=str(checkpoint_path),
            )
        return [
            {
                "chunk": original_chunk_index,
                "part": part,
                "checkpoint": False,
                "response": response,
            }
        ]


def _load_chunk_checkpoint(
    state_dir: Path,
    target: DHIS2Client,
    context: TransferContext,
    *,
    enabled: bool,
) -> tuple[Path | None, dict[str, Any]]:
    if not enabled:
        return None, {}
    scope = {
        "schemaVersion": 1,
        "job": context.job.id,
        "target": target.base_url,
        "targetDataSet": (
            context.target_data_set.id
            if context.target_data_set is not None
            else context.job.target_data_set
        ),
        "start": context.start,
        "end": context.end,
        "importStrategy": context.job.transfer.import_strategy,
    }
    scope_hash = hashlib.sha256(
        json.dumps(scope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    path = (
        state_dir
        / "checkpoints"
        / "data-values"
        / f"{context.start}__{context.end}__{scope_hash[:16]}.json"
    )
    if not path.exists():
        return path, {**scope, "scopeHash": scope_hash, "completedChunks": {}}
    try:
        checkpoint = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"Chunk checkpoint is unreadable: {path}") from exc
    if not isinstance(checkpoint, dict) or checkpoint.get("scopeHash") != scope_hash:
        raise PipelineError(f"Chunk checkpoint scope is incompatible: {path}")
    if not isinstance(checkpoint.get("completedChunks"), dict):
        raise PipelineError(f"Chunk checkpoint has invalid completedChunks: {path}")
    return path, checkpoint


def _data_value_chunk_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _is_adaptive_chunk_error(exc: HttpRequestError) -> bool:
    text = f"{exc.body} {exc}".lower()
    return exc.status_code in {429, 502, 503, 504, 520, 522, 524} or any(
        marker in text
        for marker in (
            "timed out",
            "timeout",
            "cloudflare",
            "connection reset",
        )
    )


def _exclusive_e8031_indexes(body: str, *, value_count: int) -> list[int] | None:
    """Return safe E8031 indexes only when no other DHIS2 conflict is present."""
    try:
        payload = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        return None
    conflicts = extract_conflicts(payload)
    if not conflicts or any(item.get("errorCode") != "E8031" for item in conflicts):
        return None
    indexes: set[int] = set()
    for conflict in conflicts:
        raw_indexes = conflict.get("indexes")
        if not isinstance(raw_indexes, list) or not raw_indexes:
            return None
        for raw_index in raw_indexes:
            if isinstance(raw_index, bool) or not isinstance(raw_index, int):
                return None
            if raw_index < 0 or raw_index >= value_count:
                return None
            indexes.add(raw_index)
    return sorted(indexes) or None


def _timeliness_quarantine_report(
    quarantined: list[dict[str, Any]],
    *,
    enabled: bool,
    state_dir: Path,
    current_run_id: str,
    source_data_value_count: int,
) -> dict[str, Any]:
    grouped: Counter[tuple[str, str]] = Counter()
    for item in quarantined:
        value = item.get("dataValue")
        if not isinstance(value, dict):
            continue
        grouped[
            (
                str(value.get("orgUnit") or "<missing>"),
                str(value.get("period") or "<missing>"),
            )
        ] += 1
    groups = [
        {"organisationUnit": unit, "period": period, "dataValues": count}
        for (unit, period), count in sorted(grouped.items())
    ]
    artifact: str | None = None
    if quarantined:
        artifact_path = _write_json(
            state_dir
            / "runs"
            / current_run_id
            / "data-value-quarantine"
            / "e8031.json",
            {
                "runId": current_run_id,
                "errorCode": "E8031",
                "reason": (
                    "Target organisation unit is not active for the data value period"
                ),
                "sourceDataValueCount": source_data_value_count,
                "quarantinedDataValueCount": len(quarantined),
                "groups": groups,
                "values": quarantined,
            },
        )
        artifact = str(artifact_path)
    return {
        "enabled": enabled,
        "errorCode": "E8031",
        "quarantinedDataValueCount": len(quarantined),
        "organisationUnitCount": len({item[0] for item in grouped}),
        "periodCount": len({item[1] for item in grouped}),
        "groups": groups,
        "artifact": artifact,
    }


def _quarantine_invalid_data_values(
    data_values: list[dict[str, Any]],
    *,
    state_dir: Path,
    current_run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Remove source rows that DHIS2 would reject with E8120.

    Numeric zero and boolean false are legitimate values.  Only an absent,
    null, or whitespace-only value is quarantined.
    """
    accepted: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    for index, value in enumerate(data_values):
        raw_value = value.get("value")
        if raw_value is None or (
            isinstance(raw_value, str) and not raw_value.strip()
        ):
            quarantined.append(
                {
                    "index": index,
                    "reason": "Source data value is absent, null, or empty",
                    "dataValue": value,
                }
            )
        else:
            accepted.append(value)

    artifact: str | None = None
    if quarantined:
        artifact_path = _write_json(
            state_dir
            / "runs"
            / current_run_id
            / "data-value-quarantine"
            / "e8120.json",
            {
                "runId": current_run_id,
                "errorCode": "E8120",
                "reason": "Source data value is absent, null, or empty",
                "sourceDataValueCount": len(data_values),
                "quarantinedDataValueCount": len(quarantined),
                "values": quarantined,
            },
        )
        artifact = str(artifact_path)
    return accepted, {
        "enabled": True,
        "errorCode": "E8120",
        "quarantinedDataValueCount": len(quarantined),
        "artifact": artifact,
    }


def _chunk_payload(payload: dict[str, Any], chunk_size: int) -> list[dict[str, Any]]:
    values = payload.get("dataValues")
    if not isinstance(values, list):
        raise PipelineError("Transfer payload has no dataValues list.")
    if not values:
        return [{**payload, "dataValues": []}]
    return [
        {**payload, "dataValues": values[index : index + chunk_size]}
        for index in range(0, len(values), chunk_size)
    ]


def _review_payload(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    context: TransferContext,
) -> dict[str, Any]:
    return {
        "scenario": scenario.id,
        "job": context.job.id,
        "sourceInstance": {"profile": scenario.source_instance, "baseUrl": source.base_url},
        "targetInstance": {"profile": scenario.target_instance, "baseUrl": target.base_url},
        "sourceDataSet": asdict(context.source_data_set),
        "targetDataSet": asdict(context.target_data_set) if context.target_data_set else None,
        "sourceDataElementCount": len(context.source_data_elements),
        "targetDataElementCount": len(context.target_data_elements),
        "sourceOrganisationUnitCount": len(context.source_organisation_units),
        "targetMatchedOrganisationUnitCount": len(context.target_organisation_units),
        "organisationUnitExclusions": _organisation_unit_exclusion_payload(context.job),
        "organisationUnitConflictPolicy": (
            context.job.organisation_units.synchronization.conflict_policy
        ),
        "detectedOrganisationUnitConflicts": list(
            context.detected_organisation_unit_conflicts
        ),
        "organisationUnitQuarantineActive": bool(
            context.detected_organisation_unit_conflicts
        ),
        "excludedSourceOrganisationUnitCount": len(
            context.excluded_source_organisation_units
        ),
        "excludedSourceOrganisationUnits": _organisation_unit_summary_payload(
            context.excluded_source_organisation_units
        ),
        "organisationUnitMappingStrategy": context.job.id_schemes.organisation_units,
        "start": context.start,
        "end": context.end,
        "metadataMode": scenario.metadata.mode,
        "metadataBootstrapRequired": context.metadata_bootstrap_required,
        "stateDirectory": str(scenario.job_state_dir(context.job.id)),
    }


def _write_metadata_caches(
    state_dir: Path,
    source: DHIS2Client,
    target: DHIS2Client,
    context: TransferContext,
) -> None:
    _write_json(
        state_dir / "cache" / "source" / f"{context.job.id}.json",
        {
            "source": source.base_url,
            "dataSet": asdict(context.source_data_set),
            "dataElements": list(context.source_data_elements),
            "organisationUnits": list(context.source_organisation_units),
            "organisationUnitExclusions": _organisation_unit_exclusion_payload(
                context.job
            ),
            "organisationUnitConflictPolicy": (
                context.job.organisation_units.synchronization.conflict_policy
            ),
            "detectedOrganisationUnitConflicts": list(
                context.detected_organisation_unit_conflicts
            ),
            "excludedSourceOrganisationUnits": list(
                context.excluded_source_organisation_units
            ),
        },
    )
    _write_json(
        state_dir / "cache" / "target" / f"{context.job.id}.json",
        {
            "source": target.base_url,
            "dataSet": asdict(context.target_data_set) if context.target_data_set else None,
            "dataElements": list(context.target_data_elements),
            "organisationUnits": list(context.target_organisation_units),
            "organisationUnitExclusions": _organisation_unit_exclusion_payload(
                context.job
            ),
            "excludedSourceOrganisationUnits": _organisation_unit_summary_payload(
                context.excluded_source_organisation_units
            ),
        },
    )


def _organisation_unit_exclusion_payload(
    job: TransferJobConfig,
) -> list[dict[str, str]]:
    return [asdict(item) for item in job.organisation_units.exclusions]


def _organisation_unit_conflict_payload(
    conflicts: dict[str, tuple[str, ...]] | dict[str, list[str]],
) -> list[dict[str, Any]]:
    return [
        {"uid": uid, "reasons": list(conflicts[uid])}
        for uid in sorted(conflicts)
    ]


def _organisation_unit_quarantine_path(
    scenario: TransferScenario,
    job: TransferJobConfig,
) -> Path:
    return (
        scenario.job_state_dir(job.id)
        / "cache"
        / "organisation-unit-quarantine.json"
    )


def _write_organisation_unit_quarantine(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    job: TransferJobConfig,
    source_data_set: DataSetRef,
    *,
    conflicts: dict[str, tuple[str, ...]],
    excluded_source_units: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> Path:
    return _write_json(
        _organisation_unit_quarantine_path(scenario, job),
        {
            "schemaVersion": 1,
            "scenario": scenario.id,
            "job": job.id,
            "sourceInstance": source.base_url,
            "targetInstance": target.base_url,
            "sourceDataSet": source_data_set.id,
            "conflictPolicy": job.organisation_units.synchronization.conflict_policy,
            "detectedConflicts": _organisation_unit_conflict_payload(conflicts),
            "excludedSourceOrganisationUnits": _organisation_unit_summary_payload(
                excluded_source_units
            ),
        },
    )


def _load_organisation_unit_quarantine(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    job: TransferJobConfig,
    source_data_set: DataSetRef,
) -> dict[str, tuple[str, ...]]:
    path = _organisation_unit_quarantine_path(scenario, job)
    if not path.exists():
        raise PipelineError(
            "Organisation-unit quarantine manifest is missing; run the guarded "
            f"organisation-unit reconciliation first: {path}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(
            f"Organisation-unit quarantine manifest is unreadable: {path}"
        ) from exc
    expected = {
        "scenario": scenario.id,
        "job": job.id,
        "sourceInstance": source.base_url,
        "targetInstance": target.base_url,
        "sourceDataSet": source_data_set.id,
        "conflictPolicy": "exclude",
    }
    mismatches = [
        f"{key}={payload.get(key)!r} (expected {value!r})"
        for key, value in expected.items()
        if payload.get(key) != value
    ]
    if payload.get("schemaVersion") != 1 or mismatches:
        detail = "; ".join(mismatches[:10]) or "unsupported schema version"
        raise PipelineError(
            f"Organisation-unit quarantine manifest is stale or incompatible: {detail}. "
            f"Manifest: {path}"
        )
    raw_conflicts = payload.get("detectedConflicts")
    if not isinstance(raw_conflicts, list):
        raise PipelineError(
            f"Organisation-unit quarantine manifest has no conflict list: {path}"
        )
    conflicts: dict[str, tuple[str, ...]] = {}
    for item in raw_conflicts:
        if not isinstance(item, dict) or not item.get("uid"):
            raise PipelineError(
                f"Organisation-unit quarantine manifest contains an invalid conflict: {path}"
            )
        reasons = item.get("reasons")
        if not isinstance(reasons, list) or not all(
            isinstance(reason, str) and reason for reason in reasons
        ):
            raise PipelineError(
                f"Organisation-unit quarantine manifest contains invalid reasons: {path}"
            )
        conflicts[str(item["uid"])] = tuple(reasons)
    return conflicts


def _organisation_unit_summary_payload(
    units: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fields = ("id", "code", "name", "path", "level")
    return [
        {field: unit[field] for field in fields if unit.get(field) is not None}
        for unit in units
    ]


def _resolve_window(
    job: TransferJobConfig,
    *,
    start: str | None,
    end: str | None,
    start_period: str | None,
    end_period: str | None,
    years_back: int | None,
    as_of: str | None,
) -> tuple[str | None, str | None]:
    if years_back is not None:
        if start or start_period:
            raise PipelineError("--years-back cannot be combined with --start or --start-period.")
        return relative_year_window(
            years_back,
            job.period_type,
            end=end,
            end_period=end_period,
            as_of=as_of,
        )
    if as_of:
        raise PipelineError("--as-of can only be used with --years-back.")
    if start or end:
        resolved_start = start or job.default_start
        resolved_end = end or job.default_end
        if resolved_start and resolved_end:
            return date_window_period_bounds(
                resolved_start, resolved_end, job.period_type
            )
        return resolved_start, resolved_end
    resolved_start_period = start_period or job.default_start_period
    resolved_end_period = end_period or job.default_end_period
    if resolved_start_period and resolved_end_period:
        return period_range_bounds(
            resolved_start_period, resolved_end_period, job.period_type
        )
    return job.default_start, job.default_end


def _metadata_keys(
    items: tuple[dict[str, Any], ...], scheme: str, label: str
) -> set[str]:
    field = "id" if scheme == "uid" else "code"
    keys: list[str] = []
    for item in items:
        value = item.get(field)
        if not isinstance(value, str) or not value:
            raise PipelineError(f"{label} contains an object without {field}.")
        keys.append(value)
    if len(keys) != len(set(keys)):
        raise PipelineError(f"{label} contains duplicate {field} values.")
    return set(keys)


def _metadata_by_key(
    items: tuple[dict[str, Any], ...], scheme: str
) -> dict[str, dict[str, Any]]:
    field = "id" if scheme == "uid" else "code"
    return {str(item[field]): item for item in items if item.get(field)}


def _reference_key(value: Any, scheme: str) -> str | None:
    if not isinstance(value, dict):
        return None
    field = "id" if scheme == "uid" else "code"
    result = value.get(field)
    return str(result) if result else None


def _data_value_key(value: dict[str, Any]) -> tuple[str, ...]:
    required = ("dataElement", "period", "orgUnit")
    missing = [field for field in required if not value.get(field)]
    if missing:
        raise PipelineError(
            f"Source data value is missing required fields: {', '.join(missing)}"
        )
    return tuple(
        str(value.get(field) or "")
        for field in (
            "dataElement",
            "period",
            "orgUnit",
            "categoryOptionCombo",
            "attributeOptionCombo",
        )
    )


def _normalise_data_value(value: dict[str, Any]) -> dict[str, Any]:
    allowed_fields = {
        "dataElement",
        "period",
        "orgUnit",
        "categoryOptionCombo",
        "attributeOptionCombo",
        "value",
        "comment",
        "followUp",
    }
    return {key: child for key, child in value.items() if key in allowed_fields}


def _assert_success_response(
    response: Any, label: str, *, allow_warning: bool = False
) -> None:
    statuses: list[str] = []
    error_reports: list[Any] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            status = value.get("status") or value.get("httpStatus")
            if isinstance(status, str):
                statuses.append(status.upper())
            reports = value.get("errorReports")
            if isinstance(reports, list) and reports:
                error_reports.extend(reports)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(response)
    failure_statuses = {"ERROR", "FAILURE", "FAILED", "CONFLICT"}
    if not allow_warning:
        failure_statuses.add("WARNING")
    if error_reports or any(status in failure_statuses for status in statuses):
        raise PipelineError(f"{label} failed: {json.dumps(response, sort_keys=True)[:1000]}")


def _emit_progress(
    progress: ProgressCallback | None,
    event: str,
    **details: Any,
) -> None:
    if progress is not None:
        progress(event, details)


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
