from __future__ import annotations

from copy import deepcopy
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any

from .dhis2 import DHIS2Client, DataSetRef
from .errors import PipelineError
from .http import HttpRequestError
from .journal import TransferFingerprint, TransferRunJournal
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
    _write_metadata_caches(scenario.state_dir, source, target, context)
    return _review_payload(scenario, source, target, context)


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
        start=context.start,
        end=context.end,
        metadataBootstrap=context.metadata_bootstrap_required,
    )
    if context.job.transfer.include_deleted:
        raise PipelineError(
            "Transfer of deleted data values is intentionally disabled in this version. "
            "Set transfer.include_deleted to false."
        )

    state_dir = scenario.state_dir
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

        source_payload = _export_source_values(source, context, progress=progress)
        source_values = source_payload["dataValues"]
        source_count = len(source_values)
        if not context.job.transfer.preserve_comments:
            source_values = [
                {key: value for key, value in item.items() if key != "comment"}
                for item in source_values
            ]
            source_payload = {"dataValues": source_values}
        if keep_payload:
            _write_json(
                state_dir / "artifacts" / "data" / f"{current_run_id}.values.json",
                source_payload,
            )

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
            context = TransferContext(
                job=context.job,
                source_data_set=context.source_data_set,
                target_data_set=target_data_set,
                source_data_elements=context.source_data_elements,
                target_data_elements=target_elements,
                source_organisation_units=context.source_organisation_units,
                target_organisation_units=context.target_organisation_units,
                start=context.start,
                end=context.end,
                metadata_bootstrap_required=True,
            )
            _validate_data_set_compatibility(context)
            _write_metadata_caches(state_dir, source, target, context)

        dry_run_responses: list[dict[str, Any]] = []
        apply_responses: list[dict[str, Any]] = []
        if context.target_data_set is not None:
            target_payload = _target_payload(context, source_values)
            chunks = _chunk_payload(target_payload, context.job.transfer.chunk_size)
            _emit_progress(
                progress,
                "target.dry_run.start",
                chunks=len(chunks),
                dataValues=len(source_values),
            )
            for index, chunk in enumerate(chunks, start=1):
                chunk_started = time.monotonic()
                _emit_progress(
                    progress,
                    "target.dry_run.chunk.start",
                    chunk=f"{index}/{len(chunks)}",
                    dataValues=len(chunk["dataValues"]),
                )
                response = target.import_data_value_set(
                    chunk,
                    dry_run=True,
                    import_strategy=context.job.transfer.import_strategy,
                    preheat_cache=context.job.transfer.preheat_cache,
                    async_mode=False,
                    id_schemes=context.job.id_schemes.api_params(),
                )
                _assert_success_response(
                    response, f"Target data dry-run chunk {index}", allow_warning=True
                )
                conflicts = extract_conflicts(response)
                if conflicts:
                    raise PipelineError(
                        f"Target data dry-run chunk {index} reported conflicts: "
                        f"{json.dumps(conflicts, sort_keys=True)[:1000]}"
                    )
                dry_run_responses.append({"chunk": index, "response": response})
                _emit_progress(
                    progress,
                    "target.dry_run.chunk.done",
                    chunk=f"{index}/{len(chunks)}",
                    durationSeconds=round(time.monotonic() - chunk_started, 2),
                )
            _emit_progress(progress, "target.dry_run.done", chunks=len(chunks))
            if apply:
                _emit_progress(
                    progress,
                    "target.import.start",
                    chunks=len(chunks),
                    dataValues=len(source_values),
                )
                for index, chunk in enumerate(chunks, start=1):
                    chunk_started = time.monotonic()
                    _emit_progress(
                        progress,
                        "target.import.chunk.start",
                        chunk=f"{index}/{len(chunks)}",
                        dataValues=len(chunk["dataValues"]),
                    )
                    response = target.import_data_value_set(
                        chunk,
                        dry_run=False,
                        import_strategy=context.job.transfer.import_strategy,
                        preheat_cache=context.job.transfer.preheat_cache,
                        async_mode=False,
                        id_schemes=context.job.id_schemes.api_params(),
                    )
                    _assert_success_response(
                        response, f"Target data import chunk {index}", allow_warning=True
                    )
                    conflicts = extract_conflicts(response)
                    if conflicts:
                        raise PipelineError(
                            f"Target data import chunk {index} reported conflicts: "
                            f"{json.dumps(conflicts, sort_keys=True)[:1000]}"
                        )
                    apply_responses.append({"chunk": index, "response": response})
                    _emit_progress(
                        progress,
                        "target.import.chunk.done",
                        chunk=f"{index}/{len(chunks)}",
                        durationSeconds=round(time.monotonic() - chunk_started, 2),
                    )
                _emit_progress(progress, "target.import.done", chunks=len(chunks))
        elif apply:
            raise PipelineError("Target Data Set is unavailable after metadata bootstrap.")

        counts = import_counts(apply_responses if apply else dry_run_responses)
        report = {
            "runId": current_run_id,
            "scenario": scenario.id,
            "job": context.job.id,
            "dryRun": not apply,
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
            "chunkCount": math.ceil(source_count / context.job.transfer.chunk_size)
            if source_count
            else 0,
            "metadata": metadata_report,
            "dataValidationPendingMetadataCommit": bool(
                context.target_data_set is None and not apply
            ),
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
    state_dir = scenario.state_dir
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

    structure_validation = target.import_metadata(
        structure_package,
        commit=False,
        import_strategy="UPDATE",
        atomic_mode="ALL",
        identifier="UID",
        skip_sharing=scenario.metadata.skip_sharing,
    )
    _assert_success_response(structure_validation, "Target CategoryCombo repair validation")

    structure_commit: dict[str, Any] | None = None
    assignment_validation: dict[str, Any] | None = None
    assignment_commit: dict[str, Any] | None = None
    assignment_validation_pending = True
    if apply:
        structure_commit = target.import_metadata(
            structure_package,
            commit=True,
            import_strategy="UPDATE",
            atomic_mode="ALL",
            identifier="UID",
            skip_sharing=scenario.metadata.skip_sharing,
        )
        _assert_success_response(structure_commit, "Target CategoryCombo repair")

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
        _write_json(assignment_path, assignment_package)
        assignment_validation = target.import_metadata(
            assignment_package,
            commit=False,
            import_strategy="CREATE_AND_UPDATE",
            atomic_mode="ALL",
            identifier="UID",
            skip_sharing=scenario.metadata.skip_sharing,
        )
        _assert_success_response(
            assignment_validation,
            "Target CategoryOptionCombo/DataElement repair validation",
        )
        assignment_validation_pending = False
        assignment_commit = target.import_metadata(
            assignment_package,
            commit=True,
            import_strategy="CREATE_AND_UPDATE",
            atomic_mode="ALL",
            identifier="UID",
            skip_sharing=scenario.metadata.skip_sharing,
        )
        _assert_success_response(
            assignment_commit,
            "Target CategoryOptionCombo/DataElement repair",
        )
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
            "validated": True,
            "committed": apply,
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
        "nextStep": (
            "Run repair-metadata with --apply, then rerun the normal transfer dry-run."
            if not apply
            else "Rerun the normal transfer with --dry-run before importing data."
        ),
    }
    report_path = write_report(state_dir, current_run_id, report)
    report["reportPath"] = str(report_path)
    return report


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
    source_units = tuple(_resolve_source_units(source, source_data_set, job))
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
    )
    repairs = _validate_data_set_compatibility(
        context,
        allow_metadata_repair=allow_metadata_repair,
    )
    return replace(context, category_combo_repairs=repairs)


def _validate_data_set_compatibility(
    context: TransferContext,
    *,
    allow_metadata_repair: bool = False,
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
    if context.target_data_set and missing:
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
    policy = job.organisation_units
    if policy.discovery == "configured_roots":
        return [source.resolve_organisation_unit(item) for item in policy.source_roots]
    return source.organisation_units_for_data_set(data_set.id)


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
    if not structure_package["categoryCombos"]:
        raise PipelineError(
            "Metadata repair found no empty target CategoryCombo structure to update."
        )
    assignment_package = {
        "categoryOptionCombos": source_option_combos,
        "dataElements": [deepcopy(source_elements[item]) for item in sorted(repair_ids)],
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
            raise PipelineError(
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
        "organisationUnitMappingStrategy": context.job.id_schemes.organisation_units,
        "start": context.start,
        "end": context.end,
        "metadataMode": scenario.metadata.mode,
        "metadataBootstrapRequired": context.metadata_bootstrap_required,
        "stateDirectory": str(scenario.state_dir),
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
        },
    )
    _write_json(
        state_dir / "cache" / "target" / f"{context.job.id}.json",
        {
            "source": target.base_url,
            "dataSet": asdict(context.target_data_set) if context.target_data_set else None,
            "dataElements": list(context.target_data_elements),
            "organisationUnits": list(context.target_organisation_units),
        },
    )


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
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
