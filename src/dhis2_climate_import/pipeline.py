from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

from .cache import (
    load_data_elements_cache_payload,
    load_org_units_cache,
    resolve_data_element,
)
from .config import ImportJobConfig, PipelineConfig
from .dhis2 import DHIS2Client
from .errors import PipelineError
from .journal import ImportFingerprint, RunJournal
from .ocs import OCSClient
from .periods import (
    date_window_period_bounds,
    month_bounds,
    period_labels_between,
    period_range_bounds,
    periods_between,
    relative_year_window,
)
from .reports import extract_conflicts, import_counts, run_id, write_errors_csv, write_report


@dataclass(frozen=True)
class ResolvedImport:
    job: ImportJobConfig
    data_set: str | None
    data_element: dict[str, Any]
    org_unit_level: int
    org_unit_count: int
    expected_org_units_with_geometry: int | None
    expected_data_values: int | None
    org_units_geojson: dict[str, Any]
    org_units_manifest: dict[str, Any]
    start: str
    end: str
    period_labels: list[str]


def resolve_import_context(
    config: PipelineConfig,
    *,
    job_id: str,
    start: str | None = None,
    end: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
    org_unit_level: int | None = None,
    data_set: str | None = None,
    expected_org_units: int | None = None,
    expected_data_values: int | None = None,
) -> ResolvedImport:
    job = config.get_job(job_id)
    resolved_start, resolved_end = _resolve_date_window(
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
            f"Import job '{job.id}' needs --start and --end or default_start/default_end."
        )

    resolved_org_unit_level = (
        org_unit_level or job.org_unit_level or config.dhis2.org_unit_level
    )
    resolved_data_set = data_set or job.data_set or config.dhis2.climate_data_set
    data_elements_payload = load_data_elements_cache_payload(config.root_dir)
    _validate_data_set_cache_scope(
        data_elements_payload,
        data_set=resolved_data_set,
        period_type=job.period_type,
    )
    data_elements = data_elements_payload.get("dataElements")
    if not isinstance(data_elements, list) or not data_elements:
        raise PipelineError("Data element cache contains no dataElements.")
    data_element = resolve_data_element(data_elements, job.data_element)
    _validate_data_element_data_set_membership(
        data_element,
        payload=data_elements_payload,
        data_set=resolved_data_set,
    )
    org_units_geojson, org_units_manifest = load_org_units_cache(
        root_dir=config.root_dir,
        level=resolved_org_unit_level,
        max_age_hours=config.dhis2.cache_max_age_hours,
    )
    features = org_units_geojson.get("features")
    if not features:
        raise PipelineError("Org unit cache is empty.")
    org_unit_count = len(features)
    resolved_expected_org_units = (
        expected_org_units
        if expected_org_units is not None
        else job.expected_org_units_with_geometry
    )
    if resolved_expected_org_units is not None and org_unit_count != resolved_expected_org_units:
        raise PipelineError(
            f"Import job '{job.id}' expected {resolved_expected_org_units} "
            f"organisation units with geometry at level {resolved_org_unit_level}, but cache has "
            f"{org_unit_count}. Refresh/check: cache-org-units --level {resolved_org_unit_level}"
        )
    resolved_period_labels = period_labels_between(resolved_start, resolved_end, job.period_type)
    context = ResolvedImport(
        job=job,
        data_set=resolved_data_set,
        data_element=data_element,
        org_unit_level=resolved_org_unit_level,
        org_unit_count=org_unit_count,
        expected_org_units_with_geometry=resolved_expected_org_units,
        expected_data_values=expected_data_values,
        org_units_geojson=org_units_geojson,
        org_units_manifest=org_units_manifest,
        start=resolved_start,
        end=resolved_end,
        period_labels=resolved_period_labels,
    )
    _validate_expected_data_values(context)
    return context


def aggregate_job(
    config: PipelineConfig,
    ocs: OCSClient,
    *,
    job_id: str,
    start: str | None = None,
    end: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
    org_unit_level: int | None = None,
    data_set: str | None = None,
    expected_org_units: int | None = None,
    expected_data_values: int | None = None,
) -> dict[str, Any]:
    context = resolve_import_context(
        config,
        job_id=job_id,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
        org_unit_level=org_unit_level,
        data_set=data_set,
        expected_org_units=expected_org_units,
        expected_data_values=expected_data_values,
    )
    payload = ocs.aggregate_to_dhis2_json(
        process_id=config.open_climate_service.aggregate_process_id,
        dataset_id=context.job.dataset_id,
        geometries=context.org_units_geojson,
        data_element_id=str(context.data_element["id"]),
        period_type=context.job.period_type,
        start=context.start,
        end=context.end,
        method=context.job.method,
    )
    _validate_payload_data_value_count(context, payload)
    return payload


def import_job(
    config: PipelineConfig,
    dhis2: DHIS2Client,
    ocs: OCSClient,
    *,
    job_id: str,
    start: str | None,
    end: str | None,
    dry_run: bool,
    force: bool,
    keep_payload: bool,
    poll: bool,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
    org_unit_level: int | None = None,
    data_set: str | None = None,
    expected_org_units: int | None = None,
    expected_data_values: int | None = None,
) -> dict[str, Any]:
    context = resolve_import_context(
        config,
        job_id=job_id,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
        org_unit_level=org_unit_level,
        data_set=data_set,
        expected_org_units=expected_org_units,
        expected_data_values=expected_data_values,
    )
    fingerprint = ImportFingerprint(
        dataset_id=context.job.dataset_id,
        start=context.start,
        end=context.end,
        period_type=context.job.period_type,
        data_element_id=str(context.data_element["id"]),
        org_unit_level=context.org_unit_level,
        org_unit_cache_hash=str(context.org_units_manifest["geojsonSha256"]),
    )
    journal = RunJournal(config.root_dir / "cache" / "import-runs.sqlite3")
    if not dry_run and not force and journal.has_successful_import(fingerprint):
        raise PipelineError(
            "This import already succeeded with the same dataset, period, data element, "
            "and org unit cache hash. Use --force to rerun it."
        )

    current_run_id = run_id(context.job.id)
    try:
        payload = ocs.aggregate_to_dhis2_json(
            process_id=config.open_climate_service.aggregate_process_id,
            dataset_id=context.job.dataset_id,
            geometries=context.org_units_geojson,
            data_element_id=str(context.data_element["id"]),
            period_type=context.job.period_type,
            start=context.start,
            end=context.end,
            method=context.job.method,
        )
        data_values = _validate_payload_data_value_count(context, payload)
        expected_data_values = _expected_data_values(context)
        if keep_payload:
            write_report(config.root_dir, f"{current_run_id}-payload", payload)

        responses: list[Any] = []
        for chunk_index, chunk in enumerate(_chunk_payload(payload, config.dhis2.chunk_size), start=1):
            chunk_data_values = chunk.get("dataValues", [])
            async_mode = (
                not dry_run
                and len(chunk_data_values) >= config.dhis2.async_threshold
            )
            response = dhis2.import_data_value_set(
                chunk,
                dry_run=dry_run,
                import_strategy=config.dhis2.import_strategy,
                preheat_cache=config.dhis2.preheat_cache,
                async_mode=async_mode,
            )
            responses.append({"chunk": chunk_index, "response": response})
            if async_mode and poll:
                task_ref = _task_reference(response)
                if task_ref:
                    task_response = dhis2.wait_for_data_value_import(task_ref)
                    responses.append({"chunk": chunk_index, "task": task_response})

        counts = import_counts(responses)
        status = "success" if counts["ignored"] == 0 else "partial"
        conflicts = extract_conflicts(responses)
        report = {
            "runId": current_run_id,
            "job": context.job.id,
            "datasetId": context.job.dataset_id,
            "dataSet": context.data_set,
            "dataElement": context.data_element,
            "start": context.start,
            "end": context.end,
            "dryRun": dry_run,
            "orgUnitLevel": context.org_unit_level,
            "orgUnitCount": context.org_unit_count,
            "expectedOrgUnitsWithGeometry": context.expected_org_units_with_geometry,
            "orgUnitCacheHash": context.org_units_manifest["geojsonSha256"],
            "periods": context.period_labels,
            "computedDataValueCount": _computed_data_values(context),
            "expectedDataValueCount": expected_data_values,
            "dataValueCount": len(data_values),
            "chunkCount": math.ceil(len(data_values) / config.dhis2.chunk_size)
            if data_values
            else 0,
            "counts": counts,
            "responses": responses,
        }
        report_path = write_report(config.root_dir, current_run_id, report)
        if conflicts:
            write_errors_csv(config.root_dir, f"{current_run_id}-errors", conflicts)
        journal.record(
            run_id=current_run_id,
            command="import-dhis2",
            fingerprint=fingerprint,
            dry_run=dry_run,
            status=status,
            counts=counts,
            report_path=str(report_path),
        )
        if config.dhis2.trigger_analytics and not dry_run:
            dhis2.trigger_analytics_tables()
        return report
    except Exception as exc:
        journal.record(
            run_id=current_run_id,
            command="import-dhis2",
            fingerprint=fingerprint,
            dry_run=dry_run,
            status="failed",
            counts={"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
            error=str(exc),
        )
        raise


def run_monthly_imports(
    config: PipelineConfig,
    dhis2: DHIS2Client,
    ocs: OCSClient,
    *,
    period: str,
    dry_run: bool,
    force: bool,
) -> list[dict[str, Any]]:
    start, end = month_bounds(period)
    return [
        import_job(
            config,
            dhis2,
            ocs,
            job_id=job.id,
            start=start,
            end=end,
            dry_run=dry_run,
            force=force,
            keep_payload=False,
            poll=True,
        )
        for job in config.imports
    ]


def review_import(
    config: PipelineConfig,
    *,
    job_id: str,
    start: str | None = None,
    end: str | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    years_back: int | None = None,
    as_of: str | None = None,
    org_unit_level: int | None = None,
    data_set: str | None = None,
    expected_org_units: int | None = None,
    expected_data_values: int | None = None,
) -> dict[str, Any]:
    context = resolve_import_context(
        config,
        job_id=job_id,
        start=start,
        end=end,
        start_period=start_period,
        end_period=end_period,
        years_back=years_back,
        as_of=as_of,
        org_unit_level=org_unit_level,
        data_set=data_set,
        expected_org_units=expected_org_units,
        expected_data_values=expected_data_values,
    )
    return {
        "job": context.job.id,
        "sourceData": context.job.label or context.job.dataset_id,
        "datasetId": context.job.dataset_id,
        "periodType": context.job.period_type,
        "periodStart": context.period_labels[0],
        "periodEnd": context.period_labels[-1],
        "dateStart": context.start,
        "dateEnd": context.end,
        "orgUnitLevel": context.org_unit_level,
        "orgUnitsWithGeometry": context.org_unit_count,
        "expectedOrgUnitsWithGeometry": context.expected_org_units_with_geometry,
        "dataSet": context.data_set,
        "dataElement": {
            "id": context.data_element.get("id"),
            "code": context.data_element.get("code"),
            "name": context.data_element.get("name") or context.data_element.get("displayName"),
        },
        "computedDataValues": _computed_data_values(context),
        "expectedDataValues": _expected_data_values(context),
        "message": _review_message(context),
    }


def _chunk_payload(payload: dict[str, Any], chunk_size: int) -> list[dict[str, Any]]:
    data_values = payload.get("dataValues")
    if not isinstance(data_values, list):
        raise PipelineError("Payload has no dataValues list.")
    if not data_values:
        return [payload]
    chunks: list[dict[str, Any]] = []
    for index in range(0, len(data_values), chunk_size):
        chunk = {**payload, "dataValues": data_values[index : index + chunk_size]}
        chunks.append(chunk)
    return chunks


def _validate_payload_data_value_count(
    context: ResolvedImport, payload: dict[str, Any]
) -> list[dict[str, Any]]:
    data_values = payload.get("dataValues")
    if not isinstance(data_values, list):
        raise PipelineError("OCS aggregation did not return a DHIS2 dataValueSet payload.")
    expected_data_values = _expected_data_values(context)
    if expected_data_values is not None and len(data_values) != expected_data_values:
        raise PipelineError(
            f"Import job '{context.job.id}' expected {expected_data_values} data values "
            f"({context.org_unit_count} org units x {len(context.period_labels)} periods), "
            f"but OCS returned {len(data_values)}."
        )
    return data_values


def _task_reference(response: Any) -> str | None:
    if not isinstance(response, dict):
        return None
    for key in ("id", "taskId", "uid", "relativeNotifierEndpoint"):
        value = response.get(key)
        if isinstance(value, str) and value:
            return value
    nested = response.get("response")
    if isinstance(nested, dict):
        return _task_reference(nested)
    return None


def _resolve_date_window(
    job: ImportJobConfig,
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
            return date_window_period_bounds(resolved_start, resolved_end, job.period_type)
        return resolved_start, resolved_end

    resolved_start_period = start_period or job.default_start_period
    resolved_end_period = end_period or job.default_end_period
    if resolved_start_period and resolved_end_period:
        return period_range_bounds(resolved_start_period, resolved_end_period, job.period_type)

    return job.default_start, job.default_end


def _computed_data_values(context: ResolvedImport) -> int:
    period_count = len(periods_between(context.start, context.end, context.job.period_type))
    return context.org_unit_count * period_count


def _expected_data_values(context: ResolvedImport) -> int | None:
    if context.expected_data_values is not None:
        return context.expected_data_values
    if context.job.expected_data_values is not None:
        return context.job.expected_data_values
    if context.job.expected_data_values_mode == "org_units_x_periods":
        return _computed_data_values(context)
    return None


def _validate_expected_data_values(context: ResolvedImport) -> None:
    expected = _expected_data_values(context)
    if expected is None:
        return
    computed = _computed_data_values(context)
    if expected != computed:
        raise PipelineError(
            f"Import job '{context.job.id}' expected {expected} data values, but level "
            f"{context.org_unit_level} and the period window compute {computed} "
            f"({context.org_unit_count} org units x {len(context.period_labels)} periods)."
        )


def _validate_data_set_cache_scope(
    payload: dict[str, Any],
    *,
    data_set: str | None,
    period_type: str,
) -> None:
    if not data_set:
        return
    scope = payload.get("scope")
    if scope in {None, "all", "legacy"}:
        return

    data_sets = _cache_data_sets(payload)
    if not data_sets:
        raise PipelineError(
            "Data element cache is scoped to data sets but contains no data set metadata. "
            f"Refresh it explicitly: cache-data-elements --data-set {data_set} --refresh"
        )

    matches = [item for item in data_sets if _data_set_matches(item, data_set)]
    if not matches:
        cached = ", ".join(
            str(item.get("code") or item.get("id") or item.get("name")) for item in data_sets
        )
        raise PipelineError(
            f"Data element cache was not built for DHIS2 data set '{data_set}'. "
            f"Cached data sets: {cached}. Run: cache-data-elements --data-set {data_set} --refresh"
        )
    if len(matches) > 1:
        raise PipelineError(
            f"Data set selector '{data_set}' matched multiple cached data sets. Use the UID."
        )
    _validate_data_set_period_type(matches[0], period_type)


def _validate_data_element_data_set_membership(
    data_element: dict[str, Any],
    *,
    payload: dict[str, Any],
    data_set: str | None,
) -> None:
    if not data_set:
        return
    scope = payload.get("scope")
    if scope in {None, "all", "legacy"}:
        return
    memberships = [
        item
        for item in data_element.get("dataSetMemberships", [])
        if isinstance(item, dict)
    ]
    if memberships:
        if any(_data_set_matches(item, data_set) for item in memberships):
            return
        element_name = data_element.get("code") or data_element.get("id") or data_element.get("name")
        raise PipelineError(
            f"Data element '{element_name}' is not linked to cached DHIS2 data set '{data_set}'. "
            "Refresh the data element cache for the selected data set or update the job mapping."
        )

    if len(_cache_data_sets(payload)) > 1:
        element_name = data_element.get("code") or data_element.get("id") or data_element.get("name")
        raise PipelineError(
            f"Data element '{element_name}' has no cached data set membership metadata. "
            f"Refresh explicitly: cache-data-elements --data-set {data_set} --refresh"
        )


def _cache_data_sets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data_sets = payload.get("dataSets")
    if isinstance(data_sets, list):
        return [item for item in data_sets if isinstance(item, dict)]
    data_set = payload.get("dataSet")
    if isinstance(data_set, dict):
        return [data_set]
    return []


def _data_set_matches(data_set_ref: dict[str, Any], selector: str) -> bool:
    return selector in {
        data_set_ref.get("id"),
        data_set_ref.get("code"),
        data_set_ref.get("name"),
        data_set_ref.get("displayName"),
    }


def _validate_data_set_period_type(data_set: dict[str, Any], period_type: str) -> None:
    expected_by_period = {
        "day": {"Daily"},
        "week": {"Weekly"},
        "month": {"Monthly"},
        "year": {"Yearly"},
    }
    expected = expected_by_period.get(period_type)
    actual = data_set.get("periodType") or data_set.get("period_type")
    if not expected or not actual:
        return
    if str(actual) not in expected:
        selector = data_set.get("code") or data_set.get("id") or data_set.get("name")
        expected_text = "/".join(sorted(expected))
        raise PipelineError(
            f"DHIS2 data set '{selector}' has periodType '{actual}', but import job "
            f"uses period_type '{period_type}' and expects DHIS2 periodType '{expected_text}'."
        )


def _review_message(context: ResolvedImport) -> str:
    computed = _computed_data_values(context)
    data_element_name = context.data_element.get("name") or context.data_element.get("displayName")
    lines = [
        f'"{context.job.label or context.job.dataset_id}" source data will be imported:',
        (
            f"- {context.job.period_type.title()} values from {context.period_labels[0]} "
            f"to {context.period_labels[-1]} ({context.start} to {context.end})"
        ),
        (
            f"- Selected org units: level {context.org_unit_level} "
            f"({context.org_unit_count} organisation units have geometry and will be imported)"
        ),
        f'- To data element "{data_element_name}"',
    ]
    if context.data_set:
        lines.append(f'- Using DHIS2 data set "{context.data_set}"')
    lines.append(f"- {computed} data values will be imported")
    return "\n".join(lines)
