from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from typing import Any, Callable

from .cache import (
    data_elements_cache_path,
    load_data_elements_cache,
    load_org_units_cache,
    org_unit_cache_paths,
    write_data_elements_cache,
    write_org_units_cache,
)
from .config import PipelineConfig, load_pipeline_config
from .diagnostics import diagnose_dhis2_access
from .dhis2 import DHIS2Client
from .env import load_env_file
from .errors import PipelineError
from .http import HttpRequestError
from .ocs import OCSClient
from .periods import date_window_period_bounds, period_range_bounds, relative_year_window
from .pipeline import aggregate_job, import_job, review_import, run_monthly_imports
from .reports import run_id, write_report
from .transfer import repair_target_metadata, review_transfer, run_transfer
from .transfer_config import load_instance_registry, load_transfer_scenario


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except PipelineError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    except HttpRequestError as exc:
        print(f"ERROR: {_format_http_error(exc)}", file=sys.stderr)
        raise SystemExit(2) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dhis2-climate-import",
        description="Reliable DHIS2 Climate App import orchestration.",
    )
    parser.add_argument("--config", default="config/import-pipeline.yaml")
    parser.add_argument("--env-file", default=".env")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check-dhis2", help="Check DHIS2 connectivity and version.")
    check.add_argument("--min-version", default="2.37")
    check.set_defaults(func=cmd_check_dhis2)

    diagnose = subparsers.add_parser(
        "diagnose-dhis2",
        help="Diagnose DHIS2 base URL, auth, and reverse-proxy API access.",
    )
    diagnose.add_argument("--try-context", action="append", default=[])
    diagnose.add_argument("--try-common-contexts", action="store_true")
    diagnose.add_argument("--timeout", type=float, default=15)
    diagnose.set_defaults(func=cmd_diagnose_dhis2)

    org_units = subparsers.add_parser("cache-org-units", help="Cache DHIS2 org units as GeoJSON.")
    org_units.add_argument("--level", type=int, default=None)
    org_units.add_argument("--refresh", action="store_true")
    org_units.set_defaults(func=cmd_cache_org_units)

    data_elements = subparsers.add_parser("cache-data-elements", help="Cache DHIS2 data elements.")
    data_elements.add_argument("--refresh", action="store_true")
    data_elements.add_argument(
        "--data-set",
        help=(
            "DHIS2 climate data set UID or code. Defaults to all data sets declared "
            "in config and import jobs."
        ),
    )
    data_elements.set_defaults(func=cmd_cache_data_elements)

    ingest = subparsers.add_parser("ingest-dataset", help="Start an OCS dataset ingestion.")
    ingest.add_argument("job_id")
    _add_window_args(ingest)
    ingest.add_argument("--overwrite", action="store_true")
    ingest.add_argument("--publish", action=argparse.BooleanOptionalAction, default=None)
    ingest.set_defaults(func=cmd_ingest_dataset)

    sync = subparsers.add_parser("sync-dataset", help="Plan or apply an OCS dataset sync.")
    sync.add_argument("job_id")
    sync.add_argument("--end")
    sync.add_argument("--publish", action=argparse.BooleanOptionalAction, default=None)
    sync_mode = sync.add_mutually_exclusive_group(required=True)
    sync_mode.add_argument("--plan", action="store_true")
    sync_mode.add_argument("--apply", action="store_true")
    sync.set_defaults(func=cmd_sync_dataset)

    aggregate = subparsers.add_parser("aggregate", help="Aggregate OCS climate data to DHIS2 JSON.")
    aggregate.add_argument("job_id")
    _add_window_args(aggregate)
    _add_import_context_args(aggregate)
    aggregate.add_argument("--output")
    aggregate.set_defaults(func=cmd_aggregate)

    review = subparsers.add_parser("review-import", help="Preview the import like Climate App review.")
    review.add_argument("job_id")
    _add_window_args(review)
    _add_import_context_args(review)
    review.set_defaults(func=cmd_review_import)

    import_parser = subparsers.add_parser("import-dhis2", help="Import a climate job to DHIS2.")
    import_parser.add_argument("job_id")
    _add_window_args(import_parser)
    _add_import_context_args(import_parser)
    import_mode = import_parser.add_mutually_exclusive_group(required=True)
    import_mode.add_argument("--dry-run", action="store_true")
    import_mode.add_argument("--apply", action="store_true")
    import_parser.add_argument("--force", action="store_true")
    import_parser.add_argument("--keep-payload", action="store_true")
    import_parser.add_argument("--poll", action=argparse.BooleanOptionalAction, default=True)
    import_parser.set_defaults(func=cmd_import_dhis2)

    verify = subparsers.add_parser("verify-analytics", help="Verify imported values in analytics.")
    verify.add_argument("job_id")
    verify.add_argument("--period", action="append", required=True)
    verify.add_argument("--sample", type=int, default=5)
    verify.add_argument("--org-unit-level", type=int)
    verify.set_defaults(func=cmd_verify_analytics)

    monthly = subparsers.add_parser("run-monthly", help="Run all configured imports for a month.")
    monthly.add_argument("--period", required=True, help="YYYY-MM")
    monthly_mode = monthly.add_mutually_exclusive_group(required=True)
    monthly_mode.add_argument("--dry-run", action="store_true")
    monthly_mode.add_argument("--apply", action="store_true")
    monthly.add_argument("--force", action="store_true")
    monthly.set_defaults(func=cmd_run_monthly)

    transfer = subparsers.add_parser(
        "transfer", help="Transfer aggregate Data Set values between two DHIS2 instances."
    )
    transfer.add_argument("--instances", default="config/instances.yaml")
    transfer.add_argument("--scenario", required=True)
    transfer_commands = transfer.add_subparsers(dest="transfer_command", required=True)

    transfer_review = transfer_commands.add_parser(
        "review", help="Review source/target metadata and the transfer window."
    )
    transfer_review.add_argument("job_id")
    _add_window_args(transfer_review)
    transfer_review.set_defaults(func=cmd_transfer_review)

    transfer_repair = transfer_commands.add_parser(
        "repair-metadata",
        help=(
            "Safely reconcile incomplete target CategoryCombos and Data Elements before "
            "creating a missing Data Set."
        ),
    )
    transfer_repair.add_argument("job_id")
    _add_window_args(transfer_repair)
    repair_mode = transfer_repair.add_mutually_exclusive_group(required=True)
    repair_mode.add_argument("--dry-run", action="store_true")
    repair_mode.add_argument("--apply", action="store_true")
    transfer_repair.set_defaults(func=cmd_transfer_repair_metadata)

    transfer_run = transfer_commands.add_parser(
        "run", help="Validate or apply a DHIS2-to-DHIS2 transfer."
    )
    transfer_run.add_argument("job_id")
    _add_window_args(transfer_run)
    transfer_mode = transfer_run.add_mutually_exclusive_group(required=True)
    transfer_mode.add_argument("--dry-run", action="store_true")
    transfer_mode.add_argument("--apply", action="store_true")
    transfer_run.add_argument("--force", action="store_true")
    transfer_run.add_argument("--keep-payload", action="store_true")
    transfer_run.set_defaults(func=cmd_transfer_run)

    return parser


def _add_window_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", help="End date in YYYY-MM-DD format.")
    parser.add_argument("--start-period", help="Start DHIS2 period, for example 2026-W09.")
    parser.add_argument("--end-period", help="End DHIS2 period, for example 2026-W31.")
    parser.add_argument(
        "--years-back",
        type=int,
        help="Build a dynamic complete-period window going this many years back.",
    )
    parser.add_argument(
        "--as-of",
        help="Anchor date for --years-back in YYYY-MM-DD format. Defaults to today.",
    )


def _add_import_context_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--org-unit-level", type=int, help="DHIS2 org unit level to import.")
    parser.add_argument("--data-set", help="DHIS2 data set UID or code to validate against.")
    parser.add_argument(
        "--expected-org-units",
        type=int,
        help="Fail unless the selected org unit cache has this number of features.",
    )
    parser.add_argument(
        "--expected-data-values",
        type=int,
        help="Fail unless org units x periods and OCS payload count match this value.",
    )


def cmd_check_dhis2(args: argparse.Namespace) -> None:
    env, _config = _runtime(args)
    dhis2 = DHIS2Client.from_env(env)
    min_version = _parse_min_version(args.min_version)
    result = dhis2.check(min_version=min_version)
    _print_json(result.__dict__)


def cmd_diagnose_dhis2(args: argparse.Namespace) -> None:
    env, _config = _runtime(args)
    result = diagnose_dhis2_access(
        env,
        contexts=args.try_context,
        include_common_contexts=args.try_common_contexts,
        timeout_seconds=args.timeout,
    )
    _print_json(result)


def cmd_cache_org_units(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    level = args.level or config.dhis2.org_unit_level
    geojson_path, manifest_path = org_unit_cache_paths(config.root_dir, level)
    if geojson_path.exists() and manifest_path.exists() and not args.refresh:
        load_org_units_cache(
            root_dir=config.root_dir,
            level=level,
            max_age_hours=config.dhis2.cache_max_age_hours,
        )
        _print_json(
            {
                "status": "exists",
                "level": level,
                "geojson": str(geojson_path),
                "manifest": str(manifest_path),
            }
        )
        return

    dhis2 = DHIS2Client.from_env(env)
    units = dhis2.organisation_units_level(level=level, page_size=config.dhis2.page_size)
    written_geojson, written_manifest = write_org_units_cache(
        root_dir=config.root_dir,
        level=level,
        source_base_url=dhis2.base_url,
        organisation_units=units,
    )
    _print_json(
        {
            "status": "written",
            "level": level,
            "count": len(units),
            "geojson": str(written_geojson),
            "manifest": str(written_manifest),
        }
    )


def cmd_cache_data_elements(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    path = data_elements_cache_path(config.root_dir)
    if path.exists() and not args.refresh:
        elements = load_data_elements_cache(config.root_dir)
        _print_json({"status": "exists", "count": len(elements), "path": str(path)})
        return

    dhis2 = DHIS2Client.from_env(env)
    data_set_selectors = _data_set_selectors(args.data_set, config)
    data_set_payloads: list[dict[str, Any]] = []
    if data_set_selectors:
        elements = []
        for selector in data_set_selectors:
            data_set = dhis2.resolve_data_set(selector)
            data_set_payload = _data_set_payload(data_set)
            data_set_payloads.append(data_set_payload)
            elements.extend(
                _with_data_set_membership(element, data_set_payload)
                for element in dhis2.data_elements_for_data_set(
                    data_set_id=data_set.id,
                    page_size=config.dhis2.page_size,
                )
            )
        elements = _unique_elements(elements)
    else:
        elements = dhis2.data_elements(page_size=config.dhis2.page_size)
    written_path = write_data_elements_cache(
        root_dir=config.root_dir,
        source_base_url=dhis2.base_url,
        data_elements=elements,
        data_sets=data_set_payloads,
    )
    _print_json(
        {
            "status": "written",
            "scope": "dataSet" if data_set_payloads else "all",
            "dataSets": data_set_payloads,
            "count": len(elements),
            "path": str(written_path),
        }
    )


def cmd_ingest_dataset(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    job = config.get_job(args.job_id)
    start, end = _job_date_window(job, args)
    ocs = OCSClient.from_env(env)
    response = ocs.ingest_dataset(
        job.dataset_id,
        start=start,
        end=end,
        overwrite=args.overwrite,
        publish=config.open_climate_service.publish if args.publish is None else args.publish,
    )
    _print_json(response)


def cmd_sync_dataset(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    job = config.get_job(args.job_id)
    ocs = OCSClient.from_env(env)
    if args.plan:
        response = ocs.sync_plan(job.dataset_id, end=args.end or job.default_end)
    else:
        response = ocs.sync_apply(
            job.dataset_id,
            end=args.end or job.default_end,
            publish=config.open_climate_service.publish if args.publish is None else args.publish,
        )
    _print_json(response)


def cmd_aggregate(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    ocs = OCSClient.from_env(env)
    payload = aggregate_job(
        config,
        ocs,
        job_id=args.job_id,
        start=args.start,
        end=args.end,
        start_period=args.start_period,
        end_period=args.end_period,
        years_back=args.years_back,
        as_of=args.as_of,
        org_unit_level=args.org_unit_level,
        data_set=args.data_set,
        expected_org_units=args.expected_org_units,
        expected_data_values=args.expected_data_values,
    )
    output = Path(args.output) if args.output else None
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _print_json({"status": "written", "path": str(output)})
    else:
        path = write_report(config.root_dir, f"{run_id(args.job_id)}-aggregate", payload)
        _print_json({"status": "written", "path": str(path)})


def cmd_import_dhis2(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    dhis2 = DHIS2Client.from_env(env)
    ocs = OCSClient.from_env(env)
    report = import_job(
        config,
        dhis2,
        ocs,
        job_id=args.job_id,
        start=args.start,
        end=args.end,
        dry_run=args.dry_run,
        force=args.force,
        keep_payload=args.keep_payload,
        poll=args.poll,
        start_period=args.start_period,
        end_period=args.end_period,
        years_back=args.years_back,
        as_of=args.as_of,
        org_unit_level=args.org_unit_level,
        data_set=args.data_set,
        expected_org_units=args.expected_org_units,
        expected_data_values=args.expected_data_values,
    )
    _print_json(
        {
            "runId": report["runId"],
            "dryRun": report["dryRun"],
            "dataSet": report["dataSet"],
            "orgUnitLevel": report["orgUnitLevel"],
            "dataValueCount": report["dataValueCount"],
            "computedDataValueCount": report["computedDataValueCount"],
            "expectedDataValueCount": report["expectedDataValueCount"],
            "chunkCount": report["chunkCount"],
            "counts": report["counts"],
        }
    )


def cmd_review_import(args: argparse.Namespace) -> None:
    _env, config = _runtime(args)
    review = review_import(
        config,
        job_id=args.job_id,
        start=args.start,
        end=args.end,
        start_period=args.start_period,
        end_period=args.end_period,
        years_back=args.years_back,
        as_of=args.as_of,
        org_unit_level=args.org_unit_level,
        data_set=args.data_set,
        expected_org_units=args.expected_org_units,
        expected_data_values=args.expected_data_values,
    )
    _print_json(review)


def cmd_verify_analytics(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    context = config.get_job(args.job_id)
    data_element = _resolve_job_data_element(config, context.data_element)
    org_unit_level = args.org_unit_level or context.org_unit_level or config.dhis2.org_unit_level
    geojson, _manifest = load_org_units_cache(
        root_dir=config.root_dir,
        level=org_unit_level,
        max_age_hours=config.dhis2.cache_max_age_hours,
    )
    org_units = [feature["id"] for feature in geojson["features"][: args.sample]]
    dhis2 = DHIS2Client.from_env(env)
    result = dhis2.analytics(
        dimensions=[
            f"dx:{data_element['id']}",
            f"pe:{';'.join(args.period)}",
            f"ou:{';'.join(org_units)}",
        ]
    )
    _print_json(
        {
            "job": args.job_id,
            "dataElement": data_element,
            "periods": args.period,
            "orgUnitsSampled": org_units,
            "analyticsRows": len(result.get("rows", [])),
            "analytics": result,
        }
    )


def cmd_run_monthly(args: argparse.Namespace) -> None:
    env, config = _runtime(args)
    dhis2 = DHIS2Client.from_env(env)
    ocs = OCSClient.from_env(env)
    reports = run_monthly_imports(
        config,
        dhis2,
        ocs,
        period=args.period,
        dry_run=args.dry_run,
        force=args.force,
    )
    _print_json(
        {
            "period": args.period,
            "dryRun": args.dry_run,
            "runs": [
                {
                    "runId": report["runId"],
                    "job": report["job"],
                    "dataValueCount": report["dataValueCount"],
                    "counts": report["counts"],
                }
                for report in reports
            ],
        }
    )


def cmd_transfer_review(args: argparse.Namespace) -> None:
    _env, scenario, source, target = _transfer_runtime(args)
    result = review_transfer(
        scenario,
        source,
        target,
        job_id=args.job_id,
        start=args.start,
        end=args.end,
        start_period=args.start_period,
        end_period=args.end_period,
        years_back=args.years_back,
        as_of=args.as_of,
    )
    _print_json(result)


def cmd_transfer_run(args: argparse.Namespace) -> None:
    _transfer_progress("instances.check.start", {})
    _env, scenario, source, target = _transfer_runtime(
        args, progress=_transfer_progress
    )
    _transfer_progress(
        "instances.check.done",
        {"source": source.base_url, "target": target.base_url},
    )
    result = run_transfer(
        scenario,
        source,
        target,
        job_id=args.job_id,
        apply=args.apply,
        force=args.force,
        keep_payload=args.keep_payload,
        start=args.start,
        end=args.end,
        start_period=args.start_period,
        end_period=args.end_period,
        years_back=args.years_back,
        as_of=args.as_of,
        progress=_transfer_progress,
    )
    _print_json(
        {
            "runId": result["runId"],
            "scenario": result["scenario"],
            "job": result["job"],
            "dryRun": result["dryRun"],
            "sourceDataValueCount": result["sourceDataValueCount"],
            "metadata": {
                "required": result["metadata"]["required"],
                "validated": result["metadata"]["validated"],
                "committed": result["metadata"]["committed"],
            },
            "dataValidationPendingMetadataCommit": result[
                "dataValidationPendingMetadataCommit"
            ],
            "counts": result["counts"],
            "reportPath": result["reportPath"],
        }
    )


def cmd_transfer_repair_metadata(args: argparse.Namespace) -> None:
    _env, scenario, source, target = _transfer_runtime(args)
    result = repair_target_metadata(
        scenario,
        source,
        target,
        job_id=args.job_id,
        apply=args.apply,
        start=args.start,
        end=args.end,
        start_period=args.start_period,
        end_period=args.end_period,
        years_back=args.years_back,
        as_of=args.as_of,
    )
    if result["status"] == "not_required":
        _print_json(result)
        return
    _print_json(
        {
            "runId": result["runId"],
            "scenario": result["scenario"],
            "job": result["job"],
            "dryRun": result["dryRun"],
            "status": result["status"],
            "repair": result["repair"],
            "structure": {
                "validated": result["structure"]["validated"],
                "committed": result["structure"]["committed"],
            },
            "assignments": {
                "validationPendingStructureCommit": result["assignments"][
                    "validationPendingStructureCommit"
                ],
                "validated": result["assignments"]["validated"],
                "committed": result["assignments"]["committed"],
            },
            "nextStep": result["nextStep"],
            "reportPath": result["reportPath"],
        }
    )


def _runtime(args: argparse.Namespace) -> tuple[dict[str, str], PipelineConfig]:
    file_env = load_env_file(args.env_file)
    env = {**file_env, **os.environ}
    config = load_pipeline_config(args.config, env=env)
    return env, config


def _transfer_runtime(
    args: argparse.Namespace,
    *,
    progress: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[
    dict[str, str], Any, DHIS2Client, DHIS2Client
]:
    file_env = load_env_file(args.env_file)
    env = {**file_env, **os.environ}
    registry = load_instance_registry(args.instances)
    scenario = load_transfer_scenario(args.scenario)
    source_profile = registry.get(scenario.source_instance)
    target_profile = registry.get(scenario.target_instance)
    source = DHIS2Client.from_env(source_profile.client_env(env))
    target = DHIS2Client.from_env(target_profile.client_env(env))
    if source.base_url == target.base_url:
        raise PipelineError(
            "Source and target instance profiles resolve to the same DHIS2 base URL."
        )
    source.check(min_version=(2, 37))
    if progress is not None:
        progress("instances.source.done", {"source": source.base_url})
    target.check(min_version=(2, 37))
    if progress is not None:
        progress("instances.target.done", {"target": target.base_url})
    return env, scenario, source, target


def _data_set_selectors(cli_selector: str | None, config: PipelineConfig) -> list[str]:
    selectors: list[str] = []
    if cli_selector:
        selectors.append(cli_selector)
    else:
        if config.dhis2.climate_data_set:
            selectors.append(config.dhis2.climate_data_set)
        selectors.extend(config.dhis2.climate_data_sets)
        selectors.extend(job.data_set for job in config.imports if job.data_set)
    return _unique_strings(selectors)


def _unique_strings(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _unique_elements(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    by_id: dict[str, dict[str, Any]] = {}
    result: list[dict[str, Any]] = []
    for element in elements:
        uid = element.get("id")
        if not isinstance(uid, str):
            continue
        if uid in by_id:
            _merge_data_set_memberships(by_id[uid], element)
            continue
        seen.add(uid)
        by_id[uid] = element
        result.append(element)
    return result


def _with_data_set_membership(element: dict[str, Any], data_set: dict[str, Any]) -> dict[str, Any]:
    result = {**element}
    memberships = [
        item
        for item in result.get("dataSetMemberships", [])
        if isinstance(item, dict)
    ]
    if not any(_data_set_ref_matches(item, data_set) for item in memberships):
        memberships.append(data_set)
    result["dataSetMemberships"] = memberships
    return result


def _merge_data_set_memberships(target: dict[str, Any], source: dict[str, Any]) -> None:
    memberships = [
        item
        for item in target.get("dataSetMemberships", [])
        if isinstance(item, dict)
    ]
    for item in source.get("dataSetMemberships", []):
        if isinstance(item, dict) and not any(_data_set_ref_matches(item, ref) for ref in memberships):
            memberships.append(item)
    if memberships:
        target["dataSetMemberships"] = memberships


def _data_set_ref_matches(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return bool(
        {left.get("id"), left.get("code"), left.get("name")}
        & {right.get("id"), right.get("code"), right.get("name")}
    )


def _data_set_payload(data_set: Any) -> dict[str, Any]:
    return {
        "id": data_set.id,
        "code": data_set.code,
        "name": data_set.name,
        "periodType": data_set.period_type,
    }


def _job_date_window(job: Any, args: argparse.Namespace) -> tuple[str | None, str | None]:
    if args.years_back is not None:
        if args.start or args.start_period:
            raise PipelineError("--years-back cannot be combined with --start or --start-period.")
        return relative_year_window(
            args.years_back,
            job.period_type,
            end=args.end,
            end_period=args.end_period,
            as_of=args.as_of,
        )
    if args.as_of:
        raise PipelineError("--as-of can only be used with --years-back.")
    if args.start or args.end:
        start = args.start or job.default_start
        end = args.end or job.default_end
        if start and end:
            return date_window_period_bounds(start, end, job.period_type)
        return start, end
    start_period = args.start_period or job.default_start_period
    end_period = args.end_period or job.default_end_period
    if start_period and end_period:
        return period_range_bounds(start_period, end_period, job.period_type)
    return job.default_start, job.default_end


def _resolve_job_data_element(config: PipelineConfig, selector: str) -> dict[str, Any]:
    from .cache import resolve_data_element

    return resolve_data_element(load_data_elements_cache(config.root_dir), selector)


def _parse_min_version(value: str) -> tuple[int, int]:
    parts = value.split(".", 1)
    if len(parts) != 2:
        raise PipelineError("--min-version must use major.minor format, for example 2.37.")
    return int(parts[0]), int(parts[1])


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _transfer_progress(event: str, details: dict[str, Any]) -> None:
    labels = {
        "instances.check.start": "Vérification des connexions X et Y",
        "instances.source.done": "Connexion source X validée",
        "instances.target.done": "Connexion cible Y validée",
        "instances.check.done": "Connexions X et Y validées",
        "transfer.start": "Démarrage du transfert",
        "context.resolve.start": "Lecture et comparaison des métadonnées",
        "context.resolve.done": "Métadonnées compatibles",
        "cache.metadata.written": "Cache local actualisé",
        "metadata.source_export.start": "Export des métadonnées source",
        "metadata.source_export.done": "Métadonnées source exportées",
        "metadata.validation.start": "Validation des métadonnées cible",
        "metadata.validation.done": "Métadonnées cible validées",
        "metadata.commit.start": "Création des métadonnées cible",
        "metadata.commit.done": "Métadonnées cible créées",
        "source.export.start": "Début de l’export des valeurs source",
        "source.export.request.start": "Requête source en cours",
        "source.export.request.done": "Requête source terminée",
        "source.export.request.split": "Timeout détecté, subdivision automatique",
        "source.export.done": "Export des valeurs source terminé",
        "target.dry_run.start": "Début de la validation des valeurs cible",
        "target.dry_run.chunk.start": "Validation d’un lot cible",
        "target.dry_run.chunk.done": "Lot cible validé",
        "target.dry_run.done": "Validation des valeurs cible terminée",
        "target.import.start": "Début de l’import réel des valeurs",
        "target.import.chunk.start": "Import d’un lot cible",
        "target.import.chunk.done": "Lot cible importé",
        "target.import.done": "Import réel terminé",
        "transfer.done": "Transfert terminé",
        "transfer.failed": "Transfert interrompu",
    }
    rendered_details = " | ".join(
        f"{key}={value}" for key, value in details.items()
    )
    suffix = f" | {rendered_details}" if rendered_details else ""
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(
        f"[{timestamp}] {labels.get(event, event)}{suffix}",
        file=sys.stderr,
        flush=True,
    )


def _format_http_error(exc: HttpRequestError) -> str:
    message = str(exc)
    if exc.status_code == 403 and "nginx" in exc.body.lower():
        return (
            f"{message}\n"
            "The 403 response came from nginx before a normal DHIS2 JSON error. "
            "Check that DHIS2_BASE_URL includes the correct DHIS2 context path, "
            "that the reverse proxy allows API requests from scripts, and that the "
            "technical account has metadata read access. If the instance rejects PAT "
            "authentication, set DHIS2_USERNAME, DHIS2_PASSWORD, and "
            "DHIS2_AUTH_METHOD=basic in .env, or enable PAT authentication on the DHIS2 "
            "server. Run diagnose-dhis2 --try-common-contexts to identify whether the "
            "base URL or proxy route is the real blocker."
        )
    if exc.status_code in {401, 403}:
        return (
            f"{message}\n"
            "Check DHIS2_PAT or DHIS2_USERNAME/DHIS2_PASSWORD and the user's metadata/data "
            "authorities."
        )
    if exc.status_code == 409:
        conflicts = _dhis2_error_details(exc.body)
        if conflicts:
            return f"{message}\nDHIS2 conflict details:\n" + "\n".join(
                f"- {item}" for item in conflicts[:20]
            )
    return message


def _dhis2_error_details(body: str) -> list[str]:
    try:
        payload = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        return []

    details: list[str] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            error_code = value.get("errorCode")
            error_message = value.get("message")
            if isinstance(error_message, str) and error_code:
                detail = f"{error_code}: {error_message}"
                if detail not in details:
                    details.append(detail)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return details
