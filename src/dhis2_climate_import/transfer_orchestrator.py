from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import time
from typing import Any

from .dhis2 import DHIS2Client
from .errors import PipelineError, TransientPipelineError
from .http import HttpRequestError
from .journal import TransferFingerprint, TransferRunJournal
from .locks import concurrency_slot, exclusive_lock
from .periods import period_date_windows
from .reports import run_id
from .transfer import (
    ProgressCallback,
    is_transient_transfer_http_error,
    reconcile_target_metadata,
    reconcile_target_organisation_units,
    resolve_transfer_context,
    run_transfer,
)
from .transfer_config import TransferScenario


WindowRunner = Callable[[str, str, int, int, str, str], dict[str, Any]]


def orchestrate_transfer(
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
    window_runner: WindowRunner | None = None,
) -> dict[str, Any]:
    """Run a transfer as restartable, bounded-memory execution windows."""
    job = scenario.get_job(job_id)
    state_dir = scenario.job_state_dir(job_id)
    state_dir.mkdir(parents=True, exist_ok=True)
    _emit(
        progress,
        "orchestrator.slot.wait",
        maximum=scenario.max_parallel_jobs,
    )
    with concurrency_slot(
        scenario.state_dir,
        maximum=scenario.max_parallel_jobs,
        wait_when_busy=True,
    ) as slot:
        _emit(progress, "orchestrator.slot.acquired", slot=slot)
        with exclusive_lock(
            scenario.state_dir / "locks" / f"job-{job_id}.lock",
            purpose=f"transfer job {scenario.id}/{job_id}",
            transient_when_busy=True,
        ):
            return _orchestrate_locked(
                scenario,
                source,
                target,
                job_id=job_id,
                apply=apply,
                force=force,
                keep_payload=keep_payload,
                start=start,
                end=end,
                start_period=start_period,
                end_period=end_period,
                years_back=years_back,
                as_of=as_of,
                progress=progress,
                window_runner=window_runner,
            )


def _orchestrate_locked(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    apply: bool,
    force: bool,
    keep_payload: bool,
    start: str | None,
    end: str | None,
    start_period: str | None,
    end_period: str | None,
    years_back: int | None,
    as_of: str | None,
    progress: ProgressCallback | None,
    window_runner: WindowRunner | None,
) -> dict[str, Any]:
    job = scenario.get_job(job_id)
    organisation_unit_policy = job.organisation_units.synchronization
    reconcile_organisation_units = (
        organisation_unit_policy.enabled
        and organisation_unit_policy.run_before_transfer
    )
    metadata_enabled = scenario.metadata.synchronization.enabled
    if metadata_enabled or reconcile_organisation_units:
        if metadata_enabled:
            _emit(progress, "metadata.reconcile.lock.wait", job=job_id)
        else:
            _emit(progress, "organisation_units.reconcile.lock.wait", job=job_id)
        with exclusive_lock(
            scenario.state_dir / "locks" / "metadata.lock",
            purpose=f"target metadata for {scenario.id}/{job_id}",
            wait_when_busy=True,
        ):
            if (
                reconcile_organisation_units
                and organisation_unit_policy.conflict_policy == "exclude"
            ):
                # Refresh the dynamic quarantine before metadata resolution or any
                # isolated value window can consume it.
                organisation_units = reconcile_target_organisation_units(
                    scenario,
                    source,
                    target,
                    job_id=job_id,
                    apply=apply,
                    progress=progress,
                )
            organisation_units_pending = bool(
                not apply
                and reconcile_organisation_units
                and organisation_unit_policy.conflict_policy == "exclude"
                and organisation_units.get("requiresChanges")
            )
            if metadata_enabled and not organisation_units_pending:
                metadata = reconcile_target_metadata(
                    scenario,
                    source,
                    target,
                    job_id=job_id,
                    apply=apply,
                    start=start,
                    end=end,
                    start_period=start_period,
                    end_period=end_period,
                    years_back=years_back,
                    as_of=as_of,
                    progress=progress,
                )
            elif metadata_enabled:
                metadata = {
                    "enabled": True,
                    "requiresChanges": False,
                    "committed": False,
                    "status": "pending_organisation_units",
                    "nextStep": (
                        "Apply the validated organisation-unit plan before metadata "
                        "validation."
                    ),
                }
            else:
                metadata = {
                    "enabled": False,
                    "requiresChanges": False,
                    "committed": False,
                    "status": "disabled",
                }
            if (
                reconcile_organisation_units
                and organisation_unit_policy.conflict_policy != "exclude"
            ):
                organisation_units = reconcile_target_organisation_units(
                    scenario,
                    source,
                    target,
                    job_id=job_id,
                    apply=apply,
                    progress=progress,
                )
            elif not reconcile_organisation_units:
                organisation_units = {
                    "enabled": organisation_unit_policy.enabled,
                    "automatic": False,
                    "requiresChanges": False,
                    "committed": False,
                    "status": "manual" if organisation_unit_policy.enabled else "disabled",
                }
    else:
        metadata = {
            "enabled": False,
            "requiresChanges": False,
            "committed": False,
            "status": "disabled",
        }
        organisation_units = {
            "enabled": False,
            "automatic": False,
            "requiresChanges": False,
            "committed": False,
            "status": "disabled",
        }

    if not apply and (
        metadata.get("requiresChanges")
        or organisation_units.get("requiresChanges")
    ):
        return {
            "runId": None,
            "scenario": scenario.id,
            "job": job_id,
            "dryRun": True,
            "status": "metadata_changes_required",
            "metadata": metadata,
            "organisationUnits": organisation_units,
            "organisationUnitExclusions": [
                asdict(item) for item in job.organisation_units.exclusions
            ],
            "organisationUnitConflictPolicy": (
                job.organisation_units.synchronization.conflict_policy
            ),
            "detectedOrganisationUnitConflicts": organisation_units.get(
                "detectedOrganisationUnitConflicts", []
            ),
            "windowCount": 0,
            "completedWindows": 0,
            "skippedWindows": 0,
            "failedWindows": 0,
            "nextStep": (
                "Review the metadata and organisation-unit reports, then run the "
                "same transfer with --apply. The guarded reconciliations run before "
                "any value export."
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
    excluded_source_organisation_units = tuple(
        getattr(context, "excluded_source_organisation_units", ())
    )
    job = context.job
    windows = period_date_windows(
        context.start,
        context.end,
        job.period_type,
        job.transfer.execution.period_batch_size,
    )
    config_hash = _sha256_file(scenario.path)
    quarantine_hash: str | None = None
    quarantine_artifact = organisation_units.get(
        "organisationUnitQuarantineArtifact"
    )
    if quarantine_artifact:
        quarantine_path = Path(str(quarantine_artifact))
        if quarantine_path.exists():
            quarantine_hash = _sha256_file(quarantine_path)
    execution_hash = (
        hashlib.sha256(
            f"{config_hash}:{quarantine_hash}".encode("utf-8")
        ).hexdigest()
        if quarantine_hash
        else config_hash
    )
    parent_fingerprint = _fingerprint(
        scenario,
        source,
        target,
        context.source_data_set.id,
        job.target_data_set,
        context.start,
        context.end,
        execution_hash,
        job_id,
    )
    parent_run_id = run_id(f"transfer-parent-{scenario.id}-{job_id}")
    run_dir = scenario.job_state_dir(job_id) / "runs" / parent_run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = run_dir / "configuration.json"
    _write_json(
        snapshot_path,
        {
            "scenarioPath": str(scenario.path),
            "scenarioHash": config_hash,
            "organisationUnitQuarantineHash": quarantine_hash,
            "scenario": asdict(scenario),
            "resolvedWindow": {"start": context.start, "end": context.end},
            "apply": apply,
        },
    )
    journal = TransferRunJournal(scenario.job_state_dir(job_id) / "journal.sqlite3")
    interrupted = journal.mark_running_windows_interrupted(job_id=job_id)
    if interrupted:
        _emit(progress, "orchestrator.interrupted.recovered", windows=interrupted)
    journal.start_parent_run(
        run_id=parent_run_id,
        fingerprint=parent_fingerprint,
        dry_run=not apply,
        window_count=len(windows),
        configuration_snapshot=str(snapshot_path),
    )
    journal.record_event(
        run_id=parent_run_id,
        event="parent.started",
        details={"windows": len(windows), "apply": apply},
    )
    _emit(
        progress,
        "orchestrator.start",
        runId=parent_run_id,
        windows=len(windows),
        start=context.start,
        end=context.end,
        periodBatchSize=job.transfer.execution.period_batch_size,
    )

    runner = window_runner or _inline_runner(
        scenario,
        source,
        target,
        job_id=job_id,
        apply=apply,
        force=force,
        keep_payload=keep_payload,
        progress=progress,
    )
    completed = 0
    skipped = 0
    failures: list[dict[str, Any]] = []
    metadata_pending_windows: list[dict[str, Any]] = []
    window_results: list[dict[str, Any]] = []
    source_total = 0
    invalid_value_quarantine_total = 0
    invalid_value_quarantine_windows: list[dict[str, Any]] = []
    timeliness_quarantine_total = 0
    timeliness_quarantine_windows: list[dict[str, Any]] = []
    aggregate_counts = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
    parent_finished = False

    try:
        for index, (window_start, window_end) in enumerate(windows, start=1):
            window_artifact_dir = (
                run_dir / "windows" / f"{window_start}__{window_end}"
            )
            fingerprint = _fingerprint(
                scenario,
                source,
                target,
                context.source_data_set.id,
                job.target_data_set,
                window_start,
                window_end,
                execution_hash,
                job_id,
            )
            if (
                job.transfer.execution.resume
                and not force
                and journal.has_successful_window(
                    fingerprint=fingerprint, dry_run=not apply
                )
            ):
                skipped += 1
                item = {
                    "index": index,
                    "start": window_start,
                    "end": window_end,
                    "status": "skipped_successful_checkpoint",
                }
                window_results.append(item)
                _write_json(window_artifact_dir / "skipped.json", item)
                journal.record_event(
                    run_id=parent_run_id,
                    event="window.skipped",
                    details=item,
                )
                _emit(
                    progress,
                    "orchestrator.window.skipped",
                    window=f"{index}/{len(windows)}",
                    start=window_start,
                    end=window_end,
                )
                continue

            last_error: Exception | None = None
            for attempt in range(1, job.transfer.retry.attempts + 1):
                attempt_started = time.monotonic()
                window_id = f"{parent_run_id}-w{index:04d}-a{attempt:02d}"
                journal.start_window(
                    window_id=window_id,
                    run_id=parent_run_id,
                    fingerprint=fingerprint,
                    dry_run=not apply,
                    window_index=index,
                    window_count=len(windows),
                    attempt=attempt,
                )
                _emit(
                    progress,
                    "orchestrator.window.start",
                    window=f"{index}/{len(windows)}",
                    attempt=f"{attempt}/{job.transfer.retry.attempts}",
                    start=window_start,
                    end=window_end,
                )
                journal.update_window_phase(
                    window_id=window_id,
                    phase="exporting",
                )
                try:
                    result = runner(
                        window_start,
                        window_end,
                        index,
                        len(windows),
                        window_id,
                        parent_run_id,
                    )
                    counts = result.get("counts") or {}
                    source_count = int(result.get("sourceDataValueCount") or 0)
                    timeliness_quarantine = result.get("timelinessQuarantine") or {}
                    quarantined_value_count = int(
                        timeliness_quarantine.get("quarantinedDataValueCount") or 0
                    )
                    invalid_value_quarantine = (
                        result.get("invalidDataValueQuarantine") or {}
                    )
                    invalid_value_count = int(
                        invalid_value_quarantine.get("quarantinedDataValueCount") or 0
                    )
                    metadata_pending = bool(
                        result.get("dataValidationPendingMetadataCommit")
                        or result.get(
                            "dataValidationPendingHistoricalMetadataCommit"
                        )
                    )
                    if metadata_pending:
                        if apply:
                            raise PipelineError(
                                "An apply window still has pending metadata after its "
                                "metadata commit. Data import was blocked."
                            )
                        pending_item = {
                            "index": index,
                            "start": window_start,
                            "end": window_end,
                            "status": "metadata_pending",
                            "attempt": attempt,
                            "sourceDataValueCount": source_count,
                            "historicalDataElements": result.get(
                                "historicalDataElements", {}
                            ),
                            "reportPath": result.get("reportPath"),
                            "durationSeconds": round(
                                time.monotonic() - attempt_started, 2
                            ),
                        }
                        metadata_pending_windows.append(pending_item)
                        window_results.append(pending_item)
                        source_total += source_count
                        journal.finish_window(
                            window_id=window_id,
                            status="metadata_pending",
                            source_count=source_count,
                            counts=counts,
                            report_path=result.get("reportPath"),
                            duration_seconds=pending_item["durationSeconds"],
                        )
                        _write_json(
                            window_artifact_dir / "metadata-pending.json",
                            pending_item,
                        )
                        _emit(
                            progress,
                            "orchestrator.window.metadata_pending",
                            window=f"{index}/{len(windows)}",
                            start=window_start,
                            end=window_end,
                        )
                        last_error = None
                        break
                    journal.finish_window(
                        window_id=window_id,
                        status="success",
                        source_count=source_count,
                        counts=counts,
                        report_path=result.get("reportPath"),
                        duration_seconds=round(
                            time.monotonic() - attempt_started, 2
                        ),
                    )
                    completed += 1
                    source_total += source_count
                    if invalid_value_count:
                        invalid_value_quarantine_total += invalid_value_count
                        invalid_value_quarantine_windows.append(
                            {
                                "index": index,
                                "start": window_start,
                                "end": window_end,
                                **invalid_value_quarantine,
                            }
                        )
                    if quarantined_value_count:
                        timeliness_quarantine_total += quarantined_value_count
                        timeliness_quarantine_windows.append(
                            {
                                "index": index,
                                "start": window_start,
                                "end": window_end,
                                **timeliness_quarantine,
                            }
                        )
                    for key in aggregate_counts:
                        aggregate_counts[key] += int(counts.get(key, 0))
                    item = {
                        "index": index,
                        "start": window_start,
                        "end": window_end,
                        "status": (
                            "success_with_quarantine"
                            if quarantined_value_count or invalid_value_count
                            else "success"
                        ),
                        "attempt": attempt,
                        "sourceDataValueCount": source_count,
                        "counts": counts,
                        "reportPath": result.get("reportPath"),
                        "invalidDataValueQuarantine": invalid_value_quarantine,
                        "timelinessQuarantine": timeliness_quarantine,
                        "adaptiveChunking": result.get("adaptiveChunking", {}),
                        "durationSeconds": round(
                            time.monotonic() - attempt_started, 2
                        ),
                    }
                    window_results.append(item)
                    _write_json(window_artifact_dir / "success.json", item)
                    _emit(
                        progress,
                        "orchestrator.window.done",
                        window=f"{index}/{len(windows)}",
                        dataValues=source_count,
                        quarantinedDataValues=(
                            quarantined_value_count + invalid_value_count
                        ),
                    )
                    last_error = None
                    break
                except Exception as exc:  # classified immediately below
                    last_error = exc
                    transient = _is_transient(exc)
                    journal.finish_window(
                        window_id=window_id,
                        status="failed",
                        error_type="transient" if transient else "permanent",
                        http_status=(
                            exc.status_code
                            if isinstance(exc, HttpRequestError)
                            else None
                        ),
                        error=str(exc),
                        duration_seconds=round(
                            time.monotonic() - attempt_started, 2
                        ),
                    )
                    _write_json(
                        window_artifact_dir / f"failure-attempt-{attempt}.json",
                        {
                            "index": index,
                            "attempt": attempt,
                            "start": window_start,
                            "end": window_end,
                            "transient": transient,
                            "error": str(exc),
                        },
                    )
                    if not transient or attempt >= job.transfer.retry.attempts:
                        break
                    delay = min(
                        job.transfer.retry.initial_delay_seconds * (2 ** (attempt - 1)),
                        job.transfer.retry.max_delay_seconds,
                        60.0,
                    )
                    _emit(
                        progress,
                        "orchestrator.window.retry",
                        window=f"{index}/{len(windows)}",
                        nextAttempt=attempt + 1,
                        delaySeconds=delay,
                        error=str(exc)[:300],
                    )
                    journal.record_event(
                        run_id=parent_run_id,
                        window_id=window_id,
                        event="window.retry",
                        level="warning",
                        details={"delaySeconds": delay, "error": str(exc)},
                    )
                    time.sleep(delay)

            if last_error is not None:
                failure = {
                    "index": index,
                    "start": window_start,
                    "end": window_end,
                    "transient": _is_transient(last_error),
                    "error": str(last_error),
                }
                failures.append(failure)
                window_results.append({**failure, "status": "failed"})
                if job.transfer.execution.stop_on_error:
                    break

        status = (
            "failed"
            if failures
            else (
                "metadata_changes_required"
                if metadata_pending_windows
                else (
                    "success_with_quarantine"
                    if getattr(context, "detected_organisation_unit_conflicts", ())
                    or timeliness_quarantine_total
                    or invalid_value_quarantine_total
                    else "success"
                )
            )
        )
        summary = {
            "runId": parent_run_id,
            "scenario": scenario.id,
            "job": job_id,
            "dryRun": not apply,
            "status": status,
            "start": context.start,
            "end": context.end,
            "windowCount": len(windows),
            "completedWindows": completed,
            "skippedWindows": skipped,
            "failedWindows": len(failures),
            "metadataPendingWindows": len(metadata_pending_windows),
            "remainingWindows": max(
                0,
                len(windows)
                - completed
                - skipped
                - len(failures)
                - len(metadata_pending_windows),
            ),
            "sourceDataValueCount": source_total,
            "targetDataValueCount": (
                source_total
                - invalid_value_quarantine_total
                - timeliness_quarantine_total
            ),
            "counts": aggregate_counts,
            "invalidDataValueQuarantine": {
                "errorCode": "E8120",
                "quarantinedDataValueCount": invalid_value_quarantine_total,
                "windowCount": len(invalid_value_quarantine_windows),
                "windows": invalid_value_quarantine_windows,
            },
            "timelinessQuarantine": {
                "errorCode": "E8031",
                "quarantinedDataValueCount": timeliness_quarantine_total,
                "windowCount": len(timeliness_quarantine_windows),
                "windows": timeliness_quarantine_windows,
            },
            "metadata": metadata,
            "organisationUnits": organisation_units,
            "organisationUnitExclusions": [
                asdict(item) for item in job.organisation_units.exclusions
            ],
            "organisationUnitConflictPolicy": (
                job.organisation_units.synchronization.conflict_policy
            ),
            "detectedOrganisationUnitConflicts": list(
                getattr(context, "detected_organisation_unit_conflicts", ())
            ),
            "organisationUnitQuarantineActive": bool(
                getattr(context, "detected_organisation_unit_conflicts", ())
            ),
            "excludedSourceOrganisationUnitCount": len(
                excluded_source_organisation_units
            ),
            "excludedSourceOrganisationUnits": [
                {
                    field: unit[field]
                    for field in ("id", "code", "name", "path", "level")
                    if unit.get(field) is not None
                }
                for unit in excluded_source_organisation_units
            ],
            "windows": window_results,
            "configurationSnapshot": str(snapshot_path),
        }
        if metadata_pending_windows:
            summary["nextStep"] = (
                "Historical metadata was validated but not committed in dry-run mode. "
                "Review the window reports, then run the same transfer with --apply."
            )
        summary_path = _write_json(run_dir / "run-summary.json", summary)
        summary["reportPath"] = str(summary_path)
        journal.record_event(
            run_id=parent_run_id,
            event="parent.finished",
            level="error" if failures else "info",
            details={
                "status": status,
                "completed": completed,
                "skipped": skipped,
                "failed": len(failures),
                "metadataPending": len(metadata_pending_windows),
                "reportPath": str(summary_path),
            },
        )
        journal.finish_parent_run(
            run_id=parent_run_id,
            status=status,
            report_path=str(summary_path),
            error=failures[0]["error"] if failures else None,
        )
        parent_finished = True
        if failures:
            message = (
                f"Transfer stopped with {len(failures)} failed window(s). "
                f"Audit: {summary_path}. First error: {failures[0]['error']}"
            )
            if failures[0]["transient"]:
                raise TransientPipelineError(message)
            raise PipelineError(message)
        _emit(
            progress,
            "orchestrator.done",
            completed=completed,
            skipped=skipped,
            reportPath=str(summary_path),
        )
        return summary
    except Exception as exc:
        # A failure before summary creation remains visible in the parent journal.
        if not parent_finished:
            journal.finish_parent_run(
                run_id=parent_run_id,
                status="failed",
                error=str(exc),
            )
        raise


def _inline_runner(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    *,
    job_id: str,
    apply: bool,
    force: bool,
    keep_payload: bool,
    progress: ProgressCallback | None,
) -> WindowRunner:
    def run(
        start: str,
        end: str,
        _index: int,
        _count: int,
        _window_id: str,
        _parent_run_id: str,
    ) -> dict[str, Any]:
        return run_transfer(
            scenario,
            source,
            target,
            job_id=job_id,
            apply=apply,
            force=True,
            keep_payload=keep_payload,
            start=start,
            end=end,
            progress=progress,
            resume_chunk_checkpoints=not force,
        )

    return run


def _fingerprint(
    scenario: TransferScenario,
    source: DHIS2Client,
    target: DHIS2Client,
    source_data_set: str,
    target_data_set: str,
    start: str,
    end: str,
    config_hash: str,
    job_id: str,
) -> TransferFingerprint:
    return TransferFingerprint(
        scenario_id=scenario.id,
        job_id=job_id,
        source_instance=f"{scenario.source_instance}@{source.base_url}",
        target_instance=f"{scenario.target_instance}@{target.base_url}",
        source_data_set=source_data_set,
        target_data_set=target_data_set,
        start=start,
        end=end,
        config_hash=config_hash,
    )


def _is_transient(exc: Exception) -> bool:
    return isinstance(exc, TransientPipelineError) or (
        isinstance(exc, HttpRequestError)
        and is_transient_transfer_http_error(exc)
    )


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _emit(
    progress: ProgressCallback | None, event: str, **details: Any
) -> None:
    if progress is not None:
        progress(event, details)
