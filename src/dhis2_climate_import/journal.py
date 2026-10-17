from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any


@dataclass(frozen=True)
class ImportFingerprint:
    dataset_id: str
    start: str
    end: str
    period_type: str
    data_element_id: str
    org_unit_level: int
    org_unit_cache_hash: str

    def value(self) -> str:
        raw = json.dumps(self.__dict__, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TransferFingerprint:
    scenario_id: str
    job_id: str
    source_instance: str
    target_instance: str
    source_data_set: str
    target_data_set: str
    start: str
    end: str
    config_hash: str

    def value(self) -> str:
        raw = json.dumps(self.__dict__, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class RunJournal:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def has_successful_import(self, fingerprint: ImportFingerprint) -> bool:
        with closing(sqlite3.connect(self.path)) as connection:
            row = connection.execute(
                """
                select 1 from import_runs
                where fingerprint = ? and dry_run = 0 and status = 'success'
                limit 1
                """,
                (fingerprint.value(),),
            ).fetchone()
            return row is not None

    def record(
        self,
        *,
        run_id: str,
        command: str,
        fingerprint: ImportFingerprint,
        dry_run: bool,
        status: str,
        counts: dict[str, int],
        report_path: str | None = None,
        error: str | None = None,
    ) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                """
                insert into import_runs (
                    run_id, created_at, command, fingerprint, dataset_id, start_date, end_date,
                    period_type, data_element_id, org_unit_level, org_unit_cache_hash, dry_run,
                    status, imported, updated, ignored, deleted, report_path, error
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    command,
                    fingerprint.value(),
                    fingerprint.dataset_id,
                    fingerprint.start,
                    fingerprint.end,
                    fingerprint.period_type,
                    fingerprint.data_element_id,
                    fingerprint.org_unit_level,
                    fingerprint.org_unit_cache_hash,
                    1 if dry_run else 0,
                    status,
                    int(counts.get("imported", 0)),
                    int(counts.get("updated", 0)),
                    int(counts.get("ignored", 0)),
                    int(counts.get("deleted", 0)),
                    report_path,
                    error,
                ),
            )
            connection.commit()

    def _init(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                """
                create table if not exists import_runs (
                    run_id text primary key,
                    created_at text not null,
                    command text not null,
                    fingerprint text not null,
                    dataset_id text not null,
                    start_date text not null,
                    end_date text not null,
                    period_type text not null,
                    data_element_id text not null,
                    org_unit_level integer not null,
                    org_unit_cache_hash text not null,
                    dry_run integer not null,
                    status text not null,
                    imported integer not null default 0,
                    updated integer not null default 0,
                    ignored integer not null default 0,
                    deleted integer not null default 0,
                    report_path text,
                    error text
                )
                """
            )
            connection.execute(
                "create index if not exists idx_import_runs_fingerprint on import_runs(fingerprint)"
            )
            connection.commit()


class TransferRunJournal:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def has_successful_transfer(self, fingerprint: TransferFingerprint) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                select 1 from transfer_runs
                where fingerprint = ? and dry_run = 0 and status = 'success'
                limit 1
                """,
                (fingerprint.value(),),
            ).fetchone()
            return row is not None

    def record(
        self,
        *,
        run_id: str,
        fingerprint: TransferFingerprint,
        dry_run: bool,
        status: str,
        source_count: int,
        counts: dict[str, int],
        report_path: str | None = None,
        error: str | None = None,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                insert into transfer_runs (
                    run_id, created_at, scenario_id, job_id, source_instance,
                    target_instance, source_data_set, target_data_set, start_date, end_date,
                    config_hash, fingerprint, dry_run, status, source_count,
                    imported, updated, ignored, deleted, report_path, error
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    datetime.now(timezone.utc).isoformat(),
                    fingerprint.scenario_id,
                    fingerprint.job_id,
                    fingerprint.source_instance,
                    fingerprint.target_instance,
                    fingerprint.source_data_set,
                    fingerprint.target_data_set,
                    fingerprint.start,
                    fingerprint.end,
                    fingerprint.config_hash,
                    fingerprint.value(),
                    int(dry_run),
                    status,
                    source_count,
                    counts.get("imported", 0),
                    counts.get("updated", 0),
                    counts.get("ignored", 0),
                    counts.get("deleted", 0),
                    report_path,
                    error,
                ),
            )
            connection.commit()

    def start_parent_run(
        self,
        *,
        run_id: str,
        fingerprint: TransferFingerprint,
        dry_run: bool,
        window_count: int,
        configuration_snapshot: str | None = None,
    ) -> None:
        now = _utc_now()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                insert into transfer_parent_runs (
                    run_id, created_at, started_at, updated_at, scenario_id, job_id,
                    source_instance, target_instance, source_data_set, target_data_set,
                    start_date, end_date, config_hash, fingerprint, dry_run, status,
                    window_count, configuration_snapshot
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?)
                """,
                (
                    run_id,
                    now,
                    now,
                    now,
                    fingerprint.scenario_id,
                    fingerprint.job_id,
                    fingerprint.source_instance,
                    fingerprint.target_instance,
                    fingerprint.source_data_set,
                    fingerprint.target_data_set,
                    fingerprint.start,
                    fingerprint.end,
                    fingerprint.config_hash,
                    fingerprint.value(),
                    int(dry_run),
                    window_count,
                    configuration_snapshot,
                ),
            )
            connection.commit()

    def finish_parent_run(
        self,
        *,
        run_id: str,
        status: str,
        report_path: str | None = None,
        error: str | None = None,
    ) -> None:
        now = _utc_now()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                update transfer_parent_runs
                set updated_at = ?, finished_at = ?, status = ?, report_path = ?, error = ?
                where run_id = ?
                """,
                (now, now, status, report_path, error, run_id),
            )
            connection.commit()

    def mark_running_windows_interrupted(self, *, job_id: str) -> int:
        now = _utc_now()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                update transfer_parent_runs
                set status = 'interrupted', finished_at = ?, updated_at = ?,
                    error = coalesce(error, 'Previous process stopped before completion')
                where job_id = ? and status = 'running'
                """,
                (now, now, job_id),
            )
            cursor = connection.execute(
                """
                update transfer_windows
                set status = 'interrupted', finished_at = ?, updated_at = ?,
                    error_type = 'interrupted',
                    error = coalesce(error, 'Previous process stopped before completion')
                where job_id = ? and status in (
                    'running', 'metadata', 'exporting', 'validating', 'importing'
                )
                """,
                (now, now, job_id),
            )
            connection.commit()
            return int(cursor.rowcount)

    def has_successful_window(
        self,
        *,
        fingerprint: TransferFingerprint,
        dry_run: bool,
    ) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                select 1 from transfer_windows
                where fingerprint = ? and dry_run = ? and status = 'success'
                limit 1
                """,
                (fingerprint.value(), int(dry_run)),
            ).fetchone()
            return row is not None

    def start_window(
        self,
        *,
        window_id: str,
        run_id: str,
        fingerprint: TransferFingerprint,
        dry_run: bool,
        window_index: int,
        window_count: int,
        attempt: int,
    ) -> None:
        now = _utc_now()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                insert into transfer_windows (
                    window_id, run_id, created_at, started_at, updated_at, scenario_id,
                    job_id, source_instance, target_instance, source_data_set,
                    target_data_set, start_date, end_date, config_hash, fingerprint,
                    dry_run, status, phase, window_index, window_count, attempt
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          'running', 'starting', ?, ?, ?)
                on conflict(window_id) do update set
                    run_id = excluded.run_id,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at,
                    status = 'running',
                    phase = 'starting',
                    attempt = excluded.attempt,
                    finished_at = null,
                    error_type = null,
                    http_status = null,
                    error = null
                """,
                (
                    window_id,
                    run_id,
                    now,
                    now,
                    now,
                    fingerprint.scenario_id,
                    fingerprint.job_id,
                    fingerprint.source_instance,
                    fingerprint.target_instance,
                    fingerprint.source_data_set,
                    fingerprint.target_data_set,
                    fingerprint.start,
                    fingerprint.end,
                    fingerprint.config_hash,
                    fingerprint.value(),
                    int(dry_run),
                    window_index,
                    window_count,
                    attempt,
                ),
            )
            connection.commit()

    def update_window_phase(self, *, window_id: str, phase: str) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                update transfer_windows set status = ?, phase = ?, updated_at = ?
                where window_id = ?
                """,
                (phase, phase, _utc_now(), window_id),
            )
            connection.commit()

    def finish_window(
        self,
        *,
        window_id: str,
        status: str,
        source_count: int = 0,
        counts: dict[str, int] | None = None,
        report_path: str | None = None,
        error_type: str | None = None,
        http_status: int | None = None,
        error: str | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        values = counts or {}
        now = _utc_now()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                update transfer_windows
                set updated_at = ?, finished_at = ?, status = ?, phase = ?,
                    source_count = ?, imported = ?, updated = ?, ignored = ?,
                    deleted = ?, report_path = ?, error_type = ?, http_status = ?,
                    error = ?, duration_seconds = ?
                where window_id = ?
                """,
                (
                    now,
                    now,
                    status,
                    status,
                    source_count,
                    int(values.get("imported", 0)),
                    int(values.get("updated", 0)),
                    int(values.get("ignored", 0)),
                    int(values.get("deleted", 0)),
                    report_path,
                    error_type,
                    http_status,
                    error,
                    duration_seconds,
                    window_id,
                ),
            )
            connection.commit()

    def record_event(
        self,
        *,
        run_id: str,
        event: str,
        details: dict[str, Any] | None = None,
        window_id: str | None = None,
        level: str = "info",
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                insert into transfer_events (
                    created_at, run_id, window_id, level, event, details_json
                ) values (?, ?, ?, ?, ?, ?)
                """,
                (
                    _utc_now(),
                    run_id,
                    window_id,
                    level,
                    event,
                    json.dumps(details or {}, sort_keys=True),
                ),
            )
            connection.commit()

    def window_rows(self, *, run_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                select * from transfer_windows
                where run_id = ? order by window_index
                """,
                (run_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.execute("pragma busy_timeout = 30000")
        connection.execute("pragma journal_mode = WAL")
        return connection

    def _init(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                create table if not exists transfer_runs (
                    run_id text primary key,
                    created_at text not null,
                    scenario_id text not null,
                    job_id text not null,
                    source_instance text not null,
                    target_instance text not null,
                    source_data_set text not null,
                    target_data_set text not null,
                    start_date text not null,
                    end_date text not null,
                    config_hash text not null,
                    fingerprint text not null,
                    dry_run integer not null,
                    status text not null,
                    source_count integer not null,
                    imported integer not null,
                    updated integer not null,
                    ignored integer not null,
                    deleted integer not null,
                    report_path text,
                    error text
                )
                """
            )
            connection.execute(
                """
                create index if not exists transfer_runs_fingerprint_idx
                on transfer_runs (fingerprint, dry_run, status)
                """
            )
            connection.execute(
                """
                create table if not exists transfer_parent_runs (
                    run_id text primary key,
                    created_at text not null,
                    started_at text not null,
                    updated_at text not null,
                    finished_at text,
                    scenario_id text not null,
                    job_id text not null,
                    source_instance text not null,
                    target_instance text not null,
                    source_data_set text not null,
                    target_data_set text not null,
                    start_date text not null,
                    end_date text not null,
                    config_hash text not null,
                    fingerprint text not null,
                    dry_run integer not null,
                    status text not null,
                    window_count integer not null,
                    configuration_snapshot text,
                    report_path text,
                    error text
                )
                """
            )
            connection.execute(
                """
                create table if not exists transfer_windows (
                    window_id text primary key,
                    run_id text not null,
                    created_at text not null,
                    started_at text,
                    updated_at text not null,
                    finished_at text,
                    scenario_id text not null,
                    job_id text not null,
                    source_instance text not null,
                    target_instance text not null,
                    source_data_set text not null,
                    target_data_set text not null,
                    start_date text not null,
                    end_date text not null,
                    config_hash text not null,
                    fingerprint text not null,
                    dry_run integer not null,
                    status text not null,
                    phase text not null,
                    window_index integer not null,
                    window_count integer not null,
                    attempt integer not null default 1,
                    source_count integer not null default 0,
                    imported integer not null default 0,
                    updated integer not null default 0,
                    ignored integer not null default 0,
                    deleted integer not null default 0,
                    report_path text,
                    error_type text,
                    http_status integer,
                    error text,
                    duration_seconds real
                )
                """
            )
            connection.execute(
                """
                create index if not exists transfer_windows_fingerprint_idx
                on transfer_windows (fingerprint, dry_run, status)
                """
            )
            connection.execute(
                """
                create table if not exists transfer_events (
                    id integer primary key autoincrement,
                    created_at text not null,
                    run_id text not null,
                    window_id text,
                    level text not null,
                    event text not null,
                    details_json text not null
                )
                """
            )
            connection.execute(
                """
                create index if not exists transfer_events_run_idx
                on transfer_events (run_id, window_id, created_at)
                """
            )
            columns = {
                str(row[1])
                for row in connection.execute(
                    "pragma table_info(transfer_windows)"
                ).fetchall()
            }
            if "duration_seconds" not in columns:
                connection.execute(
                    "alter table transfer_windows add column duration_seconds real"
                )
            connection.commit()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
