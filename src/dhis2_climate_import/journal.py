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
        with closing(sqlite3.connect(self.path)) as connection:
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
        with closing(sqlite3.connect(self.path)) as connection:
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

    def _init(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
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
            connection.commit()
