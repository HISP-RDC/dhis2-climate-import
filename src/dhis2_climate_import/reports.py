from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


def run_id(prefix: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{stamp}-{uuid4().hex[:8]}"


def write_report(root_dir: Path, name: str, payload: Any) -> Path:
    report_dir = root_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_errors_csv(root_dir: Path, name: str, rows: list[dict[str, Any]]) -> Path:
    report_dir = root_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"{name}.csv"
    headers = sorted({key for row in rows for key in row.keys()}) or ["message"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    return path


def import_counts(payload: Any) -> dict[str, int]:
    counts = _find_import_count(payload)
    return {
        "imported": int(counts.get("imported", 0)),
        "updated": int(counts.get("updated", 0)),
        "ignored": int(counts.get("ignored", 0)),
        "deleted": int(counts.get("deleted", 0)),
    }


def extract_conflicts(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            conflicts = value.get("conflicts")
            if isinstance(conflicts, list):
                for item in conflicts:
                    if isinstance(item, dict):
                        rows.append(item)
                    else:
                        rows.append({"message": str(item)})
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return rows


def _find_import_count(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        value = payload.get("importCount")
        if isinstance(value, dict):
            return value
        value = payload.get("dataValueCount")
        if isinstance(value, dict):
            return value
        response = payload.get("response")
        if isinstance(response, dict):
            value = response.get("importCount")
            if isinstance(value, dict):
                return value
            value = response.get("dataValueCount")
            if isinstance(value, dict):
                return value
        for child in payload.values():
            found = _find_import_count(child)
            if found:
                return found
    if isinstance(payload, list):
        total = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
        found_any = False
        for child in payload:
            found = _find_import_count(child)
            if found:
                found_any = True
                for key in total:
                    total[key] += int(found.get(key, 0))
        if found_any:
            return total
    return {}
