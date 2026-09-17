from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from .errors import PipelineError


def org_unit_cache_paths(root_dir: Path, level: int) -> tuple[Path, Path]:
    cache_dir = root_dir / "cache"
    return (
        cache_dir / f"org-units-level-{level}.geojson",
        cache_dir / f"org-units-level-{level}.manifest.json",
    )


def data_elements_cache_path(root_dir: Path) -> Path:
    return root_dir / "cache" / "data-elements.json"


def write_org_units_cache(
    *,
    root_dir: Path,
    level: int,
    source_base_url: str,
    organisation_units: list[dict[str, Any]],
) -> tuple[Path, Path]:
    missing_geometry = [
        str(unit.get("id") or "<unknown>")
        for unit in organisation_units
        if not unit.get("geometry")
    ]
    if missing_geometry:
        preview = ", ".join(missing_geometry[:10])
        raise PipelineError(
            "Cannot write org unit cache because level "
            f"{level} contains units without geometry: {preview}"
        )

    geojson_path, manifest_path = org_unit_cache_paths(root_dir, level)
    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    collection = {
        "type": "FeatureCollection",
        "features": [_org_unit_to_feature(unit, level) for unit in organisation_units],
    }
    _write_json(geojson_path, collection)
    geojson_hash = sha256_file(geojson_path)
    manifest = {
        "generatedAt": utc_now(),
        "source": source_base_url,
        "level": level,
        "count": len(organisation_units),
        "fields": ["id", "code", "name", "path", "level", "geometry"],
        "geojson": geojson_path.name,
        "geojsonSha256": geojson_hash,
    }
    _write_json(manifest_path, manifest)
    return geojson_path, manifest_path


def load_org_units_cache(
    *, root_dir: Path, level: int, max_age_hours: int
) -> tuple[dict[str, Any], dict[str, Any]]:
    geojson_path, manifest_path = org_unit_cache_paths(root_dir, level)
    if not geojson_path.exists() or not manifest_path.exists():
        raise PipelineError(
            f"Org unit cache for level {level} is missing. "
            f"Run: cache-org-units --level {level}"
        )
    geojson = _read_json(geojson_path)
    manifest = _read_json(manifest_path)
    _validate_manifest_age(manifest, max_age_hours, manifest_path)

    if manifest.get("level") != level:
        raise PipelineError(f"Org unit cache manifest level does not match requested level {level}.")
    if manifest.get("geojsonSha256") != sha256_file(geojson_path):
        raise PipelineError("Org unit cache hash mismatch. Refresh the org unit cache.")
    features = geojson.get("features")
    if not isinstance(features, list) or not features:
        raise PipelineError("Org unit cache has no GeoJSON features.")
    missing_geometry = [
        feature.get("id", "<unknown>")
        for feature in features
        if not feature.get("geometry")
    ]
    if missing_geometry:
        preview = ", ".join(str(item) for item in missing_geometry[:10])
        raise PipelineError(f"Org unit cache contains features without geometry: {preview}")
    return geojson, manifest


def write_data_elements_cache(
    *,
    root_dir: Path,
    source_base_url: str,
    data_elements: list[dict[str, Any]],
    data_set: dict[str, Any] | None = None,
    data_sets: list[dict[str, Any]] | None = None,
) -> Path:
    path = data_elements_cache_path(root_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    resolved_data_sets = data_sets or ([data_set] if data_set else [])
    _write_json(
        path,
        {
            "generatedAt": utc_now(),
            "source": source_base_url,
            "scope": "dataSet" if resolved_data_sets else "all",
            "dataSet": resolved_data_sets[0] if len(resolved_data_sets) == 1 else None,
            "dataSets": resolved_data_sets,
            "count": len(data_elements),
            "dataElements": data_elements,
        },
    )
    return path


def load_data_elements_cache(root_dir: Path) -> list[dict[str, Any]]:
    payload = load_data_elements_cache_payload(root_dir)
    data_elements = payload.get("dataElements")
    if not isinstance(data_elements, list) or not data_elements:
        raise PipelineError("Data element cache contains no dataElements.")
    return data_elements


def load_data_elements_cache_payload(root_dir: Path) -> dict[str, Any]:
    path = data_elements_cache_path(root_dir)
    if not path.exists():
        raise PipelineError("Data element cache is missing. Run: cache-data-elements")
    payload = _read_json(path)
    if isinstance(payload, list):
        return {
            "scope": "legacy",
            "dataSet": None,
            "dataSets": [],
            "dataElements": payload,
        }
    if not isinstance(payload, dict):
        raise PipelineError("Data element cache must be a JSON object or a legacy list.")
    return payload


def resolve_data_element(data_elements: list[dict[str, Any]], selector: str) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for data_element in data_elements:
        if selector in {
            data_element.get("id"),
            data_element.get("code"),
            data_element.get("name"),
            data_element.get("displayName"),
        }:
            matches.append(data_element)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise PipelineError(
            f"Data element selector '{selector}' matched {len(matches)} elements. Use UID or code."
        )
    sample = ", ".join(
        str(item.get("code") or item.get("id")) for item in data_elements[:10] if item.get("id")
    )
    raise PipelineError(
        f"Data element '{selector}' was not found in cache/data-elements.json. "
        f"Refresh the cache or update config/import-pipeline.yaml. Sample: {sample}"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _org_unit_to_feature(unit: dict[str, Any], level: int) -> dict[str, Any]:
    if int(unit.get("level", level)) != level:
        raise PipelineError(f"Received org unit outside level {level}: {unit.get('id')}")
    uid = unit.get("id")
    if not uid:
        raise PipelineError("Received org unit without id.")
    return {
        "type": "Feature",
        "id": uid,
        "geometry": unit["geometry"],
        "properties": {
            "id": uid,
            "code": unit.get("code"),
            "name": unit.get("name"),
            "path": unit.get("path"),
            "level": level,
        },
    }


def _validate_manifest_age(manifest: dict[str, Any], max_age_hours: int, path: Path) -> None:
    generated_at = manifest.get("generatedAt")
    if not isinstance(generated_at, str):
        raise PipelineError(f"Org unit cache manifest is missing generatedAt: {path}")
    parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    age_hours = (datetime.now(timezone.utc) - parsed).total_seconds() / 3600
    if age_hours > max_age_hours:
        raise PipelineError(
            f"Org unit cache is {age_hours:.1f} hours old, above the "
            f"{max_age_hours}h limit. Refresh it explicitly."
        )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)
