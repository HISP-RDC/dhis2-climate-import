from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from typing import Any

from .errors import PipelineError


_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


@dataclass(frozen=True)
class Dhis2Settings:
    org_unit_level: int = 3
    climate_data_set: str | None = None
    climate_data_sets: tuple[str, ...] = ()
    cache_max_age_hours: int = 168
    page_size: int = 500
    chunk_size: int = 5000
    async_threshold: int = 5000
    import_strategy: str = "CREATE_AND_UPDATE"
    preheat_cache: bool = True
    trigger_analytics: bool = False


@dataclass(frozen=True)
class OcsSettings:
    aggregate_process_id: str = "aggregate_to_dhis2_json"
    publish: bool = True


@dataclass(frozen=True)
class ImportJobConfig:
    id: str
    label: str | None
    dataset_id: str
    data_set: str | None
    data_element: str
    period_type: str
    method: str
    expected_units: str
    default_start: str | None = None
    default_end: str | None = None
    default_start_period: str | None = None
    default_end_period: str | None = None
    org_unit_level: int | None = None
    expected_org_units_with_geometry: int | None = None
    expected_data_values: int | None = None
    expected_data_values_mode: str | None = None


@dataclass(frozen=True)
class PipelineConfig:
    path: Path
    dhis2: Dhis2Settings
    open_climate_service: OcsSettings
    imports: tuple[ImportJobConfig, ...]

    @property
    def root_dir(self) -> Path:
        return self.path.parent.parent if self.path.parent.name == "config" else self.path.parent

    def get_job(self, job_id: str) -> ImportJobConfig:
        for job in self.imports:
            if job.id == job_id:
                return job
        known = ", ".join(job.id for job in self.imports)
        raise PipelineError(f"Unknown import job '{job_id}'. Known jobs: {known}")


def load_pipeline_config(path: str | Path, env: dict[str, str] | None = None) -> PipelineConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise PipelineError(f"Pipeline configuration not found: {config_path}")

    raw_data = _load_structured_text(config_path.read_text(encoding="utf-8"))
    data = _interpolate(raw_data, env or os.environ)
    if not isinstance(data, dict):
        raise PipelineError("Pipeline configuration must be a mapping/object.")

    dhis2_raw = _as_mapping(data.get("dhis2", {}), "dhis2")
    ocs_raw = _as_mapping(data.get("open_climate_service", {}), "open_climate_service")
    imports_raw = data.get("imports")
    if not isinstance(imports_raw, list) or not imports_raw:
        raise PipelineError("Pipeline configuration must define at least one import job.")

    imports = tuple(_parse_import_job(item) for item in imports_raw)
    return PipelineConfig(
        path=config_path,
        dhis2=Dhis2Settings(
            org_unit_level=_as_int(dhis2_raw.get("org_unit_level", 3), "dhis2.org_unit_level"),
            climate_data_set=_optional_str(dhis2_raw.get("climate_data_set")),
            climate_data_sets=_as_str_tuple(dhis2_raw.get("climate_data_sets", [])),
            cache_max_age_hours=_as_int(
                dhis2_raw.get("cache_max_age_hours", 168), "dhis2.cache_max_age_hours"
            ),
            page_size=_as_int(dhis2_raw.get("page_size", 500), "dhis2.page_size"),
            chunk_size=_as_int(dhis2_raw.get("chunk_size", 5000), "dhis2.chunk_size"),
            async_threshold=_as_int(
                dhis2_raw.get("async_threshold", 5000), "dhis2.async_threshold"
            ),
            import_strategy=str(dhis2_raw.get("import_strategy", "CREATE_AND_UPDATE")),
            preheat_cache=_as_bool(dhis2_raw.get("preheat_cache", True)),
            trigger_analytics=_as_bool(dhis2_raw.get("trigger_analytics", False)),
        ),
        open_climate_service=OcsSettings(
            aggregate_process_id=str(
                ocs_raw.get("aggregate_process_id", "aggregate_to_dhis2_json")
            ),
            publish=_as_bool(ocs_raw.get("publish", True)),
        ),
        imports=imports,
    )


def _parse_import_job(item: Any) -> ImportJobConfig:
    raw = _as_mapping(item, "imports[]")
    required = ["id", "dataset_id", "data_element", "period_type", "method", "expected_units"]
    missing = [key for key in required if not raw.get(key)]
    if missing:
        raise PipelineError(f"Import job is missing required fields: {', '.join(missing)}")
    period_type = str(raw["period_type"]).lower()
    if period_type not in {"day", "month", "year", "week"}:
        raise PipelineError(
            f"Unsupported period_type '{period_type}' for import job '{raw['id']}'."
        )
    return ImportJobConfig(
        id=str(raw["id"]),
        label=_optional_str(raw.get("label")),
        dataset_id=str(raw["dataset_id"]),
        data_set=_optional_str(raw.get("data_set")),
        data_element=str(raw["data_element"]),
        period_type=period_type,
        method=str(raw["method"]),
        expected_units=str(raw["expected_units"]),
        default_start=_optional_str(raw.get("default_start")),
        default_end=_optional_str(raw.get("default_end")),
        default_start_period=_optional_str(raw.get("default_start_period")),
        default_end_period=_optional_str(raw.get("default_end_period")),
        org_unit_level=_optional_int(raw.get("org_unit_level"), f"imports.{raw['id']}.org_unit_level"),
        expected_org_units_with_geometry=_optional_int(
            raw.get("expected_org_units_with_geometry"),
            f"imports.{raw['id']}.expected_org_units_with_geometry",
        ),
        expected_data_values=_optional_int(
            raw.get("expected_data_values"),
            f"imports.{raw['id']}.expected_data_values",
        ),
        expected_data_values_mode=_optional_str(raw.get("expected_data_values_mode")),
    )


def _load_structured_text(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return {}
    if stripped.startswith("{") or stripped.startswith("["):
        return json.loads(stripped)
    return _parse_simple_yaml(text)


def _parse_simple_yaml(text: str) -> Any:
    """Parse the small YAML subset used by this project without external dependencies."""
    lines: list[tuple[int, str]] = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if "\t" in raw[:indent]:
            raise PipelineError("Tabs are not supported in configuration indentation.")
        lines.append((indent, raw.strip()))

    def parse_block(index: int, indent: int) -> tuple[Any, int]:
        if index >= len(lines):
            return {}, index
        current_indent, current_text = lines[index]
        if current_indent < indent:
            return {}, index

        if current_text.startswith("- "):
            values: list[Any] = []
            while index < len(lines):
                item_indent, item_text = lines[index]
                if item_indent != indent or not item_text.startswith("- "):
                    break
                rest = item_text[2:].strip()
                index += 1
                if not rest:
                    child, index = parse_block(index, indent + 2)
                    values.append(child)
                elif _looks_like_key_value(rest):
                    key, value = _split_key_value(rest)
                    item: dict[str, Any] = {}
                    if value == "":
                        child, index = parse_block(index, indent + 2)
                        item[key] = child
                    else:
                        item[key] = _parse_scalar(value)
                    if index < len(lines) and lines[index][0] == indent + 2:
                        extra, index = parse_block(index, indent + 2)
                        if not isinstance(extra, dict):
                            raise PipelineError("List item continuation must be a mapping.")
                        item.update(extra)
                    values.append(item)
                else:
                    values.append(_parse_scalar(rest))
            return values, index

        mapping: dict[str, Any] = {}
        while index < len(lines):
            item_indent, item_text = lines[index]
            if item_indent != indent or item_text.startswith("- "):
                break
            if not _looks_like_key_value(item_text):
                raise PipelineError(f"Invalid configuration line: {item_text}")
            key, value = _split_key_value(item_text)
            index += 1
            if value == "":
                if index < len(lines) and lines[index][0] > indent:
                    child, index = parse_block(index, lines[index][0])
                    mapping[key] = child
                else:
                    mapping[key] = None
            else:
                mapping[key] = _parse_scalar(value)
        return mapping, index

    parsed, final_index = parse_block(0, lines[0][0] if lines else 0)
    if final_index != len(lines):
        raise PipelineError("Could not parse full configuration file.")
    return parsed


def _looks_like_key_value(text: str) -> bool:
    return ":" in text and not text.startswith(("'", '"'))


def _split_key_value(text: str) -> tuple[str, str]:
    key, value = text.split(":", 1)
    key = key.strip()
    if not key:
        raise PipelineError(f"Invalid empty configuration key in line: {text}")
    return key, value.strip()


def _parse_scalar(value: str) -> Any:
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value in {"null", "None", "~"}:
        return None
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    if value.startswith("[") or value.startswith("{"):
        return json.loads(value)
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _interpolate(value: Any, env: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: _interpolate(child, env) for key, child in value.items()}
    if isinstance(value, list):
        return [_interpolate(child, env) for child in value]
    if isinstance(value, str):
        return _ENV_PATTERN.sub(lambda match: env.get(match.group(1), match.group(2) or ""), value)
    return value


def _as_mapping(value: Any, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise PipelineError(f"Expected '{label}' to be a mapping/object.")
    return value


def _as_int(value: Any, label: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"Expected '{label}' to be an integer.") from exc


def _optional_int(value: Any, label: str) -> int | None:
    if value in {None, ""}:
        return None
    return _as_int(value, label)


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list):
        return tuple(str(item) for item in value if item not in {None, ""})
    raise PipelineError("Expected a string or list of strings.")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return bool(value)


def _optional_str(value: Any) -> str | None:
    return None if value in {None, ""} else str(value)
