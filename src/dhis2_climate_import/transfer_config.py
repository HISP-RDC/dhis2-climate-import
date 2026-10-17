from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from .config import load_structured_config
from .errors import PipelineError


_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_PERIOD_TYPES = {"day", "week", "month", "year"}
_ID_SCHEMES = {"uid": "UID", "code": "CODE"}


@dataclass(frozen=True)
class InstanceAuthConfig:
    method: str
    token_env: str | None = None
    username_env: str | None = None
    password_env: str | None = None


@dataclass(frozen=True)
class InstanceProfile:
    id: str
    base_url: str | None
    base_url_env: str | None
    auth: InstanceAuthConfig
    transport: str | None = None

    def client_env(self, runtime_env: dict[str, str]) -> dict[str, str]:
        base_url = self.base_url or _required_env(
            runtime_env, self.base_url_env, self.id, "base URL"
        )
        method = self.auth.method
        result = {
            key: value
            for key, value in runtime_env.items()
            if key
            in {
                "DHIS2_DEFAULT_PAGE_SIZE",
                "DHIS2_TIMEOUT_SECONDS",
                "DHIS2_CONNECT_TIMEOUT_SECONDS",
                "DHIS2_RETRIES",
                "DHIS2_CLIENT_LOG_LEVEL",
            }
        }
        result["DHIS2_BASE_URL"] = base_url
        result["DHIS2_TRANSPORT"] = self.transport or runtime_env.get(
            "DHIS2_TRANSPORT", "dhis2-client"
        )
        result["DHIS2_AUTH_METHOD"] = method

        token = _optional_env(runtime_env, self.auth.token_env)
        username = _optional_env(runtime_env, self.auth.username_env)
        password = _optional_env(runtime_env, self.auth.password_env)
        if method == "pat":
            if not token:
                raise PipelineError(
                    f"Instance profile '{self.id}' requires PAT environment variable "
                    f"'{self.auth.token_env}'."
                )
            result["DHIS2_PAT"] = token
        elif method == "basic":
            if not username or not password:
                raise PipelineError(
                    f"Instance profile '{self.id}' requires username/password environment "
                    f"variables '{self.auth.username_env}' and '{self.auth.password_env}'."
                )
            result["DHIS2_USERNAME"] = username
            result["DHIS2_PASSWORD"] = password
        else:
            if token:
                result["DHIS2_PAT"] = token
            elif username and password:
                result["DHIS2_USERNAME"] = username
                result["DHIS2_PASSWORD"] = password
            else:
                raise PipelineError(
                    f"Instance profile '{self.id}' uses auth method 'auto' but none of its "
                    "configured PAT or username/password environment variables are set."
                )
        return result


@dataclass(frozen=True)
class InstanceRegistry:
    path: Path
    instances: dict[str, InstanceProfile]

    def get(self, profile_id: str) -> InstanceProfile:
        try:
            return self.instances[profile_id]
        except KeyError as exc:
            known = ", ".join(sorted(self.instances))
            raise PipelineError(
                f"Unknown instance profile '{profile_id}'. Known profiles: {known}"
            ) from exc


@dataclass(frozen=True)
class MetadataPolicy:
    mode: str = "require_existing"
    identity_strategy: str = "preserve_uid"
    reconcile_existing: bool = False
    skip_sharing: bool = True
    import_strategy: str = "CREATE"
    atomic_mode: str = "ALL"


@dataclass(frozen=True)
class OrganisationUnitPolicy:
    discovery: str = "linked_to_dataset"
    source_roots: tuple[str, ...] = ()
    include_children: bool = False
    mapping_strategy: str = "uid"
    batch_size: int = 40


@dataclass(frozen=True)
class IdSchemePolicy:
    data_elements: str = "uid"
    organisation_units: str = "uid"
    category_option_combos: str = "uid"
    attribute_option_combos: str = "uid"
    data_sets: str = "uid"

    def api_params(self) -> dict[str, str]:
        return {
            "dataElementIdScheme": _ID_SCHEMES[self.data_elements],
            "orgUnitIdScheme": _ID_SCHEMES[self.organisation_units],
            "categoryOptionComboIdScheme": _ID_SCHEMES[self.category_option_combos],
            "attributeOptionComboIdScheme": _ID_SCHEMES[self.attribute_option_combos],
            "dataSetIdScheme": _ID_SCHEMES[self.data_sets],
        }


@dataclass(frozen=True)
class TransferSettings:
    import_strategy: str = "CREATE_AND_UPDATE"
    chunk_size: int = 5000
    source_period_batch_size: int = 1
    preheat_cache: bool = True
    include_deleted: bool = False
    preserve_comments: bool = True


@dataclass(frozen=True)
class TransferJobConfig:
    id: str
    source_data_set: str
    target_data_set: str
    period_type: str
    default_start: str | None
    default_end: str | None
    default_start_period: str | None
    default_end_period: str | None
    organisation_units: OrganisationUnitPolicy
    id_schemes: IdSchemePolicy
    transfer: TransferSettings


@dataclass(frozen=True)
class TransferScenario:
    path: Path
    id: str
    source_instance: str
    target_instance: str
    metadata: MetadataPolicy
    state_directory: str
    jobs: tuple[TransferJobConfig, ...]

    @property
    def project_root(self) -> Path:
        for parent in (self.path.parent, *self.path.parents):
            if parent.name == "config":
                return parent.parent
        return self.path.parent

    @property
    def state_dir(self) -> Path:
        return self.project_root / self.state_directory / self.target_instance / self.id

    def get_job(self, job_id: str) -> TransferJobConfig:
        for job in self.jobs:
            if job.id == job_id:
                return job
        known = ", ".join(job.id for job in self.jobs)
        raise PipelineError(f"Unknown transfer job '{job_id}'. Known jobs: {known}")


def load_instance_registry(path: str | Path) -> InstanceRegistry:
    config_path = Path(path)
    data = load_structured_config(config_path, interpolate=False)
    root = _mapping(data, "instances configuration")
    schema_version = int(root.get("schema_version", 2))
    if schema_version != 2:
        raise PipelineError("Instance configuration schema_version must be 2.")
    raw_instances = _mapping(root.get("instances"), "instances")
    if not raw_instances:
        raise PipelineError("Instance configuration must define at least one instance.")

    instances: dict[str, InstanceProfile] = {}
    for profile_id, item in raw_instances.items():
        _validate_safe_id(str(profile_id), "instance profile")
        raw = _mapping(item, f"instances.{profile_id}")
        if str(raw.get("type", "dhis2")).lower() != "dhis2":
            raise PipelineError(f"Instance profile '{profile_id}' must have type 'dhis2'.")
        auth_raw = _mapping(raw.get("auth"), f"instances.{profile_id}.auth")
        method = str(auth_raw.get("method", "auto")).lower()
        if method not in {"auto", "pat", "basic"}:
            raise PipelineError(
                f"instances.{profile_id}.auth.method must be one of: auto, pat, basic."
            )
        base_url = _optional_string(raw.get("base_url"))
        base_url_env = _optional_string(raw.get("base_url_env"))
        if not base_url and not base_url_env:
            raise PipelineError(
                f"Instance profile '{profile_id}' requires base_url or base_url_env."
            )
        instances[str(profile_id)] = InstanceProfile(
            id=str(profile_id),
            base_url=base_url,
            base_url_env=base_url_env,
            transport=_optional_string(raw.get("transport")),
            auth=InstanceAuthConfig(
                method=method,
                token_env=_optional_string(auth_raw.get("token_env")),
                username_env=_optional_string(auth_raw.get("username_env")),
                password_env=_optional_string(auth_raw.get("password_env")),
            ),
        )
    return InstanceRegistry(path=config_path, instances=instances)


def load_transfer_scenario(path: str | Path) -> TransferScenario:
    config_path = Path(path)
    data = load_structured_config(config_path, interpolate=False)
    root = _mapping(data, "transfer scenario")
    if int(root.get("schema_version", 2)) != 2:
        raise PipelineError("Transfer scenario schema_version must be 2.")

    scenario_id = _required_string(root.get("id"), "scenario.id")
    _validate_safe_id(scenario_id, "scenario")
    source_raw = _mapping(root.get("source"), "source")
    target_raw = _mapping(root.get("target"), "target")
    source_instance = _required_string(source_raw.get("instance"), "source.instance")
    target_instance = _required_string(target_raw.get("instance"), "target.instance")
    if source_instance == target_instance:
        raise PipelineError("Source and target instance profiles must be different.")

    metadata_raw = _mapping(target_raw.get("metadata", {}), "target.metadata")
    metadata_mode = str(metadata_raw.get("mode", "require_existing")).lower()
    if metadata_mode not in {"require_existing", "create_if_missing"}:
        raise PipelineError(
            "target.metadata.mode must be 'require_existing' or 'create_if_missing'."
        )
    identity_strategy = str(metadata_raw.get("identity_strategy", "preserve_uid")).lower()
    if identity_strategy != "preserve_uid":
        raise PipelineError("Only target.metadata.identity_strategy=preserve_uid is supported.")
    metadata_import_raw = _mapping(metadata_raw.get("import", {}), "target.metadata.import")
    metadata = MetadataPolicy(
        mode=metadata_mode,
        identity_strategy=identity_strategy,
        reconcile_existing=_boolean(metadata_raw.get("reconcile_existing", False)),
        skip_sharing=_boolean(metadata_raw.get("skip_sharing", True)),
        import_strategy=str(metadata_import_raw.get("strategy", "CREATE")).upper(),
        atomic_mode="ALL" if _boolean(metadata_import_raw.get("atomic", True)) else "NONE",
    )
    if metadata.import_strategy not in {"CREATE", "CREATE_AND_UPDATE"}:
        raise PipelineError(
            "target.metadata.import.strategy must be CREATE or CREATE_AND_UPDATE."
        )

    default_ou = _mapping(target_raw.get("organisation_units", {}), "target.organisation_units")
    default_transfer = _mapping(root.get("transfer", {}), "transfer")
    default_mapping = _mapping(root.get("mapping", {}), "mapping")
    jobs_raw = root.get("jobs")
    if not isinstance(jobs_raw, list) or not jobs_raw:
        raise PipelineError("Transfer scenario must define at least one job.")

    jobs = tuple(
        _parse_job(
            item,
            default_ou=default_ou,
            default_transfer=default_transfer,
            default_mapping=default_mapping,
        )
        for item in jobs_raw
    )
    ids = [job.id for job in jobs]
    if len(ids) != len(set(ids)):
        raise PipelineError("Transfer job IDs must be unique within a scenario.")

    state_raw = _mapping(root.get("state", {}), "state")
    state_directory = str(state_raw.get("directory", "state"))
    if Path(state_directory).is_absolute() or ".." in Path(state_directory).parts:
        raise PipelineError("state.directory must be a safe relative path.")
    return TransferScenario(
        path=config_path,
        id=scenario_id,
        source_instance=source_instance,
        target_instance=target_instance,
        metadata=metadata,
        state_directory=state_directory,
        jobs=jobs,
    )


def _parse_job(
    item: Any,
    *,
    default_ou: dict[str, Any],
    default_transfer: dict[str, Any],
    default_mapping: dict[str, Any],
) -> TransferJobConfig:
    raw = _mapping(item, "jobs[]")
    job_id = _required_string(raw.get("id"), "jobs[].id")
    _validate_safe_id(job_id, "transfer job")
    source_data_set = _required_string(raw.get("source_data_set"), f"jobs.{job_id}.source_data_set")
    target_data_set = str(raw.get("target_data_set") or source_data_set)
    period_type = _normalise_period_type(
        _required_string(raw.get("period_type"), f"jobs.{job_id}.period_type")
    )

    ou_raw = {**default_ou, **_mapping(raw.get("organisation_units", {}), "organisation_units")}
    source_roots = _string_tuple(ou_raw.get("source_roots", ()))
    default_discovery = "configured_roots" if source_roots else "linked_to_dataset"
    discovery = str(ou_raw.get("discovery", default_discovery))
    if discovery not in {"linked_to_dataset", "configured_roots"}:
        raise PipelineError(
            f"jobs.{job_id}.organisation_units.discovery must be linked_to_dataset or "
            "configured_roots."
        )
    if discovery == "configured_roots" and not source_roots:
        raise PipelineError(f"jobs.{job_id} configured_roots requires source_roots.")

    mapping_raw = {**default_mapping, **_mapping(raw.get("mapping", {}), "mapping")}
    data_elements_raw = _mapping(raw.get("data_elements", {}), "data_elements")
    discovery_mode = str(data_elements_raw.get("discovery", "linked_to_dataset"))
    if discovery_mode != "linked_to_dataset":
        raise PipelineError(
            f"jobs.{job_id}.data_elements.discovery must be linked_to_dataset."
        )
    de_scheme = data_elements_raw.get(
        "mapping_strategy", mapping_raw.get("data_elements", "uid")
    )
    id_schemes = IdSchemePolicy(
        data_elements=_id_scheme(de_scheme, f"jobs.{job_id}.data_elements"),
        organisation_units=_id_scheme(
            ou_raw.get("mapping_strategy", mapping_raw.get("organisation_units", "uid")),
            f"jobs.{job_id}.organisation_units",
        ),
        category_option_combos=_id_scheme(
            mapping_raw.get("category_option_combos", "uid"),
            f"jobs.{job_id}.category_option_combos",
        ),
        attribute_option_combos=_id_scheme(
            mapping_raw.get("attribute_option_combos", "uid"),
            f"jobs.{job_id}.attribute_option_combos",
        ),
        data_sets=_id_scheme(
            mapping_raw.get("data_sets", "uid"), f"jobs.{job_id}.data_sets"
        ),
    )

    transfer_raw = {
        **default_transfer,
        **_mapping(raw.get("transfer", {}), f"jobs.{job_id}.transfer"),
    }
    transfer = TransferSettings(
        import_strategy=str(transfer_raw.get("import_strategy", "CREATE_AND_UPDATE")).upper(),
        chunk_size=_positive_int(transfer_raw.get("chunk_size", 5000), "chunk_size"),
        source_period_batch_size=_positive_int(
            transfer_raw.get(
                "source_period_batch_size",
                _default_source_period_batch_size(period_type),
            ),
            "source_period_batch_size",
        ),
        preheat_cache=_boolean(transfer_raw.get("preheat_cache", True)),
        include_deleted=_boolean(transfer_raw.get("include_deleted", False)),
        preserve_comments=_boolean(transfer_raw.get("preserve_comments", True)),
    )
    if transfer.import_strategy not in {"CREATE", "UPDATE", "CREATE_AND_UPDATE"}:
        raise PipelineError(
            f"jobs.{job_id}.transfer.import_strategy must be CREATE, UPDATE or "
            "CREATE_AND_UPDATE."
        )

    return TransferJobConfig(
        id=job_id,
        source_data_set=source_data_set,
        target_data_set=target_data_set,
        period_type=period_type,
        default_start=_optional_string(raw.get("default_start")),
        default_end=_optional_string(raw.get("default_end")),
        default_start_period=_optional_string(raw.get("default_start_period")),
        default_end_period=_optional_string(raw.get("default_end_period")),
        organisation_units=OrganisationUnitPolicy(
            discovery=discovery,
            source_roots=source_roots,
            include_children=_boolean(ou_raw.get("include_children", bool(source_roots))),
            mapping_strategy=id_schemes.organisation_units,
            batch_size=_positive_int(ou_raw.get("batch_size", 40), "batch_size"),
        ),
        id_schemes=id_schemes,
        transfer=transfer,
    )


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise PipelineError(f"Expected '{label}' to be a mapping/object.")
    return value


def _required_string(value: Any, label: str) -> str:
    if value in {None, ""}:
        raise PipelineError(f"Configuration field '{label}' is required.")
    return str(value)


def _optional_string(value: Any) -> str | None:
    return None if value in {None, ""} else str(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if item not in {None, ""})
    raise PipelineError("Expected a string or list of strings.")


def _boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower().strip()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return bool(value)


def _positive_int(value: Any, label: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"Configuration field '{label}' must be an integer.") from exc
    if result <= 0:
        raise PipelineError(f"Configuration field '{label}' must be greater than zero.")
    return result


def _normalise_period_type(value: str) -> str:
    aliases = {
        "daily": "day",
        "weekly": "week",
        "monthly": "month",
        "yearly": "year",
    }
    result = aliases.get(value.lower(), value.lower())
    if result not in _PERIOD_TYPES:
        raise PipelineError(f"Unsupported transfer period_type '{value}'.")
    return result


def _default_source_period_batch_size(period_type: str) -> int:
    return {
        "day": 31,
        "week": 13,
        "month": 1,
        "year": 1,
    }[period_type]


def _id_scheme(value: Any, label: str) -> str:
    result = str(value).lower()
    if result not in _ID_SCHEMES:
        raise PipelineError(f"{label} mapping strategy must be 'uid' or 'code'.")
    return result


def _validate_safe_id(value: str, label: str) -> None:
    if not _SAFE_ID.fullmatch(value):
        raise PipelineError(
            f"Invalid {label} ID '{value}'. Use letters, numbers, dots, underscores or dashes."
        )


def _required_env(
    env: dict[str, str], variable: str | None, profile_id: str, label: str
) -> str:
    if not variable:
        raise PipelineError(f"Instance profile '{profile_id}' does not configure its {label}.")
    value = env.get(variable, "").strip()
    if not value:
        raise PipelineError(
            f"Instance profile '{profile_id}' requires environment variable '{variable}'."
        )
    return value


def _optional_env(env: dict[str, str], variable: str | None) -> str:
    if not variable:
        return ""
    return env.get(variable, "").strip()
