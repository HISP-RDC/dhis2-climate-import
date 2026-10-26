#!/usr/bin/env bash

set -u -o pipefail

readonly PROJECT_DIR="${DHIS2_CLIMATE_PROJECT_DIR:-/opt/dhis2-climate-import}"
readonly PYTHON_BIN="${DHIS2_CLIMATE_PYTHON_BIN:-${PROJECT_DIR}/.venv/bin/python}"
readonly DATE_PYTHON_BIN="${DHIS2_CLIMATE_DATE_PYTHON_BIN:-${PYTHON_BIN}}"
readonly ENV_FILE="${DHIS2_CLIMATE_ENV_FILE:-${PROJECT_DIR}/.env}"
readonly CONFIG_FILE="${DHIS2_CLIMATE_CONFIG_FILE:-${PROJECT_DIR}/config/import-pipeline.yaml}"
readonly STATE_DIR="${DHIS2_CLIMATE_STATE_DIR:-${PROJECT_DIR}/state/climate-pipeline}"
readonly CHECKPOINT_DIR="${STATE_DIR}/checkpoints"
readonly WINDOW_MODE="${DHIS2_CLIMATE_WINDOW_MODE:-rolling}"
readonly LOOKBACK_WEEKS="${DHIS2_CLIMATE_LOOKBACK_WEEKS:-8}"
readonly LAG_WEEKS="${DHIS2_CLIMATE_LAG_WEEKS:-3}"
readonly PERIOD_BATCH_SIZE="${DHIS2_CLIMATE_PERIOD_BATCH_SIZE:-13}"
readonly AS_OF="${DHIS2_CLIMATE_AS_OF:-}"
readonly CONTINUE_ON_FAILURE="${DHIS2_CLIMATE_CONTINUE_ON_FAILURE:-true}"
readonly REFRESH_CACHES="${DHIS2_CLIMATE_REFRESH_CACHES:-true}"
readonly SKIP_MISSING_GEOMETRY="${DHIS2_CLIMATE_SKIP_MISSING_GEOMETRY:-true}"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*"
}

fail_configuration() {
  log "ERROR: $*"
  exit 2
}

validate_boolean() {
  local name="$1"
  local value="$2"
  case "$value" in
    true|false) ;;
    *) fail_configuration "${name} must be true or false." ;;
  esac
}

validate_non_negative_integer() {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*) fail_configuration "${name} must be a non-negative integer." ;;
  esac
}

run_cli() {
  "$PYTHON_BIN" -m dhis2_climate_import \
    --config "$CONFIG_FILE" \
    --env-file "$ENV_FILE" \
    "$@"
}

resolve_period_window() {
  case "$WINDOW_MODE" in
    fixed)
      START_PERIOD="${DHIS2_CLIMATE_START_PERIOD:-}"
      END_PERIOD="${DHIS2_CLIMATE_END_PERIOD:-}"
      [[ -n "$START_PERIOD" ]] || fail_configuration \
        "DHIS2_CLIMATE_START_PERIOD is required in fixed mode."
      [[ -n "$END_PERIOD" ]] || fail_configuration \
        "DHIS2_CLIMATE_END_PERIOD is required in fixed mode."
      ;;
    rolling)
      validate_non_negative_integer DHIS2_CLIMATE_LOOKBACK_WEEKS "$LOOKBACK_WEEKS"
      validate_non_negative_integer DHIS2_CLIMATE_LAG_WEEKS "$LAG_WEEKS"
      [[ "$LOOKBACK_WEEKS" != "0" ]] || fail_configuration \
        "DHIS2_CLIMATE_LOOKBACK_WEEKS must be greater than zero."
      local resolved
      resolved="$($DATE_PYTHON_BIN -c '
from datetime import date, datetime, timedelta
import sys

lookback = int(sys.argv[1])
lag = int(sys.argv[2])
anchor = datetime.strptime(sys.argv[3], "%Y-%m-%d").date() if sys.argv[3] else date.today()
latest_complete_week_end = anchor - timedelta(days=anchor.isoweekday())
end = latest_complete_week_end - timedelta(weeks=lag)
start = end - timedelta(weeks=lookback - 1)

def period(value):
    iso = value.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

print(period(start), period(end))
' "$LOOKBACK_WEEKS" "$LAG_WEEKS" "$AS_OF")" || fail_configuration \
        "could not calculate the weekly window. Check DHIS2_CLIMATE_AS_OF."
      read -r START_PERIOD END_PERIOD <<< "$resolved"
      ;;
    *) fail_configuration "DHIS2_CLIMATE_WINDOW_MODE must be rolling or fixed." ;;
  esac

  "$DATE_PYTHON_BIN" -c '
from datetime import date
import re
import sys

def start(period):
    match = re.fullmatch(r"(\d{4})-?W(\d{1,2})", period)
    if not match:
        raise ValueError(f"invalid ISO week: {period}")
    return date.fromisocalendar(int(match.group(1)), int(match.group(2)), 1)

if start(sys.argv[1]) > start(sys.argv[2]):
    raise ValueError("start period is after end period")
' "$START_PERIOD" "$END_PERIOD" || fail_configuration \
    "invalid ISO window: ${START_PERIOD} to ${END_PERIOD}."
}

build_period_windows() {
  validate_non_negative_integer DHIS2_CLIMATE_PERIOD_BATCH_SIZE "$PERIOD_BATCH_SIZE"
  [[ "$PERIOD_BATCH_SIZE" != "0" ]] || fail_configuration \
    "DHIS2_CLIMATE_PERIOD_BATCH_SIZE must be greater than zero."

  WINDOW_STARTS=()
  WINDOW_ENDS=()
  while read -r window_start window_end; do
    [[ -n "$window_start" && -n "$window_end" ]] || continue
    WINDOW_STARTS+=("$window_start")
    WINDOW_ENDS+=("$window_end")
  done < <("$DATE_PYTHON_BIN" -c '
from datetime import date, timedelta
import re
import sys

def monday(period):
    match = re.fullmatch(r"(\d{4})-?W(\d{1,2})", period)
    if not match:
        raise ValueError(f"invalid ISO week: {period}")
    return date.fromisocalendar(int(match.group(1)), int(match.group(2)), 1)

def label(value):
    iso = value.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

current = monday(sys.argv[1])
end = monday(sys.argv[2])
batch_size = int(sys.argv[3])
while current <= end:
    window_end = min(current + timedelta(weeks=batch_size - 1), end)
    print(label(current), label(window_end))
    current = window_end + timedelta(weeks=1)
' "$START_PERIOD" "$END_PERIOD" "$PERIOD_BATCH_SIZE")

  ((${#WINDOW_STARTS[@]} > 0)) || fail_configuration \
    "no subwindow was calculated for ${START_PERIOD} to ${END_PERIOD}."
}

record_failure() {
  local job="$1"
  local stage="$2"
  local status="$3"
  local window_start="$4"
  local window_end="$5"
  FAILED_JOBS+=("$job")
  FAILED_STAGES+=("${job}:${window_start}:${window_end}:${stage}:${status}")
  if [[ "$status" == "2" ]]; then
    HAS_PERMANENT_FAILURE=true
  else
    HAS_TRANSIENT_FAILURE=true
  fi
  log "Climate job failed | job=$job | start=$window_start | end=$window_end | stage=$stage | exitStatus=$status"
}

write_checkpoint() {
  local job="$1"
  local checkpoint="$2"
  local window_start="$3"
  local window_end="$4"
  local temporary="${checkpoint}.tmp.$$"
  printf '{"job":"%s","startPeriod":"%s","endPeriod":"%s","completedAt":"%s"}\n' \
    "$job" "$window_start" "$window_end" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$temporary"
  mv "$temporary" "$checkpoint"
  log "Climate checkpoint recorded | job=$job | checkpoint=$checkpoint"
}

run_job() {
  local job="$1"
  local window_start="$2"
  local window_end="$3"
  local checkpoint="${CHECKPOINT_DIR}/${job}__${window_start}__${window_end}.json"
  local status
  local apply_log

  if [[ -f "$checkpoint" ]]; then
    log "Climate window already completed; checkpoint reused | job=$job | start=$window_start | end=$window_end"
    return 0
  fi

  log "OCS ingestion started | job=$job | start=$window_start | end=$window_end"
  run_cli ingest-dataset "$job" \
    --start-period "$window_start" \
    --end-period "$window_end" \
    --publish
  status=$?
  if ((status != 0)); then
    LAST_FAILURE_STAGE=ingest
    LAST_FAILURE_STATUS=$status
    return 1
  fi

  log "DHIS2 review started | job=$job | start=$window_start | end=$window_end"
  run_cli review-import "$job" \
    --start-period "$window_start" \
    --end-period "$window_end"
  status=$?
  if ((status != 0)); then
    LAST_FAILURE_STAGE=review
    LAST_FAILURE_STATUS=$status
    return 1
  fi

  log "DHIS2 dry-run started | job=$job | start=$window_start | end=$window_end"
  run_cli import-dhis2 "$job" \
    --start-period "$window_start" \
    --end-period "$window_end" \
    --dry-run
  status=$?
  if ((status != 0)); then
    LAST_FAILURE_STAGE=dry-run
    LAST_FAILURE_STATUS=$status
    return 1
  fi

  log "DHIS2 import started | job=$job | start=$window_start | end=$window_end"
  apply_log="${TEMPORARY_DIR}/${job}__${window_start}__${window_end}.apply.log"
  run_cli import-dhis2 "$job" \
    --start-period "$window_start" \
    --end-period "$window_end" \
    --apply 2>&1 | tee "$apply_log"
  status="${PIPESTATUS[0]}"
  if ((status != 0)); then
    if grep -Fq "This import already succeeded with the same dataset" "$apply_log"; then
      log "DHIS2 import already confirmed by the local journal | job=$job"
    else
      LAST_FAILURE_STAGE=apply
      LAST_FAILURE_STATUS=$status
      return 1
    fi
  fi

  write_checkpoint "$job" "$checkpoint" "$window_start" "$window_end"
  log "Climate window completed | job=$job | start=$window_start | end=$window_end"
}

validate_boolean DHIS2_CLIMATE_CONTINUE_ON_FAILURE "$CONTINUE_ON_FAILURE"
validate_boolean DHIS2_CLIMATE_REFRESH_CACHES "$REFRESH_CACHES"
validate_boolean DHIS2_CLIMATE_SKIP_MISSING_GEOMETRY "$SKIP_MISSING_GEOMETRY"

read -r -a JOBS <<< "${DHIS2_CLIMATE_JOBS:-}"
((${#JOBS[@]} > 0)) || fail_configuration "DHIS2_CLIMATE_JOBS does not contain any jobs."
for job in "${JOBS[@]}"; do
  [[ "$job" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || fail_configuration \
    "unsafe job identifier: ${job}"
done

resolve_period_window
build_period_windows
mkdir -p "$CHECKPOINT_DIR"
TEMPORARY_DIR="$(mktemp -d "${STATE_DIR}/run.XXXXXX")" || fail_configuration \
  "could not create a temporary directory in ${STATE_DIR}."
trap 'rm -rf -- "$TEMPORARY_DIR"' EXIT

FAILED_JOBS=()
FAILED_STAGES=()
HAS_PERMANENT_FAILURE=false
HAS_TRANSIENT_FAILURE=false
LAST_FAILURE_STAGE=
LAST_FAILURE_STATUS=0

log "Climate pipeline started | mode=$WINDOW_MODE | start=$START_PERIOD | end=$END_PERIOD | periodBatchSize=$PERIOD_BATCH_SIZE | windows=${#WINDOW_STARTS[@]} | jobs=${JOBS[*]}"

run_cli check-dhis2
status=$?
if ((status != 0)); then
  log "DHIS2 pre-flight check failed | exitStatus=$status"
  exit "$status"
fi

if [[ "$REFRESH_CACHES" == "true" ]]; then
  org_unit_args=(cache-org-units --level 3 --refresh)
  if [[ "$SKIP_MISSING_GEOMETRY" == "true" ]]; then
    org_unit_args+=(--skip-missing-geometry)
  fi
  log "Refreshing the organisation-unit cache"
  run_cli "${org_unit_args[@]}"
  status=$?
  if ((status != 0)); then
    log "Organisation-unit cache refresh failed | exitStatus=$status"
    exit "$status"
  fi

  log "Refreshing the climate Data Element cache"
  run_cli cache-data-elements --refresh
  status=$?
  if ((status != 0)); then
    log "Data Element cache refresh failed | exitStatus=$status"
    exit "$status"
  fi
fi

STOP_PIPELINE=false
for index in "${!JOBS[@]}"; do
  job="${JOBS[$index]}"
  position=$((index + 1))
  log "Climate job requested | job=$job | position=$position/${#JOBS[@]} | windows=${#WINDOW_STARTS[@]}"

  for window_index in "${!WINDOW_STARTS[@]}"; do
    window_start="${WINDOW_STARTS[$window_index]}"
    window_end="${WINDOW_ENDS[$window_index]}"
    window_position=$((window_index + 1))
    log "Climate window requested | job=$job | window=$window_position/${#WINDOW_STARTS[@]} | start=$window_start | end=$window_end"

    if run_job "$job" "$window_start" "$window_end"; then
      continue
    fi

    record_failure \
      "$job" "$LAST_FAILURE_STAGE" "$LAST_FAILURE_STATUS" "$window_start" "$window_end"
    if [[ "$CONTINUE_ON_FAILURE" == "false" ]]; then
      STOP_PIPELINE=true
    fi
    # A failed period leaves a gap. Stop this job here and let a systemd retry
    # resume it from the first missing checkpoint before processing later windows.
    break
  done

  if [[ "$STOP_PIPELINE" == "true" ]]; then
    break
  fi
done

if ((${#FAILED_JOBS[@]} > 0)); then
  log "Climate pipeline completed with errors | failedJobs=${FAILED_JOBS[*]} | failures=${FAILED_STAGES[*]}"
  if [[ "$HAS_PERMANENT_FAILURE" == "true" ]]; then
    exit 2
  fi
  exit 75
fi

log "Climate pipeline completed successfully | start=$START_PERIOD | end=$END_PERIOD | jobs=${JOBS[*]}"
