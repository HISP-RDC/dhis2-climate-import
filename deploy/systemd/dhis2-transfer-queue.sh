#!/usr/bin/env bash

set -u -o pipefail

readonly SYSTEMCTL_BIN="${SYSTEMCTL_BIN:-/usr/bin/systemctl}"
readonly POLL_SECONDS="${DHIS2_TRANSFER_QUEUE_POLL_SECONDS:-15}"
readonly CONTINUE_ON_FAILURE="${DHIS2_TRANSFER_CONTINUE_ON_FAILURE:-true}"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*"
}

case "$POLL_SECONDS" in
  ''|*[!0-9]*)
    log "ERROR: DHIS2_TRANSFER_QUEUE_POLL_SECONDS must be a positive integer."
    exit 2
    ;;
  0)
    log "ERROR: DHIS2_TRANSFER_QUEUE_POLL_SECONDS must be greater than zero."
    exit 2
    ;;
esac

case "$CONTINUE_ON_FAILURE" in
  true|false) ;;
  *)
    log "ERROR: DHIS2_TRANSFER_CONTINUE_ON_FAILURE must be true or false."
    exit 2
    ;;
esac

read -r -a jobs <<< "${DHIS2_TRANSFER_JOBS:-}"
if ((${#jobs[@]} == 0)); then
  log "ERROR: DHIS2_TRANSFER_JOBS does not contain any jobs."
  exit 2
fi

failed_jobs=()

log "Transfer queue started | jobs=${jobs[*]}"

for index in "${!jobs[@]}"; do
  job="${jobs[$index]}"
  if [[ ! "$job" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
    log "ERROR: unsafe job identifier | job=$job"
    exit 2
  fi

  unit="dhis2-transfer@${job}.service"
  position=$((index + 1))

  log "Queue job requested | job=$job | position=$position/${#jobs[@]} | unit=$unit"

  # A previous scheduled execution may have exhausted the unit start limit.
  # Reset it once at the beginning of this explicit queue attempt; the unit's
  # own StartLimitBurst still bounds all retries performed during this run.
  "$SYSTEMCTL_BIN" reset-failed "$unit" >/dev/null 2>&1 || true

  if ! "$SYSTEMCTL_BIN" start --no-block "$unit"; then
    log "Service start failed | job=$job | unit=$unit"
    failed_jobs+=("$job")
    if [[ "$CONTINUE_ON_FAILURE" == "false" ]]; then
      break
    fi
    continue
  fi

  # A start request made while a long oneshot unit is already running joins
  # that execution. Wait through application retries and systemd auto-restarts
  # until the unit becomes stably inactive (success) or failed.
  while true; do
    active_state="$($SYSTEMCTL_BIN show "$unit" --property=ActiveState --value)"
    sub_state="$($SYSTEMCTL_BIN show "$unit" --property=SubState --value)"

    case "$active_state" in
      active|activating|reloading|deactivating)
        sleep "$POLL_SECONDS"
        ;;
      inactive|failed)
        break
        ;;
      *)
        log "Unexpected systemd state; checking again | job=$job | activeState=$active_state | subState=$sub_state"
        sleep "$POLL_SECONDS"
        ;;
    esac
  done

  result="$($SYSTEMCTL_BIN show "$unit" --property=Result --value)"
  exit_status="$($SYSTEMCTL_BIN show "$unit" --property=ExecMainStatus --value)"

  if [[ "$active_state" == "inactive" && "$result" == "success" ]]; then
    log "Queue job completed | job=$job | position=$position/${#jobs[@]} | result=$result"
    continue
  fi

  log "Queue job failed | job=$job | position=$position/${#jobs[@]} | activeState=$active_state | result=$result | exitStatus=$exit_status"
  failed_jobs+=("$job")
  if [[ "$CONTINUE_ON_FAILURE" == "false" ]]; then
    break
  fi
done

if ((${#failed_jobs[@]} > 0)); then
  log "Transfer queue completed with errors | failedJobs=${failed_jobs[*]}"
  exit 1
fi

log "Transfer queue completed successfully | jobs=${jobs[*]}"
