# Operations runbook

## Pre-flight checks

```bash
cd /opt/dhis2-climate-import

test -r .env
test -x .venv/bin/dhis2-climate-import
.venv/bin/dhis2-climate-import list-jobs --ids-only
.venv/bin/dhis2-climate-import check-dhis2
```

For a transfer job:

```bash
.venv/bin/dhis2-climate-import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  review hospital_service
```

## systemd operations

### Individual transfer

```bash
sudo systemctl start --no-block dhis2-transfer@hospital_service.service
sudo systemctl status dhis2-transfer@hospital_service.service
sudo journalctl -fu dhis2-transfer@hospital_service.service
```

Compact state:

```bash
sudo systemctl show dhis2-transfer@hospital_service.service \
  --property=ActiveState,SubState,Result,ExecMainStatus,MemoryPeak
```

### Climate pipeline

```bash
sudo systemctl start --no-block dhis2-climate-pipeline.service
sudo systemctl status dhis2-climate-pipeline.service
sudo journalctl -fu dhis2-climate-pipeline.service
```

A `Type=oneshot` service remains in the `activating` state for its entire run. This is expected.

### After correcting a permanent error

```bash
sudo systemctl reset-failed dhis2-transfer@hospital_service.service
sudo systemctl start --no-block dhis2-transfer@hospital_service.service
```

## Reading transfer logs

Important messages:

| Message | Meaning |
| --- | --- |
| `Target chunk validated` | the DHIS2 dry-run accepted the chunk |
| `Target chunk checkpoint recorded` | an import response was confirmed and persisted |
| `Target chunk imported` | the real import completed without a blocking conflict |
| `Target chunk already imported; resumed from checkpoint` | no new POST was sent for that chunk |
| `Target chunk split after timeout` | only the failing chunk was subdivided |
| `Values quarantined` | excluded values were recorded in an audit artifact |
| `Window completed` | the complete period window was checkpointed |

A `Target chunk import started` message without a checkpoint or completion message does not prove
that the client received a response.

## Reports and checkpoints

```bash
JOB_ROOT=/opt/dhis2-climate-import/state/y/dhis2-x-to-y/jobs/hospital_service

find "$JOB_ROOT/reports" -type f -name '*.json' -print | sort
find "$JOB_ROOT/runs" -type f -name 'run-summary.json' -print | sort
find "$JOB_ROOT/checkpoints" -type f -name '*.json' -print | sort
```

Inspect the latest parent summary as the service account:

```bash
SUMMARY=$(find "$JOB_ROOT/runs" -type f -name 'run-summary.json' -print | sort | tail -1)
sudo -u dhis2-import jq . "$SUMMARY"
```

Do not delete a checkpoint to force recovery. Use `--force` only after understanding the impact
and only during a controlled manual run.

## OCS jobs

The pipeline logs the UUID of every ingestion. Inspect a known job:

```bash
curl -sS http://127.0.0.1:9000/ingestions/jobs/<job-id> |
  jq '{jobID, status, request, progress, error}'
```

List published datasets:

```bash
curl -sS http://127.0.0.1:9000/datasets |
  jq -r '.items[]? | [.dataset_id, .extent.temporal.start, .extent.temporal.end] | @tsv'
```

`409 An ingest or sync is already running` means another OCS job for the same dataset is active.
Do not start a concurrent ingestion. Find its UUID in the OCS journal or persistent job registry.

## Common incidents

### DHIS2 disconnect or timeout

Examples: `connection reset`, `server disconnected`, `502`, `504`, or `524`.

The pipeline classifies these as temporary. Transfers retry and then subdivide only the failing
chunk. If the minimum chunk still fails, exit code `75` enables a bounded systemd restart.

Keep the proxy enabled. If small chunks still fail, inspect the reverse proxy, Cloudflare,
PostgreSQL locks, and DHIS2 resource usage.

### Metadata conflict 409

Read the `E....` codes and the JSON plan shown in the journal. Do not bypass a structural conflict
by deleting target objects. Run:

```bash
.venv/bin/dhis2-climate-import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata <job> --dry-run
```

### Organisation unit mismatch

```bash
.venv/bin/dhis2-climate-import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units <job> --dry-run
```

A different parent for an existing UID is never repaired automatically. The `exclude` policy can
quarantine that branch; `block` requires a human decision.

### Missing climate service variables

The service reads job and window settings from:

```bash
sudo grep '^DHIS2_CLIMATE_' /etc/default/dhis2-climate-pipeline
```

Do not place these settings only in `.env`.

### Invalid period

A value such as `NaNWNaN` comes from a malformed payload. Correlate the request in the DHIS2 log
and reverse-proxy access log. Disabling the proxy does not correct malformed data.

## Concurrency

- the climate pipeline processes jobs sequentially to protect writable OCS storage;
- multiple `dhis2-transfer@...` services may be requested simultaneously;
- `execution.max_parallel_jobs` limits active value transfers;
- a separate lock serializes metadata changes;
- do not enable individual timers and the sequential queue for the same jobs.

## Operational backups

Back up:

- `/opt/dhis2-climate-import/.env` in a secure secret store;
- `/opt/dhis2-climate-import/state/`;
- `/opt/dhis2-climate-import/reports/`;
- the OCS configuration and data directory;
- `/etc/default/dhis2-climate-pipeline` and `/etc/default/dhis2-transfer-queue`.

Never include credentials in a public archive or an unprotected incident report.
