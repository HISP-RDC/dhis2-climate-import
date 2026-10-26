# Command reference

After installation:

```bash
CLI=.venv/bin/dhis2-climate-import
$CLI --help
```

Global options must appear before the subcommand:

```bash
$CLI --config config/import-pipeline.yaml --env-file .env check-dhis2
```

## Climate import commands

| Command | Purpose |
| --- | --- |
| `list-jobs` | list climate jobs declared in the pipeline configuration |
| `check-dhis2` | verify DHIS2 access and the minimum version |
| `diagnose-dhis2` | diagnose base URL, context path, authentication, and proxy behavior |
| `cache-org-units` | refresh organisation-unit geometries |
| `cache-data-elements` | refresh Data Elements for the configured climate Data Sets |
| `ingest-dataset` | submit an asynchronous OCS ingestion and poll its state |
| `sync-dataset` | plan or apply an OCS synchronization |
| `aggregate` | generate a DHIS2 payload without importing it |
| `review-import` | show the period, organisation units, and expected value count |
| `import-dhis2 --dry-run` | validate values in DHIS2 without writing |
| `import-dhis2 --apply` | import values after explicit validation |
| `verify-analytics` | verify a sample through the Analytics API |
| `run-monthly` | run all configured monthly imports |

Weekly example:

```bash
$CLI ingest-dataset precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39 --publish

$CLI review-import precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39

$CLI import-dhis2 precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39 --dry-run

$CLI import-dhis2 precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39 --apply
```

## Time windows

Climate commands accept, depending on the operation:

- `--start-period` and `--end-period`;
- `--start` and `--end` ISO dates;
- `--years-back`, optionally with `--as-of`.

Weekly periods use `YYYY-Www`, for example `2026-W39`.

## DHIS2-to-DHIS2 transfer commands

Common prefix:

```bash
TRANSFER="$CLI transfer --instances config/instances.yaml --scenario config/scenarios/dhis2-x-to-y.yaml"
```

| Subcommand | Purpose |
| --- | --- |
| `review JOB` | compare Data Sets, Data Elements, and organisation units |
| `reconcile-org-units JOB --dry-run` | generate the hierarchy synchronization plan |
| `reconcile-org-units JOB --apply` | create only approved missing organisation units |
| `reconcile-metadata JOB --dry-run` | validate the target metadata package |
| `reconcile-metadata JOB --apply` | create or attach compatible metadata |
| `repair-metadata JOB` | handle a narrowly defined historical repair after audit |
| `run JOB --dry-run` | export and validate all values without applying them |
| `run JOB --apply` | perform the transfer |
| `run-window` | internal worker; do not invoke manually |

Examples:

```bash
$TRANSFER review hospital_service
$TRANSFER reconcile-org-units hospital_service --dry-run
$TRANSFER reconcile-metadata hospital_service --dry-run
$TRANSFER run hospital_service --dry-run
$TRANSFER run hospital_service --apply
```

Limit a diagnostic to a smaller period:

```bash
$TRANSFER run hospital_service \
  --start-period 2025-01 --end-period 2025-03 --dry-run
```

## Exit codes

- `0`: success;
- `2`: permanent error that requires intervention;
- `75`: temporary error that systemd may retry.

## Development commands

```bash
make help
make install
make list-jobs
make test
make check
```
