# DHIS2 Climate Import

Reliable Python orchestration for two complementary workflows:

- importing climate data prepared by Open Climate Service (OCS) into DHIS2;
- transferring an aggregate Data Set and its dependencies between two DHIS2 instances.

The project prioritizes validation before writes, bounded processing windows, restart safety,
targeted quarantine, and auditable operations.

## Main features

- DHIS2 authentication with a personal access token or username/password;
- automatic discovery of Data Elements linked to a Data Set;
- controlled creation of missing target metadata;
- parent-first synchronization of missing organisation units;
- dry-run validation before sensitive writes;
- bounded period windows and adaptive chunks for large volumes;
- checkpoints for confirmed chunks and restart-safe execution;
- documented quarantine of incompatible values;
- SQLite journals, JSON reports, and systemd-friendly progress logs;
- weekly OCS-to-DHIS2 pipelines and multi-job DHIS2 transfers.

## Functional architecture

```text
Climate data providers
        │
        ▼
Open Climate Service ── geospatial aggregation ──┐
                                                ▼
DHIS2 instance X ── Data Set transfer ───────► dhis2-climate-import ──► DHIS2 Y
                                                │
                                                └── state, checkpoints, reports
```

OCS does not push values directly to DHIS2. This project orchestrates OCS ingestion, aggregation,
DHIS2 validation, and import.

## Requirements

- Linux or macOS for development;
- Python 3.12 or later;
- [`uv`](https://docs.astral.sh/uv/);
- API access to one or two DHIS2 instances;
- OCS only for climate imports.

## Quick installation

```bash
git clone https://github.com/HISP-RDC/dhis2-climate-import.git
cd dhis2-climate-import

uv python install 3.12
uv sync --locked --python 3.12

cp .env.example .env
cp config/import-pipeline.yaml.example config/import-pipeline.yaml
cp config/instances.yaml.example config/instances.yaml
cp config/scenarios/dhis2-x-to-y.yaml.example \
  config/scenarios/dhis2-x-to-y.yaml
cp climate-service.yaml.example climate-service.yaml
chmod 600 .env
```

Set instance URLs and credentials in `.env`, then verify the installation:

```bash
.venv/bin/dhis2-climate-import --help
make check
```

The project is installed into `.venv` and exposes the `dhis2-climate-import` command. The legacy
`PYTHONPATH=src python -m dhis2_climate_import ...` form remains compatible but is no longer needed
after `uv sync`.

See [Installation](docs/installation.md) for Ubuntu, updates, service accounts, and systemd.

## Configuration

Secrets belong only in `.env`. Runtime YAML files are local and ignored by Git. Commit only their
`.example` templates, which must never contain passwords, tokens, private URLs, or deployment UID
mappings.

| File | Purpose |
| --- | --- |
| `.env` | URLs, credentials, transport settings, and the OCS endpoint |
| `config/import-pipeline.yaml.example` | Shareable OCS-to-DHIS2 configuration template |
| `config/instances.yaml.example` | Shareable source and target profile template |
| `config/scenarios/dhis2-x-to-y.yaml.example` | Shareable transfer scenario template |
| `config/*.yaml`, `config/scenarios/*.yaml` | Ignored local runtime configuration |
| `/etc/default/dhis2-climate-pipeline` | Climate service window and job list |
| `/etc/default/dhis2-transfer-queue` | Sequential transfer job order |

See [Configuration](docs/configuration.md) for the complete reference.

## List climate jobs

```bash
.venv/bin/dhis2-climate-import list-jobs
.venv/bin/dhis2-climate-import list-jobs --ids-only
```

## Import climate data from OCS to DHIS2

Start OCS and test a small period before any historical backfill:

```bash
CLI=.venv/bin/dhis2-climate-import

$CLI check-dhis2
$CLI cache-org-units --level 3 --refresh --skip-missing-geometry
$CLI cache-data-elements --refresh

$CLI ingest-dataset precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39 --publish

$CLI review-import precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39

$CLI import-dhis2 precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39 --dry-run

$CLI import-dhis2 precipitation_weekly_chirps \
  --start-period 2026-W39 --end-period 2026-W39 --apply
```

See [Weekly climate pipeline](docs/weekly-climate-ocs.md) for multi-year backfills and scheduled
operations.

## Transfer DHIS2 X to DHIS2 Y

Always start with review and dry-run commands:

```bash
CLI=.venv/bin/dhis2-climate-import
SCENARIO=config/scenarios/dhis2-x-to-y.yaml

$CLI transfer --instances config/instances.yaml --scenario "$SCENARIO" \
  review hospital_service

$CLI transfer --instances config/instances.yaml --scenario "$SCENARIO" \
  reconcile-org-units hospital_service --dry-run

$CLI transfer --instances config/instances.yaml --scenario "$SCENARIO" \
  reconcile-metadata hospital_service --dry-run

$CLI transfer --instances config/instances.yaml --scenario "$SCENARIO" \
  run hospital_service --dry-run
```

After reviewing the generated plans and reports, apply each required change explicitly:

```bash
$CLI transfer --instances config/instances.yaml --scenario "$SCENARIO" \
  reconcile-org-units hospital_service --apply

$CLI transfer --instances config/instances.yaml --scenario "$SCENARIO" \
  reconcile-metadata hospital_service --apply

$CLI transfer --instances config/instances.yaml --scenario "$SCENARIO" \
  run hospital_service --apply
```

Long periods are split into isolated windows. Confirmed chunks are fingerprinted and skipped on
restart. Metadata writes remain serialized even when multiple value transfers run concurrently.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | success |
| `2` | permanent configuration, data, permission, or metadata error |
| `75` | temporary network or service error that may be retried |

## State and audit files

Generated files are not versioned:

```text
cache/      organisation unit and Data Element caches
reports/    climate import reports
state/      journals, checkpoints, plans, quarantines, and transfer reports
data/       optional local climate data
```

Do not delete `state/` to resolve an incident. It proves which chunks were confirmed and enables
idempotent restarts.

## Quality checks

```bash
make check
```

This compiles the Python modules, validates shell scripts, and runs the full test suite. See
[CONTRIBUTING.md](CONTRIBUTING.md) for contribution rules.

## Documentation

- [Documentation index](docs/README.md)
- [Installation](docs/installation.md)
- [Configuration](docs/configuration.md)
- [Command reference](docs/commands.md)
- [Architecture](docs/architecture.md)
- [Operations runbook](docs/operations.md)
- [Weekly climate pipeline](docs/weekly-climate-ocs.md)
- [DHIS2 transfer guarantees](docs/features.md)
- [Official sources](docs/official-sources.md)

## Compatibility

- Python: 3.12+
- DHIS2: declared minimum 2.37; validate API behavior against the exact target version before
  production use.
- OCS: deployed as a separate service; provider plugins and credentials are not bundled here.

## License

No reuse license is currently declared. Before presenting the repository as open-source software
or accepting external redistribution, the maintainers must select an appropriate license and add
a `LICENSE` file.
