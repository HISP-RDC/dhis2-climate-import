# Configuration

## Principles

1. Keep secrets in `.env` with mode `0600`.
2. Keep runtime YAML files local and commit only sanitized `.example` templates.
3. Validate every mapping or strategy change with a dry-run.
4. Never put tokens, passwords, or credential-bearing URLs in versioned examples.

## Initialize local YAML configuration

Runtime configuration is intentionally ignored by Git. Create it from the tracked templates:

```bash
cp config/import-pipeline.yaml.example config/import-pipeline.yaml
cp config/instances.yaml.example config/instances.yaml
cp config/scenarios/dhis2-x-to-y.yaml.example \
  config/scenarios/dhis2-x-to-y.yaml
cp climate-service.yaml.example climate-service.yaml
```

Edit only the copied files for a deployment. When a new option should be documented publicly,
update the corresponding `.example` file with safe placeholder values.

## `.env`

Create the local file from the template:

```bash
cp .env.example .env
chmod 600 .env
```

### Climate import target

```env
DHIS2_BASE_URL=https://dhis2.example.org
DHIS2_AUTH_METHOD=auto
DHIS2_PAT=
DHIS2_USERNAME=
DHIS2_PASSWORD=
```

`auto` prefers a PAT when one is present and falls back to Basic authentication. Use `pat` or
`basic` explicitly when the behavior must be fixed.

### Transfer profiles X and Y

```env
DHIS2_X_BASE_URL=https://source.example.org
DHIS2_X_USERNAME=
DHIS2_X_PASSWORD=
DHIS2_X_PAT=

DHIS2_Y_BASE_URL=https://target.example.org
DHIS2_Y_USERNAME=
DHIS2_Y_PASSWORD=
DHIS2_Y_PAT=
```

`config/instances.yaml` selects which variables each profile uses.

### Open Climate Service

```env
CLIMATE_SERVICE_BASE_URL=http://127.0.0.1:9000
CLIMATE_SERVICE_HTTP_TIMEOUT_SECONDS=60
CLIMATE_SERVICE_POLL_SECONDS=5
CLIMATE_SERVICE_JOB_TIMEOUT_SECONDS=86400
```

The HTTP timeout covers submission and polling requests, not the total provider download time.

## `config/import-pipeline.yaml`

The local file, created from `config/import-pipeline.yaml.example`, declares:

- the organisation-unit level;
- the target climate Data Set;
- DHIS2 chunk sizes;
- OCS jobs and their target Data Elements;
- expected units and period types.

Inspect the loaded configuration:

```bash
.venv/bin/dhis2-climate-import list-jobs
```

Validate every Data Element mapping against the target instance before applying an import.

## `config/instances.yaml`

PAT profile:

```yaml
instances:
  y:
    type: dhis2
    base_url_env: DHIS2_Y_BASE_URL
    auth:
      method: pat
      token_env: DHIS2_Y_PAT
```

Basic authentication profile:

```yaml
instances:
  x:
    type: dhis2
    base_url_env: DHIS2_X_BASE_URL
    auth:
      method: basic
      username_env: DHIS2_X_USERNAME
      password_env: DHIS2_X_PASSWORD
```

## `config/scenarios/dhis2-x-to-y.yaml`

All jobs inherit the global policy:

- metadata identity and creation strategy;
- UID or code mapping;
- adaptive chunks;
- period-window and chunk recovery;
- concurrent-job limit;
- organisation-unit conflict policy.

Values under `jobs[].transfer` override only the required settings for that job.

An identity or parent mismatch on an existing organisation unit remains blocking with
`conflict_policy: block`. With `exclude`, only conflicting source branches are quarantined and
audited.

## `/etc/default/dhis2-climate-pipeline`

systemd reads this file independently from `.env`:

```env
DHIS2_CLIMATE_JOBS="precipitation_weekly_chirps precipitation_weekly_era5_land"
DHIS2_CLIMATE_WINDOW_MODE=rolling
DHIS2_CLIMATE_LOOKBACK_WEEKS=8
DHIS2_CLIMATE_LAG_WEEKS=3
DHIS2_CLIMATE_PERIOD_BATCH_SIZE=13
DHIS2_CLIMATE_CONTINUE_ON_FAILURE=true
```

Controlled historical backfill:

```env
DHIS2_CLIMATE_WINDOW_MODE=fixed
DHIS2_CLIMATE_START_PERIOD=2017-W01
DHIS2_CLIMATE_END_PERIOD=2026-W39
```

After changing the file:

```bash
sudo systemctl daemon-reload
sudo systemctl start --no-block dhis2-climate-pipeline.service
```

If the service is already active, it reads the new configuration on its next run. Do not restart
it in the middle of an ingestion without an explicit decision. Stop it first, inspect any active
OCS job, and then use `start --no-block`.

## Validation

```bash
.venv/bin/dhis2-climate-import list-jobs --ids-only
.venv/bin/dhis2-climate-import check-dhis2
.venv/bin/dhis2-climate-import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  review hospital_service
```
