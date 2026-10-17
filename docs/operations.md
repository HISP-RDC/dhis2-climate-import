# Operations Runbook

## 1. Prepare Local Configuration

Create local files that are never committed:

```sh
cp .env.example .env
cp climate-service.yaml.example climate-service.yaml
```

Fill `.env` with `DHIS2_BASE_URL`, `DHIS2_PAT`, and `CLIMATE_SERVICE_BASE_URL`.
Use a personal access token or a dedicated technical account.

The default DHIS2 transport is the official `dhis2-python-client` package:

```sh
DHIS2_TRANSPORT=dhis2-client
DHIS2_CLIENT_LOG_LEVEL=CRITICAL
```

Use `DHIS2_TRANSPORT=stdlib` only as an emergency fallback while diagnosing dependency issues.

If the target instance rejects PAT requests, set:

```sh
DHIS2_AUTH_METHOD=basic
DHIS2_USERNAME=...
DHIS2_PASSWORD=...
```

On DHIS2 versions before 2.38, PAT authentication can be disabled by server configuration. In
that case, either enable PAT authentication on the server or use Basic Auth for the technical
account on the test instance.

## 2. Start Open Climate Service Locally

Open Climate Service is a separate service from DHIS2. Our pipeline expects it at:

```sh
CLIMATE_SERVICE_BASE_URL=http://127.0.0.1:9000
```

Fast local setup from the official OCS repository:

```sh
git clone https://github.com/dhis2/open-climate-service.git
cd open-climate-service
make sync
cp climate-service.yaml.example climate-service.yaml
cp .env.example .env
make run
```

For the DRC, edit `climate-service.yaml` before `make run` and use a DRC extent, for example:

```yaml
id: cod-climate-service
name: Democratic Republic of the Congo Climate Service

extent:
  name: Democratic Republic of the Congo
  bbox: [12.0, -13.5, 31.5, 5.4]
  country_code: COD

data_dir: ./data
```

When the service is running, verify:

```sh
curl http://127.0.0.1:9000/extent
```

The web interface is available at:

- `http://127.0.0.1:9000/` landing page
- `http://127.0.0.1:9000/manage` ingestion and sync
- `http://127.0.0.1:9000/map` map viewer
- `http://127.0.0.1:9000/docs` API docs

Start with CHIRPS3 daily precipitation if you only want to test OCS quickly. ERA5-Land can require
additional data-source credentials depending on the configured source.

## 3. Check DHIS2

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import check-dhis2
```

This checks `/api/system/info` and `/api/me`, then validates that the instance version is at
least 2.37.

## 4. Cache Organisation Units Level 3

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 3
```

The command fetches only DHIS2 organisation units at level 3 with `id`, `code`, `name`, `path`,
`level`, and `geometry`. It writes:

- `cache/org-units-level-3.geojson`
- `cache/org-units-level-3.manifest.json`

Imports do not refresh this cache implicitly. To refresh deliberately:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 3 --refresh
```

For the Climate App weekly Province/DPS example, cache the Province/DPS level as well. In the DRC
test hierarchy this is expected to be level 2:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 2 --refresh
```

## 5. Cache Data Elements

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-data-elements --data-set CCH_CLIMATE_WEATHER
```

Use the climate data set UID or code from the target instance. If `dhis2.climate_data_set` is
set in `config/import-pipeline.yaml`, `--data-set` can be omitted.
For weekly Climate App imports, use the weekly data set code or UID, for example `CCH_WEEKLY`,
when that is the configured data set in the target instance.

The command resolves the DHIS2 data set first, then caches only data elements linked through
`dataSetElements`. The pipeline resolves configured climate mappings against
`cache/data-elements.json`.
If a code or UID is missing, the import stops with a clear metadata error.

The code first resolves the data set, then reads its `dataSetElements`. This is more reliable
than scanning every data element in the instance.

If nginx returns `403 Forbidden` instead of a DHIS2 JSON error, verify:

- `DHIS2_BASE_URL` includes the correct DHIS2 context path, for example `/dhis` if the instance
  is not mounted at the domain root.
- The reverse proxy allows scripted API calls.
- PAT authentication is enabled on the DHIS2 server, or `.env` uses `DHIS2_AUTH_METHOD=basic`.
- The technical account can read metadata for data sets and data elements.

Run the local diagnostic before changing import logic:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import diagnose-dhis2 --try-common-contexts
```

If one candidate returns DHIS2 JSON with status `200`, set `DHIS2_BASE_URL` to that candidate.
If all candidates return nginx HTML `403`, the block is at the reverse proxy, VPN/IP allow-list,
or API route policy before DHIS2 can evaluate the user authorities.

## 6. Review Before Import

Use `review-import` to reproduce the Climate App "Review and import" summary before hitting OCS
or DHIS2 imports:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import temperature_max_weekly_era5_land --org-unit-level 2 --data-set CCH_WEEKLY
```

For rolling historical windows, use `--years-back`:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import temperature_max_weekly_era5_land --years-back 10 --org-unit-level 2 --data-set CCH_WEEKLY
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 temperature_max_weekly_era5_land --years-back 7 --org-unit-level 2 --data-set CCH_WEEKLY --dry-run
```

`--years-back` computes whole DHIS2 periods. For a weekly job, the default end is the latest
completed ISO week; for monthly, the previous completed month; for yearly, the previous completed
year. Add `--as-of YYYY-MM-DD` to make a scheduled run reproducible, or pass `--end-period` to
anchor the window on a specific DHIS2 period.

For the weekly max-temperature DRC example, the expected control is:

- `2026-W09` to `2026-W31`, resolved to `2026-02-23` through `2026-08-02`
- `26` Province/DPS org units with geometry
- `598` data values, computed as `26 x 23 weekly periods`

To enforce that exact control, run:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import temperature_max_weekly_era5_land --org-unit-level 2 --data-set CCH_WEEKLY --expected-org-units 26 --expected-data-values 598
```

The configured weekly job currently uses `data_set: CCH_CLIMATE_WEATHER`, matching the local
configuration. If the real weekly data set in the instance has another UID/code, pass `--data-set`
or update that single field before refreshing `cache/data-elements.json`. The pipeline validates
the cached data set `periodType`; a weekly job requires a DHIS2 `Weekly` data set.

For the screenshot-style Zone de Sante weekly precipitation review, use level 3:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 3 --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-data-elements --data-set CCH_CLIMATE_WEATHER --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import precipitation_weekly_era5_land --start 2026-03-21 --end 2026-08-21 --org-unit-level 3 --data-set CCH_CLIMATE_WEATHER --expected-org-units 519 --expected-data-values 11937
```

The user-entered weekly date range is expanded to full DHIS2 calendar weeks, so
`2026-03-21` through `2026-08-21` becomes `2026-W12` through `2026-W34`
(`2026-03-16` through `2026-08-23`).

## 7. Validate an Import

Start with dry-run:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 precipitation_daily --dry-run
```

Then run a small real import in non-production:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 precipitation_daily --apply
```

Reports are written to `reports/`. Run history is stored in `cache/import-runs.sqlite3`.

## 8. Verify Analytics

After a real import and analytics table generation:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import verify-analytics precipitation_daily --period 20240101
```

If `trigger_analytics` is true in `config/import-pipeline.yaml`, the pipeline attempts to call
the analytics maintenance endpoint after successful imports. Otherwise, keep this as an explicit
operations step handled by the DHIS2 administrator.

## 9. Transfer a Data Set Between DHIS2 Instances

Copy the examples and configure `.env` with the referenced variables:

```sh
cp config/instances.yaml.example config/instances.yaml
cp config/scenarios/dhis2-x-to-y.yaml.example config/scenarios/dhis2-x-to-y.yaml
```

The source account needs metadata/data read access. The target account needs data import access and,
when `create_if_missing` is enabled, permission to import the Data Set metadata package.

The review also verifies that every source organisation unit involved in the transfer exists on the
target using `organisation_units.mapping_strategy` (`uid` or `code`). A missing or ambiguous match
blocks the transfer before metadata or data mutation. Extra organisation units on the target do not
block it.

Always review first:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  review weekly_climate
```

If the review reports `missing on target`, use the dedicated hierarchy reconciliation. It requires
`mapping_strategy: uid` and `organisation_units.synchronization.enabled: true`:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units relevant_epidemiological_data --dry-run
```

Inspect `planArtifact` and `packageArtifact`. The package contains only missing units, ordered from
parent to child, plus an additive target Data Set assignment when the Data Set already exists. It
never removes existing assignments. Apply the exact validated plan, then rerun `review`:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units relevant_epidemiological_data --apply

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  review relevant_epidemiological_data
```

The operation blocks on UID/code collisions, identity differences, or a different existing parent.
Root creation is disabled by default because a DHIS2 instance must keep a single organisation-unit
root. If the target is completely empty, root creation therefore requires an explicit, separately
reviewed `allow_root_creation: true` decision.

After the first plan is approved, set `run_before_transfer: true` to perform this reconciliation
automatically before each parent transfer. `require_same_name` and `require_same_code` control the
two identity fields independently; UID collisions and parent differences always remain blocking.
For a target that intentionally prefixes OU codes, keep `require_same_name: true` and set
`require_same_code: false`. The value import also performs a final DataSet assignment audit and
stops before DHIS2 can return `E8022`.

Validate without mutation:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  run weekly_climate --dry-run
```

If this reports `metadata_changes_required`, inspect and apply the dedicated metadata plan before
exporting the large value range:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --dry-run

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --apply
```

If the target Data Set is missing, this validates the metadata package but reports that data-value
validation is pending the metadata commit. Apply only after reviewing the generated report:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  run weekly_climate --apply
```

The apply workflow creates missing metadata atomically, refreshes the target cache, dry-runs every
data-value chunk, and writes the values only if no conflicts were reported. Generated state is kept
in `state/<target-profile>/<scenario>/jobs/<job>/` and is ignored by Git.

For large Data Sets, source export is two-dimensional: organisation-unit batches and DHIS2-period
batches. A monthly job defaults to one month per request. You can make the setting explicit per job:

```yaml
jobs:
  - id: hospital_service
    # ...
    transfer:
      source_period_batch_size: 1
      execution:
        period_batch_size: 3
        process_isolation: true
        resume: true
        stop_on_error: true
```

`source_period_batch_size` limits one HTTP request. `execution.period_batch_size` limits the whole
in-memory export/validation/import unit. The parent starts a new process for each execution window,
records its result in `state/<target>/<scenario>/jobs/<job>/journal.sqlite3`, then releases all of
that child's memory. On restart, successful windows are skipped automatically.

Transient failures (timeouts, resets, 429, 502/503/504 and common Cloudflare gateway errors) use
the configured retry policy and exit as `75` after exhaustion. Configuration, permissions and
metadata incompatibilities exit as `2` without blind retries.

Every source request is logged to stderr with its date window, organisation-unit count, duration,
and cumulative value count. If DHIS2, nginx, or Cloudflare returns a timeout/gateway error, the
pipeline automatically halves the period range and then the organisation-unit batch. A failure at
the minimum size (one DHIS2 period and one organisation unit) is reported with a concise diagnostic;
in that case inspect source performance or increase `DHIS2_TIMEOUT_SECONDS` in `.env`.

With metadata strategy `CREATE`, dependencies reported by DHIS2 as already existing (`E5000`) are
removed from the commit package and reused by UID. They are never updated implicitly. The full source
package is retained as `*.metadata.source.json`; the validated minimal package is retained as
`*.metadata.json`. Any discarded stale category references are summarized in the run report.

### systemd execution per Data Set

Install the templates after adapting `User`, `Group` and paths when the checkout is not under
`/opt/dhis2-climate-import`:

```sh
sudo cp deploy/systemd/dhis2-transfer@.service /etc/systemd/system/
sudo cp deploy/systemd/dhis2-transfer@.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dhis2-transfer@hospital_service.timer
sudo systemctl enable --now dhis2-transfer@sigl_fosa.timer
systemctl list-timers 'dhis2-transfer@*'
```

Audit commands:

```sh
sudo systemctl status dhis2-transfer@hospital_service.service
sudo journalctl -u dhis2-transfer@hospital_service.service --since today
sudo journalctl -fu dhis2-transfer@hospital_service.service
```

The template intentionally omits `--force`. The YAML setting `execution.max_parallel_jobs` limits
heavy jobs globally, while a per-job lock and one target-metadata lock prevent conflicting runs.

### Repair partial target metadata

Do not bypass a CategoryCombo compatibility error with a global `CREATE_AND_UPDATE`. When the audit
shows that the target has the same Data Element UIDs and labels but incomplete CategoryCombos, opt
in explicitly in the scenario:

```yaml
target:
  metadata:
    reconcile_existing: true
```

Validate the repair plan without mutation:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  repair-metadata hospital_service --dry-run
```

The command blocks unless all affected Data Elements have matching identity fields, have no target
Data Set memberships, and have no raw or deleted values in the 1900–2100 audit window under the
target account's organisation-unit roots. It also requires Categories and CategoryOptions to match
by UID, label, and membership. Review the generated report and rollback-evidence artifact before
applying:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  repair-metadata hospital_service --apply
```

DHIS2 validates CategoryOptionCombos against the committed CategoryCombo structure, so apply runs
two atomic stages. If the second-stage validation fails, no Data Element assignment is changed; the
first-stage CategoryCombo repair remains recorded in the report. After a successful repair, rerun
the ordinary transfer with `--dry-run`, then review its report before using `--apply`.

### Historical UID collisions

An old source CategoryOptionCombo can reference a CategoryCombo UID that already exists on Y with
a different structure. Do not overwrite that target object. Enable isolated cloning instead:

```yaml
target:
  metadata:
    synchronization:
      historical_data_elements:
        enabled: true
        strategy: versioned
        uid_collision_strategy: clone
        name_suffix: " — Historique"
```

The pipeline clones the complete historical category graph under deterministic UIDs, validates it
with `importMode=VALIDATE`, and commits it only during `--apply`. It then creates or reuses the
versioned historical Data Element and rewrites both `dataElement` and `categoryOptionCombo` in the
affected values. The incompatible object that was already on Y remains untouched. The source and
target UID mappings, detected conflicts, validation response and commit response are retained in
the window's `historical-metadata` artifacts.
