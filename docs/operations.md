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

## 2. Check DHIS2

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import check-dhis2
```

This checks `/api/system/info` and `/api/me`, then validates that the instance version is at
least 2.37.

## 3. Cache Organisation Units Level 3

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

## 4. Cache Data Elements

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

## 5. Review Before Import

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

## 6. Validate an Import

Start with dry-run:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 precipitation_daily --dry-run
```

Then run a small real import in non-production:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 precipitation_daily --apply
```

Reports are written to `reports/`. Run history is stored in `cache/import-runs.sqlite3`.

## 7. Verify Analytics

After a real import and analytics table generation:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import verify-analytics precipitation_daily --period 20240101
```

If `trigger_analytics` is true in `config/import-pipeline.yaml`, the pipeline attempts to call
the analytics maintenance endpoint after successful imports. Otherwise, keep this as an explicit
operations step handled by the DHIS2 administrator.
