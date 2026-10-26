# Weekly OCS-to-DHIS2 climate import

The workflow has two distinct stages:

1. Open Climate Service downloads, transforms, and publishes weekly raster data.
2. `dhis2-climate-import` asks OCS to aggregate the raster over organisation units and sends the
   resulting values to DHIS2.

OCS does not send values directly to DHIS2.

## Sources and authentication

- CHIRPS3: no API key is required.
- MODIS NDVI/EVI: Microsoft Planetary Computer; no Google Earth Engine key is required.
- ERA5-Land: `CDSAPI_KEY` or `~/.cdsapirc`. `EDH_API_KEY` or `~/.netrc` is optional and may speed up
  historical mean temperature and precipitation retrieval.
- ERA5-HEAT UTCI: `CDSAPI_KEY` or `~/.cdsapirc` is required.

The OCS `climate-service.yaml` file must include:

```yaml
plugins_dir: ./plugins/
```

Restart OCS after adding or changing a plugin:

```bash
cd /path/to/open-climate-service
make run
```

Verify the dataset registry:

```bash
curl -sS http://127.0.0.1:9000/dataset-templates/chirps3_precipitation_weekly |
  jq '{id, period_type, units, ingestable}'
curl -sS http://127.0.0.1:9000/dataset-templates/modis_ndvi_weekly |
  jq '{id, period_type, units, ingestable}'
```

## Prepare DHIS2

From the `dhis2-climate-import` checkout:

```bash
CLI=.venv/bin/dhis2-climate-import

$CLI --env-file .env check-dhis2
$CLI --env-file .env cache-org-units --level 3 --refresh
$CLI --env-file .env cache-data-elements --data-set mck6wRvBafz --refresh
$CLI list-jobs
```

## OCS ingestion

Use a small period for the first test:

```bash
CLIMATE_START_PERIOD=2026-W39
CLIMATE_END_PERIOD=2026-W40

CLIMATE_JOBS=(
  precipitation_weekly_chirps
  precipitation_weekly_era5_land
  temperature_mean_weekly_era5_land
  temperature_max_weekly_era5_land
  temperature_min_weekly_era5_land
  dewpoint_temperature_weekly_era5_land
  relative_humidity_weekly_era5_land
  heat_stress_weekly_era5_heat
  heat_stress_max_weekly_era5_heat
  heat_stress_min_weekly_era5_heat
  vegetation_ndvi_weekly
  vegetation_evi_weekly
)

for CLIMATE_JOB in "${CLIMATE_JOBS[@]}"; do
  $CLI --env-file .env ingest-dataset "${CLIMATE_JOB}" \
    --start-period "${CLIMATE_START_PERIOD}" \
    --end-period "${CLIMATE_END_PERIOD}" \
    --publish || break
done
```

The `break` deliberately stops at the first failure so a missing provider credential or invalid
dataset cannot be hidden by later jobs.

## DHIS2 validation and import

Validate one job first:

```bash
$CLI --env-file .env review-import vegetation_ndvi_weekly \
  --start-period 2026-W39 --end-period 2026-W40

$CLI --env-file .env import-dhis2 vegetation_ndvi_weekly \
  --start-period 2026-W39 --end-period 2026-W40 --dry-run
```

$CLI --env-file .env ingest-dataset vegetation_ndvi_weekly \
 --start-period 2017-W01 \
 --end-period 2017-W52 \
 --publish || break
After an accepted dry-run:

```bash
$CLI --env-file .env import-dhis2 vegetation_ndvi_weekly \
  --start-period 2026-W39 --end-period 2026-W40 --apply
```

Repeat the import for each required job.

## Production deployment with systemd

The server runs two distinct units:

1. `open-climate-service.service` remains active on `127.0.0.1:9000`.
2. `dhis2-climate-pipeline.service` starts on demand, processes jobs sequentially, and exits. Its
   timer triggers the workflow weekly.

Do not use `make run` in production because it enables automatic reload. The supplied OCS unit uses
one Uvicorn worker to prevent multiple processes from writing to the same Icechunk stores.

Even on a multi-core server, keep `--workers 1` for this writable OCS instance. Store locking is
local to the process. Provider downloads and Xarray/Dask calculations may still use threads or
internal parallelism.

### 1. Create the service account and install OCS

The examples use the dedicated operating-system account `dhis2-import`. Create it once. If your
organization uses another account, update all `User`, `Group`, ownership, and `sudo -u` references.

```bash
sudo useradd --system --user-group \
  --home-dir /opt/dhis2-climate-import \
  --shell /usr/sbin/nologin \
  dhis2-import

sudo install -d -o dhis2-import -g dhis2-import /opt/open-climate-service
sudo -u dhis2-import git clone \
  <OCS_REPOSITORY_WITH_REQUIRED_PLUGINS> \
  /opt/open-climate-service

sudo -u dhis2-import bash -lc '
  cd /opt/open-climate-service
  uv sync --all-extras
'

sudo install -d -o dhis2-import -g dhis2-import /var/lib/open-climate-service
```

The deployed OCS repository must include the required instance plugins, such as
`plugins/datasets/drc_weekly.py` and `plugins/datasets/drc_weekly.yaml` when using those datasets.

Install configuration templates and enter provider credentials without exposing them in logs:

```bash
sudo install -o dhis2-import -g dhis2-import -m 0600 \
  /opt/dhis2-climate-import/deploy/systemd/open-climate-service.env.example \
  /opt/open-climate-service/.env

sudo install -o dhis2-import -g dhis2-import -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/open-climate-service.yaml.example \
  /opt/open-climate-service/climate-service.yaml

sudoedit /opt/open-climate-service/.env
```

The example keeps data outside the Git checkout in `/var/lib/open-climate-service`. Never copy a
store while ingestion is active. A safe migration must also update persisted artifact paths.

### 2. Start OCS

```bash
sudo install -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/open-climate-service.service \
  /etc/systemd/system/open-climate-service.service

sudo systemctl daemon-reload
sudo systemctl enable --now open-climate-service.service
sudo systemctl status open-climate-service.service
curl -sS http://127.0.0.1:9000/extent | jq .
```

Follow OCS logs:

```bash
sudo journalctl -fu open-climate-service.service
```

### 3. Install the climate pipeline

Verify `/opt/dhis2-climate-import/.env`:

```dotenv
CLIMATE_SERVICE_BASE_URL=http://127.0.0.1:9000
CLIMATE_SERVICE_HTTP_TIMEOUT_SECONDS=60
CLIMATE_SERVICE_POLL_SECONDS=5
CLIMATE_SERVICE_JOB_TIMEOUT_SECONDS=86400
```

Install the units and runtime configuration:

```bash
sudo install -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/dhis2-climate-pipeline.service \
  /etc/systemd/system/dhis2-climate-pipeline.service

sudo install -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/dhis2-climate-pipeline.timer \
  /etc/systemd/system/dhis2-climate-pipeline.timer

sudo install -m 0600 \
  /opt/dhis2-climate-import/deploy/systemd/dhis2-climate-pipeline.env.example \
  /etc/default/dhis2-climate-pipeline

sudo systemctl daemon-reload
```

For every configured job, the service runs:

```text
ingest-dataset → review-import → import-dhis2 --dry-run → import-dhis2 --apply → checkpoint
```

With `DHIS2_CLIMATE_CONTINUE_ON_FAILURE=true`, the service records a failed job and continues with
the next one. The unit still exits as failed so the incident remains visible. systemd may retry a
temporary exit code `75` up to its configured start limit; it does not retry permanent exit code
`2`.

### 4. Run an initial historical backfill

Before enabling the timer, edit `/etc/default/dhis2-climate-pipeline`:

```bash
DHIS2_CLIMATE_WINDOW_MODE=fixed
DHIS2_CLIMATE_START_PERIOD=2017-W01
DHIS2_CLIMATE_END_PERIOD=2026-W39
DHIS2_CLIMATE_PERIOD_BATCH_SIZE=13
```

This can backfill many years without sending one multi-year request. It generates independent
13-week windows:

```text
OCS ingestion for 13 weeks
→ DHIS2 review for 13 weeks
→ DHIS2 dry-run for 13 weeks
→ DHIS2 apply for 13 weeks
→ window checkpoint
```

Each stage runs in a new Python process, releasing its memory before the next stage. After a
timeout, systemd resumes from the first window without a checkpoint instead of replaying all years.

Start it in the background:

```bash
sudo systemctl start --no-block dhis2-climate-pipeline.service
sudo journalctl -fu dhis2-climate-pipeline.service
```

Checkpoints are stored under:

```text
/opt/dhis2-climate-import/state/climate-pipeline/checkpoints/
```

A confirmed window is skipped after restart. If the SQLite journal already records success but the
pipeline checkpoint is missing, the service reconstructs that checkpoint without `--force`.

### 5. Switch to weekly updates

After the backfill succeeds, configure a rolling window:

```bash
DHIS2_CLIMATE_WINDOW_MODE=rolling
DHIS2_CLIMATE_LOOKBACK_WEEKS=8
DHIS2_CLIMATE_LAG_WEEKS=3
DHIS2_CLIMATE_PERIOD_BATCH_SIZE=13
```

The window ends three weeks before the latest complete ISO week and revisits eight weeks. This
absorbs late provider publications and missed runs. Existing values are updated idempotently with
`CREATE_AND_UPDATE`.

Enable the timer:

```bash
sudo systemctl enable --now dhis2-climate-pipeline.timer
systemctl list-timers dhis2-climate-pipeline.timer
```

Operational commands:

```bash
sudo systemctl status dhis2-climate-pipeline.service
sudo journalctl -u dhis2-climate-pipeline.service --since today
sudo journalctl -fu dhis2-climate-pipeline.service

# Manual non-blocking trigger
sudo systemctl start --no-block dhis2-climate-pipeline.service
```

Keep the internal OCS scheduler disabled for the same datasets while the systemd timer orchestrates
them, otherwise concurrent submissions may receive a `409` conflict.
