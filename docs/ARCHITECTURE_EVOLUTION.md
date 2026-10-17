# DHIS2 Climate Import — Architecture Evolution

> **Repository:** `HISP-RDC/dhis2-climate-import`  
> **Purpose of this document:** define the target architecture for evolving the existing operational pipeline without replacing the parts that already work.  
> **Status:** architecture proposal for incremental implementation  
> **Last updated:** 2026-09-21

---

## 1. Executive summary

`dhis2-climate-import` already has the most important foundation of a production climate-data integration pipeline:

- connection to DHIS2;
- organisation-unit geometry caching;
- climate Data Element / Data Set metadata caching;
- Open Climate Service (OCS) integration;
- pre-import review;
- dry-run and explicit `--apply`;
- controlled `CREATE_AND_UPDATE` behaviour;
- reports written to `reports/`;
- run history stored in `cache/import-runs.sqlite3`;
- period-aware windows;
- Analytics verification;
- an existing `dhis2.trigger_analytics` configuration switch.

The next evolution should **not** turn this project into another Climate App, another Open Climate Service, or another CHAP scheduler.

The project should evolve into an **operational integration and orchestration layer between Open Climate Service and DHIS2**.

Its responsibility should be:

> **Ensure that configured climate data is synchronized, aggregated, validated, imported, made available in DHIS2 Analytics, verified, auditable, repeatable and observable from end to end.**

The architectural target is therefore:

```text
Climate / EO providers
        │
        ▼
┌──────────────────────┐
│ Open Climate Service │
│                      │
│ ingestion / sync     │
│ GeoZarr              │
│ openEO               │
│ workflows            │
│ spatial aggregation  │
└──────────┬───────────┘
           │
           ▼
┌─────────────────────────────────────┐
│ dhis2-climate-import                │
│                                     │
│ preflight                           │
│ orchestration                       │
│ scheduling                          │
│ period calculation                  │
│ validation                          │
│ import                              │
│ retry / resume                      │
│ audit                               │
│ reporting                           │
│ Analytics execution + verification │
└──────────────────┬──────────────────┘
                   │
                   ▼
              ┌─────────┐
              │  DHIS2  │
              └────┬────┘
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
      Maps      Analytics   Modeling App
                               │
                               ▼
                              CHAP
```

---

# 2. Product positioning

## 2.1 What this project is

The project should be positioned as:

> **An operational OCS → DHIS2 climate data pipeline for recurring, validated and auditable climate-data integration.**

A longer description suitable for the README:

> `dhis2-climate-import` is an open-source operational integration layer between Open Climate Service and DHIS2. It orchestrates climate-data aggregation, DHIS2 metadata validation, idempotent imports, scheduling, reporting, Analytics refresh and end-to-end verification against DHIS2 organisation-unit hierarchies.

## 2.2 What this project is not

It must not become:

- a replacement for **Open Climate Service**;
- a replacement for **DHIS2 Climate App**;
- a replacement for **DHIS2 Maps**;
- a replacement for **CHAP**;
- a generic geospatial raster engine;
- another implementation of CHIRPS/ERA5 ingestion;
- a second modeling platform.

## 2.3 Clear separation of responsibilities

| Component | Primary responsibility |
|---|---|
| Open Climate Service | Acquire, store, synchronize and process climate / Earth Observation datasets |
| DHIS2 Climate App | User-facing climate exploration and interactive climate import workflows |
| `dhis2-climate-import` | Operational orchestration, automation, validation, DHIS2 import, audit and Analytics readiness |
| DHIS2 Analytics / Maps | Analysis and visualization of imported values |
| DHIS2 Modeling App | DHIS2 interface for predictive-model workflows |
| CHAP | Disease modeling, evaluation and forecasting |

This separation is important because the OCS roadmap already includes deeper direct DHIS2 integration and scheduling. The project must therefore add operational value **around the OCS ↔ DHIS2 boundary**, not duplicate OCS internals.

---

# 3. Existing baseline that must be preserved

The current repository already contains design choices that should be treated as assets, not rewritten without need.

## 3.1 Current configuration baseline

The existing `config/import-pipeline.yaml` already supports important DHIS2 operational parameters such as:

```yaml
dhis2:
  org_unit_level: 3
  climate_data_set: CCH_CLIMATE_WEATHER
  climate_data_sets:
    - CCH_CLIMATE_WEATHER
  cache_max_age_hours: 168
  page_size: 500
  chunk_size: 5000
  async_threshold: 5000
  import_strategy: CREATE_AND_UPDATE
  preheat_cache: true
  trigger_analytics: false

open_climate_service:
  aggregate_process_id: aggregate_to_dhis2_json
  publish: true
```

The configuration already contains multiple import definitions with:

- OCS dataset;
- DHIS2 Data Element;
- period type;
- aggregation method;
- expected units;
- default date/period windows;
- optional Data Set.

Examples currently include:

- weekly ERA5-Land max temperature;
- daily CHIRPS precipitation;
- daily ERA5-Land temperature;
- daily temperature derived from hourly data;
- monthly temperature.

These mappings remain the basis of the evolved pipeline.

## 3.2 Existing commands and safeguards

The operational runbook already establishes:

- `check-dhis2`;
- `diagnose-dhis2`;
- `cache-org-units`;
- `cache-data-elements`;
- `review-import`;
- rolling windows with `--years-back`;
- deterministic `--as-of`;
- explicit `--end-period`;
- `import-dhis2 --dry-run`;
- `import-dhis2 --apply`;
- `verify-analytics`.

The architecture evolution must preserve these commands and gradually make them reusable by a higher-level orchestration layer.

## 3.3 Existing state and reporting

The current project already writes:

```text
reports/
cache/import-runs.sqlite3
```

This is important.

The next version must **extend** this operational history rather than replace it with console-only behaviour.

---

# 4. Core architectural principle: one engine, multiple entry points

The most important refactoring principle is:

> **Manual CLI execution, scheduled execution and a future API/UI must call the same pipeline engine.**

Do not implement three different import paths.

Target architecture:

```text
             ┌─────────────────┐
             │      CLI        │
             │ run-pipeline    │
             │ import-dhis2    │
             └────────┬────────┘
                      │
             ┌────────▼────────┐
             │ Pipeline Engine │
             └────────┬────────┘
                      │
     ┌────────────────┼────────────────┐
     │                │                │
┌────▼────┐      ┌────▼────┐      ┌────▼────┐
│Scheduler│      │ REST API │      │ Tests   │
│ future  │      │ future   │      │         │
└─────────┘      └─────────┘      └─────────┘
```

The engine owns the orchestration.

The CLI only supplies parameters.

The scheduler only decides **when** to call the engine.

A future UI/API only decides **what** pipeline to run and shows its state.

---

# 5. End-to-end definition of success

A major evolution is to stop considering a successful `/dataValueSets` request as the final success condition.

The project should introduce an explicit end-to-end state:

```text
END_TO_END_SUCCESS
```

A job reaches `END_TO_END_SUCCESS` only after all required phases have succeeded.

## 5.1 Target end-to-end flow

```text
TRIGGER
   │
   ▼
PREFLIGHT
   │
   ▼
METADATA / CACHE CHECK
   │
   ▼
PERIOD RESOLUTION
   │
   ▼
OCS DATASET CHECK / SYNC
   │
   ▼
SPATIAL AGGREGATION
   │
   ▼
VALIDATION
   │
   ▼
IMPORT PLAN
CREATE / UPDATE / SKIP
   │
   ▼
DHIS2 IMPORT
   │
   ▼
ANALYTICS GENERATION
   │
   ▼
ANALYTICS VERIFICATION
   │
   ▼
REPORT + AUDIT
   │
   ▼
END_TO_END_SUCCESS
```

## 5.2 Required pipeline phases

### Phase 0 — Trigger

Possible trigger types:

- manual CLI;
- scheduled;
- API/UI in a later version;
- recovery/resume of a failed job.

Every run gets a globally unique `job_id`.

Example:

```text
CLM-20260921-020000-7F3A
```

### Phase 1 — Preflight

Validate before expensive processing:

- DHIS2 reachable;
- authenticated DHIS2 user;
- minimum supported DHIS2 version;
- OCS reachable;
- configured import definition exists;
- configured Data Set can be resolved;
- configured Data Element can be resolved;
- Data Element belongs to the intended Data Set where required;
- period type compatible with Data Set period type;
- OCS dataset/workflow exists;
- target organisation-unit cache exists and is valid;
- geometry is present where required;
- credentials required by the selected execution mode are available;
- sufficient permissions exist for import;
- if Analytics triggering is enabled, permissions for the Analytics maintenance task are checked.

Fail early.

Do not download or aggregate climate data if basic DHIS2 configuration is invalid.

### Phase 2 — Metadata and cache preparation

Reuse the current cache approach.

The orchestrator may:

- use an existing valid cache;
- preheat the cache when configured;
- refresh only when explicitly required or stale according to policy.

No import step should silently mutate organisation-unit metadata.

Cache refresh must remain visible in logs and job steps.

### Phase 3 — Period resolution

The engine determines the exact time window that should be processed.

Sources may include:

- explicit `--start` / `--end`;
- explicit DHIS2 periods;
- `--years-back`;
- deterministic `--as-of`;
- schedule configuration;
- latest completed day/week/month/year;
- later: source-data availability policies.

The resolved window must be persisted in the job record.

A scheduled run must be reproducible later.

Therefore:

```text
schedule evaluation time != implicit runtime date
```

Store an explicit `as_of` value for every scheduled execution.

### Phase 4 — OCS dataset preparation / synchronization

The pipeline must call OCS only through a dedicated adapter.

Responsibilities:

- resolve configured OCS dataset;
- optionally request sync/ingestion when supported and enabled;
- wait or poll safely for OCS jobs if asynchronous;
- capture the OCS dataset version / update timestamp where available;
- never duplicate OCS ingestion logic in this repository.

The pipeline should not implement CHIRPS/ERA5 download clients itself.

### Phase 5 — OCS spatial aggregation

Execute the configured OCS workflow/process, for example:

```yaml
aggregate_process_id: aggregate_to_dhis2_json
```

Aggregation inputs should include the exact organisation-unit geometry set used by the job.

Persist enough provenance to reproduce the calculation:

- OCS dataset ID;
- OCS workflow/process ID;
- period range;
- organisation-unit level;
- organisation-unit cache hash;
- aggregation method;
- expected units;
- relevant workflow parameters.

### Phase 6 — Validation

Before importing:

- required fields present;
- each output maps to a known OrgUnit;
- each output maps to a known period;
- values parse as expected numeric type;
- units are expected;
- no invalid NaN/Infinity unless explicitly supported;
- count sanity checks;
- configured expected OrgUnit count if present;
- configured expected data-value count if present;
- duplicate `(dataElement, period, orgUnit, categoryOptionCombo, attributeOptionCombo)` keys rejected or reconciled deterministically.

Validation errors must block real import unless an explicitly documented partial-import policy exists.

Default should be fail-closed.

### Phase 7 — Import plan

Before mutation, calculate and persist the plan:

```text
CREATE
UPDATE
SKIP / UNCHANGED
INVALID
```

The existing `review-import` / dry-run behaviour should be reused.

The plan becomes an artifact.

Example:

```text
598 candidate values
  31 CREATE
  48 UPDATE
  519 UNCHANGED
   0 INVALID
```

No `--apply` without a valid plan.

### Phase 8 — DHIS2 import

Use the existing DHIS2 transport abstraction.

Current preferred transport remains the official `dhis2-python-client`, with emergency fallback preserved if still required.

Import rules:

- chunk according to `chunk_size`;
- use the configured `import_strategy`;
- preserve idempotency;
- record each chunk;
- handle asynchronous DHIS2 import jobs when thresholds require it;
- capture DHIS2 import summaries;
- never log secrets or authorization headers.

### Phase 9 — Analytics generation

This is a first-class stage, not an optional afterthought.

Current configuration already contains:

```yaml
trigger_analytics: false
```

Evolution:

```yaml
analytics:
  mode: verify_only      # off | verify_only | trigger_and_verify
  wait_for_completion: true
  timeout_seconds: 3600
  poll_interval_seconds: 10
  last_years: 2
```

#### `off`

Import ends after DHIS2 mutation.

Use only when Analytics is intentionally managed elsewhere.

#### `verify_only`

The pipeline does not trigger Analytics.

It verifies whether imported values have become available through `/api/analytics`.

Useful when the DHIS2 Scheduler / system administrator owns Analytics table generation.

#### `trigger_and_verify`

The pipeline:

1. triggers DHIS2 Analytics table generation using the supported maintenance/resource-table API for the target DHIS2 version;
2. monitors the server-side task if a task identifier/status mechanism is available;
3. waits for completion with timeout;
4. executes Analytics queries for the imported periods/Data Elements;
5. compares expected and observed results;
6. marks the job `END_TO_END_SUCCESS` only after verification.

This stage must be isolated in a version-aware DHIS2 Analytics adapter because DHIS2 versions can differ in maintenance/task endpoints.

### Phase 10 — End-to-end verification

Verification should not mean only "the Analytics endpoint returned HTTP 200".

It should answer:

- does the Data Element appear?
- does the imported period appear?
- are target OrgUnits represented?
- are expected values returned?
- does the count meet the expected threshold?
- are values materially consistent with the import result?

Possible verification levels:

```yaml
analytics:
  verification:
    mode: sampled       # none | sampled | full
    sample_size: 20
```

`sampled` is a practical default for large imports.

`full` can be used for controlled workloads.

### Phase 11 — Finalization

Every run finishes by:

- persisting final status;
- closing step timings;
- writing report artifacts;
- flushing file logs;
- storing error summaries;
- updating schedule state;
- releasing pipeline lock;
- printing a concise console summary.

---

# 6. Sequential pipeline runner

The project already supports executing operations step by step manually.

The next step is to expose one orchestration command that executes the same steps sequentially.

Proposed command:

```bash
python -m dhis2_climate_import run-pipeline precipitation_daily --apply
```

Dry-run:

```bash
python -m dhis2_climate_import run-pipeline precipitation_daily --dry-run
```

Scheduled/reproducible:

```bash
python -m dhis2_climate_import run-pipeline precipitation_daily \
  --as-of 2026-09-21 \
  --apply
```

Resume:

```bash
python -m dhis2_climate_import resume-job CLM-20260921-020000-7F3A
```

The sequential runner should call internal step functions, not shell out to the CLI.

Bad:

```python
subprocess.run(["python", "-m", "...", "cache-org-units"])
```

Good:

```python
runner.run_step(CacheOrgUnitsStep(...))
runner.run_step(AggregateStep(...))
runner.run_step(ImportStep(...))
```

---

# 7. Scheduler architecture

Scheduling is a natural extension of the existing deterministic period logic.

## 7.1 Important distinction

**Dataset periodicity is not the same as scheduler frequency.**

Example:

```text
Data period type: Weekly
Scheduler check: Daily
```

The daily scheduler can check whether a new complete weekly period is now ready.

This avoids assuming that an upstream source always publishes at an exact hour.

## 7.2 Scheduler responsibility

The scheduler decides:

- which pipeline should run;
- when it should be evaluated;
- what deterministic `as_of` value is passed to the pipeline;
- whether another run is already active;
- how retry/backoff rules should apply.

The scheduler does **not** implement import logic.

## 7.3 Proposed schedule configuration

```yaml
schedules:
  - id: chirps_daily_auto
    pipeline: precipitation_daily
    enabled: true

    trigger:
      type: cron
      cron: "0 2 * * *"
      timezone: Africa/Kinshasa

    window:
      strategy: completed_periods
      lookback_periods: 7

    execution:
      mode: apply
      retry:
        max_attempts: 3
        backoff_seconds: [60, 300, 900]

      overlap_policy: skip
```

Weekly example:

```yaml
  - id: era5_weekly_auto
    pipeline: temperature_max_weekly_era5_land
    enabled: true

    trigger:
      type: cron
      cron: "0 3 * * *"
      timezone: Africa/Kinshasa

    window:
      strategy: completed_periods
      lookback_periods: 4

    execution:
      mode: apply
      retry:
        max_attempts: 3
      overlap_policy: skip
```

Although the data is weekly, the job checks daily.

## 7.4 Schedule execution policies

Supported overlap policies:

- `skip`: if another run of the same pipeline is active, do not start;
- `queue`: enqueue one next execution;
- `fail`: treat overlap as an operational error.

Default:

```text
skip
```

for climate ingestion pipelines.

## 7.5 Locks

A pipeline lock must prevent:

```text
scheduler run + manual run
```

from importing the same data simultaneously.

Lock key:

```text
pipeline_id + target_dhis2_instance
```

A stale-lock recovery mechanism is required.

---

# 8. Job model and state machine

Each execution should become a first-class `job`.

## 8.1 Job states

Recommended:

```text
PENDING
RUNNING
WAITING_EXTERNAL
RETRYING
SUCCEEDED_IMPORT
SUCCEEDED_ANALYTICS
END_TO_END_SUCCESS
PARTIAL
FAILED
CANCELLED
SKIPPED
```

`SUCCEEDED_IMPORT` is intentionally different from `END_TO_END_SUCCESS`.

## 8.2 Step states

```text
PENDING
RUNNING
SUCCESS
FAILED
SKIPPED
RETRYING
```

## 8.3 Example state progression

```text
PENDING
  ↓
RUNNING
  ↓
IMPORT SUCCESS
  ↓
SUCCEEDED_IMPORT
  ↓
ANALYTICS TRIGGERED
  ↓
WAITING_EXTERNAL
  ↓
ANALYTICS VERIFIED
  ↓
END_TO_END_SUCCESS
```

If Analytics fails:

```text
SUCCEEDED_IMPORT
  ↓
ANALYTICS FAILED
  ↓
PARTIAL
```

This is more truthful than reporting the whole pipeline as failed when DHIS2 data values were already mutated.

---

# 9. Persistence evolution

Do not immediately replace `cache/import-runs.sqlite3`.

Evolve it through schema migration.

Recommended future DB path:

```text
cache/import-runs.sqlite3
```

Keep the existing filename for compatibility unless a migration is explicitly justified.

## 9.1 Proposed tables

### `jobs`

Core execution record.

Suggested fields:

```text
id
pipeline_id
trigger_type
schedule_id
mode
as_of
requested_start
requested_end
resolved_start
resolved_end
org_unit_level
data_set_id
data_element_id
ocs_dataset_id
status
started_at
finished_at
attempt
parent_job_id
config_hash
org_unit_cache_hash
error_code
error_message
created_at
updated_at
```

### `job_steps`

```text
id
job_id
step_name
sequence
status
started_at
finished_at
attempt
duration_ms
summary_json
error_code
error_message
```

### `schedules`

```text
id
pipeline_id
enabled
trigger_type
cron_expression
timezone
window_strategy
window_config_json
execution_config_json
last_evaluated_at
last_job_id
next_run_at
created_at
updated_at
```

### `job_artifacts`

```text
id
job_id
artifact_type
relative_path
content_type
checksum
created_at
```

### `job_import_chunks`

```text
id
job_id
chunk_no
value_count
status
dhis2_task_id
created_count
updated_count
ignored_count
error_count
started_at
finished_at
```

### `job_retries`

```text
id
job_id
step_name
attempt
reason
scheduled_at
started_at
finished_at
```

---

# 10. Logging architecture

This is a priority improvement.

The application must stop relying on console output as the only operational evidence.

## 10.1 Three logging/reporting layers

### A. Console

Purpose:

- developer feedback;
- manual operation;
- concise live progress.

Console should be human-readable and short.

Example:

```text
[21:02:01] job=CLM-... pipeline=precipitation_daily START
[21:02:02] preflight                         OK
[21:02:03] metadata                          OK 519 org units
[21:02:18] ocs.sync                          OK
[21:02:47] aggregate                         OK 519 values
[21:02:49] validate                          OK
[21:02:52] import                            OK create=12 update=507
[21:03:01] analytics.trigger                 OK
[21:04:21] analytics.verify                  OK
[21:04:21] END_TO_END_SUCCESS
```

### B. Application log file

Path:

```text
logs/dhis2-climate-import.log
```

Use rotation.

Recommended policy:

- rotate daily or by size;
- keep 14–30 files;
- UTF-8;
- structured context;
- no secrets.

### C. Per-job log

Path:

```text
logs/jobs/YYYY/MM/<job_id>.log
```

This file is immutable after job completion.

It contains all detailed messages for the job.

A failure report can therefore point directly to one file.

## 10.2 Structured logging fields

Every log event should be able to carry:

```text
timestamp
level
job_id
pipeline_id
schedule_id
step
attempt
dhis2_instance
org_unit_level
period_start
period_end
message
error_code
```

Optional JSON logging can later be added without changing business logic.

## 10.3 Secret redaction

Never log:

- PAT values;
- passwords;
- Authorization headers;
- upstream provider credentials;
- full `.env`;
- cookies.

Create one central redaction utility.

---

# 11. Reports and artifacts

Reports are different from logs.

Logs answer:

> What did the program do?

Reports answer:

> What happened to the data?

The project already writes to `reports/`. Keep that convention.

## 11.1 Proposed job report layout

```text
reports/
└── 2026/
    └── 09/
        └── CLM-20260921-020000-7F3A/
            ├── summary.md
            ├── summary.json
            ├── pipeline-config.snapshot.yaml
            ├── import-plan.csv
            ├── import-result.json
            ├── validation.json
            ├── analytics-verification.json
            ├── errors.ndjson
            └── provenance.json
```

## 11.2 `summary.md`

Human-readable.

Example:

```markdown
# Climate Import Run

- Job: CLM-...
- Pipeline: precipitation_daily
- Trigger: scheduled
- Period: 2026-09-20
- Organisation-unit level: 3
- OCS dataset: chirps3_precipitation_daily
- DHIS2 Data Element: CCH_PRECIP_CHIRPS_DAILY

## Result

- Candidate values: 519
- Created: 12
- Updated: 507
- Unchanged: 0
- Failed: 0

## Analytics

- Triggered: yes
- Generation completed: yes
- Verification: passed
- Verified values: 519/519

## Final status

END_TO_END_SUCCESS
```

## 11.3 `summary.json`

Machine-readable equivalent.

Useful for:

- future UI;
- CI;
- external monitoring;
- support;
- automated notifications.

## 11.4 `provenance.json`

Must capture enough provenance for reproducibility:

```json
{
  "jobId": "...",
  "pipeline": "precipitation_daily",
  "ocsDataset": "chirps3_precipitation_daily",
  "ocsProcess": "aggregate_to_dhis2_json",
  "method": "mean",
  "units": "mm",
  "orgUnitLevel": 3,
  "orgUnitCacheHash": "...",
  "configHash": "...",
  "periodStart": "2026-09-20",
  "periodEnd": "2026-09-20",
  "dhis2DataElement": "..."
}
```

---

# 12. Error handling, retry and recovery

## 12.1 Error classes

Create stable categories:

```text
CONFIG_ERROR
AUTH_ERROR
DHIS2_METADATA_ERROR
DHIS2_IMPORT_ERROR
DHIS2_ANALYTICS_ERROR
OCS_CONNECTIVITY_ERROR
OCS_DATASET_ERROR
OCS_PROCESSING_ERROR
VALIDATION_ERROR
GEOMETRY_ERROR
TIMEOUT_ERROR
LOCK_ERROR
INTERNAL_ERROR
```

## 12.2 Retry rules

Do retry:

- transient network failure;
- HTTP 429;
- HTTP 502/503/504;
- temporary OCS job failure where safe;
- DHIS2 asynchronous task temporarily unavailable.

Do not automatically retry:

- invalid Data Element;
- invalid Data Set period type;
- malformed geometry;
- invalid mapping;
- authentication failure caused by wrong credentials;
- deterministic validation failure.

## 12.3 Resume

A failed job should be resumable when safe.

Example:

```text
aggregate SUCCESS
validation SUCCESS
import SUCCESS
analytics FAILED
```

Resume should start from Analytics, not re-import blindly.

This requires persisted step state and immutable import artifacts.

---

# 13. Idempotency

Idempotency is central for scheduling.

The pipeline must ensure that rerunning the same logical window does not produce uncontrolled duplicate operations.

Idempotency inputs should include:

```text
target DHIS2 instance
pipeline
Data Element
OrgUnit level
period window
source/OCS dataset
relevant configuration hash
```

The project can choose:

- skip an identical already successful job;
- verify again;
- explicitly force rerun with `--force`.

Example:

```bash
run-pipeline precipitation_daily --as-of 2026-09-21 --apply
```

second execution:

```text
Identical successful job found.
Action: SKIP
Use --force to recalculate/reimport.
```

---

# 14. Analytics as an operational contract

Analytics must become an explicit contract between the importer and the consuming DHIS2 ecosystem.

The pipeline should define:

```text
Imported != Analytics-ready
```

Therefore:

```text
DHIS2_WRITE_SUCCESS
```

and:

```text
ANALYTICS_READY
```

are separate states.

## 14.1 Analytics triggering

When configured and authorized:

```text
POST/PUT DHIS2 resource/analytics table generation endpoint
```

The exact endpoint and supported parameters must be determined by the target DHIS2 version and isolated in the DHIS2 adapter.

Do not hard-code one API version into business logic.

## 14.2 Waiting and task tracking

If DHIS2 returns task metadata:

- persist task ID;
- poll status;
- enforce timeout;
- log progress where available.

If the DHIS2 version does not expose a reliable task ID:

- trigger;
- poll Analytics readiness with bounded backoff;
- clearly mark that task completion is inferred.

## 14.3 Verification strategies

### Sampled

For each imported period:

- sample N OrgUnits;
- query Analytics;
- compare values.

### Full

For smaller imports:

- query all expected combinations;
- compare count and values.

### Count-only

Allowed only as a lower-confidence operational mode.

---

# 15. Observability

The pipeline should expose a small operational status model even before a web UI exists.

Proposed CLI:

```bash
python -m dhis2_climate_import jobs list --limit 20
python -m dhis2_climate_import jobs show <job_id>
python -m dhis2_climate_import schedules list
python -m dhis2_climate_import health
```

Example:

```text
SYSTEM HEALTH

DHIS2                 OK
Open Climate Service  OK
Scheduler              OK
Active jobs            0
Failed jobs / 24h      1
Last E2E success       2026-09-21 02:04
```

Later the same service layer can back a UI.

---

# 16. Future UI: Climate Operations, not another Climate App

A future DHIS2 app should focus on operations.

Suggested name:

```text
DHIS2 Climate Operations
```

Suggested sections:

```text
Overview
Pipelines
Schedules
Jobs
Import Review
Analytics
Logs / Reports
Configuration
```

Example overview:

```text
CLIMATE OPERATIONS
────────────────────────────────────────────

DHIS2                    ● Connected
Open Climate Service     ● Connected
Scheduler                 ● Running

PIPELINES
────────────────────────────────────────────
CHIRPS Daily Rainfall     ✓ Last E2E success
ERA5-Land Temperature    ✓ Last E2E success
ERA5 Weekly Max Temp      ⚠ Analytics pending

RECENT JOBS
────────────────────────────────────────────
CLM-001  CHIRPS      END_TO_END_SUCCESS
CLM-002  ERA5        END_TO_END_SUCCESS
CLM-003  ERA5-WEEK   PARTIAL
```

The UI must not duplicate Climate App's interactive variable/date/import workflow unless a concrete missing operational requirement justifies it.

---

# 17. CHAP boundary and readiness

The project should not run CHAP models directly.

Its responsibility ends when reliable climate covariates are available and Analytics-ready in DHIS2.

A later optional status can be:

```text
CHAP_INPUT_READY
```

This should mean only that configured prerequisite climate series are available according to a defined policy.

It must not imply:

- model validity;
- forecast quality;
- CHAP job completion.

---

# 18. Security and permissions

Use least privilege.

Separate capabilities:

```text
read metadata
read OrgUnits
write aggregate data values
trigger Analytics maintenance
read Analytics
```

Analytics-trigger permission should be optional.

When the technical account cannot trigger Analytics:

```yaml
analytics:
  mode: verify_only
```

The system should not require a superuser account merely to import climate values.

---

# 19. Deployment model

## 19.1 First production target

Keep the first production architecture simple:

```text
one service instance
one SQLite operational database
one scheduler process
one worker/execution process
one target DHIS2 instance
one OCS instance
```

This is enough before introducing queues or distributed workers.

## 19.2 Process supervision

Recommended deployment options:

- Docker Compose; or
- `systemd`.

The scheduler must run as a long-lived supervised process.

Do not rely on a developer terminal.

## 19.3 Later scale-out

Only when needed:

```text
API
Scheduler
Queue
Workers
PostgreSQL
Object storage for artifacts
```

Do not adopt this complexity prematurely.

---

# 20. Testing strategy

## 20.1 Unit tests

Must cover:

- period resolution;
- schedule evaluation;
- retry classification;
- state transitions;
- config parsing;
- redaction;
- import-plan classification;
- Analytics comparison;
- job idempotency key;
- lock behavior.

## 20.2 Contract tests

Mock or fixture contracts for:

- DHIS2 system info;
- DHIS2 metadata;
- Data Value Set import;
- asynchronous import status;
- Analytics endpoint;
- Analytics generation trigger;
- OCS dataset discovery;
- OCS aggregation output.

## 20.3 Integration tests

At least one controlled test flow:

```text
fake/sandbox OCS output
        ↓
DHIS2 test instance
        ↓
real dataValueSet import
        ↓
Analytics generation / wait
        ↓
Analytics verification
```

## 20.4 End-to-end regression test

The critical regression scenario is:

```text
review
→ aggregate
→ plan
→ CREATE/UPDATE
→ Analytics
→ verify
→ report
```

---

# 21. Configuration evolution

Keep existing fields compatible.

Proposed additions:

```yaml
runtime:
  timezone: Africa/Kinshasa

logging:
  level: INFO
  console: true
  file:
    enabled: true
    path: logs/dhis2-climate-import.log
    rotate: daily
    retention_days: 30
  per_job:
    enabled: true
    directory: logs/jobs

reports:
  directory: reports
  write_markdown_summary: true
  write_json_summary: true
  write_import_plan_csv: true
  write_provenance: true

analytics:
  mode: verify_only
  wait_for_completion: true
  timeout_seconds: 3600
  poll_interval_seconds: 10
  last_years: 2
  verification:
    mode: sampled
    sample_size: 20

scheduler:
  enabled: false
  store: cache/import-runs.sqlite3
  misfire_grace_seconds: 3600

schedules: []
```

Backward compatibility:

```text
dhis2.trigger_analytics
```

may initially be translated internally to the new Analytics mode, then deprecated with a clear warning in a later release.

---

# 22. Recommended CLI evolution

Existing commands remain.

Add:

```text
run-pipeline
resume-job

jobs list
jobs show

schedules list
schedules validate
schedules run-due

scheduler start

report show
health
```

Possible examples:

```bash
# Existing manual workflow remains valid
python -m dhis2_climate_import review-import precipitation_daily
python -m dhis2_climate_import import-dhis2 precipitation_daily --dry-run
python -m dhis2_climate_import import-dhis2 precipitation_daily --apply

# New orchestration
python -m dhis2_climate_import run-pipeline precipitation_daily --dry-run
python -m dhis2_climate_import run-pipeline precipitation_daily --apply

# Operations
python -m dhis2_climate_import jobs list
python -m dhis2_climate_import jobs show CLM-...
python -m dhis2_climate_import health

# Scheduler
python -m dhis2_climate_import schedules validate
python -m dhis2_climate_import scheduler start
```

---

# 23. Roadmap

## Stage A — Stabilize the current baseline

Goal:

> Protect what already works.

Deliverables:

- regression tests for current commands;
- freeze current config semantics;
- document current CREATE/UPDATE behaviour;
- document current report schema;
- document current SQLite schema;
- confirm Analytics verification against the real target instance.

## Stage B — Unified pipeline runner

Goal:

> One sequential end-to-end command.

Deliverables:

- `run-pipeline`;
- job ID;
- step state;
- persisted state;
- deterministic period resolution;
- existing functions reused internally.

## Stage C — Logging and reports

Goal:

> Every run leaves a complete operational trace.

Deliverables:

- rotating application log;
- per-job log;
- structured context;
- `summary.md`;
- `summary.json`;
- `import-plan.csv`;
- `provenance.json`;
- stable error codes.

## Stage D — Analytics end-to-end

Goal:

> Import success is not final success.

Deliverables:

- version-aware Analytics trigger adapter;
- `off | verify_only | trigger_and_verify`;
- task wait/polling;
- Analytics verification;
- `END_TO_END_SUCCESS`.

## Stage E — Scheduler

Goal:

> Run configured pipelines automatically and safely.

Deliverables:

- persisted schedules;
- timezone support;
- deterministic `as_of`;
- pipeline locking;
- retry/backoff;
- no-overlap policy;
- job history.

## Stage F — Recovery and operational maturity

Deliverables:

- resume failed jobs;
- stale-job detection;
- stale-lock recovery;
- error categorization;
- health command;
- retention policies.

## Stage G — API / UI

Only after the engine is stable.

Deliverables:

- read-only job API first;
- schedule management API;
- `Run now`;
- DHIS2 Climate Operations UI;
- dashboard-oriented operational views.

---

# 24. Explicit non-goals for the next implementation cycle

Do **not** implement now:

- a new climate-data downloader;
- custom CHIRPS scraping;
- custom ERA5 ingestion;
- raster storage;
- a new STAC implementation;
- a new openEO engine;
- CHAP model execution;
- a complete DHIS2 web app before scheduler/engine stabilization;
- distributed workers;
- Kafka/RabbitMQ;
- Kubernetes;
- multi-tenant complexity.

---

# 25. Open design topic: climate-data availability visibility

A separate product/design discussion is still required for:

> How should an administrator see which climate periods are currently available, imported and usable?

This document intentionally does **not** freeze the representation yet.

The final design should be decided after clarifying:

- whether the requirement is about source availability, OCS availability, DHIS2 imported periods, Analytics-visible periods, or a combination;
- whether the information must live inside standard DHIS2 analytics objects;
- whether a Data Element, indicator, DataStore entry, plugin, dashboard widget, API endpoint or a hybrid is most appropriate;
- what exactly the administrator expects to see in Climate App / Dashboard.

Implementation of this feature should therefore be kept behind a separate design decision and not mixed into the initial scheduler refactor.

---

# 26. Definition of architectural success

The evolved system is successful when an administrator can configure a pipeline once and later observe:

```text
Pipeline: CHIRPS Daily Rainfall
Period:   2026-09-20

✓ DHIS2 preflight
✓ OCS dataset ready
✓ OrgUnit geometry set validated
✓ Aggregation completed
✓ Import plan created
✓ DHIS2 import completed
✓ Analytics generation completed
✓ Analytics values verified
✓ Audit report written
✓ Logs persisted

Final status:
END_TO_END_SUCCESS
```

and when the exact same pipeline can be launched by:

```text
manual CLI
scheduled execution
future API
future UI
```

without duplicating business logic.

---

# 27. References

- Open Climate Service repository: https://github.com/dhis2/open-climate-service
- Open Climate Service roadmap: https://open-climate-service.dhis2.org/roadmap/
- DHIS2 Open Climate Service introduction: https://dhis2.org/introducing-open-climate-service/
- DHIS2 Analytics Web API: https://github.com/dhis2/dhis2-docs/blob/master/src/developer/web-api/analytics.md
- DHIS2 Data Administration / Analytics tables: https://github.com/dhis2/dhis2-docs/blob/master/src/user/data-administration.md
- DHIS2 Climate & Health Academy 2026 community reference: https://community.dhis2.org/t/dhis2-climate-health-academy-2026-wiki-ai-technical-reference-and-implementation-guide/75837
