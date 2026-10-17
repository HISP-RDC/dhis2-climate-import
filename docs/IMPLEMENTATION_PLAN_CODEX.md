# DHIS2 Climate Import — Implementation Plan for Codex

> **Repository:** `HISP-RDC/dhis2-climate-import`  
> **Audience:** developer + Codex agent  
> **Purpose:** incremental implementation plan for evolving the existing project into a scheduled, observable, end-to-end OCS → DHIS2 climate operations pipeline.  
> **Important:** preserve all existing working behaviour first.

---

# 1. Non-negotiable implementation rules

Codex must follow these rules throughout the work.

## 1.1 Preserve existing behaviour

Do not break:

```text
check-dhis2
diagnose-dhis2
cache-org-units
cache-data-elements
review-import
import-dhis2 --dry-run
import-dhis2 --apply
verify-analytics
```

Do not silently change:

- current mapping semantics;
- current CREATE/UPDATE behaviour;
- current period resolution;
- existing cache formats unless a migration/backward-compatibility path exists;
- `reports/`;
- `cache/import-runs.sqlite3`.

## 1.2 Refactor before duplicating

If current CLI handlers contain reusable business logic, extract it into services/functions.

Do not make the scheduler call shell commands.

Do not copy the import logic into scheduler code.

The target dependency direction is:

```text
CLI ─────────┐
Scheduler ───┼──> Application services / Pipeline engine
Future API ──┘
                    │
                    ├── OCS adapter
                    ├── DHIS2 adapter
                    ├── storage
                    ├── reporting
                    └── logging
```

## 1.3 One change set at a time

Implement in phases.

At the end of every phase:

1. run tests;
2. run current CLI smoke tests;
3. update docs;
4. commit a coherent unit of work.

Avoid one giant refactor.

## 1.4 Fail closed for data mutations

If validation is uncertain:

```text
DO NOT APPLY
```

Dry-run may continue far enough to produce diagnostics.

Real import must stop.

## 1.5 Never log secrets

Redact:

```text
DHIS2_PAT
DHIS2_PASSWORD
Authorization
provider credentials
cookies
```

---

# 2. Start with a codebase audit

Before modifying code, inspect and document the existing implementation.

Produce a temporary developer note or issue checklist answering:

- Where is CLI argument parsing implemented?
- Which module implements `review-import`?
- Which module implements `import-dhis2`?
- Which module implements `verify-analytics`?
- Where is the OCS client/adapter?
- Where is the DHIS2 client/transport abstraction?
- Where is SQLite initialized?
- What is the existing run-history schema?
- What files are currently written to `reports/`?
- How is the current run hash/idempotency check implemented?
- How are dynamic periods currently resolved?
- What tests already exist?
- How is `dhis2.trigger_analytics` currently used?

Do not redesign these components until the answers are known.

---

# 3. Target package structure

Adapt this to the existing structure rather than blindly creating duplicates.

Proposed logical layout:

```text
src/dhis2_climate_import/
│
├── cli/
│   ├── main.py
│   ├── pipeline_commands.py
│   ├── job_commands.py
│   └── schedule_commands.py
│
├── application/
│   ├── pipeline_runner.py
│   ├── context.py
│   ├── result.py
│   └── steps/
│       ├── base.py
│       ├── preflight.py
│       ├── metadata.py
│       ├── period_resolution.py
│       ├── ocs_sync.py
│       ├── aggregate.py
│       ├── validate.py
│       ├── import_plan.py
│       ├── dhis2_import.py
│       ├── analytics_trigger.py
│       ├── analytics_verify.py
│       └── finalize.py
│
├── domain/
│   ├── jobs.py
│   ├── schedules.py
│   ├── statuses.py
│   ├── errors.py
│   └── periods.py
│
├── infrastructure/
│   ├── dhis2/
│   ├── ocs/
│   ├── persistence/
│   ├── logging/
│   └── reporting/
│
├── scheduler/
│   ├── service.py
│   ├── locks.py
│   └── policies.py
│
└── ...
```

If equivalent modules already exist, reuse/rename incrementally instead of creating parallel abstractions.

---

# 4. Phase 0 — Freeze the working baseline

## Goal

Create enough regression protection before restructuring.

## Tasks

- [ ] Add tests for `review-import`.
- [ ] Add tests for period resolution:
  - [ ] daily;
  - [ ] weekly;
  - [ ] monthly;
  - [ ] yearly if supported;
  - [ ] `--years-back`;
  - [ ] `--as-of`;
  - [ ] explicit end period.
- [ ] Add tests for Data Set period-type compatibility.
- [ ] Add tests for Data Element resolution by UID/code.
- [ ] Add tests for import plan generation.
- [ ] Add tests for `CREATE_AND_UPDATE`.
- [ ] Add tests for dry-run not mutating DHIS2.
- [ ] Add tests for current SQLite run-history behavior.
- [ ] Add tests for current report writing.
- [ ] Add at least one test for `verify-analytics`.

## Acceptance criteria

All existing CLI commands still behave identically.

No scheduler code yet.

---

# 5. Phase 1 — Introduce a first-class Job model

## Goal

Every pipeline execution has durable identity and state.

## 5.1 Job ID

Add a generator.

Suggested format:

```text
CLM-YYYYMMDD-HHMMSS-RANDOM
```

Example:

```text
CLM-20260921-020000-7F3A
```

Requirements:

- sortable by creation time;
- collision resistant;
- readable in logs/support messages.

## 5.2 Job status enum

Implement:

```python
class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_EXTERNAL = "WAITING_EXTERNAL"
    RETRYING = "RETRYING"
    SUCCEEDED_IMPORT = "SUCCEEDED_IMPORT"
    SUCCEEDED_ANALYTICS = "SUCCEEDED_ANALYTICS"
    END_TO_END_SUCCESS = "END_TO_END_SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"
```

Use one central enum.

Do not use free-text statuses throughout the code.

## 5.3 Step status enum

```python
PENDING
RUNNING
SUCCESS
FAILED
SKIPPED
RETRYING
```

## 5.4 Persistence migration

Extend the existing SQLite DB.

Do not delete old tables/data.

Create a migration mechanism.

At minimum:

```sql
jobs
job_steps
job_artifacts
job_import_chunks
job_retries
```

### Minimum `jobs` columns

```text
id
pipeline_id
trigger_type
schedule_id nullable
mode
as_of
resolved_start
resolved_end
org_unit_level
data_set_id
data_element_id
ocs_dataset_id
status
attempt
config_hash
org_unit_cache_hash
started_at
finished_at
error_code
error_message
created_at
updated_at
```

## Acceptance criteria

Running an existing import through the new internal job wrapper creates a durable job record without changing import results.

---

# 6. Phase 2 — Unified Pipeline Runner

## Goal

One internal orchestration service executes the end-to-end sequence.

## 6.1 New application service

Create a `PipelineRunner`.

Conceptual interface:

```python
result = runner.run(
    pipeline_id="precipitation_daily",
    mode="apply",
    as_of=date(2026, 9, 21),
    start=None,
    end=None,
    org_unit_level=None,
    force=False,
)
```

## 6.2 Initial step sequence

```python
steps = [
    PreflightStep(),
    MetadataStep(),
    PeriodResolutionStep(),
    OCSSyncStep(),
    AggregateStep(),
    ValidateStep(),
    ImportPlanStep(),
    DHIS2ImportStep(),
    AnalyticsTriggerStep(),
    AnalyticsVerifyStep(),
    FinalizeStep(),
]
```

A step can be skipped by policy/config but remains visible in job history.

## 6.3 Pipeline context

Introduce a typed context object passed between steps.

Example:

```python
@dataclass
class PipelineContext:
    job_id: str
    pipeline_id: str
    mode: Literal["dry-run", "apply"]
    as_of: date

    config: PipelineConfig

    org_units: list[OrgUnit] | None = None
    data_element: DataElement | None = None
    data_set: DataSet | None = None

    resolved_start: date | None = None
    resolved_end: date | None = None

    aggregated_values: list[ClimateValue] | None = None
    import_plan: ImportPlan | None = None
    import_result: ImportResult | None = None
    analytics_result: AnalyticsVerification | None = None
```

Do not turn the context into an untyped dictionary.

## 6.4 Step contract

Conceptual:

```python
class PipelineStep(Protocol):
    name: str

    def should_run(self, ctx: PipelineContext) -> bool:
        ...

    def execute(self, ctx: PipelineContext) -> StepResult:
        ...
```

The runner is responsible for:

- step timing;
- state persistence;
- log context;
- exceptions;
- retry policy hooks;
- final job status.

## 6.5 New CLI command

Add:

```bash
python -m dhis2_climate_import run-pipeline <pipeline_id> [options]
```

Support initially:

```text
--dry-run
--apply
--as-of
--start
--end
--start-period
--end-period
--years-back
--org-unit-level
--data-set
--force
```

Reuse existing parsers/helpers where possible.

## Acceptance criteria

These two paths produce equivalent import plans/results:

```text
existing manual sequence
```

and:

```text
run-pipeline
```

---

# 7. Phase 3 — Logging

## Goal

Every execution is diagnosable after the terminal is closed.

## 7.1 Central logging configuration

Create one logging bootstrap.

Configuration:

```yaml
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
```

## 7.2 Context injection

Every pipeline log after job creation must include:

```text
job_id
pipeline_id
step
```

When relevant:

```text
schedule_id
attempt
period_start
period_end
```

Use a logger adapter/context mechanism instead of formatting these manually in every call.

## 7.3 Global application log

Create:

```text
logs/dhis2-climate-import.log
```

Use a rotating handler.

## 7.4 Per-job log

Create:

```text
logs/jobs/YYYY/MM/<job_id>.log
```

Open when the job starts.

Close during finalization.

## 7.5 Console format

Keep concise:

```text
21:02:01 INFO  preflight          OK
21:02:03 INFO  metadata           org_units=519
21:02:47 INFO  aggregate          values=519
21:02:52 INFO  import             create=12 update=507
21:04:21 INFO  analytics.verify   passed
```

## 7.6 Redaction

Implement a central redaction filter.

Unit tests must prove that known token/password field names are hidden.

## Acceptance criteria

A failed job can be diagnosed using only:

```text
job ID
per-job log
summary report
```

without needing the original terminal output.

---

# 8. Phase 4 — Reports and artifacts

## Goal

Produce durable, human-readable and machine-readable evidence.

## 8.1 Directory

For every real pipeline execution:

```text
reports/YYYY/MM/<job_id>/
```

## 8.2 Required files

### `summary.md`

Always write.

### `summary.json`

Always write.

### `pipeline-config.snapshot.yaml`

Write sanitized configuration only.

No secrets.

### `import-plan.csv`

Write after plan generation.

Suggested columns:

```text
data_element
period
org_unit
org_unit_name
candidate_value
existing_value
action
validation_status
```

### `validation.json`

Write validation counts and errors.

### `import-result.json`

Write DHIS2 response summary / normalized result.

### `analytics-verification.json`

Write when Analytics verification runs.

### `provenance.json`

Write source/OCS/process/cache/config provenance.

### `errors.ndjson`

Append normalized error records for failed/partial jobs.

## 8.3 Register artifacts in SQLite

Each artifact must be registered in `job_artifacts`.

Store:

```text
job_id
artifact_type
relative_path
content_type
checksum
created_at
```

## Acceptance criteria

`jobs show <job_id>` can list report artifact paths.

---

# 9. Phase 5 — Analytics end-to-end support

## Goal

Make DHIS2 Analytics readiness part of pipeline completion.

This is a core requirement.

## 9.1 New configuration

Add:

```yaml
analytics:
  mode: verify_only
  wait_for_completion: true
  timeout_seconds: 3600
  poll_interval_seconds: 10
  last_years: 2

  verification:
    mode: sampled
    sample_size: 20
```

Modes:

```text
off
verify_only
trigger_and_verify
```

## 9.2 Backward compatibility

Current:

```yaml
dhis2:
  trigger_analytics: false
```

Migration logic for first release:

```text
if new analytics.mode exists:
    use it
else if dhis2.trigger_analytics is true:
    mode = trigger_and_verify
else:
    mode = verify_only
```

Emit a deprecation warning only after the new config has been documented.

Do not immediately break existing configs.

## 9.3 DHIS2 Analytics service

Create a dedicated adapter/service.

Responsibilities:

```python
class DHIS2AnalyticsService:
    def trigger_generation(...): ...
    def get_task_status(...): ...
    def wait_for_generation(...): ...
    def query_values(...): ...
    def verify_import(...): ...
```

The service must detect/handle the DHIS2 version.

Avoid hard-coding `/api/33/...` if an unversioned endpoint is supported by the target instance.

## 9.4 Trigger step

Rules:

- only after a successful real import;
- skip in dry-run;
- only when mode is `trigger_and_verify`;
- persist task/job identifiers when returned;
- timeout must not erase the fact that import already succeeded.

## 9.5 Analytics failure semantics

If:

```text
DHIS2 import SUCCESS
Analytics FAILED
```

job final state:

```text
PARTIAL
```

not:

```text
FAILED
```

because data mutation has already occurred.

## 9.6 Verify step

Support:

```text
none
sampled
full
```

For `sampled`:

- deterministic sample seeded from job ID;
- include first/last period;
- include a stable set of OrgUnits;
- compare expected/imported values.

For `full`:

- retrieve all intended combinations when practical;
- compare counts and values.

## 9.7 Final-state rule

Only:

```text
END_TO_END_SUCCESS
```

when all required Analytics checks pass.

## Acceptance criteria

A real import that reaches Data Values but not Analytics is visibly `PARTIAL`.

A job is never falsely presented as fully successful when Analytics is stale/unavailable.

---

# 10. Phase 6 — Retry and resume

## Goal

Handle transient failure without redoing successful destructive work unnecessarily.

## 10.1 Stable error taxonomy

Create:

```python
class ErrorCode(str, Enum):
    CONFIG_ERROR = "CONFIG_ERROR"
    AUTH_ERROR = "AUTH_ERROR"
    DHIS2_METADATA_ERROR = "DHIS2_METADATA_ERROR"
    DHIS2_IMPORT_ERROR = "DHIS2_IMPORT_ERROR"
    DHIS2_ANALYTICS_ERROR = "DHIS2_ANALYTICS_ERROR"
    OCS_CONNECTIVITY_ERROR = "OCS_CONNECTIVITY_ERROR"
    OCS_DATASET_ERROR = "OCS_DATASET_ERROR"
    OCS_PROCESSING_ERROR = "OCS_PROCESSING_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    GEOMETRY_ERROR = "GEOMETRY_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    LOCK_ERROR = "LOCK_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"
```

## 10.2 Retry classifier

Retryable examples:

```text
timeout
connection reset
HTTP 429
HTTP 502
HTTP 503
HTTP 504
temporary external-job failure
```

Not retryable:

```text
wrong Data Element
wrong Data Set
wrong period type
invalid mapping
invalid geometry
validation failure
wrong credentials
```

## 10.3 Resume command

Add:

```bash
python -m dhis2_climate_import resume-job <job_id>
```

Rules:

- never resume a completed `END_TO_END_SUCCESS` unless `--force`;
- preserve original `as_of`;
- preserve original resolved period window;
- preserve original config snapshot;
- restart from the first safely resumable failed/incomplete step.

Example:

```text
import SUCCESS
analytics.trigger SUCCESS
analytics.verify FAILED
```

Resume begins at Analytics verification.

## Acceptance criteria

Resume must not cause accidental double import.

---

# 11. Phase 7 — Idempotency and execution locks

## Goal

Make scheduling safe.

## 11.1 Logical run key

Calculate a deterministic key from:

```text
target DHIS2 instance identity
pipeline ID
Data Element
OrgUnit level
resolved period window
OCS dataset/workflow
relevant config hash
```

Store it in the job.

## 11.2 Duplicate successful run

If an identical successful job exists:

Default:

```text
SKIPPED
```

Allow:

```bash
--force
```

## 11.3 Pipeline lock

Lock key:

```text
target_instance + pipeline_id
```

Lock metadata:

```text
owner job ID
created_at
heartbeat_at
process identifier if useful
```

## 11.4 Stale lock

A lock may be reclaimed only according to an explicit stale policy.

Never delete a lock merely because a new process wants it.

## Acceptance criteria

Two concurrent executions of the same pipeline cannot mutate DHIS2 simultaneously.

---

# 12. Phase 8 — Scheduler

## Goal

Persist and execute recurring pipelines.

## 12.1 Scheduler library

For the first single-node implementation, use a stable Python scheduler compatible with the supported Python version.

Do not tightly couple application logic to the scheduler library.

Wrap it with:

```python
class SchedulerService:
    ...
```

## 12.2 Configuration

Add:

```yaml
runtime:
  timezone: Africa/Kinshasa

scheduler:
  enabled: false
  misfire_grace_seconds: 3600

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
      overlap_policy: skip

      retry:
        max_attempts: 3
        backoff_seconds:
          - 60
          - 300
          - 900
```

## 12.3 Important rule

The scheduler checks pipelines according to a configured cadence.

Do not assume:

```text
weekly data => execute once per week
```

It can be:

```text
weekly data
daily check
```

This provides resilience to publication delays.

## 12.4 Persist schedules

Add `schedules` table.

Even if config is YAML-driven initially, persist evaluation state:

```text
last_evaluated_at
last_job_id
next_run_at
```

## 12.5 Commands

Add:

```bash
python -m dhis2_climate_import schedules list
python -m dhis2_climate_import schedules validate
python -m dhis2_climate_import schedules run-due
python -m dhis2_climate_import scheduler start
```

`schedules run-due` is especially useful for:

- testing;
- system cron fallback;
- CI;
- deterministic operational control.

## 12.6 Scheduler job creation

Scheduler does not call `import-dhis2` CLI.

It calls:

```python
PipelineRunner.run(...)
```

with:

```text
trigger_type = scheduled
schedule_id = ...
as_of = deterministic scheduler evaluation date
```

## Acceptance criteria

A scheduler restart does not lose persistent schedule history.

A missed execution follows the configured misfire policy.

---

# 13. Phase 9 — Operational CLI

Add commands around the new job model.

## 13.1 Jobs

```bash
jobs list
jobs show <job_id>
jobs failures
```

Example output:

```text
JOB                       PIPELINE              STATUS                 START
CLM-...                    precipitation_daily  END_TO_END_SUCCESS     ...
CLM-...                    temperature_daily    PARTIAL                ...
```

## 13.2 Job detail

```bash
jobs show CLM-...
```

Output:

```text
Job:        CLM-...
Pipeline:   precipitation_daily
Status:     PARTIAL
Period:     2026-09-20
Trigger:    scheduled

STEPS
✓ preflight
✓ metadata
✓ period-resolution
✓ ocs-sync
✓ aggregate
✓ validate
✓ import-plan
✓ dhis2-import
✓ analytics-trigger
✗ analytics-verify

Artifacts:
reports/.../summary.md
reports/.../analytics-verification.json

Log:
logs/jobs/.../CLM-....log
```

## 13.3 Health

Add:

```bash
health
```

Minimum checks:

```text
configuration valid
SQLite accessible
DHIS2 reachable
OCS reachable
scheduler status
active jobs
last E2E success
failed jobs in last 24h
```

Do not mutate anything in `health`.

---

# 14. Phase 10 — Reporting quality

## Goal

Make reports useful for operations and community demonstration.

## 14.1 Human-readable final console block

At finalization:

```text
────────────────────────────────────────────
Climate pipeline completed
────────────────────────────────────────────
Job                CLM-...
Pipeline            precipitation_daily
Period              2026-09-20
OrgUnit level       3

Import
  Candidate          519
  Create              12
  Update             507
  Unchanged            0
  Failed               0

Analytics
  Triggered           yes
  Completed           yes
  Verified            519/519

Final status
  END_TO_END_SUCCESS

Report
  reports/.../summary.md

Log
  logs/jobs/.../CLM-....log
────────────────────────────────────────────
```

## 14.2 Failure summary

Example:

```text
Final status: PARTIAL

DHIS2 import succeeded.
Analytics verification failed after 3600 seconds.

No automatic re-import will occur during resume.
Resume from:
  analytics.verify

Command:
  python -m dhis2_climate_import resume-job CLM-...
```

This distinction is critical.

---

# 15. Phase 11 — Future API boundary

Do not implement until the CLI/scheduler engine is stable.

Define interfaces first.

Candidate endpoints:

```text
GET  /api/health

GET  /api/pipelines
GET  /api/pipelines/{id}
POST /api/pipelines/{id}/run

GET  /api/jobs
GET  /api/jobs/{id}
POST /api/jobs/{id}/resume

GET  /api/schedules
POST /api/schedules
PATCH /api/schedules/{id}
POST /api/schedules/{id}/run-now
```

The API calls application services.

No business logic in HTTP handlers.

---

# 16. Phase 12 — Future DHIS2 Climate Operations UI

Do not start until the backend operational model is stable.

Minimum screens:

```text
Overview
Pipelines
Schedules
Jobs
Job detail
Reports
Configuration
```

## Overview

Show operational state, not duplicate Climate App.

Example:

```text
DHIS2                 Connected
OCS                   Connected
Scheduler             Running
Active jobs           0
Last E2E success      02:04
Failed / 24h          1
```

## Pipeline detail

```text
Pipeline mapping
OCS dataset
DHIS2 Data Element
Data Set
period type
aggregation method
OrgUnit level
last job
schedule
```

## Job detail

Render the exact step timeline from `job_steps`.

---

# 17. Climate-data availability feature — HOLD FOR DESIGN DECISION

Do not implement the previously discussed availability visualization/storage model yet.

Create only a backlog issue/place-holder:

```text
FEATURE: Climate data availability visibility
STATUS: DESIGN REQUIRED
```

Questions to resolve with the project owner before coding:

1. What exactly does "available" mean in the admin's complaint?
2. Does the admin need:
   - source period range?
   - OCS period range?
   - periods imported into DHIS2?
   - periods visible in Analytics?
   - only the final usable range?
3. Where must it appear:
   - Climate App?
   - normal DHIS2 Dashboard?
   - Data Visualizer?
   - custom plugin?
   - this project's UI?
4. Must it be analyzable as a native DHIS2 Data Element?
5. Does it need one value per dataset/source/variable?
6. Is the expected output a date range, last-available period, completeness matrix, or another concept?

Do not create a monitoring Data Element/DataStore/plugin until these requirements are agreed.

---

# 18. Database migration requirements

Use versioned migrations.

Example:

```text
schema_version
```

Migration order:

```text
001 existing baseline marker
002 jobs
003 job_steps
004 job_artifacts
005 job_import_chunks
006 job_retries
007 schedules
008 locks
```

Requirements:

- migration transactional where SQLite allows;
- backup guidance before production migration;
- tests for upgrading a DB fixture from the old schema;
- no destructive automatic downgrade.

---

# 19. Configuration validation

Introduce strict typed configuration.

Prefer a typed model such as dataclasses/Pydantic if compatible with current dependencies.

Validation examples:

Reject:

```yaml
period_type: week
```

when Data Set resolves to monthly.

Reject invalid enum:

```yaml
analytics:
  mode: maybe
```

Reject scheduler without timezone.

Reject `trigger_and_verify` if required DHIS2 configuration cannot be resolved during preflight.

Config errors must reference:

```text
file
field path
invalid value
expected values
```

Example:

```text
config/import-pipeline.yaml:
analytics.mode='trigger'
Invalid value.
Expected one of: off, verify_only, trigger_and_verify
```

---

# 20. Suggested internal result models

## Import plan

```python
@dataclass(frozen=True)
class PlannedValue:
    data_element: str
    period: str
    org_unit: str
    candidate_value: Decimal
    existing_value: Decimal | None
    action: Literal["CREATE", "UPDATE", "UNCHANGED"]
```

## Import summary

```python
@dataclass
class ImportSummary:
    candidates: int
    created: int
    updated: int
    unchanged: int
    failed: int
    dhis2_task_ids: list[str]
```

## Analytics verification

```python
@dataclass
class AnalyticsVerification:
    mode: str
    expected_count: int
    observed_count: int
    checked_count: int
    matched_count: int
    mismatched_count: int
    missing_count: int
    passed: bool
```

Avoid passing raw dicts between every layer.

---

# 21. OCS integration boundaries

Keep OCS behind an adapter.

Conceptual:

```python
class OCSService:
    def check(self): ...
    def get_dataset(self, dataset_id): ...
    def sync_dataset(self, dataset_id): ...
    def wait_for_job(self, job_id): ...
    def aggregate(self, request): ...
```

Do not expose OCS HTTP response formats throughout the application layer.

Normalize them.

The OCS roadmap is active; APIs may evolve.

This adapter is therefore an important compatibility boundary.

---

# 22. DHIS2 integration boundaries

Separate concerns.

Suggested services:

```text
DHIS2SystemService
DHIS2MetadataService
DHIS2DataValueService
DHIS2AnalyticsService
```

Do not grow one giant DHIS2 client class.

## `DHIS2SystemService`

```text
system info
me
version/capabilities
```

## `DHIS2MetadataService`

```text
OrgUnits
Data Sets
Data Elements
period-type checks
```

## `DHIS2DataValueService`

```text
read existing values
build/import dataValueSets
async import tracking
```

## `DHIS2AnalyticsService`

```text
trigger analytics
wait
query
verify
```

---

# 23. Concurrency and crash safety

## 23.1 Transaction boundaries

Persist step `RUNNING` before performing external mutation.

Persist result immediately after completion.

## 23.2 Crash during import

On restart:

- detect `RUNNING` job with stale heartbeat;
- mark `INTERRUPTED` internally or normalize to `FAILED` with reason;
- do not automatically replay Data Value import;
- inspect persisted chunk/task results;
- require safe resume logic.

## 23.3 Heartbeat

For long-running jobs:

```text
job heartbeat_at
```

Update periodically.

Used for stale-job detection.

---

# 24. Retention

Add future config:

```yaml
retention:
  job_logs_days: 90
  reports_days: 365
  successful_jobs_days: 365
  failed_jobs_days: 730
```

Do not delete operational evidence by default in the first release.

Implement retention only after job/report stability.

---

# 25. Test matrix

Codex should build tests around this minimum matrix.

| Scenario | Expected result |
|---|---|
| DHIS2 unavailable at preflight | FAILED, no OCS work |
| OCS unavailable | FAILED, no DHIS2 mutation |
| invalid Data Element | FAILED, no aggregation/import |
| invalid weekly/monthly Data Set match | FAILED |
| dry-run | report produced, no mutation |
| all values new | CREATE counts correct |
| existing changed values | UPDATE counts correct |
| existing equal values | UNCHANGED/SKIP correct |
| transient OCS 503 | retry |
| permanent OCS dataset missing | no retry |
| import success + Analytics success | END_TO_END_SUCCESS |
| import success + Analytics timeout | PARTIAL |
| Analytics verify-only eventually ready | END_TO_END_SUCCESS |
| duplicate scheduled execution | SKIPPED or locked |
| resume Analytics failure | no second import |
| secret in exception payload | redacted in log |
| scheduler restart | schedule history preserved |

---

# 26. Concrete implementation order for Codex

Follow this order.

## Batch 1 — protect baseline

- [ ] inspect repository;
- [ ] map existing modules;
- [ ] add regression tests;
- [ ] no behavior changes.

## Batch 2 — jobs/state

- [ ] Job model;
- [ ] step model;
- [ ] SQLite migrations;
- [ ] job repository;
- [ ] tests.

## Batch 3 — runner

- [ ] typed PipelineContext;
- [ ] PipelineRunner;
- [ ] adapt existing functions into steps;
- [ ] `run-pipeline`;
- [ ] parity tests with current commands.

## Batch 4 — logging

- [ ] central bootstrap;
- [ ] rotating application file;
- [ ] per-job logs;
- [ ] contextual fields;
- [ ] secret redaction.

## Batch 5 — reports

- [ ] job artifact directory;
- [ ] summary.md;
- [ ] summary.json;
- [ ] import-plan.csv;
- [ ] validation.json;
- [ ] provenance.json;
- [ ] artifact DB records.

## Batch 6 — Analytics

- [ ] typed Analytics config;
- [ ] backward compatibility with `trigger_analytics`;
- [ ] trigger adapter;
- [ ] wait/poll;
- [ ] verification;
- [ ] partial/end-to-end status rules;
- [ ] tests.

## Batch 7 — recovery

- [ ] error taxonomy;
- [ ] retry classifier;
- [ ] retry persistence;
- [ ] resume rules;
- [ ] `resume-job`.

## Batch 8 — idempotency + locks

- [ ] run key;
- [ ] duplicate successful-run detection;
- [ ] pipeline locks;
- [ ] stale-lock logic;
- [ ] concurrency tests.

## Batch 9 — scheduler

- [ ] scheduler abstraction;
- [ ] YAML schema;
- [ ] schedule persistence;
- [ ] cron timezone;
- [ ] due-run evaluation;
- [ ] no-overlap;
- [ ] retry/backoff;
- [ ] CLI commands.

## Batch 10 — operations CLI

- [ ] `jobs list`;
- [ ] `jobs show`;
- [ ] `health`;
- [ ] report links/paths;
- [ ] documentation.

## Batch 11 — production hardening

- [ ] systemd/Docker examples;
- [ ] shutdown handling;
- [ ] lock cleanup;
- [ ] heartbeat;
- [ ] retention design;
- [ ] integration test against test DHIS2.

## Batch 12 — only later

- [ ] REST API;
- [ ] DHIS2 Climate Operations UI;
- [ ] availability visualization after design approval.

---

# 27. PR / commit boundaries

Recommended coherent commits:

```text
test: freeze current climate import behavior

feat(jobs): add persisted job and step model

refactor(pipeline): introduce reusable sequential pipeline runner

feat(logging): add rotating and per-job logs

feat(reports): add structured job artifacts

feat(analytics): add trigger-and-verify end-to-end stage

feat(recovery): add retry and resume semantics

feat(runtime): add idempotency and pipeline locks

feat(scheduler): add persistent scheduled pipeline execution

feat(cli): add operational job and health commands

docs: document end-to-end climate operations workflow
```

Avoid commits named:

```text
big refactor
changes
fix things
scheduler + ui + db + analytics
```

---

# 28. Definition of Done for the next milestone

The next major milestone is complete when the following works:

```bash
python -m dhis2_climate_import run-pipeline precipitation_daily --apply
```

and produces:

1. durable job ID;
2. preflight result;
3. deterministic period window;
4. OCS aggregation;
5. validation;
6. import plan;
7. DHIS2 CREATE/UPDATE/SKIP result;
8. Analytics generation according to configured mode;
9. Analytics verification;
10. final end-to-end status;
11. application log;
12. per-job log;
13. Markdown summary report;
14. JSON summary report;
15. provenance artifact;
16. persisted SQLite history.

Then this must also work without changing business logic:

```bash
python -m dhis2_climate_import scheduler start
```

with a configured schedule causing the same `PipelineRunner` to execute.

A scheduled success should be inspectable later using:

```bash
python -m dhis2_climate_import jobs show <job_id>
```

and the result must clearly distinguish:

```text
IMPORT SUCCESS
```

from:

```text
END_TO_END_SUCCESS
```

---

# 29. Example final configuration target

This is illustrative. Preserve current keys during migration.

```yaml
runtime:
  timezone: Africa/Kinshasa

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

open_climate_service:
  aggregate_process_id: aggregate_to_dhis2_json
  publish: true

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
  mode: trigger_and_verify
  wait_for_completion: true
  timeout_seconds: 3600
  poll_interval_seconds: 10
  last_years: 2
  verification:
    mode: sampled
    sample_size: 20

scheduler:
  enabled: true
  misfire_grace_seconds: 3600

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
      overlap_policy: skip
      retry:
        max_attempts: 3
        backoff_seconds: [60, 300, 900]

imports:
  - id: precipitation_daily
    dataset_id: chirps3_precipitation_daily
    data_element: CCH_PRECIP_CHIRPS_DAILY
    period_type: day
    method: mean
    expected_units: mm

  - id: temperature_daily
    dataset_id: era5land_temperature_daily
    data_element: CCH_TEMP_ERA5_DAILY
    period_type: day
    method: mean
    expected_units: celsius
```

---

# 30. Final instruction to Codex

When implementing this plan, optimize for:

```text
correctness
reproducibility
observability
safe reruns
backward compatibility
clear operational evidence
```

not for maximum feature count.

The strongest version of this project is not the one with the most commands.

It is the one where a system administrator can answer, for every climate import:

```text
What ran?
Why did it run?
Which source/workflow produced the values?
Which periods and OrgUnits were processed?
What was created?
What was updated?
What was skipped?
Did DHIS2 accept the values?
Did Analytics become ready?
Can I reproduce the run?
Can I safely retry it?
Where is the report?
Where is the log?
```

If the software can answer all of those reliably, it has evolved from a working import script into an operational DHIS2 climate-data integration service.
