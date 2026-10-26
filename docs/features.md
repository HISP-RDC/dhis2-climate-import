# DHIS2 X-to-Y transfer guarantees

## Purpose

This document defines the implemented guarantees for transferring aggregate Data Sets between two
DHIS2 instances. It covers long historical periods, bounded memory, recovery, metadata safety,
organisation-unit reconciliation, audit artifacts, and concurrent execution.

The architecture described here is implemented and covered by unit tests. Integration against a
real target remains explicit: run `reconcile-metadata --dry-run`, review its plan, and apply only on
an approved target.

## Safety principles

1. Temporary network failures cannot be eliminated, so they must be recoverable and auditable.
2. Long periods must not require all values to remain in memory at once.
3. A confirmed window or chunk must not be repeated during normal recovery.
4. Metadata must be checked before a large value export.
5. An unambiguous missing target object may be created from the source.
6. An incompatible existing target object must never be overwritten without a safe proof.
7. Value import starts only after target metadata compatibility is established.
8. Credentials remain in `.env` and never appear in YAML, reports, or logs.
9. Concurrent metadata changes against one target are serialized.
10. SQLite journals and JSON reports must be sufficient to explain the final state.

## Implemented capabilities

The transfer pipeline can:

- load several instance profiles from `config/instances.yaml`;
- resolve credentials from environment variables;
- use PAT or Basic authentication independently for source and target;
- discover the source Data Set and its linked Data Elements;
- compare source and target organisation units by UID or code;
- plan and create a missing parent hierarchy in parent-first order;
- create a missing target Data Set with its minimal dependency closure;
- reuse compatible existing dependencies without updating them implicitly;
- include Option Sets and Options referenced by Data Elements;
- block incompatible existing Data Elements;
- perform an explicitly configured narrow metadata repair;
- batch source requests by DHIS2 period and organisation unit;
- subdivide a request after a timeout or gateway failure;
- dry-run target values before a real import;
- emit timestamped progress suitable for journald;
- record global and window results in SQLite and JSON;
- execute bounded period windows in separate processes;
- resume without replaying successful windows or chunks;
- classify permanent exit code `2` and temporary exit code `75`;
- retry temporary failures with bounded exponential backoff;
- preserve historical category semantics through versioned Data Elements;
- quarantine values that are empty, inactive for their organisation-unit period, or linked to an
  safely isolated historical conflict.

## Strict boundaries

The following cases remain blocking by design:

- ambiguous UID or code collisions;
- incompatible value or domain types;
- a current category structure that differs while target data already exists;
- unsafe repair of an object used by another Data Set;
- an existing organisation unit under a different parent, unless its whole source subtree is
  explicitly quarantined by policy;
- any condition where excluding values would hide a different DHIS2 conflict;
- quarantine of every value in an execution window.

Payloads are retained only when `--keep-payload` is requested. Start every new integration on a
small period and a non-production target.

## Two levels of batching

### Source request batching

`source_period_batch_size` controls the number of DHIS2 periods in one source `dataValueSets`
request:

```yaml
transfer:
  source_period_batch_size: 1
```

Organisation units are batched separately with `organisation_units.batch_size`. A timed-out request
can be subdivided again by organisation unit.

This protects DHIS2, the reverse proxy, and Cloudflare, but it does not bound the memory of the
complete execution by itself.

### Execution-window batching

`transfer.execution.period_batch_size` bounds the complete export, validation, and import window:

```yaml
jobs:
  - id: hospital_service
    source_data_set: OeWrFwkFMvf
    target_data_set: OeWrFwkFMvf
    period_type: month
    default_start_period: 2017-01
    default_end_period: 2019-12
    transfer:
      source_period_batch_size: 1
      execution:
        period_batch_size: 3
        process_isolation: true
        resume: true
        resume_chunks: true
        stop_on_error: true
```

The batch size uses the job's period type:

- `3` means three months for a monthly job;
- `13` means thirteen weeks for a weekly job;
- `31` means thirty-one days for a daily job;
- `1` means one year for a yearly job.

A shorter range produces one window. A larger range is split until the configured end period, and
the final window may be smaller.

## Process isolation and memory recovery

Deleting Python objects does not guarantee that reserved memory is immediately returned to Linux.
The parent process therefore starts one subprocess per window.

The parent process:

1. resolves the complete period;
2. builds the ordered window list;
3. creates the global journal entry;
4. starts the first unfinished window;
5. forwards its logs;
6. records its result;
7. waits for the subprocess to exit;
8. continues with the next window.

The worker process:

1. loads one window;
2. exports and deduplicates its values;
3. validates all target chunks;
4. imports chunks when `--apply` is active;
5. writes its report;
6. exits and releases its memory.

With a three-month window, 36 monthly periods become 12 independent subprocesses.

## Checkpoints and recovery

Each window fingerprint includes:

- scenario and job identifiers;
- source and target instances;
- source and target Data Sets;
- period bounds;
- mapping strategies;
- configuration hash;
- dry-run or apply mode.

A successful window is skipped during normal recovery. A failed window follows the retry policy. A
window left in `running` after an abrupt stop is marked interrupted before it is retried.

With `resume_chunks: true`, every confirmed target import response also creates a content
checkpoint under:

```text
state/<target>/<scenario>/jobs/<job>/checkpoints/data-values/
```

An identical confirmed chunk is skipped even if the process failed before completing its window.
A `dryRun=false` request that timed out is not considered confirmed because the outcome is
ambiguous. It is replayed safely with `CREATE_AND_UPDATE`.

The `--force` option remains available for controlled manual use and is never added automatically
to a systemd service.

## Error classification and retry

Temporary failures include:

- network timeout or disconnect;
- HTTP 429;
- HTTP 502, 503, or 504;
- Cloudflare 520, 522, or 524;
- temporary DNS resolution failure.

Example retry policy:

```yaml
transfer:
  retry:
    attempts: 5
    initial_delay_seconds: 5
    max_delay_seconds: 120
```

Target value requests also support adaptive splitting:

```yaml
transfer:
  chunk_size: 5000
  adaptive_chunking:
    enabled: true
    minimum_chunk_size: 500
    retry_before_split: 1
    backoff_seconds: 10
  execution:
    resume_chunks: true
```

After local retries, only the failing chunk is halved, for example
`5000 → 2500 → 1250 → 625`, until the minimum size. Functional DHIS2 conflicts are never hidden by
this mechanism.

Permanent failures include invalid configuration, permission errors, structural metadata
conflicts, unsupported period types, and incompatible target values. They stop with exit code `2`.

## Metadata synchronization

Metadata synchronization is a preliminary phase, not an improvised response after a value failure:

```yaml
target:
  metadata:
    mode: create_if_missing
    identity_strategy: preserve_uid
    reconcile_existing: true
    synchronization:
      enabled: true
      create_missing_objects: true
      attach_compatible_data_elements: true
      incompatible:
        policy: safe_repair
        require_no_data_values: true
        require_no_other_dataset_memberships: true
        require_same_identity: true
      historical_data_elements:
        enabled: true
        strategy: versioned
        uid_collision_strategy: clone
        name_suffix: " — Historical"
```

Before a large value export, the pipeline:

1. retrieves the source Data Set and Data Elements;
2. inventories corresponding target objects;
3. generates a minimal create, attach, or repair plan;
4. stores snapshots of affected target objects;
5. validates the package through the DHIS2 metadata importer;
6. stops on every unapproved conflict;
7. commits only with `--apply`;
8. reads the target metadata again;
9. repeats the compatibility comparison;
10. starts value windows only after compatibility is proven.

Commands:

```bash
.venv/bin/dhis2-climate-import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --dry-run

.venv/bin/dhis2-climate-import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --apply
```

## Organisation-unit synchronization

Organisation-unit synchronization is separate from Data Element reconciliation and requires UID
mapping:

```yaml
target:
  organisation_units:
    discovery: linked_to_dataset
    mapping_strategy: uid
    synchronization:
      enabled: true
      run_before_transfer: true
      conflict_policy: exclude
      create_missing: true
      require_same_name: true
      require_same_code: false
      allow_root_creation: false
      attach_to_target_data_set: true
      preserve_geometry: true
```

The command reads every selected source path, loads all ancestors, and creates only missing UIDs in
parent-first order. It never renames, moves, or overwrites an existing target organisation unit.
Root creation is disabled by default.

```bash
.venv/bin/dhis2-climate-import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units relevant_epidemiological_data --dry-run
```

With `conflict_policy: exclude`, an existing UID with a required identity or parent mismatch becomes
the root of a dynamically quarantined source subtree. That subtree is not queried from the source,
created on the target, attached to the Data Set, or included in value payloads. The exclusion and
its reason remain in the audit artifacts. The pipeline blocks if quarantine would remove the whole
selection.

## Value quarantine

### Inactive organisation units (`E8031`)

If DHIS2 rejects values because an organisation unit is inactive for the value period, the pipeline
uses the exact response indexes to isolate those values and revalidates the remainder. It never
changes target opening or closing dates. If the same response includes another conflict code, the
whole chunk remains blocking.

### Missing or empty source values (`E8120`)

Before target validation, the pipeline removes only rows whose `value` is missing, `null`, or an
empty string after trimming. Numeric zero is preserved. The quarantine artifact records the key and
reason for each removed row.

### Historical category values

When old values reference an obsolete category combination, the pipeline can map an exact current
equivalent or create a versioned historical Data Element and cloned category graph. Ambiguous or
colliding semantics remain blocking.

## Audit artifacts

Each job keeps isolated state under:

```text
state/<target>/<scenario>/jobs/<job>/
├── artifacts/
├── cache/
├── checkpoints/
├── reports/
├── runs/
└── transfer-runs.sqlite3
```

Reports include source and target instances, Data Sets, period bounds, counts, metadata actions,
organisation-unit exclusions, quarantine totals, chunk retry statistics, and final status.

Useful statuses include:

- `planned`;
- `running`;
- `success`;
- `success_with_quarantine`;
- `failed`;
- `interrupted`;
- `metadata_changes_required`;
- `organisation_unit_changes_required`.

## Concurrent execution

The supplied template service runs one named job:

```bash
sudo systemctl start --no-block dhis2-transfer@hospital_service.service
sudo systemctl start --no-block dhis2-transfer@service_base.service
```

The scenario-level value-transfer limit is shared by every job:

```yaml
execution:
  max_parallel_jobs: 3
```

Additional jobs wait for a slot instead of failing. Metadata changes remain protected by a separate
exclusive lock. Use either independent timers or the sequential queue for a given set of jobs, not
both.

## Definition of success

The design is successful when a long historical transfer can run unattended, release memory
between bounded windows, survive temporary network failures, avoid replaying confirmed work,
preserve target metadata integrity, and provide enough evidence to explain every creation,
quarantine, retry, checkpoint, and imported value count.
