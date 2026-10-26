# Architecture

## Project responsibility

`dhis2-climate-import` is an integration layer. It does not replace DHIS2, Open Climate Service,
or an analytics platform. It makes configured discovery, aggregation, validation, import, and
audit operations reproducible.

## OCS to DHIS2 flow

```text
Provider dataset
      │
      ▼
OCS ingestion/sync ──► published dataset ──► aggregation over DHIS2 geometries
                                                   │
                                                   ▼
                                         review → dry-run → apply
                                                   │
                                                   ▼
                                                 DHIS2
```

OCS tasks are asynchronous. The client submits a job, polls its state, and enforces a global job
timeout that is separate from the shorter HTTP request timeout.

## DHIS2 X to DHIS2 Y flow

```text
X: Data Set, dependencies, and values
          │
          ├── organisation unit audit
          ├── metadata preparation and validation
          ├── export by period and organisation-unit batch
          └── adaptive validation and import
                              │
                              ▼
                     Y: Data Set and values
```

Existing incompatible objects are not overwritten silently. Values that can be isolated without
breaking consistency are quarantined together with their reason and audit payload.

## Modules

| Module | Responsibility |
| --- | --- |
| `cli.py` | arguments, operator messages, and exit codes |
| `config.py` | climate import configuration |
| `transfer_config.py` | instance profiles and transfer scenarios |
| `dhis2.py` | DHIS2 Web API operations |
| `dhis2_transport.py` | official client adapter and fallback transport |
| `http.py` | JSON HTTP client and shared error classification |
| `ocs.py` | OCS job submission and polling |
| `pipeline.py` | climate aggregation, review, and import |
| `transfer.py` | metadata, values, and quarantine rules |
| `transfer_orchestrator.py` | isolated windows, retries, and parent summaries |
| `journal.py` | SQLite idempotency and execution history |
| `locks.py` | job, metadata, and concurrency locks |
| `cache.py` | organisation unit and Data Element caches |
| `reports.py` | report output and DHIS2 response normalization |

## Persistent state

```text
state/
└── <target>/<scenario>/jobs/<job>/
    ├── artifacts/       source and prepared packages
    ├── checkpoints/     confirmed value chunks
    ├── reports/         transfer summaries
    ├── runs/            plans, details, and quarantines by run
    └── transfer-runs.sqlite3
```

A checkpoint is written only after DHIS2 confirms a request. A response lost in transit is replayed
with `CREATE_AND_UPDATE`; this keeps replay safe without assuming that a missing response means the
server rejected the request.

## Reliability controls

- long date ranges are split into subprocess windows;
- process memory is released between windows;
- source exports are batched by period and organisation unit;
- temporary failures use bounded exponential backoff;
- only a failing DHIS2 POST is subdivided;
- identical confirmed chunks resume by fingerprint;
- metadata writes are serialized;
- concurrent value transfers use a configurable limit.

## Safety boundaries

- `.env` and runtime state directories are never versioned;
- remote writes require an explicit `--apply` option;
- logs must never expose credentials;
- a successful dry-run does not replace review of a metadata plan;
- existing organisation units are never moved automatically;
- structural incompatibilities remain blocking.

## Deployment

Units under `deploy/systemd/` call the same CLI behavior and add no business logic. The climate
pipeline is sequential on the writable OCS instance. X-to-Y value transfers may run concurrently
up to `execution.max_parallel_jobs`.
