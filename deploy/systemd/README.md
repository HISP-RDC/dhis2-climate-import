# systemd deployment

## Supplied units

| File | Purpose |
| --- | --- |
| `dhis2-transfer@.service` | run one named transfer job |
| `dhis2-transfer@.timer` | schedule one independent transfer job |
| `dhis2-transfer-queue.service` | run a list of transfer jobs sequentially |
| `dhis2-transfer-queue.timer` | schedule the sequential transfer queue |
| `dhis2-climate-pipeline.service` | run the OCS-to-DHIS2 pipeline |
| `dhis2-climate-pipeline.timer` | schedule the weekly climate pipeline |
| `open-climate-service.service` | run the local OCS process |

## Service account

The supplied units use the dedicated operating-system account `dhis2-import` and the paths
`/opt/dhis2-climate-import` and `/opt/open-climate-service`. Create the account as documented in
[Installation](../../docs/installation.md). If another account is required, change `User`, `Group`,
directory ownership, and all `sudo -u` commands consistently.

Files under `/etc/default` do not contain DHIS2 credentials. Secrets remain in
`/opt/dhis2-climate-import/.env` and `/opt/open-climate-service/.env` with mode `0600`.

## Transfer scheduling modes

- `dhis2-transfer@<job>.timer`: independent jobs, with concurrency bounded by the scenario;
- `dhis2-transfer-queue.timer`: strictly sequential jobs.

Do not enable both modes for the same jobs.

## Reload and verify

```bash
sudo systemctl daemon-reload
sudo systemd-analyze verify /etc/systemd/system/dhis2-transfer@.service
sudo systemd-analyze verify /etc/systemd/system/dhis2-climate-pipeline.service
```

See [Installation](../../docs/installation.md) and the
[Operations runbook](../../docs/operations.md).
