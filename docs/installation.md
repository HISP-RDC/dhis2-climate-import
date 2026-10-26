# Installation

## Requirements

- Python 3.12 or later;
- Git, curl, CA certificates, make, and jq;
- `uv` for Python and locked dependency installation;
- a DHIS2 service account with the required authorities.

On Ubuntu or Debian:

```bash
sudo apt update
sudo apt install -y git curl ca-certificates make jq
```

Install `uv` with its official installer:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
```

## Local installation

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

Verify local components without remote access:

```bash
.venv/bin/dhis2-climate-import --help
make check
```

Verify target connectivity:

```bash
.venv/bin/dhis2-climate-import --env-file .env check-dhis2
```

## Server installation

The supplied systemd units use a dedicated operating-system account named `dhis2-import` and the
path `/opt/dhis2-climate-import`. Create that account once. If your organization requires another
account name, update `User`, `Group`, ownership commands, and documentation consistently.

```bash
sudo useradd --system --user-group \
  --home-dir /opt/dhis2-climate-import \
  --shell /usr/sbin/nologin \
  dhis2-import

sudo install -d -o dhis2-import -g dhis2-import /opt/dhis2-climate-import
sudo -u dhis2-import git clone \
  https://github.com/HISP-RDC/dhis2-climate-import.git \
  /opt/dhis2-climate-import

sudo -u dhis2-import bash -lc '
  cd /opt/dhis2-climate-import
  uv python install 3.12
  uv sync --locked --python 3.12
  cp .env.example .env
  cp config/import-pipeline.yaml.example config/import-pipeline.yaml
  cp config/instances.yaml.example config/instances.yaml
  cp config/scenarios/dhis2-x-to-y.yaml.example \
    config/scenarios/dhis2-x-to-y.yaml
  chmod 600 .env
'
```

Edit `/opt/dhis2-climate-import/.env` without exposing secrets in shell history or logs. See
[configuration.md](configuration.md) for all settings.

## Install systemd units

### DHIS2 transfers

```bash
sudo install -m 0644 deploy/systemd/dhis2-transfer@.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/dhis2-transfer@.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

Start a manual run without blocking the terminal:

```bash
sudo systemctl start --no-block dhis2-transfer@hospital_service.service
sudo journalctl -fu dhis2-transfer@hospital_service.service
```

### Climate pipeline

OCS must be installed separately. Once OCS is healthy:

```bash
sudo install -m 0644 deploy/systemd/dhis2-climate-pipeline.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/dhis2-climate-pipeline.timer /etc/systemd/system/
sudo install -m 0600 \
  deploy/systemd/dhis2-climate-pipeline.env.example \
  /etc/default/dhis2-climate-pipeline

sudo systemctl daemon-reload
sudo systemctl start --no-block dhis2-climate-pipeline.service
sudo journalctl -fu dhis2-climate-pipeline.service
```

See [weekly-climate-ocs.md](weekly-climate-ocs.md) for the full OCS deployment.

## Updating a server checkout

```bash
cd /opt/dhis2-climate-import
sudo -u dhis2-import git pull --ff-only
sudo -u dhis2-import uv sync --locked --python 3.12
sudo -u dhis2-import make check
sudo systemctl daemon-reload
```

Do not run `uv lock` on the server. Update and test `uv.lock` deliberately in development, then
commit it.

## Uninstalling

Stop the relevant timers and services first. Preserve secured backups of `.env`, `state/`,
`reports/`, and `cache/` if audit history must be retained. Deleting these directories removes
restart checkpoints and transfer evidence.
