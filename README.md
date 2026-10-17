# DHIS2 Climate Import

Pipeline Python pour automatiser les imports de donnees climat vers DHIS2 Climate App.

Le projet garde Open Climate Service comme moteur climat/geospatial et utilise
`dhis2-python-client` comme transport DHIS2 principal. Il fournit une couche fiable pour DHIS2 :
cache des unites d'organisation niveau 3, cache des elements de donnees, agregation, import
`dataValueSets`, verification analytics, journal SQLite et rapports.

## Installation sur un serveur Linux

Le projet exige Python 3.12 ou une version plus recente. Les dependances sont verrouillees dans
`uv.lock`; sur un serveur, utilisez ce fichier sans le regenerer.

Les commandes suivantes conviennent a Ubuntu 24.04 et aux distributions Debian recentes. Elles
sont a executer avec un utilisateur Linux dedie au pipeline, pas avec `root` :

```sh
sudo apt update
sudo apt install -y git curl ca-certificates make
```

Installez ensuite `uv` avec l'installateur officiel et rendez la commande accessible dans le shell
courant :

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
```

Depuis la racine du depot clone, installez Python 3.12 et toutes les dependances verrouillees. `uv`
cree automatiquement l'environnement virtuel local `.venv` :

```sh
cd /chemin/vers/dhis2-climate-import
uv python install 3.12
make install
```

La commande equivalente, sans `make`, est :

```sh
uv sync --locked --python 3.12
```

Verifiez ensuite l'installation locale, sans contacter ni modifier DHIS2 :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --help
make test
```

### Configuration des instances DHIS2

Le fichier `.env` contient les URL et les identifiants. Il est ignore par Git et doit rester
lisible uniquement par l'utilisateur qui execute le pipeline :

```sh
test -f .env || cp .env.example .env
chmod 600 .env
nano .env
```

Pour un transfert X vers Y, renseignez au minimum les variables du mode d'authentification utilise :

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

`config/instances.yaml` indique, pour chaque profil, si l'authentification utilise `basic`, `pat`
ou `auto`. Ce YAML ne doit contenir que les noms des variables d'environnement, jamais les mots de
passe ou les tokens. Si les fichiers de configuration ne sont pas deja presents dans votre clone :

```sh
test -f config/instances.yaml || \
  cp config/instances.yaml.example config/instances.yaml
test -f config/scenarios/dhis2-x-to-y.yaml || \
  cp config/scenarios/dhis2-x-to-y.yaml.example config/scenarios/dhis2-x-to-y.yaml
```

La premiere verification X vers Y est en lecture seule :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  review hospital_service
```

Si cette commande signale des unités d’organisation absentes sur Y, validez puis appliquez leur
hiérarchie avant de relancer `review` :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units hospital_service --dry-run

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units hospital_service --apply
```

La commande crée seulement les UID absents, dans l’ordre parent-enfant. Elle bloque si une unité
existante possède un code, un nom ou un parent différent et ne déplace jamais une branche de Y.

Validez d'abord les métadonnées puis les données avant le premier import réel :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --dry-run
```

Si le plan annonce des changements, relisez son rapport puis appliquez uniquement les métadonnées :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata hospital_service --apply
```

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  run hospital_service --dry-run
```

`--apply` est reserve a l'import confirme vers l'instance cible.

### Mise a jour apres un `git pull`

Apres chaque mise a jour du code, resynchronisez les dependances verrouillees puis executez les
tests locaux :

```sh
git pull --ff-only
make install
make test
```

N'utilisez pas `uv lock` sur le serveur : cette commande est reservee au developpement lorsque les
dependances du projet doivent volontairement etre mises a jour.

### Open Climate Service (optionnel)

Open Climate Service n'est pas necessaire pour un simple transfert DHIS2 X vers Y. Il est requis
uniquement pour les workflows qui calculent ou recuperent des donnees climat avant de les importer.
Il s'execute comme un service separe, depuis son depot officiel :

```sh
git clone https://github.com/dhis2/open-climate-service.git
cd open-climate-service
make sync
cp climate-service.yaml.example climate-service.yaml
cp .env.example .env
make run
```

The official quick start starts OCS on `http://127.0.0.1:9000`, so this project's `.env` should use:

```env
CLIMATE_SERVICE_BASE_URL=http://127.0.0.1:9000
```

Ordre recommande sur une instance DHIS2 non-production :

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import check-dhis2
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 3
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-data-elements --data-set CCH_CLIMATE_WEATHER
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 precipitation_daily --dry-run
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 precipitation_daily --apply
```

## Safety Defaults

- `.env`, `cache/`, `data/`, `reports/` et les journaux SQLite sont ignores par Git.
- `DHIS2_TRANSPORT=dhis2-client` active le client Python DHIS2 officiel; `stdlib` reste un fallback.
- `DHIS2_AUTH_METHOD=basic` est disponible si l'instance de test rejette les PAT.
- L'import reel demande `--apply`; `--dry-run` est prevu pour chaque validation initiale.
- Les imports lisent le cache org units du niveau selectionne et refusent de continuer si le
  cache est absent, expire ou contient des geometries manquantes.
- Les data elements sont resolus depuis le cache local `cache/data-elements.json`.
- Un import deja reussi avec le meme dataset, la meme periode, le meme data element et le meme
  hash de cache org units n'est pas relance sans `--force`.

## DHIS2-to-DHIS2 Data Set Transfer

The transfer workflow discovers every Data Element linked to a configured source Data Set. It can
validate an existing target Data Set or bootstrap a missing target from the official DHIS2 metadata
dependency export before importing the data values.

Every source organisation unit involved in the transfer must exist on the target according to the
configured `uid` or `code` mapping. Missing or ambiguous matches block the normal transfer. With UID
mapping, the separate `reconcile-org-units` command can validate and create only missing units and
their missing ancestors. It never moves or overwrites an existing target unit. Additional target
units are allowed.

Set `target.organisation_units.synchronization.run_before_transfer: true` after approving the
first hierarchy dry-run to execute the same guarded reconciliation before every parent transfer.
It also restores missing DataSet–organisation-unit assignments before values are exported. A final
preflight blocks the import locally if DHIS2 would reject it with `E8022`.

`target.organisation_units.synchronization.conflict_policy` controls hierarchy conflicts for every
job. Its default, `block`, preserves strict fail-fast behavior. `exclude` dynamically detects
conflicting source UIDs (required name/code differences, parent differences and code collisions),
quarantines only the selected source subtrees that contain them, and continues with the remaining
units. It requires synchronization, automatic pre-transfer reconciliation and UID mapping. The
quarantine is refreshed before each parent transfer, reused by isolated windows, recorded in JSON,
and included in the checkpoint fingerprint. Structural errors and a quarantine that would empty the
complete source selection still block execution. `jobs[].organisation_units.exclude` remains
available for an explicit manual quarantine with a mandatory reason.

With `exclude`, target validation also quarantines only values rejected with DHIS2 `E8031`
(organisation unit inactive for that period), revalidates the remaining values, and records the
exact `OU + period` rows under `runs/<transfer-run>/data-value-quarantine/e8031.json`. It does not
alter target opening/closing dates, and any mixed conflict remains blocking.

Prepare profiles and a scenario:

```sh
cp config/instances.yaml.example config/instances.yaml
cp config/scenarios/dhis2-x-to-y.yaml.example config/scenarios/dhis2-x-to-y.yaml
```

The instance file references environment variable names only. Each profile supports `pat`, `basic`,
or `auto` authentication; passwords and tokens stay in `.env`.

Review, validate, then apply:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  review weekly_climate

# Manual audit (recommended before enabling run_before_transfer):
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units weekly_climate --dry-run

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-org-units weekly_climate --apply

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  reconcile-metadata weekly_climate --dry-run

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  run weekly_climate --dry-run

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import transfer \
  --instances config/instances.yaml \
  --scenario config/scenarios/dhis2-x-to-y.yaml \
  run weekly_climate --apply
```

With `target.metadata.mode: create_if_missing`, `reconcile-metadata --dry-run` exports the Data Set
dependency package from X and validates it against Y without writing metadata. After review,
`reconcile-metadata --apply` creates missing objects, attaches compatible Data Elements and rereads
Y before any large data export. `transfer run` also executes this preflight automatically. When
`run_before_transfer` is enabled, it then audits/creates missing organisation units and completes
the target DataSet assignments under the same metadata lock. Caches,
artifacts, reports, and the SQLite journal are isolated per job under
`state/<target-profile>/<scenario>/jobs/<job>/`.

`transfer run` splits the requested range according to `transfer.execution.period_batch_size`.
Each window runs in a child process by default, so Linux reclaims its memory before the next one.
Successful windows are checkpointed in SQLite and skipped on restart unless `--force` is supplied.
Transient network errors are retried with exponential backoff; permanent metadata or permission
errors stop immediately.

When `import.strategy: CREATE` finds dependencies that already exist on the target with the same
UID, the transfer reuses them instead of updating them. The original source package and the final
minimal package are both kept as artifacts. Invalid stale category-option-combo references and
incompatible compulsory operands are removed from the import package and recorded in the report.

If a target contains partial copies of the source metadata (same Data Element UIDs and labels, but
the default CategoryCombo instead of the source CategoryCombo), enable
`target.metadata.reconcile_existing` only after a read-only audit. The guarded reconciliation
entry point is:

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

The lower-level `repair-metadata` command remains available for auditing the narrowly supported
legacy case; incompatible objects containing values are never overwritten automatically.

The repair accepts only matching UIDs and identity fields, empty target CategoryCombos, Data
Elements with no target Data Set membership, and no raw or deleted target values between 1900 and
2100 under the target account's organisation-unit roots. It first updates CategoryCombo structure,
then validates and commits CategoryOptionCombos and Data Element assignments. A dry-run validates
the first stage without mutation; the second stage necessarily remains pending until the first
stage has been committed. Source, structure, assignment, rollback-evidence, and report artifacts
are retained under the scenario state directory.

With `identity_strategy: preserve_uid`, target discovery first uses `target_data_set`, then falls
back to the source Data Set UID. This prevents an existing target object without a code from being
mistaken for missing metadata.

Long source exports have two limits. `transfer.execution.period_batch_size` bounds one complete
child execution (defaults: 31 days, 13 weeks, 3 months or 1 year), while
`transfer.source_period_batch_size` bounds one request to X (defaults: 31 days, 13 weeks, 1 month
or 1 year). Organisation units are also batched with `organisation_units.batch_size`; a timeout or
gateway response subdivides the failing request until it succeeds or reaches one period and one
unit. `transfer run` prints timestamped progress to stderr and keeps the final JSON summary on
stdout.

Historical source values whose old CategoryOptionCombo has an exact equivalent are remapped by
their complete CategoryOption UID set. When no lossless equivalent exists, the optional
`historical_data_elements.strategy: versioned` policy creates a deterministic historical Data
Element on Y, preserves the old CategoryCombo semantics and attaches that version to the target
Data Set. With `uid_collision_strategy: clone`, an incompatible historical UID already present on
Y is never overwritten: the complete historical CategoryOption/Category/CategoryCombo graph is
cloned under deterministic UIDs and the transferred historical values are rewritten to the cloned
CategoryOptionCombos. Current Data Element collisions and incompatible deterministic clone UIDs
still block the transfer.

### systemd multi-jobs

Templates are provided in `deploy/systemd/`. Adapt `User`, `Group` and `/opt` paths if needed, then:

```sh
sudo cp deploy/systemd/dhis2-transfer@.service /etc/systemd/system/
sudo cp deploy/systemd/dhis2-transfer@.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dhis2-transfer@hospital_service.timer
sudo systemctl enable --now dhis2-transfer@sigl_fosa.timer
```

Follow one job with:

```sh
sudo journalctl -fu dhis2-transfer@hospital_service.service
```

The service never uses `--force`. Exit code `2` is permanent; exit code `75` is transient and can
be restarted by systemd after the pipeline's own retries. A per-job lock, a target metadata lock
and `execution.max_parallel_jobs` prevent unsafe concurrency.

## Commands

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import check-dhis2
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 3 --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-data-elements --data-set CCH_CLIMATE_WEATHER --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import ingest-dataset precipitation_daily
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import sync-dataset precipitation_daily --plan
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import aggregate precipitation_daily
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import temperature_max_weekly_era5_land --org-unit-level 2 --data-set CCH_WEEKLY
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import temperature_max_weekly_era5_land --years-back 10 --org-unit-level 2 --data-set CCH_WEEKLY
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 precipitation_daily --dry-run
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import verify-analytics precipitation_daily --period 20240101
```

## Dynamic Period Windows

Every command with a date window accepts either explicit bounds or a relative window:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import temperature_max_weekly_era5_land --years-back 10 --org-unit-level 2 --data-set CCH_WEEKLY
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import import-dhis2 temperature_max_weekly_era5_land --years-back 7 --org-unit-level 2 --data-set CCH_WEEKLY --dry-run
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import ingest-dataset temperature_max_weekly_era5_land --years-back 8
```

`--years-back` uses complete DHIS2 periods by default. With today's date as anchor, a weekly job
ends on the latest completed ISO week; a monthly job ends on the previous complete month. Use
`--as-of YYYY-MM-DD` for reproducible runs, or `--end` / `--end-period` to anchor the relative
window yourself.

Example matching the Climate App Zone de Sante weekly review:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 3 --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-data-elements --data-set CCH_CLIMATE_WEATHER --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import precipitation_weekly_era5_land --start 2026-03-21 --end 2026-08-21 --org-unit-level 3 --data-set CCH_CLIMATE_WEATHER --expected-org-units 519 --expected-data-values 11937
```

For a ten-year import at the same level:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import precipitation_weekly_era5_land --years-back 10 --org-unit-level 3 --data-set CCH_CLIMATE_WEATHER
```

## Climate App Weekly Review Example

The job `temperature_max_weekly_era5_land` models the Climate App review. The org unit level is
runtime-driven: Province/DPS is only one execution choice, not a hardcoded rule.

- source data: `Max air temperature (ERA5-Land)`
- DHIS2 data element code: `ERA5_LAND_TEMPERATURE_MAX`
- period range: `2026-W09` to `2026-W31`, resolved to `2026-02-23` through `2026-08-02`
- org unit level: `2` for Province/DPS in the DRC test hierarchy
- expected values: `26 org units x 23 weeks = 598 data values`

The job uses `data_set: CCH_CLIMATE_WEATHER` because that is the code currently visible in the
local configuration/error trace. If the actual weekly data set code is `CCH_WEEKLY` or another
UID/code, either update `data_set` in `config/import-pipeline.yaml` or pass `--data-set` at runtime.

Prepare the matching cache before the review/import:

```sh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-org-units --level 2 --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import cache-data-elements --data-set CCH_WEEKLY --refresh
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import review-import temperature_max_weekly_era5_land --org-unit-level 2 --data-set CCH_WEEKLY --expected-org-units 26 --expected-data-values 598
```

For another level, refresh the matching cache and change only `--org-unit-level`. The review
computes `computedDataValues = org units with geometry x periods`; `--expected-*` flags are strict
controls for a known scenario.

The OCS dataset id `era5land_temperature_max_weekly` is intentionally a custom workflow placeholder.
Open Climate Service built-ins currently document mean ERA5-Land temperature datasets, so the custom
OCS workflow must reproduce the Climate App max-temperature calculation before applying the import.

Voir aussi `docs/operations.md` et `docs/official-sources.md`.
