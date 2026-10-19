# Import hebdomadaire OCS vers DHIS2

Le flux comporte deux étapes distinctes :

1. Open Climate Service (OCS) télécharge, transforme et publie le raster hebdomadaire.
2. `dhis2-climate-import` demande à OCS d’agréger ce raster sur les unités d’organisation, puis envoie les valeurs à DHIS2.

OCS n’envoie donc pas directement les valeurs dans DHIS2.

## Sources et authentification

- CHIRPS3 : aucune clé.
- MODIS NDVI/EVI : Microsoft Planetary Computer, aucune clé Google Earth Engine.
- ERA5-Land : `CDSAPI_KEY` ou `~/.cdsapirc`. `EDH_API_KEY`/`~/.netrc` est facultatif et accélère l’historique de la température moyenne et des précipitations.
- ERA5-HEAT UTCI : `CDSAPI_KEY` ou `~/.cdsapirc` obligatoire.

Dans le dépôt OCS, `climate-service.yaml` doit contenir :

```yaml
plugins_dir: ./plugins/
```

Après l’ajout ou la modification d’un plugin, redémarrer OCS :

```bash
cd /Users/ebedmeleck/MELECK-DEV/open-climate-service
make run
```

Vérifier ensuite le registre :

```bash
curl -sS http://127.0.0.1:9000/dataset-templates/chirps3_precipitation_weekly | jq '{id, period_type, units, ingestable}'
curl -sS http://127.0.0.1:9000/dataset-templates/modis_ndvi_weekly | jq '{id, period_type, units, ingestable}'
```

## Préparer DHIS2

Depuis `dhis2-climate-import` :

```bash
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --env-file .env check-dhis2

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --env-file .env \
  cache-org-units --level 3 --refresh

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --env-file .env \
  cache-data-elements --data-set mck6wRvBafz --refresh
```

## Ingestion OCS

Exemple limité à deux semaines, recommandé pour le premier essai :

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
  PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --env-file .env \
    ingest-dataset "${CLIMATE_JOB}" \
    --start-period "${CLIMATE_START_PERIOD}" \
    --end-period "${CLIMATE_END_PERIOD}" \
    --publish || break
done
```

Le `break` arrête volontairement la boucle au premier échec afin d’éviter de masquer une source ou une authentification mal configurée.

## Validation et import DHIS2

Valider d’abord un seul job :

```bash
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --env-file .env \
  review-import vegetation_ndvi_weekly \
  --start-period 2026-W39 --end-period 2026-W40

PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --env-file .env \
  import-dhis2 vegetation_ndvi_weekly \
  --start-period 2026-W39 --end-period 2026-W40 --dry-run
```

Après un dry-run accepté :

```bash
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --env-file .env \
  import-dhis2 vegetation_ndvi_weekly \
  --start-period 2026-W39 --end-period 2026-W40 --apply
```

Répéter ensuite l’import pour chaque job de `CLIMATE_JOBS`.

## Déploiement systemd sur le serveur

Le serveur exécute deux unités différentes :

1. `open-climate-service.service` reste actif et écoute uniquement sur `127.0.0.1:9000` ;
2. `dhis2-climate-pipeline.service` démarre à la demande, traite les jobs séquentiellement puis
   s’arrête. Son timer le déclenche chaque semaine.

Ne pas utiliser `make run` en production : cette commande active le rechargement automatique.
L’unité fournie démarre directement Uvicorn avec un seul worker, nécessaire pour ne pas avoir
plusieurs processus qui écrivent simultanément dans les stores OCS.

Même sur un serveur de 12 vCPU, ne pas remplacer `--workers 1` par `--workers 2` pour cette instance
OCS writable. La soumission atomique et le verrou du store sont encore locaux au processus OCS :
deux processus pourraient accepter deux écritures incompatibles sur le même store Icechunk. Le
worker unique n’interdit pas l’utilisation des autres CPU : OCS exécute les téléchargements
bloquants dans des threads et les calculs Xarray/Dask peuvent paralléliser le travail interne.

### 1. Installer OCS

Le dépôt déployé doit contenir les plugins DRC `plugins/datasets/drc_weekly.py` et
`plugins/datasets/drc_weekly.yaml` :

```bash
sudo install -d -o meleck -g meleck /opt/open-climate-service
sudo -u meleck git clone \
  <URL_DU_DEPOT_OCS_AVEC_PLUGINS_DRC> \
  /opt/open-climate-service

sudo -u meleck bash -lc '
  cd /opt/open-climate-service
  uv sync --all-extras
'

sudo mkdir -p /var/lib/open-climate-service
sudo chown meleck:meleck /var/lib/open-climate-service
```

Installer les exemples, puis renseigner la clé CDS sans l’afficher dans les journaux :

```bash
sudo install -o meleck -g meleck -m 0600 \
  /opt/dhis2-climate-import/deploy/systemd/open-climate-service.env.example \
  /opt/open-climate-service/.env

sudo install -o meleck -g meleck -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/open-climate-service.yaml.example \
  /opt/open-climate-service/climate-service.yaml

sudoedit /opt/open-climate-service/.env
```

L’exemple stocke les données hors du checkout Git dans `/var/lib/open-climate-service`. Ne pas
copier un store local pendant qu’une ingestion est active. Une migration de données existantes
doit aussi traiter les chemins d’artefacts enregistrés par OCS ; un simple `rsync` d’un store actif
n’est pas une migration sûre.

### 2. Installer et démarrer le service OCS

```bash
sudo install -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/open-climate-service.service \
  /etc/systemd/system/open-climate-service.service

sudo systemctl daemon-reload
sudo systemctl enable --now open-climate-service.service
sudo systemctl status open-climate-service.service
curl -sS http://127.0.0.1:9000/extent | jq .
```

Suivre OCS :

```bash
sudo journalctl -fu open-climate-service.service
```

### 3. Installer le pipeline climat

Vérifier d’abord `/opt/dhis2-climate-import/.env` :

```dotenv
CLIMATE_SERVICE_BASE_URL=http://127.0.0.1:9000
CLIMATE_SERVICE_HTTP_TIMEOUT_SECONDS=60
CLIMATE_SERVICE_POLL_SECONDS=5
CLIMATE_SERVICE_JOB_TIMEOUT_SECONDS=86400
```

Installer les unités et leur configuration :

```bash
sudo install -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/dhis2-climate-pipeline.service \
  /etc/systemd/system/dhis2-climate-pipeline.service

sudo install -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/dhis2-climate-pipeline.timer \
  /etc/systemd/system/dhis2-climate-pipeline.timer

sudo install -m 0644 \
  /opt/dhis2-climate-import/deploy/systemd/dhis2-climate-pipeline.env.example \
  /etc/default/dhis2-climate-pipeline

sudo systemctl daemon-reload
```

Le service effectue pour chaque job :

```text
ingest-dataset → review-import → import-dhis2 --dry-run → import-dhis2 --apply → checkpoint
```

Avec `DHIS2_CLIMATE_CONTINUE_ON_FAILURE=true`, un job en erreur est enregistré et le suivant est
traité. L’unité termine néanmoins en échec afin que l’incident reste visible. Un code temporaire
`75` peut être relancé au maximum trois fois par systemd ; un code permanent `2` n’est pas relancé.

### 4. Premier backfill historique

Avant d’activer le timer, éditer `/etc/default/dhis2-climate-pipeline` :

```bash
DHIS2_CLIMATE_WINDOW_MODE=fixed
DHIS2_CLIMATE_START_PERIOD=2017-W01
DHIS2_CLIMATE_END_PERIOD=2026-W39
DHIS2_CLIMATE_PERIOD_BATCH_SIZE=13
```

Cette configuration peut donc reprendre environ neuf années. Elle ne crée pas une requête OCS ni
un import DHIS2 de neuf ans : elle génère des tranches successives de treize semaines. Chaque
tranche exécute indépendamment :

```text
ingestion OCS 13 semaines
→ revue DHIS2 13 semaines
→ dry-run DHIS2 13 semaines
→ apply DHIS2 13 semaines
→ checkpoint de la tranche
```

Treize semaines donnent environ quatre tranches par année. Chaque étape utilise un nouveau
processus Python ; sa mémoire est donc libérée avant l’étape ou la tranche suivante. En cas de
timeout, systemd reprend depuis la première tranche sans checkpoint au lieu de recommencer les neuf
années.

Puis démarrer en arrière-plan :

```bash
sudo systemctl start --no-block dhis2-climate-pipeline.service
sudo journalctl -fu dhis2-climate-pipeline.service
```

Les checkpoints sont conservés dans :

```text
/opt/dhis2-climate-import/state/climate-pipeline/checkpoints/
```

Après un redémarrage, une fenêtre déjà confirmée est ignorée. Si le journal SQLite connaît déjà
un import réussi mais que le checkpoint n’existe pas encore, le pipeline reconnaît également ce
succès et crée le checkpoint sans utiliser `--force`.

### 5. Passer en mise à jour hebdomadaire

Après le succès du backfill, remettre `/etc/default/dhis2-climate-pipeline` en mode glissant :

```bash
DHIS2_CLIMATE_WINDOW_MODE=rolling
DHIS2_CLIMATE_LOOKBACK_WEEKS=8
DHIS2_CLIMATE_LAG_WEEKS=3
DHIS2_CLIMATE_PERIOD_BATCH_SIZE=13
```

La fenêtre se termine trois semaines avant la dernière semaine ISO complète et revisite huit
semaines. Cette marge absorbe les publications tardives et les exécutions manquées. Les valeurs
déjà présentes sont mises à jour de manière idempotente avec `CREATE_AND_UPDATE`.

Activer ensuite le timer :

```bash
sudo systemctl enable --now dhis2-climate-pipeline.timer
systemctl list-timers dhis2-climate-pipeline.timer
```

Commandes d’exploitation :

```bash
sudo systemctl status dhis2-climate-pipeline.service
sudo journalctl -u dhis2-climate-pipeline.service --since today
sudo journalctl -fu dhis2-climate-pipeline.service

# Déclenchement manuel, sans attendre le timer
sudo systemctl start --no-block dhis2-climate-pipeline.service
```

Le scheduler interne d’OCS reste désactivé dans l’exemple. Ne pas l’activer pour ces mêmes datasets
tant que `dhis2-climate-pipeline.timer` les orchestre, sinon deux ingestions concurrentes pourraient
recevoir un conflit `409`.
