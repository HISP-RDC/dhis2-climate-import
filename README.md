# DHIS2 Climate Import

Pipeline Python pour automatiser les imports de donnees climat vers DHIS2 Climate App.

Le projet garde Open Climate Service comme moteur climat/geospatial et utilise
`dhis2-python-client` comme transport DHIS2 principal. Il fournit une couche fiable pour DHIS2 :
cache des unites d'organisation niveau 3, cache des elements de donnees, agregation, import
`dataValueSets`, verification analytics, journal SQLite et rapports.

## Quick Start

```sh
cp .env.example .env
cp climate-service.yaml.example climate-service.yaml
python3 -m uv sync
PYTHONPATH=src .venv/bin/python -m dhis2_climate_import --help
```

Open Climate Service runs as a separate local service. Start it from the official repository,
then point this pipeline to it:

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
