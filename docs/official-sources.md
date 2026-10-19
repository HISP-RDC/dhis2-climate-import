# Official Sources

Derniere revue: 2026-10-19.

Ce projet doit rester aligne sur les sources officielles, surtout avant tout import vers une
instance DHIS2 de production.

## DHIS2 Climate

- DHIS2 Climate App user guide: <https://docs.dhis2.org/en/implement/climate-health/climate-app-user-guide.html>
- Open Climate Service: <https://open-climate-service.dhis2.org/>
- DHIS2 Climate Tools: <https://climate-tools.dhis2.org/>
- CHAP: <https://chap.dhis2.org/>

## DHIS2 Platform and Web API

- DHIS2 Web API data values: <https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-242/data.html>
- DHIS2 metadata filters and nested field selection: <https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-243/metadata.html>
- DHIS2 Python Client: <https://developers.dhis2.org/docs/integration/dhis2-python-client/>
- DHIS2 Python Client GitHub: <https://github.com/dhis2/dhis2-python-client>
- DHIS2 developer manual: <https://docs.dhis2.org/en/full/develop/dhis-core-version-master/developer-manual.html>
- DHIS2 App Platform: <https://developers.dhis2.org/docs/app-platform/getting-started/>
- DHIS2 App Runtime: <https://developers.dhis2.org/docs/app-runtime/getting-started/>

## Notes for v1

- V1 supports precipitation, temperature, dew point, relative humidity, UTCI, NDVI and EVI.
- The Climate App metadata guide maps max air temperature to `ERA5_LAND_TEMPERATURE_MAX` /
  `CCH - Max air temperature (ERA5-Land)`.
- Climate App imports should use the DHIS2 data set matching the period type, for example weekly
  imports from a weekly data set.
- The instance plugin in `open-climate-service/plugins/datasets/drc_weekly.py` provides the
  custom weekly OCS datasets required for humidity, UTCI, NDVI and EVI.
- The target for validation is DHIS2 2.37+ on a non-production instance.
- Exact endpoint behavior must be checked against the target DHIS2 version before production use.
