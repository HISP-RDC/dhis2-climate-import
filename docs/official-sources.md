# Official sources

Last reviewed: 2026-10-19.

Keep this project aligned with official sources, especially before importing into a production
DHIS2 instance.

## DHIS2 Climate

- DHIS2 Climate App user guide: <https://docs.dhis2.org/en/implement/climate-health/climate-app-user-guide.html>
- Open Climate Service: <https://open-climate-service.dhis2.org/>
- DHIS2 Climate Tools: <https://climate-tools.dhis2.org/>
- CHAP: <https://chap.dhis2.org/>

## DHIS2 Platform and Web API

- DHIS2 Web API data values: <https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-242/data.html>
- DHIS2 metadata filters and nested fields: <https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-243/metadata.html>
- DHIS2 Python Client: <https://developers.dhis2.org/docs/integration/dhis2-python-client/>
- DHIS2 Python Client source: <https://github.com/dhis2/dhis2-python-client>
- DHIS2 developer manual: <https://docs.dhis2.org/en/full/develop/dhis-core-version-master/developer-manual.html>

## Version 1 notes

- Version 1 covers precipitation, temperature, dew point, relative humidity, UTCI, NDVI, and EVI.
- Climate App imports must use a DHIS2 Data Set whose period type matches the source data.
- The instance plugin at `open-climate-service/plugins/datasets/drc_weekly.py` provides the custom
  weekly OCS datasets required for humidity, UTCI, NDVI, and EVI.
- The declared minimum validation target is DHIS2 2.37 on a non-production instance.
- Verify exact endpoint behavior against the target DHIS2 version before production use.
