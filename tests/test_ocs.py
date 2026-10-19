import unittest

from dhis2_climate_import.errors import PipelineError
from dhis2_climate_import.ocs import OCSClient


class FakeHttp:
    def __init__(self) -> None:
        self.last_call = None

    def request_json(self, method, path, *, params=None, json_body=None, headers=None):
        self.last_call = (method, path, params, json_body)
        return {"dataValues": []}


class SequenceHttp:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls = []

    def request_json(self, method, path, *, params=None, json_body=None, headers=None):
        self.calls.append((method, path, params, json_body, headers))
        return self.responses.pop(0)


class OCSTests(unittest.TestCase):
    def test_ingestion_uses_native_async_job_and_returns_result(self) -> None:
        http = SequenceHttp(
            [
                {"ingestion_id": "job-1", "status": "accepted", "dataset": None},
                {
                    "jobID": "job-1",
                    "status": "running",
                    "progress": {"done": 1, "total": 2, "percent": 50, "message": "Fetching"},
                },
                {
                    "jobID": "job-1",
                    "status": "successful",
                    "progress": {"done": 2, "total": 2, "percent": 100, "message": "Completed"},
                    "result": {"ingestion_id": "artifact-1", "status": "completed"},
                },
            ]
        )
        events = []
        client = OCSClient(
            "http://ocs.example.org",
            http=http,
            poll_seconds=0,
            job_timeout_seconds=10,
        )

        response = client.ingest_dataset(
            "era5land_precipitation_weekly",
            start="2026-W39",
            end="2026-W39",
            progress=lambda event, details: events.append((event, details)),
        )

        self.assertEqual(response, {"ingestion_id": "artifact-1", "status": "completed"})
        self.assertEqual(http.calls[0][0:2], ("POST", "/ingestions"))
        self.assertEqual(http.calls[0][4], {"Prefer": "respond-async"})
        self.assertEqual(http.calls[1][0:2], ("GET", "/ingestions/jobs/job-1"))
        self.assertEqual(events[0][0], "ocs.ingestion.queued")
        self.assertEqual(events[-1][0], "ocs.ingestion.done")

    def test_failed_async_ingestion_reports_job_error(self) -> None:
        http = SequenceHttp(
            [
                {"ingestion_id": "job-2", "status": "accepted", "dataset": None},
                {
                    "jobID": "job-2",
                    "status": "failed",
                    "progress": {"message": "Failed"},
                    "error": {"type": "RuntimeError", "message": "CDS rejected request"},
                },
            ]
        )
        client = OCSClient("http://ocs.example.org", http=http, poll_seconds=0)

        with self.assertRaisesRegex(PipelineError, "CDS rejected request"):
            client.ingest_dataset("era5land_precipitation_weekly")

    def test_environment_controls_ocs_polling_without_extending_each_http_call(self) -> None:
        client = OCSClient.from_env(
            {
                "CLIMATE_SERVICE_BASE_URL": "http://ocs.example.org",
                "CLIMATE_SERVICE_HTTP_TIMEOUT_SECONDS": "30",
                "CLIMATE_SERVICE_POLL_SECONDS": "2",
                "CLIMATE_SERVICE_JOB_TIMEOUT_SECONDS": "7200",
            }
        )

        self.assertEqual(client.http.timeout_seconds, 30)
        self.assertEqual(client.http.retries, 0)
        self.assertEqual(client.poll_seconds, 2)
        self.assertEqual(client.job_timeout_seconds, 7200)

    def test_aggregate_builds_openeo_process_graph(self) -> None:
        http = FakeHttp()
        geometries = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "OrgUnit0001",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                    },
                }
            ],
        }
        response = OCSClient("http://ocs.example.org", http=http).aggregate_to_dhis2_json(
            process_id="aggregate_to_dhis2_json",
            dataset_id="chirps3_precipitation_daily",
            geometries=geometries,
            data_element_id="DataEl00001",
            period_type="day",
            start="2024-01-01",
            end="2024-01-31",
            method="mean",
        )
        method, path, _params, body = http.last_call
        self.assertEqual(response, {"dataValues": []})
        self.assertEqual(method, "POST")
        self.assertEqual(path, "/result")
        self.assertEqual(
            body["process_graph"]["aggregate"]["arguments"]["dataset_id"],
            "chirps3_precipitation_daily",
        )
        arguments = body["process_graph"]["aggregate"]["arguments"]
        self.assertEqual(arguments["temporal_extent"], ["2024-01-01", "2024-01-31"])
        self.assertEqual(arguments["geometries"], geometries)
        self.assertNotIn("org_units_geojson", arguments)
        self.assertNotIn("start", arguments)
        self.assertNotIn("end", arguments)


if __name__ == "__main__":
    unittest.main()
