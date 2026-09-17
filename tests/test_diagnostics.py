import unittest

from dhis2_climate_import.diagnostics import candidate_base_urls, summarise_probes


class DiagnosticsTests(unittest.TestCase):
    def test_candidate_base_urls_adds_requested_contexts(self) -> None:
        self.assertEqual(
            candidate_base_urls(
                "https://test.snisrdc.com/api/",
                contexts=["dhis"],
                include_common_contexts=False,
            ),
            ["https://test.snisrdc.com", "https://test.snisrdc.com/dhis"],
        )

    def test_summarise_nginx_html_points_to_proxy_or_base_url(self) -> None:
        summary = summarise_probes(
            [
                {
                    "url": "https://test.snisrdc.com/api/system/info",
                    "statusCode": 403,
                    "looksLikeDhis2Json": False,
                    "looksLikeNginxHtml": True,
                }
            ]
        )
        self.assertIn("nginx returned HTML", summary)


if __name__ == "__main__":
    unittest.main()
