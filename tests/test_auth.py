import unittest

from dhis2_climate_import.dhis2_auth import auth_headers, auth_options
from dhis2_climate_import.errors import PipelineError


class AuthTests(unittest.TestCase):
    def test_auto_uses_pat_when_present(self) -> None:
        headers = auth_headers({"DHIS2_PAT": "d2pat_token"})
        self.assertEqual(headers["Authorization"], "ApiToken d2pat_token")
        self.assertEqual(auth_options({"DHIS2_PAT": "d2pat_token"}), {"token": "d2pat_token"})

    def test_pat_value_can_include_scheme(self) -> None:
        headers = auth_headers({"DHIS2_PAT": "ApiToken d2pat_token"})
        self.assertEqual(headers["Authorization"], "ApiToken d2pat_token")
        self.assertEqual(
            auth_options({"DHIS2_PAT": "ApiToken d2pat_token"}),
            {"token": "d2pat_token"},
        )

    def test_basic_can_be_forced(self) -> None:
        env = (
            {
                "DHIS2_AUTH_METHOD": "basic",
                "DHIS2_PAT": "d2pat_token",
                "DHIS2_USERNAME": "admin",
                "DHIS2_PASSWORD": "district",
            }
        )
        headers = auth_headers(env)
        self.assertTrue(headers["Authorization"].startswith("Basic "))
        self.assertEqual(auth_options(env), {"username": "admin", "password": "district"})

    def test_basic_requires_credentials(self) -> None:
        with self.assertRaises(PipelineError):
            auth_headers({"DHIS2_AUTH_METHOD": "basic", "DHIS2_PAT": "d2pat_token"})


if __name__ == "__main__":
    unittest.main()
