import unittest

from dhis2_climate_import.http import (
    HttpRequestError,
    is_timeout_or_gateway_error,
    is_transient_http_error,
)


class HttpErrorClassificationTests(unittest.TestCase):
    def test_disconnect_is_retryable_and_splittable(self) -> None:
        error = HttpRequestError(
            "POST",
            "https://example.org/api/dataValueSets",
            None,
            "Server disconnected without sending a response.",
        )

        self.assertTrue(is_transient_http_error(error))
        self.assertTrue(is_timeout_or_gateway_error(error))

    def test_rate_limit_is_transient_but_not_a_timeout(self) -> None:
        error = HttpRequestError("POST", "https://example.org/api", 429, "Too Many Requests")

        self.assertTrue(is_transient_http_error(error))
        self.assertFalse(is_timeout_or_gateway_error(error))

    def test_data_conflict_is_permanent(self) -> None:
        error = HttpRequestError("POST", "https://example.org/api", 409, "Conflict")

        self.assertFalse(is_transient_http_error(error))
        self.assertFalse(is_timeout_or_gateway_error(error))


if __name__ == "__main__":
    unittest.main()
