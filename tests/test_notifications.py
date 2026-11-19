from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from dhis2_climate_import.errors import PipelineError
from dhis2_climate_import.notifications import send_climate_pipeline_notification


def _smtp_env() -> dict[str, str]:
    return {
        "DHIS2_CLIMATE_EMAIL_ENABLED": "true",
        "DHIS2_CLIMATE_ADMIN_EMAIL_ON_SUCCESS": "true",
        "DHIS2_CLIMATE_ADMIN_EMAIL_ON_FAILURE": "true",
        "DHIS2_CLIMATE_ADMIN_EMAIL_TO": "ops@example.org, manager@example.org",
        "DHIS2_CLIMATE_USER_EMAIL_ON_SUCCESS": "false",
        "DHIS2_CLIMATE_USER_EMAIL_TO": "",
        "DHIS2_CLIMATE_EMAIL_FROM": "pipeline@example.org",
        "DHIS2_CLIMATE_EMAIL_LOCALE": "fr",
        "DHIS2_CLIMATE_SMTP_HOST": "smtp.example.org",
        "DHIS2_CLIMATE_SMTP_PORT": "587",
        "DHIS2_CLIMATE_SMTP_SECURITY": "starttls",
        "DHIS2_CLIMATE_SMTP_USERNAME": "pipeline-user",
        "DHIS2_CLIMATE_SMTP_PASSWORD": "secret",
    }


class ClimateEmailNotificationTests(unittest.TestCase):
    def test_disabled_notification_is_skipped_without_opening_smtp(self) -> None:
        with patch("dhis2_climate_import.notifications.smtplib.SMTP") as smtp:
            result = send_climate_pipeline_notification(
                {"DHIS2_CLIMATE_EMAIL_ENABLED": "false"},
                outcome="success",
                exit_status=0,
                started_at="2026-11-18T00:00:00Z",
                finished_at="2026-11-18T00:01:00Z",
            )

        self.assertEqual(result["status"], "skipped")
        smtp.assert_not_called()

    def test_starttls_delivery_contains_pipeline_context(self) -> None:
        smtp_client = MagicMock()
        smtp_context = MagicMock()
        smtp_context.__enter__.return_value = smtp_client
        smtp_context.__exit__.return_value = False
        smtp_client.send_message.return_value = {}

        with (
            patch(
                "dhis2_climate_import.notifications.smtplib.SMTP",
                return_value=smtp_context,
            ) as smtp,
            patch(
                "dhis2_climate_import.notifications.socket.getfqdn",
                return_value="climate.example.org",
            ),
        ):
            result = send_climate_pipeline_notification(
                _smtp_env(),
                outcome="failure",
                exit_status=75,
                started_at="2026-11-18T00:00:00Z",
                finished_at="2026-11-18T00:05:00Z",
                start_period="2026-W42",
                end_period="2026-W45",
                jobs="vegetation_ndvi_weekly precipitation_weekly_chirps",
                failures="vegetation_ndvi_weekly:2026-W42:2026-W45:apply:75",
            )

        self.assertEqual(result["status"], "sent")
        smtp.assert_called_once_with("smtp.example.org", 587, timeout=30.0)
        smtp_client.starttls.assert_called_once()
        smtp_client.login.assert_called_once_with("pipeline-user", "secret")
        message = smtp_client.send_message.call_args.args[0]
        self.assertIn("ÉCHEC", message["Subject"])
        plain = message.get_body(preferencelist=("plain",)).get_content()
        html = message.get_body(preferencelist=("html",)).get_content()
        self.assertIn("2026-W42 → 2026-W45", plain)
        self.assertIn("apply:75", plain)
        self.assertIn("Le pipeline climatique nécessite", html)
        self.assertEqual(message["To"], "undisclosed-recipients:;")
        self.assertNotIn("ops@example.org", str(message))

    def test_notification_failure_is_actionable_and_hides_password(self) -> None:
        with patch(
            "dhis2_climate_import.notifications.smtplib.SMTP",
            side_effect=OSError("connection refused"),
        ):
            with self.assertRaisesRegex(PipelineError, "connection refused") as raised:
                send_climate_pipeline_notification(
                    _smtp_env(),
                    outcome="success",
                    exit_status=0,
                    started_at="2026-11-18T00:00:00Z",
                    finished_at="2026-11-18T00:05:00Z",
                )

        self.assertNotIn("secret", str(raised.exception))

    def test_admin_host_defaults_to_dhis2_instance_instead_of_vps(self) -> None:
        env = _smtp_env()
        env["DHIS2_BASE_URL"] = "https://sante-climate.hispdrc.org/dhis2"
        client = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = client
        context.__exit__.return_value = False
        client.send_message.return_value = {}

        with (
            patch(
                "dhis2_climate_import.notifications.smtplib.SMTP",
                return_value=context,
            ),
            patch(
                "dhis2_climate_import.notifications.socket.getfqdn",
                return_value="vmi3662353.contaboserver.net",
            ),
        ):
            result = send_climate_pipeline_notification(
                env,
                outcome="test",
                exit_status=0,
                started_at="",
                finished_at="",
                audience="admins",
                force=True,
            )

        message = client.send_message.call_args.args[0]
        self.assertEqual(result["host"], "sante-climate.hispdrc.org")
        self.assertIn("sante-climate.hispdrc.org", message["Subject"])
        self.assertNotIn("vmi3662353", str(message))

    def test_explicit_admin_host_name_overrides_dhis2_base_url(self) -> None:
        env = _smtp_env()
        env["DHIS2_BASE_URL"] = "https://internal-dhis2.example.org"
        env["DHIS2_CLIMATE_EMAIL_HOST_NAME"] = "sante-climate.hispdrc.org"
        client = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = client
        context.__exit__.return_value = False
        client.send_message.return_value = {}

        with patch(
            "dhis2_climate_import.notifications.smtplib.SMTP",
            return_value=context,
        ):
            result = send_climate_pipeline_notification(
                env,
                outcome="test",
                exit_status=0,
                started_at="",
                finished_at="",
                audience="admins",
                force=True,
            )

        self.assertEqual(result["host"], "sante-climate.hispdrc.org")

    def test_ssl_delivery_uses_smtp_ssl(self) -> None:
        env = _smtp_env()
        env["DHIS2_CLIMATE_SMTP_SECURITY"] = "ssl"
        env["DHIS2_CLIMATE_SMTP_PORT"] = "465"
        client = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = client
        context.__exit__.return_value = False
        client.send_message.return_value = {}

        with patch(
            "dhis2_climate_import.notifications.smtplib.SMTP_SSL",
            return_value=context,
        ) as smtp_ssl:
            send_climate_pipeline_notification(
                env,
                outcome="test",
                exit_status=0,
                started_at="2026-11-18T00:00:00Z",
                finished_at="2026-11-18T00:00:00Z",
                audience="admins",
                force=True,
            )

        smtp_ssl.assert_called_once()
        client.starttls.assert_not_called()

    def test_success_sends_separate_admin_and_private_user_messages(self) -> None:
        env = _smtp_env()
        env["DHIS2_CLIMATE_USER_EMAIL_ON_SUCCESS"] = "true"
        env["DHIS2_CLIMATE_USER_EMAIL_TO"] = ",".join(
            f"user{index}@example.org" for index in range(1, 11)
        )
        client = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = client
        context.__exit__.return_value = False
        client.send_message.return_value = {}

        with patch(
            "dhis2_climate_import.notifications.smtplib.SMTP",
            return_value=context,
        ):
            result = send_climate_pipeline_notification(
                env,
                outcome="success",
                exit_status=0,
                started_at="2026-11-18T00:00:00Z",
                finished_at="2026-11-18T00:08:00Z",
                start_period="2026-W42",
                end_period="2026-W45",
                jobs="precipitation_weekly_chirps vegetation_ndvi_weekly",
            )

        self.assertEqual(
            result["deliveries"],
            [
                {"audience": "admins", "recipientCount": 2},
                {"audience": "users", "recipientCount": 10},
            ],
        )
        self.assertEqual(client.send_message.call_count, 2)
        user_call = client.send_message.call_args_list[1]
        user_message = user_call.args[0]
        self.assertEqual(len(user_call.kwargs["to_addrs"]), 10)
        self.assertNotIn("user1@example.org", str(user_message))
        self.assertIn("Les données climatiques sont à jour", str(user_message))
        self.assertNotIn("journalctl", str(user_message))

    def test_logo_is_attached_inline_to_both_html_messages(self) -> None:
        env = _smtp_env()
        env["DHIS2_CLIMATE_USER_EMAIL_ON_SUCCESS"] = "true"
        env["DHIS2_CLIMATE_USER_EMAIL_TO"] = "user@example.org"
        client = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = client
        context.__exit__.return_value = False
        client.send_message.return_value = {}

        with TemporaryDirectory() as temporary_directory:
            logo = Path(temporary_directory) / "hisp-rdc.png"
            logo.write_bytes(b"\x89PNG\r\n\x1a\nfixture")
            env["DHIS2_CLIMATE_EMAIL_LOGO_PATH"] = str(logo)
            with patch(
                "dhis2_climate_import.notifications.smtplib.SMTP",
                return_value=context,
            ):
                send_climate_pipeline_notification(
                    env,
                    outcome="success",
                    exit_status=0,
                    started_at="2026-11-18T00:00:00Z",
                    finished_at="2026-11-18T00:01:00Z",
                    jobs="vegetation_ndvi_weekly",
                )

        for call in client.send_message.call_args_list:
            message = call.args[0]
            html = message.get_body(preferencelist=("html",)).get_content()
            self.assertIn("cid:hisp-rdc-logo", html)
            self.assertIn("Content-ID: <hisp-rdc-logo>", str(message))

    def test_failure_is_never_sent_to_users(self) -> None:
        env = _smtp_env()
        env["DHIS2_CLIMATE_USER_EMAIL_ON_SUCCESS"] = "true"
        env["DHIS2_CLIMATE_USER_EMAIL_TO"] = "user@example.org"
        client = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = client
        context.__exit__.return_value = False
        client.send_message.return_value = {}

        with patch(
            "dhis2_climate_import.notifications.smtplib.SMTP",
            return_value=context,
        ):
            result = send_climate_pipeline_notification(
                env,
                outcome="failure",
                exit_status=75,
                started_at="",
                finished_at="",
            )

        self.assertEqual(
            result["deliveries"],
            [{"audience": "admins", "recipientCount": 2}],
        )
        self.assertEqual(client.send_message.call_count, 1)

    def test_invalid_partial_credentials_are_rejected(self) -> None:
        env = _smtp_env()
        env["DHIS2_CLIMATE_SMTP_PASSWORD"] = ""

        with self.assertRaisesRegex(PipelineError, "must either both be set"):
            send_climate_pipeline_notification(
                env,
                outcome="test",
                exit_status=0,
                started_at="",
                finished_at="",
                force=True,
            )


if __name__ == "__main__":
    unittest.main()
