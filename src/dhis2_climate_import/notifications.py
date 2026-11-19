from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import mimetypes
from pathlib import Path
import re
import smtplib
import socket
import ssl
from typing import Mapping

from .email_templates import (
    EmailBrand,
    PipelineEmailContext,
    RenderedEmail,
    render_admin_email,
    render_user_email,
)
from .errors import PipelineError


_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}
_HEX_COLOR = re.compile(r"#[0-9a-fA-F]{6}")
_LOGO_CID = "hisp-rdc-logo"
_DEFAULT_LOGO_PATH = Path(__file__).with_name("assets") / "hisp-rdc-logo.png"


@dataclass(frozen=True)
class ClimateEmailSettings:
    enabled: bool
    notify_admins_on_success: bool
    notify_admins_on_failure: bool
    notify_users_on_success: bool
    admin_recipients: tuple[str, ...]
    user_recipients: tuple[str, ...]
    sender: str
    smtp_host: str
    smtp_port: int
    smtp_security: str
    smtp_username: str
    smtp_password: str
    smtp_timeout_seconds: float
    brand: EmailBrand
    logo_path: Path | None

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ClimateEmailSettings:
        security = env.get("DHIS2_CLIMATE_SMTP_SECURITY", "starttls").strip().lower()
        if security not in {"starttls", "ssl", "none"}:
            raise PipelineError(
                "DHIS2_CLIMATE_SMTP_SECURITY must be starttls, ssl, or none."
            )

        default_port = "465" if security == "ssl" else "587"
        port = _positive_int(
            "DHIS2_CLIMATE_SMTP_PORT",
            env.get("DHIS2_CLIMATE_SMTP_PORT", default_port),
            maximum=65535,
        )
        timeout = _positive_float(
            "DHIS2_CLIMATE_SMTP_TIMEOUT_SECONDS",
            env.get("DHIS2_CLIMATE_SMTP_TIMEOUT_SECONDS", "30"),
        )
        username = env.get("DHIS2_CLIMATE_SMTP_USERNAME", "").strip()
        password = env.get("DHIS2_CLIMATE_SMTP_PASSWORD", "")
        if bool(username) != bool(password):
            raise PipelineError(
                "DHIS2_CLIMATE_SMTP_USERNAME and DHIS2_CLIMATE_SMTP_PASSWORD "
                "must either both be set or both be empty."
            )

        legacy_recipients = env.get("DHIS2_CLIMATE_EMAIL_TO", "")
        admin_recipients = _parse_recipients(
            "DHIS2_CLIMATE_ADMIN_EMAIL_TO",
            env.get("DHIS2_CLIMATE_ADMIN_EMAIL_TO", "") or legacy_recipients,
        )
        user_recipients = _parse_recipients(
            "DHIS2_CLIMATE_USER_EMAIL_TO",
            env.get("DHIS2_CLIMATE_USER_EMAIL_TO", ""),
        )
        sender = env.get("DHIS2_CLIMATE_EMAIL_FROM", "").strip() or username
        _reject_header_injection("DHIS2_CLIMATE_EMAIL_FROM", sender)

        logo_value = env.get("DHIS2_CLIMATE_EMAIL_LOGO_PATH", "").strip()
        logo_path = Path(logo_value).expanduser() if logo_value else _DEFAULT_LOGO_PATH
        brand = EmailBrand(
            name=env.get("DHIS2_CLIMATE_EMAIL_BRAND_NAME", "HISP RDC").strip()
            or "HISP RDC",
            locale=env.get("DHIS2_CLIMATE_EMAIL_LOCALE", "en").strip() or "en",
            primary_color=_color(
                "DHIS2_CLIMATE_EMAIL_PRIMARY_COLOR",
                env.get("DHIS2_CLIMATE_EMAIL_PRIMARY_COLOR", "#1477E5"),
            ),
            navy_color=_color(
                "DHIS2_CLIMATE_EMAIL_NAVY_COLOR",
                env.get("DHIS2_CLIMATE_EMAIL_NAVY_COLOR", "#091B67"),
            ),
            accent_color=_color(
                "DHIS2_CLIMATE_EMAIL_ACCENT_COLOR",
                env.get("DHIS2_CLIMATE_EMAIL_ACCENT_COLOR", "#F02B16"),
            ),
            gold_color=_color(
                "DHIS2_CLIMATE_EMAIL_GOLD_COLOR",
                env.get("DHIS2_CLIMATE_EMAIL_GOLD_COLOR", "#FFD500"),
            ),
            portal_url=env.get("DHIS2_CLIMATE_EMAIL_PORTAL_URL", "").strip(),
            support_email=env.get("DHIS2_CLIMATE_EMAIL_SUPPORT_EMAIL", "").strip(),
            logo_cid=_LOGO_CID if logo_path else "",
        )
        for name, value in (
            ("DHIS2_CLIMATE_EMAIL_BRAND_NAME", brand.name),
            ("DHIS2_CLIMATE_EMAIL_PORTAL_URL", brand.portal_url),
            ("DHIS2_CLIMATE_EMAIL_SUPPORT_EMAIL", brand.support_email),
        ):
            _reject_header_injection(name, value)

        return cls(
            enabled=_boolean(
                "DHIS2_CLIMATE_EMAIL_ENABLED",
                env.get("DHIS2_CLIMATE_EMAIL_ENABLED", "false"),
            ),
            notify_admins_on_success=_boolean(
                "DHIS2_CLIMATE_ADMIN_EMAIL_ON_SUCCESS",
                env.get(
                    "DHIS2_CLIMATE_ADMIN_EMAIL_ON_SUCCESS",
                    env.get("DHIS2_CLIMATE_EMAIL_ON_SUCCESS", "true"),
                ),
            ),
            notify_admins_on_failure=_boolean(
                "DHIS2_CLIMATE_ADMIN_EMAIL_ON_FAILURE",
                env.get(
                    "DHIS2_CLIMATE_ADMIN_EMAIL_ON_FAILURE",
                    env.get("DHIS2_CLIMATE_EMAIL_ON_FAILURE", "true"),
                ),
            ),
            notify_users_on_success=_boolean(
                "DHIS2_CLIMATE_USER_EMAIL_ON_SUCCESS",
                env.get("DHIS2_CLIMATE_USER_EMAIL_ON_SUCCESS", "false"),
            ),
            admin_recipients=admin_recipients,
            user_recipients=user_recipients,
            sender=sender,
            smtp_host=env.get("DHIS2_CLIMATE_SMTP_HOST", "").strip(),
            smtp_port=port,
            smtp_security=security,
            smtp_username=username,
            smtp_password=password,
            smtp_timeout_seconds=timeout,
            brand=brand,
            logo_path=logo_path,
        )

    def validate_smtp(self) -> None:
        if not self.sender:
            raise PipelineError(
                "DHIS2_CLIMATE_EMAIL_FROM is required when SMTP username is empty."
            )
        if not self.smtp_host:
            raise PipelineError("DHIS2_CLIMATE_SMTP_HOST is required.")
        if self.logo_path is not None:
            _validate_logo(self.logo_path)


@dataclass(frozen=True)
class EmailDelivery:
    audience: str
    recipients: tuple[str, ...]
    message: EmailMessage


def send_climate_pipeline_notification(
    env: Mapping[str, str],
    *,
    outcome: str,
    exit_status: int,
    started_at: str,
    finished_at: str,
    start_period: str = "",
    end_period: str = "",
    jobs: str = "",
    failures: str = "",
    audience: str = "all",
    force: bool = False,
) -> dict[str, object]:
    if outcome not in {"success", "failure", "test"}:
        raise PipelineError("Email notification outcome must be success, failure, or test.")
    if audience not in {"all", "admins", "users"}:
        raise PipelineError("Email notification audience must be all, admins, or users.")

    settings = ClimateEmailSettings.from_env(env)
    if not force and not settings.enabled:
        return {"status": "skipped", "reason": "email notifications are disabled"}

    context = PipelineEmailContext(
        outcome=outcome,
        exit_status=exit_status,
        host_name=socket.getfqdn() or socket.gethostname() or "unknown-host",
        started_at=started_at,
        finished_at=finished_at,
        start_period=start_period,
        end_period=end_period,
        jobs=tuple(jobs.split()),
        failures=tuple(failures.split()),
    )
    deliveries = _build_deliveries(
        settings,
        context,
        audience=audience,
        force=force,
    )
    if not deliveries:
        return {
            "status": "skipped",
            "reason": "no notification is enabled for this outcome and audience",
        }

    settings.validate_smtp()
    _deliver(settings, deliveries)
    return {
        "status": "sent",
        "outcome": outcome,
        "host": context.host_name,
        "deliveries": [
            {"audience": item.audience, "recipientCount": len(item.recipients)}
            for item in deliveries
        ],
    }


def _build_deliveries(
    settings: ClimateEmailSettings,
    context: PipelineEmailContext,
    *,
    audience: str,
    force: bool,
) -> list[EmailDelivery]:
    deliveries: list[EmailDelivery] = []
    admin_requested = audience in {"all", "admins"}
    users_requested = audience in {"all", "users"}
    admin_enabled = (
        context.outcome == "test"
        or context.outcome == "success" and settings.notify_admins_on_success
        or context.outcome == "failure" and settings.notify_admins_on_failure
    )
    users_enabled = context.outcome in {"success", "test"} and (
        force or settings.notify_users_on_success
    )

    if admin_requested and (force or admin_enabled):
        if not settings.admin_recipients:
            raise PipelineError(
                "DHIS2_CLIMATE_ADMIN_EMAIL_TO must contain at least one address."
            )
        deliveries.append(
            _delivery(
                "admins",
                settings.admin_recipients,
                settings,
                render_admin_email(settings.brand, context),
            )
        )
    if users_requested and users_enabled:
        if not settings.user_recipients:
            raise PipelineError(
                "DHIS2_CLIMATE_USER_EMAIL_TO must contain at least one address."
            )
        deliveries.append(
            _delivery(
                "users",
                settings.user_recipients,
                settings,
                render_user_email(settings.brand, context),
            )
        )
    return deliveries


def _delivery(
    audience: str,
    recipients: tuple[str, ...],
    settings: ClimateEmailSettings,
    rendered: RenderedEmail,
) -> EmailDelivery:
    message = EmailMessage()
    message["From"] = settings.sender
    message["To"] = "undisclosed-recipients:;"
    message["Subject"] = rendered.subject
    message.set_content(rendered.text)
    message.add_alternative(rendered.html, subtype="html")
    if settings.logo_path is not None:
        _attach_logo(message, settings.logo_path)
    return EmailDelivery(audience=audience, recipients=recipients, message=message)


def _attach_logo(message: EmailMessage, path: Path) -> None:
    content_type, _encoding = mimetypes.guess_type(path.name)
    if content_type not in {"image/png", "image/jpeg", "image/gif"}:
        raise PipelineError(
            "DHIS2_CLIMATE_EMAIL_LOGO_PATH must reference a PNG, JPEG, or GIF image."
        )
    maintype, subtype = content_type.split("/", 1)
    html_part = message.get_body(preferencelist=("html",))
    if html_part is None:
        raise PipelineError("Could not construct the HTML email body.")
    html_part.add_related(
        path.read_bytes(),
        maintype=maintype,
        subtype=subtype,
        cid=f"<{_LOGO_CID}>",
        filename=path.name,
        disposition="inline",
    )


def _deliver(settings: ClimateEmailSettings, deliveries: list[EmailDelivery]) -> None:
    context = ssl.create_default_context()
    if settings.smtp_security == "ssl":
        client_factory = smtplib.SMTP_SSL
        client_kwargs = {"context": context}
    else:
        client_factory = smtplib.SMTP
        client_kwargs = {}

    try:
        with client_factory(
            settings.smtp_host,
            settings.smtp_port,
            timeout=settings.smtp_timeout_seconds,
            **client_kwargs,
        ) as client:
            client.ehlo()
            if settings.smtp_security == "starttls":
                client.starttls(context=context)
                client.ehlo()
            if settings.smtp_username:
                client.login(settings.smtp_username, settings.smtp_password)
            for delivery in deliveries:
                refused = client.send_message(
                    delivery.message,
                    from_addr=settings.sender,
                    to_addrs=list(delivery.recipients),
                )
                if refused:
                    raise PipelineError(
                        f"SMTP refused {len(refused)} recipient(s) in the "
                        f"{delivery.audience} audience."
                    )
    except PipelineError:
        raise
    except (OSError, smtplib.SMTPException) as exc:
        raise PipelineError(f"SMTP notification failed: {exc}") from exc


def _parse_recipients(name: str, value: str) -> tuple[str, ...]:
    recipients = tuple(part.strip() for part in re.split(r"[,;]", value) if part.strip())
    for recipient in recipients:
        _reject_header_injection(name, recipient)
    return recipients


def _validate_logo(path: Path) -> None:
    if not path.is_file():
        raise PipelineError(f"Email logo file not found: {path}")
    if path.stat().st_size > 2 * 1024 * 1024:
        raise PipelineError("Email logo must be smaller than 2 MiB.")


def _color(name: str, value: str) -> str:
    normalized = value.strip()
    if not _HEX_COLOR.fullmatch(normalized):
        raise PipelineError(f"{name} must use the #RRGGBB format.")
    return normalized.upper()


def _reject_header_injection(name: str, value: str) -> None:
    if "\r" in value or "\n" in value:
        raise PipelineError(f"{name} must not contain line breaks.")


def _boolean(name: str, value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise PipelineError(f"{name} must be true or false.")


def _positive_int(name: str, value: str, *, maximum: int | None = None) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise PipelineError(f"{name} must be an integer.") from exc
    if parsed <= 0 or (maximum is not None and parsed > maximum):
        suffix = f" between 1 and {maximum}" if maximum is not None else " greater than zero"
        raise PipelineError(f"{name} must be{suffix}.")
    return parsed


def _positive_float(name: str, value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise PipelineError(f"{name} must be a number.") from exc
    if parsed <= 0:
        raise PipelineError(f"{name} must be greater than zero.")
    return parsed
