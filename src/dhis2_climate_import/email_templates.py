from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape


@dataclass(frozen=True)
class EmailBrand:
    name: str
    locale: str
    primary_color: str
    navy_color: str
    accent_color: str
    gold_color: str
    portal_url: str
    support_email: str
    logo_cid: str = ""


@dataclass(frozen=True)
class PipelineEmailContext:
    outcome: str
    exit_status: int
    host_name: str
    started_at: str
    finished_at: str
    start_period: str
    end_period: str
    jobs: tuple[str, ...]
    failures: tuple[str, ...]

    @property
    def period_window(self) -> str:
        if self.start_period and self.end_period:
            return f"{self.start_period} → {self.end_period}"
        return "—"

    @property
    def duration(self) -> str:
        try:
            started = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
            finished = datetime.fromisoformat(self.finished_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return "—"
        seconds = max(0, int((finished - started).total_seconds()))
        hours, remainder = divmod(seconds, 3600)
        minutes, remaining_seconds = divmod(remainder, 60)
        if hours:
            return f"{hours}h {minutes:02d}m"
        if minutes:
            return f"{minutes}m {remaining_seconds:02d}s"
        return f"{remaining_seconds}s"


@dataclass(frozen=True)
class RenderedEmail:
    subject: str
    text: str
    html: str


def render_admin_email(brand: EmailBrand, context: PipelineEmailContext) -> RenderedEmail:
    locale = _locale(brand.locale)
    is_failure = context.outcome == "failure"
    is_test = context.outcome == "test"
    if locale == "fr":
        status = "ÉCHEC" if is_failure else "TEST" if is_test else "SUCCÈS"
        title = (
            "Le pipeline climatique nécessite une intervention"
            if is_failure
            else "Test de notification administrateur"
            if is_test
            else "Le pipeline climatique s’est terminé correctement"
        )
        intro = (
            "Une ou plusieurs étapes ont échoué. Consultez les détails techniques ci-dessous."
            if is_failure
            else "Ceci est un aperçu du message destiné aux administrateurs."
            if is_test
            else "Toutes les tâches configurées ont été validées et exécutées."
        )
        labels = {
            "window": "Période",
            "duration": "Durée",
            "host": "Serveur",
            "exit": "Code de sortie",
            "jobs": "Jobs traités",
            "failures": "Étapes en échec",
            "none": "Aucune",
            "logs": "Journal complet",
        }
    else:
        status = "FAILURE" if is_failure else "TEST" if is_test else "SUCCESS"
        title = (
            "The climate pipeline requires attention"
            if is_failure
            else "Administrator notification test"
            if is_test
            else "The climate pipeline completed successfully"
        )
        intro = (
            "One or more stages failed. Review the technical details below."
            if is_failure
            else "This is a preview of the administrator notification."
            if is_test
            else "All configured tasks were validated and completed."
        )
        labels = {
            "window": "Period",
            "duration": "Duration",
            "host": "Host",
            "exit": "Exit status",
            "jobs": "Processed jobs",
            "failures": "Failed stages",
            "none": "None",
            "logs": "Complete journal",
        }

    status_color = brand.accent_color if is_failure else brand.primary_color
    subject = f"[{brand.name}] {status} — {context.host_name}"
    jobs_text = "\n".join(f"- {job}" for job in context.jobs) or f"- {labels['none']}"
    failures_text = (
        "\n".join(f"- {failure}" for failure in context.failures)
        or f"- {labels['none']}"
    )
    text = "\n".join(
        [
            title,
            intro,
            "",
            f"{labels['window']}: {context.period_window}",
            f"{labels['duration']}: {context.duration}",
            f"{labels['host']}: {context.host_name}",
            f"{labels['exit']}: {context.exit_status}",
            "",
            f"{labels['jobs']}:",
            jobs_text,
            "",
            f"{labels['failures']}:",
            failures_text,
            "",
            f"{labels['logs']}: journalctl -u dhis2-climate-pipeline.service",
        ]
    ) + "\n"
    details = _details_table(
        [
            (labels["window"], context.period_window),
            (labels["duration"], context.duration),
            (labels["host"], context.host_name),
            (labels["exit"], str(context.exit_status)),
        ],
        brand,
    )
    jobs_html = _list_html(context.jobs, labels["none"])
    failures_html = _list_html(context.failures, labels["none"])
    body = f"""
      {_status_badge(status, status_color)}
      <h1 style="margin:18px 0 10px;color:{brand.navy_color};font-size:26px;line-height:1.25;">
        {escape(title)}
      </h1>
      <p style="margin:0 0 24px;color:#526071;font-size:15px;line-height:1.65;">
        {escape(intro)}
      </p>
      {details}
      {_section(labels['jobs'], jobs_html, brand)}
      {_section(labels['failures'], failures_html, brand)}
      <div style="margin-top:24px;padding:14px 16px;background:#0b1739;border-radius:8px;color:#dce6ff;font-family:Consolas,Monaco,monospace;font-size:12px;line-height:1.5;">
        journalctl -u dhis2-climate-pipeline.service
      </div>
    """
    return RenderedEmail(
        subject=subject,
        text=text,
        html=_email_shell(brand, body, preheader=intro, status_color=status_color),
    )


def render_user_email(brand: EmailBrand, context: PipelineEmailContext) -> RenderedEmail:
    locale = _locale(brand.locale)
    is_test = context.outcome == "test"
    if locale == "fr":
        status = "APERÇU DU MODÈLE" if is_test else "MISE À JOUR TERMINÉE"
        title = (
            "Test de la notification aux utilisateurs"
            if is_test
            else "Les données climatiques sont à jour"
        )
        intro = (
            "Ce message permet de vérifier le rendu destiné aux utilisateurs."
            if is_test
            else (
                "L’importation automatique s’est terminée correctement. "
                "Les données climatiques disponibles dans DHIS2 ont été actualisées."
            )
        )
        window_label = "Période mise à jour"
        sources_label = "Flux climatiques traités"
        action_message = (
            "Aucune action n’est requise de votre part."
            if not is_test
            else "Aucune donnée DHIS2 n’a été modifiée par ce test."
        )
        button = "Consulter les données"
        footer = "Notification automatique du programme HISP RDC"
        support = "Assistance"
    else:
        status = "TEMPLATE PREVIEW" if is_test else "UPDATE COMPLETED"
        title = "User notification test" if is_test else "Climate data is up to date"
        intro = (
            "This message verifies the notification delivered to end users."
            if is_test
            else (
                "The automated import completed successfully. "
                "The climate data available in DHIS2 has been updated."
            )
        )
        window_label = "Updated period"
        sources_label = "Climate feeds processed"
        action_message = (
            "No action is required from you."
            if not is_test
            else "No DHIS2 data was modified by this test."
        )
        button = "View climate data"
        footer = "Automated notification from the HISP RDC programme"
        support = "Support"

    subject = f"[{brand.name}] {title} — {context.period_window}"
    text_lines = [
        title,
        "",
        intro,
        "",
        f"{window_label}: {context.period_window}",
        f"{sources_label}: {len(context.jobs)}",
        "",
        action_message,
    ]
    if brand.portal_url:
        text_lines.extend(["", f"{button}: {brand.portal_url}"])
    if brand.support_email:
        text_lines.extend(["", f"{support}: {brand.support_email}"])
    text = "\n".join(text_lines) + "\n"

    button_html = ""
    if brand.portal_url:
        button_html = f"""
          <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="margin:26px 0 6px;">
            <tr><td bgcolor="{brand.primary_color}" style="border-radius:8px;">
              <a href="{escape(brand.portal_url, quote=True)}" style="display:inline-block;padding:13px 22px;color:#ffffff;text-decoration:none;font-size:14px;font-weight:700;">
                {escape(button)}
              </a>
            </td></tr>
          </table>
        """
    cards = _metric_cards(
        window_label,
        context.period_window,
        sources_label,
        str(len(context.jobs)),
        brand,
    )
    body = f"""
      {_status_badge(status, brand.primary_color)}
      <h1 style="margin:18px 0 10px;color:{brand.navy_color};font-size:28px;line-height:1.25;">
        {escape(title)}
      </h1>
      <p style="margin:0 0 24px;color:#526071;font-size:16px;line-height:1.7;">
        {escape(intro)}
      </p>
      {cards}
      <div style="margin-top:22px;padding:16px 18px;border-left:4px solid {brand.gold_color};background:#fffbed;color:#4b5563;font-size:14px;line-height:1.6;">
        {escape(action_message)}
      </div>
      {button_html}
    """
    support_html = (
        f' · {escape(support)}: <a href="mailto:{escape(brand.support_email, quote=True)}" style="color:{brand.primary_color};text-decoration:none;">{escape(brand.support_email)}</a>'
        if brand.support_email
        else ""
    )
    return RenderedEmail(
        subject=subject,
        text=text,
        html=_email_shell(
            brand,
            body,
            preheader=intro,
            status_color=brand.primary_color,
            footer=f"{escape(footer)}{support_html}",
        ),
    )


def _email_shell(
    brand: EmailBrand,
    body: str,
    *,
    preheader: str,
    status_color: str,
    footer: str = "",
) -> str:
    logo = (
        f'<img src="cid:{escape(brand.logo_cid, quote=True)}" width="116" alt="{escape(brand.name, quote=True)}" style="display:block;width:116px;max-width:116px;height:auto;border:0;">'
        if brand.logo_cid
        else f'<span style="color:{brand.navy_color};font-size:22px;font-weight:800;letter-spacing:.2px;">{escape(brand.name)}</span>'
    )
    footer_content = footer or escape(brand.name)
    return f"""<!doctype html>
<html lang="{_locale(brand.locale)}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{escape(brand.name)}</title>
  <style>
    @media only screen and (max-width: 640px) {{
      .email-container {{ width:100% !important; }}
      .email-body {{ padding:28px 22px !important; }}
      .metric-cell {{ display:block !important; width:100% !important; padding:0 0 10px !important; }}
    }}
  </style>
</head>
<body style="margin:0;padding:0;background:#f2f5fa;font-family:Arial,Helvetica,sans-serif;-webkit-text-size-adjust:100%;">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">{escape(preheader)}</div>
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#f2f5fa">
    <tr><td align="center" style="padding:28px 12px;">
      <table role="presentation" width="620" class="email-container" cellspacing="0" cellpadding="0" border="0" style="width:620px;max-width:620px;background:#ffffff;border-radius:14px;overflow:hidden;box-shadow:0 10px 30px rgba(12,35,76,.10);">
        <tr><td style="height:5px;background:{status_color};font-size:0;line-height:0;">&nbsp;</td></tr>
        <tr><td align="center" style="padding:24px 32px 18px;background:#ffffff;border-bottom:1px solid #e8edf5;">{logo}</td></tr>
        <tr><td class="email-body" style="padding:36px 42px 38px;background:#ffffff;">{body}</td></tr>
        <tr><td style="padding:18px 32px;background:#f8fafe;border-top:1px solid #e8edf5;text-align:center;color:#7a8798;font-size:12px;line-height:1.6;">{footer_content}</td></tr>
        <tr><td>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0"><tr>
            <td width="46%" bgcolor="{brand.primary_color}" style="height:4px;font-size:0;">&nbsp;</td>
            <td width="27%" bgcolor="{brand.gold_color}" style="height:4px;font-size:0;">&nbsp;</td>
            <td width="27%" bgcolor="{brand.accent_color}" style="height:4px;font-size:0;">&nbsp;</td>
          </tr></table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _status_badge(label: str, color: str) -> str:
    return f'<span style="display:inline-block;padding:7px 11px;background:{color};border-radius:999px;color:#ffffff;font-size:11px;font-weight:800;letter-spacing:.7px;">{escape(label)}</span>'


def _details_table(rows: list[tuple[str, str]], brand: EmailBrand) -> str:
    rendered = "".join(
        f'<tr><td style="padding:10px 14px;color:#788597;font-size:13px;border-bottom:1px solid #edf1f6;">{escape(label)}</td><td style="padding:10px 14px;color:{brand.navy_color};font-size:13px;font-weight:700;text-align:right;border-bottom:1px solid #edf1f6;">{escape(value)}</td></tr>'
        for label, value in rows
    )
    return f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin:0 0 24px;background:#f8fafe;border:1px solid #e7edf5;border-radius:8px;overflow:hidden;">{rendered}</table>'


def _metric_cards(
    first_label: str,
    first_value: str,
    second_label: str,
    second_value: str,
    brand: EmailBrand,
) -> str:
    return f"""
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0"><tr>
        <td class="metric-cell" width="50%" valign="top" style="padding-right:6px;">
          {_metric_card(first_label, first_value, brand)}
        </td>
        <td class="metric-cell" width="50%" valign="top" style="padding-left:6px;">
          {_metric_card(second_label, second_value, brand)}
        </td>
      </tr></table>
    """


def _metric_card(label: str, value: str, brand: EmailBrand) -> str:
    return f'<div style="padding:18px;background:#f7faff;border:1px solid #e2eaf6;border-radius:9px;"><div style="color:#7a8798;font-size:12px;line-height:1.4;">{escape(label)}</div><div style="margin-top:7px;color:{brand.navy_color};font-size:18px;font-weight:800;line-height:1.3;">{escape(value)}</div></div>'


def _section(title: str, content: str, brand: EmailBrand) -> str:
    return f'<div style="margin-top:22px;"><h2 style="margin:0 0 9px;color:{brand.navy_color};font-size:15px;">{escape(title)}</h2>{content}</div>'


def _list_html(items: tuple[str, ...], empty_label: str) -> str:
    values = items or (empty_label,)
    rows = "".join(
        f'<li style="margin:0 0 6px;color:#526071;font-size:13px;line-height:1.5;">{escape(item)}</li>'
        for item in values
    )
    return f'<ul style="margin:0;padding-left:20px;">{rows}</ul>'


def _locale(value: str) -> str:
    return "fr" if value.lower().startswith("fr") else "en"
