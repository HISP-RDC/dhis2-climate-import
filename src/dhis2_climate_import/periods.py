from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
import re

from .errors import PipelineError


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise PipelineError(f"Expected date in YYYY-MM-DD format, got: {value}") from exc


def periods_between(start: str, end: str, period_type: str) -> list[str]:
    start_date = parse_date(start)
    end_date = parse_date(end)
    if end_date < start_date:
        raise PipelineError("End date must be after or equal to start date.")
    period_type = period_type.lower()
    if period_type == "day":
        periods: list[str] = []
        current = start_date
        while current <= end_date:
            periods.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return periods
    if period_type == "month":
        periods = []
        current = date(start_date.year, start_date.month, 1)
        end_month = date(end_date.year, end_date.month, 1)
        while current <= end_month:
            periods.append(current.strftime("%Y%m"))
            current = _next_month(current)
        return periods
    if period_type == "year":
        return [str(year) for year in range(start_date.year, end_date.year + 1)]
    if period_type == "week":
        periods = []
        current = start_date
        seen: set[str] = set()
        while current <= end_date:
            iso = current.isocalendar()
            period = f"{iso.year}W{iso.week:02d}"
            if period not in seen:
                seen.add(period)
                periods.append(period)
            current += timedelta(days=1)
        return periods
    raise PipelineError(f"Unsupported period_type: {period_type}")


def period_range_bounds(start_period: str, end_period: str, period_type: str) -> tuple[str, str]:
    period_type = period_type.lower()
    if period_type == "day":
        return _day_period_date(start_period).isoformat(), _day_period_date(end_period).isoformat()
    if period_type == "week":
        start, _ = week_bounds(start_period)
        _, end = week_bounds(end_period)
        return start, end
    if period_type == "month":
        start, _ = month_bounds(start_period)
        _, end = month_bounds(end_period)
        return start, end
    if period_type == "year":
        if not (start_period.isdigit() and end_period.isdigit()):
            raise PipelineError("Yearly periods must use YYYY format.")
        return f"{start_period}-01-01", f"{end_period}-12-31"
    raise PipelineError(f"Unsupported period_type: {period_type}")


def period_labels_between(start: str, end: str, period_type: str) -> list[str]:
    return [format_period(period, period_type) for period in periods_between(start, end, period_type)]


def date_window_period_bounds(start: str, end: str, period_type: str) -> tuple[str, str]:
    start_date = parse_date(start)
    end_date = parse_date(end)
    if end_date < start_date:
        raise PipelineError("End date must be after or equal to start date.")
    period_type = period_type.lower()
    if period_type == "day":
        return start, end
    if period_type == "week":
        return (
            (start_date - timedelta(days=start_date.isoweekday() - 1)).isoformat(),
            (end_date + timedelta(days=7 - end_date.isoweekday())).isoformat(),
        )
    if period_type == "month":
        return date(start_date.year, start_date.month, 1).isoformat(), _month_end(end_date).isoformat()
    if period_type == "year":
        return date(start_date.year, 1, 1).isoformat(), date(end_date.year, 12, 31).isoformat()
    raise PipelineError(f"Unsupported period_type: {period_type}")


def relative_year_window(
    years_back: int,
    period_type: str,
    *,
    end: str | None = None,
    end_period: str | None = None,
    as_of: str | None = None,
) -> tuple[str, str]:
    if years_back <= 0:
        raise PipelineError("--years-back must be greater than 0.")
    if end and end_period:
        raise PipelineError("Use either --end or --end-period with --years-back, not both.")
    if as_of and (end or end_period):
        raise PipelineError("--as-of can only be used with --years-back when --end is omitted.")

    period_type = period_type.lower()
    if end_period:
        _period_start, resolved_end = period_range_bounds(end_period, end_period, period_type)
        end_date = parse_date(resolved_end)
    elif end:
        end_date = _align_period_end_on_or_before(parse_date(end), period_type)
    else:
        anchor = parse_date(as_of) if as_of else date.today()
        end_date = _latest_complete_period_end(period_type, anchor)

    raw_start = _add_years(end_date, -years_back) + timedelta(days=1)
    start_date = _align_period_start_on_or_after(raw_start, period_type)
    if start_date > end_date:
        raise PipelineError("Relative period window is empty. Check --years-back and --end.")
    return start_date.isoformat(), end_date.isoformat()


def format_period(period: str, period_type: str) -> str:
    period_type = period_type.lower()
    if period_type == "week":
        match = re.fullmatch(r"(\d{4})W(\d{1,2})", period)
        if not match:
            return period
        return f"{match.group(1)}-W{int(match.group(2)):02d}"
    if period_type == "month" and re.fullmatch(r"\d{6}", period):
        return f"{period[:4]}-{period[4:]}"
    if period_type == "day" and re.fullmatch(r"\d{8}", period):
        return f"{period[:4]}-{period[4:6]}-{period[6:]}"
    return period


def week_bounds(period: str) -> tuple[str, str]:
    match = re.fullmatch(r"(\d{4})-?W(\d{1,2})", period)
    if not match:
        raise PipelineError("Weekly periods must use YYYY-Www or YYYYWww format.")
    year = int(match.group(1))
    week = int(match.group(2))
    try:
        start = date.fromisocalendar(year, week, 1)
        end = date.fromisocalendar(year, week, 7)
    except ValueError as exc:
        raise PipelineError(f"Invalid ISO week period: {period}") from exc
    return start.isoformat(), end.isoformat()


def month_bounds(period: str) -> tuple[str, str]:
    try:
        normalized = f"{period[:4]}-{period[4:]}" if re.fullmatch(r"\d{6}", period) else period
        parsed = datetime.strptime(normalized, "%Y-%m").date()
    except ValueError as exc:
        raise PipelineError("Monthly period must use YYYY-MM format.") from exc
    return parsed.replace(day=1).isoformat(), _month_end(parsed).isoformat()


def _month_end(value: date) -> date:
    last_day = calendar.monthrange(value.year, value.month)[1]
    return value.replace(day=last_day)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(year=value.year + years, day=28)


def _latest_complete_period_end(period_type: str, anchor: date) -> date:
    if period_type == "day":
        return anchor - timedelta(days=1)
    if period_type == "week":
        return anchor - timedelta(days=anchor.isoweekday())
    if period_type == "month":
        return date(anchor.year, anchor.month, 1) - timedelta(days=1)
    if period_type == "year":
        return date(anchor.year, 1, 1) - timedelta(days=1)
    raise PipelineError(f"Unsupported period_type: {period_type}")


def _align_period_end_on_or_before(value: date, period_type: str) -> date:
    if period_type == "day":
        return value
    if period_type == "week":
        return value - timedelta(days=value.isoweekday() % 7)
    if period_type == "month":
        last_day = calendar.monthrange(value.year, value.month)[1]
        if value.day == last_day:
            return value
        return date(value.year, value.month, 1) - timedelta(days=1)
    if period_type == "year":
        if value.month == 12 and value.day == 31:
            return value
        return date(value.year, 1, 1) - timedelta(days=1)
    raise PipelineError(f"Unsupported period_type: {period_type}")


def _align_period_start_on_or_after(value: date, period_type: str) -> date:
    if period_type == "day":
        return value
    if period_type == "week":
        if value.isoweekday() == 1:
            return value
        return value + timedelta(days=8 - value.isoweekday())
    if period_type == "month":
        if value.day == 1:
            return value
        return _next_month(date(value.year, value.month, 1))
    if period_type == "year":
        if value.month == 1 and value.day == 1:
            return value
        return date(value.year + 1, 1, 1)
    raise PipelineError(f"Unsupported period_type: {period_type}")


def _day_period_date(period: str) -> date:
    normalized = (
        f"{period[:4]}-{period[4:6]}-{period[6:]}" if re.fullmatch(r"\d{8}", period) else period
    )
    return parse_date(normalized)
