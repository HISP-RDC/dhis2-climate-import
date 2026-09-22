import unittest

from dhis2_climate_import.periods import (
    date_window_period_bounds,
    month_bounds,
    period_labels_between,
    period_range_bounds,
    periods_between,
    relative_year_window,
    week_bounds,
)


class PeriodTests(unittest.TestCase):
    def test_daily_periods(self) -> None:
        self.assertEqual(
            periods_between("2024-01-30", "2024-02-01", "day"),
            ["20240130", "20240131", "20240201"],
        )

    def test_monthly_periods(self) -> None:
        self.assertEqual(
            periods_between("2024-01-15", "2024-03-01", "month"),
            ["202401", "202402", "202403"],
        )

    def test_month_bounds(self) -> None:
        self.assertEqual(month_bounds("2024-02"), ("2024-02-01", "2024-02-29"))

    def test_week_bounds(self) -> None:
        self.assertEqual(week_bounds("2026-W09"), ("2026-02-23", "2026-03-01"))
        self.assertEqual(week_bounds("2026W31"), ("2026-07-27", "2026-08-02"))

    def test_weekly_period_range(self) -> None:
        self.assertEqual(
            period_range_bounds("2026-W09", "2026-W31", "week"),
            ("2026-02-23", "2026-08-02"),
        )
        self.assertEqual(
            period_labels_between("2026-02-23", "2026-08-02", "week")[0],
            "2026-W09",
        )
        self.assertEqual(len(periods_between("2026-02-23", "2026-08-02", "week")), 23)

    def test_weekly_date_window_expands_to_full_calendar_weeks(self) -> None:
        self.assertEqual(
            date_window_period_bounds("2026-03-21", "2026-08-21", "week"),
            ("2026-03-16", "2026-08-23"),
        )
        self.assertEqual(len(periods_between("2026-03-16", "2026-08-23", "week")), 23)

    def test_relative_year_window_uses_latest_complete_period(self) -> None:
        self.assertEqual(
            relative_year_window(10, "day", as_of="2026-08-27"),
            ("2016-08-27", "2026-08-26"),
        )
        self.assertEqual(
            relative_year_window(10, "week", as_of="2026-08-27"),
            ("2016-08-29", "2026-08-23"),
        )
        self.assertEqual(
            relative_year_window(10, "month", as_of="2026-08-27"),
            ("2016-08-01", "2026-07-31"),
        )
        self.assertEqual(
            relative_year_window(10, "year", as_of="2026-08-27"),
            ("2016-01-01", "2025-12-31"),
        )

    def test_relative_year_window_can_anchor_on_end_period(self) -> None:
        self.assertEqual(
            relative_year_window(7, "week", end_period="2026-W31"),
            ("2019-08-05", "2026-08-02"),
        )


if __name__ == "__main__":
    unittest.main()
