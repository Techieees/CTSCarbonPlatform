"""Unit tests: site tags + reporting period helpers (Data Entry foundation)."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from frontend.services.reporting_period_service import (
    get_reporting_period_options_2026,
    normalize_reporting_period,
)
from frontend.services.site_tag_service import (
    get_site_tags_for_company,
    normalize_site_tag,
    resolve_site_tag_from_project_name,
)


class SiteTagServiceTests(unittest.TestCase):
    def test_resolve_ccc_style_project_name(self) -> None:
        self.assertEqual(
            resolve_site_tag_from_project_name("30 - PRO3021 3021 Verne Vantaa"),
            "3021 Verne Vantaa",
        )

    def test_resolve_without_numeric_prefix(self) -> None:
        self.assertEqual(
            resolve_site_tag_from_project_name("PRO3021 3021 Verne Vantaa"),
            "3021 Verne Vantaa",
        )

    def test_resolve_unknown_returns_normalized(self) -> None:
        raw = "  Unknown Site XYZ  "
        self.assertEqual(resolve_site_tag_from_project_name(raw), normalize_site_tag(raw))

    def test_get_site_tags_finland(self) -> None:
        tags = get_site_tags_for_company("CTS Finland")
        self.assertIn("3021 Verne Vantaa", tags)
        self.assertIn("3016 Kajaani", tags)
        self.assertGreaterEqual(len(tags), 5)


class ReportingPeriodServiceTests(unittest.TestCase):
    def test_options_2026_exact(self) -> None:
        expected = [
            "Jan'-2026",
            "Feb'-2026",
            "Mar'-2026",
            "Apr'-2026",
            "May'-2026",
            "Jun'-2026",
            "Jul'-2026",
            "Aug'-2026",
            "Sep'-2026",
            "Oct'-2026",
            "Nov'-2026",
            "Dec'-2026",
        ]
        self.assertEqual(get_reporting_period_options_2026(), expected)

    def test_normalize_iso_month(self) -> None:
        self.assertEqual(normalize_reporting_period("2026-04"), "Apr'-2026")

    def test_normalize_slash_dmy(self) -> None:
        self.assertEqual(normalize_reporting_period("15/04/2026"), "Apr'-2026")

    def test_normalize_slash_ymd(self) -> None:
        self.assertEqual(normalize_reporting_period("2026/04/15"), "Apr'-2026")

    def test_normalize_month_name(self) -> None:
        self.assertEqual(normalize_reporting_period("January 2026"), "Jan'-2026")

    def test_normalize_date_objects(self) -> None:
        self.assertEqual(normalize_reporting_period(date(2026, 3, 7)), "Mar'-2026")
        self.assertEqual(normalize_reporting_period(datetime(2026, 11, 1, 12, 0)), "Nov'-2026")

    def test_non_2026_returns_cleaned_string(self) -> None:
        self.assertEqual(normalize_reporting_period("Jan 2025"), "Jan 2025")


if __name__ == "__main__":
    unittest.main()
