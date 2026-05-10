"""Reporting period normalization for Data Entry (2026 canonical labels)."""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime

_WS_RE = re.compile(r"\s+")

_MONTH_ABBREV = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)

# Canonical *storage* labels for 2026: machine-parsable, no typographic apostrophe (Jan-2026).
# Filtering/sorting should use parsed keys (YYYY-MM), not string collation on labels.
_CANONICAL_PERIODS = [f"{abbr}-2026" for abbr in _MONTH_ABBREV]
_LEGACY_APOSTROPHE_PERIODS = [f"{abbr}'-2026" for abbr in _MONTH_ABBREV]

_MONTH_NAME_TO_NUM = {
    **{calendar.month_name[i].lower(): i for i in range(1, 13)},
    **{calendar.month_abbr[i].lower(): i for i in range(1, 13)},
}

_MONTH_TOKEN_RE = re.compile(
    r"^(?P<mon>[A-Za-z]+)[\s'\-]*(?P<year>\d{4})\s*$",
)
_ISO_MONTH_RE = re.compile(r"^(?P<year>\d{4})\s*[-/]\s*(?P<month>\d{1,2})\s*$")
_SLASH_MY_RE = re.compile(r"^(?P<month>\d{1,2})\s*/\s*(?P<year>\d{4})\s*$")
_SLASH_DMY_RE = re.compile(r"^(?P<day>\d{1,2})\s*/\s*(?P<month>\d{1,2})\s*/\s*(?P<year>\d{4})\s*$")
_SLASH_YMD_RE = re.compile(r"^(?P<year>\d{4})\s*/\s*(?P<month>\d{1,2})\s*/\s*(?P<day>\d{1,2})\s*$")


def get_reporting_period_options_2026() -> list[str]:
    """Exactly twelve canonical labels for calendar months in 2026 (chronological order)."""
    return list(_CANONICAL_PERIODS)


def _canonical_for_month(month: int, year: int) -> str | None:
    if year != 2026 or month < 1 or month > 12:
        return None
    return _CANONICAL_PERIODS[month - 1]


def display_reporting_period_label(value: object) -> str:
    """
    User-facing label: Jan-2026 (no apostrophe). Accepts legacy Jan'-2026 stored in older rows.
    Internal values remain unchanged when already canonical.
    """
    if value is None:
        return ""
    s = _WS_RE.sub(" ", str(value).strip())
    if not s:
        return ""
    # Normalize legacy apostrophe forms to hyphen form for display only.
    for legacy, modern in zip(_LEGACY_APOSTROPHE_PERIODS, _CANONICAL_PERIODS):
        if s.casefold() == legacy.casefold():
            return modern
    return s.replace("'-", "-").replace("'-'-", "-").replace("'", "")


def reporting_period_sort_key(label: object) -> tuple[int, int]:
    """
    Chronological sort key for dropdowns: (year, month). Non-resolved values sort last.
    """
    z = normalize_reporting_period(label)
    m = _MONTH_TOKEN_RE.match(str(z).strip())
    if m:
        mon_raw = m.group("mon").lower()
        year = int(m.group("year"))
        month_num = _MONTH_NAME_TO_NUM.get(mon_raw)
        if month_num is None and len(mon_raw) >= 3:
            month_num = _MONTH_NAME_TO_NUM.get(mon_raw[:3])
        if month_num:
            return year, month_num
    m2 = _ISO_MONTH_RE.match(str(z).strip())
    if m2:
        return int(m2.group("year")), int(m2.group("month"))
    return 9999, 99


def normalize_reporting_period(value: object) -> str:
    """
    Normalize common inputs to Jan-2026 … Dec-2026 when year/month resolves to 2026.
    Otherwise returns stripped / whitespace-collapsed original string (never raises).

    Legacy apostrophe forms (Jan'-2026) are normalized to Jan-2026 so duplicate malformed
    variants collapse for display and deduping, while month/year semantics stay sortable.
    """
    if value is None:
        return ""

    if isinstance(value, datetime):
        return _canonical_for_month(value.month, value.year) or normalize_reporting_period(value.date())

    if isinstance(value, date):
        c = _canonical_for_month(value.month, value.year)
        return c if c else ""

    s = _WS_RE.sub(" ", str(value).strip())
    if not s:
        return ""

    for p in _CANONICAL_PERIODS:
        if s.casefold() == p.casefold():
            return p
    for legacy, modern in zip(_LEGACY_APOSTROPHE_PERIODS, _CANONICAL_PERIODS):
        if s.casefold() == legacy.casefold():
            return modern

    m = _MONTH_TOKEN_RE.match(s)
    if m:
        mon_raw = m.group("mon").lower()
        year = int(m.group("year"))
        month_num = _MONTH_NAME_TO_NUM.get(mon_raw)
        if month_num is None and len(mon_raw) >= 3:
            month_num = _MONTH_NAME_TO_NUM.get(mon_raw[:3])
        if month_num:
            c = _canonical_for_month(month_num, year)
            if c:
                return c
        return display_reporting_period_label(s)

    m = _ISO_MONTH_RE.match(s)
    if m:
        year = int(m.group("year"))
        month = int(m.group("month"))
        c = _canonical_for_month(month, year)
        if c:
            return c
        return s

    m = _SLASH_MY_RE.match(s)
    if m:
        month = int(m.group("month"))
        year = int(m.group("year"))
        c = _canonical_for_month(month, year)
        if c:
            return c
        return s

    m = _SLASH_DMY_RE.match(s)
    if m:
        month = int(m.group("month"))
        year = int(m.group("year"))
        c = _canonical_for_month(month, year)
        if c:
            return c
        return s

    m = _SLASH_YMD_RE.match(s)
    if m:
        month = int(m.group("month"))
        year = int(m.group("year"))
        c = _canonical_for_month(month, year)
        if c:
            return c
        return s

    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            c = _canonical_for_month(dt.month, dt.year)
            if c:
                return c
        except ValueError:
            continue

    return display_reporting_period_label(s)
