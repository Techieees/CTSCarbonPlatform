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

_CANONICAL_PERIODS = [f"{abbr}'-2026" for abbr in _MONTH_ABBREV]

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
    """Exactly twelve canonical labels for calendar months in 2026."""
    return list(_CANONICAL_PERIODS)


def _canonical_for_month(month: int, year: int) -> str | None:
    if year != 2026 or month < 1 or month > 12:
        return None
    return _CANONICAL_PERIODS[month - 1]


def normalize_reporting_period(value: object) -> str:
    """
    Normalize common inputs to Jan'-2026 … Dec'-2026 when year/month resolves to 2026.
    Otherwise returns stripped / whitespace-collapsed original string (never raises).
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
        return s

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

    return s
