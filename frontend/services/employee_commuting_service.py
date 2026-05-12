"""
Scope 3 Category 7 — Employee commuting dataset helpers.

Formulas (aligned with engine/stage2_mapping/main_mapping.py national-average preprocessing):
- km_per_day = average_one_day_km_one_way * 2
- km_per_month = km_per_day * 20  (assumes ~20 commute days / month)
- Headcount is split across transport modes using national mode shares (%), with
  largest-remainder allocation so integer person counts sum exactly to headcount.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

from frontend.services.reporting_period_service import normalize_reporting_period

EMPLOYEE_COMMUTING_DATA_SOURCE = "Employee Commuting"
EMPLOYEE_COMMUTING_TARGET_SHEET = "Scope 3 Cat 7 Employee Commute"
EMPLOYEE_COMMUTING_DISPLAY_CATEGORY_NAME = "Scope 3 Category 7 Employee Commuting"
EMPLOYEE_COMMUTING_DEDUP_COLUMN = "employee_commuting_import_dedup"

# Enterprise guardrail: reject absurd payloads (still allows large national enterprises).
EMPLOYEE_COMMUTING_MAX_HEADCOUNT = 1_500_000

# SQLite: cap sequential mapping targets per generation (umbrella job); excess is skipped with warning.
EMPLOYEE_COMMUTING_MAX_MAPPING_JOBS = 10

ENTRY_ROW_KEYS_IN_ORDER: tuple[str, ...] = (
    "Source_File",
    "Country",
    "Reporting period (month, year)",
    "Mode of Transport",
    "km travelled one way",
    "km travelled per day",
    "km travelled per month",
    "Synthetic",
    "Synthetic_Record_Note",
    "Data_Type",
    EMPLOYEE_COMMUTING_DEDUP_COLUMN,
    "Site Tag",
    "Data Source",
)


def stable_employee_commuting_dedup_value(
    *,
    company_key: str,
    period_yyyy_mm: str,
    mode_name: str,
    data_type_label: str,
    category_sheet_key: str,
    slot_within_mode: int,
) -> str:
    """
    Stable value for Data Entry column `employee_commuting_import_dedup`.

    Must be identical across repeated "Generate Dataset" runs for the same logical
    synthetic line so the importer skips duplicates safely (see app.py pre-check +
    Data Entry fingerprint).

    Fingerprint includes:
    - company (normalized)
    - calendar month (YYYY-MM)
    - commute mode (allocation bucket)
    - Data_Type (source archetype, e.g. National average)
    - category (internal template sheet key — not user-facing display name)
    - slot_within_mode — 1..N synthetic rows for that company/month/mode from headcount split

    Volatile fields (background job id, wall-clock) MUST NOT be part of this hash.
    """
    pipe = "|".join(
        [
            str(company_key or "").strip().lower(),
            str(period_yyyy_mm or "").strip(),
            str(mode_name or "").strip().lower(),
            str(data_type_label or "").strip().lower(),
            str(category_sheet_key or "").strip(),
            str(int(slot_within_mode)),
        ]
    )
    return hashlib.sha256(pipe.encode("utf-8")).hexdigest()[:40]


def period_key_from_label(label: object) -> str | None:
    """Map Jan-YYYY (any supported calendar year) to YYYY-MM; empty if unknown."""
    s = normalize_reporting_period(label)
    if not s:
        return None
    m = re.match(r"^(?P<mon>[A-Za-z]{3})[\s'\-]*(?P<year>\d{4})\s*$", str(s).strip())
    if not m:
        return None
    mon = m.group("mon").title()
    year = int(m.group("year"))
    month_map = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
    }
    mi = month_map.get(mon[:3].title() if len(mon) >= 3 else mon)
    if not mi or year < 1990 or year > 2100:
        return None
    return f"{year:04d}-{mi:02d}"


def reporting_date_for_period_key(period_key: str) -> str:
    """First day of month as YYYY-MM-DD for Data Entry date normalization."""
    raw = (period_key or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}", raw):
        y, mo = raw.split("-", 1)
        return f"{int(y):04d}-{int(mo):02d}-01"
    from datetime import datetime as _dt

    return _dt.utcnow().replace(day=1).strftime("%Y-%m-%d")


def sort_period_keys(keys: Sequence[str]) -> list[str]:
    def keyf(k: str) -> tuple[int, int]:
        raw = str(k).strip()
        if re.fullmatch(r"\d{4}-\d{2}", raw):
            y, m = raw.split("-", 1)
            return int(y), int(m)
        return 9999, 99

    return sorted({str(k).strip() for k in keys if str(k).strip()}, key=keyf)


def allocate_mode_counts(headcount: int, ratios: Mapping[str, float]) -> dict[str, int]:
    """Largest-remainder allocation; ratios are 0–100 scale."""
    if headcount <= 0:
        return {}
    raw = {mode: (headcount * max(0.0, float(ratio)) / 100.0) for mode, ratio in ratios.items()}
    counts = {mode: int(math.floor(val)) for mode, val in raw.items()}
    remainder = max(0, int(headcount - sum(counts.values())))
    order = sorted(raw.keys(), key=lambda mode: (raw[mode] - counts[mode], raw[mode], mode), reverse=True)
    for idx in range(remainder):
        counts[order[idx % len(order)]] += 1
    return {k: v for k, v in counts.items() if v > 0}


def normalize_headcount_rows(rows: object) -> list[dict[str, Any]]:
    """Validate list of {company_name, month|reporting_period_key, headcount}."""
    if not isinstance(rows, list):
        raise ValueError("Rows payload must be a list.")
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            continue
        company = str(raw.get("company_name") or "").strip()
        hc_raw = raw.get("headcount")
        month_label = str(raw.get("month") or raw.get("reporting_period_label") or "").strip()
        month_key = str(raw.get("reporting_period_key") or "").strip()
        if not company and hc_raw in (None, "") and not month_label and not month_key:
            continue
        if not company:
            raise ValueError(f"Row {idx}: Company name is required.")
        if not month_label and not month_key:
            raise ValueError(f"Row {idx}: Month is required.")
        if month_key and not re.fullmatch(r"\d{4}-\d{2}", month_key):
            raise ValueError(f"Row {idx}: Invalid reporting period key.")
        if not month_key:
            mk = period_key_from_label(month_label)
            if not mk:
                raise ValueError(
                    f"Row {idx}: Month must be a valid reporting period label (for example Jan-2026 or Mar-2029)."
                )
            month_key = mk
            month_label = normalize_reporting_period(month_label) or month_label
        else:
            if not month_label:
                try:
                    y, m = month_key.split("-", 1)
                    from calendar import month_abbr

                    month_label = f"{month_abbr[int(m)]}-{int(y)}"
                except Exception:
                    month_label = month_key
        try:
            hc = int(round(float(hc_raw)))
        except (TypeError, ValueError):
            raise ValueError(f"Row {idx}: Headcount must be numeric.")
        if hc < 0:
            raise ValueError(f"Row {idx}: Headcount cannot be negative.")
        if hc > EMPLOYEE_COMMUTING_MAX_HEADCOUNT:
            raise ValueError(
                f"Row {idx}: Headcount exceeds maximum allowed ({EMPLOYEE_COMMUTING_MAX_HEADCOUNT:,})."
            )
        dedup = (company.lower(), month_key)
        if dedup in seen:
            raise ValueError(f"Row {idx}: Duplicate company/month `{company}` / `{month_key}`.")
        seen.add(dedup)
        out.append(
            {
                "company_name": company,
                "headcount": hc,
                "reporting_period_key": month_key,
                "reporting_period_label": month_label,
            }
        )
    return out


def build_commuting_row_value_map(
    *,
    company_name: str,
    country: str,
    period_key: str,
    mode_name: str,
    average_one_day_km: float,
    slot_within_mode: int,
    data_type_label: str = "National average",
    generated_run_id: int | None = None,
    generated_by_user_id: int | None = None,
    generated_at_iso: str = "",
    generation_job_id: str = "",
) -> dict[str, str]:
    """Semantic column map; caller maps to resolved template header order."""
    km_one = round(float(average_one_day_km), 2)
    km_day = round(km_one * 2, 2)
    km_month = round(km_day * 20, 2)
    rep_date = reporting_date_for_period_key(period_key)
    dedup = stable_employee_commuting_dedup_value(
        company_key=str(company_name or "").strip(),
        period_yyyy_mm=str(period_key or "").strip(),
        mode_name=str(mode_name or "").strip(),
        data_type_label=str(data_type_label or "").strip(),
        category_sheet_key=EMPLOYEE_COMMUTING_TARGET_SHEET,
        slot_within_mode=int(slot_within_mode),
    )
    audit_bits = [
        f"dedup={dedup}",
        f"run_db_id={int(generated_run_id)}" if generated_run_id is not None else "",
        f"gen_user={int(generated_by_user_id)}" if generated_by_user_id is not None else "",
        f"gen_at={generated_at_iso}" if generated_at_iso else "",
        f"bg_job={generation_job_id}" if generation_job_id else "",
    ]
    audit_tail = " ".join(b for b in audit_bits if b)
    base_note = (
        f"Generated for {EMPLOYEE_COMMUTING_DISPLAY_CATEGORY_NAME} "
        f"via {EMPLOYEE_COMMUTING_DATA_SOURCE} (template: {EMPLOYEE_COMMUTING_TARGET_SHEET})"
    )
    synthetic_note = f"{base_note} | {audit_tail}" if audit_tail else base_note
    return {
        "Source_File": company_name,
        "Country": country,
        "Reporting period (month, year)": rep_date,
        "Mode of Transport": mode_name,
        "km travelled one way": str(km_one),
        "km travelled per day": str(km_day),
        "km travelled per month": str(km_month),
        "Synthetic": "TRUE",
        "Synthetic_Record_Note": synthetic_note,
        "Data_Type": str(data_type_label or "").strip() or "National average",
        EMPLOYEE_COMMUTING_DEDUP_COLUMN: dedup,
        "Site Tag": "",
        "Data Source": EMPLOYEE_COMMUTING_DATA_SOURCE,
    }


def map_values_to_headers(headers: Sequence[str], value_map: Mapping[str, str]) -> list[str]:
    padded: list[str] = []
    for h in headers:
        key = str(h or "").strip()
        padded.append(str(value_map.get(key, "") or "").strip())
    return padded


def emission_activity_tco2e(*, km_travelled_per_month: float, ef_value: float | None) -> float | None:
    """
    Transparency helper: downstream mapping calculates tCO₂e as km/month × ef_value
    when the EF applies per kilometre travelled (see calculate_me_the_chosen_one.py).
    ef_value comes from mapped emission-factor tables.
    """
    if ef_value is None:
        return None
    return float(km_travelled_per_month) * float(ef_value)


def generate_employee_commuting_rows(
    *,
    headcount_rows: Sequence[Mapping[str, Any]],
    country_averages: Sequence[Mapping[str, Any]],
    company_to_country: Mapping[str, str | None],
    generated_run_id: int | None = None,
    generated_by_user_id: int | None = None,
    generated_at_iso: str = "",
    generation_job_id: str = "",
) -> tuple[list[tuple[str, dict[str, str]]], dict[str, Any]]:
    """
    Returns list of (resolved_company, header→value row map) and a stats dict.
    country_averages: rows with country, average_one_day, car_pct, bus_pct, walking_and_cycling_pct, mixed_pct
    """
    by_country: dict[str, dict[str, Any]] = {}
    for row in country_averages:
        c = str(row.get("country") or "").strip()
        if not c:
            continue
        by_country[c.lower()] = dict(row)

    stats: dict[str, Any] = {
        "rows_written": 0,
        "companies_skipped_no_country": [],
        "companies_skipped_no_average": [],
        "warnings": [],
    }
    out: list[tuple[str, dict[str, str]]] = []

    for hr in headcount_rows:
        company_raw = str(hr.get("company_name") or "").strip()
        canon = str(hr.get("canonical_company") or company_raw).strip()
        period_key = str(hr.get("reporting_period_key") or "").strip()
        hc = int(hr.get("headcount") or 0)
        if not canon or not period_key or hc <= 0:
            continue
        country = company_to_country.get(canon) or company_to_country.get(company_raw)
        if not country:
            stats["companies_skipped_no_country"].append(canon)
            continue
        avg_row = by_country.get(str(country).strip().lower())
        if not avg_row:
            stats["companies_skipped_no_average"].append(f"{canon} ({country})")
            continue

        try:
            km_one = float(avg_row.get("average_one_day"))
        except (TypeError, ValueError):
            stats["warnings"].append(f"Invalid average_one_day for country {country}")
            continue

        mode_counts = allocate_mode_counts(
            hc,
            {
                "Car": float(avg_row.get("car_pct") or 0),
                "Bus": float(avg_row.get("bus_pct") or 0),
                "Walking and Cycling": float(avg_row.get("walking_and_cycling_pct") or 0),
                "Mixed": float(avg_row.get("mixed_pct") or 0),
            },
        )
        # Deterministic mode iteration so slot indices are stable across runs.
        for mode_name in sorted(mode_counts.keys(), key=lambda m: m.lower()):
            count = int(mode_counts[mode_name] or 0)
            for slot in range(1, count + 1):
                row_map = build_commuting_row_value_map(
                    company_name=canon,
                    country=str(country).strip(),
                    period_key=period_key,
                    mode_name=mode_name,
                    average_one_day_km=km_one,
                    slot_within_mode=slot,
                    data_type_label="National average",
                    generated_run_id=generated_run_id,
                    generated_by_user_id=generated_by_user_id,
                    generated_at_iso=generated_at_iso,
                    generation_job_id=generation_job_id,
                )
                out.append((canon, row_map))

    stats["rows_written"] = len(out)
    return out, stats
