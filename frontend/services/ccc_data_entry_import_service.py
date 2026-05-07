"""
CCC API purchase-order rows → Data Entry (Scope 3 Category 1) transformation helpers.

Orchestration, persistence, and jobs live in app.py; this module is transform + CCC fetch wiring only.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from frontend.services.reporting_period_service import normalize_reporting_period
from frontend.services.site_tag_service import normalize_site_tag

CCC_DATA_SOURCE_LABEL = "CCC API Purchase Orders"
CCC_TARGET_SHEET = "Scope 3 Category 1 Purchased Goods & Services"
CCC_IMPORT_DEDUP_COLUMN = "ccc_import_dedup"

_LOG_MAX = 140


def _log_safe(message: object) -> str:
    text = str(message or "").replace("\n", " ").strip()
    if len(text) <= _LOG_MAX:
        return text
    return text[: _LOG_MAX - 3] + "..."


def log_import(message: object) -> None:
    print(f"[CCC_IMPORT] {_log_safe(message)}")


def log_warning(message: object) -> None:
    print(f"[CCC_IMPORT_WARNING] {_log_safe(message)}")


def log_duplicate(message: object) -> None:
    print(f"[CCC_IMPORT_DUPLICATE] {_log_safe(message)}")


def log_inserted(message: object) -> None:
    print(f"[CCC_IMPORT_INSERTED] {_log_safe(message)}")


def resolve_ccc_company(registration: Mapping[str, str] | None) -> str:
    if not registration:
        return ""
    return normalize_site_tag(registration.get("responsible_company"))


def resolve_ccc_site_tag(registration: Mapping[str, str] | None) -> str:
    if not registration:
        return ""
    return normalize_site_tag(registration.get("platform_site_tag"))


def normalize_ccc_reporting_period(value: object) -> str:
    return normalize_reporting_period(value)


def _normalize_price_token(total_price: object) -> str:
    if total_price is None:
        return ""
    try:
        if pd.isna(total_price):
            return ""
    except Exception:
        pass
    if isinstance(total_price, (int, float)):
        f = float(total_price)
        if not math.isfinite(f):
            return ""
        text = f"{f:.12f}".rstrip("0").rstrip(".")
        return text.lower()
    s = normalize_site_tag(str(total_price)).lower().replace(",", ".")
    return s


def generate_ccc_dedup_key(
    project_code: object,
    purchase_order: object,
    supplier: object,
    total_price: object,
) -> str:
    raw = "|".join(
        [
            str(project_code or "").strip().lower(),
            str(purchase_order or "").strip().lower(),
            str(supplier or "").strip().lower(),
            _normalize_price_token(total_price),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _fold_project_label(label: str) -> str:
    return re.sub(r"\s+", " ", str(label or "").strip()).casefold()


def resolve_ccc_project_id(projects: Sequence[dict[str, Any]], project_label: str) -> int | None:
    want = _fold_project_label(project_label)
    if not want:
        return None
    for p in projects:
        lab = str(p.get("label") or "").strip()
        if _fold_project_label(lab) == want:
            try:
                return int(p.get("id"))
            except (TypeError, ValueError):
                continue
    return None


def format_spend_for_data_entry(total_price: object) -> str:
    if total_price is None:
        return ""
    try:
        if pd.isna(total_price):
            return ""
    except Exception:
        pass
    if isinstance(total_price, (int, float)):
        f = float(total_price)
        if not math.isfinite(f):
            return ""
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        text = f"{f:.10f}".rstrip("0").rstrip(".")
        return text
    return normalize_site_tag(str(total_price))


def transform_ccc_row_to_data_entry(
    headers: Sequence[str],
    *,
    supplier: str,
    description: str,
    category: str,
    spend: str,
    currency: str,
    country: str,
    site_tag: str,
    reporting_period: str,
    data_source: str = CCC_DATA_SOURCE_LABEL,
) -> list[str]:
    mapping = {
        "Reporting period (month, year)": reporting_period,
        "Supplier": supplier,
        "Description": description,
        "Category": category,
        "Spend": spend,
        "Currency": currency,
        "Country": country,
        "Site Tag": site_tag,
        "Data Source": data_source,
    }
    return [mapping.get(str(h or "").strip(), "") for h in headers]


def prepare_ccc_data_entry_rows(
    df: pd.DataFrame,
    headers: Sequence[str],
    *,
    registration: Mapping[str, str],
) -> list[tuple[str, list[str]]]:
    site_tag = resolve_ccc_site_tag(registration)
    country = normalize_site_tag(registration.get("project_location"))
    rows_out: list[tuple[str, list[str]]] = []
    if df.empty:
        return rows_out
    for _, row in df.iterrows():
        dedup_key = generate_ccc_dedup_key(
            row.get("Project Code"),
            row.get("Purchase Order"),
            row.get("Supplier"),
            row.get("Total Price"),
        )
        rp = normalize_ccc_reporting_period(row.get("CreatedOn"))
        cells = transform_ccc_row_to_data_entry(
            headers,
            supplier=normalize_site_tag(row.get("Supplier")),
            description=normalize_site_tag(row.get("Description")),
            category=normalize_site_tag(row.get("Category")),
            spend=format_spend_for_data_entry(row.get("Total Price")),
            currency=normalize_site_tag(row.get("Currency")),
            country=country,
            site_tag=site_tag,
            reporting_period=rp,
            data_source=CCC_DATA_SOURCE_LABEL,
        )
        rows_out.append((dedup_key, cells))
    return rows_out
