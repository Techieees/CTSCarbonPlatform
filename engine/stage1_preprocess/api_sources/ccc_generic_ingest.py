from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

from config import CCC_GET_ENDPOINTS_PATH, STAGE1_INPUT_DIR
from engine.stage1_preprocess.api_sources import ccc_client

CCC_YEAR_FILTER_DEFAULT = "2026"
CCC_YEAR_DATE_FIELDS = (
    "createdOn",
    "createdAt",
    "purchaseDate",
    "orderDate",
    "documentDate",
    "invoiceDate",
    "date",
    "updatedOn",
    "year",
    "orderYear",
    "reportingYear",
    "createdYear",
    "invoiceYear",
)


def load_endpoint_config() -> dict[str, str]:
    try:
        data = json.loads(CCC_GET_ENDPOINTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}


def _sanitize_endpoint_name(endpoint_name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(endpoint_name or "").strip().lower())
    return safe.strip("_") or "endpoint"


def _timestamped_output_path(endpoint_name: str) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H%M")
    candidate = STAGE1_INPUT_DIR / f"ccc_{_sanitize_endpoint_name(endpoint_name)}_{ts}.xlsx"
    if not candidate.exists():
        return candidate
    ts_precise = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return STAGE1_INPUT_DIR / f"ccc_{_sanitize_endpoint_name(endpoint_name)}_{ts_precise}.xlsx"


def _parse_json_arg(value: str | dict[str, Any] | None, field_name: str) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    try:
        data = json.loads(str(value))
    except Exception as exc:
        raise RuntimeError(f"{field_name} must be valid JSON object.") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"{field_name} must be a JSON object.")
    return data


def _strip(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_year_filter(year_filter: Any) -> str:
    raw = _strip(year_filter or CCC_YEAR_FILTER_DEFAULT).lower()
    if raw == "all":
        return "all"
    try:
        year = int(raw)
    except Exception:
        return CCC_YEAR_FILTER_DEFAULT
    return str(year) if 1900 <= year <= 2200 else CCC_YEAR_FILTER_DEFAULT


def _query_params_with_year_filter(query_params: dict[str, Any], year_filter: Any) -> dict[str, Any]:
    params = {str(key): val for key, val in query_params.items() if val is not None and _strip(val) != ""}
    selected_year = _coerce_year_filter(year_filter)
    if selected_year != "all":
        params["year"] = selected_year
    return params


def _get_nested(payload: Any, path: str) -> Any:
    current = payload
    for part in str(path or "").split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _extract_year_token(value: Any) -> str:
    raw = _strip(value)
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 4:
        return raw
    parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return raw[:4] if len(raw) >= 4 and raw[:4].isdigit() else ""
    return str(parsed.year)


def _record_matches_year_filter(row: Any, year_filter: Any) -> bool:
    selected_year = _coerce_year_filter(year_filter)
    if selected_year == "all" or not isinstance(row, dict):
        return True
    saw_year_field = False
    for field in CCC_YEAR_DATE_FIELDS:
        value = _get_nested(row, field)
        if value in (None, ""):
            continue
        saw_year_field = True
        if _extract_year_token(value) == selected_year:
            return True
    return not saw_year_field


def _filter_records_by_year(rows: list[Any], year_filter: Any) -> list[Any]:
    selected_year = _coerce_year_filter(year_filter)
    if selected_year == "all":
        return list(rows)
    return [row for row in rows if _record_matches_year_filter(row, selected_year)]


def _format_endpoint_path(template_path: str, path_params: dict[str, Any]) -> str:
    try:
        return str(template_path).format(**path_params)
    except KeyError as exc:
        raise RuntimeError(f"Missing path parameter for endpoint: {exc.args[0]}") from exc


def _flatten_records(rows: list[Any], endpoint_name: str) -> pd.DataFrame:
    import_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not rows:
        return pd.DataFrame(columns=["source", "endpoint_name", "import_timestamp"])
    normalized = pd.json_normalize(rows, sep=".")
    normalized.insert(0, "import_timestamp", import_timestamp)
    normalized.insert(0, "endpoint_name", endpoint_name)
    normalized.insert(0, "source", "CCC_API")
    return normalized


def ingest_endpoint(
    endpoint_name: str,
    *,
    base_url: str | None = None,
    username: str | None = None,
    password: str | None = None,
    page_size: int | None = None,
    path_params: str | dict[str, Any] | None = None,
    query_params: str | dict[str, Any] | None = None,
    year_filter: Any = CCC_YEAR_FILTER_DEFAULT,
) -> dict[str, Any]:
    endpoints = load_endpoint_config()
    if endpoint_name not in endpoints:
        raise RuntimeError(f"Unknown CCC GET endpoint: {endpoint_name}")
    endpoint_path = _format_endpoint_path(endpoints[endpoint_name], _parse_json_arg(path_params, "Path params"))
    parsed_query_params = _parse_json_arg(query_params, "Query params")
    selected_year = _coerce_year_filter(year_filter)
    try:
        rows = ccc_client.get_paginated(
            endpoint_path,
            base_url=base_url,
            username=username,
            password=password,
            query_params=_query_params_with_year_filter(parsed_query_params, selected_year),
            page_size=page_size,
        )
    except RuntimeError:
        if selected_year == "all":
            raise
        rows = ccc_client.get_paginated(
            endpoint_path,
            base_url=base_url,
            username=username,
            password=password,
            query_params=parsed_query_params,
            page_size=page_size,
        )
    rows_before_year_filter = len(rows)
    rows = _filter_records_by_year(rows, selected_year)
    df = _flatten_records(rows, endpoint_name)
    output_path = _timestamped_output_path(endpoint_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=_sanitize_endpoint_name(endpoint_name)[:31], index=False)
    return {
        "endpoint": endpoint_name,
        "records_imported": int(len(df.index)),
        "records_before_year_filter": int(rows_before_year_filter),
        "year_filter": _coerce_year_filter(year_filter),
        "output_path": output_path,
        "output_file": output_path.name,
        "status": "success",
    }
