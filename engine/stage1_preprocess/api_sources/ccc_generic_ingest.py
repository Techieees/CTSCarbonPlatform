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
) -> dict[str, Any]:
    endpoints = load_endpoint_config()
    if endpoint_name not in endpoints:
        raise RuntimeError(f"Unknown CCC GET endpoint: {endpoint_name}")
    endpoint_path = _format_endpoint_path(endpoints[endpoint_name], _parse_json_arg(path_params, "Path params"))
    rows = ccc_client.get_paginated(
        endpoint_path,
        base_url=base_url,
        username=username,
        password=password,
        query_params=_parse_json_arg(query_params, "Query params"),
        page_size=page_size,
    )
    df = _flatten_records(rows, endpoint_name)
    output_path = _timestamped_output_path(endpoint_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=_sanitize_endpoint_name(endpoint_name)[:31], index=False)
    return {
        "endpoint": endpoint_name,
        "records_imported": int(len(df.index)),
        "output_path": output_path,
        "output_file": output_path.name,
        "status": "success",
    }
