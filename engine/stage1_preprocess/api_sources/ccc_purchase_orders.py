from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, parse, request

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    CCC_API_BASE_URL,
    CCC_API_PAGE_SIZE,
    CCC_PASSWORD,
    CCC_SHEET_MAPPING_PATH,
    CCC_USERNAME,
    STAGE1_INPUT_DIR,
)


DEFAULT_TIMEOUT_SEC = 60
DEFAULT_PAGE_SIZE = 100
DEFAULT_OUTPUT_BASENAME = "ccc_purchase_orders_raw"
DEFAULT_SHEET_MAPPING = {
    "common_purchases": {
        "suppliers": [],
        "keywords": ["material", "equipment", "hardware", "product", "parts"],
    },
    "services_spend": {
        "suppliers": [],
        "keywords": ["service", "consulting", "subscription", "maintenance", "software"],
    },
    "waste_review": {
        "suppliers": [],
        "keywords": ["waste", "recycling", "recycle", "scrap", "landfill", "disposal", "hazardous", "skip"],
    },
}


def _strip(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _get_nested(payload: Any, *paths: str) -> Any:
    for path in paths:
        current = payload
        ok = True
        for part in path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                ok = False
                break
        if ok and current not in (None, ""):
            return current
    return None


def _normalize_date(value: Any) -> str:
    raw = _strip(value)
    if not raw:
        return ""
    candidates = [
        raw,
        raw.replace("Z", "+00:00"),
    ]
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            return dt.date().isoformat()
        except Exception:
            pass
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(candidate[:10], fmt).date().isoformat()
            except Exception:
                continue
    return raw[:10]


def _reporting_period(date_value: str) -> str:
    if not date_value:
        return ""
    try:
        dt = datetime.strptime(date_value[:10], "%Y-%m-%d")
        return dt.strftime("%Y-%m-01")
    except Exception:
        return date_value[:10]


def _normalize_amount(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = _strip(value).replace(" ", "").replace(",", ".")
    try:
        return float(raw)
    except Exception:
        return None


def _load_sheet_mapping() -> dict[str, dict[str, list[str]]]:
    try:
        data = json.loads(CCC_SHEET_MAPPING_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    merged: dict[str, dict[str, list[str]]] = {}
    for key, defaults in DEFAULT_SHEET_MAPPING.items():
        current = data.get(key) if isinstance(data, dict) else {}
        merged[key] = {
            "suppliers": [
                _strip(v) for v in ((current.get("suppliers") if isinstance(current, dict) else None) or defaults["suppliers"])
                if _strip(v)
            ],
            "keywords": [
                _strip(v).lower() for v in ((current.get("keywords") if isinstance(current, dict) else None) or defaults["keywords"])
                if _strip(v)
            ],
        }
    return merged


def _timestamped_output_path() -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H%M")
    candidate = STAGE1_INPUT_DIR / f"{DEFAULT_OUTPUT_BASENAME}_{ts}.xlsx"
    if not candidate.exists():
        return candidate
    ts_precise = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return STAGE1_INPUT_DIR / f"{DEFAULT_OUTPUT_BASENAME}_{ts_precise}.xlsx"


def resolve_runtime_config(
    *,
    base_url: str | None = None,
    username: str | None = None,
    password: str | None = None,
    page_size: int | None = None,
) -> dict[str, Any]:
    return {
        "base_url": _strip(base_url) or _strip(CCC_API_BASE_URL),
        "username": _strip(username) or _strip(CCC_USERNAME),
        "password": _strip(password) or _strip(CCC_PASSWORD),
        "page_size": max(1, int(page_size or CCC_API_PAGE_SIZE or DEFAULT_PAGE_SIZE)),
    }


def _request_json(method: str, url: str, *, payload: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> Any:
    data = None
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=req_headers, method=method.upper())
    try:
        with request.urlopen(req, timeout=DEFAULT_TIMEOUT_SEC) as resp:
            body = resp.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"CCC API request failed ({exc.code}): {detail or exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"CCC API request failed: {exc}") from exc
    try:
        return json.loads(body)
    except Exception as exc:
        raise RuntimeError(f"CCC API returned non-JSON response from {url}") from exc


def _build_api_url(base_url: str, endpoint_path: str) -> str:
    root = _strip(base_url).rstrip("/")
    path = "/" + _strip(endpoint_path).lstrip("/")
    if root.lower().endswith("/api"):
        return f"{root}{path}"
    return f"{root}/api{path}"


def login(base_url: str, username: str, password: str) -> str:
    payload = {
        "email": username,
        "password": password,
    }
    data = _request_json("POST", _build_api_url(base_url, "/user/login"), payload=payload)
    token = (
        _get_nested(data, "token")
        or _get_nested(data, "access_token")
        or _get_nested(data, "jwt")
        or _get_nested(data, "data.token")
        or _get_nested(data, "data.access_token")
    )
    token_str = _strip(token)
    if not token_str:
        raise RuntimeError("CCC API login succeeded but no JWT token was returned.")
    return token_str


def test_connection(*, base_url: str | None = None, username: str | None = None, password: str | None = None) -> dict[str, Any]:
    runtime = resolve_runtime_config(base_url=base_url, username=username, password=password)
    if not runtime["base_url"]:
        raise RuntimeError("CCC API base URL is required.")
    if not runtime["username"]:
        raise RuntimeError("CCC username is required.")
    if not runtime["password"]:
        raise RuntimeError("CCC password is required.")
    token = login(str(runtime["base_url"]), str(runtime["username"]), str(runtime["password"]))
    return {
        "ok": True,
        "token_received": bool(_strip(token)),
        "base_url": str(runtime["base_url"]),
        "username": str(runtime["username"]),
    }


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    for key in ("items", "results", "rows", "data", "purchaseOrders"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            for nested_key in ("items", "results", "rows"):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, list):
                    return [row for row in nested_value if isinstance(row, dict)]
    return []


def fetch_purchase_orders(base_url: str, token: str, *, page_size: int = DEFAULT_PAGE_SIZE) -> list[dict[str, Any]]:
    page = 1
    rows: list[dict[str, Any]] = []
    headers = {"Authorization": f"Bearer {token}"}
    while True:
        qs = parse.urlencode({"currentPage": page, "pageSize": page_size})
        data = _request_json("GET", f"{_build_api_url(base_url, '/purchase_order')}?{qs}", headers=headers)
        items = _extract_items(data)
        if not items:
            break
        rows.extend(items)
        total_pages = _get_nested(data, "totalPages") or _get_nested(data, "pagination.totalPages") or _get_nested(data, "meta.totalPages")
        current_page = _get_nested(data, "currentPage") or _get_nested(data, "pagination.currentPage") or page
        if total_pages:
            try:
                if int(current_page) >= int(total_pages):
                    break
            except Exception:
                pass
        if len(items) < page_size:
            break
        page += 1
    return rows


def _classification_text(row: dict[str, Any]) -> str:
    return " ".join(
        part for part in [
            _strip(row.get("supplier_name")),
            _strip(row.get("description")),
            _strip(row.get("project")),
            _strip(row.get("cost_center")),
        ] if part
    ).lower()


def _route_record(row: dict[str, Any], mapping_rules: dict[str, dict[str, list[str]]]) -> str:
    supplier = _strip(row.get("supplier_name")).casefold()
    text = _classification_text(row)
    for target in ("waste_review", "services_spend", "common_purchases"):
        rules = mapping_rules.get(target) or {}
        suppliers = {str(v).strip().casefold() for v in (rules.get("suppliers") or []) if _strip(v)}
        keywords = [str(v).strip().lower() for v in (rules.get("keywords") or []) if _strip(v)]
        if supplier and supplier in suppliers:
            return target
        if any(keyword in text for keyword in keywords):
            return target
    return "common_purchases"


def normalize_purchase_orders(items: list[dict[str, Any]], *, mapping_rules: dict[str, dict[str, list[str]]] | None = None) -> pd.DataFrame:
    active_rules = mapping_rules or _load_sheet_mapping()
    rows: list[dict[str, Any]] = []
    for item in items:
        date_value = _normalize_date(
            _get_nested(item, "purchaseDate", "orderDate", "date", "createdAt", "documentDate")
        )
        normalized = {
            "supplier_name": _strip(_get_nested(item, "supplier.name", "vendor.name", "supplierName", "vendorName", "supplier")),
            "supplier_country": _strip(_get_nested(item, "supplier.country", "vendor.country", "supplierCountry", "vendorCountry", "country")),
            "amount": _normalize_amount(_get_nested(item, "amount", "totalAmount", "grossAmount", "netAmount", "total")),
            "currency": _strip(_get_nested(item, "currency.code", "currency", "currencyCode")),
            "date": date_value,
            "description": _strip(_get_nested(item, "description", "title", "itemDescription", "summary", "name")),
            "project": _strip(_get_nested(item, "project.name", "project.code", "project", "projectName", "projectCode")),
            "cost_center": _strip(_get_nested(item, "costCenter.name", "costCenter.code", "costCenter", "cost_center", "costCentre")),
            "purchase_order_id": _strip(_get_nested(item, "id", "purchaseOrderId", "orderNumber", "number")),
        }
        normalized["target_sheet"] = _route_record(normalized, active_rules)
        rows.append(normalized)
    df = pd.DataFrame(rows, columns=[
        "supplier_name",
        "supplier_country",
        "amount",
        "currency",
        "date",
        "description",
        "project",
        "cost_center",
        "purchase_order_id",
        "target_sheet",
    ])
    if df.empty:
        return df
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["date"] = df["date"].fillna("")
    return df


def build_stage1_workbook_frames(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    raw_df = df.copy()
    common_input = raw_df.loc[raw_df["target_sheet"] == "common_purchases"].copy() if not raw_df.empty else pd.DataFrame(columns=raw_df.columns)
    services_input = raw_df.loc[raw_df["target_sheet"] == "services_spend"].copy() if not raw_df.empty else pd.DataFrame(columns=raw_df.columns)
    waste_review = raw_df.loc[raw_df["target_sheet"] == "waste_review"].copy() if not raw_df.empty else pd.DataFrame(columns=raw_df.columns)
    common_purchases = pd.DataFrame({
        "Reporting period (month, year) when product purchased": common_input["date"].map(_reporting_period) if not common_input.empty else pd.Series(dtype="object"),
        "Product Type": common_input["description"] if not common_input.empty else pd.Series(dtype="object"),
        "Product Supplier": common_input["supplier_name"] if not common_input.empty else pd.Series(dtype="object"),
        "Product Origin, Country of Manufacture": common_input["supplier_country"] if not common_input.empty else pd.Series(dtype="object"),
        "Product Function (ie electrical, building, design, etc)": common_input["project"].where(common_input["project"].astype(str).str.strip().ne(""), common_input["cost_center"]) if not common_input.empty else pd.Series(dtype="object"),
        "Product Cost": common_input["amount"] if not common_input.empty else pd.Series(dtype="float64"),
        "Product Weight": "",
        "Product Weight Unit": "",
        "Primary materials (ie metal, copper, concrete)": "",
    })
    services_spend = pd.DataFrame({
        "Reporting period (month, year)": services_input["date"].map(_reporting_period) if not services_input.empty else pd.Series(dtype="object"),
        "Purchase Date (Purchase order date or invoice date)": services_input["date"] if not services_input.empty else pd.Series(dtype="object"),
        "Service Provided": services_input["description"] if not services_input.empty else pd.Series(dtype="object"),
        "Service Provider Name": services_input["supplier_name"] if not services_input.empty else pd.Series(dtype="object"),
        "Spend on service": services_input["amount"] if not services_input.empty else pd.Series(dtype="float64"),
        "Spend currency": services_input["currency"] if not services_input.empty else pd.Series(dtype="object"),
        "Service Provider Function": services_input["project"].where(services_input["project"].astype(str).str.strip().ne(""), services_input["cost_center"]) if not services_input.empty else pd.Series(dtype="object"),
        "Service Provider Location": services_input["supplier_country"] if not services_input.empty else pd.Series(dtype="object"),
        "Data Source (invoice tracker)": "CCC API",
    })
    return {
        "CCC Purchase Orders Raw": raw_df,
        "Scope 3 Cat 1 Common Purchases": common_purchases,
        "Scope 3 Cat 1 Services Spend": services_spend,
        "CCC Waste Suppliers Review": waste_review,
    }


def save_workbook(frames: dict[str, pd.DataFrame], output_path: Path = DEFAULT_OUTPUT_PATH) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in frames.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return output_path


def sync_purchase_orders(*, base_url: str | None = None, username: str | None = None, password: str | None = None, page_size: int = DEFAULT_PAGE_SIZE, output_path: Path | None = None) -> dict[str, Any]:
    runtime = resolve_runtime_config(base_url=base_url, username=username, password=password, page_size=page_size)
    if not _strip(runtime["base_url"]):
        raise RuntimeError("CCC API base URL is required.")
    if not _strip(runtime["username"]):
        raise RuntimeError("CCC username is required.")
    if not _strip(runtime["password"]):
        raise RuntimeError("CCC API password is required.")
    mapping_rules = _load_sheet_mapping()
    token = login(str(runtime["base_url"]), str(runtime["username"]), str(runtime["password"]))
    items = fetch_purchase_orders(str(runtime["base_url"]), token, page_size=int(runtime["page_size"]))
    normalized = normalize_purchase_orders(items, mapping_rules=mapping_rules)
    frames = build_stage1_workbook_frames(normalized)
    target_path = save_workbook(frames, output_path or _timestamped_output_path())
    return {
        "output_path": target_path,
        "rows_fetched": int(len(normalized.index)),
        "waste_rows": int((normalized["target_sheet"] == "waste_review").sum()) if not normalized.empty else 0,
        "services_rows": int((normalized["target_sheet"] == "services_spend").sum()) if not normalized.empty else 0,
        "common_rows": int((normalized["target_sheet"] == "common_purchases").sum()) if not normalized.empty else 0,
        "sheet_names": list(frames.keys()),
        "mapping_rules": mapping_rules,
    }


def main() -> None:
    result = sync_purchase_orders(
        base_url=CCC_API_BASE_URL,
        username=CCC_USERNAME,
        password=CCC_PASSWORD,
        page_size=CCC_API_PAGE_SIZE,
    )
    print(f"Wrote CCC purchase order workbook: {Path(result['output_path']).name}")
    print(f"Rows fetched: {result['rows_fetched']}")


if __name__ == "__main__":
    main()
