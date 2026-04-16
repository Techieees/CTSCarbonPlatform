from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, parse, request

import pandas as pd

try:
    import pyarrow  # noqa: F401

    PARQUET_ENGINE = "pyarrow"
except ImportError:
    try:
        import fastparquet  # noqa: F401

        PARQUET_ENGINE = "fastparquet"
    except ImportError:
        PARQUET_ENGINE = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    CCC_API_BASE_URL,
    CCC_API_PAGE_SIZE,
    CCC_PASSWORD,
    CCC_SHEET_MAPPING_PATH,
    CCC_USERNAME,
    DATA_DIR,
    STAGE1_INPUT_DIR,
)


DEFAULT_TIMEOUT_SEC = 60
DEFAULT_PAGE_SIZE = 100
DEFAULT_OUTPUT_BASENAME = "ccc_purchase_orders_raw"
PURCHASE_ORDER_PROJECT_ID = 14
PURCHASE_ORDER_SORTING = "D"
PURCHASE_ORDER_PAGE_SIZE = 200
CCC_EXTERNAL_DIR = DATA_DIR / "external" / "ccc"
PROJECTS_CACHE_PATH = CCC_EXTERNAL_DIR / "projects.json"
PURCHASE_ORDER_OUTPUT_COLUMNS = [
    "Project Code",
    "Purchase Order",
    "Discipline",
    "Category",
    "TFM-Code",
    "Description",
    "Currency",
    "Total Price",
    "Status",
    "Supplier",
    "Supplier Contact Name",
    "Supplier Contact Email",
    "Supplier Contact Phone",
    "CTS Contact",
    "CTS Contact Email",
    "CreatedOn",
    "CreatedBy",
    "UpdatedOn",
    "UpdatedBy",
]
PURCHASE_ORDER_PREVIEW_COLUMNS = [
    ("Supplier", "Supplier"),
    ("Total Price", "Total Price"),
    ("Currency", "Currency"),
    ("CreatedOn", "CreatedOn"),
    ("Status", "Status"),
]
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


def _coerce_project_id(project_id: Any, default: int = PURCHASE_ORDER_PROJECT_ID) -> int:
    try:
        return max(1, int(project_id))
    except Exception:
        return int(default)


def _purchase_orders_parquet_path(project_id: Any) -> Path:
    return CCC_EXTERNAL_DIR / f"{_coerce_project_id(project_id)}_purchase_orders.parquet"


def _purchase_orders_csv_path(project_id: Any) -> Path:
    return CCC_EXTERNAL_DIR / f"{_coerce_project_id(project_id)}_purchase_orders.csv"


def _purchase_orders_output_path(project_id: Any) -> Path:
    return _purchase_orders_csv_path(project_id)


def _project_display_name(project: dict[str, Any]) -> str:
    project_id = _coerce_project_id(project.get("id"))
    project_code = _strip(project.get("projectCode"))
    project_name = _strip(project.get("projectName"))
    label = f"{project_id}"
    if project_code:
        label = f"{label} - {project_code}"
    if project_name:
        label = f"{label} {project_name}"
    return label.strip()


def _parse_query_params(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): val
        for key, val in value.items()
        if val is not None and _strip(val) != ""
    }


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload.get(key) not in (None, ""):
            return payload.get(key)
    return None


def _series_from_candidates(df: pd.DataFrame, *names: str, default: Any = "") -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series([default] * len(df.index), index=df.index)


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
        "username": username,
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


def _save_available_projects_cache(projects: list[dict[str, Any]]) -> None:
    CCC_EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    PROJECTS_CACHE_PATH.write_text(json.dumps(projects, indent=2), encoding="utf-8")


def load_available_projects_cache() -> list[dict[str, Any]]:
    if not PROJECTS_CACHE_PATH.exists():
        return []
    try:
        data = json.loads(PROJECTS_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    projects: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        project = {
            "id": _coerce_project_id(item.get("id")),
            "projectCode": _strip(item.get("projectCode")),
            "projectName": _strip(item.get("projectName")),
        }
        project["label"] = _project_display_name(project)
        projects.append(project)
    return sorted(projects, key=lambda item: int(item.get("id") or 0))


def get_available_projects(
    *,
    base_url: str | None = None,
    username: str | None = None,
    password: str | None = None,
    start_id: int = 1,
    end_id: int = 200,
) -> list[dict[str, Any]]:
    runtime = resolve_runtime_config(base_url=base_url, username=username, password=password)
    if not _strip(runtime["base_url"]):
        raise RuntimeError("CCC API base URL is required.")
    if not _strip(runtime["username"]):
        raise RuntimeError("CCC username is required.")
    if not _strip(runtime["password"]):
        raise RuntimeError("CCC API password is required.")
    token = login(str(runtime["base_url"]), str(runtime["username"]), str(runtime["password"]))
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    projects: list[dict[str, Any]] = []
    for project_id in range(max(1, int(start_id)), max(1, int(end_id)) + 1):
        try:
            payload = _request_json(
                "GET",
                _build_api_url(str(runtime["base_url"]), f"/project/{project_id}"),
                headers=headers,
            )
        except RuntimeError:
            continue
        project = {
            "id": _coerce_project_id(_get_nested(payload, "id", "data.id", "project.id") or project_id),
            "projectCode": _strip(_get_nested(payload, "projectCode", "code", "data.projectCode", "data.code", "project.code")),
            "projectName": _strip(_get_nested(payload, "projectName", "name", "data.projectName", "data.name", "project.name")),
        }
        project["label"] = _project_display_name(project)
        projects.append(project)
    projects.sort(key=lambda item: int(item.get("id") or 0))
    _save_available_projects_cache(projects)
    return projects


def load_available_projects(
    *,
    base_url: str | None = None,
    username: str | None = None,
    password: str | None = None,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    cached = load_available_projects_cache()
    if cached and not force_refresh:
        return cached
    if not (_strip(base_url) or _strip(CCC_API_BASE_URL)):
        return cached
    if not (_strip(username) or _strip(CCC_USERNAME)):
        return cached
    if not (_strip(password) or _strip(CCC_PASSWORD)):
        return cached
    try:
        return get_available_projects(
            base_url=base_url,
            username=username,
            password=password,
        )
    except Exception:
        return cached


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("result"), list):
        return [row for row in payload.get("result", []) if isinstance(row, dict)]
    for key in ("items", "results", "result", "rows", "data", "purchaseOrders"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            for nested_key in ("items", "results", "rows"):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, list):
                    return [row for row in nested_value if isinstance(row, dict)]
    return []


def fetch_purchase_orders(
    base_url: str,
    token: str,
    *,
    project_id: int = PURCHASE_ORDER_PROJECT_ID,
    sorting: str = PURCHASE_ORDER_SORTING,
    page_size: int = PURCHASE_ORDER_PAGE_SIZE,
    query_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    page = 1
    rows: list[dict[str, Any]] = []
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    extra_query_params = _parse_query_params(query_params)
    while True:
        params = dict(extra_query_params)
        params.update(
            {
                "projectId": int(project_id),
                "sorting": _strip(sorting) or PURCHASE_ORDER_SORTING,
                "pageSize": max(1, int(page_size)),
                "currentPage": page,
            }
        )
        print("CCC request params:", params)
        qs = parse.urlencode(params)
        data = _request_json("GET", f"{_build_api_url(base_url, '/purchase_order')}?{qs}", headers=headers)
        items = (
            [row for row in data.get("result", []) if isinstance(row, dict)]
            if isinstance(data, dict) and isinstance(data.get("result"), list)
            else _extract_items(data)
        )
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
        elif len(items) < max(1, int(page_size)):
            break
        page += 1
    return rows


def normalize_purchase_orders_for_cache(items: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                "Project Code": _strip(item.get("projectCode")),
                "Purchase Order": _strip(item.get("code")),
                "Discipline": _strip(item.get("discipline")),
                "Category": _strip(item.get("category")),
                "TFM-Code": _strip(item.get("tfm")),
                "Description": _strip(item.get("boQ")),
                "Currency": _strip(item.get("currency")),
                "Total Price": _normalize_amount(item.get("amount")),
                "Status": _strip(item.get("statusDescription")),
                "Supplier": _strip(item.get("supplier")),
                "Supplier Contact Name": _strip(item.get("supplierNameContact")),
                "Supplier Contact Email": _strip(item.get("supplierEmailContact")),
                "Supplier Contact Phone": _strip(item.get("supplierPhoneContact")),
                "CTS Contact": _strip(item.get("contactName")),
                "CTS Contact Email": _strip(item.get("contactEmail")),
                "CreatedOn": _normalize_date(item.get("createdOn")),
                "CreatedBy": _strip(item.get("createdBy")),
                "UpdatedOn": _normalize_date(item.get("updatedOn")),
                "UpdatedBy": _strip(item.get("updatedBy")),
            }
        )
    df = pd.DataFrame(rows, columns=PURCHASE_ORDER_OUTPUT_COLUMNS)
    if df.empty:
        return df
    df["Total Price"] = pd.to_numeric(df["Total Price"], errors="coerce")
    return df


def save_purchase_orders_cache(
    df: pd.DataFrame,
    *,
    project_id: Any = PURCHASE_ORDER_PROJECT_ID,
    output_path: Path | None = None,
) -> Path:
    target_path = Path(output_path or _purchase_orders_output_path(project_id))
    target_path.parent.mkdir(parents=True, exist_ok=True)
    payload = df.copy() if not df.empty else pd.DataFrame(columns=PURCHASE_ORDER_OUTPUT_COLUMNS)
    parquet_path = _purchase_orders_parquet_path(project_id)
    csv_path = _purchase_orders_csv_path(project_id)
    payload.to_csv(target_path, index=False)
    if parquet_path.exists():
        parquet_path.unlink()
    if csv_path != target_path and csv_path.exists():
        csv_path.unlink()
    return target_path


def load_purchase_orders_cache(
    *,
    project_id: Any = PURCHASE_ORDER_PROJECT_ID,
    output_path: Path | None = None,
) -> pd.DataFrame:
    target_path = Path(output_path or _purchase_orders_output_path(project_id))
    parquet_path = _purchase_orders_parquet_path(project_id)
    csv_path = _purchase_orders_csv_path(project_id)
    if output_path is None:
        if csv_path.exists():
            target_path = csv_path
        elif PARQUET_ENGINE and parquet_path.exists():
            target_path = parquet_path
        elif parquet_path.exists():
            target_path = parquet_path
    if not target_path.exists():
        return pd.DataFrame(columns=PURCHASE_ORDER_OUTPUT_COLUMNS)
    try:
        if target_path.suffix.lower() == ".parquet":
            if not PARQUET_ENGINE:
                raise RuntimeError("Parquet engine is not available.")
            df = pd.read_parquet(target_path, engine=PARQUET_ENGINE)
        else:
            df = pd.read_csv(target_path)
    except Exception:
        if target_path != csv_path and csv_path.exists():
            df = pd.read_csv(csv_path)
        else:
            return pd.DataFrame(columns=PURCHASE_ORDER_OUTPUT_COLUMNS)
    for column in PURCHASE_ORDER_OUTPUT_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df[PURCHASE_ORDER_OUTPUT_COLUMNS].copy()


def _format_preview_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if pd.isna(value):
            return ""
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    return str(value)


def load_purchase_orders_cache_summary(
    *,
    project_id: Any = PURCHASE_ORDER_PROJECT_ID,
    output_path: Path | None = None,
    preview_limit: int = 20,
) -> dict[str, Any]:
    parquet_path = _purchase_orders_parquet_path(project_id)
    csv_path = _purchase_orders_csv_path(project_id)
    if output_path is not None:
        target_path = Path(output_path)
    elif csv_path.exists():
        target_path = csv_path
    elif PARQUET_ENGINE and parquet_path.exists():
        target_path = parquet_path
    else:
        target_path = csv_path
    summary: dict[str, Any] = {
        "available": False,
        "output_path": target_path,
        "output_file": target_path.name,
        "project_id": _coerce_project_id(project_id),
        "records_synced": 0,
        "last_sync_time": "",
        "total_amount": 0.0,
        "supplier_count": 0,
        "preview": {
            "name": "Latest purchase orders",
            "columns": [label for label, _source in PURCHASE_ORDER_PREVIEW_COLUMNS],
            "rows": [],
            "row_count": 0,
            "column_count": len(PURCHASE_ORDER_PREVIEW_COLUMNS),
            "truncated": False,
        },
    }
    if not target_path.exists():
        return summary
    summary["available"] = True
    summary["last_sync_time"] = datetime.fromtimestamp(target_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    df = load_purchase_orders_cache(project_id=project_id, output_path=target_path)
    summary["records_synced"] = int(len(df.index))
    if df.empty:
        return summary
    amounts = pd.to_numeric(_series_from_candidates(df, "Total Price", "Amount", "amount"), errors="coerce").fillna(0.0)
    suppliers = _series_from_candidates(df, "Supplier", "supplier").fillna("").astype(str).str.strip()
    summary["total_amount"] = float(amounts.sum())
    summary["supplier_count"] = int(suppliers.loc[suppliers.ne("")].nunique())
    preview_df = df.copy()
    preview_df["_created_on_sort"] = pd.to_datetime(
        _series_from_candidates(preview_df, "CreatedOn", "Created On", "createdOn"),
        errors="coerce",
    )
    preview_df["_id_sort"] = _series_from_candidates(preview_df, "Purchase Order", "ID", "id").astype(str)
    preview_df = preview_df.sort_values(by=["_created_on_sort", "_id_sort"], ascending=[False, False], na_position="last")
    preview_source_columns = [source for _label, source in PURCHASE_ORDER_PREVIEW_COLUMNS]
    preview_df = pd.DataFrame(
        {
            source: _series_from_candidates(
                preview_df,
                source,
                {
                    "Supplier": "supplier",
                    "Total Price": "amount",
                    "Currency": "currency",
                    "CreatedOn": "createdOn",
                    "Status": "statusDescription",
                }.get(source, source),
            )
            for source in preview_source_columns
        }
    ).head(max(1, int(preview_limit)))
    preview_rows: list[list[str]] = []
    for ridx in range(len(preview_df.index)):
        preview_rows.append([_format_preview_value(preview_df.iloc[ridx][column]) for column in preview_source_columns])
    summary["preview"] = {
        "name": "Latest purchase orders",
        "columns": [label for label, _source in PURCHASE_ORDER_PREVIEW_COLUMNS],
        "rows": preview_rows,
        "row_count": int(len(df.index)),
        "column_count": len(PURCHASE_ORDER_PREVIEW_COLUMNS),
        "truncated": int(len(df.index)) > int(len(preview_rows)),
    }
    return summary


def sync_purchase_orders_cache(
    *,
    base_url: str | None = None,
    username: str | None = None,
    password: str | None = None,
    project_id: Any = PURCHASE_ORDER_PROJECT_ID,
    query_params: dict[str, Any] | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    resolved_project_id = _coerce_project_id(project_id)
    runtime = resolve_runtime_config(
        base_url=base_url,
        username=username,
        password=password,
        page_size=PURCHASE_ORDER_PAGE_SIZE,
    )
    if not _strip(runtime["base_url"]):
        raise RuntimeError("CCC API base URL is required.")
    if not _strip(runtime["username"]):
        raise RuntimeError("CCC username is required.")
    if not _strip(runtime["password"]):
        raise RuntimeError("CCC API password is required.")
    token = login(str(runtime["base_url"]), str(runtime["username"]), str(runtime["password"]))
    items = fetch_purchase_orders(
        str(runtime["base_url"]),
        token,
        project_id=resolved_project_id,
        sorting=PURCHASE_ORDER_SORTING,
        page_size=PURCHASE_ORDER_PAGE_SIZE,
        query_params=query_params,
    )
    normalized = normalize_purchase_orders_for_cache(items)
    target_path = save_purchase_orders_cache(
        normalized,
        project_id=resolved_project_id,
        output_path=output_path,
    )
    summary = load_purchase_orders_cache_summary(project_id=resolved_project_id, output_path=target_path)
    return {
        "endpoint": "purchase_order",
        "project_id": resolved_project_id,
        "page_size": PURCHASE_ORDER_PAGE_SIZE,
        "sorting": PURCHASE_ORDER_SORTING,
        "records_imported": int(len(normalized.index)),
        "total_amount": float(summary.get("total_amount") or 0.0),
        "supplier_count": int(summary.get("supplier_count") or 0),
        "output_path": target_path,
        "output_file": target_path.name,
        "status": "success",
    }


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


def save_workbook(frames: dict[str, pd.DataFrame], output_path: Path | None = None) -> Path:
    target_path = Path(output_path or _timestamped_output_path())
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(target_path, engine="openpyxl") as writer:
        for sheet_name, df in frames.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return target_path


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
