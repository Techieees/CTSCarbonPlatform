from __future__ import annotations

import base64
import json
import os
import time
from typing import Any
from urllib import error, parse, request

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in os.sys.path:
    os.sys.path.insert(0, PROJECT_ROOT)

from config import CCC_API_BASE_URL, CCC_API_PAGE_SIZE, CCC_PASSWORD, CCC_USERNAME


DEFAULT_TIMEOUT_SEC = 60
DEFAULT_PAGE_SIZE = max(1, int(CCC_API_PAGE_SIZE or 100))
_TOKEN_CACHE: dict[str, Any] = {
    "token": "",
    "base_url": "",
    "username": "",
    "password": "",
    "expires_at": 0.0,
}


def _strip(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_api_url(base_url: str, endpoint_path: str) -> str:
    root = _strip(base_url).rstrip("/")
    path = "/" + _strip(endpoint_path).lstrip("/")
    if root.lower().endswith("/api"):
        return f"{root}{path}"
    return f"{root}/api{path}"


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


def _decode_jwt_exp(token: str) -> float:
    parts = str(token or "").split(".")
    if len(parts) < 2:
        return 0.0
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode((payload + padding).encode("utf-8")).decode("utf-8")
        data = json.loads(decoded)
    except Exception:
        return 0.0
    try:
        return float(data.get("exp") or 0.0)
    except Exception:
        return 0.0


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


def _credentials(base_url: str | None = None, username: str | None = None, password: str | None = None) -> dict[str, str]:
    return {
        "base_url": _strip(base_url) or _strip(CCC_API_BASE_URL),
        "username": _strip(username) or _strip(CCC_USERNAME),
        "password": _strip(password) or _strip(CCC_PASSWORD),
    }


def login(*, base_url: str | None = None, username: str | None = None, password: str | None = None, force: bool = False) -> str:
    creds = _credentials(base_url, username, password)
    if not creds["base_url"]:
        raise RuntimeError("CCC API base URL is required.")
    if not creds["username"]:
        raise RuntimeError("CCC username is required.")
    if not creds["password"]:
        raise RuntimeError("CCC password is required.")
    now = time.time()
    if (
        not force
        and _TOKEN_CACHE["token"]
        and _TOKEN_CACHE["base_url"] == creds["base_url"]
        and _TOKEN_CACHE["username"] == creds["username"]
        and _TOKEN_CACHE["password"] == creds["password"]
        and float(_TOKEN_CACHE.get("expires_at") or 0.0) > now + 30
    ):
        return str(_TOKEN_CACHE["token"])
    data = _request_json(
        "POST",
        _build_api_url(creds["base_url"], "/user/login"),
        payload={"email": creds["username"], "password": creds["password"]},
    )
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
    _TOKEN_CACHE.update(
        {
            "token": token_str,
            "base_url": creds["base_url"],
            "username": creds["username"],
            "password": creds["password"],
            "expires_at": _decode_jwt_exp(token_str) or (now + 55 * 60),
        }
    )
    return token_str


def test_connection(*, base_url: str | None = None, username: str | None = None, password: str | None = None) -> dict[str, Any]:
    token = login(base_url=base_url, username=username, password=password, force=True)
    return {
        "ok": True,
        "token_received": bool(_strip(token)),
        "base_url": _credentials(base_url, username, password)["base_url"],
    }


def _authorized_headers(base_url: str | None = None, username: str | None = None, password: str | None = None) -> dict[str, str]:
    token = login(base_url=base_url, username=username, password=password)
    return {"Authorization": f"Bearer {token}"}


def get(endpoint: str, *, base_url: str | None = None, username: str | None = None, password: str | None = None, query_params: dict[str, Any] | None = None, retry_on_auth: bool = True) -> Any:
    creds = _credentials(base_url, username, password)
    url = _build_api_url(creds["base_url"], endpoint)
    if query_params:
        clean_params = {k: v for k, v in query_params.items() if v is not None and _strip(v) != ""}
        if clean_params:
            url = f"{url}?{parse.urlencode(clean_params, doseq=True)}"
    try:
        return _request_json("GET", url, headers=_authorized_headers(**creds))
    except RuntimeError as exc:
        if retry_on_auth and ("401" in str(exc) or "403" in str(exc)):
            login(base_url=creds["base_url"], username=creds["username"], password=creds["password"], force=True)
            return get(endpoint, base_url=creds["base_url"], username=creds["username"], password=creds["password"], query_params=query_params, retry_on_auth=False)
        raise


def _extract_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "results", "rows", "data", "purchaseOrders"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                for nested_key in ("items", "results", "rows"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return nested
        return [payload]
    return []


def _pagination_meta(payload: Any) -> dict[str, int]:
    meta = {
        "current_page": 1,
        "page_size": DEFAULT_PAGE_SIZE,
        "total_pages": 1,
        "total_count": 0,
    }
    candidates = [payload]
    if isinstance(payload, dict):
        for key in ("pagination", "meta", "data"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
    for node in candidates:
        if not isinstance(node, dict):
            continue
        for key, target in (
            ("page", "current_page"),
            ("currentPage", "current_page"),
            ("pageSize", "page_size"),
            ("totalPages", "total_pages"),
            ("totalCount", "total_count"),
        ):
            if key in node:
                try:
                    meta[target] = max(1, int(node[key])) if target != "total_count" else max(0, int(node[key]))
                except Exception:
                    pass
    if meta["total_count"] and meta["page_size"] and meta["total_pages"] == 1:
        meta["total_pages"] = max(1, (meta["total_count"] + meta["page_size"] - 1) // meta["page_size"])
    return meta


def get_paginated(endpoint: str, *, base_url: str | None = None, username: str | None = None, password: str | None = None, query_params: dict[str, Any] | None = None, page_size: int | None = None) -> list[Any]:
    effective_page_size = max(1, int(page_size or DEFAULT_PAGE_SIZE))
    first_payload = get(endpoint, base_url=base_url, username=username, password=password, query_params=query_params)
    items = list(_extract_items(first_payload))
    meta = _pagination_meta(first_payload)
    total_pages = max(1, int(meta.get("total_pages") or 1))
    if total_pages <= 1:
        return items
    for page in range(2, total_pages + 1):
        payload = get(
            endpoint,
            base_url=base_url,
            username=username,
            password=password,
            query_params={**(query_params or {}), "currentPage": page, "pageSize": effective_page_size},
        )
        page_items = _extract_items(payload)
        if not page_items:
            break
        items.extend(page_items)
    return items
