"""
Engage Waste API → normalized rows → Scope 3 Category 5 Waste (Data Entry).

HTTP orchestration archive targets STORAGE_ROOT/engage_waste/ (see config).
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
import uuid
from collections.abc import Mapping, MutableMapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from config import ENGAGE_WASTE_BASE_URL, ENGAGE_WASTE_SUBSCRIPTION_KEY, STORAGE_ROOT

ENGAGE_DATA_SOURCE_LABEL = "Engage API"
ENGAGE_TARGET_SHEET = "Scope 3 Category 5 Waste"
ENGAGE_IMPORT_DEDUP_COLUMN = "engage_waste_import_dedup"

DEFAULT_PAGE_LIMIT = 1000
REQUEST_TIMEOUT_S = 120

_WS_RE = re.compile(r"\s+")
_CODE_PREFIX_RE = re.compile(r"^\s*(?P<code>\d+)\s+(?P<rest>.+)$", re.DOTALL)

# Raw label → canonical Site Tag (lookup uses normalized pickup key).
_SITE_TAG_RULES: tuple[tuple[str, str], ...] = (
    ("Orrhusveien 77 – NEP", "NordicEPOD AS"),
    ("Orrhusveien 77 - NEP", "NordicEPOD AS"),
    ("1070 Pegasus Ph3 Construction Site", "1070 Pegasus Ph3"),
    ("1070 Pegasus Ph3 Barracks Rig", "1070 Pegasus Ph3"),
    ("Security shredding", "CTS Nordics AS Oslo Office"),
    ("Orrhusveien 77 - NEP-SWB", "NEP Switchboards"),
    ("Ensjøveien 20", "CTS Nordics AS Oslo Office"),
)


def _repo_storage_engage_root() -> Path:
    root = STORAGE_ROOT / "engage_waste"
    (root / "raw").mkdir(parents=True, exist_ok=True)
    (root / "previews").mkdir(parents=True, exist_ok=True)
    return root


def get_engage_waste_client(*, subscription_key: str | None = None) -> dict[str, Any]:
    """
    Runtime client descriptor (no secrets in logs — pass subscription_key explicitly).
    """
    key = str(subscription_key if subscription_key is not None else ENGAGE_WASTE_SUBSCRIPTION_KEY or "").strip()
    base = str(ENGAGE_WASTE_BASE_URL or "").strip().rstrip("/")
    return {
        "base_url": base or "https://prod.apim.ngn.no/engage-waste",
        "subscription_key_configured": bool(key),
        "headers": {"Ocp-Apim-Subscription-Key": key, "Accept": "application/json"},
    }


def _normalize_pickup_match_key(value: object) -> str:
    s = unicodedata.normalize("NFKC", str(value or ""))
    for ch in ("\u2013", "\u2014", "\u2212", "‐", "‑", "‒", "–", "—"):
        s = s.replace(ch, "-")
    s = _WS_RE.sub(" ", s.strip()).lower()
    return s


_PICKUP_SITE_TAG_LOOKUP: dict[str, str] = {
    _normalize_pickup_match_key(raw): tag for raw, tag in _SITE_TAG_RULES
}


def map_pickup_location_to_site_tag(pickup_location: object) -> tuple[str, bool]:
    """
    Returns (site_tag, unmapped_site_tag_flag).
    When unknown, keeps original pickup label as Site Tag and flags True.
    """
    raw = _WS_RE.sub(" ", str(pickup_location or "").strip())
    if not raw:
        return "", True
    key = _normalize_pickup_match_key(raw)
    mapped = _PICKUP_SITE_TAG_LOOKUP.get(key)
    if mapped:
        return mapped, False
    return raw, True


def translate_waste_stream(text: object, *, translator_cache: MutableMapping[str, str] | None = None) -> str:
    """
    Translate waste stream / product-group description to English while preserving a leading numeric waste code.
    """
    raw = _WS_RE.sub(" ", str(text or "").strip())
    if not raw:
        return ""

    cache = translator_cache if translator_cache is not None else {}
    if raw in cache:
        return cache[raw]

    m = _CODE_PREFIX_RE.match(raw)
    code = (m.group("code") if m else "") or ""
    rest = (m.group("rest").strip() if m else raw.strip())

    to_translate = rest if code else raw
    if not to_translate.strip():
        cache[raw] = raw
        return raw

    try:
        from deep_translator import GoogleTranslator

        tr = GoogleTranslator(source="auto", target="en")
        translated = str(tr.translate(to_translate) or "").strip()
    except Exception:
        translated = ""

    if not translated:
        translated = to_translate

    out = f"{code} {translated}".strip() if code else translated
    cache[raw] = out
    return out


def archive_raw_response(payload: object, *, archive_root: Path | None = None) -> Path:
    """Persist JSON under engage_waste/raw/YYYY/MM/<timestamp>_<nonce>.json"""
    root = archive_root or (_repo_storage_engage_root() / "raw")
    now = datetime.now(timezone.utc)
    month_dir = root / f"{now.year:04d}" / f"{now.month:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}.json"
    path = month_dir / fname
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _extract_waste_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("items", "data", "waste", "wastes", "results", "value", "records"):
        block = payload.get(key)
        if isinstance(block, list):
            return [x for x in block if isinstance(x, dict)]
    inner = payload.get("payload") or payload.get("response") or payload.get("body")
    if isinstance(inner, list):
        return [x for x in inner if isinstance(x, dict)]
    if isinstance(inner, dict):
        return _extract_waste_items(inner)
    return []


def fetch_waste_data(
    *,
    offset: int = 0,
    limit: int = DEFAULT_PAGE_LIMIT,
    extra_params: Mapping[str, Any] | None = None,
    subscription_key: str | None = None,
    timeout_s: float = REQUEST_TIMEOUT_S,
) -> dict[str, Any]:
    """
    Single GET page against /api/v2/waste.
    Returns dict with keys: ok, status_code, json (parsed or None), text (error snippet), url.
    """
    client = get_engage_waste_client(subscription_key=subscription_key)
    key = str((client["headers"] or {}).get("Ocp-Apim-Subscription-Key") or "").strip()
    if not key:
        return {"ok": False, "status_code": 0, "json": None, "text": "ENGAGE_WASTE_SUBSCRIPTION_KEY is not set.", "url": ""}

    base = str(client.get("base_url") or "").rstrip("/")
    url = f"{base}/api/v2/waste"
    params: dict[str, Any] = {"offset": int(offset), "limit": int(limit)}
    if extra_params:
        for k, v in extra_params.items():
            if v is None or v == "":
                continue
            params[str(k)] = v

    try:
        resp = requests.get(url, headers=client["headers"], params=params, timeout=timeout_s)
    except requests.RequestException as exc:
        return {"ok": False, "status_code": 0, "json": None, "text": str(exc), "url": url}

    parsed: dict[str, Any] | list[Any] | None
    try:
        parsed = resp.json()
    except Exception:
        parsed = None

    if resp.status_code >= 400:
        snippet = ""
        if isinstance(parsed, dict):
            snippet = json.dumps(parsed, ensure_ascii=True)[:800]
        else:
            snippet = (resp.text or "")[:800]
        return {"ok": False, "status_code": int(resp.status_code), "json": parsed, "text": snippet, "url": url}

    return {"ok": True, "status_code": int(resp.status_code), "json": parsed, "text": "", "url": url}


def fetch_all_waste_data(
    *,
    limit_per_page: int = DEFAULT_PAGE_LIMIT,
    max_pages: int = 100,
    extra_params: Mapping[str, Any] | None = None,
    subscription_key: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Walk pagination until an empty page or max_pages.
    Returns (flattened_items, page_summaries_for_audit_meta_only).
    """
    summaries: list[dict[str, Any]] = []
    items_out: list[dict[str, Any]] = []

    offset = 0
    for page_idx in range(max(1, int(max_pages))):
        chunk = fetch_waste_data(
            offset=offset,
            limit=int(limit_per_page),
            extra_params=extra_params,
            subscription_key=subscription_key,
        )
        summaries.append(
            {
                "page": page_idx,
                "offset": offset,
                "limit": int(limit_per_page),
                "ok": chunk.get("ok"),
                "status_code": chunk.get("status_code"),
                "url": chunk.get("url"),
            }
        )
        if not chunk.get("ok"):
            raise RuntimeError(chunk.get("text") or "Engage Waste API request failed.")

        payload = chunk.get("json")
        page_items = _extract_waste_items(payload)
        summaries[-1]["rows"] = len(page_items)

        if not page_items:
            break

        items_out.extend(page_items)

        if len(page_items) < int(limit_per_page):
            break

        offset += int(limit_per_page)

    return items_out, summaries


def _dig(mapping: Mapping[str, Any] | None, *path: str) -> Any:
    cur: Any = mapping
    for p in path:
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(p)
    return cur


def _parse_pickup_date(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    iso = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s[:19], fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _coerce_weight_kg(volume: object, unit: object | None = None) -> float | None:
    try:
        if volume is None:
            return None
        if isinstance(volume, (int, float)):
            v = float(volume)
            return v if math.isfinite(v) else None
        s = str(volume).strip().replace(",", ".")
        if not s:
            return None
        v = float(s)
        if not math.isfinite(v):
            return None
    except (TypeError, ValueError):
        return None

    u = str(unit or "").strip().lower()
    if u in {"t", "ton", "tons", "tonne", "tonnes", "metric ton", "mt"}:
        return v * 1000.0
    if u in {"g", "gram", "grams"}:
        return v / 1000.0
    return v


def _format_weight_cell(value: float | None) -> str:
    if value is None:
        return ""
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    text = f"{value:.10f}".rstrip("0").rstrip(".")
    return text


def generate_engage_waste_dedup_key(row: Mapping[str, Any]) -> str:
    """Prefer entryUUID; otherwise deterministic composite."""
    eu = str(row.get("entryUUID") or row.get("entryUuid") or row.get("entry_uuid") or "").strip()
    if eu:
        return hashlib.sha256(f"uuid:{eu.lower()}".encode("utf-8")).hexdigest()

    pickup = str(row.get("_pickup_raw") or row.get("pickup_location_raw") or "").strip().lower()
    dt_src = row.get("wastePickupDate") or row.get("_reporting_period_used") or ""
    dt_key = str(dt_src).strip().lower()
    code = str(row.get("wasteCode") or "").strip().lower()
    name = str(row.get("wasteName") or "").strip().lower()
    vol_tok = _format_weight_cell(_coerce_weight_kg(row.get("wasteVolume"), row.get("wasteUnit")))
    loc_id = str(
        row.get("locationId")
        or row.get("_location_id")
        or _dig(row.get("location") if isinstance(row.get("location"), Mapping) else None, "locationId")
        or ""
    ).strip().lower()

    raw = "|".join([pickup, dt_key, code, name, vol_tok, loc_id])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_engage_waste_rows(raw_items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Flatten nested API shapes into row dicts used by builders."""
    out: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, Mapping):
            continue
        row = dict(item)
        loc = row.get("location") if isinstance(row.get("location"), Mapping) else {}
        pickup = row.get("locationName") or _dig(loc, "locationName") or row.get("pickUpLocation") or ""
        row["_pickup_raw"] = str(pickup or "").strip()
        row["_location_id"] = str(
            row.get("locationId") or _dig(loc, "locationId") or _dig(loc, "id") or ""
        ).strip()
        out.append(row)
    return out


def build_scope3_cat5_data_entry_rows(
    normalized_rows: Sequence[Mapping[str, Any]],
    headers: Sequence[str],
    *,
    reporting_period_fallback: str = "",
    translator_cache: MutableMapping[str, str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Build preview/import payloads aligned to template headers.

    Each element: {
      "dedup_key": str,
      "cells": list[str],
      "preview": {
          "pickup_location_raw": str,
          "unmapped_site_tag": bool,
          "waste_stream_en": str,
      },
    }
    """
    from frontend.services.reporting_period_service import normalize_reporting_period

    stats = {"input_rows": 0, "ready_rows": 0, "skipped_missing_weight": 0, "unmapped_site_tags": 0}

    previews: list[dict[str, Any]] = []

    hdr = [str(h or "").strip() for h in headers]

    def cell_map(
        *,
        rp: str,
        waste_stream: str,
        weight: str,
        weight_unit: str,
        treatment: str,
        country: str,
        site_tag: str,
        data_source: str,
    ) -> list[str]:
        mapping = {
            "Reporting period (month, year)": rp,
            "Waste Stream": waste_stream,
            "Weight": weight,
            "Weight Unit": weight_unit,
            "Treatment Method": treatment,
            "Country": country,
            "Site Tag": site_tag,
            "Data Source": data_source,
        }
        return [mapping.get(col, "") for col in hdr]

    for row in normalized_rows:
        stats["input_rows"] += 1

        pickup_raw = str(row.get("_pickup_raw") or "").strip()
        site_tag, unmapped_site = map_pickup_location_to_site_tag(pickup_raw)
        if unmapped_site:
            stats["unmapped_site_tags"] += 1

        waste_code = str(row.get("wasteCode") or "").strip()
        waste_name = str(row.get("wasteName") or row.get("wasteType") or "").strip()
        waste_stream_src = " ".join(x for x in (waste_code, waste_name) if x).strip() or waste_name or waste_code

        waste_stream_en = translate_waste_stream(waste_stream_src, translator_cache=translator_cache)

        wt_kg = _coerce_weight_kg(row.get("wasteVolume"), row.get("wasteUnit"))
        if wt_kg is None or wt_kg <= 0:
            stats["skipped_missing_weight"] += 1
            continue

        dt = _parse_pickup_date(row.get("wastePickupDate"))
        rp = ""
        if dt:
            rp = normalize_reporting_period(dt)
        if not rp and reporting_period_fallback:
            rp = normalize_reporting_period(reporting_period_fallback)
        if not rp and dt:
            rp = normalize_reporting_period(dt.date())

        treatment = str(row.get("treatmentMethod") or row.get("treatment") or "").strip()

        dedup_row = {
            "entryUUID": row.get("entryUUID") or row.get("entryUuid"),
            "_pickup_raw": pickup_raw,
            "wastePickupDate": row.get("wastePickupDate"),
            "_reporting_period_used": rp,
            "wasteCode": waste_code,
            "wasteName": waste_name,
            "wasteVolume": wt_kg,
            "wasteUnit": "kg",
            "locationId": row.get("locationId") or row.get("_location_id"),
        }
        dedup_key = generate_engage_waste_dedup_key(dedup_row)

        cells = cell_map(
            rp=rp,
            waste_stream=waste_stream_en,
            weight=_format_weight_cell(wt_kg),
            weight_unit="kg",
            treatment=treatment,
            country="Norway",
            site_tag=site_tag,
            data_source=ENGAGE_DATA_SOURCE_LABEL,
        )

        bundle_row_index = len(previews)
        previews.append(
            {
                "dedup_key": dedup_key,
                "cells": cells,
                "bundle_row_index": bundle_row_index,
                "preview": {
                    "pickup_location_raw": pickup_raw,
                    "unmapped_site_tag": unmapped_site,
                    "waste_stream_en": waste_stream_en,
                    "reporting_period": rp,
                },
            }
        )
        stats["ready_rows"] += 1

    return previews, stats


def write_preview_bundle(path: Path, bundle: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")


def read_preview_bundle(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def update_status_file(fields: Mapping[str, Any]) -> Path:
    """Merge into engage_waste/status.json"""
    root = _repo_storage_engage_root()
    status_path = root / "status.json"
    prev: dict[str, Any] = {}
    if status_path.exists():
        try:
            loaded = json.loads(status_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                prev = loaded
        except Exception:
            prev = {}
    prev.update({str(k): v for k, v in fields.items()})
    prev["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status_path.write_text(json.dumps(prev, ensure_ascii=False, indent=2), encoding="utf-8")
    return status_path
