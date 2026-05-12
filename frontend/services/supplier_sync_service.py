"""
CCC supplier directory synchronization into `ccc_supplier_registry`.

Uses live CCC `/supplier` pagination via `ccc_client.get_paginated`.
Does not modify CCC purchase-order import paths.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Callable

from frontend.services.supplier_normalize import normalize_supplier_key


def _strip(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _pick(item: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in item and item[k] not in (None, ""):
            return item[k]
    return None


def stable_synthetic_external_id(normalized_key: str) -> str:
    h = hashlib.sha256(normalized_key.encode("utf-8")).hexdigest()[:28]
    return f"ccc:synth:{h}"


def parse_ccc_supplier_row(row: dict[str, Any]) -> dict[str, Any]:
    """Map arbitrary CCC supplier JSON into registry columns."""
    ext = _pick(row, "id", "supplierId", "supplierID", "supplier_id", "code", "supplierCode")
    ext_s = _strip(ext) if ext is not None else ""
    name = _strip(_pick(row, "name", "supplierName", "title", "companyName"))
    normalized = normalize_supplier_key(name) or normalize_supplier_key(ext_s)
    if not ext_s:
        ext_s = stable_synthetic_external_id(normalized or json.dumps(row, sort_keys=True, ensure_ascii=True)[:200])
    country = _strip(_pick(row, "country", "countryName", "countryCode", "address.country"))
    currency = _strip(_pick(row, "currency", "currencyCode", "defaultCurrency"))
    org = _strip(_pick(row, "organization", "organizationName", "company.name", "parentCompany"))
    stype = _strip(_pick(row, "supplierType", "type", "category"))
    usage = _pick(row, "usageCount", "referenceCount", "purchaseOrderCount")
    usage_int = None
    if usage is not None:
        try:
            usage_int = int(float(usage))
        except (TypeError, ValueError):
            usage_int = None
    return {
        "external_supplier_id": ext_s[:128],
        "supplier_name": name[:512] if name else ext_s[:512],
        "normalized_name": (normalized or "")[:512],
        "country": country[:120] if country else None,
        "currency": currency[:32] if currency else None,
        "company_relation": org[:256] if org else None,
        "supplier_type": stype[:120] if stype else None,
        "usage_count": usage_int,
        "raw_json": json.dumps(row, ensure_ascii=False, default=str),
    }


def run_ccc_supplier_sync(
    db_session,
    *,
    CccSupplierRegistry: type,
    SupplierSyncCheckpoint: type,
    mode: str,
    user_id: int | None,
    job_progress: Callable[[int, str], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
    commit_session: bool = True,
) -> dict[str, Any]:
    """
    Pull suppliers from CCC and upsert into registry.

    mode: "full" | "incremental" (incremental may fall back to full if API has no delta filter).
    """
    from engine.stage1_preprocess.api_sources import ccc_client
    from engine.stage1_preprocess.api_sources.ccc_purchase_orders import resolve_runtime_config

    stats: dict[str, Any] = {
        "mode": mode,
        "fetched": 0,
        "upserted": 0,
        "skipped": 0,
        "errors": [],
    }

    def prog(p: int, msg: str) -> None:
        if job_progress:
            job_progress(p, msg)

    runtime = resolve_runtime_config()
    base_url = str(runtime.get("base_url") or "").strip()
    username = str(runtime.get("username") or "").strip()
    password = str(runtime.get("password") or "").strip()
    if not base_url or not username or not password:
        raise RuntimeError("CCC API credentials are not configured (base URL / username / password).")

    prog(5, "Connecting to CCC API…")
    query_params: dict[str, Any] = {}
    if str(mode or "").lower() == "incremental":
        cp = (
            db_session.query(SupplierSyncCheckpoint)
            .filter(SupplierSyncCheckpoint.checkpoint_key == "ccc_supplier_last_incremental_anchor")
            .first()
        )
        if cp and (cp.checkpoint_value_json or "").strip():
            try:
                payload = json.loads(cp.checkpoint_value_json or "{}")
                anchor = str(payload.get("anchor") or "").strip()
                if anchor:
                    # Best-effort; CCC may ignore unknown params.
                    query_params["modifiedAfter"] = anchor
            except Exception:
                pass

    prog(12, "Fetching supplier pages from CCC…")
    rows_raw = ccc_client.get_paginated(
        "/supplier",
        base_url=base_url,
        username=username,
        password=password,
        query_params=query_params or None,
        page_size=None,
    )
    items = [x for x in rows_raw if isinstance(x, dict)]
    stats["fetched"] = len(items)
    prog(35, f"Normalizing {len(items)} supplier row(s)…")

    now = datetime.utcnow()
    seen_ext: set[str] = set()

    for idx, item in enumerate(items):
        if cancel_check and idx % 120 == 0:
            cancel_check()
        if idx % 500 == 0:
            prog(35 + int((idx / max(1, len(items))) * 40), f"Upserting suppliers ({idx + 1}/{len(items)})…")
        try:
            parsed = parse_ccc_supplier_row(item)
            ext = parsed["external_supplier_id"]
            if not ext or ext in seen_ext:
                stats["skipped"] += 1
                continue
            seen_ext.add(ext)
            norm = parsed["normalized_name"]
            if not norm:
                stats["skipped"] += 1
                continue

            row = (
                db_session.query(CccSupplierRegistry)
                .filter(
                    CccSupplierRegistry.source_system == "CCC",
                    CccSupplierRegistry.external_supplier_id == ext,
                )
                .first()
            )
            if row is None:
                row = CccSupplierRegistry(
                    external_supplier_id=ext,
                    source_system="CCC",
                    first_synced_at=now,
                )
                db_session.add(row)

            row.supplier_name = parsed["supplier_name"] or row.supplier_name
            row.normalized_name = norm
            row.country = parsed["country"] or row.country
            row.currency = parsed["currency"] or row.currency
            row.company_relation = parsed["company_relation"] or row.company_relation
            row.supplier_type = parsed["supplier_type"] or row.supplier_type
            row.raw_json = parsed["raw_json"]
            row.last_synced_at = now
            row.active = True
            row.deleted_at = None
            if parsed.get("usage_count") is not None:
                row.usage_count = int(parsed["usage_count"])
            stats["upserted"] += 1
        except Exception as exc:
            stats["errors"].append(str(exc)[:500])

    # Checkpoint for incremental best-effort
    cp_row = (
        db_session.query(SupplierSyncCheckpoint)
        .filter(SupplierSyncCheckpoint.checkpoint_key == "ccc_supplier_last_incremental_anchor")
        .first()
    )
    if cp_row is None:
        cp_row = SupplierSyncCheckpoint(checkpoint_key="ccc_supplier_last_incremental_anchor")
        db_session.add(cp_row)
    cp_row.checkpoint_value_json = json.dumps({"anchor": now.strftime("%Y-%m-%dT%H:%M:%SZ")}, ensure_ascii=True)
    cp_row.updated_at = now

    if commit_session:
        db_session.commit()
        prog(95, "Supplier sync committed.")
    else:
        db_session.flush()
        prog(95, "Supplier sync staged.")
    return stats
