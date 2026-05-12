"""
Export active internal suppliers for stage-2 double counting (offline / batch jobs).

Writes a token list consumable by engine.stage2_mapping.internal_supplier_registry.
"""

from __future__ import annotations

import json
from pathlib import Path

from frontend.services.supplier_normalize import normalize_supplier_key


def internal_dc_cache_path() -> Path:
    """Stable path under engine stage2 tree (no secrets)."""
    root = Path(__file__).resolve().parents[2]
    d = root / "engine" / "stage2_mapping" / "cache"
    d.mkdir(parents=True, exist_ok=True)
    return d / "internal_dc_tokens.json"


def export_internal_supplier_tokens_for_stage2(db_session, InternalSupplierModel) -> Path:
    """Persist normalized tokens for batch double-counting runs."""
    rows = (
        db_session.query(InternalSupplierModel)
        .filter(
            InternalSupplierModel.deleted_at.is_(None),
            InternalSupplierModel.active.is_(True),
        )
        .all()
    )
    tokens: set[str] = set()
    for r in rows:
        t = normalize_supplier_key(getattr(r, "supplier_name", None) or "")
        if t:
            tokens.add(t)
    path = internal_dc_cache_path()
    path.write_text(
        json.dumps({"normalized_tokens": sorted(tokens), "source": "platform_db"}, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    return path
