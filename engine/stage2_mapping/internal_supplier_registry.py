"""
Dynamic internal supplier tokens for double-counting Rule 1 (non-built-in layer).

Returns the UNION of all available runtime sources (never "first wins"):
- SQLite `internal_supplier_registry` active rows (PLATFORM_GHG_DB_PATH, default frontend/instance/ghg_data.db)
- Cache JSON (`engine/stage2_mapping/cache/internal_dc_tokens.json`) from the Flask export job
- Seed JSON (`frontend/data/supplier_mgmt/internal_dc_seed.json`) optional extra aliases

Built-in canonical aliases live in `builtin_internal_supplier_aliases.py` and are merged in
`double_countin_booklets._build_internal_sets()` — not here — so CCC / supplier sync stays decoupled.

This module MUST NOT import Flask or pandas.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_WS = re.compile(r"\s+")


def _normalize_token_local(text: Any) -> str:
    if text is None:
        return ""
    s = str(text).strip().lower()
    if not s:
        return ""
    s = s.replace(".xlsx", "").replace(".xls", "")
    s = s.replace("\u00a0", " ")
    s = _WS.sub(" ", s)
    return re.sub(r"[^a-z0-9 ]", "", s)


def resolve_default_db_path() -> Path:
    env = (os.getenv("PLATFORM_GHG_DB_PATH") or "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (PROJECT_ROOT / "frontend" / "instance" / "ghg_data.db").resolve()


def _load_tokens_from_sqlite(db_path: Path) -> set[str] | None:
    if not db_path.is_file():
        return None
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.execute(
                "SELECT supplier_name FROM internal_supplier_registry "
                "WHERE deleted_at IS NULL AND (active IS NULL OR active = 1)"
            )
            tokens: set[str] = set()
            for (name,) in cur.fetchall():
                t = _normalize_token_local(name)
                if t:
                    tokens.add(t)
            return tokens
        finally:
            conn.close()
    except Exception:
        return None


def _load_tokens_from_seed_json() -> set[str] | None:
    seed = PROJECT_ROOT / "frontend" / "data" / "supplier_mgmt" / "internal_dc_seed.json"
    if not seed.is_file():
        return None
    try:
        data = json.loads(seed.read_text(encoding="utf-8"))
        arr = data.get("supplier_names") if isinstance(data, dict) else None
        if not isinstance(arr, list) or not arr:
            return None
        out: set[str] = set()
        for x in arr:
            t = _normalize_token_local(x)
            if t:
                out.add(t)
        return out if out else None
    except Exception:
        return None


def _load_tokens_from_cache_file() -> set[str] | None:
    cache = PROJECT_ROOT / "engine" / "stage2_mapping" / "cache" / "internal_dc_tokens.json"
    if not cache.is_file():
        return None
    try:
        data = json.loads(cache.read_text(encoding="utf-8"))
        arr = data.get("normalized_tokens") if isinstance(data, dict) else None
        if not isinstance(arr, list):
            return None
        out: set[str] = set()
        for x in arr:
            t = _normalize_token_local(x)
            if t:
                out.add(t)
        return out
    except Exception:
        return None


def load_internal_supplier_normalized_tokens() -> set[str]:
    """
    Union of dynamic sources (DB ∪ cache ∪ seed). Empty components are skipped.

    Built-in baseline aliases are merged separately in double_countin_booklets._build_internal_sets.
    """
    combined: set[str] = set()
    from_db = _load_tokens_from_sqlite(resolve_default_db_path())
    if from_db is not None:
        combined |= from_db
    cached = _load_tokens_from_cache_file()
    if cached is not None:
        combined |= cached
    seeded = _load_tokens_from_seed_json()
    if seeded is not None:
        combined |= seeded
    return combined
