"""
Resolve internal supplier normalized tokens for double counting.

Priority:
1. SQLite `internal_supplier_registry` rows (PLATFORM_GHG_DB_PATH, default frontend/instance/ghg_data.db)
2. Cache JSON (`engine/stage2_mapping/cache/internal_dc_tokens.json`) from the Flask export job
3. Legacy seed file (`frontend/data/supplier_mgmt/internal_dc_seed.json`) — same supplier_names as bootstrap; keeps batch/offline parity when DB/cache are empty
4. Empty set (Rule 1 no-ops until data exists)
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
    Tokens used by Rule 1 supplier matching (CTS source + internal provider).
    """
    from_db = _load_tokens_from_sqlite(resolve_default_db_path())
    if from_db is not None and len(from_db) > 0:
        return from_db
    cached = _load_tokens_from_cache_file()
    if cached is not None and len(cached) > 0:
        return cached
    seeded = _load_tokens_from_seed_json()
    return seeded if seeded is not None else set()
