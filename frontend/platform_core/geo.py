"""Geographic reference data (no app.py dependency)."""

from __future__ import annotations

import json
from pathlib import Path

_FRONTEND_DIR = Path(__file__).resolve().parents[1]
ISO_COUNTRIES_PATH = _FRONTEND_DIR / "data" / "iso_countries.json"


def load_iso_countries() -> list[tuple[str, str]]:
    try:
        with ISO_COUNTRIES_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return []
    out: list[tuple[str, str]] = []
    if not isinstance(raw, list):
        return out
    for row in raw:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").strip().upper()
        name = str(row.get("name") or "").strip()
        if code and name:
            out.append((code, name))
    out.sort(key=lambda x: x[1].casefold())
    return out
