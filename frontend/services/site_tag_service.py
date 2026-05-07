"""Canonical site tag reference (CCC / Data Entry foundation)."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

_WS_RE = re.compile(r"\s+")
_LEADING_NUM_DASH = re.compile(r"^\s*\d+\s*-\s*", re.IGNORECASE)
_PRO_PREFIX = re.compile(r"(?i)^PRO\d+\s*")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _config_path() -> Path:
    return _project_root() / "config" / "site_tags_2026.json"


def normalize_site_tag(value: object) -> str:
    """Collapse whitespace; preserve casing (canonical labels come from JSON)."""
    if value is None:
        return ""
    s = _WS_RE.sub(" ", str(value).strip())
    return s


def _fold_key(value: str) -> str:
    return normalize_site_tag(value).casefold()


def load_site_tags() -> list[dict[str, str]]:
    path = _config_path()
    with path.open(encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, list):
        raise ValueError("site_tags_2026.json must be a JSON array")
    rows: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "project_name": str(item.get("project_name") or ""),
                "platform_site_tag": str(item.get("platform_site_tag") or ""),
                "project_location": str(item.get("project_location") or ""),
                "responsible_company": str(item.get("responsible_company") or ""),
            }
        )
    return rows


def _expand_variants(text: str) -> list[str]:
    """Generate normalized search variants from a free-form project / tag string."""
    base = normalize_site_tag(text)
    if not base:
        return []
    seen: set[str] = set()
    out: list[str] = []

    def push(x: str) -> None:
        nx = normalize_site_tag(x)
        if nx and nx not in seen:
            seen.add(nx)
            out.append(nx)

    push(base)
    stripped_dash = _LEADING_NUM_DASH.sub("", base).strip()
    push(stripped_dash)
    cur = stripped_dash
    while True:
        nxt = _PRO_PREFIX.sub("", cur).strip()
        if nxt == cur:
            break
        push(nxt)
        cur = nxt
    return out


def _register_variant(resolve_map: dict[str, str], variant: str, canonical: str) -> None:
    for piece in _expand_variants(variant):
        k = piece.casefold()
        if k:
            resolve_map[k] = canonical


@lru_cache(maxsize=1)
def _resolution_map() -> dict[str, str]:
    resolve: dict[str, str] = {}
    for row in load_site_tags():
        canon = normalize_site_tag(row.get("platform_site_tag") or "")
        if not canon:
            continue
        pn = normalize_site_tag(row.get("project_name") or "")
        _register_variant(resolve, canon, canon)
        if pn:
            _register_variant(resolve, pn, canon)
    return resolve


def resolve_registered_project(project_label: object) -> dict[str, str] | None:
    """
    Match a CCC/UI project label to a row in site_tags_2026 (case-insensitive, variant-aware).
    Returns platform_site_tag, responsible_company, and project_location, or None if unknown.
    """
    cleaned = normalize_site_tag(project_label or "")
    if not cleaned:
        return None
    mp = _resolution_map()
    for variant in _expand_variants(cleaned):
        canon = mp.get(variant.casefold())
        if not canon:
            continue
        cf_canon = canon.casefold()
        for row in load_site_tags():
            pst = normalize_site_tag(row.get("platform_site_tag"))
            if pst.casefold() == cf_canon:
                return {
                    "platform_site_tag": pst,
                    "responsible_company": normalize_site_tag(row.get("responsible_company")),
                    "project_location": normalize_site_tag(row.get("project_location")),
                }
    return None


def get_site_tags_for_company(company_name: str) -> list[str]:
    """Distinct canonical platform_site_tag values for responsible_company (case-insensitive)."""
    want = _fold_key(company_name or "")
    if not want:
        return []
    tags: list[str] = []
    seen_fold: set[str] = set()
    for row in load_site_tags():
        rc = normalize_site_tag(row.get("responsible_company") or "")
        if _fold_key(rc) != want:
            continue
        canon = normalize_site_tag(row.get("platform_site_tag") or "")
        if not canon:
            continue
        fk = canon.casefold()
        if fk in seen_fold:
            continue
        seen_fold.add(fk)
        tags.append(canon)
    tags.sort(key=lambda x: x.casefold())
    return tags


def resolve_site_tag_from_project_name(project_name: object) -> str:
    """
    Map CCC-style project names to canonical platform_site_tag when known.
    Unknown values return normalize_site_tag(project_name); never raises.
    """
    cleaned = normalize_site_tag(project_name or "")
    if not cleaned:
        return ""
    mp = _resolution_map()
    for variant in _expand_variants(cleaned):
        hit = mp.get(variant.casefold())
        if hit:
            return hit
    return cleaned
