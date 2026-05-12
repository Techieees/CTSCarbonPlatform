"""Shared supplier name normalization (aligned with double-counting workbook rules)."""

from __future__ import annotations

import re
from typing import Optional

_WS = re.compile(r"\s+")


def normalize_supplier_key(text: Optional[str]) -> str:
    if text is None:
        return ""
    s = str(text).strip().lower()
    if not s:
        return ""
    s = s.replace(".xlsx", "").replace(".xls", "")
    s = s.replace("\u00a0", " ")
    s = _WS.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return s
