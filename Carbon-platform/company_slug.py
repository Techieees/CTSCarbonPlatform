from __future__ import annotations

import re


def company_slug(value: str) -> str:
    raw = str(value or "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return slug or "company"
