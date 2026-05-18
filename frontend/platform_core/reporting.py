"""Reporting period helpers (no app.py dependency)."""

from __future__ import annotations

from datetime import date


def products_current_period(today: date | None = None) -> tuple[str, str]:
    value = today or date.today()
    return value.strftime("%Y-%m"), value.strftime("%B %Y")
