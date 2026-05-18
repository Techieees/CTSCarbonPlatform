"""Sustainability-facing platform helpers (import-safe; no frontend.app)."""

from __future__ import annotations

from frontend.platform_core.companies import (
    clean_company_name,
    list_sustainability_company_options,
)
from frontend.platform_core.geo import load_iso_countries
from frontend.platform_core.reporting import products_current_period
from frontend.platform_core.roles import is_owner_user, is_readonly_user

__all__ = [
    "clean_company_name",
    "current_reporting_period",
    "is_owner_user",
    "is_readonly_user",
    "iso_countries",
    "list_company_options",
]


def iso_countries() -> list[tuple[str, str]]:
    return load_iso_countries()


def current_reporting_period() -> tuple[str, str]:
    return products_current_period()


def list_company_options() -> list[str]:
    return list_sustainability_company_options()
