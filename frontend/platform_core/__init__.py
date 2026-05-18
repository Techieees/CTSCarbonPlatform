"""Import-safe platform primitives (no Flask app factory)."""

from frontend.platform_core.companies import (
    clean_company_name,
    company_country_canonical,
    list_registered_companies,
    list_sustainability_company_options,
    resolve_template_company_name,
)
from frontend.platform_core.geo import load_iso_countries
from frontend.platform_core.reporting import products_current_period
from frontend.platform_core.roles import is_owner_user, is_readonly_user, normalize_user_role

__all__ = [
    "clean_company_name",
    "company_country_canonical",
    "is_owner_user",
    "is_readonly_user",
    "list_registered_companies",
    "list_sustainability_company_options",
    "load_iso_countries",
    "normalize_user_role",
    "products_current_period",
    "resolve_template_company_name",
]
