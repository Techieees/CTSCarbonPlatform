"""
CTS workbook workflow controller — business function drives UI, categories, and calculations.
"""

from __future__ import annotations

from typing import Any

# Canonical business functions (profile_setup uses title-case variants; normalize on read).
BUSINESS_FUNCTIONS: tuple[str, ...] = (
    "Service Provider",
    "Manufacturer",
    "Construction (CTS Office)",
    "Execution",
)

MANUFACTURER_COMPANIES: frozenset[str] = frozenset({"Nordic EPOD", "DC Piping", "GT Nordics"})

# Fixed workbook category activation by company (not product-type generic).
COMPANY_ENABLED_CATEGORIES: dict[str, tuple[str, ...]] = {
    "Nordic EPOD": ("9", "11", "12"),
    "DC Piping": ("9", "12"),
    "GT Nordics": ("9", "11", "12"),
}

PRODUCT_TYPE_OPTIONS: tuple[str, ...] = ("EPOD", "Piping")

ALLOWED_PRODUCT_TYPES: frozenset[str] = frozenset(PRODUCT_TYPE_OPTIONS)


def normalize_product_type(value: object) -> str:
    raw = str(value or "").strip()
    if raw.casefold() == "pipe":
        return "Piping"
    if raw.upper() == "EPOD":
        return "EPOD"
    return raw


def validate_product_type(value: object) -> str | None:
    pt = normalize_product_type(value)
    if pt not in ALLOWED_PRODUCT_TYPES:
        return f"Product type must be one of: {', '.join(PRODUCT_TYPE_OPTIONS)}."
    return None

_BUSINESS_FUNCTION_ALIASES: dict[str, str] = {
    "service provider": "Service Provider",
    "service_provider": "Service Provider",
    "manufacturer": "Manufacturer",
    "construction": "Construction (CTS Office)",
    "construction (cts office)": "Construction (CTS Office)",
    "execution": "Execution",
}


def normalize_business_function(value: object) -> str:
    raw = " ".join(str(value or "").strip().split())
    if not raw:
        return ""
    key = raw.casefold()
    if key in _BUSINESS_FUNCTION_ALIASES:
        return _BUSINESS_FUNCTION_ALIASES[key]
    if raw == "Service provider":
        return "Service Provider"
    if raw == "Construction":
        return "Construction (CTS Office)"
    return raw


def normalize_company_key(value: object) -> str:
    raw = " ".join(str(value or "").strip().split())
    aliases = {
        "nordicepod": "Nordic EPOD",
        "nordic epod": "Nordic EPOD",
        "dc piping": "DC Piping",
        "piping": "DC Piping",
        "gt nordics": "GT Nordics",
    }
    return aliases.get(raw.casefold(), raw)


def company_enabled_categories(company_name: str) -> tuple[str, ...]:
    return COMPANY_ENABLED_CATEGORIES.get(normalize_company_key(company_name), ())


def resolve_workflow_context(
    *,
    business_function: str,
    company_name: str,
    product_type: str | None = None,
) -> dict[str, Any]:
    """Primary controller payload for UI + API + calculation orchestration."""
    bf = normalize_business_function(business_function)
    company = normalize_company_key(company_name)
    pt = str(product_type or "").strip()
    if pt.casefold() == "pipe":
        pt = "Piping"

    company_categories = company_enabled_categories(company)

    capabilities = {
        "questionnaire": True,
        "products_page": False,
        "facility_estimation": False,
        "shared_office": False,
        "employee_commuting": False,
        "category_9": False,
        "category_11": False,
        "category_12": False,
        "gt_monthly_scenario": False,
        "execution_site_ops": False,
    }
    enabled_categories: list[str] = []
    reasons: dict[str, str] = {}

    if bf == "Manufacturer":
        if company not in MANUFACTURER_COMPANIES:
            reasons["company"] = "manufacturer_company_not_in_workbook"
        else:
            capabilities["products_page"] = True
            for cat in company_categories:
                enabled_categories.append(cat)
                capabilities[f"category_{cat}"] = True
                reasons[cat] = f"workbook_company_{company.replace(' ', '_').lower()}"
            if company == "GT Nordics":
                capabilities["gt_monthly_scenario"] = True

    elif bf == "Service Provider":
        capabilities["facility_estimation"] = True
        capabilities["employee_commuting"] = True
        reasons["facility"] = "service_provider_office_workflow"

    elif bf == "Construction (CTS Office)":
        capabilities["facility_estimation"] = True
        capabilities["shared_office"] = True
        reasons["facility"] = "cts_office_construction_workflow"

    elif bf == "Execution":
        capabilities["execution_site_ops"] = True
        capabilities["facility_estimation"] = True
        reasons["execution"] = "execution_extensible_workflow"

    # Intersect with company rules when manufacturer company is known
    if company and company_categories and bf == "Manufacturer":
        enabled_categories = [c for c in enabled_categories if c in company_categories]

    return {
        "business_function": bf,
        "company_name": company,
        "product_type": pt or None,
        "enabled_categories": enabled_categories,
        "disabled_categories": [c for c in ("9", "11", "12") if c not in enabled_categories],
        "capabilities": capabilities,
        "reason_by_category": reasons,
        "is_manufacturer_company": company in MANUFACTURER_COMPANIES,
        "company_category_rules": list(company_categories),
    }


def validate_calculation_request(
    workflow: dict[str, Any],
    *,
    requested_categories: list[str] | None = None,
) -> list[str]:
    errors: list[str] = []
    caps = workflow.get("capabilities") or {}
    enabled = set(workflow.get("enabled_categories") or [])
    for cat in requested_categories or []:
        key = f"category_{cat}"
        if cat not in enabled or not caps.get(key):
            errors.append(f"Category {cat} is not enabled for this business function and company.")
    if caps.get("products_page") and not enabled:
        errors.append("Manufacturer workflow requires an enabled company methodology profile.")
    return errors
