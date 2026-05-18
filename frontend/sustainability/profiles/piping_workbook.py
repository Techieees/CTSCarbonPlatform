"""DC Piping — frozen workbook material composition profile (Cat 12)."""

from __future__ import annotations

from typing import Any

WORKBOOK_PROFILE_KEY = "dc-piping-workbook-composition-v1"
COMPANY_KEY = "DC Piping"
PRODUCT_TYPE = "Piping"

# Workbook: Carbon Steel 95% → recycling; Other 5% → combustion
MATERIAL_COMPOSITION: dict[str, dict[str, Any]] = {
    "carbon_steel": {
        "label": "Carbon Steel",
        "composition_pct": 95.0,
        "disposal_stream": "recycling",
    },
    "other_material": {
        "label": "Other material",
        "composition_pct": 5.0,
        "disposal_stream": "combustion",
    },
}


def is_dc_piping_workbook(company_key: str, product_type: str | None) -> bool:
    from frontend.sustainability.workflow_registry import normalize_company_key

    company = normalize_company_key(company_key)
    pt = str(product_type or "").strip().casefold()
    return company == COMPANY_KEY and pt in {"piping", "pipe"}
