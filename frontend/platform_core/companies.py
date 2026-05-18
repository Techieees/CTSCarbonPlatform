"""Canonical company registry and name resolution (no app.py dependency)."""

from __future__ import annotations

# User-provided source-of-truth list (deduplicated).
COMPANY_COUNTRY_CANONICAL: dict[str, str] = {
    "BIMMS": "Portugal",
    "CTS Finland": "Finland",
    "CTS-VDC Services": "Ireland",
    "DC Piping": "Portugal",
    "GT Nordics": "Norway",
    "Mecwide Nordics": "Norway",
    "Porvelox": "Portugal",
    "Caerus Nordics": "Norway",
    "CTS Sweden": "Sweden",
    "Navitas Portugal": "Portugal",
    "NEP Switchboards": "Norway",
    "CTS Denmark": "Denmark",
    "CTS Group": "Switzerland",
    "CTS Nordics": "Norway",
    "Fortica": "Norway",
    "MC Prefab": "Sweden",
    "Navitas Norway": "Norway",
    "Nordic EPOD": "Norway",
    "QEC": "Norway",
    "SD Nordics": "Norway",
    "CTS Security Solutions": "Sweden",
    "Velox": "Norway",
    "CTS EU": "Portugal",
    "Gapit": "Norway",
}

# Sustainability workbook manufacturers (always selectable for owner/admin flows).
SUSTAINABILITY_MANUFACTURER_COMPANIES: frozenset[str] = frozenset(
    {"Nordic EPOD", "DC Piping", "GT Nordics"}
)


def _norm_company_token(s: str) -> str:
    return "".join(ch.lower() for ch in (s or "") if ch.isalnum())


_COMPANY_SYNONYMS: dict[str, str] = {
    _norm_company_token("CTS-VDC"): "CTS-VDC Services",
    _norm_company_token("CTS VDC"): "CTS-VDC Services",
    _norm_company_token("CTS-VDC Services"): "CTS-VDC Services",
    _norm_company_token("CTS Group HQ"): "CTS Group",
    _norm_company_token("CTS Group"): "CTS Group",
    _norm_company_token("NordicEPOD"): "Nordic EPOD",
    _norm_company_token("Nordic EPOD"): "Nordic EPOD",
    _norm_company_token("Caerus Nordics"): "Caerus Nordics",
    _norm_company_token("CTS EU"): "CTS EU",
    _norm_company_token("GT Nordics"): "GT Nordics",
    _norm_company_token("Mecwide Nordics"): "Mecwide Nordics",
    _norm_company_token("Navitas Norway"): "Navitas Norway",
    _norm_company_token("Navitas Portugal"): "Navitas Portugal",
    _norm_company_token("CTS Denmark"): "CTS Denmark",
    _norm_company_token("CTS Finland"): "CTS Finland",
    _norm_company_token("CTS Sweden"): "CTS Sweden",
    _norm_company_token("CTS Nordics"): "CTS Nordics",
    _norm_company_token("CTS Security Solutions"): "CTS Security Solutions",
    _norm_company_token("DC Piping"): "DC Piping",
    _norm_company_token("MC Prefab"): "MC Prefab",
    _norm_company_token("Porvelox"): "Porvelox",
    _norm_company_token("Fortica"): "Fortica",
    _norm_company_token("Gapit"): "Gapit",
    _norm_company_token("QEC"): "QEC",
    _norm_company_token("SD Nordics"): "SD Nordics",
    _norm_company_token("Velox"): "Velox",
    _norm_company_token("BIMMS"): "BIMMS",
    _norm_company_token("NEP Switchboards"): "NEP Switchboards",
    _norm_company_token("NEP Switchboards AS"): "NEP Switchboards",
}


def canonical_company_name_and_country(name: str) -> tuple[str, str | None]:
    raw = (name or "").strip()
    if not raw:
        return "", None

    n = _norm_company_token(raw)
    canonical = _COMPANY_SYNONYMS.get(n, raw)
    country = COMPANY_COUNTRY_CANONICAL.get(canonical)
    if country is None:
        want = _norm_company_token(canonical)
        for key, value in COMPANY_COUNTRY_CANONICAL.items():
            if _norm_company_token(key) == want:
                country = value
                canonical = key
                break

    return canonical, country


def company_country_canonical() -> dict[str, str]:
    return COMPANY_COUNTRY_CANONICAL


def resolve_template_company_name(company_name: str) -> str | None:
    raw = (company_name or "").strip()
    if not raw:
        return None
    canon, _country = canonical_company_name_and_country(raw)
    return (canon or raw).strip() or None


def clean_company_name(value: object) -> str:
    raw = str(value or "").strip()
    return resolve_template_company_name(raw) or raw


def list_registered_companies() -> list[str]:
    return sorted(COMPANY_COUNTRY_CANONICAL.keys())


def list_sustainability_company_options() -> list[str]:
    """Owner-visible companies for sustainability workflows."""
    names = set(COMPANY_COUNTRY_CANONICAL.keys()) | set(SUSTAINABILITY_MANUFACTURER_COMPANIES)
    return sorted(names)
