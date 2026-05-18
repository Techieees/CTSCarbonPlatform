"""
Nordic EPOD — frozen CTS Group workbook lifecycle (literal audited kg values).

Official workbook assumptions at reference product weight 47,863 kg.
Do NOT normalize, rebalance, infer %, or route through generic disposal-ratio logic.

Cat 9: company transport table (seed).
Cat 11: fixed lifetime kWh (workbook).
Cat 12: literal kg waste streams below.
"""

from __future__ import annotations

from typing import Any

WORKBOOK_PROFILE_KEY = "nordic-epod-workbook-literal-kg-v1"
COMPANY_KEY = "Nordic EPOD"
PRODUCT_TYPE = "EPOD"

# --- Workbook reference (audited) ---
REFERENCE_PRODUCT_WEIGHT_KG = 47_863.0

# --- Category 11 ---
LIFETIME_END_PRODUCT_KWH = 1_480_000

# --- Category 12: EPD supplied information (kg) ---
EPD_SUPPLIED = {
    "label": "EPD supplied information",
    "subtotal_kg": 42_506.0,
    "streams_kg": {
        "recycling": 29_925.0,
        "energy_recovery": 3_877.0,
        "landfill": 8_704.0,
    },
}

# --- Packaging (kg) — 50/50 recycling and energy recovery per workbook ---
PACKAGING = {
    "label": "Packaging",
    "packaging_total_kg": 57.0,
    "processing_note": "50/50 recycling and energy recovery",
    "streams_kg": {
        "recycling": 28.5,
        "energy_recovery": 28.5,
    },
}

# --- Switchboard PEP EOL (kg) ---
SWITCHBOARD_PEP = {
    "label": "Switchboard PEP EOL",
    "switchboard_total_kg": 5_300.0,
    "streams_kg": {
        "recycling": 5_088.0,
        "landfill": 212.0,
    },
}

# --- Final total waste stream tally (kg) — workbook audited totals ---
FINAL_WASTE_STREAM_TALLY_KG = {
    "recycling": 35_041.5,
    "energy_recovery": 3_905.5,
    "landfill": 8_916.0,
    "final_total": 47_863.0,
}

EMISSION_FACTORS_KG_PER_KG: dict[str, float] = {
    "recycling": 0.00641061,
    "energy_recovery": 0.0212808072368763,
    "landfill": 0.5203342,
}

# Ordered audit sections for UI / run output
WORKBOOK_SECTIONS: tuple[dict[str, Any], ...] = (
    EPD_SUPPLIED,
    PACKAGING,
    SWITCHBOARD_PEP,
)


def _lifecycle_table_row(
    section: str,
    streams: dict[str, float],
    *,
    is_total: bool = False,
) -> dict[str, Any]:
    return {
        "section": section,
        "recycling_kg": float(streams.get("recycling") or 0),
        "energy_recovery_kg": float(streams.get("energy_recovery") or 0),
        "landfill_kg": float(streams.get("landfill") or 0),
        "is_total": is_total,
    }


def admin_workbook_lifecycle_rows() -> list[dict[str, Any]]:
    """Read-only admin grid rows (literal kg, three waste streams)."""
    return [
        _lifecycle_table_row("EPD supplied", EPD_SUPPLIED["streams_kg"]),
        _lifecycle_table_row(
            "Packaging",
            {
                "recycling": PACKAGING["streams_kg"]["recycling"],
                "energy_recovery": PACKAGING["streams_kg"]["energy_recovery"],
                "landfill": 0.0,
            },
        ),
        _lifecycle_table_row(
            "Switchboard PEP",
            {
                "recycling": SWITCHBOARD_PEP["streams_kg"]["recycling"],
                "energy_recovery": 0.0,
                "landfill": SWITCHBOARD_PEP["streams_kg"]["landfill"],
            },
        ),
        _lifecycle_table_row("FINAL TOTAL", FINAL_WASTE_STREAM_TALLY_KG, is_total=True),
    ]


def workbook_profile_document() -> dict[str, Any]:
    """Full audited workbook snapshot for cards, API, and run transparency."""
    return {
        "profile_key": WORKBOOK_PROFILE_KEY,
        "company_key": COMPANY_KEY,
        "product_type": PRODUCT_TYPE,
        "methodology_type": "nordic_epod_workbook_literal_kg",
        "reference_product_weight_kg": REFERENCE_PRODUCT_WEIGHT_KG,
        "category_11": {
            "lifetime_end_product_kwh": LIFETIME_END_PRODUCT_KWH,
            "formula": "lifetime_kwh × country_electricity_ef",
        },
        "category_12": {
            "structure": "literal_workbook_kg",
            "epd_supplied": EPD_SUPPLIED,
            "packaging": PACKAGING,
            "switchboard_pep": SWITCHBOARD_PEP,
            "final_waste_stream_tally_kg": FINAL_WASTE_STREAM_TALLY_KG,
            "emission_factors_kg_per_kg": EMISSION_FACTORS_KG_PER_KG,
        },
        "category_9": {
            "structure": "company_transport_route_table",
            "company_key": COMPANY_KEY,
        },
    }


def calculate_category12_workbook(
    product_weight_kg: float,
    *,
    emission_factors: dict[str, float] | None = None,
) -> dict[str, Any]:
    """
    Apply audited workbook kg exactly (reference product weight 47,863 kg).

    Cat 12 always uses literal workbook waste kg — not % splits, not rebalance.
    `product_weight_kg` is recorded for traceability; waste streams are fixed workbook values.
    """
    ef = emission_factors or EMISSION_FACTORS_KG_PER_KG
    _ = float(product_weight_kg)  # traceability only for Cat 12

    def section_result(section: dict[str, Any], section_key: str) -> dict[str, Any]:
        streams_out: list[dict[str, Any]] = []
        for stream, literal_kg in section["streams_kg"].items():
            ef_val = ef.get(stream, 0.0)
            emissions_kg = literal_kg * ef_val
            streams_out.append(
                {
                    "disposal_stream": stream,
                    "waste_kg": literal_kg,
                    "emission_factor_kg_per_kg": ef_val,
                    "emissions_kg": emissions_kg,
                }
            )
        subtotal_key = "subtotal_kg"
        subtotal = section.get(subtotal_key) or section.get("packaging_total_kg") or section.get("switchboard_total_kg")
        return {
            "section_key": section_key,
            "label": section["label"],
            "streams": streams_out,
            "subtotal_kg": subtotal,
        }

    workbook_components = [
        section_result(EPD_SUPPLIED, "epd_supplied"),
        section_result(PACKAGING, "packaging"),
        section_result(SWITCHBOARD_PEP, "switchboard_pep"),
    ]

    aggregated_streams: list[dict[str, Any]] = []
    total_kg_co2e = 0.0
    for stream in ("recycling", "energy_recovery", "landfill"):
        w = FINAL_WASTE_STREAM_TALLY_KG[stream]
        ef_val = ef.get(stream, 0.0)
        emissions_kg = w * ef_val
        total_kg_co2e += emissions_kg
        aggregated_streams.append(
            {
                "disposal_stream": stream,
                "waste_kg": w,
                "emissions_kg": emissions_kg,
                "emissions_t": emissions_kg / 1000.0,
            }
        )

    return {
        "methodology_type": "nordic_epod_workbook_literal_kg",
        "profile_key": WORKBOOK_PROFILE_KEY,
        "scenario_label": "Nordic EPOD Workbook Literal kg (Cat 12)",
        "product_type": PRODUCT_TYPE,
        "company_key": COMPANY_KEY,
        "workbook_product_weight_kg": REFERENCE_PRODUCT_WEIGHT_KG,
        "reported_product_weight_kg": float(product_weight_kg),
        "epd_supplied": EPD_SUPPLIED,
        "packaging": PACKAGING,
        "switchboard_pep": SWITCHBOARD_PEP,
        "workbook_components": workbook_components,
        "final_waste_stream_tally_kg": dict(FINAL_WASTE_STREAM_TALLY_KG),
        "streams": aggregated_streams,
        "total_emissions_kg": total_kg_co2e,
        "total_emissions_t": total_kg_co2e / 1000.0,
        "audit_note": "Literal audited workbook kg at 47,863 kg reference — no % inference",
    }


def is_nordic_epod_workbook(company_key: str, product_type: str | None) -> bool:
    from frontend.sustainability.workflow_registry import normalize_company_key

    company = normalize_company_key(company_key)
    pt = str(product_type or "").strip()
    return company == COMPANY_KEY and pt.upper() == "EPOD"


def sync_workbook_snapshot_to_db() -> None:
    """Persist literal kg rows for admin read-only audit (not ratio_pct methodology)."""
    import json

    from frontend.extensions import db
    from frontend.sustainability.models import EolComponentProfile, EolScenario, MethodologyVersion

    meth = MethodologyVersion.query.filter_by(version_key="nordic-epod-eol-v1").first()
    if meth is None:
        return

    scenario = EolScenario.query.filter_by(
        methodology_version_id=meth.id,
        scenario_key="epod-workbook-lifecycle",
    ).first()
    if scenario is None:
        scenario = EolScenario(
            methodology_version_id=meth.id,
            scenario_key="epod-workbook-lifecycle",
            label="Nordic EPOD Workbook Literal kg",
            product_type=PRODUCT_TYPE,
            company_key=COMPANY_KEY,
        )
        db.session.add(scenario)
        db.session.flush()

    scenario.methodology_type = "nordic_epod_workbook_literal_kg"
    scenario.description = json.dumps(workbook_profile_document(), ensure_ascii=True)
    scenario.is_default = True
    EolComponentProfile.query.filter_by(eol_scenario_id=scenario.id).delete(
        synchronize_session=False
    )
    db.session.commit()
