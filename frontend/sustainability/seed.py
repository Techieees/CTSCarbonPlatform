"""Bootstrap embedded CTS methodology configuration (workbook-aligned, idempotent)."""

from __future__ import annotations

import json

from frontend.extensions import db
from frontend.sustainability.models import (
    AverageFactor,
    Category11Methodology,
    CompanyMethodologyConfig,
    EolComponentProfile,
    EolMaterialComposition,
    EolScenario,
    GtNordicsMonthlyScenario,
    MethodologyVersion,
    SharedOfficeProfile,
    TransportRouteAssumption,
)
from frontend.sustainability.workflow_registry import COMPANY_ENABLED_CATEGORIES

SEED_MARKER_KEY = "cts-workbook-v2"

EF_RECYCLING_KG_PER_KG = 0.00641061
EF_LANDFILL_KG_PER_KG = 0.5203342
EF_ENERGY_RECOVERY_KG_PER_KG = 0.0212808072368763
EF_COMBUSTION_KG_PER_KG = EF_ENERGY_RECOVERY_KG_PER_KG

ROAD_EF_T_PER_TONNE_KM = 0.000059
SEA_EF_T_PER_TONNE_KM = 0.000016

# Workbook-aligned electricity factors (kg CO2e / kWh) for Cat 11
COUNTRY_ELECTRICITY_EF_KG_PER_KWH: dict[str, float] = {
    "Norway": 0.017,
    "Finland": 0.079,
    "Sweden": 0.012,
    "Scandinavia": 0.030,
    "Europe": 0.295,
    "default": 0.030,
}

DESTINATIONS = ("Norway", "Finland", "Sweden", "Scandinavia", "Europe")

# Company-specific Cat 9 route tables (km) — do not share between companies.
TRANSPORT_ROUTES: dict[str, dict[str, tuple[float, float]]] = {
    "Nordic EPOD": {
        "Norway": (300, 0),
        "Finland": (450, 250),
        "Sweden": (380, 120),
        "Scandinavia": (520, 350),
        "Europe": (1400, 650),
    },
    "DC Piping": {
        "Norway": (2850, 4100),
        "Finland": (2650, 3950),
        "Sweden": (2480, 3700),
        "Scandinavia": (2200, 3400),
        "Europe": (750, 180),
    },
    "GT Nordics": {
        "Norway": (280, 0),
        "Finland": (430, 270),
        "Sweden": (360, 140),
        "Scandinavia": (500, 320),
        "Europe": (1350, 620),
    },
}


def _upsert_methodology(**kwargs) -> MethodologyVersion:
    version_key = kwargs["version_key"]
    row = MethodologyVersion.query.filter_by(version_key=version_key).first()
    if row is None:
        row = MethodologyVersion(version_key=version_key, label=kwargs["label"])
        db.session.add(row)
    for k, v in kwargs.items():
        setattr(row, k, v)
    row.is_active = True
    row.is_published = True
    return row


def _upsert_company_config(company_key: str, *, origin_country: str) -> None:
    cats = list(COMPANY_ENABLED_CATEGORIES.get(company_key, ()))
    row = CompanyMethodologyConfig.query.filter_by(company_key=company_key).first()
    if row is None:
        row = CompanyMethodologyConfig(company_key=company_key)
        db.session.add(row)
    row.enabled_categories_json = json.dumps(cats)
    row.is_manufacturer = company_key in {"Nordic EPOD", "DC Piping", "GT Nordics"}
    row.origin_country = origin_country


def _upsert_transport_routes(company_key: str) -> None:
    routes = TRANSPORT_ROUTES.get(company_key, {})
    for dest in DESTINATIONS:
        land, sea = routes.get(dest, (500, 200))
        row = TransportRouteAssumption.query.filter_by(company_key=company_key, destination_region=dest).first()
        if row is None:
            row = TransportRouteAssumption(company_key=company_key, destination_region=dest)
            db.session.add(row)
        row.land_distance_km = float(land)
        row.sea_distance_km = float(sea)
        row.road_ef_t_per_tonne_km = ROAD_EF_T_PER_TONNE_KM
        row.sea_ef_t_per_tonne_km = SEA_EF_T_PER_TONNE_KM
        row.is_active = True


def _replace_eol_profiles(scenario: EolScenario, profiles: list[dict]) -> None:
    EolComponentProfile.query.filter_by(eol_scenario_id=scenario.id).delete()
    EolMaterialComposition.query.filter_by(eol_scenario_id=scenario.id).delete()
    scenario.methodology_type = "disposal_ratios"
    for idx, p in enumerate(profiles):
        db.session.add(
            EolComponentProfile(
                eol_scenario_id=scenario.id,
                component_key=p["component_key"],
                component_label=p["component_label"],
                weight_fraction=float(p["weight_fraction"]),
                disposal_stream=p["disposal_stream"],
                ratio_pct=float(p["ratio_pct"]),
                sort_order=idx,
            )
        )


def _replace_material_composition(scenario: EolScenario, materials: list[dict]) -> None:
    EolComponentProfile.query.filter_by(eol_scenario_id=scenario.id).delete()
    EolMaterialComposition.query.filter_by(eol_scenario_id=scenario.id).delete()
    scenario.methodology_type = "material_composition"
    for idx, m in enumerate(materials):
        db.session.add(
            EolMaterialComposition(
                eol_scenario_id=scenario.id,
                material_key=m["material_key"],
                material_label=m["material_label"],
                composition_pct=float(m["composition_pct"]),
                disposal_stream=m["disposal_stream"],
                sort_order=idx,
            )
        )


def _upsert_eol_scenario(
    methodology: MethodologyVersion,
    *,
    company_key: str,
    scenario_key: str,
    label: str,
    product_type: str,
    is_default: bool,
    profiles: list[dict] | None = None,
    materials: list[dict] | None = None,
) -> EolScenario:
    row = EolScenario.query.filter_by(methodology_version_id=methodology.id, scenario_key=scenario_key).first()
    if row is None:
        row = EolScenario(
            methodology_version_id=methodology.id,
            scenario_key=scenario_key,
            label=label,
            product_type=product_type,
        )
        db.session.add(row)
        db.session.flush()
    row.label = label
    row.product_type = product_type
    row.company_key = company_key
    row.is_default = is_default
    if materials:
        _replace_material_composition(row, materials)
    elif profiles:
        _replace_eol_profiles(row, profiles)
    return row


def _upsert_cat11(company_key: str, *, lifetime_kwh: float, profile_key: str = "default", label: str | None = None) -> None:
    row = Category11Methodology.query.filter_by(company_key=company_key, profile_key=profile_key).first()
    if row is None:
        row = Category11Methodology(company_key=company_key, profile_key=profile_key, label=label or f"{company_key} Cat 11")
        db.session.add(row)
    row.lifetime_kwh = lifetime_kwh
    row.country_ef_json = json.dumps(COUNTRY_ELECTRICITY_EF_KG_PER_KWH)
    row.default_country = "Norway" if company_key != "DC Piping" else "Portugal"
    row.is_active = True


def _upsert_gt_monthly() -> None:
    row = GtNordicsMonthlyScenario.query.filter_by(scenario_key="gt-nordics-monthly-default").first()
    if row is None:
        row = GtNordicsMonthlyScenario(scenario_key="gt-nordics-monthly-default", label="GT Nordics Monthly Default")
        db.session.add(row)
    row.product_type = "EPOD"
    row.default_quantity = 12.0
    row.quantity_unit = "pieces"
    row.average_product_weight_kg = 920.0
    row.product_unit = "kg"
    row.default_destination = "Norway"
    row.assumptions_json = json.dumps(
        {
            "workbook_source": "GT Nordics fixed monthly scenario",
            "cat9_default_destination": "Norway",
            "cat11_lifetime_kwh": 1250000,
            "cat11_country": "Norway",
            "notes": "Embedded monthly product quantity/weight/destination assumptions",
        }
    )
    row.is_active = True


def ensure_sustainability_seed_data() -> None:
    if (
        MethodologyVersion.query.filter_by(version_key=SEED_MARKER_KEY).first()
        and TransportRouteAssumption.query.filter_by(company_key="Nordic EPOD").count() >= 5
    ):
        return

    _upsert_methodology(
        version_key=SEED_MARKER_KEY,
        label="CTS Workbook Methodology v2",
        company_key=None,
        product_type=None,
        description="Master seed marker for workbook-aligned configuration.",
    )
    core = _upsert_methodology(
        version_key="cts-core-v1",
        label="CTS Core Facility Methodology v1",
        company_key=None,
        product_type=None,
        description="CTS office secondary-data defaults.",
    )
    db.session.flush()

    for company, origin in (("Nordic EPOD", "Norway"), ("DC Piping", "Portugal"), ("GT Nordics", "Norway")):
        _upsert_company_config(company, origin_country=origin)
        _upsert_transport_routes(company)

    epod_m = _upsert_methodology(
        version_key="nordic-epod-eol-v1",
        label="Nordic EPOD Methodology",
        company_key="Nordic EPOD",
        product_type="EPOD",
        description="Cat 9/11/12 workbook profiles for Nordic EPOD.",
    )
    db.session.flush()
    from frontend.sustainability.profiles.nordic_epod_workbook import (
        LIFETIME_END_PRODUCT_KWH,
        sync_workbook_snapshot_to_db,
    )

    _upsert_cat11("Nordic EPOD", lifetime_kwh=LIFETIME_END_PRODUCT_KWH)
    sync_workbook_snapshot_to_db()

    pipe_m = _upsert_methodology(
        version_key="dc-piping-eol-v1",
        label="DC Piping Methodology",
        company_key="DC Piping",
        product_type="Piping",
        description="Material composition + disposal for DC Piping Cat 12.",
    )
    db.session.flush()
    _upsert_eol_scenario(
        pipe_m,
        company_key="DC Piping",
        scenario_key="piping-composition",
        label="DC Piping Material EOL",
        product_type="Piping",
        is_default=True,
        materials=[
            {"material_key": "carbon_steel", "material_label": "Carbon Steel", "composition_pct": 95.0,
             "disposal_stream": "recycling"},
            {"material_key": "other_material", "material_label": "Other material", "composition_pct": 5.0,
             "disposal_stream": "combustion"},
        ],
    )

    gt_m = _upsert_methodology(
        version_key="gt-nordics-v1",
        label="GT Nordics Methodology",
        company_key="GT Nordics",
        product_type="EPOD",
        description="GT Nordics monthly scenario + Cat 9/11/12.",
    )
    db.session.flush()
    _upsert_cat11("GT Nordics", lifetime_kwh=1_250_000, profile_key="monthly-default")
    _upsert_gt_monthly()
    _upsert_eol_scenario(
        gt_m,
        company_key="GT Nordics",
        scenario_key="gt-epod-eol",
        label="GT Nordics EOL (EPOD-class)",
        product_type="EPOD",
        is_default=True,
        profiles=[
            {"component_key": "main_product", "component_label": "Main product", "weight_fraction": 1.0,
             "disposal_stream": "recycling", "ratio_pct": 75.0},
            {"component_key": "main_product", "component_label": "Main product", "weight_fraction": 1.0,
             "disposal_stream": "energy_recovery", "ratio_pct": 10.0},
            {"component_key": "main_product", "component_label": "Main product", "weight_fraction": 1.0,
             "disposal_stream": "landfill", "ratio_pct": 15.0},
        ],
    )

    defaults = [
        ("electricity_kwh_per_m2", "Electricity intensity", "electricity", None, 12.5, "kWh/m²/month", 0.00052410, "Scope 2 Electricity"),
        ("heating_kwh_per_m2", "District heating intensity", "heating", None, 8.0, "kWh/m²/month", 0.00015, "Scope 2 District Heating"),
        ("water_m3_per_m2", "Water intensity", "water", None, 0.05, "m³/m²/month", None, "Water"),
        ("waste_kg_per_m2", "Waste generation intensity", "waste", None, 2.5, "kg/m²/month", 0.00052, "Scope 3 Category 5 Waste"),
    ]
    for key, label, group, country, val, unit, ef, scope in defaults:
        row = AverageFactor.query.filter_by(factor_key=key, country=country).first()
        if row is None:
            row = AverageFactor(factor_key=key, country=country)
            db.session.add(row)
        row.label = label
        row.metric_group = group
        row.value = val
        row.unit = unit
        row.emission_factor = ef
        row.scope_category = scope
        row.methodology_version_id = core.id
        row.is_active = True

    for stream, ef in (
        ("eol_ef_recycling", EF_RECYCLING_KG_PER_KG),
        ("eol_ef_landfill", EF_LANDFILL_KG_PER_KG),
        ("eol_ef_energy_recovery", EF_ENERGY_RECOVERY_KG_PER_KG),
        ("eol_ef_combustion", EF_COMBUSTION_KG_PER_KG),
    ):
        row = AverageFactor.query.filter_by(factor_key=stream, country=None).first()
        if row is None:
            row = AverageFactor(factor_key=stream, country=None)
            db.session.add(row)
        row.label = stream.replace("_", " ").title()
        row.metric_group = "eol_emission_factor"
        row.value = ef
        row.unit = "kgCO2e/kg"
        row.emission_factor = ef
        row.scope_category = "Scope 3 Cat 12 End of Life"
        row.is_active = True

    for site_key, label, country, employees, area in (
        ("cts-helsinki", "CTS Helsinki Hub", "Finland", 120, 2400.0),
        ("cts-stockholm", "CTS Stockholm Hub", "Sweden", 85, 1800.0),
    ):
        row = SharedOfficeProfile.query.filter_by(office_site_key=site_key).first()
        if row is None:
            row = SharedOfficeProfile(office_site_key=site_key, office_label=label)
            db.session.add(row)
        row.country = country
        row.total_employee_count = employees
        row.total_floor_area_m2 = area
        row.is_active = True

    db.session.commit()
