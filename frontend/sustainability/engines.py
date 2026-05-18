"""CTS workbook calculation engines (company-specific, not generic ESG)."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from frontend.sustainability.models import (
    AverageFactor,
    Category11Methodology,
    EolComponentProfile,
    EolMaterialComposition,
    EolScenario,
    GtNordicsMonthlyScenario,
    SharedOfficeProfile,
    TransportRouteAssumption,
)
from frontend.sustainability.seed import (
    COUNTRY_ELECTRICITY_EF_KG_PER_KWH,
    EF_COMBUSTION_KG_PER_KG,
    EF_ENERGY_RECOVERY_KG_PER_KG,
    EF_LANDFILL_KG_PER_KG,
    EF_RECYCLING_KG_PER_KG,
)
from frontend.sustainability.profiles.nordic_epod_workbook import (
    COMPANY_KEY as NORDIC_EPOD_COMPANY,
    LIFETIME_END_PRODUCT_KWH,
    calculate_category12_workbook,
    is_nordic_epod_workbook,
    workbook_profile_document,
)
from frontend.sustainability.profiles.piping_workbook import is_dc_piping_workbook
from frontend.sustainability.workflow_registry import (
    normalize_company_key,
    resolve_workflow_context,
)


def _safe_float(value: object, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_destination(end_use_location: str) -> str:
    loc = " ".join(str(end_use_location or "").strip().split())
    if not loc:
        return "Europe"
    key = loc.casefold()
    for dest in ("Norway", "Finland", "Sweden", "Scandinavia", "Europe"):
        if dest.casefold() in key or key == dest.casefold():
            return dest
    if any(x in key for x in ("scandi", "nordic")):
        return "Scandinavia"
    return "Europe"


def occupancy_ratio(company_employees: int, total_office_employees: int) -> float:
    if total_office_employees <= 0:
        return 0.0
    return max(0.0, min(1.0, float(company_employees) / float(total_office_employees)))


def allocate_shared_office(
    total_emissions_t: float,
    company_employee_count: int,
    office_profile: SharedOfficeProfile | None,
) -> dict[str, Any]:
    total_employees = int(office_profile.total_employee_count) if office_profile else 0
    ratio = occupancy_ratio(company_employee_count, total_employees)
    allocated = total_emissions_t * ratio
    return {
        "total_office_employees": total_employees,
        "company_employee_count": company_employee_count,
        "occupancy_ratio": ratio,
        "total_emissions_t": total_emissions_t,
        "allocated_emissions_t": allocated,
        "office_site_key": getattr(office_profile, "office_site_key", None),
        "office_label": getattr(office_profile, "office_label", None),
    }


def get_transport_route(company_key: str, destination_region: str) -> TransportRouteAssumption | None:
    company = normalize_company_key(company_key)
    dest = _normalize_destination(destination_region)
    return (
        TransportRouteAssumption.query.filter_by(
            company_key=company,
            destination_region=dest,
            is_active=True,
        ).first()
    )


def calculate_category9_transport(
    *,
    company_key: str,
    weight_kg: float,
    end_use_location: str,
    quantity: float = 1.0,
) -> dict[str, Any]:
    """
    transport_emissions =
      (weight_tonnes × land_distance × road_ef) + (weight_tonnes × sea_distance × sea_ef)
    """
    company = normalize_company_key(company_key)
    destination = _normalize_destination(end_use_location)
    route = get_transport_route(company, destination)
    if route is None:
        return {"error": f"No transport route for {company} → {destination}", "total_emissions_t": 0.0}

    total_weight_kg = float(weight_kg) * float(quantity)
    weight_tonnes = total_weight_kg / 1000.0
    land = float(route.land_distance_km)
    sea = float(route.sea_distance_km)
    road_ef = float(route.road_ef_t_per_tonne_km)
    sea_ef = float(route.sea_ef_t_per_tonne_km)

    road_t = weight_tonnes * land * road_ef
    sea_t = weight_tonnes * sea * sea_ef
    total_t = road_t + sea_t

    return {
        "company_key": company,
        "destination_region": destination,
        "weight_kg": total_weight_kg,
        "weight_tonnes": weight_tonnes,
        "land_distance_km": land,
        "sea_distance_km": sea,
        "road_ef_t_per_tonne_km": road_ef,
        "sea_ef_t_per_tonne_km": sea_ef,
        "road_emissions_t": road_t,
        "sea_emissions_t": sea_t,
        "total_emissions_t": total_t,
        "formula": "(weight_tonnes × land_km × road_ef) + (weight_tonnes × sea_km × sea_ef)",
    }


def _country_ef_kg_per_kwh(country: str | None, cat11: Category11Methodology | None) -> float:
    if cat11 and cat11.country_ef_json:
        try:
            mapping = json.loads(cat11.country_ef_json)
            if isinstance(mapping, dict):
                c = str(country or cat11.default_country or "default").strip()
                if c in mapping:
                    return float(mapping[c])
                if c.casefold() in {k.casefold(): v for k, v in mapping.items()}:
                    for k, v in mapping.items():
                        if k.casefold() == c.casefold():
                            return float(v)
                return float(mapping.get("default", COUNTRY_ELECTRICITY_EF_KG_PER_KWH["default"]))
        except Exception:
            pass
    if cat11 and cat11.electricity_ef_kg_per_kwh:
        return float(cat11.electricity_ef_kg_per_kwh)
    c = str(country or "default").strip()
    return float(COUNTRY_ELECTRICITY_EF_KG_PER_KWH.get(c, COUNTRY_ELECTRICITY_EF_KG_PER_KWH["default"]))


def calculate_category11_use_of_sold(
    *,
    company_key: str,
    country: str | None = None,
    lifetime_kwh: float | None = None,
) -> dict[str, Any]:
    company = normalize_company_key(company_key)
    profile = (
        Category11Methodology.query.filter_by(company_key=company, is_active=True)
        .order_by(Category11Methodology.id.asc())
        .first()
    )
    if profile is None:
        return {"error": f"No Category 11 profile for {company}", "total_emissions_t": 0.0}

    kwh = float(lifetime_kwh if lifetime_kwh is not None else profile.lifetime_kwh)
    ef = _country_ef_kg_per_kwh(country or profile.default_country, profile)
    emissions_kg = kwh * ef
    emissions_t = emissions_kg / 1000.0

    return {
        "company_key": company,
        "lifetime_kwh": kwh,
        "country": country or profile.default_country,
        "electricity_ef_kg_per_kwh": ef,
        "emissions_kg": emissions_kg,
        "total_emissions_t": emissions_t,
        "formula": "lifetime_kwh × country_electricity_ef",
        "profile_label": profile.label,
    }


def _eol_ef_map() -> dict[str, float]:
    factors = {str(f.factor_key): f for f in AverageFactor.query.filter_by(is_active=True).all()}
    return {
        "recycling": float(factors.get("eol_ef_recycling", type("X", (), {"emission_factor": EF_RECYCLING_KG_PER_KG})()).emission_factor or EF_RECYCLING_KG_PER_KG),
        "landfill": float(factors.get("eol_ef_landfill", type("X", (), {"emission_factor": EF_LANDFILL_KG_PER_KG})()).emission_factor or EF_LANDFILL_KG_PER_KG),
        "energy_recovery": float(factors.get("eol_ef_energy_recovery", type("X", (), {"emission_factor": EF_ENERGY_RECOVERY_KG_PER_KG})()).emission_factor or EF_ENERGY_RECOVERY_KG_PER_KG),
        "combustion": float(factors.get("eol_ef_combustion", type("X", (), {"emission_factor": EF_COMBUSTION_KG_PER_KG})()).emission_factor or EF_COMBUSTION_KG_PER_KG),
    }


def calculate_eol_disposal(*, product_weight_kg: float, scenario: EolScenario) -> dict[str, Any]:
    if str(scenario.methodology_type or "") == "material_composition":
        return _calculate_eol_material_composition(product_weight_kg=product_weight_kg, scenario=scenario)
    return _calculate_eol_disposal_ratios(product_weight_kg=product_weight_kg, scenario=scenario)


def _calculate_eol_material_composition(*, product_weight_kg: float, scenario: EolScenario) -> dict[str, Any]:
    materials: list[EolMaterialComposition] = list(scenario.material_compositions or [])
    if not materials:
        return {"error": "No material composition configured.", "streams": [], "total_emissions_t": 0.0}

    ef_map = _eol_ef_map()
    breakdown: list[dict[str, Any]] = []
    stream_weights: dict[str, float] = defaultdict(float)
    total_kg_co2e = 0.0

    for mat in materials:
        mat_kg = product_weight_kg * (float(mat.composition_pct) / 100.0)
        stream = str(mat.disposal_stream)
        if stream == "combustion":
            stream = "combustion"
        stream_weights[stream] += mat_kg
        ef = ef_map.get(stream, ef_map.get("energy_recovery", 0.0))
        emissions_kg = mat_kg * ef
        total_kg_co2e += emissions_kg
        breakdown.append(
            {
                "material_key": mat.material_key,
                "material_label": mat.material_label,
                "composition_pct": float(mat.composition_pct),
                "disposal_stream": stream,
                "waste_kg": mat_kg,
                "emissions_kg": emissions_kg,
            }
        )

    streams_out = [
        {
            "disposal_stream": s,
            "waste_kg": w,
            "emission_factor_kg_per_kg": ef_map.get(s, 0.0),
            "emissions_kg": w * ef_map.get(s, 0.0),
            "emissions_t": (w * ef_map.get(s, 0.0)) / 1000.0,
        }
        for s, w in stream_weights.items()
    ]

    return {
        "methodology_type": "material_composition",
        "scenario_id": scenario.id,
        "scenario_key": scenario.scenario_key,
        "scenario_label": scenario.label,
        "company_key": scenario.company_key,
        "product_type": scenario.product_type,
        "product_weight_kg": product_weight_kg,
        "breakdown": breakdown,
        "streams": streams_out,
        "total_emissions_kg": total_kg_co2e,
        "total_emissions_t": total_kg_co2e / 1000.0,
    }


def _calculate_eol_disposal_ratios(*, product_weight_kg: float, scenario: EolScenario) -> dict[str, Any]:
    profiles: list[EolComponentProfile] = list(scenario.component_profiles or [])
    if not profiles:
        return {"error": "No disposal profiles configured.", "streams": [], "total_emissions_t": 0.0}

    ef_map = _eol_ef_map()
    stream_weights: dict[str, float] = defaultdict(float)
    breakdown: list[dict[str, Any]] = []

    for profile in profiles:
        comp_weight = product_weight_kg * float(profile.weight_fraction)
        stream = str(profile.disposal_stream)
        w = comp_weight * (float(profile.ratio_pct) / 100.0)
        stream_weights[stream] += w
        breakdown.append(
            {
                "component_key": profile.component_key,
                "component_label": profile.component_label,
                "weight_fraction": float(profile.weight_fraction),
                "disposal_stream": stream,
                "ratio_pct": float(profile.ratio_pct),
                "waste_kg": w,
            }
        )

    total_kg_co2e = 0.0
    streams_out: list[dict[str, Any]] = []
    for stream, waste_kg in stream_weights.items():
        ef = ef_map.get(stream, 0.0)
        emissions_kg = waste_kg * ef
        total_kg_co2e += emissions_kg
        streams_out.append(
            {
                "disposal_stream": stream,
                "waste_kg": waste_kg,
                "emission_factor_kg_per_kg": ef,
                "emissions_kg": emissions_kg,
                "emissions_t": emissions_kg / 1000.0,
            }
        )

    return {
        "methodology_type": "disposal_ratios",
        "scenario_id": scenario.id,
        "scenario_key": scenario.scenario_key,
        "scenario_label": scenario.label,
        "product_type": scenario.product_type,
        "product_weight_kg": product_weight_kg,
        "breakdown": breakdown,
        "streams": streams_out,
        "total_emissions_kg": total_kg_co2e,
        "total_emissions_t": total_kg_co2e / 1000.0,
    }


def load_eol_scenario(
    *,
    company_key: str,
    product_type: str | None,
    scenario_id: int | None = None,
) -> EolScenario | None:
    if scenario_id:
        return EolScenario.query.get(scenario_id)
    company = normalize_company_key(company_key)
    pt = str(product_type or "").strip()
    if pt.casefold() == "pipe":
        pt = "Piping"
    q = EolScenario.query.filter_by(is_default=True)
    if company:
        q = q.filter_by(company_key=company)
    if pt:
        q = q.filter_by(product_type=pt)
    return q.order_by(EolScenario.id.asc()).first()


def get_active_average_factors(metric_group: str | None = None) -> list[AverageFactor]:
    q = AverageFactor.query.filter_by(is_active=True)
    if metric_group:
        q = q.filter_by(metric_group=metric_group)
    return q.order_by(AverageFactor.factor_key.asc()).all()


def calculate_average_estimation(
    *,
    office_size_m2: float,
    country: str | None,
    use_fallback: bool,
    actual: dict[str, float | None] | None = None,
) -> dict[str, Any]:
    actual = actual or {}
    factors = {str(f.factor_key): f for f in get_active_average_factors()}
    lines: list[dict[str, Any]] = []
    total_t = 0.0

    for metric_group, factor_key, actual_key, activity_unit in (
        ("electricity", "electricity_kwh_per_m2", "electricity_kwh", "kWh"),
        ("heating", "heating_kwh_per_m2", "heating_kwh", "kWh"),
        ("water", "water_m3_per_m2", "water_m3", "m³"),
        ("waste", "waste_kg_per_m2", "waste_kg", "kg"),
    ):
        factor = factors.get(factor_key)
        if factor is None:
            continue
        measured = _safe_float(actual.get(actual_key))
        if measured is not None and measured > 0 and not use_fallback:
            activity = measured
            source = "measured"
        else:
            activity = office_size_m2 * float(factor.value)
            source = "average_factor"

        ef = float(factor.emission_factor or 0.0)
        emissions_kg = activity * ef if ef > 0 else 0.0
        emissions_t = emissions_kg / 1000.0
        total_t += emissions_t
        lines.append(
            {
                "metric_group": metric_group,
                "scope_category": factor.scope_category,
                "activity": activity,
                "activity_unit": activity_unit,
                "emission_factor": ef,
                "emissions_t": emissions_t,
                "data_source": source,
            }
        )

    return {"office_size_m2": office_size_m2, "use_fallback": use_fallback, "lines": lines, "total_emissions_t": total_t}


def apply_shared_allocation_to_facility(
    facility_result: dict[str, Any],
    *,
    company_employee_count: int,
    office_profile: SharedOfficeProfile | None,
) -> dict[str, Any]:
    allocated_lines: list[dict[str, Any]] = []
    total_allocated = 0.0
    for line in facility_result.get("lines", []):
        alloc = allocate_shared_office(float(line.get("emissions_t") or 0.0), company_employee_count, office_profile)
        line_copy = dict(line)
        line_copy["shared_office_allocation"] = alloc
        line_copy["emissions_t"] = alloc["allocated_emissions_t"]
        allocated_lines.append(line_copy)
        total_allocated += alloc["allocated_emissions_t"]
    facility_result["lines"] = allocated_lines
    facility_result["total_emissions_t"] = total_allocated
    facility_result["shared_office"] = allocate_shared_office(total_allocated, company_employee_count, office_profile)
    return facility_result


def run_manufacturer_product_calculation(
    *,
    company_key: str,
    product_rows: list[dict[str, Any]],
    workflow: dict[str, Any],
    country: str | None = None,
) -> dict[str, Any]:
    enabled = set(workflow.get("enabled_categories") or [])
    cat9_lines: list[dict[str, Any]] = []
    cat11_result: dict[str, Any] | None = None
    cat12_lines: list[dict[str, Any]] = []
    total_t = 0.0

    gt_scenario = None
    if workflow.get("capabilities", {}).get("gt_monthly_scenario"):
        gt_scenario = GtNordicsMonthlyScenario.query.filter_by(is_active=True).first()

    for row in product_rows:
        pt = str(row.get("product_type") or "").strip()
        qty = float(_safe_float(row.get("quantity"), 1.0) or 1.0)
        unit_weight = float(_safe_float(row.get("product_weight"), 0.0) or 0.0)
        total_weight_kg = unit_weight * qty
        dest = str(row.get("end_use_location") or (gt_scenario.default_destination if gt_scenario else "Norway"))

        if "9" in enabled:
            c9 = calculate_category9_transport(
                company_key=company_key,
                weight_kg=unit_weight,
                end_use_location=dest,
                quantity=qty,
            )
            cat9_lines.append({**c9, "product_type": pt, "row": row})
            total_t += float(c9.get("total_emissions_t") or 0.0)

        if "12" in enabled and total_weight_kg > 0:
            if is_nordic_epod_workbook(company_key, pt):
                c12 = calculate_category12_workbook(total_weight_kg)
                cat12_lines.append({**c12, "product_type": pt})
                total_t += float(c12.get("total_emissions_t") or 0.0)
            elif is_dc_piping_workbook(company_key, pt):
                scenario = load_eol_scenario(company_key=company_key, product_type=pt)
                if scenario:
                    c12 = calculate_eol_disposal(product_weight_kg=total_weight_kg, scenario=scenario)
                    cat12_lines.append({**c12, "product_type": pt})
                    total_t += float(c12.get("total_emissions_t") or 0.0)
            else:
                scenario = load_eol_scenario(company_key=company_key, product_type=pt)
                if scenario:
                    c12 = calculate_eol_disposal(product_weight_kg=total_weight_kg, scenario=scenario)
                    cat12_lines.append({**c12, "product_type": pt})
                    total_t += float(c12.get("total_emissions_t") or 0.0)

    if "11" in enabled:
        lifetime = None
        if normalize_company_key(company_key) == NORDIC_EPOD_COMPANY:
            lifetime = LIFETIME_END_PRODUCT_KWH
        elif gt_scenario and gt_scenario.assumptions_json:
            try:
                assumptions = json.loads(gt_scenario.assumptions_json)
                lifetime = _safe_float(assumptions.get("cat11_lifetime_kwh"))
            except Exception:
                lifetime = None
        cat11_result = calculate_category11_use_of_sold(
            company_key=company_key,
            country=country,
            lifetime_kwh=lifetime,
        )
        if normalize_company_key(company_key) == NORDIC_EPOD_COMPANY:
            cat11_result["workbook_profile"] = workbook_profile_document()["category_11"]
            cat11_result["workbook_source"] = "nordic_epod_workbook_lifecycle"
        total_t += float(cat11_result.get("total_emissions_t") or 0.0)

    result_workbook = workbook_profile_document() if normalize_company_key(company_key) == NORDIC_EPOD_COMPANY else None

    return {
        "category_9": {"lines": cat9_lines, "total_emissions_t": sum(float(x.get("total_emissions_t") or 0) for x in cat9_lines)},
        "category_11": cat11_result,
        "category_12": {"lines": cat12_lines, "total_emissions_t": sum(float(x.get("total_emissions_t") or 0) for x in cat12_lines)},
        "nordic_epod_workbook_profile": result_workbook,
        "total_co2e_t": total_t,
        "gt_monthly_scenario": (
            {
                "label": gt_scenario.label,
                "default_quantity": gt_scenario.default_quantity,
                "average_product_weight_kg": gt_scenario.average_product_weight_kg,
                "default_destination": gt_scenario.default_destination,
            }
            if gt_scenario
            else None
        ),
    }


def run_full_calculation(
    questionnaire: dict[str, Any],
    *,
    office_profile: SharedOfficeProfile | None = None,
    eol_scenario: EolScenario | None = None,
    actual_utilities: dict[str, float | None] | None = None,
    product_weight_kg: float | None = None,
    product_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    company = normalize_company_key(str(questionnaire.get("company_name") or ""))
    bf = str(questionnaire.get("business_function") or "").strip()
    workflow = resolve_workflow_context(
        business_function=bf,
        company_name=company,
        product_type=questionnaire.get("product_type"),
    )

    result: dict[str, Any] = {
        "workflow": workflow,
        "company_key": company,
        "facility": None,
        "manufacturer": None,
        "eol": None,
        "total_co2e_t": 0.0,
    }
    caps = workflow.get("capabilities") or {}
    total_t = 0.0

    if caps.get("facility_estimation"):
        office_m2 = float(_safe_float(questionnaire.get("office_size_m2"), 0.0) or 0.0)
        employees = int(_safe_int(questionnaire.get("employee_count"), 0) or 0)
        use_fallback = not bool(questionnaire.get("has_utility_bills"))
        facility = calculate_average_estimation(
            office_size_m2=office_m2,
            country=questionnaire.get("country"),
            use_fallback=use_fallback,
            actual=actual_utilities,
        )
        if caps.get("shared_office") and questionnaire.get("is_shared_office"):
            facility = apply_shared_allocation_to_facility(
                facility, company_employee_count=employees, office_profile=office_profile
            )
        result["facility"] = facility
        total_t += float(facility.get("total_emissions_t") or 0.0)

    if caps.get("products_page") and product_rows:
        mfg = run_manufacturer_product_calculation(
            company_key=company,
            product_rows=product_rows,
            workflow=workflow,
            country=questionnaire.get("country"),
        )
        result["manufacturer"] = mfg
        total_t += float(mfg.get("total_co2e_t") or 0.0)
    elif caps.get("category_12") or caps.get("category_9"):
        weight = _safe_float(product_weight_kg) or _safe_float(questionnaire.get("product_weight_kg"))
        if weight and weight > 0:
            if "9" in workflow.get("enabled_categories", []):
                c9 = calculate_category9_transport(
                    company_key=company,
                    weight_kg=weight,
                    end_use_location=str(questionnaire.get("end_use_location") or "Norway"),
                )
                result["category_9"] = c9
                total_t += float(c9.get("total_emissions_t") or 0.0)
            pt = questionnaire.get("product_type")
            if "12" in workflow.get("enabled_categories", []):
                if is_nordic_epod_workbook(company, pt):
                    eol = calculate_category12_workbook(weight)
                    result["eol"] = eol
                    total_t += float(eol.get("total_emissions_t") or 0.0)
                else:
                    scenario = eol_scenario or load_eol_scenario(company_key=company, product_type=pt)
                    if scenario:
                        eol = calculate_eol_disposal(product_weight_kg=weight, scenario=scenario)
                        result["eol"] = eol
                        total_t += float(eol.get("total_emissions_t") or 0.0)
            if "11" in workflow.get("enabled_categories", []):
                lifetime = LIFETIME_END_PRODUCT_KWH if is_nordic_epod_workbook(company, pt) else None
                c11 = calculate_category11_use_of_sold(
                    company_key=company,
                    country=questionnaire.get("country"),
                    lifetime_kwh=lifetime,
                )
                result["category_11"] = c11
                total_t += float(c11.get("total_emissions_t") or 0.0)

    result["total_co2e_t"] = total_t
    return result


def serialize_json(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=True, default=str)
