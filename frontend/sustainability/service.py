"""Persistence and API payloads for the CTS sustainability methodology engine."""

from __future__ import annotations

import json
from typing import Any

from frontend.extensions import db
from frontend.sustainability.engines import run_full_calculation, serialize_json
from frontend.sustainability.models import (
    AverageFactor,
    BusinessQuestionnaireAnswer,
    Category11Methodology,
    CompanyMethodologyConfig,
    EolComponentProfile,
    EolScenario,
    GtNordicsMonthlyScenario,
    MethodologyVersion,
    SharedOfficeProfile,
    SustainabilityCalculationRun,
    SustainabilityProductEntry,
    TransportRouteAssumption,
)
from frontend.sustainability.seed import ensure_sustainability_seed_data
from frontend.sustainability.workflow_registry import (
    BUSINESS_FUNCTIONS,
    PRODUCT_TYPE_OPTIONS,
    normalize_business_function,
    normalize_company_key,
    resolve_workflow_context,
)


def bootstrap() -> None:
    from frontend.sustainability.profiles.nordic_epod_workbook import sync_workbook_snapshot_to_db
    from frontend.sustainability.schema import ensure_sustainability_schema

    ensure_sustainability_schema()
    ensure_sustainability_seed_data()
    try:
        sync_workbook_snapshot_to_db()
    except Exception:
        pass


def workflow_payload_for_user(user: object, *, company_name: str, product_type: str | None = None) -> dict[str, Any]:
    bf = normalize_business_function(getattr(user, "business_type", None))
    return resolve_workflow_context(
        business_function=bf,
        company_name=company_name,
        product_type=product_type,
    )


def list_transport_routes(company_key: str) -> list[dict[str, Any]]:
    company = normalize_company_key(company_key)
    return [
        {
            "destination_region": r.destination_region,
            "land_distance_km": r.land_distance_km,
            "sea_distance_km": r.sea_distance_km,
            "road_ef_t_per_tonne_km": r.road_ef_t_per_tonne_km,
            "sea_ef_t_per_tonne_km": r.sea_ef_t_per_tonne_km,
        }
        for r in TransportRouteAssumption.query.filter_by(company_key=company, is_active=True)
        .order_by(TransportRouteAssumption.destination_region.asc())
        .all()
    ]


def list_cat11_profiles(company_key: str) -> list[dict[str, Any]]:
    company = normalize_company_key(company_key)
    return [
        {
            "profile_key": r.profile_key,
            "label": r.label,
            "lifetime_kwh": r.lifetime_kwh,
            "default_country": r.default_country,
        }
        for r in Category11Methodology.query.filter_by(company_key=company, is_active=True).all()
    ]


def gt_nordics_scenario_payload() -> dict[str, Any] | None:
    row = GtNordicsMonthlyScenario.query.filter_by(is_active=True).first()
    if not row:
        return None
    assumptions = {}
    try:
        assumptions = json.loads(row.assumptions_json or "{}")
    except Exception:
        pass
    return {
        "label": row.label,
        "product_type": row.product_type,
        "default_quantity": row.default_quantity,
        "quantity_unit": row.quantity_unit,
        "average_product_weight_kg": row.average_product_weight_kg,
        "default_destination": row.default_destination,
        "assumptions": assumptions,
    }


def methodology_version_payload(row: MethodologyVersion) -> dict[str, Any]:
    scenarios = []
    for s in row.eol_scenarios.order_by(EolScenario.id.asc()).all():
        scenarios.append(
            {
                "id": s.id,
                "scenario_key": s.scenario_key,
                "label": s.label,
                "product_type": s.product_type,
                "company_key": s.company_key,
                "methodology_type": s.methodology_type,
                "is_default": s.is_default,
                "components": [
                    {
                        "component_key": p.component_key,
                        "component_label": p.component_label,
                        "weight_fraction": p.weight_fraction,
                        "disposal_stream": p.disposal_stream,
                        "ratio_pct": p.ratio_pct,
                    }
                    for p in (s.component_profiles or [])
                ],
                "materials": [
                    {
                        "material_key": m.material_key,
                        "material_label": m.material_label,
                        "composition_pct": m.composition_pct,
                        "disposal_stream": m.disposal_stream,
                    }
                    for m in (s.material_compositions or [])
                ],
            }
        )
    return {
        "id": row.id,
        "version_key": row.version_key,
        "label": row.label,
        "company_key": row.company_key,
        "product_type": row.product_type,
        "eol_scenarios": scenarios,
    }


def list_methodologies(company_key: str | None = None) -> list[dict[str, Any]]:
    q = MethodologyVersion.query.filter_by(is_active=True, is_published=True)
    if company_key:
        q = q.filter(
            (MethodologyVersion.company_key == normalize_company_key(company_key))
            | (MethodologyVersion.company_key.is_(None))
        )
    return [methodology_version_payload(r) for r in q.order_by(MethodologyVersion.label.asc()).all()]


def list_average_factors() -> list[dict[str, Any]]:
    return [
        {
            "id": r.id,
            "factor_key": r.factor_key,
            "label": r.label,
            "metric_group": r.metric_group,
            "country": r.country,
            "value": r.value,
            "unit": r.unit,
            "emission_factor": r.emission_factor,
            "scope_category": r.scope_category,
        }
        for r in AverageFactor.query.filter_by(is_active=True).order_by(AverageFactor.metric_group.asc()).all()
    ]


def list_shared_offices() -> list[dict[str, Any]]:
    return [
        {
            "id": r.id,
            "office_site_key": r.office_site_key,
            "office_label": r.office_label,
            "country": r.country,
            "total_employee_count": r.total_employee_count,
            "total_floor_area_m2": r.total_floor_area_m2,
        }
        for r in SharedOfficeProfile.query.filter_by(is_active=True).order_by(SharedOfficeProfile.office_label.asc()).all()
    ]


def company_config_payload(company_key: str) -> dict[str, Any] | None:
    row = CompanyMethodologyConfig.query.filter_by(company_key=normalize_company_key(company_key)).first()
    if not row:
        return None
    try:
        cats = json.loads(row.enabled_categories_json or "[]")
    except Exception:
        cats = []
    return {
        "company_key": row.company_key,
        "enabled_categories": cats,
        "is_manufacturer": row.is_manufacturer,
        "origin_country": row.origin_country,
    }


def save_questionnaire(user_id: int, payload: dict[str, Any]) -> BusinessQuestionnaireAnswer:
    period = str(payload.get("reporting_period_key") or "").strip()
    company = str(payload.get("company_name") or "").strip()
    row = BusinessQuestionnaireAnswer.query.filter_by(
        user_id=user_id,
        company_name=company,
        reporting_period_key=period,
    ).first()
    if row is None:
        row = BusinessQuestionnaireAnswer(
            user_id=user_id,
            company_name=company,
            reporting_period_key=period,
        )
        db.session.add(row)

    row.business_function = normalize_business_function(payload.get("business_function")) or None
    row.office_site_key = str(payload.get("office_site_key") or "").strip() or None
    row.country = str(payload.get("country") or "").strip() or None
    row.employee_count = payload.get("employee_count")
    row.office_size_m2 = payload.get("office_size_m2")
    row.is_shared_office = bool(payload.get("is_shared_office"))
    row.has_utility_bills = bool(payload.get("has_utility_bills"))
    row.electricity_type = str(payload.get("electricity_type") or "").strip() or None
    row.heating_type = str(payload.get("heating_type") or "").strip() or None
    row.product_type = str(payload.get("product_type") or "").strip() or None
    row.methodology_version_id = payload.get("methodology_version_id")
    row.eol_scenario_id = payload.get("eol_scenario_id")
    row.answers_json = json.dumps(payload.get("extra") or {}, ensure_ascii=True)
    db.session.commit()
    return row


def list_product_entries(company_name: str, period_key: str) -> list[dict[str, Any]]:
    rows = (
        SustainabilityProductEntry.query.filter_by(company_name=company_name, reporting_period_key=period_key)
        .order_by(SustainabilityProductEntry.row_index.asc())
        .all()
    )
    return [
        {
            "id": r.id,
            "row_index": r.row_index,
            "product_type": r.product_type,
            "quantity": r.quantity,
            "quantity_unit": r.quantity_unit,
            "end_use_location": r.end_use_location,
            "product_weight": r.product_weight,
            "product_unit": r.product_unit,
            "proof_attachment_name": r.proof_attachment_name,
            "has_proof": bool(r.proof_attachment_path),
        }
        for r in rows
    ]


def save_product_entries(
    user_id: int,
    *,
    company_name: str,
    period_key: str,
    period_label: str,
    rows: list[dict[str, Any]],
) -> list[SustainabilityProductEntry]:
    SustainabilityProductEntry.query.filter_by(
        company_name=company_name,
        reporting_period_key=period_key,
    ).delete()
    saved: list[SustainabilityProductEntry] = []
    for idx, raw in enumerate(rows, start=1):
        entry = SustainabilityProductEntry(
            user_id=user_id,
            company_name=company_name,
            reporting_period_key=period_key,
            reporting_period_label=period_label,
            row_index=idx,
            product_type=str(raw.get("product_type") or "").strip(),
            quantity=float(raw.get("quantity") or 0),
            quantity_unit=str(raw.get("quantity_unit") or "").strip(),
            end_use_location=str(raw.get("end_use_location") or "").strip(),
            product_weight=float(raw.get("product_weight") or 0),
            product_unit=str(raw.get("product_unit") or "").strip(),
            proof_attachment_path=raw.get("proof_attachment_path"),
            proof_attachment_name=raw.get("proof_attachment_name"),
        )
        db.session.add(entry)
        saved.append(entry)
    db.session.commit()
    return saved


def product_rows_for_calculation(company_name: str, period_key: str) -> list[dict[str, Any]]:
    return [
        {
            "product_type": r.product_type,
            "quantity": r.quantity,
            "quantity_unit": r.quantity_unit,
            "end_use_location": r.end_use_location,
            "product_weight": r.product_weight,
            "product_unit": r.product_unit,
        }
        for r in SustainabilityProductEntry.query.filter_by(
            company_name=company_name,
            reporting_period_key=period_key,
        )
        .order_by(SustainabilityProductEntry.row_index.asc())
        .all()
    ]


def run_calculation_for_questionnaire(
    user_id: int,
    questionnaire: BusinessQuestionnaireAnswer,
    *,
    actual_utilities: dict[str, float | None] | None = None,
    product_weight_kg: float | None = None,
    product_rows: list[dict[str, Any]] | None = None,
) -> SustainabilityCalculationRun:
    office_profile = None
    if questionnaire.office_site_key:
        office_profile = SharedOfficeProfile.query.filter_by(
            office_site_key=questionnaire.office_site_key,
            is_active=True,
        ).first()

    if product_rows is None:
        product_rows = product_rows_for_calculation(
            questionnaire.company_name,
            questionnaire.reporting_period_key,
        )

    bf = questionnaire.business_function or ""
    q_dict = {
        "company_name": questionnaire.company_name,
        "business_function": bf,
        "country": questionnaire.country,
        "employee_count": questionnaire.employee_count,
        "office_size_m2": questionnaire.office_size_m2,
        "is_shared_office": questionnaire.is_shared_office,
        "has_utility_bills": questionnaire.has_utility_bills,
        "product_type": questionnaire.product_type,
        "reporting_period_key": questionnaire.reporting_period_key,
        "product_weight_kg": product_weight_kg,
        "end_use_location": (product_rows[0].get("end_use_location") if product_rows else None),
    }

    result = run_full_calculation(
        q_dict,
        office_profile=office_profile,
        actual_utilities=actual_utilities,
        product_weight_kg=product_weight_kg,
        product_rows=product_rows or None,
    )

    run = SustainabilityCalculationRun(
        questionnaire_id=questionnaire.id,
        user_id=user_id,
        company_name=questionnaire.company_name,
        reporting_period_key=questionnaire.reporting_period_key,
        status="completed",
        input_snapshot_json=serialize_json({**q_dict, "product_rows": product_rows or []}),
        result_json=serialize_json(result),
        total_co2e_t=float(result.get("total_co2e_t") or 0.0),
    )
    db.session.add(run)
    db.session.commit()
    return run


def calculation_run_payload(run: SustainabilityCalculationRun) -> dict[str, Any]:
    try:
        result = json.loads(run.result_json or "{}")
    except Exception:
        result = {}
    return {
        "id": run.id,
        "company_name": run.company_name,
        "reporting_period_key": run.reporting_period_key,
        "total_co2e_t": run.total_co2e_t,
        "created_at": run.created_at.isoformat() if run.created_at else "",
        "result": result,
    }


def admin_update_eol_profile(profile_id: int, payload: dict[str, Any]) -> EolComponentProfile:
    row = EolComponentProfile.query.get_or_404(profile_id)
    if "ratio_pct" in payload:
        row.ratio_pct = float(payload["ratio_pct"])
    if "weight_fraction" in payload:
        row.weight_fraction = float(payload["weight_fraction"])
    db.session.commit()
    return row


def admin_update_average_factor(factor_id: int, payload: dict[str, Any]) -> AverageFactor:
    row = AverageFactor.query.get_or_404(factor_id)
    if "value" in payload:
        row.value = float(payload["value"])
    if "emission_factor" in payload:
        row.emission_factor = float(payload["emission_factor"]) if payload["emission_factor"] is not None else None
    db.session.commit()
    return row
