"""Flask routes — CTS workbook-driven sustainability methodology engine."""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from frontend.extensions import db
from frontend.sustainability import service
from frontend.sustainability.platform_helpers import (
    clean_company_name,
    current_reporting_period,
    is_owner_user,
    is_readonly_user,
    iso_countries,
    list_company_options,
)
from frontend.sustainability.models import (
    AverageFactor,
    BusinessQuestionnaireAnswer,
    EolComponentProfile,
    MethodologyVersion,
    SustainabilityCalculationRun,
)
from frontend.sustainability.workflow_registry import (
    BUSINESS_FUNCTIONS,
    PRODUCT_TYPE_OPTIONS,
    normalize_business_function,
    resolve_workflow_context,
    validate_calculation_request,
    validate_product_type,
)

bp = Blueprint("sustainability", __name__, url_prefix="/sustainability")

ELECTRICITY_TYPES = ("Grid", "Renewable PPA", "Mixed")
HEATING_TYPES = ("District heating", "Electric", "Gas", "None")
DESTINATION_OPTIONS = ("Norway", "Finland", "Sweden", "Scandinavia", "Europe")
QUANTITY_UNITS = ("pieces", "units", "sets", "kg", "tonnes")
PRODUCT_UNITS = ("kg", "tonnes")

PROOF_UPLOAD_DIR = Path(__file__).resolve().parents[1] / "instance" / "sustainability_proofs"


def _current_period_key() -> str:
    return current_reporting_period()[0]


def _period_label(period_key: str) -> str:
    try:
        y, m = period_key.split("-")
        return datetime(int(y), int(m), 1).strftime("%B %Y")
    except Exception:
        return period_key


def _company_options() -> list[str]:
    if is_owner_user(current_user):
        return list_company_options()
    name = clean_company_name(getattr(current_user, "company_name", "") or "")
    return [name] if name else []


def _ensure_schema() -> None:
    from frontend.sustainability.schema import ensure_sustainability_schema

    try:
        ensure_sustainability_schema()
    except Exception:
        pass


def _hub_context(company: str, period_key: str, tab: str) -> dict:
    workflow = service.workflow_payload_for_user(current_user, company_name=company)
    caps = workflow.get("capabilities") or {}
    questionnaire = None
    if company:
        questionnaire = BusinessQuestionnaireAnswer.query.filter_by(
            user_id=int(current_user.id),
            company_name=company,
            reporting_period_key=period_key,
        ).first()

    return {
        "user": current_user,
        "active_tab": tab,
        "period_key": period_key,
        "company_name": company,
        "company_options": _company_options(),
        "business_functions": BUSINESS_FUNCTIONS,
        "business_function_current": normalize_business_function(getattr(current_user, "business_type", None)),
        "product_types": PRODUCT_TYPE_OPTIONS,
        "electricity_types": ELECTRICITY_TYPES,
        "heating_types": HEATING_TYPES,
        "country_options": iso_countries(),
        "destination_options": DESTINATION_OPTIONS,
        "workflow": workflow,
        "capabilities": caps,
        "enabled_categories": workflow.get("enabled_categories", []),
        "company_config": service.company_config_payload(company),
        "transport_routes": service.list_transport_routes(company) if company else [],
        "cat11_profiles": service.list_cat11_profiles(company) if company else [],
        "gt_monthly": service.gt_nordics_scenario_payload() if caps.get("gt_monthly_scenario") else None,
        "methodologies": service.list_methodologies(company),
        "average_factors": service.list_average_factors(),
        "shared_offices": service.list_shared_offices(),
        "questionnaire": questionnaire,
        "recent_runs": [
            service.calculation_run_payload(r)
            for r in SustainabilityCalculationRun.query.filter_by(user_id=int(current_user.id))
            .order_by(SustainabilityCalculationRun.created_at.desc())
            .limit(8)
            .all()
        ],
        "can_edit_admin": is_owner_user(current_user)
        or str(getattr(current_user, "role", "")).lower() in {"admin", "super_admin"},
        "products_enabled": caps.get("products_page", False),
        "nordic_epod_workbook": _nordic_workbook_card_context(company),
    }


def _nordic_workbook_card_context(company: str):
    from frontend.sustainability.profiles.nordic_epod_workbook import workbook_profile_document
    from frontend.sustainability.workflow_registry import normalize_company_key

    if normalize_company_key(company) == "Nordic EPOD":
        return workbook_profile_document()
    return None


@bp.route("/estimation", methods=["GET"], endpoint="sustainability_estimation_hub")
@login_required
def estimation_hub():
    _ensure_schema()
    service.bootstrap()
    period_key = str(request.args.get("period") or _current_period_key()).strip()
    tab = str(request.args.get("tab") or "workflow").strip().lower()
    companies = _company_options()
    company = str(request.args.get("company") or (companies[0] if companies else "")).strip()
    if company and company not in companies and companies:
        company = companies[0]
    return render_template("sustainability/estimation_hub.html", **_hub_context(company, period_key, tab))


@bp.route("/products", methods=["GET"], endpoint="sustainability_products_page")
@login_required
def products_page():
    _ensure_schema()
    service.bootstrap()
    period_key = str(request.args.get("period") or _current_period_key()).strip()
    companies = _company_options()
    company = str(request.args.get("company") or (companies[0] if companies else "")).strip()
    if company and company not in companies and companies:
        company = companies[0]

    workflow = service.workflow_payload_for_user(current_user, company_name=company)
    if not workflow.get("capabilities", {}).get("products_page"):
        flash("Products workflow is only available for Manufacturer business function at Nordic EPOD, DC Piping, or GT Nordics.")
        return redirect(url_for("sustainability.sustainability_estimation_hub", company=company, period=period_key))

    from frontend.sustainability.profiles.nordic_epod_workbook import (
        workbook_profile_document as nordic_epod_workbook_profile,
    )
    from frontend.sustainability.workflow_registry import normalize_company_key

    is_epod = normalize_company_key(company) == "Nordic EPOD"

    return render_template(
        "sustainability/products_workflow.html",
        user=current_user,
        period_key=period_key,
        period_label=_period_label(period_key),
        company_name=company,
        company_options=companies,
        workflow=workflow,
        enabled_categories=workflow.get("enabled_categories", []),
        product_types=PRODUCT_TYPE_OPTIONS,
        nordic_epod_workbook=nordic_epod_workbook_profile() if is_epod else None,
        destination_options=DESTINATION_OPTIONS,
        quantity_units=QUANTITY_UNITS,
        product_units=PRODUCT_UNITS,
        product_rows=service.list_product_entries(company, period_key),
        transport_routes=service.list_transport_routes(company),
        cat11_profiles=service.list_cat11_profiles(company),
        gt_monthly=service.gt_nordics_scenario_payload(),
        methodologies=service.list_methodologies(company),
    )


@bp.route("/estimation/run/<int:run_id>", methods=["GET"], endpoint="sustainability_estimation_run_detail")
@login_required
def estimation_run_detail(run_id: int):
    _ensure_schema()
    run = SustainabilityCalculationRun.query.get_or_404(run_id)
    if int(run.user_id) != int(current_user.id) and not is_owner_user(current_user):
        flash("You do not have access to this calculation run.")
        return redirect(url_for("sustainability.sustainability_estimation_hub"))
    return render_template(
        "sustainability/estimation_run_detail.html",
        user=current_user,
        run=service.calculation_run_payload(run),
    )


@bp.route("/api/workflow", methods=["GET"])
@login_required
def api_workflow():
    service.bootstrap()
    company = clean_company_name(request.args.get("company"))
    bf = request.args.get("business_function") or getattr(current_user, "business_type", None)
    workflow = resolve_workflow_context(
        business_function=normalize_business_function(bf),
        company_name=company,
        product_type=request.args.get("product_type"),
    )
    return jsonify(
        {
            "ok": True,
            "workflow": workflow,
            "transport_routes": service.list_transport_routes(company) if company else [],
            "cat11_profiles": service.list_cat11_profiles(company) if company else [],
            "company_config": service.company_config_payload(company),
            "gt_monthly": service.gt_nordics_scenario_payload(),
        }
    )


@bp.route("/api/questionnaire", methods=["GET", "POST"])
@login_required
def api_questionnaire():
    _ensure_schema()
    service.bootstrap()
    if is_readonly_user(current_user):
        return jsonify({"error": "Auditor accounts are read-only."}), 403

    if request.method == "GET":
        company = clean_company_name(request.args.get("company"))
        period = str(request.args.get("period") or _current_period_key()).strip()
        row = BusinessQuestionnaireAnswer.query.filter_by(
            user_id=int(current_user.id),
            company_name=company,
            reporting_period_key=period,
        ).first()
        if not row:
            return jsonify({"ok": True, "data": None, "workflow": service.workflow_payload_for_user(current_user, company_name=company)})
        return jsonify(
            {
                "ok": True,
                "data": {
                    "company_name": row.company_name,
                    "business_function": row.business_function,
                    "office_site_key": row.office_site_key,
                    "country": row.country,
                    "employee_count": row.employee_count,
                    "office_size_m2": row.office_size_m2,
                    "is_shared_office": row.is_shared_office,
                    "has_utility_bills": row.has_utility_bills,
                    "electricity_type": row.electricity_type,
                    "heating_type": row.heating_type,
                    "product_type": row.product_type,
                    "reporting_period_key": row.reporting_period_key,
                },
                "workflow": service.workflow_payload_for_user(
                    current_user,
                    company_name=company,
                    product_type=row.product_type,
                ),
            }
        )

    payload = request.get_json(silent=True) or {}
    payload["business_function"] = normalize_business_function(
        payload.get("business_function") or getattr(current_user, "business_type", None)
    )
    payload["reporting_period_key"] = str(payload.get("reporting_period_key") or _current_period_key()).strip()
    row = service.save_questionnaire(int(current_user.id), payload)
    return jsonify({"ok": True, "id": row.id, "message": "Questionnaire saved."})


@bp.route("/api/products", methods=["GET", "POST"])
@login_required
def api_products():
    _ensure_schema()
    service.bootstrap()
    if is_readonly_user(current_user):
        return jsonify({"error": "Auditor accounts are read-only."}), 403

    company = clean_company_name(request.args.get("company") if request.method == "GET" else (request.get_json(silent=True) or {}).get("company_name"))
    period = str(
        (request.args.get("period") if request.method == "GET" else (request.get_json(silent=True) or {}).get("reporting_period_key"))
        or _current_period_key()
    ).strip()

    workflow = service.workflow_payload_for_user(current_user, company_name=company)
    if not workflow.get("capabilities", {}).get("products_page"):
        return jsonify({"error": "Products workflow not enabled for this business function/company."}), 403

    if request.method == "GET":
        return jsonify(
            {
                "ok": True,
                "rows": service.list_product_entries(company, period),
                "workflow": workflow,
            }
        )

    payload = request.get_json(silent=True) or {}
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return jsonify({"error": "At least one product row is required."}), 400

    for i, row in enumerate(rows, start=1):
        if not str(row.get("product_type") or "").strip():
            return jsonify({"error": f"Row {i}: product type required."}), 400
        pt_err = validate_product_type(row.get("product_type"))
        if pt_err:
            return jsonify({"error": f"Row {i}: {pt_err}"}), 400
        if float(row.get("product_weight") or 0) <= 0:
            return jsonify({"error": f"Row {i}: product weight required."}), 400

    service.save_product_entries(
        int(current_user.id),
        company_name=company,
        period_key=period,
        period_label=_period_label(period),
        rows=rows,
    )
    return jsonify({"ok": True, "message": "Product log saved.", "rows": service.list_product_entries(company, period)})


@bp.route("/api/products/proof", methods=["POST"])
@login_required
def api_products_proof():
    if is_readonly_user(current_user):
        return jsonify({"error": "Read-only."}), 403
    f = request.files.get("proof")
    if not f or not f.filename:
        return jsonify({"error": "Proof file required."}), 400
    PROOF_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    safe = secure_filename(f.filename)
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    dest = PROOF_UPLOAD_DIR / f"{int(current_user.id)}_{stamp}_{safe}"
    f.save(dest)
    return jsonify({"ok": True, "proof_attachment_path": str(dest), "proof_attachment_name": safe})


@bp.route("/api/calculate", methods=["POST"])
@login_required
def api_calculate():
    _ensure_schema()
    service.bootstrap()
    if is_readonly_user(current_user):
        return jsonify({"error": "Auditor accounts are read-only."}), 403

    payload = request.get_json(silent=True) or {}
    company = clean_company_name(payload.get("company_name"))
    period = str(payload.get("reporting_period_key") or _current_period_key()).strip()
    if not company:
        return jsonify({"error": "Company is required."}), 400

    bf = normalize_business_function(payload.get("business_function") or getattr(current_user, "business_type", None))
    workflow = service.workflow_payload_for_user(current_user, company_name=company, product_type=payload.get("product_type"))
    errs = validate_calculation_request(workflow)
    if errs:
        return jsonify({"error": errs[0], "validation_errors": errs}), 400

    questionnaire = service.save_questionnaire(
        int(current_user.id),
        {**payload, "company_name": company, "reporting_period_key": period, "business_function": bf},
    )
    actual = payload.get("actual_utilities") if isinstance(payload.get("actual_utilities"), dict) else None
    product_weight = payload.get("product_weight_kg")
    product_rows = service.product_rows_for_calculation(company, period)

    run = service.run_calculation_for_questionnaire(
        int(current_user.id),
        questionnaire,
        actual_utilities=actual,
        product_weight_kg=float(product_weight) if product_weight not in (None, "") else None,
        product_rows=product_rows,
    )
    return jsonify(
        {
            "ok": True,
            "run_id": run.id,
            "total_co2e_t": run.total_co2e_t,
            "result": service.calculation_run_payload(run)["result"],
            "workflow": workflow,
            "detail_url": url_for("sustainability.sustainability_estimation_run_detail", run_id=run.id),
        }
    )


@bp.route("/api/methodologies", methods=["GET"])
@login_required
def api_methodologies():
    service.bootstrap()
    company = clean_company_name(request.args.get("company"))
    return jsonify({"ok": True, "methodologies": service.list_methodologies(company or None)})


@bp.route("/admin/methodologies", methods=["GET"], endpoint="sustainability_admin_methodologies")
@login_required
def admin_methodologies():
    service.bootstrap()
    if not (is_owner_user(current_user) or str(getattr(current_user, "role", "")).lower() in {"admin", "super_admin"}):
        flash("Admin access required.")
        return redirect(url_for("sustainability.sustainability_estimation_hub"))

    from frontend.sustainability.models import EolMaterialComposition, EolScenario, TransportRouteAssumption
    from frontend.sustainability.profiles.nordic_epod_workbook import (
        admin_workbook_lifecycle_rows,
        workbook_profile_document,
    )

    eol_profiles = (
        EolComponentProfile.query.join(EolScenario, EolComponentProfile.eol_scenario_id == EolScenario.id)
        .filter(
            EolScenario.methodology_type != "nordic_epod_workbook_literal_kg",
            db.or_(
                EolScenario.company_key.is_(None),
                EolScenario.company_key != "Nordic EPOD",
            ),
        )
        .order_by(EolComponentProfile.sort_order.asc())
        .all()
    )

    return render_template(
        "sustainability/admin_methodologies.html",
        user=current_user,
        methodologies=MethodologyVersion.query.order_by(MethodologyVersion.label.asc()).all(),
        average_factors=AverageFactor.query.order_by(AverageFactor.metric_group.asc()).all(),
        nordic_epod_workbook=workbook_profile_document(),
        nordic_admin_lifecycle_rows=admin_workbook_lifecycle_rows(),
        eol_profiles=eol_profiles,
        eol_materials=EolMaterialComposition.query.order_by(EolMaterialComposition.sort_order.asc()).all(),
        transport_routes=TransportRouteAssumption.query.order_by(
            TransportRouteAssumption.company_key.asc(),
            TransportRouteAssumption.destination_region.asc(),
        ).all(),
    )


@bp.route("/api/admin/average-factor/<int:factor_id>", methods=["POST"])
@login_required
def api_admin_update_average_factor(factor_id: int):
    if not (is_owner_user(current_user) or str(getattr(current_user, "role", "")).lower() in {"admin", "super_admin"}):
        return jsonify({"error": "Forbidden"}), 403
    row = service.admin_update_average_factor(factor_id, request.get_json(silent=True) or {})
    return jsonify({"ok": True, "id": row.id})


@bp.route("/api/admin/eol-profile/<int:profile_id>", methods=["POST"])
@login_required
def api_admin_update_eol_profile(profile_id: int):
    if not (is_owner_user(current_user) or str(getattr(current_user, "role", "")).lower() in {"admin", "super_admin"}):
        return jsonify({"error": "Forbidden"}), 403
    row = service.admin_update_eol_profile(profile_id, request.get_json(silent=True) or {})
    return jsonify({"ok": True, "id": row.id})

