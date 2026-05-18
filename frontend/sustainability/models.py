"""Normalized sustainability methodology tables (CTS Group workbook logic)."""

from __future__ import annotations

from datetime import datetime

from frontend.extensions import db


class MethodologyVersion(db.Model):
    __tablename__ = "methodology_versions"

    id = db.Column(db.Integer, primary_key=True)
    version_key = db.Column(db.String(80), unique=True, nullable=False, index=True)
    label = db.Column(db.String(200), nullable=False)
    company_key = db.Column(db.String(120), nullable=True, index=True)
    product_type = db.Column(db.String(80), nullable=True, index=True)
    description = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    is_published = db.Column(db.Boolean, default=True, nullable=False)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    eol_scenarios = db.relationship("EolScenario", back_populates="methodology_version", lazy="dynamic")


class CompanyMethodologyConfig(db.Model):
    """Workbook-fixed company rules (category activation, defaults)."""

    __tablename__ = "company_methodology_configs"

    id = db.Column(db.Integer, primary_key=True)
    company_key = db.Column(db.String(120), unique=True, nullable=False, index=True)
    enabled_categories_json = db.Column(db.Text, nullable=False, default='["9","11","12"]')
    is_manufacturer = db.Column(db.Boolean, default=False, nullable=False)
    origin_country = db.Column(db.String(120), nullable=True)
    notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class TransportRouteAssumption(db.Model):
    """Embedded Cat 9 land/sea distances per company and destination region."""

    __tablename__ = "transport_route_assumptions"

    id = db.Column(db.Integer, primary_key=True)
    company_key = db.Column(db.String(120), nullable=False, index=True)
    destination_region = db.Column(db.String(80), nullable=False, index=True)
    land_distance_km = db.Column(db.Float, nullable=False)
    sea_distance_km = db.Column(db.Float, nullable=False, default=0.0)
    road_ef_t_per_tonne_km = db.Column(db.Float, nullable=False, default=0.000059)
    sea_ef_t_per_tonne_km = db.Column(db.Float, nullable=False, default=0.000016)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("company_key", "destination_region", name="uq_transport_company_destination"),
    )


class Category11Methodology(db.Model):
    """Embedded Cat 11 lifetime kWh and electricity EF assumptions."""

    __tablename__ = "category11_methodologies"

    id = db.Column(db.Integer, primary_key=True)
    company_key = db.Column(db.String(120), nullable=False, index=True)
    profile_key = db.Column(db.String(80), nullable=False, default="default")
    label = db.Column(db.String(200), nullable=False)
    lifetime_kwh = db.Column(db.Float, nullable=False)
    electricity_ef_kg_per_kwh = db.Column(db.Float, nullable=True)
    default_country = db.Column(db.String(120), nullable=True)
    country_ef_json = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("company_key", "profile_key", name="uq_cat11_company_profile"),
    )


class GtNordicsMonthlyScenario(db.Model):
    """Fixed GT Nordics monthly product methodology (workbook scenario)."""

    __tablename__ = "gt_nordics_monthly_scenarios"

    id = db.Column(db.Integer, primary_key=True)
    scenario_key = db.Column(db.String(80), unique=True, nullable=False)
    label = db.Column(db.String(200), nullable=False)
    product_type = db.Column(db.String(80), nullable=False)
    default_quantity = db.Column(db.Float, nullable=False)
    quantity_unit = db.Column(db.String(40), nullable=False, default="pieces")
    average_product_weight_kg = db.Column(db.Float, nullable=False)
    product_unit = db.Column(db.String(40), nullable=False, default="kg")
    default_destination = db.Column(db.String(80), nullable=False)
    assumptions_json = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class EolScenario(db.Model):
    __tablename__ = "eol_scenarios"

    id = db.Column(db.Integer, primary_key=True)
    methodology_version_id = db.Column(db.Integer, db.ForeignKey("methodology_versions.id"), nullable=False, index=True)
    scenario_key = db.Column(db.String(80), nullable=False, index=True)
    label = db.Column(db.String(200), nullable=False)
    product_type = db.Column(db.String(80), nullable=False, index=True)
    company_key = db.Column(db.String(120), nullable=True, index=True)
    methodology_type = db.Column(db.String(40), nullable=False, default="disposal_ratios")
    description = db.Column(db.Text, nullable=True)
    is_default = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    methodology_version = db.relationship("MethodologyVersion", back_populates="eol_scenarios")
    component_profiles = db.relationship("EolComponentProfile", back_populates="eol_scenario", lazy="joined")
    material_compositions = db.relationship("EolMaterialComposition", back_populates="eol_scenario", lazy="joined")


class EolComponentProfile(db.Model):
    __tablename__ = "eol_component_profiles"

    id = db.Column(db.Integer, primary_key=True)
    eol_scenario_id = db.Column(db.Integer, db.ForeignKey("eol_scenarios.id"), nullable=False, index=True)
    component_key = db.Column(db.String(80), nullable=False)
    component_label = db.Column(db.String(200), nullable=False)
    weight_fraction = db.Column(db.Float, nullable=False, default=1.0)
    disposal_stream = db.Column(db.String(40), nullable=False)
    ratio_pct = db.Column(db.Float, nullable=False)
    sort_order = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    eol_scenario = db.relationship("EolScenario", back_populates="component_profiles")


class EolMaterialComposition(db.Model):
    """DC Piping-style material % → single disposal stream per material."""

    __tablename__ = "eol_material_compositions"

    id = db.Column(db.Integer, primary_key=True)
    eol_scenario_id = db.Column(db.Integer, db.ForeignKey("eol_scenarios.id"), nullable=False, index=True)
    material_key = db.Column(db.String(80), nullable=False)
    material_label = db.Column(db.String(200), nullable=False)
    composition_pct = db.Column(db.Float, nullable=False)
    disposal_stream = db.Column(db.String(40), nullable=False)
    sort_order = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    eol_scenario = db.relationship("EolScenario", back_populates="material_compositions")


class AverageFactor(db.Model):
    __tablename__ = "average_factors"

    id = db.Column(db.Integer, primary_key=True)
    factor_key = db.Column(db.String(80), nullable=False, index=True)
    label = db.Column(db.String(200), nullable=False)
    metric_group = db.Column(db.String(40), nullable=False, index=True)
    country = db.Column(db.String(120), nullable=True, index=True)
    value = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String(40), nullable=False)
    emission_factor = db.Column(db.Float, nullable=True)
    scope_category = db.Column(db.String(120), nullable=False)
    methodology_version_id = db.Column(db.Integer, db.ForeignKey("methodology_versions.id"), nullable=True, index=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SharedOfficeProfile(db.Model):
    __tablename__ = "shared_office_profiles"

    id = db.Column(db.Integer, primary_key=True)
    office_site_key = db.Column(db.String(120), unique=True, nullable=False, index=True)
    office_label = db.Column(db.String(200), nullable=False)
    country = db.Column(db.String(120), nullable=True)
    total_employee_count = db.Column(db.Integer, nullable=False, default=0)
    total_floor_area_m2 = db.Column(db.Float, nullable=True)
    notes = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class BusinessQuestionnaireAnswer(db.Model):
    __tablename__ = "business_questionnaire_answers"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    company_name = db.Column(db.String(200), nullable=False, index=True)
    business_function = db.Column(db.String(120), nullable=True, index=True)
    office_site_key = db.Column(db.String(120), nullable=True)
    country = db.Column(db.String(120), nullable=True)
    employee_count = db.Column(db.Integer, nullable=True)
    office_size_m2 = db.Column(db.Float, nullable=True)
    is_shared_office = db.Column(db.Boolean, default=False, nullable=False)
    has_utility_bills = db.Column(db.Boolean, default=False, nullable=False)
    electricity_type = db.Column(db.String(80), nullable=True)
    heating_type = db.Column(db.String(80), nullable=True)
    product_type = db.Column(db.String(80), nullable=True)
    reporting_period_key = db.Column(db.String(7), nullable=False, index=True)
    reporting_year = db.Column(db.Integer, nullable=True)
    reporting_month = db.Column(db.Integer, nullable=True)
    methodology_version_id = db.Column(db.Integer, db.ForeignKey("methodology_versions.id"), nullable=True)
    eol_scenario_id = db.Column(db.Integer, db.ForeignKey("eol_scenarios.id"), nullable=True)
    answers_json = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    calculation_runs = db.relationship("SustainabilityCalculationRun", back_populates="questionnaire", lazy="dynamic")


class SustainabilityProductEntry(db.Model):
    """Methodology-driven product log (replaces prototype products page)."""

    __tablename__ = "sustainability_product_entries"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    company_name = db.Column(db.String(200), nullable=False, index=True)
    reporting_period_key = db.Column(db.String(7), nullable=False, index=True)
    reporting_period_label = db.Column(db.String(40), nullable=False)
    row_index = db.Column(db.Integer, nullable=False, default=1)
    product_type = db.Column(db.String(80), nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    quantity_unit = db.Column(db.String(80), nullable=False)
    end_use_location = db.Column(db.String(200), nullable=False)
    product_weight = db.Column(db.Float, nullable=False)
    product_unit = db.Column(db.String(80), nullable=False)
    proof_attachment_path = db.Column(db.String(500), nullable=True)
    proof_attachment_name = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.Index("ix_sust_product_company_period", "company_name", "reporting_period_key"),
    )


class SustainabilityCalculationRun(db.Model):
    __tablename__ = "sustainability_calculation_runs"

    id = db.Column(db.Integer, primary_key=True)
    questionnaire_id = db.Column(db.Integer, db.ForeignKey("business_questionnaire_answers.id"), nullable=True, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    company_name = db.Column(db.String(200), nullable=False, index=True)
    reporting_period_key = db.Column(db.String(7), nullable=False, index=True)
    status = db.Column(db.String(40), default="completed", nullable=False)
    input_snapshot_json = db.Column(db.Text, nullable=True)
    result_json = db.Column(db.Text, nullable=True)
    total_co2e_t = db.Column(db.Float, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    questionnaire = db.relationship("BusinessQuestionnaireAnswer", back_populates="calculation_runs")
