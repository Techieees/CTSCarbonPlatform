"""Reporting period + Employee Commuting dedup helpers (normalization stability)."""

from frontend.services import employee_commuting_service as ecs
from frontend.services.reporting_period_service import normalize_reporting_period


def test_normalize_reporting_period_variants_2028():
    want = "Jan-2028"
    assert normalize_reporting_period("Jan-2028") == want
    assert normalize_reporting_period("JAN-2028") == want
    assert normalize_reporting_period("JAN 2028") == want
    assert normalize_reporting_period("January 2028") == want
    assert normalize_reporting_period("2028-01") == want


def test_period_key_from_label_matches_iso_month():
    assert ecs.period_key_from_label("JAN 2028") == "2028-01"
    assert ecs.period_key_from_label("2028-01") == "2028-01"


def test_stable_dedup_independent_of_job_id():
    a = ecs.stable_employee_commuting_dedup_value(
        company_key="Acme",
        period_yyyy_mm="2028-01",
        mode_name="Bus",
        data_type_label="National average",
        category_sheet_key=ecs.EMPLOYEE_COMMUTING_TARGET_SHEET,
        slot_within_mode=3,
    )
    b = ecs.stable_employee_commuting_dedup_value(
        company_key="Acme",
        period_yyyy_mm="2028-01",
        mode_name="Bus",
        data_type_label="National average",
        category_sheet_key=ecs.EMPLOYEE_COMMUTING_TARGET_SHEET,
        slot_within_mode=3,
    )
    assert a == b
    assert len(a) == 40
