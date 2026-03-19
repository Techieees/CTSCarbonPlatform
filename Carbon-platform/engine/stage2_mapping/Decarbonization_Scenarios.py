from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR

from Decarb_S3Cat1 import build_s3_cat1_bau_and_decarb


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR
WINDOW_PATTERN = "mapped_results_window_*.xlsx"

# Sheet names / prefixes
SCOPE1_SHEET = "Scope 1"
SCOPE2_SHEET = "Scope 2"
S3CAT1_SHEET = "S3 Cat 1 Purchased G&S"
S3_PREFIX = "S3 Cat "

# Common columns
DATE_COL = "Date"
CO2_COL = "co2e (t)"

# Scope 1 specifics
S1_VEHICLE_COL = "Vehicle Type"
S1_COUNTRY_COL = "country"
S1_SPEND_COL = "Spend_Euro"
S1_EF_COL = "ef_value"

# Scope 2 specifics (we use co2e(t) directly for robustness)

# S3 Cat 1 specifics
S3_EF_ID_COL = "ef_id"
DUMMY_EF_ID_COL = "dummy_ef_id"


# Scope 1 conversion inputs (from your previous implementation)
ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY: Dict[str, float] = {
    "Denmark": 0.00005118,
    "Europe": 0.00026720,
    "EU_average": 0.00005180,
    "Finland": 0.00003320,
    "France": 0.00001807,
    "Germany": 0.00031158,
    "Global": 0.00046240,
    "Iceland": 0.00000017,
    "India": 0.00075190,
    "Ireland": 0.00024557,
    "Italy": 0.00023524,
    "nordic_electricity_mix": 0.00052410,
    "Norway": 0.00000674,
    "Portugal": 0.00004330,
    "Qatar": 0.00047410,
    "Spain": 0.00010066,
    "Sweden": 0.00000505,
    "Switzerland": 0.00000578,
    "Turkey": 0.00042980,
    "United Arab Emirates": 0.00041930,
    "United Kingdom": 0.00014850,
}
HVO100_EF_T_PER_EUR = 0.00013


def _find_latest_window_workbook(base_dir: Path) -> Optional[Path]:
    out_dir = STAGE2_OUTPUT_DIR
    try:
        candidates = [p for p in out_dir.rglob(WINDOW_PATTERN) if p.is_file() and (not p.name.startswith("~$"))]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]
    except Exception:
        return None


def _safe_to_numeric(series: pd.Series) -> pd.Series:
    try:
        if series is None:
            return pd.Series(dtype="float64")
        if pd.api.types.is_numeric_dtype(series):
            return pd.to_numeric(series, errors="coerce")
        txt = series.astype(str).str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False)

        def _parse_one(v: str) -> Optional[float]:
            try:
                vv = str(v).strip()
                if vv == "" or vv.lower() == "nan":
                    return None
                if "," in vv and "." in vv:
                    last_c = vv.rfind(",")
                    last_d = vv.rfind(".")
                    if last_c > last_d:
                        vv = vv.replace(".", "").replace(",", ".")
                    else:
                        vv = vv.replace(",", "")
                else:
                    vv = vv.replace(",", ".")
                return float(vv)
            except Exception:
                return None

        parsed = txt.map(_parse_one)
        return pd.to_numeric(parsed, errors="coerce")
    except Exception:
        return pd.to_numeric(series, errors="coerce")


def _monthly_sum_2025(df: pd.DataFrame, *, date_col: str = DATE_COL, value_col: str = CO2_COL) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if date_col not in df.columns or value_col not in df.columns:
        return pd.Series(dtype=float)
    dt = pd.to_datetime(df[date_col], errors="coerce")
    val = _safe_to_numeric(df[value_col]).fillna(0.0)
    tmp = pd.DataFrame({"dt": dt, "val": val}).dropna(subset=["dt"])
    if tmp.empty:
        return pd.Series(dtype=float)
    tmp["m"] = tmp["dt"].dt.to_period("M").dt.to_timestamp(how="start")
    m = tmp.groupby("m", dropna=False)["val"].sum().sort_index()
    idx = pd.date_range("2025-01-01", "2025-12-01", freq="MS")
    m = m.reindex(idx).fillna(0.0)
    m.index.name = "Month"
    return m.astype(float)


def _pick_electricity_ef(country: object) -> float:
    c = "" if country is None else str(country).strip()
    if c in ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY:
        return float(ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY[c])
    if c.lower() in {"eu", "european union"} and "EU_average" in ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY:
        return float(ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY["EU_average"])
    return float(ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY.get("Global", np.nan))


def _scope1_full_decarb_monthly_2025(df_scope1: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    Returns (baseline_monthly, full_decarb_monthly) for 2025.
    Full decarb logic:
      - Vehicle Type contains 'Generator diesel' -> HVO100 EF
      - Else -> EV (electricity EF by country)
    Emissions computed as Spend_Euro * EF_new, with fallback ratio method.
    """
    base = df_scope1.copy()
    for c in [DATE_COL, CO2_COL, S1_VEHICLE_COL, S1_COUNTRY_COL, S1_SPEND_COL, S1_EF_COL]:
        if c not in base.columns:
            raise ValueError(f"Scope 1 missing column: {c}")

    base[DATE_COL] = pd.to_datetime(base[DATE_COL], errors="coerce")
    base = base.dropna(subset=[DATE_COL]).copy()
    base["_m"] = base[DATE_COL].dt.to_period("M").dt.to_timestamp(how="start")
    base = base[(base["_m"] >= pd.Timestamp("2025-01-01")) & (base["_m"] <= pd.Timestamp("2025-12-01"))].copy()

    spend = _safe_to_numeric(base[S1_SPEND_COL])
    ef_old = _safe_to_numeric(base[S1_EF_COL])
    co2_old = _safe_to_numeric(base[CO2_COL])

    vt = base[S1_VEHICLE_COL].astype(str)
    is_gen_diesel = vt.str.strip().str.lower().eq("generator diesel") | vt.str.lower().str.contains("generator diesel", na=False)
    ef_new = ef_old.astype(float).copy()
    ef_new.loc[is_gen_diesel] = float(HVO100_EF_T_PER_EUR)
    ef_new.loc[~is_gen_diesel] = base.loc[~is_gen_diesel, S1_COUNTRY_COL].map(_pick_electricity_ef).astype(float)

    co2_new = np.full(len(base), np.nan, dtype=float)
    m_spend = np.isfinite(spend.to_numpy(dtype=float)) & np.isfinite(ef_new.to_numpy(dtype=float))
    co2_new[m_spend] = spend.to_numpy(dtype=float)[m_spend] * ef_new.to_numpy(dtype=float)[m_spend]

    m_ratio = (~m_spend) & np.isfinite(co2_old.to_numpy(dtype=float)) & np.isfinite(ef_new.to_numpy(dtype=float)) & np.isfinite(
        ef_old.to_numpy(dtype=float)
    ) & (ef_old.to_numpy(dtype=float) > 0)
    co2_new[m_ratio] = co2_old.to_numpy(dtype=float)[m_ratio] * (ef_new.to_numpy(dtype=float)[m_ratio] / ef_old.to_numpy(dtype=float)[m_ratio])

    # fallback keep baseline
    m_keep = ~np.isfinite(co2_new)
    co2_new[m_keep] = co2_old.to_numpy(dtype=float)[m_keep]

    base_monthly = _monthly_sum_2025(base, date_col=DATE_COL, value_col=CO2_COL)
    tmp_new = base[["_m"]].copy()
    tmp_new["val"] = co2_new
    new_monthly = tmp_new.groupby("_m", dropna=False)["val"].sum().sort_index()
    idx = pd.date_range("2025-01-01", "2025-12-01", freq="MS")
    new_monthly = new_monthly.reindex(idx).fillna(0.0)
    new_monthly.index.name = "Month"
    return base_monthly.astype(float), new_monthly.astype(float)


def _s3cat1_full_decarb_monthly_2025(df_s3: pd.DataFrame, *, ef_reduction_by_id: Dict[str, float]) -> Tuple[pd.Series, pd.Series]:
    base = df_s3.copy()
    for c in [DATE_COL, CO2_COL, "ef_id", "ef_value", "Spend_Euro"]:
        if c not in base.columns:
            raise ValueError(f"S3 Cat 1 missing column: {c}")

    base[DATE_COL] = pd.to_datetime(base[DATE_COL], errors="coerce")
    base = base.dropna(subset=[DATE_COL]).copy()
    base["_m"] = base[DATE_COL].dt.to_period("M").dt.to_timestamp(how="start")
    base = base[(base["_m"] >= pd.Timestamp("2025-01-01")) & (base["_m"] <= pd.Timestamp("2025-12-01"))].copy()

    stacked = build_s3_cat1_bau_and_decarb(base, ef_reduction_by_id=ef_reduction_by_id)
    # Use the robust emissions column from the function
    stacked["_m"] = pd.to_datetime(stacked[DATE_COL], errors="coerce").dt.to_period("M").dt.to_timestamp(how="start")
    stacked = stacked.dropna(subset=["_m"])

    bau = stacked[stacked["Scenario"] == "BAU"].copy()
    dec = stacked[stacked["Scenario"] == "DECARB"].copy()

    bau_m = bau.groupby("_m", dropna=False)["Emissions_tCO2e"].sum().sort_index()
    dec_m = dec.groupby("_m", dropna=False)["Emissions_tCO2e"].sum().sort_index()

    idx = pd.date_range("2025-01-01", "2025-12-01", freq="MS")
    bau_m = bau_m.reindex(idx).fillna(0.0)
    dec_m = dec_m.reindex(idx).fillna(0.0)
    bau_m.index.name = "Month"
    dec_m.index.name = "Month"
    return bau_m.astype(float), dec_m.astype(float)


def _repeat_monthly_profile(base_2025: pd.Series, idx: pd.DatetimeIndex) -> pd.Series:
    base = base_2025.copy()
    base.index = pd.to_datetime(base.index)
    by_mo = {int(d.month): float(v) for d, v in base.items()}
    vals = [by_mo.get(int(d.month), 0.0) for d in idx]
    return pd.Series(vals, index=idx, name=str(getattr(base_2025, "name", "value"))).astype(float)


def _growth_factor(idx: pd.DatetimeIndex, *, annual_growth: float, base_month: pd.Timestamp) -> np.ndarray:
    g = float(annual_growth)
    g = max(g, -0.99)
    months_ahead = (idx.year - base_month.year) * 12 + (idx.month - base_month.month)
    months_ahead = months_ahead.astype(float)
    return (1.0 + g) ** (months_ahead / 12.0)


def _linear_ramp(idx: pd.DatetimeIndex, *, start: pd.Timestamp, end: pd.Timestamp, start_value: float, end_value: float) -> np.ndarray:
    sv = float(np.clip(start_value, 0.0, 1.0))
    ev = float(np.clip(end_value, 0.0, 1.0))
    if end <= start:
        return np.full(len(idx), ev, dtype=float)
    t = (idx - start).days.astype(float)
    denom = float((end - start).days)
    w = np.clip(t / max(1.0, denom), 0.0, 1.0)
    out = np.array((sv + (ev - sv) * w), dtype=float, copy=True)
    out[idx < start] = sv
    out[idx >= end] = ev
    return out.astype(float)


@dataclass(frozen=True)
class ScenarioConfig:
    start_year: int = 2025
    end_year: int = 2030
    rollout_start: pd.Timestamp = pd.Timestamp("2026-01-01")
    rollout_end: pd.Timestamp = pd.Timestamp("2030-01-01")  # reach full decarb by Jan 2030
    annual_growth_default: float = 0.0
    annual_growth_10pct: float = 0.10
    annual_growth_expansion: float = 0.15
    # Expansion scenario multipliers (applied on top of growth)
    expansion_shipping_multiplier: float = 1.35
    expansion_manufacturing_multiplier: float = 1.15  # proxy via S3 Cat 1
    expansion_scope2_dc_multiplier: float = 1.20  # new DCs -> higher electricity demand (BAU before PPA)
    ef_reduction_by_id: Dict[str, float] = None  # set in factory

    # --- User 4-scenario inputs (S3 Cat 1, dummy-based) ---
    steel_dummy_ids: Tuple[str, ...] = ("DUMMY_00068", "DUMMY_00041", "DUMMY_00009")
    concrete_dummy_ids: Tuple[str, ...] = ("DUMMY_00021", "DUMMY_00012")
    steel_reduction: float = 0.40  # low-carbon steel vs baseline (fraction reduction of co2e(t) for the switched share)
    concrete_reduction: float = 0.30  # low-carbon concrete vs baseline (fraction reduction)
    # Procurement shares by year (applied as: effective multiplier = 1 - share*reduction)
    # Defaults reflect "20/50/70" ramp. Concrete is delayed by default (only kicks in late in horizon).
    steel_share_by_year: Dict[int, float] = None
    concrete_share_by_year: Dict[int, float] = None

    # Scenario 3/4 growth drivers (proxies; can be tuned)
    headcount_growth_yoy: float = 0.30  # applied as smooth annual growth on all tabs
    # One-off build emissions (e.g., data centres) added on top of baseline
    dc_build_count_year1: int = 10
    dc_build_tco2e_each: float = 7066.3
    dc_build_year: int = 2026
    dc_build_target_tab: str = "Scope 3 Category 2 Capital Good"
    # EPOD factories proxy (adds extra demand to Scope 2 + S3 Cat 1)
    epod_factories_total: int = 4
    epod_factories_build_per_year: int = 1
    epod_factory_increment_frac_scope2: float = 0.10  # each fully-ramped factory adds +10% of baseline Scope 2
    epod_factory_increment_frac_s3cat1: float = 0.10  # each fully-ramped factory adds +10% of baseline S3 Cat 1
    # Offices proxy (adds extra demand to Scope 2 + S3 Cat 1)
    new_offices_per_year: int = 3
    office_increment_frac_scope2_each: float = 0.01  # each new office adds +1% of baseline Scope 2
    office_increment_frac_s3cat1_each: float = 0.01  # each new office adds +1% of baseline S3 Cat 1


def _shares_default(start_year: int) -> Dict[int, float]:
    """
    Default 20/50/70 procurement share schedule (year-based).

    User requirement:
    - 20% by end of Year 1
    - 50% in Year 3
    - 70% in Year 5 (2030 for start_year=2025)

    With projection years 2026..2030 (start_year=2025), we interpret this as:
    - 2026: 0.20
    - 2027: 0.20
    - 2028: 0.50
    - 2029: 0.50
    - 2030: 0.70
    """
    return {
        start_year + 1: 0.20,
        start_year + 2: 0.20,
        start_year + 3: 0.50,
        start_year + 4: 0.50,
        start_year + 5: 0.70,
    }


def _share_series(idx: pd.DatetimeIndex, share_by_year: Dict[int, float]) -> np.ndarray:
    if not share_by_year:
        return np.zeros(len(idx), dtype=float)
    years = idx.year.astype(int)
    out = np.zeros(len(idx), dtype=float)
    for i, y in enumerate(years):
        out[i] = float(share_by_year.get(int(y), 0.0))
    return np.clip(out.astype(float), 0.0, 1.0)


def _s3cat1_split_monthly_2025_by_dummy_groups(
    df_s3: pd.DataFrame,
    *,
    dummy_col: str,
    steel_ids: Tuple[str, ...],
    concrete_ids: Tuple[str, ...],
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Returns (steel_2025, concrete_2025, other_2025) monthly sums for 2025.
    Uses the already calculated co2e (t) column (robust across mixed EF sources).
    """
    base = df_s3.copy()
    for c in [DATE_COL, CO2_COL, dummy_col]:
        if c not in base.columns:
            raise ValueError(f"S3 Cat 1 missing column: {c}")

    base[DATE_COL] = pd.to_datetime(base[DATE_COL], errors="coerce")
    base = base.dropna(subset=[DATE_COL]).copy()
    base["_m"] = base[DATE_COL].dt.to_period("M").dt.to_timestamp(how="start")
    base = base[(base["_m"] >= pd.Timestamp("2025-01-01")) & (base["_m"] <= pd.Timestamp("2025-12-01"))].copy()

    base["_co2"] = _safe_to_numeric(base[CO2_COL]).fillna(0.0)
    base["_dummy"] = base[dummy_col].astype(str).str.strip()

    is_steel = base["_dummy"].isin(set(steel_ids))
    is_conc = base["_dummy"].isin(set(concrete_ids))

    steel = base.loc[is_steel].groupby("_m", dropna=False)["_co2"].sum().sort_index()
    conc = base.loc[is_conc].groupby("_m", dropna=False)["_co2"].sum().sort_index()
    other = base.loc[~(is_steel | is_conc)].groupby("_m", dropna=False)["_co2"].sum().sort_index()

    idx = pd.date_range("2025-01-01", "2025-12-01", freq="MS")
    steel = steel.reindex(idx).fillna(0.0)
    conc = conc.reindex(idx).fillna(0.0)
    other = other.reindex(idx).fillna(0.0)
    steel.index.name = "Month"
    conc.index.name = "Month"
    other.index.name = "Month"
    return steel.astype(float), conc.astype(float), other.astype(float)


def build_scenarios(window_path: Path, cfg: ScenarioConfig) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    xls = pd.ExcelFile(window_path)
    s1 = pd.read_excel(xls, sheet_name=SCOPE1_SHEET)
    s2 = pd.read_excel(xls, sheet_name=SCOPE2_SHEET)
    s3cat1_df = pd.read_excel(xls, sheet_name=S3CAT1_SHEET)

    # Load ALL additive S3 tabs so TOTAL always equals the full footprint (matches Power BI total).
    s3_tabs = [sh for sh in xls.sheet_names if str(sh).startswith(S3_PREFIX)]
    s3_dfs: Dict[str, pd.DataFrame] = {}
    for sh in s3_tabs:
        try:
            s3_dfs[str(sh)] = pd.read_excel(xls, sheet_name=sh)
        except Exception:
            continue

    # 2025 base & full-decarb (month-of-year factors)
    s1_bau_2025, s1_full_2025 = _scope1_full_decarb_monthly_2025(s1)
    s2_bau_2025 = _monthly_sum_2025(s2)
    s3cat1_bau_2025, s3cat1_full_2025 = _s3cat1_full_decarb_monthly_2025(
        s3cat1_df, ef_reduction_by_id=cfg.ef_reduction_by_id or {}
    )

    # All S3 cats monthly BAU (2025)
    s3_bau_2025_by_tab: Dict[str, pd.Series] = {}
    for sh, df in s3_dfs.items():
        if sh == S3CAT1_SHEET:
            s3_bau_2025_by_tab[sh] = s3cat1_bau_2025
        else:
            s3_bau_2025_by_tab[sh] = _monthly_sum_2025(df)

    # Convert full decarb to factors by month-of-year
    def _factor(full: pd.Series, base: pd.Series) -> pd.Series:
        b = base.to_numpy(dtype=float)
        f = full.to_numpy(dtype=float)
        out = np.ones_like(b, dtype=float)
        m = b > 0
        out[m] = f[m] / b[m]
        s = pd.Series(out, index=base.index, name="factor")
        return s.clip(lower=0.0, upper=10.0)

    s1_factor_2025 = _factor(s1_full_2025, s1_bau_2025)
    s2_factor_2025 = pd.Series(np.zeros(12, dtype=float), index=s2_bau_2025.index)  # full renewable => 0
    s3cat1_factor_2025 = _factor(s3cat1_full_2025, s3cat1_bau_2025)

    idx = pd.date_range(pd.Timestamp(cfg.start_year, 1, 1), pd.Timestamp(cfg.end_year, 12, 1), freq="MS")
    base_month = pd.Timestamp(cfg.start_year, 1, 1)

    # Baseline projections per scope
    s1_base = _repeat_monthly_profile(s1_bau_2025, idx)
    s2_base = _repeat_monthly_profile(s2_bau_2025, idx)
    s3_base_by_tab: Dict[str, pd.Series] = {sh: _repeat_monthly_profile(ser, idx) for sh, ser in s3_bau_2025_by_tab.items()}

    g_def = _growth_factor(idx, annual_growth=cfg.annual_growth_default, base_month=base_month)
    s1_base = s1_base * g_def
    s2_base = s2_base * g_def
    for sh in list(s3_base_by_tab.keys()):
        s3_base_by_tab[sh] = s3_base_by_tab[sh] * g_def

    # Full decarb factors repeated
    s1_fac = _repeat_monthly_profile(s1_factor_2025, idx)
    s2_fac = _repeat_monthly_profile(s2_factor_2025, idx)
    s3cat1_fac = _repeat_monthly_profile(s3cat1_factor_2025, idx)

    adopt = _linear_ramp(
        idx,
        start=pd.Timestamp(cfg.rollout_start),
        end=pd.Timestamp(cfg.rollout_end),
        start_value=0.0,
        end_value=1.0,
    )

    def apply_adoption(base: pd.Series, fac: pd.Series) -> pd.Series:
        # emissions = base*(1 - adopt*(1 - fac))
        return base * (1.0 - adopt * (1.0 - fac.to_numpy(dtype=float)))

    # Scenario table builder
    scenarios: Dict[str, Dict[str, pd.Series]] = {}

    def _base_parts() -> Dict[str, pd.Series]:
        parts: Dict[str, pd.Series] = {"Scope 1": s1_base, "Scope 2": s2_base}
        for sh, ser in s3_base_by_tab.items():
            parts[sh] = ser
        return parts

    scenarios["BAU"] = _base_parts()
    scenarios["DECARB_Scope1"] = {
        **_base_parts(),
        "Scope 1": apply_adoption(s1_base, s1_fac),
    }
    scenarios["DECARB_Scope2"] = {
        **_base_parts(),
        "Scope 2": apply_adoption(s2_base, s2_fac),
    }
    scenarios["DECARB_S3Cat1"] = {
        **_base_parts(),
        S3CAT1_SHEET: apply_adoption(s3_base_by_tab[S3CAT1_SHEET], s3cat1_fac),
    }

    scenarios["DECARB_All"] = {
        **_base_parts(),
        "Scope 1": apply_adoption(s1_base, s1_fac),
        "Scope 2": apply_adoption(s2_base, s2_fac),
        S3CAT1_SHEET: apply_adoption(s3_base_by_tab[S3CAT1_SHEET], s3cat1_fac),
    }

    # Growth 10% BAU
    g10 = _growth_factor(idx, annual_growth=cfg.annual_growth_10pct, base_month=base_month)
    s1_10 = _repeat_monthly_profile(s1_bau_2025, idx) * g10
    s2_10 = _repeat_monthly_profile(s2_bau_2025, idx) * g10
    s3_10_by_tab = {sh: _repeat_monthly_profile(ser, idx) * g10 for sh, ser in s3_bau_2025_by_tab.items()}
    scenarios["BAU_Growth10"] = {"Scope 1": s1_10, "Scope 2": s2_10, **s3_10_by_tab}

    # New clients & accelerated regional expansion (proxy model):
    # - faster overall growth
    # - more activity in "dirty regions" -> increases shipping/manufacturing related emissions
    g_exp = _growth_factor(idx, annual_growth=cfg.annual_growth_expansion, base_month=base_month)
    s1_exp = _repeat_monthly_profile(s1_bau_2025, idx) * g_exp
    s2_exp = _repeat_monthly_profile(s2_bau_2025, idx) * g_exp * float(cfg.expansion_scope2_dc_multiplier)
    s3_exp_by_tab = {sh: _repeat_monthly_profile(ser, idx) * g_exp for sh, ser in s3_bau_2025_by_tab.items()}
    # Apply multipliers on top of growth for impacted categories (proxies)
    if S3CAT1_SHEET in s3_exp_by_tab:
        s3_exp_by_tab[S3CAT1_SHEET] = s3_exp_by_tab[S3CAT1_SHEET] * float(cfg.expansion_manufacturing_multiplier)
    if "S3 Cat 4 Upstream Transport" in s3_exp_by_tab:
        s3_exp_by_tab["S3 Cat 4 Upstream Transport"] = s3_exp_by_tab["S3 Cat 4 Upstream Transport"] * float(cfg.expansion_shipping_multiplier)
    if "S3 Cat 9 Downstream Transport" in s3_exp_by_tab:
        s3_exp_by_tab["S3 Cat 9 Downstream Transport"] = s3_exp_by_tab["S3 Cat 9 Downstream Transport"] * float(cfg.expansion_shipping_multiplier)
    scenarios["EXPANSION_NewClients_DirtyRegions"] = {"Scope 1": s1_exp, "Scope 2": s2_exp, **s3_exp_by_tab}

    # Assemble tidy monthly output (per scope + total)
    rows = []
    for sc_name, parts in scenarios.items():
        for scope_name, ser in parts.items():
            tmp = pd.DataFrame({"Scenario": sc_name, "Scope": scope_name, "Month": ser.index, "Emissions_tCO2e": ser.values})
            rows.append(tmp)
        tot = parts["Scope 1"].add(parts["Scope 2"], fill_value=0.0)
        for sh in s3_tabs:
            if sh in parts:
                tot = tot.add(parts[sh], fill_value=0.0)
        rows.append(
            pd.DataFrame(
                {
                    "Scenario": sc_name,
                    "Scope": "TOTAL(All additive tabs)",
                    "Month": tot.index,
                    "Emissions_tCO2e": tot.values,
                }
            )
        )

    monthly_all = pd.concat(rows, ignore_index=True)
    monthly_all["Year"] = pd.to_datetime(monthly_all["Month"]).dt.year

    yearly_all = monthly_all.groupby(["Scenario", "Scope", "Year"], dropna=False)["Emissions_tCO2e"].sum().reset_index()

    # Deltas vs BAU for totals
    tot = yearly_all[yearly_all["Scope"] == "TOTAL(All additive tabs)"].copy()
    pivot = tot.pivot_table(index="Year", columns="Scenario", values="Emissions_tCO2e", aggfunc="sum")
    if "BAU" in pivot.columns:
        for c in list(pivot.columns):
            if c == "BAU":
                continue
            pivot[f"Delta_vs_BAU__{c}"] = pivot[c] - pivot["BAU"]
    pivot = pivot.reset_index()

    meta = pd.DataFrame(
        [
            {"key": "input_window_workbook", "value": str(window_path)},
            {"key": "start_year", "value": str(cfg.start_year)},
            {"key": "end_year", "value": str(cfg.end_year)},
            {"key": "rollout_start", "value": str(cfg.rollout_start)},
            {"key": "rollout_end", "value": str(cfg.rollout_end)},
            {"key": "annual_growth_default", "value": str(cfg.annual_growth_default)},
            {"key": "annual_growth_10pct", "value": str(cfg.annual_growth_10pct)},
            {"key": "annual_growth_expansion", "value": str(cfg.annual_growth_expansion)},
            {"key": "expansion_shipping_multiplier", "value": str(cfg.expansion_shipping_multiplier)},
            {"key": "expansion_manufacturing_multiplier", "value": str(cfg.expansion_manufacturing_multiplier)},
            {"key": "expansion_scope2_dc_multiplier", "value": str(cfg.expansion_scope2_dc_multiplier)},
            {"key": "s3cat1_rules", "value": str(cfg.ef_reduction_by_id or {})},
        ]
    )

    return meta, monthly_all, yearly_all, pivot


def build_user_4_scenarios(
    window_path: Path, cfg: ScenarioConfig
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    User-requested scenarios:
    1) Flat / no growth
    2) Flat + low-carbon steel (100% switch)
    3) Continuous growth (headcount growth + expansion proxies + DC builds additive)
    4) Scenario 3 + decarbonisation levers (steel share ramp + delayed concrete + Scope2 renewable after Year 3 + Scope1 after Year 5)

    IMPORTANT:
    - Scope 1/2 decarb logic is unchanged (we reuse existing factor/adoption pattern).
    - S3 Cat 1 procurement reductions are applied on baseline co2e(t) using dummy_ef_id (NOT ef_value).
    """
    xls = pd.ExcelFile(window_path)
    s1 = pd.read_excel(xls, sheet_name=SCOPE1_SHEET)
    s2 = pd.read_excel(xls, sheet_name=SCOPE2_SHEET)
    s3cat1_df = pd.read_excel(xls, sheet_name=S3CAT1_SHEET)

    s3_tabs = [sh for sh in xls.sheet_names if str(sh).startswith(S3_PREFIX)]
    s3_dfs: Dict[str, pd.DataFrame] = {}
    for sh in s3_tabs:
        try:
            s3_dfs[str(sh)] = pd.read_excel(xls, sheet_name=sh)
        except Exception:
            continue

    # 2025 monthly profiles
    s1_bau_2025, s1_full_2025 = _scope1_full_decarb_monthly_2025(s1)
    s2_bau_2025 = _monthly_sum_2025(s2)

    if DUMMY_EF_ID_COL not in s3cat1_df.columns:
        raise RuntimeError(
            f"S3 Cat 1 is missing column '{DUMMY_EF_ID_COL}'. "
            "Run the dummy step first (Run_Everything writes a *_DUMMY_EFID.xlsx output)."
        )

    steel_ids = tuple(getattr(cfg, "steel_dummy_ids", ()) or ())
    conc_ids = tuple(getattr(cfg, "concrete_dummy_ids", ()) or ())
    steel_2025, conc_2025, other_2025 = _s3cat1_split_monthly_2025_by_dummy_groups(
        s3cat1_df,
        dummy_col=DUMMY_EF_ID_COL,
        steel_ids=steel_ids,
        concrete_ids=conc_ids,
    )

    s3_bau_2025_by_tab: Dict[str, pd.Series] = {}
    for sh, df in s3_dfs.items():
        if sh == S3CAT1_SHEET:
            s3_bau_2025_by_tab[sh] = steel_2025.add(conc_2025, fill_value=0.0).add(other_2025, fill_value=0.0)
        else:
            s3_bau_2025_by_tab[sh] = _monthly_sum_2025(df)

    idx = pd.date_range(pd.Timestamp(cfg.start_year, 1, 1), pd.Timestamp(cfg.end_year, 12, 1), freq="MS")
    base_month = pd.Timestamp(cfg.start_year, 1, 1)

    def repeat(ser2025: pd.Series) -> pd.Series:
        return _repeat_monthly_profile(ser2025, idx)

    # Baselines
    s1_base = repeat(s1_bau_2025)
    s2_base = repeat(s2_bau_2025)
    s3_base_by_tab = {sh: repeat(ser) for sh, ser in s3_bau_2025_by_tab.items()}

    def base_parts() -> Dict[str, pd.Series]:
        parts: Dict[str, pd.Series] = {"Scope 1": s1_base, "Scope 2": s2_base}
        for sh, ser in s3_base_by_tab.items():
            parts[sh] = ser
        return parts

    def apply_growth(parts: Dict[str, pd.Series], g: np.ndarray) -> Dict[str, pd.Series]:
        return {k: (v * g) for k, v in parts.items()}

    def add_dc_build(parts: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
        out = {k: v.copy() for k, v in parts.items()}
        year = int(getattr(cfg, "dc_build_year", 2026))
        tab = str(getattr(cfg, "dc_build_target_tab", "Scope 3 Category 2 Capital Good"))
        n = int(getattr(cfg, "dc_build_count_year1", 10))
        each = float(getattr(cfg, "dc_build_tco2e_each", 7066.3))
        total = float(n) * float(each)
        if tab not in out:
            out[tab] = pd.Series(np.zeros(len(idx), dtype=float), index=idx)
        mask = idx.year == year
        if mask.any():
            out[tab].loc[mask] = out[tab].loc[mask].to_numpy(dtype=float) + (total / 12.0)
        return out

    def apply_epod_office_proxies(parts: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
        out = {k: v.copy() for k, v in parts.items()}
        total_fact = int(getattr(cfg, "epod_factories_total", 4))
        per_year = max(1, int(getattr(cfg, "epod_factories_build_per_year", 1)))
        inc_s2 = float(getattr(cfg, "epod_factory_increment_frac_scope2", 0.10))
        inc_s3 = float(getattr(cfg, "epod_factory_increment_frac_s3cat1", 0.10))
        offices_per_year = int(getattr(cfg, "new_offices_per_year", 3))
        inc_off_s2 = float(getattr(cfg, "office_increment_frac_scope2_each", 0.01))
        inc_off_s3 = float(getattr(cfg, "office_increment_frac_s3cat1_each", 0.01))

        first_build_year = int(cfg.start_year) + 1  # projection starts 2026 for default cfg.start_year=2025
        mult_s2 = np.ones(len(idx), dtype=float)
        mult_s3 = np.ones(len(idx), dtype=float)
        for i, d in enumerate(idx):
            y = int(d.year)
            years_of_builds = max(0, y - first_build_year + 1)
            factories_built = min(total_fact, years_of_builds * per_year)
            # ramp: factory contributes from year after build (simple deterministic proxy)
            factories_online = max(0, factories_built - per_year)
            offices_built = max(0, years_of_builds * offices_per_year)
            mult_s2[i] += factories_online * inc_s2 + offices_built * inc_off_s2
            mult_s3[i] += factories_online * inc_s3 + offices_built * inc_off_s3

        if "Scope 2" in out:
            out["Scope 2"] = out["Scope 2"] * mult_s2
        if S3CAT1_SHEET in out:
            out[S3CAT1_SHEET] = out[S3CAT1_SHEET] * mult_s3
        return out

    def apply_s3cat1_procurement_switch(
        parts: Dict[str, pd.Series], *, steel_share: np.ndarray, conc_share: np.ndarray
    ) -> Dict[str, pd.Series]:
        out = {k: v.copy() for k, v in parts.items()}

        steel_ser = repeat(steel_2025)
        conc_ser = repeat(conc_2025)
        other_ser = repeat(other_2025)
        raw_total = steel_ser.add(conc_ser, fill_value=0.0).add(other_ser, fill_value=0.0)
        target_total = out.get(S3CAT1_SHEET, raw_total).copy()

        ratio = np.ones(len(idx), dtype=float)
        b = raw_total.to_numpy(dtype=float)
        m = b > 0
        ratio[m] = target_total.to_numpy(dtype=float)[m] / b[m]
        steel_ser = steel_ser * ratio
        conc_ser = conc_ser * ratio
        other_ser = other_ser * ratio

        steel_mult = 1.0 - steel_share * float(getattr(cfg, "steel_reduction", 0.40))
        conc_mult = 1.0 - conc_share * float(getattr(cfg, "concrete_reduction", 0.30))
        steel_mult = np.clip(steel_mult, 0.0, 1.0)
        conc_mult = np.clip(conc_mult, 0.0, 1.0)

        cat1 = (steel_ser.to_numpy(dtype=float) * steel_mult) + (conc_ser.to_numpy(dtype=float) * conc_mult) + other_ser.to_numpy(dtype=float)
        out[S3CAT1_SHEET] = pd.Series(cat1, index=idx).astype(float)
        return out

    # Scope 1/2 decarb factors (existing logic)
    def factor(full: pd.Series, base: pd.Series) -> pd.Series:
        b = base.to_numpy(dtype=float)
        f = full.to_numpy(dtype=float)
        out = np.ones_like(b, dtype=float)
        m = b > 0
        out[m] = f[m] / b[m]
        s = pd.Series(out, index=base.index, name="factor")
        return s.clip(lower=0.0, upper=10.0)

    s1_fac = repeat(factor(s1_full_2025, s1_bau_2025))
    s2_fac = repeat(pd.Series(np.zeros(12, dtype=float), index=s2_bau_2025.index))  # full renewable => 0

    def adoption(start_year_offset: int) -> np.ndarray:
        start = pd.Timestamp(int(cfg.start_year) + 1 + int(start_year_offset), 1, 1)
        end = pd.Timestamp(int(cfg.end_year), 1, 1)
        return _linear_ramp(idx, start=start, end=end, start_value=0.0, end_value=1.0)

    def apply_adoption(base: pd.Series, fac: pd.Series, adopt: np.ndarray) -> pd.Series:
        return base * (1.0 - adopt * (1.0 - fac.to_numpy(dtype=float)))

    def adoption_global() -> np.ndarray:
        # Use the same rollout window as the legacy combined scenarios entrypoint.
        return _linear_ramp(
            idx,
            start=pd.Timestamp(cfg.rollout_start),
            end=pd.Timestamp(cfg.rollout_end),
            start_value=0.0,
            end_value=1.0,
        )

    # Shares
    steel_share_by_year = cfg.steel_share_by_year or _shares_default(cfg.start_year)
    concrete_share_by_year = cfg.concrete_share_by_year or {cfg.end_year: 0.20}
    steel_share = _share_series(idx, steel_share_by_year)
    conc_share = _share_series(idx, concrete_share_by_year)

    g_flat = _growth_factor(idx, annual_growth=0.0, base_month=base_month)
    g_hc = _growth_factor(idx, annual_growth=float(getattr(cfg, "headcount_growth_yoy", 0.30)), base_month=base_month)

    scenarios: Dict[str, Dict[str, pd.Series]] = {}

    # Scenario 1
    p1 = apply_growth(base_parts(), g_flat)
    scenarios["Scenario_1_Flat_NoGrowth"] = p1

    # Scenario 2 (100% green steel)
    p2 = apply_growth(base_parts(), g_flat)
    p2 = apply_s3cat1_procurement_switch(p2, steel_share=np.ones(len(idx), dtype=float), conc_share=np.zeros(len(idx), dtype=float))
    scenarios["Scenario_2_Flat_LowCarbonSteel"] = p2

    # Scenario 3
    p3 = apply_growth(base_parts(), g_hc)
    p3 = apply_epod_office_proxies(p3)
    p3 = add_dc_build(p3)
    scenarios["Scenario_3_ContinuousGrowth"] = p3

    # Scenario 4
    p4 = {k: v.copy() for k, v in p3.items()}
    p4["Scope 2"] = apply_adoption(p4["Scope 2"], s2_fac, adoption(2))  # after Year 3
    p4["Scope 1"] = apply_adoption(p4["Scope 1"], s1_fac, adoption(4))  # after Year 5
    p4 = apply_s3cat1_procurement_switch(p4, steel_share=steel_share, conc_share=conc_share)
    scenarios["Scenario_4_GrowthPlusDecarb"] = p4

    # Extra scenarios (legacy standalone scope decarb) – requested:
    # Scope 1 only / Scope 2 only, using the global rollout window (cfg.rollout_start..cfg.rollout_end).
    p5 = apply_growth(base_parts(), g_flat)
    p5["Scope 1"] = apply_adoption(p5["Scope 1"], s1_fac, adoption_global())
    scenarios["Scenario_5_Flat_Scope1DecarbOnly"] = p5

    p6 = apply_growth(base_parts(), g_flat)
    p6["Scope 2"] = apply_adoption(p6["Scope 2"], s2_fac, adoption_global())
    scenarios["Scenario_6_Flat_Scope2RenewableOnly"] = p6

    # Assemble outputs
    rows = []
    for sc_name, parts in scenarios.items():
        for scope_name, ser in parts.items():
            rows.append(pd.DataFrame({"Scenario": sc_name, "Scope": scope_name, "Month": ser.index, "Emissions_tCO2e": ser.values}))

        tot = parts["Scope 1"].add(parts["Scope 2"], fill_value=0.0)
        for sh in s3_tabs:
            if sh in parts:
                tot = tot.add(parts[sh], fill_value=0.0)
        rows.append(pd.DataFrame({"Scenario": sc_name, "Scope": "TOTAL(All additive tabs)", "Month": tot.index, "Emissions_tCO2e": tot.values}))

    monthly_all = pd.concat(rows, ignore_index=True)
    monthly_all["Year"] = pd.to_datetime(monthly_all["Month"]).dt.year
    yearly_all = monthly_all.groupby(["Scenario", "Scope", "Year"], dropna=False)["Emissions_tCO2e"].sum().reset_index()

    tot = yearly_all[yearly_all["Scope"] == "TOTAL(All additive tabs)"].copy()
    pivot = tot.pivot_table(index="Year", columns="Scenario", values="Emissions_tCO2e", aggfunc="sum")
    base_col = "Scenario_1_Flat_NoGrowth"
    if base_col in pivot.columns:
        for c in list(pivot.columns):
            if c == base_col:
                continue
            pivot[f"Delta_vs_{base_col}__{c}"] = pivot[c] - pivot[base_col]
    pivot = pivot.reset_index()

    meta = pd.DataFrame(
        [
            {"key": "input_window_workbook", "value": str(window_path)},
            {"key": "start_year", "value": str(cfg.start_year)},
            {"key": "end_year", "value": str(cfg.end_year)},
            {"key": "headcount_growth_yoy", "value": str(getattr(cfg, "headcount_growth_yoy", 0.30))},
            {"key": "steel_dummy_ids", "value": str(steel_ids)},
            {"key": "concrete_dummy_ids", "value": str(conc_ids)},
            {"key": "steel_reduction", "value": str(getattr(cfg, "steel_reduction", 0.40))},
            {"key": "concrete_reduction", "value": str(getattr(cfg, "concrete_reduction", 0.30))},
            {"key": "steel_share_by_year", "value": str(steel_share_by_year)},
            {"key": "concrete_share_by_year", "value": str(concrete_share_by_year)},
            {"key": "dc_build_count_year1", "value": str(getattr(cfg, "dc_build_count_year1", 10))},
            {"key": "dc_build_tco2e_each", "value": str(getattr(cfg, "dc_build_tco2e_each", 7066.3))},
            {"key": "dc_build_target_tab", "value": str(getattr(cfg, "dc_build_target_tab", ""))},
        ]
    )
    return meta, monthly_all, yearly_all, pivot


def run(
    output_name: Optional[str] = None,
    input_window: Optional[str] = None,
    *,
    user_scenarios: bool = False,
    annual_growth_default: float = 0.0,
    annual_growth_10pct: float = 0.10,
    annual_growth_expansion: float = 0.15,
    expansion_shipping_multiplier: float = 1.35,
    expansion_manufacturing_multiplier: float = 1.15,
    expansion_scope2_dc_multiplier: float = 1.20,
    rollout_start: str = "2026-01-01",
    rollout_end: str = "2030-01-01",
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    wp = Path(str(input_window)).expanduser() if str(input_window or "").strip() else _find_latest_window_workbook(BASE_DIR)
    if wp is None or (not wp.exists()):
        raise RuntimeError("No mapped_results_window_*.xlsx found under output/.")

    cfg = ScenarioConfig(
        start_year=2025,
        end_year=2030,
        rollout_start=pd.Timestamp(str(rollout_start)),
        rollout_end=pd.Timestamp(str(rollout_end)),
        annual_growth_default=float(annual_growth_default),
        annual_growth_10pct=float(annual_growth_10pct),
        annual_growth_expansion=float(annual_growth_expansion),
        expansion_shipping_multiplier=float(expansion_shipping_multiplier),
        expansion_manufacturing_multiplier=float(expansion_manufacturing_multiplier),
        expansion_scope2_dc_multiplier=float(expansion_scope2_dc_multiplier),
        ef_reduction_by_id={"300S005": 0.30, "300S014": 0.40},
    )

    if bool(user_scenarios):
        meta, monthly_all, yearly_all, pivot = build_user_4_scenarios(wp, cfg)
    else:
        meta, monthly_all, yearly_all, pivot = build_scenarios(wp, cfg)

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / (output_name or f"decarb_scenarios_2025_2030_{ts}.xlsx")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        yearly_all.to_excel(writer, sheet_name="Yearly", index=False)
        monthly_all.to_excel(writer, sheet_name="Monthly", index=False)
        pivot.to_excel(writer, sheet_name="Delta_vs_BAU", index=False)

    print(f"[info] Wrote scenarios -> {out_path}")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Combined decarbonization scenarios (2025-2030) using implemented rules.")
    ap.add_argument("--output", help="Optional output filename (xlsx)")
    ap.add_argument("--input", help="Optional window workbook path (xlsx). If omitted, uses latest under output/.")
    ap.add_argument("--user-scenarios", action="store_true", help="If set: generate the 4 requested user scenarios.")
    ap.add_argument("--growth-bau", type=float, default=0.0, help="BAU annual growth (default: 0.0)")
    ap.add_argument("--growth-10", type=float, default=0.10, help="BAU_Growth10 annual growth (default: 0.10)")
    ap.add_argument("--growth-exp", type=float, default=0.15, help="Expansion annual growth (default: 0.15)")
    ap.add_argument("--exp-shipping-mult", type=float, default=1.35, help="Expansion shipping multiplier for S3 Cat 4/9 (default: 1.35)")
    ap.add_argument("--exp-manufacturing-mult", type=float, default=1.15, help="Expansion multiplier for S3 Cat 1 (default: 1.15)")
    ap.add_argument("--exp-scope2dc-mult", type=float, default=1.20, help="Expansion multiplier for Scope 2 DC electricity (default: 1.20)")
    ap.add_argument("--rollout-start", default="2026-01-01", help="Decarb rollout start date (YYYY-MM-01). Default: 2026-01-01")
    ap.add_argument("--rollout-end", default="2030-01-01", help="Decarb rollout end date (YYYY-MM-01). Default: 2030-01-01")
    args = ap.parse_args()
    run(
        args.output,
        input_window=str(getattr(args, "input", "") or ""),
        user_scenarios=bool(getattr(args, "user_scenarios", False)),
        annual_growth_default=float(getattr(args, "growth_bau", 0.0) or 0.0),
        annual_growth_10pct=float(getattr(args, "growth_10", 0.10) or 0.10),
        annual_growth_expansion=float(getattr(args, "growth_exp", 0.15) or 0.15),
        expansion_shipping_multiplier=float(getattr(args, "exp_shipping_mult", 1.35) or 1.35),
        expansion_manufacturing_multiplier=float(getattr(args, "exp_manufacturing_mult", 1.15) or 1.15),
        expansion_scope2_dc_multiplier=float(getattr(args, "exp_scope2dc_mult", 1.20) or 1.20),
        rollout_start=str(getattr(args, "rollout_start", "2026-01-01") or "2026-01-01"),
        rollout_end=str(getattr(args, "rollout_end", "2030-01-01") or "2030-01-01"),
    )


if __name__ == "__main__":
    main()

