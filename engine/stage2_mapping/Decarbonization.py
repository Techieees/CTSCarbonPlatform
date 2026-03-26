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


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR


WINDOW_PATTERN = "mapped_results_window_*.xlsx"
STACKED_MONTHS_SHEET = "Company Stacked Months Window"
MONTH_COL = "Month"
TOTAL_COL = "Row Total (t)"

# Common date column candidates in raw tabs
DATE_CANDIDATES = ["Date", "date", "Reporting period (month, year)", "Reporting Period", "Reporting_Month", "Month"]
CO2_COL = "co2e (t)"

# Columns in STACKED_MONTHS_SHEET we treat as the “emissions basket”
# (Water and share columns are excluded).
EXCLUDE_COLS = {MONTH_COL, "Company", "Water", "Company Share in Total (%)", "Company Share in Month (%)"}
EXCLUDE_COLS.add(TOTAL_COL)


def _detect_date_col(df: pd.DataFrame) -> Optional[str]:
    for c in DATE_CANDIDATES:
        if c in df.columns:
            return c
    return None


def _safe_to_numeric(series: pd.Series) -> pd.Series:
    try:
        if series is None:
            return pd.Series(dtype="float64")
        # If duplicate columns are selected, pandas returns a DataFrame.
        # In that case, convert each column and sum row-wise.
        if isinstance(series, pd.DataFrame):
            cols = []
            for c in series.columns:
                cols.append(_safe_to_numeric(series[c]).fillna(0.0))
            if not cols:
                return pd.Series(dtype="float64")
            out = cols[0].copy()
            for s in cols[1:]:
                out = out.add(s, fill_value=0.0)
            return pd.to_numeric(out, errors="coerce")
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


def _load_2025_monthly_basket(window_path: Path) -> Tuple[pd.DataFrame, List[str]]:
    """
    Reads STACKED_MONTHS_SHEET and returns:
      - df_2025: one row per month (2025-01..2025-12), columns = categories + TOTAL_COL
      - basket_cols: list of category columns (excluding Month/Company/Water/shares)
    """
    df = pd.read_excel(window_path, sheet_name=STACKED_MONTHS_SHEET)
    if df is None or df.empty:
        raise RuntimeError(f"Sheet is empty: {STACKED_MONTHS_SHEET}")
    if MONTH_COL not in df.columns:
        raise RuntimeError(f"Missing column {MONTH_COL} in {STACKED_MONTHS_SHEET}")
    if TOTAL_COL not in df.columns:
        raise RuntimeError(f"Missing column {TOTAL_COL} in {STACKED_MONTHS_SHEET}")

    df = df.copy()
    df[MONTH_COL] = pd.to_datetime(df[MONTH_COL], errors="coerce")
    df = df.dropna(subset=[MONTH_COL])
    df["_m"] = df[MONTH_COL].dt.to_period("M").dt.to_timestamp(how="start")

    # Determine basket columns
    basket_cols = [c for c in df.columns if c not in EXCLUDE_COLS and c != "_m"]
    # Keep only numeric-ish columns
    basket_cols = [c for c in basket_cols if pd.api.types.is_numeric_dtype(df[c]) or df[c].dtype == object]
    # Deduplicate while preserving order (some inputs can contain duplicate column names)
    basket_cols = list(dict.fromkeys([str(c) for c in basket_cols]))
    if not basket_cols:
        raise RuntimeError("No basket columns found in stacked months sheet.")

    # Build tmp explicitly to handle duplicated column names safely.
    tmp = df[["_m"]].copy()
    for c in [TOTAL_COL] + basket_cols:
        tmp[c] = _safe_to_numeric(df[c]).fillna(0.0)
    g = tmp.groupby("_m", dropna=False)[[TOTAL_COL] + basket_cols].sum().sort_index()

    df_2025 = g[(g.index >= pd.Timestamp("2025-01-01")) & (g.index <= pd.Timestamp("2025-12-01"))].copy()
    if len(df_2025) != 12:
        # we still continue, but warn via exception message for visibility
        raise RuntimeError(f"Expected 12 months for 2025, got {len(df_2025)}. Check input window.")

    df_2025.index.name = "Month"
    return df_2025.reset_index(), basket_cols


def _load_2025_monthly_basket_from_additive_tabs(window_path: Path) -> Tuple[pd.DataFrame, List[str]]:
    """
    Build the 2025 monthly basket directly from the "full dataset" tabs that are additive:
      - Scope 1, Scope 2, and all "S3 Cat ..." tabs.

    Output columns become:
      - one column per tab name (e.g., "Scope 1", "S3 Cat 1 Purchased G&S", ...)
      - TOTAL_COL as sum of those columns
    """
    xls = pd.ExcelFile(window_path)
    include_prefixes = ("Scope ", "S3 Cat ")
    series_by_tab: Dict[str, pd.Series] = {}

    for sheet_name in xls.sheet_names:
        if not str(sheet_name).startswith(include_prefixes):
            continue
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        dcol = _detect_date_col(df)
        if not dcol or CO2_COL not in df.columns:
            continue
        dt = pd.to_datetime(df[dcol], errors="coerce")
        tmp = pd.DataFrame({"dt": dt, "val": _safe_to_numeric(df[CO2_COL]).fillna(0.0)})
        tmp = tmp.dropna(subset=["dt"])
        if tmp.empty:
            continue
        tmp["m"] = tmp["dt"].dt.to_period("M").dt.to_timestamp(how="start")
        mser = tmp.groupby("m", dropna=False)["val"].sum().sort_index()
        y2025 = mser[(mser.index >= pd.Timestamp("2025-01-01")) & (mser.index <= pd.Timestamp("2025-12-01"))]
        if len(y2025) == 0:
            continue
        # ensure full 12 months
        idx = pd.date_range(pd.Timestamp("2025-01-01"), pd.Timestamp("2025-12-01"), freq="MS")
        y2025 = y2025.reindex(idx).fillna(0.0)
        y2025.index.name = "Month"
        series_by_tab[str(sheet_name)] = y2025.astype(float)

    if not series_by_tab:
        raise RuntimeError("Could not build basket from additive tabs (Scope 1/2 + S3 Cat ...).")

    df = pd.DataFrame({"Month": pd.date_range("2025-01-01", "2025-12-01", freq="MS")})
    basket_cols = []
    for k, ser in series_by_tab.items():
        df[k] = ser.values
        basket_cols.append(k)

    df[TOTAL_COL] = np.sum(df[basket_cols].to_numpy(dtype=float), axis=1)
    return df, basket_cols


def _build_future_month_index(start_year: int, years: int) -> pd.DatetimeIndex:
    start = pd.Timestamp(start_year, 1, 1)
    return pd.date_range(start, periods=years * 12, freq="MS")


def _repeat_seasonal_profile(df_2025: pd.DataFrame, basket_cols: List[str], future_idx: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Repeat 2025 monthly pattern across future months (by month-of-year).
    """
    base = df_2025.copy()
    base["mo"] = pd.to_datetime(base["Month"]).dt.month
    out = pd.DataFrame({"Month": future_idx})
    out["mo"] = out["Month"].dt.month
    merged = out.merge(base[["mo", TOTAL_COL] + basket_cols], on="mo", how="left")
    merged = merged.drop(columns=["mo"])
    return merged


def _apply_growth(df: pd.DataFrame, *, annual_growth: float, pod_multiplier_end: float, years: int) -> pd.DataFrame:
    """
    Apply smooth growth factors (monthly) to all basket columns and TOTAL_COL:
      - annual_growth: e.g. 0.05 => +5% per year compounded smoothly per month
      - pod_multiplier_end: e.g. 2.0 => pods double by the end of horizon (linear ramp)
    """
    out = df.copy()
    n = len(out)
    if n == 0:
        return out

    # Smooth compounding growth per month
    g = float(annual_growth)
    g = max(g, -0.99)
    months = np.arange(n, dtype=float)
    f_growth = (1.0 + g) ** (months / 12.0)

    # Pod multiplier: linear ramp from 1.0 to pod_multiplier_end over horizon
    p_end = float(pod_multiplier_end)
    p_end = max(p_end, 0.0)
    if years <= 1:
        f_pod = np.ones(n, dtype=float) * p_end
    else:
        f_pod = 1.0 + (p_end - 1.0) * (months / max(1.0, float(n - 1)))

    factor = f_growth * f_pod

    for c in [TOTAL_COL] + [col for col in out.columns if col not in ("Month", TOTAL_COL)]:
        if c == "Month":
            continue
        out[c] = np.asarray(out[c], dtype=float) * factor
    return out


@dataclass(frozen=True)
class Lever:
    name: str
    applies_to_cols: List[str]
    reduction_pct: float  # 0.30 = -30%
    start_year_offset: int = 1  # 1 means starts in Year 2
    ramp_years: int = 3  # linear ramp to full effect
    # If True: scale all emissions (all columns incl total) by the lever multiplier.
    # Useful when slide inputs are expressed as "percent reduction potential of total footprint".
    scale_all: bool = False
    lever_key: str = ""
    notes: str = ""


def _write_levers_template(path: Path) -> None:
    """
    Creates a CSV template for user-defined levers (numbers are examples; replace with your own).
    """
    rows = [
        {
            "lever_key": "S1_fuel_switch",
            "name": "Scope 1 fuel switching (e.g., gas boiler to heat pump, fleet EV)",
            "reduction_pct": 0.10,
            "start_year_offset": 0,
            "ramp_years": 5,
            "scale_all": False,
            "applies_to_cols": "Scope 1",
            "notes": "Replace reduction_pct with your assumption. If modeling by fuel type, extend logic later.",
        },
        {
            "lever_key": "S2_PPA",
            "name": "Scope 2 renewable PPA",
            "reduction_pct": 0.20,
            "start_year_offset": 1,
            "ramp_years": 3,
            "scale_all": False,
            "applies_to_cols": "Scope 2",
            "notes": "",
        },
        {
            "lever_key": "S3_procurement",
            "name": "Scope 3 procurement program",
            "reduction_pct": 0.10,
            "start_year_offset": 2,
            "ramp_years": 3,
            "scale_all": False,
            "applies_to_cols": "S3 Cat 1 Purchased G&S",
            "notes": "",
        },
        {
            "lever_key": "TOTAL_offsets",
            "name": "Offsets applied to total footprint (if you want it as TOTAL-level)",
            "reduction_pct": 0.05,
            "start_year_offset": 1,
            "ramp_years": 1,
            "scale_all": True,
            "applies_to_cols": "",
            "notes": "scale_all=True means it scales all columns incl total (total-level lever).",
        },
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def _load_levers_csv(path: Path) -> List[Lever]:
    df = pd.read_csv(path)
    levers: List[Lever] = []
    for _, r in df.iterrows():
        applies = str(r.get("applies_to_cols", "") or "").strip()
        applies_to = [c.strip() for c in applies.split(",") if c.strip()]
        levers.append(
            Lever(
                lever_key=str(r.get("lever_key", "") or ""),
                name=str(r.get("name", "") or ""),
                applies_to_cols=applies_to,
                reduction_pct=float(r.get("reduction_pct", 0.0) or 0.0),
                start_year_offset=int(r.get("start_year_offset", 0) or 0),
                ramp_years=int(r.get("ramp_years", 1) or 1),
                scale_all=bool(r.get("scale_all", False)),
                notes=str(r.get("notes", "") or ""),
            )
        )
    return levers


def _apply_levers(df: pd.DataFrame, levers: List[Lever], *, start_year: int) -> pd.DataFrame:
    out = df.copy()
    if out.empty or not levers:
        return out

    years_from_start = (out["Month"].dt.year - int(start_year)).astype(int)
    for lev in levers:
        r = float(lev.reduction_pct)
        r = min(max(r, 0.0), 1.0)
        # ramp 0..1
        t = (years_from_start - int(lev.start_year_offset)).astype(float)
        ramp = np.clip(t / max(1.0, float(lev.ramp_years)), 0.0, 1.0)
        mult = 1.0 - r * ramp.to_numpy(dtype=float)
        if bool(getattr(lev, "scale_all", False)):
            # Scale all component columns so scope contribution stays consistent.
            comp_cols = [c for c in out.columns if c not in ("Month",)]
            for c in comp_cols:
                if c in out.columns and c != "Month":
                    out[c] = np.asarray(out[c], dtype=float) * mult
        else:
            for c in lev.applies_to_cols:
                if c in out.columns:
                    out[c] = np.asarray(out[c], dtype=float) * mult
    # Recompute total from components if possible
    comp_cols = [c for c in out.columns if c not in ("Month", TOTAL_COL)]
    if comp_cols:
        out[TOTAL_COL] = np.sum(out[comp_cols].to_numpy(dtype=float), axis=1)
    return out


def _scenario_outputs(
    df_2025: pd.DataFrame,
    basket_cols: List[str],
    *,
    years: int,
    annual_growth: float,
    pod_multiplier_end: float,
    s3_cat1_multiplier: float = 1.0,
    dirty_region_multiplier: float = 1.0,
    dirty_cols: Optional[List[str]] = None,
    levers: Optional[List[Lever]] = None,
) -> pd.DataFrame:
    start_year = 2026
    idx = _build_future_month_index(start_year, years)
    base = _repeat_seasonal_profile(df_2025, basket_cols, idx)

    # Apply growth
    base = _apply_growth(base, annual_growth=annual_growth, pod_multiplier_end=pod_multiplier_end, years=years)

    # Scenario 2: increase Purchased G&S (S3 Cat 1)
    if "S3 Cat 1 Purchased G&S" in base.columns:
        base["S3 Cat 1 Purchased G&S"] = np.asarray(base["S3 Cat 1 Purchased G&S"], dtype=float) * float(s3_cat1_multiplier)

    # Scenario 3: dirty-region effect as intensity multiplier on selected columns (shipping/manufacturing proxies)
    if dirty_cols is None:
        dirty_cols = [
            "S3 Cat 4 Upstream Transport",
            "S3 Cat 9 Downstream Transport",
            "S3 Cat 1 Purchased G&S",
            "S3 Cat 12 End of Life",
        ]
    dm = float(dirty_region_multiplier)
    for c in dirty_cols:
        if c in base.columns:
            base[c] = np.asarray(base[c], dtype=float) * dm

    # Apply levers (decarbonization)
    if levers:
        base["Month"] = pd.to_datetime(base["Month"])
        base = _apply_levers(base, levers, start_year=start_year)

    # Recompute total from components if possible
    comp_cols = [c for c in base.columns if c not in ("Month", TOTAL_COL)]
    if comp_cols:
        base[TOTAL_COL] = np.sum(base[comp_cols].to_numpy(dtype=float), axis=1)
    return base


def _yearly_summary(df_monthly: pd.DataFrame) -> pd.DataFrame:
    tmp = df_monthly.copy()
    tmp["Year"] = pd.to_datetime(tmp["Month"]).dt.year
    cols = [c for c in tmp.columns if c not in ("Month", "Year")]
    g = tmp.groupby("Year", dropna=False)[cols].sum().reset_index()
    return g


def run_scenarios(
    *,
    years: int = 5,
    annual_growth: float = 0.0,
    pod_multiplier_end: float = 2.0,
    s3_cat1_multiplier: float = 1.2,
    dirty_region_multiplier: float = 1.15,
    baseline_source: str = "additive_tabs",  # "additive_tabs" | "stacked_months"
    levers_csv: str = "",
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    wp = _find_latest_window_workbook(BASE_DIR)
    if wp is None or (not wp.exists()):
        raise RuntimeError("No mapped_results_window_*.xlsx found under output/.")

    if str(baseline_source).strip().lower() == "stacked_months":
        df_2025, basket_cols = _load_2025_monthly_basket(wp)
    else:
        df_2025, basket_cols = _load_2025_monthly_basket_from_additive_tabs(wp)

    # Common baseline growth assumptions
    common = dict(years=years, annual_growth=annual_growth, pod_multiplier_end=pod_multiplier_end)


    # User-defined levers (we do NOT hardcode slide numbers).
    levers_path = Path(str(levers_csv)).expanduser() if str(levers_csv).strip() else None
    if levers_path is None:
        # Create a template so you can fill it.
        tmpl = OUTPUT_DIR / "levers_template.csv"
        if not tmpl.exists():
            _write_levers_template(tmpl)
        levers: List[Lever] = []
    else:
        if not levers_path.exists():
            raise RuntimeError(f"Levers CSV not found: {levers_path}")
        levers = _load_levers_csv(levers_path)

    scenarios: Dict[str, pd.DataFrame] = {}
    scenarios["S1_BAU"] = _scenario_outputs(df_2025, basket_cols, **common)
    scenarios["S2_Higher_S3Cat1"] = _scenario_outputs(
        df_2025, basket_cols, **common, s3_cat1_multiplier=s3_cat1_multiplier
    )
    common_fast = {**common, "annual_growth": max(float(annual_growth), 0.05)}
    scenarios["S3_Accel_Expansion_DirtyRegions"] = _scenario_outputs(
        df_2025, basket_cols, **common_fast, dirty_region_multiplier=dirty_region_multiplier
    )
    scenarios["S4_DoNothing"] = _scenario_outputs(
        df_2025, basket_cols, **common_fast, dirty_region_multiplier=dirty_region_multiplier
    )
    scenarios["S4_Decarbonization"] = _scenario_outputs(
        df_2025, basket_cols, **common_fast, dirty_region_multiplier=dirty_region_multiplier, levers=levers
    )

    # Additional output: lever impact chart data (do-nothing vs each lever vs combined)
    lever_impacts: Dict[str, pd.DataFrame] = {}
    lever_impacts["Do_nothing"] = scenarios["S4_DoNothing"].copy()
    if levers:
        for lev in levers:
            lever_impacts[f"Lever_{lev.lever_key}_{lev.name}"] = _scenario_outputs(
                df_2025, basket_cols, **common_fast, dirty_region_multiplier=dirty_region_multiplier, levers=[lev]
            )
        lever_impacts["Decarbonization_combined"] = scenarios["S4_Decarbonization"].copy()
    else:
        # no-lever run: still provide same key for chart convenience
        lever_impacts["Decarbonization_combined"] = scenarios["S4_DoNothing"].copy()

    # Combine tidy outputs
    rows = []
    for name, dfm in scenarios.items():
        tmp = dfm.copy()
        tmp.insert(0, "Scenario", name)
        rows.append(tmp)
    monthly_all = pd.concat(rows, ignore_index=True)

    yearly_rows = []
    for name, dfm in scenarios.items():
        yy = _yearly_summary(dfm)
        yy.insert(0, "Scenario", name)
        yearly_rows.append(yy)
    yearly_all = pd.concat(yearly_rows, ignore_index=True)



    lever_yearly_rows = []
    for name, dfm in lever_impacts.items():
        yy = _yearly_summary(dfm)
        yy.insert(0, "Series", name)
        lever_yearly_rows.append(yy)
    lever_yearly_all = pd.concat(lever_yearly_rows, ignore_index=True)

    # Delta table for S4
    delta = yearly_all[yearly_all["Scenario"].isin(["S4_DoNothing", "S4_Decarbonization"])].copy()
    pivot = delta.pivot_table(index="Year", columns="Scenario", values=TOTAL_COL, aggfunc="sum")
    pivot["Delta(t)"] = pivot.get("S4_DoNothing", 0.0) - pivot.get("S4_Decarbonization", 0.0)
    pivot = pivot.reset_index()

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"scenario_results_{ts}.xlsx"
    meta = pd.DataFrame(
        [
            {"key": "input_window_workbook", "value": str(wp)},
            {"key": "years", "value": str(years)},
            {"key": "annual_growth", "value": str(annual_growth)},
            {"key": "pod_multiplier_end", "value": str(pod_multiplier_end)},
            {"key": "s3_cat1_multiplier", "value": str(s3_cat1_multiplier)},
            {"key": "dirty_region_multiplier", "value": str(dirty_region_multiplier)},
            {"key": "baseline_source", "value": str(baseline_source)},
            {"key": "levers_csv", "value": str(levers_path) if levers_path is not None else ""},
        ]
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        df_2025.to_excel(writer, sheet_name="Actual_2025_Profile", index=False)
        monthly_all.to_excel(writer, sheet_name="Scenarios_Monthly", index=False)
        yearly_all.to_excel(writer, sheet_name="Scenarios_Yearly", index=False)
        pivot.to_excel(writer, sheet_name="S4_Delta", index=False)
        lever_yearly_all.to_excel(writer, sheet_name="Lever_Impact_Yearly", index=False)

        levers_df = pd.DataFrame(
            [
                {
                    "lever_key": l.lever_key,
                    "name": l.name,
                    "reduction_pct": l.reduction_pct,
                    "start_year_offset": l.start_year_offset,
                    "ramp_years": l.ramp_years,
                    "scale_all": bool(getattr(l, "scale_all", False)),
                    "applies_to_cols": ", ".join(l.applies_to_cols),
                    "notes": str(getattr(l, "notes", "")),
                }
                for l in levers
            ]
        )
        levers_df.to_excel(writer, sheet_name="Levers", index=False)

    print(f"[info] Scenarios: Wrote scenario workbook -> {out_path.name}")
    return out_path


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Scenario + decarbonization framework (baseline from 2025 monthly profile).")
    ap.add_argument("--years", type=int, default=5, help="Horizon in years (default: 5)")
    ap.add_argument("--annual-growth", type=float, default=0.0, help="Smooth annual growth (e.g., 0.05 = +5 percent)")
    ap.add_argument("--pod-multiplier-end", type=float, default=2.0, help="Pod multiplier at end of horizon (default: 2.0)")
    ap.add_argument("--s3-cat1-multiplier", type=float, default=1.2, help="Scenario 2 multiplier for S3 Cat 1 (default: 1.2)")
    ap.add_argument(
        "--dirty-region-multiplier",
        type=float,
        default=1.15,
        help="Scenario 3/4 intensity multiplier for dirty-region-impacted categories (default: 1.15)",
    )
    ap.add_argument(
        "--baseline-source",
        default="additive_tabs",
        choices=["additive_tabs", "stacked_months"],
        help="Baseline source: additive_tabs (Scope 1/2 + S3 Cat tabs) or stacked_months (Company Stacked Months Window).",
    )
    ap.add_argument(
        "--levers-csv",
        default="",
        help="Path to levers CSV. If omitted, a template is written to output/levers_template.csv and no levers are applied.",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    run_scenarios(
        years=int(args.years),
        annual_growth=float(args.annual_growth),
        pod_multiplier_end=float(args.pod_multiplier_end),
        s3_cat1_multiplier=float(args.s3_cat1_multiplier),
        dirty_region_multiplier=float(args.dirty_region_multiplier),
        baseline_source=str(getattr(args, "baseline_source", "additive_tabs")),
        levers_csv=str(getattr(args, "levers_csv", "")),
    )


if __name__ == "__main__":
    main()

