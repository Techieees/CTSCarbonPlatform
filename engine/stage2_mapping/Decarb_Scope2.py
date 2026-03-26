from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
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


SCOPE2_SHEET = "Scope 2"
DATE_COL = "Date"
SPEND_COL = "Spend_Euro"
EF_COL = "ef_value"
CO2_COL = "co2e (t)"


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


@dataclass(frozen=True)
class Scope2DecarbConfig:
    year: int = 2025
    # Share of electricity covered by renewable PPAs (0..1).
    # 1.0 => fully renewable => EF = 0 for all applicable rows.
    renewable_share: float = 1.0
    # Optional rollout path: project baseline monthly profile to target year
    # and increase renewable share to 1.0 by target year.
    project_to_year: int = 2025
    rollout_start_year: int = 2026
    rollout_start_month: int = 1
    rollout_start_share: float = 0.0
    rollout_end_year: int = 2030
    rollout_end_month: int = 1  # 1 => reach 100% by Jan of end year (so end-year total can be ~0)
    rollout_end_share: float = 1.0
    annual_growth: float = 0.0  # optional baseline growth for Scope 2 projection


def run_scope2_decarb(window_path: Optional[str] = None, cfg: Optional[Scope2DecarbConfig] = None) -> Path:
    cfg = cfg or Scope2DecarbConfig()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    wp: Optional[Path]
    if window_path:
        wp = Path(window_path)
        if wp.suffix.lower() != ".xlsx":
            wp = wp.with_suffix(".xlsx")
        if not wp.exists():
            wp = None
    else:
        wp = None

    if wp is None:
        wp = _find_latest_window_workbook(BASE_DIR)
    if wp is None or (not wp.exists()):
        raise RuntimeError("No mapped_results_window_*.xlsx found under output/.")

    df = pd.read_excel(wp, sheet_name=SCOPE2_SHEET)
    if df is None or df.empty:
        raise RuntimeError(f"Sheet is empty: {SCOPE2_SHEET}")

    for col in [DATE_COL, SPEND_COL, EF_COL, CO2_COL]:
        if col not in df.columns:
            raise RuntimeError(f"Missing column '{col}' in sheet '{SCOPE2_SHEET}'.")

    out = df.copy()
    out[DATE_COL] = pd.to_datetime(out[DATE_COL], errors="coerce")
    out = out.dropna(subset=[DATE_COL])
    out["_m"] = out[DATE_COL].dt.to_period("M").dt.to_timestamp(how="start")

    # Filter baseline year window
    y0 = int(cfg.year)
    out = out[(out["_m"] >= pd.Timestamp(y0, 1, 1)) & (out["_m"] <= pd.Timestamp(y0, 12, 1))].copy()

    out[SPEND_COL] = _safe_to_numeric(out[SPEND_COL])
    out[EF_COL] = _safe_to_numeric(out[EF_COL])
    out[CO2_COL] = _safe_to_numeric(out[CO2_COL])

    # Renewable PPA assumption: EF for renewable-covered share goes to 0.
    s = float(cfg.renewable_share)
    s = 0.0 if not np.isfinite(s) else float(np.clip(s, 0.0, 1.0))

    out["renewable_share"] = s
    # Keep a best-effort EF trace (can be NaN if baseline EF is missing).
    # The emissions computation below does NOT rely on baseline EF completeness.
    out["ef_value_new"] = (1.0 - s) * out[EF_COL].astype(float)  # linear blend where EF exists
    out["decarb_action"] = f"Renewable PPA share={s:g} (EF blended to 0)"

    co2_old = np.asarray(out[CO2_COL], dtype=float)
    # For Scope 2, the cleanest interpretation of "renewable share" is:
    # emissions reduce proportionally, regardless of missing spend / EF columns.
    co2_new = co2_old * float(1.0 - s)

    out["co2e_new (t)"] = co2_new
    out["delta_co2e (t)"] = out["co2e_new (t)"] - out[CO2_COL]
    out["delta_pct"] = np.where(out[CO2_COL].to_numpy(dtype=float) > 0, out["delta_co2e (t)"] / out[CO2_COL], np.nan)

    monthly = out.groupby("_m", dropna=False)[[CO2_COL, "co2e_new (t)"]].sum().reset_index().rename(columns={"_m": "Month"})
    monthly["delta (t)"] = monthly["co2e_new (t)"] - monthly[CO2_COL]
    monthly["delta_pct"] = np.where(monthly[CO2_COL].to_numpy(dtype=float) > 0, monthly["delta (t)"] / monthly[CO2_COL], np.nan)

    yearly = pd.DataFrame(
        [
            {
                "year": y0,
                "baseline_scope2_co2e_t": float(np.nansum(out[CO2_COL].to_numpy(dtype=float))),
                "decarb_scope2_co2e_t": float(np.nansum(out["co2e_new (t)"].to_numpy(dtype=float))),
                "renewable_share": s,
            }
        ]
    )
    yearly["delta (t)"] = yearly["decarb_scope2_co2e_t"] - yearly["baseline_scope2_co2e_t"]
    yearly["delta_pct"] = np.where(yearly["baseline_scope2_co2e_t"] > 0, yearly["delta (t)"] / yearly["baseline_scope2_co2e_t"], np.nan)

    # Build a projection + rollout table (baseline profile repeated by month-of-year)
    target_year = int(getattr(cfg, "project_to_year", y0) or y0)
    proj = pd.DataFrame()
    yearly_proj = pd.DataFrame()
    if target_year > y0:
        base_monthly = monthly[["Month", CO2_COL]].copy()
        base_monthly["mo"] = pd.to_datetime(base_monthly["Month"]).dt.month

        idx = pd.date_range(pd.Timestamp(y0, 1, 1), pd.Timestamp(target_year, 12, 1), freq="MS")
        proj = pd.DataFrame({"Month": idx})
        proj["mo"] = proj["Month"].dt.month
        proj = proj.merge(base_monthly[["mo", CO2_COL]], on="mo", how="left").drop(columns=["mo"])
        proj = proj.rename(columns={CO2_COL: "baseline_co2e (t)"})

        # Optional smooth growth on baseline
        g = float(getattr(cfg, "annual_growth", 0.0) or 0.0)
        g = max(g, -0.99)
        months_ahead = np.arange(len(proj), dtype=float)
        growth_factor = (1.0 + g) ** (months_ahead / 12.0)
        proj["baseline_co2e (t)"] = np.asarray(proj["baseline_co2e (t)"], dtype=float) * growth_factor

        # Rollout renewable share linearly from rollout_start_year/share to rollout_end_year/share
        rs_y = int(getattr(cfg, "rollout_start_year", y0 + 1) or (y0 + 1))
        re_y = int(getattr(cfg, "rollout_end_year", target_year) or target_year)
        rs_mo = int(getattr(cfg, "rollout_start_month", 1) or 1)
        re_mo = int(getattr(cfg, "rollout_end_month", 1) or 1)
        rs_mo = int(np.clip(rs_mo, 1, 12))
        re_mo = int(np.clip(re_mo, 1, 12))
        rs = float(getattr(cfg, "rollout_start_share", 0.0) or 0.0)
        re = float(getattr(cfg, "rollout_end_share", 1.0) or 1.0)
        rs = float(np.clip(rs, 0.0, 1.0))
        re = float(np.clip(re, 0.0, 1.0))

        start_m = pd.Timestamp(rs_y, rs_mo, 1)
        end_m = pd.Timestamp(re_y, re_mo, 1)
        t = (proj["Month"] - start_m).dt.days.astype(float)
        denom = float((end_m - start_m).days) if end_m > start_m else 1.0
        w = np.clip(t.to_numpy(dtype=float) / max(1.0, denom), 0.0, 1.0)
        proj["renewable_share"] = rs + (re - rs) * w
        # Before start_m -> rs; after end_m -> re
        proj.loc[proj["Month"] < start_m, "renewable_share"] = rs
        proj.loc[proj["Month"] >= end_m, "renewable_share"] = re

        proj["decarb_co2e (t)"] = np.asarray(proj["baseline_co2e (t)"], dtype=float) * (1.0 - np.asarray(proj["renewable_share"], dtype=float))
        proj["delta (t)"] = proj["decarb_co2e (t)"] - proj["baseline_co2e (t)"]

        tmpy = proj.copy()
        tmpy["Year"] = pd.to_datetime(tmpy["Month"]).dt.year
        yearly_proj = tmpy.groupby("Year", dropna=False)[["baseline_co2e (t)", "decarb_co2e (t)"]].sum().reset_index()
        yearly_proj["delta (t)"] = yearly_proj["decarb_co2e (t)"] - yearly_proj["baseline_co2e (t)"]
        yearly_proj["delta_pct"] = np.where(
            yearly_proj["baseline_co2e (t)"].to_numpy(dtype=float) > 0,
            yearly_proj["delta (t)"] / yearly_proj["baseline_co2e (t)"],
            np.nan,
        )

    meta = pd.DataFrame(
        [
            {"key": "input_window_workbook", "value": str(wp)},
            {"key": "sheet", "value": SCOPE2_SHEET},
            {"key": "year", "value": str(y0)},
            {"key": "renewable_share", "value": str(s)},
            {"key": "note", "value": "Assumes renewable PPA makes electricity EF=0 for covered share; implemented via ef_value_new=(1-share)*ef_value."},
            {"key": "project_to_year", "value": str(int(getattr(cfg, "project_to_year", y0) or y0))},
            {"key": "rollout_start_year", "value": str(int(getattr(cfg, "rollout_start_year", y0 + 1) or (y0 + 1)))},
            {"key": "rollout_end_year", "value": str(int(getattr(cfg, "rollout_end_year", y0) or y0))},
            {"key": "rollout_start_month", "value": str(int(getattr(cfg, "rollout_start_month", 1) or 1))},
            {"key": "rollout_end_month", "value": str(int(getattr(cfg, "rollout_end_month", 1) or 1))},
            {"key": "rollout_start_share", "value": str(float(getattr(cfg, "rollout_start_share", 0.0) or 0.0))},
            {"key": "rollout_end_share", "value": str(float(getattr(cfg, "rollout_end_share", 1.0) or 1.0))},
            {"key": "annual_growth", "value": str(float(getattr(cfg, "annual_growth", 0.0) or 0.0))},
        ]
    )

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"scope2_decarb_{y0}_{ts}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        yearly.to_excel(writer, sheet_name="Yearly", index=False)
        monthly.to_excel(writer, sheet_name="Monthly", index=False)
        out.to_excel(writer, sheet_name="Scope2_Rows", index=False)
        if proj is not None and (not proj.empty):
            proj.to_excel(writer, sheet_name="Projection_Monthly", index=False)
        if yearly_proj is not None and (not yearly_proj.empty):
            yearly_proj.to_excel(writer, sheet_name="Projection_Yearly", index=False)

    print(f"[info] Scope2 decarb: wrote -> {out_path.name}")
    return out_path


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Scope 2 decarbonization (Renewable PPA -> EF=0).")
    ap.add_argument("--input", help="Path to mapped_results_window_*.xlsx (optional)")
    ap.add_argument("--year", type=int, default=2025, help="Baseline year to evaluate (default: 2025)")
    ap.add_argument(
        "--renewable-share",
        type=float,
        default=1.0,
        help="Renewable PPA coverage share (0..1). 1.0 => fully renewable (EF=0).",
    )
    ap.add_argument("--project-to-year", type=int, default=2030, help="Project baseline profile through this year (default: 2030)")
    ap.add_argument("--rollout-start-year", type=int, default=2026, help="Start year for renewable rollout (default: 2026)")
    ap.add_argument("--rollout-end-year", type=int, default=2030, help="End year for renewable rollout (default: 2030)")
    ap.add_argument("--rollout-start-month", type=int, default=1, help="Start month for rollout (1-12, default: 1=Jan)")
    ap.add_argument("--rollout-end-month", type=int, default=1, help="End month for rollout (1-12, default: 1=Jan)")
    ap.add_argument("--rollout-start-share", type=float, default=0.0, help="Renewable share at rollout start (default: 0.0)")
    ap.add_argument("--rollout-end-share", type=float, default=1.0, help="Renewable share at rollout end (default: 1.0)")
    ap.add_argument("--annual-growth", type=float, default=0.0, help="Optional annual baseline growth during projection (default: 0.0)")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    run_scope2_decarb(
        args.input,
        Scope2DecarbConfig(
            year=int(args.year),
            renewable_share=float(args.renewable_share),
            project_to_year=int(getattr(args, "project_to_year", int(args.year)) or int(args.year)),
            rollout_start_year=int(getattr(args, "rollout_start_year", int(args.year) + 1) or (int(args.year) + 1)),
            rollout_start_month=int(getattr(args, "rollout_start_month", 1) or 1),
            rollout_start_share=float(getattr(args, "rollout_start_share", 0.0) or 0.0),
            rollout_end_year=int(getattr(args, "rollout_end_year", int(args.year)) or int(args.year)),
            rollout_end_month=int(getattr(args, "rollout_end_month", 1) or 1),
            rollout_end_share=float(getattr(args, "rollout_end_share", 1.0) or 1.0),
            annual_growth=float(getattr(args, "annual_growth", 0.0) or 0.0),
        ),
    )


if __name__ == "__main__":
    main()

