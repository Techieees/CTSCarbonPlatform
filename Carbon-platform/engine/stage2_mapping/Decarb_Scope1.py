from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple
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


SCOPE1_SHEET = "Scope 1"
DATE_COL = "Date"
COUNTRY_COL = "country"
VEHICLE_COL = "Vehicle Type"
SPEND_COL = "Spend_Euro"
EF_COL = "ef_value"
EF_UNIT_COL = "ef_unit"
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


def _norm_country(x: object) -> str:
    s = "" if x is None else str(x).strip()
    return s


def _pick_electricity_ef(country: str) -> Tuple[float, str]:
    c = _norm_country(country)
    if c in ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY:
        return float(ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY[c]), c
    # Common fallbacks
    if c.lower() in {"eu", "european union"} and "EU_average" in ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY:
        return float(ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY["EU_average"]), "EU_average"
    if "Global" in ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY:
        return float(ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY["Global"]), "Global"
    # ultimate fallback
    return float("nan"), ""


@dataclass(frozen=True)
class Scope1DecarbConfig:
    year: int = 2025


def run_scope1_decarb(window_path: Optional[str] = None, cfg: Optional[Scope1DecarbConfig] = None) -> Path:
    cfg = cfg or Scope1DecarbConfig()
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

    df = pd.read_excel(wp, sheet_name=SCOPE1_SHEET)
    if df is None or df.empty:
        raise RuntimeError(f"Sheet is empty: {SCOPE1_SHEET}")

    # Normalize required columns
    for col in [DATE_COL, COUNTRY_COL, VEHICLE_COL, SPEND_COL, EF_COL, CO2_COL]:
        if col not in df.columns:
            raise RuntimeError(f"Missing column '{col}' in sheet '{SCOPE1_SHEET}'.")

    out = df.copy()
    out[DATE_COL] = pd.to_datetime(out[DATE_COL], errors="coerce")
    out = out.dropna(subset=[DATE_COL])
    out["_m"] = out[DATE_COL].dt.to_period("M").dt.to_timestamp(how="start")

    # Baseline numeric columns
    out[SPEND_COL] = _safe_to_numeric(out[SPEND_COL])
    out[EF_COL] = _safe_to_numeric(out[EF_COL])
    out[CO2_COL] = _safe_to_numeric(out[CO2_COL])

    # Filter year window
    y0 = int(cfg.year)
    out = out[(out["_m"] >= pd.Timestamp(y0, 1, 1)) & (out["_m"] <= pd.Timestamp(y0, 12, 1))].copy()

    # Build decarb EF per row:
    vt = out[VEHICLE_COL].astype(str)
    is_gen_diesel = vt.str.strip().str.lower().eq("generator diesel") | vt.str.lower().str.contains("generator diesel", na=False)

    out["ef_value_new"] = out[EF_COL].astype(float)
    out["ef_source_new"] = ""
    out["decarb_action"] = ""

    # Generator diesel -> HVO100
    out.loc[is_gen_diesel, "ef_value_new"] = float(HVO100_EF_T_PER_EUR)
    out.loc[is_gen_diesel, "ef_source_new"] = "HVO100_EF_T_PER_EUR"
    out.loc[is_gen_diesel, "decarb_action"] = "Generator diesel -> HVO100"

    # Everything else -> EV electricity EF by country (t/EUR)
    idx_other = ~is_gen_diesel
    if idx_other.any():
        picked = out.loc[idx_other, COUNTRY_COL].map(lambda c: _pick_electricity_ef(c)[0])
        picked_src = out.loc[idx_other, COUNTRY_COL].map(lambda c: _pick_electricity_ef(c)[1])
        out.loc[idx_other, "ef_value_new"] = picked.astype(float)
        out.loc[idx_other, "ef_source_new"] = picked_src.astype(str)
        out.loc[idx_other, "decarb_action"] = "Vehicle -> Electric (country grid EF)"

    # Compute new emissions.
    # Primary method: Spend_Euro * ef_value_new (units consistent with current Scope 1 mapping: t CO2e/EUR)
    # Fallback: scale baseline co2e by EF ratio if spend is missing.
    spend = np.asarray(out[SPEND_COL], dtype=float)
    ef_new = np.asarray(out["ef_value_new"], dtype=float)
    ef_old = np.asarray(out[EF_COL], dtype=float)
    co2_old = np.asarray(out[CO2_COL], dtype=float)

    co2_new = np.full_like(co2_old, fill_value=np.nan, dtype=float)
    m_spend = np.isfinite(spend) & np.isfinite(ef_new)
    co2_new[m_spend] = spend[m_spend] * ef_new[m_spend]

    m_ratio = (~m_spend) & np.isfinite(co2_old) & np.isfinite(ef_new) & np.isfinite(ef_old) & (ef_old > 0)
    co2_new[m_ratio] = co2_old[m_ratio] * (ef_new[m_ratio] / ef_old[m_ratio])

    # Final fallback: keep baseline
    m_keep = ~np.isfinite(co2_new)
    co2_new[m_keep] = co2_old[m_keep]

    out["co2e_new (t)"] = co2_new
    out["delta_co2e (t)"] = out["co2e_new (t)"] - out[CO2_COL]
    out["delta_pct"] = np.where(out[CO2_COL].to_numpy(dtype=float) > 0, out["delta_co2e (t)"] / out[CO2_COL], np.nan)

    # Monthly aggregation
    monthly = out.groupby("_m", dropna=False)[[CO2_COL, "co2e_new (t)"]].sum().reset_index().rename(columns={"_m": "Month"})
    monthly["delta (t)"] = monthly["co2e_new (t)"] - monthly[CO2_COL]
    monthly["delta_pct"] = np.where(monthly[CO2_COL].to_numpy(dtype=float) > 0, monthly["delta (t)"] / monthly[CO2_COL], np.nan)

    # Year summary
    yearly = pd.DataFrame(
        [
            {
                "year": y0,
                "baseline_scope1_co2e_t": float(np.nansum(out[CO2_COL].to_numpy(dtype=float))),
                "decarb_scope1_co2e_t": float(np.nansum(out["co2e_new (t)"].to_numpy(dtype=float))),
            }
        ]
    )
    yearly["delta (t)"] = yearly["decarb_scope1_co2e_t"] - yearly["baseline_scope1_co2e_t"]
    yearly["delta_pct"] = np.where(yearly["baseline_scope1_co2e_t"] > 0, yearly["delta (t)"] / yearly["baseline_scope1_co2e_t"], np.nan)

    meta = pd.DataFrame(
        [
            {"key": "input_window_workbook", "value": str(wp)},
            {"key": "sheet", "value": SCOPE1_SHEET},
            {"key": "year", "value": str(y0)},
            {"key": "generator_hvo100_ef_t_per_eur", "value": str(HVO100_EF_T_PER_EUR)},
            {"key": "electricity_ef_dict_size", "value": str(len(ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY))},
            {"key": "note_units", "value": "Assumes Scope 1 mapping uses ef_unit t CO2e/EUR; co2e_new computed via Spend_Euro * ef_value_new."},
        ]
    )

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"scope1_decarb_{y0}_{ts}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        yearly.to_excel(writer, sheet_name="Yearly", index=False)
        monthly.to_excel(writer, sheet_name="Monthly", index=False)
        out.to_excel(writer, sheet_name="Scope1_Rows", index=False)

        # Also write the EF table for transparency
        ef_tbl = pd.DataFrame(
            [{"country": k, "ef_t_per_eur": v} for k, v in ELECTRICITY_EF_T_PER_EUR_BY_COUNTRY.items()]
        ).sort_values("country")
        ef_tbl.to_excel(writer, sheet_name="EV_EF_By_Country", index=False)

    print(f"[info] Scope1 decarb: wrote -> {out_path.name}")
    return out_path


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Scope 1 decarbonization (vehicle electrification + generator HVO100).")
    ap.add_argument("--input", help="Path to mapped_results_window_*.xlsx (optional)")
    ap.add_argument("--year", type=int, default=2025, help="Baseline year to evaluate (default: 2025)")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    run_scope1_decarb(args.input, Scope1DecarbConfig(year=int(args.year)))


if __name__ == "__main__":
    main()

