from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR
from Decarb_S3Cat1 import build_s3_cat1_bau_and_decarb


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR
WINDOW_PATTERN = "mapped_results_window_*.xlsx"
SHEET_NAME = "S3 Cat 1 Purchased G&S"


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


def main() -> Path:
    ap = argparse.ArgumentParser(description="Run Scope 3 Cat 1 BAU vs DECARB (EF reductions).")
    ap.add_argument("--input", help="Path to mapped_results_window_*.xlsx (optional)")
    ap.add_argument("--sheet", default=SHEET_NAME, help=f"Sheet name (default: {SHEET_NAME})")
    ap.add_argument("--year", type=int, default=2025, help="Year filter (optional, default: 2025)")
    args = ap.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    wp: Optional[Path] = Path(args.input) if args.input else None
    if wp is not None:
        if wp.suffix.lower() != ".xlsx":
            wp = wp.with_suffix(".xlsx")
        if not wp.exists():
            wp = None
    if wp is None:
        wp = _find_latest_window_workbook(BASE_DIR)
    if wp is None or (not wp.exists()):
        raise RuntimeError("No mapped_results_window_*.xlsx found under output/. Provide --input.")

    df = pd.read_excel(wp, sheet_name=str(args.sheet))
    if df is None or df.empty:
        raise RuntimeError(f"Sheet is empty: {args.sheet}")

    # Optional: filter by year if a Date column exists
    if "Date" in df.columns and args.year:
        dt = pd.to_datetime(df["Date"], errors="coerce")
        df = df.assign(_dt=dt).dropna(subset=["_dt"])
        df = df[df["_dt"].dt.year == int(args.year)].drop(columns=["_dt"])

    reductions = {"300S005": 0.30, "300S014": 0.40}
    out = build_s3_cat1_bau_and_decarb(df, ef_reduction_by_id=reductions)

    # Quick summary for stakeholders
    summary = (
        out.groupby(["Scenario"], dropna=False)["Emissions_tCO2e"]
        .sum()
        .reset_index()
        .rename(columns={"Emissions_tCO2e": "Total_Emissions_tCO2e"})
    )
    by_ef = (
        out.groupby(["Scenario", "ef_id"], dropna=False)["Emissions_tCO2e"]
        .sum()
        .reset_index()
        .rename(columns={"Emissions_tCO2e": "Total_Emissions_tCO2e"})
        .sort_values(["Scenario", "Total_Emissions_tCO2e"], ascending=[True, False])
    )

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"s3cat1_decarb_{ts}.xlsx"
    meta = pd.DataFrame(
        [
            {"key": "input_window_workbook", "value": str(wp)},
            {"key": "sheet", "value": str(args.sheet)},
            {"key": "year_filter", "value": str(args.year) if ("Date" in df.columns and args.year) else ""},
            {"key": "rule_300S005_concrete_reduction", "value": "0.30"},
            {"key": "rule_300S014_steel_reduction", "value": "0.40"},
        ]
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)
        by_ef.to_excel(writer, sheet_name="By_EF_ID", index=False)
        out.to_excel(writer, sheet_name="Rows_Stacked", index=False)

    print(f"[info] S3 Cat 1 decarb: wrote -> {out_path}")
    return out_path


if __name__ == "__main__":
    main()

