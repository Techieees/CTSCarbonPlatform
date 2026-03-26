from __future__ import annotations

import argparse
import os
import glob
from pathlib import Path
import sys
from typing import Dict, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR

"""
Filter the latest Run Everything output to a specified time window
while preserving ALL sheet names and their column order.

Behavior
- Picks the latest mapped workbook produced by the pipeline:
  1) mapped_results_merged_*.xlsx
  2) mapped_results_merged_dc_*.xlsx
  3) mapped_results_*.xlsx
  4) mapped_results.xlsx
  (Excludes '*_with_sources_*' helper copies)
- For each sheet, attempts to detect a date-like column (priority):
  'Date' → 'Reporting period (month, year)' → 'Reporting_Month' → 'Month'
- Rows strictly outside the provided [start_date, end_date] are dropped.
- If no date-like column is found in a sheet, that sheet is written UNCHANGED.
- Sheet names and column orders are preserved.

Usage examples
  py -3 filter_run_output_by_period.py --start 2025-01-01 --months 6
  py -3 filter_run_output_by_period.py --start 2025-01-01 --end 2025-06-30

Notes
- Comments tailored for the Sustainability Data Analyst.
"""

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR


def _find_latest_pipeline_workbook(base_dir: Path) -> Optional[Path]:
    out = STAGE2_OUTPUT_DIR
    patterns = [
        str(out / "mapped_results_by_ghgp_clean_*.xlsx"),
        str(out / "mapped_results_merged_*.xlsx"),
        str(out / "mapped_results_merged_dc_*.xlsx"),
        str(out / "mapped_results_*.xlsx"),
        str(out / "mapped_results.xlsx"),
    ]
    candidates: List[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    # Exclude helper copies + previously window-filtered outputs.
    # Window files match "mapped_results_*.xlsx" and can accidentally become the base.
    filtered: List[str] = []
    for c in candidates:
        base = os.path.basename(c).lower()
        if "with_sources" in base:
            continue
        if base.startswith("mapped_results_window_"):
            continue
        filtered.append(c)
    if not filtered:
        filtered = candidates
    filtered.sort(key=os.path.getmtime, reverse=True)
    return Path(filtered[0])


def _detect_date_series(df: pd.DataFrame) -> Optional[pd.Series]:
    # 1) Date
    for name in ["Date", "date"]:
        if name in df.columns:
            try:
                return pd.to_datetime(df[name], errors="coerce")
            except Exception:
                pass
    # 2) Reporting period (month, year)
    for name in ["Reporting period (month, year)", "Reporting Period"]:
        if name in df.columns:
            try:
                ser = df[name].astype(str)
                # Common formats: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
                dt1 = pd.to_datetime(ser, format="%Y-%m-%d", errors="coerce")
                dt2 = pd.to_datetime(ser, format="%Y-%m-%d %H:%M:%S", errors="coerce")
                dtg = pd.to_datetime(ser, errors="coerce")
                return dt1.combine_first(dt2).combine_first(dtg)
            except Exception:
                pass
    # 3) Reporting_Month
    if "Reporting_Month" in df.columns:
        try:
            ser = df["Reporting_Month"]
            return pd.to_datetime(ser.astype(str), errors="coerce")
        except Exception:
            pass
    # 4) Month
    if "Month" in df.columns:
        try:
            return pd.to_datetime(df["Month"], errors="coerce")
        except Exception:
            pass
    return None


def _compute_end_from_months(start: pd.Timestamp, months: int) -> pd.Timestamp:
    # Inclusive window end = end of day of (start + months - 1 months)'s last day
    # Example: start=2025-01-01, months=6 → end=2025-06-30
    per = (start.to_period("M") + (months - 1)).to_timestamp(how="end")
    # Set to end of day for safety
    return per.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


def _filter_dataframe_by_range(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    dt = _detect_date_series(df)
    if dt is None:
        # No date → leave unchanged (preserve structure)
        return df
    try:
        dt_norm = pd.to_datetime(dt, errors="coerce")
    except Exception:
        dt_norm = pd.to_datetime(dt.astype(str), errors="coerce")
    mask = (dt_norm >= start) & (dt_norm <= end)
    # Preserve column order; do not add/drop columns beyond filtering
    return df.loc[mask].copy()


def _autosize_and_style(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    try:
        ws = writer.sheets.get(sheet_name)
        if ws is None:
            return
        wb = writer.book
        header_fmt = wb.add_format({"bold": True, "bg_color": "#D8EAD3", "border": 1})
        dec2_fmt = wb.add_format({"num_format": "0.00"})
        dec5_fmt = wb.add_format({"num_format": "0.00000"})
        date_fmt = wb.add_format({"num_format": "yyyy-mm-dd"})
        # Headers
        for idx, col in enumerate(list(df.columns)):
            ws.write(0, idx, str(col), header_fmt)
        # Widths + formats
        import pandas as _pd
        sheet_low = str(sheet_name).strip().lower()
        for idx, col in enumerate(list(df.columns)):
            try:
                series = df[col].astype(str)
                max_len = max([len(str(col))] + series.str.len().tolist())
                width = min(max(8, max_len + 1), 40)
            except Exception:
                width = 16
            col_low = str(col).strip().lower()
            try:
                # Date-like columns
                if col_low in {"date", "reporting_month"} or _pd.api.types.is_datetime64_any_dtype(df[col]):
                    ws.set_column(idx, idx, width, date_fmt)
                # CO2e columns: 2 decimals normally, 5 on Waste sheets
                elif col_low in {"co2e", "co2e (t)", "tco2e_total"}:
                    if "waste" in sheet_low:
                        ws.set_column(idx, idx, width, dec5_fmt)
                    else:
                        ws.set_column(idx, idx, width, dec2_fmt)
                # ef_value: 5 decimals
                elif col_low == "ef_value":
                    ws.set_column(idx, idx, width, dec5_fmt)
                # Scope: integer-like (general)
                elif col_low == "scope":
                    ws.set_column(idx, idx, width)
                else:
                    # All other columns: no forced decimal formatting
                    ws.set_column(idx, idx, width)
            except Exception:
                ws.set_column(idx, idx, width)
        ws.freeze_panes(1, 0)
    except Exception:
        pass


def filter_workbook(start_date: str, end_date: Optional[str], months: Optional[int], base_path: Optional[str]) -> Optional[Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Resolve base workbook
    if base_path:
        base = Path(base_path)
    else:
        base = _find_latest_pipeline_workbook(BASE_DIR)
    if base is None or not base.exists():
        print("No suitable workbook found to filter.")
        return None

    # Resolve time window
    s = pd.to_datetime(start_date)
    if months is not None and months > 0:
        e = _compute_end_from_months(s, months)
    else:
        if not end_date:
            print("Either --months or --end must be provided.")
            return None
        e = pd.to_datetime(end_date)

    # Read all sheets
    try:
        xls = pd.ExcelFile(base)
    except Exception:
        print(f"Failed to open workbook: {base}")
        return None

    # Filter each sheet and write output with same sheet names
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"mapped_results_window_{s.date()}_{(months or 'custom')}_{ts}.xlsx"

    # Prefer xlsxwriter (styling), but fall back to openpyxl if not installed.
    try:
        import xlsxwriter as _xlsxwriter  # noqa: F401
        _engine = "xlsxwriter"
    except Exception:
        _engine = "openpyxl"

    with pd.ExcelWriter(
        out_path,
        engine=_engine,
        datetime_format="yyyy-mm-dd",
        date_format="yyyy-mm-dd",
    ) as writer:
        for sheet_name in xls.sheet_names:
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
            except Exception:
                # If sheet cannot be read, write an empty placeholder with same name
                pd.DataFrame().to_excel(writer, sheet_name=sheet_name[:31], index=False)
                continue

            if df is None:
                df = pd.DataFrame()

            filtered = _filter_dataframe_by_range(df, s, e)
            # Ensure 'Date' column is true date (no time component) so Excel doesn't show HH:MM:SS
            try:
                if "Date" in filtered.columns:
                    filtered["Date"] = pd.to_datetime(filtered["Date"], errors="coerce").dt.date
            except Exception:
                pass
            # Write with the original visible name (Excel 31-char limit)
            safe_name = sheet_name[:31] if len(sheet_name) > 31 else sheet_name
            filtered.to_excel(writer, sheet_name=safe_name, index=False)
            _autosize_and_style(writer, safe_name, filtered)

    print(f"Wrote filtered workbook: {out_path.name}")
    print(f"Base: {base.name}")
    print(f"Window: {s.date()} .. {e.date()}")
    return out_path


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Filter Run Everything output by a date window, preserving sheet names."
    )
    # Make arguments OPTIONAL and provide sensible defaults so it can run without args.
    ap.add_argument("--start", help="Start date (YYYY-MM-DD)", default=None)
    g = ap.add_mutually_exclusive_group(required=False)
    g.add_argument("--months", type=int, help="Number of months to include starting at --start (e.g., 6)", default=None)
    g.add_argument("--end", help="End date (YYYY-MM-DD)", default=None)
    ap.add_argument("--base", help="Explicit path to the input workbook (optional)")

    args = ap.parse_args()

    # If no args provided, run with today's requested defaults: 2025-01-01 + 12 months
    if len(sys.argv) == 1:
        args.start = "2025-01-01"
        args.months = 12

    # Fallbacks: ensure start and a window are set
    if args.start is None:
        args.start = "2025-01-01"
    if args.months is None and args.end is None:
        args.months = 12

    return args


def main() -> None:
    args = _parse_args()
    filter_workbook(args.start, args.end, args.months, args.base)


if __name__ == "__main__":
    main()

