from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import os
import glob
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR


# Generate a semi-annual (or arbitrary range) emissions report.
# Default range: 2025-01-01 .. 2025-06-30 (inclusive)
# Output: output/semiannual_report_YYYYMMDD_HHMMSS.xlsx

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR


def _find_latest_for_reporting(base_dir: Path) -> Optional[Path]:
    """
    Pick the latest workbook to use as the reporting base.
    Priority order:
      1) mapped_results_by_ghgp_*.xlsx
      2) mapped_results_merged_*.xlsx
      3) mapped_results_merged_dc_*.xlsx
      4) mapped_results_*.xlsx
      5) mapped_results.xlsx
    Exclude helper copies like '*_with_sources_*'.
    """
    out = STAGE2_OUTPUT_DIR
    patterns = [
        str(out / "mapped_results_by_ghgp_*.xlsx"),
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
    filtered = [c for c in candidates if "with_sources" not in os.path.basename(c).lower()]
    if not filtered:
        filtered = candidates
    filtered.sort(key=os.path.getmtime, reverse=True)
    return Path(filtered[0])


def _concat_all_sheets(xls_path: Path) -> pd.DataFrame:
    """Concatenate all sheets into a single DataFrame and add sheet name as 'Sheet_booklets'."""
    try:
        xls = pd.ExcelFile(xls_path)
    except Exception:
        return pd.DataFrame()
    parts: List[pd.DataFrame] = []
    for s in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=s)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        temp = df.copy()
        if "Sheet_booklets" not in temp.columns:
            temp["Sheet_booklets"] = s
        parts.append(temp)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, axis=0, join="outer", ignore_index=True)


def _detect_co2e_column(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        low = str(c).strip().lower()
        if low in {"co2e (t)", "co2e_t", "tco2e"}:
            return c
    for c in df.columns:
        if str(c).strip().lower() == "co2e":
            return c
    return None


def _detect_date_series(df: pd.DataFrame) -> Optional[pd.Series]:
    """Detect the date-like series: 'Date' > 'Reporting period (month, year)' > 'Reporting_Month' > 'Month'."""
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
    # 3) Reporting_Month (Period veya YYYY-MM)
    if "Reporting_Month" in df.columns:
        try:
            ser = df["Reporting_Month"]
            # Could be Period('YYYY-MM') or plain string
            return pd.to_datetime(ser.astype(str), errors="coerce")
        except Exception:
            pass
    # 4) Month (highly variable)
    if "Month" in df.columns:
        try:
            return pd.to_datetime(df["Month"], errors="coerce")
        except Exception:
            pass
    return None


def _autosize_and_style(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    try:
        ws = writer.sheets.get(sheet_name)
        if ws is None:
            return
        wb = writer.book
        header_fmt = wb.add_format({"bold": True, "bg_color": "#D8EAD3", "border": 1})
        dec2_fmt = wb.add_format({"num_format": "0.00"})
        date_fmt = wb.add_format({"num_format": "yyyy-mm-dd"})
        # Headers
        for idx, col in enumerate(list(df.columns)):
            ws.write(0, idx, str(col), header_fmt)
        # Widths
        for idx, col in enumerate(list(df.columns)):
            try:
                series = df[col].astype(str)
                max_len = max([len(str(col))] + series.str.len().tolist())
                width = min(max(8, max_len + 1), 40)
            except Exception:
                width = 16
            low = str(col).strip().lower()
            if low in {"co2e", "co2e (t)", "tco2e_total"}:
                ws.set_column(idx, idx, width, dec2_fmt)
            elif low in {"date", "reporting_month"}:
                ws.set_column(idx, idx, width, date_fmt)
            else:
                ws.set_column(idx, idx, width)
        ws.freeze_panes(1, 0)
    except Exception:
        pass


def generate_report(start_date: str = "2025-01-01", end_date: str = "2025-06-30") -> Optional[Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    base = _find_latest_for_reporting(BASE_DIR)
    if base is None:
        print("No base workbook found in output/.")
        return None

    try:
        df_all = _concat_all_sheets(base)
    except Exception:
        print(f"Failed to read workbook: {base}")
        return None
    if df_all is None or df_all.empty:
        print("Base workbook is empty.")
        return None

    # Detect date and filter
    dt_ser = _detect_date_series(df_all)
    if dt_ser is None:
        print("No date-like column found; cannot filter by date range.")
        return None
    try:
        dt_norm = pd.to_datetime(dt_ser, errors="coerce")
    except Exception:
        dt_norm = pd.to_datetime(dt_ser.astype(str), errors="coerce")

    s_date = pd.to_datetime(start_date)
    e_date = pd.to_datetime(end_date)
    mask = (dt_norm >= s_date) & (dt_norm <= e_date)
    df_f = df_all.loc[mask].copy()
    # Make date visible
    df_f["Date"] = dt_norm.loc[mask].dt.date

    # Detect CO2e column and convert to numeric
    co2e_col = _detect_co2e_column(df_f)
    if co2e_col is None:
        print("No CO2e column found in filtered data.")
        return None
    df_f[co2e_col] = pd.to_numeric(df_f[co2e_col], errors="coerce").fillna(0.0)

    # Detect or create Company field
    company_col = None
    for c in df_f.columns:
        if str(c).strip().lower() == "company":
            company_col = c
            break
    if company_col is None:
        # Derive from Source_file when possible
        src_col = None
        lowmap = {str(c).strip().lower(): c for c in df_f.columns}
        for key in ["source_file", "source file", "sourcefile", "source filename"]:
            if key in lowmap:
                src_col = lowmap[key]
                break
        if src_col is not None and src_col in df_f.columns:
            df_f["Company"] = (
                df_f[src_col].astype(str).str.strip().str.replace(r"(?i)\\.xlsx?$", "", regex=True)
            ).astype("object")
            company_col = "Company"
        else:
            df_f["Company"] = None
            company_col = "Company"

    # GHGP Category column
    ghgp_col = None
    for c in df_f.columns:
        low = str(c).strip().lower()
        if low in {"ghgp category", "ghgp_category", "ghgpcategory"}:
            ghgp_col = c
            break
    if ghgp_col is None:
        ghgp_col = "GHGP Category"
        if ghgp_col not in df_f.columns:
            df_f[ghgp_col] = df_f.get("ghg_category", None)

    # Aggregations
    by_company = (
        df_f.groupby(company_col, dropna=False)[co2e_col].sum(min_count=1).reset_index()
        .rename(columns={co2e_col: "tCO2e_total"})
        .sort_values("tCO2e_total", ascending=False)
        .reset_index(drop=True)
    )
    by_ghgp = (
        df_f.groupby(ghgp_col, dropna=False)[co2e_col].sum(min_count=1).reset_index()
        .rename(columns={co2e_col: "tCO2e_total"})
        .sort_values("tCO2e_total", ascending=False)
        .reset_index(drop=True)
    )
    by_comp_ghgp = (
        df_f.groupby([company_col, ghgp_col], dropna=False)[co2e_col].sum(min_count=1).reset_index()
        .rename(columns={co2e_col: "tCO2e_total"})
        .sort_values(["Company", "tCO2e_total"], ascending=[True, False])
        .reset_index(drop=True)
    )
    # Monthly totals (when possible)
    try:
        dt_for_month = pd.to_datetime(df_f["Date"], errors="coerce")
        df_f["Month"] = dt_for_month.dt.to_period("M").astype(str)
        by_month = (
            df_f.groupby(["Month"], dropna=False)[co2e_col].sum(min_count=1).reset_index()
            .rename(columns={co2e_col: "tCO2e_total"})
            .sort_values("Month", ascending=True)
            .reset_index(drop=True)
        )
    except Exception:
        by_month = pd.DataFrame(columns=["Month", "tCO2e_total"])

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"semiannual_report_{ts}.xlsx"

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        # Meta
        meta = pd.DataFrame(
            {
                "Key": ["Base workbook", "Start date", "End date", "Rows filtered", "CO2e column"],
                "Value": [str(base.name), str(s_date.date()), str(e_date.date()), len(df_f), co2e_col],
            }
        )
        meta.to_excel(writer, sheet_name="Meta", index=False)
        _autosize_and_style(writer, "Meta", meta)

        # Raw filtered rows
        df_f.to_excel(writer, sheet_name="Raw_Filtered", index=False)
        _autosize_and_style(writer, "Raw_Filtered", df_f)

        # Totals
        by_company.to_excel(writer, sheet_name="Totals_by_Company", index=False)
        _autosize_and_style(writer, "Totals_by_Company", by_company)

        by_ghgp.to_excel(writer, sheet_name="Totals_by_GHGP", index=False)
        _autosize_and_style(writer, "Totals_by_GHGP", by_ghgp)

        by_comp_ghgp.to_excel(writer, sheet_name="Totals_by_Company_and_GHGP", index=False)
        _autosize_and_style(writer, "Totals_by_Company_and_GHGP", by_comp_ghgp)

        by_month.to_excel(writer, sheet_name="Totals_by_Month", index=False)
        _autosize_and_style(writer, "Totals_by_Month", by_month)

    print(f"Wrote semi-annual report: {out_path.name}")
    return out_path


def main() -> None:
    # Varsayılan yarıyıl: 2025-01-01..2025-06-30
    generate_report("2025-01-01", "2025-06-30")


if __name__ == "__main__":
    main()

