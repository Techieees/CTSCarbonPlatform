from __future__ import annotations

import argparse
import glob
import os
import re
from pathlib import Path
import sys
from typing import Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR


# Final cleaning for GHGP workbook: drop specified columns per sheet.
# Writes a cleaned copy under output/mapped_results_by_ghgp_clean_YYYYMMDD_HHMMSS.xlsx

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = STAGE2_OUTPUT_DIR


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).strip().lower())


# ---------- Styling & helpers ----------
def _autosize_and_style(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Apply green headers, auto column widths, date formatting and zebra striping."""
    try:
        ws = writer.sheets.get(sheet_name)
        if ws is None:
            return
        wb = writer.book
        header_fmt = wb.add_format({"bold": True, "bg_color": "#D8EAD3", "border": 1})
        base_fmt = wb.add_format({"border": 1})
        date_fmt = wb.add_format({"num_format": "yyyy-mm-dd", "border": 1})
        dec2_fmt = wb.add_format({"num_format": "0.00", "border": 1})
        dec5_fmt = wb.add_format({"num_format": "0.00000", "border": 1})
        zebra_fmt = wb.add_format({"bg_color": "#F2F2F2", "border": 1})

        # Headers
        for idx, col in enumerate(list(df.columns)):
            ws.write(0, idx, str(col), header_fmt)

        # Width + basic formats
        import pandas as _pd
        sheet_low = str(sheet_name).strip().lower()
        for idx, col in enumerate(list(df.columns)):
            try:
                s = df[col].astype(str)
                width = min(max(8, max([len(str(col))] + s.str.len().tolist()) + 1), 40)
            except Exception:
                width = 16
            col_low = str(col).strip().lower()
            try:
                if col_low in {"date", "reporting_month"} or _pd.api.types.is_datetime64_any_dtype(df[col]):
                    ws.set_column(idx, idx, width, date_fmt)
                elif col_low in {"co2e", "co2e (t)", "tco2e_total"}:
                    if "waste" in sheet_low:
                        ws.set_column(idx, idx, width, dec5_fmt)
                    else:
                        ws.set_column(idx, idx, width, dec2_fmt)
                elif col_low == "ef_value":
                    ws.set_column(idx, idx, width, dec5_fmt)
                elif col_low == "scope":
                    ws.set_column(idx, idx, width, base_fmt)
                else:
                    ws.set_column(idx, idx, width, base_fmt)
            except Exception:
                ws.set_column(idx, idx, width, base_fmt)

        # Zebra striping
        if df.shape[0] > 0 and df.shape[1] > 0:
            ws.conditional_format(1, 0, df.shape[0], df.shape[1] - 1, {
                'type': 'formula',
                'criteria': '=MOD(ROW(),2)=0',
                'format': zebra_fmt,
            })

        # Freeze header row
        ws.freeze_panes(1, 0)
    except Exception:
        pass


def _normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure 'Date' column is real date (no time component)."""
    if df is None or df.empty:
        return df
    try:
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    except Exception:
        pass
    return df


# Per-sheet columns to drop (robust matching: case/space/punct-insensitive)
SHEET_DROP_MAP: Dict[str, List[str]] = {
    "Scope 1": [
        "Data Source (receipt tracker)", "Project", "Unnamed: 6", "subsidiary_name",
        "Unnamed: 5", "Electric vehicle charging geographic grid", "Unnamed: 9",
        "crowd", "supplier_clean", "source_company_clean", "category_clean",
        "co2e", "project start date", "project end date", "project id parent", "Source_File", "scope_category", "ghg_category", "source_id", "Sheet", "Event date", "Product name", "Rented or owned vehicle"
        "Reporting period (month, year)", "release date", "activity volume_1", "Reporting period (month, year)",
    ],
    "Scope 2": [
        "Source_File", "Reporting period (month, year)", "Data Source (bill tracker)",
        "subsidiary_name", "status", "Reporting Period", "mapping_status",
        "attachment date", "release date", "ghg category", "co2 (kg)", "ch4 (kg)",
        "n2o (kg)", "activity volume_1", "emission factor category", "project id",
        "project name", "project number", "project start date", "project end date",
        "project id parent", "source_company", "supplier_clean", "source_company_clean", "source_id","scope_category", "ghg_category",
        "category_clean", "co2e",
    ],
    "S3 Cat 1 Purchased G&S": [
        "Source_File", "Reporting period (month, year)",
        "Purchase Date (Purchase order date or invoice date)",
        "Data Source (invoice tracker)", "Product Code", "Unnamed: 8", "Unnamed: 9",
        "Unnamed: 6", "subsidiary_name", "ghg_category", "mapped_date",
        "Data Source (pension scheme policy documentation trail)", "activity volume_1",
        "Source_file", "co2e", "category_clean", "source_company_clean",
        "supplier_clean", "source_company", "project start date", "project end date", "scope_category", "ghg_category", "source_id",
        "project id parent", "Year", "Month", "Source_File",
    ],
    "S3 Cat 7 Employee Commute": [
        "Source_File", "Reporting period (month, year)", "Link to employee survey",
        "subsidiary_name", "scope_category", "ghg_category", "mapped_date",
        "Spend_Euro", "Reporting_Month", "_src_norm", "source_id", "Sheet",
    ],
    "S3 Cat 5 Waste": [
        "Reporting period (month, year)", "Data Source (waste removal report tracker)",
        "Unnamed: 5", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 11",
        "Unnamed: 12", "Unnamed: 13", "Unnamed: 14", "subsidiary_name",
        "scope_category", "ghg_category", "mapped_date", "Spend_Euro", "ef_unit",
        "ef_source", "emissions_tco2e", "mapping_status", "Source_file", "source_id", "Sheet", "status",
    ],
    "S3 Cat 9 Downstream Transport": [
        "Source_File", "Reporting period (month, year)", "subsidiary_name",
        "scope_category", "ghg_category", "mapped_date", "Spend_Euro", "ef_unit",
        "status", "co2e (t).1", "source_id", "Sheet", "Calculation Method",
    ],
    "S3 Cat 12 End of Life": [
        "Reporting period (month, year)", "subsidiary_name", "scope_category",
        "ghg_category", "mapped_date", "Spend_Euro", "status", "Source_File",
        "source_id", "Sheet", "Data Source (waste removal report tracker or destruction certificate)",
    ],
    "S3 Cat 11 Use of Sold": [
        "Source_File", "Reporting period (month, year)", "subsidiary_name",
        "scope_category", "ghg_category", "mapped_date", "Spend_Euro",
        "match_method", "Data Source (invoice tracker)", "ef_name", "ef_unit",
        "mapping_status", "mapped_by", "ef_value", "ef_source", "emissions_tco2e",
        "ef_value", "ef_source", "emissions_tco2e", "source_id", "Sheet"
    ],
    "S3 Cat 6 Business Travel": [
        "Source_File", "Reporting period (month, year)", "Date duration for travel",
        "Data Source (expense report tracker)", "subsidiary_name", "scope_category",
        "ghg_category", "mapped_date", "status", "attachment date", "release date",
        "ghg category", "activity volume_1", "source_company", "supplier_clean",
        "source_company_clean", "category_clean", "co2e", "Year", "Month",
        "Source_file","Origin Location", "Final Location", "Transaction Date", "Mode of Transport",
    ],
    "S3 Cat 4 Upstream Transport": [
        "Reporting_Month", "attachment date", "release date", "ghg category",
        "activity volume_1", "source_company", "supplier_clean",
        "source_company_clean", "category_clean", "co2e","Source_file", "Sheet", "project start date", "project end date", "project id parent",
    ],
    "Water": [
        "Source_File", "Reporting period (month, year)", "subsidiary_name",
        "Scope_category", "ghg_category", "scource_id", "Sheet", "Spend_Euro",
        "match_method", "status", "Calculation Method","mapping_status", "mapped_by",
    ],
}


def _best_sheet_key(name: str) -> Optional[str]:
    k = _norm(name)
    for key in SHEET_DROP_MAP.keys():
        if k == _norm(key) or k.startswith(_norm(key)):
            return key
    return None


def _find_latest_base(explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    patterns = [
        str(OUT_DIR / "mapped_results_by_ghgp_*.xlsx"),
        str(OUT_DIR / "mapped_results_merged_*.xlsx"),
        str(OUT_DIR / "mapped_results_merged_dc_*.xlsx"),
        str(OUT_DIR / "mapped_results_*.xlsx"),
        str(OUT_DIR / "mapped_results.xlsx"),
    ]
    cands: List[str] = []
    for pat in patterns:
        cands.extend(glob.glob(pat))
    if not cands:
        return None
    cands.sort(key=os.path.getmtime, reverse=True)
    return Path(cands[0])


def _drop_columns(df: pd.DataFrame, to_drop: List[str]) -> pd.DataFrame:
    if df is None or df.empty or not to_drop:
        return df
    norm_map = {_norm(c): c for c in df.columns}
    wanted = {_norm(c) for c in to_drop}
    cols_actual = [norm_map[n] for n in wanted if n in norm_map]
    if not cols_actual:
        return df
    return df.drop(columns=list(dict.fromkeys(cols_actual)), errors="ignore")


def main() -> None:
    ap = argparse.ArgumentParser(description="Final cleaning for GHGP workbook (drop columns per sheet).")
    ap.add_argument("--base", help="Explicit base workbook path (optional). If omitted, latest GHGP is used.")
    args = ap.parse_args()

    base = _find_latest_base(args.base)
    if not base:
        print("No base GHGP workbook found under output/.")
        return

    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(base, sheet_name=None)
    except Exception as e:
        print(f"Failed to read base workbook: {e}")
        return

    cleaned: Dict[str, pd.DataFrame] = {}
    for sheet_name, df in all_sheets.items():
        key = _best_sheet_key(sheet_name)
        if key and df is not None and not df.empty:
            cleaned_df = _drop_columns(df, SHEET_DROP_MAP[key])
        else:
            cleaned_df = df.copy() if df is not None else df

        # Business rule: Water sheet must have Scope = 3
        try:
            if sheet_name.strip().lower().startswith("water") and cleaned_df is not None and not cleaned_df.empty:
                if "Scope" in cleaned_df.columns:
                    cleaned_df["Scope"] = 3
        except Exception:
            pass

        # Business rule: Calculation Method override for booklet sources
        # If Sheet_booklets == 'Travel' or 'Klarakarbon' → 'Precalculated'
        try:
            if cleaned_df is not None and not cleaned_df.empty:
                if "Sheet_booklets" in cleaned_df.columns:
                    sb = cleaned_df["Sheet_booklets"].astype(str).str.strip().str.lower()
                    mask = sb.isin({"travel", "klarakarbon"})
                    if "Calculation Method" in cleaned_df.columns:
                        cleaned_df.loc[mask, "Calculation Method"] = "Precalculated"
        except Exception:
            pass

        

        cleaned[sheet_name[:31]] = cleaned_df

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"mapped_results_by_ghgp_clean_{ts}.xlsx"
    try:
        with pd.ExcelWriter(
            out_path,
            engine="xlsxwriter",
            datetime_format="yyyy-mm-dd",
            date_format="yyyy-mm-dd",
        ) as writer:
            for name, df in cleaned.items():
                df_out = _normalize_date_columns(df.copy()) if df is not None else pd.DataFrame()
                df_out.to_excel(writer, sheet_name=name[:31], index=False)
                _autosize_and_style(writer, name[:31], df_out)
        print(f"Wrote cleaned GHGP workbook: {out_path.name}")
    except PermissionError:
        out_path = OUT_DIR / f"mapped_results_by_ghgp_clean_{ts}_2.xlsx"
        with pd.ExcelWriter(
            out_path,
            engine="xlsxwriter",
            datetime_format="yyyy-mm-dd",
            date_format="yyyy-mm-dd",
        ) as writer:
            for name, df in cleaned.items():
                df_out = _normalize_date_columns(df.copy()) if df is not None else pd.DataFrame()
                df_out.to_excel(writer, sheet_name=name[:31], index=False)
                _autosize_and_style(writer, name[:31], df_out)
        print(f"Wrote cleaned GHGP workbook: {out_path.name}")






if __name__ == "__main__":
    main()

