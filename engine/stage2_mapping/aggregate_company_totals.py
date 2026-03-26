from __future__ import annotations

from pathlib import Path
import os
import glob
from typing import Dict, Tuple, List, Optional
import re
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR


# This script prepares final outputs and groupwide summaries for the latest mapped results workbook.


def find_latest_mapped_results(base_dir: Path) -> Optional[Path]:
    """
    En güncel final workbook'u bulmak için öncelik sırası:
      1) mapped_results_merged_*.xlsx
      2) mapped_results_merged_dc_*.xlsx
      3) mapped_results_*.xlsx
      4) mapped_results.xlsx
    'with_sources' yardımcı kopyalarını tercih etme.
    """
    out = STAGE2_OUTPUT_DIR
    patterns = [
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
    # Exclude helper copies like *_with_sources_*
    filtered = [c for c in candidates if "with_sources" not in os.path.basename(c).lower()]
    if not filtered:
        filtered = candidates
    filtered.sort(key=os.path.getmtime, reverse=True)
    return Path(filtered[0])


def _detect_source_column(df: pd.DataFrame) -> Optional[str]:
    """Return the most likely column name holding the source file/company name.

    Tries common variants and fuzzy matches like 'Source file', 'source_file', etc.
    """
    preferred_exact = {"source_file", "source file", "sourcefile"}
    # First pass: exact-ish preferred matches
    for col in df.columns:
        lower = str(col).lower()
        if lower in preferred_exact:
            return col

    # Second pass: fuzzy 'source' and 'file' tokens
    for col in df.columns:
        lower = str(col).lower()
        compact = lower.replace(" ", "").replace("_", "")
        if ("source" in lower and "file" in lower) or compact == "sourcefile":
            return col

    return None


# Previous "Company Totals" helpers were removed per user's request.


def _autosize_and_style(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Auto-fit columns and apply a light-green header background with borders."""
    try:
        ws = writer.sheets.get(sheet_name)
        if ws is None:
            return
        wb = writer.book
        header_fmt = wb.add_format({
            "bold": True,
            "bg_color": "#D8EAD3",  # light green
            "border": 1,
        })
        dec2_fmt = wb.add_format({"num_format": "0.00"})
        dec5_fmt = wb.add_format({"num_format": "0.00000"})
        pct_fmt = wb.add_format({
            "num_format": "0.0%",
        })
        date_fmt = wb.add_format({
            "num_format": "yyyy-mm-dd",
        })
        zebra_fmt = wb.add_format({
            "bg_color": "#F2F2F2",
        })
        # Rewrite headers with format
        for idx, col in enumerate(list(df.columns)):
            ws.write(0, idx, str(col), header_fmt)
        # Auto-fit columns width (cap at 60)
        for idx, col in enumerate(list(df.columns)):
            try:
                series = df[col].astype(str)
                max_len = max([len(str(col))] + series.str.len().tolist())
                # Slightly tighter widths by default
                width = min(max(8, max_len + 1), 40)
                # Apply formats selectively
                col_low = str(col).strip().lower()
                sheet_low = str(sheet_name).strip().lower()
                if col_low in {"co2e", "co2e (t)", "tco2e_total"}:
                    if "waste" in sheet_low:
                        ws.set_column(idx, idx, width, dec5_fmt)
                    else:
                        ws.set_column(idx, idx, width, dec2_fmt)
                elif col_low == "contribution":
                    ws.set_column(idx, idx, width, pct_fmt)
                elif col_low == "date" or col_low == "reporting_month":
                    ws.set_column(idx, idx, width, date_fmt)
                else:
                    # Do not force generic numerics; keep 'scope' as plain integer-like
                    if col_low in {"scope"}:
                        ws.set_column(idx, idx, width)
                    else:
                        ws.set_column(idx, idx, width)
            except Exception:
                ws.set_column(idx, idx, 16)
        # Freeze top row
        ws.freeze_panes(1, 0)
        # Zebra striping for data rows (apply to full used range)
        if df.shape[0] > 0 and df.shape[1] > 0:
            ws.conditional_format(1, 0, df.shape[0], df.shape[1] - 1, {
                'type': 'formula',
                'criteria': '=MOD(ROW(),2)=0',
                'format': zebra_fmt,
            })
    except Exception:
        # Best effort styling; ignore failures
        pass

def _detect_month_series(df: pd.DataFrame) -> Optional[pd.Series]:
    """Return a normalized month series (YYYY-MM) from likely date/month columns.

    Tries common headers, then any column containing 'report' and 'month'.
    If not found, returns None.
    """
    candidates = [
        "Reporting period (month, year)",
        "Reporting period",
        "Reporting_Month",
        "Month",
        "Date",
    ]
    for c in candidates:
        if c in df.columns:
            try:
                s = pd.to_datetime(df[c], errors="coerce").dt.to_period("M").astype(str)
                return s
            except Exception:
                pass
    # Fallback: pick first column whose name hints reporting month
    for col in df.columns:
        low = str(col).lower()
        if ("report" in low and "month" in low) or ("period" in low and "month" in low):
            try:
                s = pd.to_datetime(df[col], errors="coerce").dt.to_period("M").astype(str)
                return s
            except Exception:
                continue
    return None


def compute_transportation_ten_percent(all_sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute Transportation %10 per company per month from Cat 1 sheets.

    Sheets considered:
      - Scope 3 Cat 1 Goods Spend
      - Scope 3 Cat 1 Services Spend
      - Scope 3 Cat 1 Goods Services
    For each sheet, sums 'co2e' per (Source_file, Reporting_Month) then combines and
    applies 10% factor. Returns a DataFrame with columns:
      - Source_file
      - Reporting_Month
      - Transportation_10_percent_tCO2e
    """
    target_sheets = {
        "Scope 3 Cat 1 Goods Spend",
        "Scope 3 Cat 1 Goods Services",
              
    }
    parts: List[pd.DataFrame] = []
    for sheet_name, df in all_sheets.items():
        if sheet_name not in target_sheets:
            continue
        if df is None or df.empty or "co2e" not in df.columns:
            continue
        source_col = _detect_source_column(df)
        if source_col is None:
            continue
        month_series = _detect_month_series(df)
        # Default month label when not available
        if month_series is None:
            month_series = pd.Series([None] * len(df), index=df.index, dtype=object)

        co2e_numeric = pd.to_numeric(df["co2e"], errors="coerce").fillna(0.0)
        source_series = df[source_col].astype(str)

        temp = pd.DataFrame({
            "Source_file": source_series,
            "Reporting_Month": month_series,
            "co2e": co2e_numeric,
        })
        grouped = temp.groupby(["Source_file", "Reporting_Month"], dropna=False)["co2e"].sum().reset_index()
        parts.append(grouped)

    if not parts:
        return pd.DataFrame(columns=["Source_file", "Reporting_Month", "Transportation_10_percent_tCO2e"]).astype({
            "Source_file": "object",
            "Reporting_Month": "object",
            "Transportation_10_percent_tCO2e": "float64",
        })

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.groupby(["Source_file", "Reporting_Month"], dropna=False)["co2e"].sum().reset_index()
    combined["Transportation_10_percent_tCO2e"] = combined["co2e"] * 0.10
    combined = combined.drop(columns=["co2e"]).sort_values(["Source_file", "Reporting_Month"], na_position="last").reset_index(drop=True)
    return combined


def _normalize_colname(col: str) -> str:
    return str(col).strip().lower().replace(" ", "")


def _find_first_present_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    norm_map = {_normalize_colname(c): c for c in df.columns}
    for cand in candidates:
        key = _normalize_colname(cand)
        if key in norm_map:
            return norm_map[key]
    return None


# Robust parser for numeric strings with mixed decimal/thousand separators
def _parse_mixed_number(val) -> Optional[float]:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if s == "":
            return None
        s = s.replace("\u00A0", "").replace(" ", "")
        # Keep only digits, separators and signs
        import re as _re
        s = _re.sub(r"[^0-9,\.\-\+eE]", "", s)
        if "," in s and "." in s:
            last_comma = s.rfind(",")
            last_dot = s.rfind(".")
            if last_comma > last_dot:
                dec = ","; thou = "."
            else:
                dec = "."; thou = ","
            s = s.replace(thou, "")
            s = s.replace(dec, ".")
        else:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None

def _to_numeric_mixed(series: pd.Series) -> pd.Series:
    try:
        parsed = series.map(_parse_mixed_number)
        return pd.to_numeric(parsed, errors="coerce")
    except Exception:
        return pd.to_numeric(series, errors="coerce")

def _parse_km_value(val) -> Optional[float]:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip().lower()
        if s == "":
            return None
        # Remove unit tokens
        s = s.replace("km", "").strip()
        # Normalize separators/dashes
        s = s.replace("–", "-")
        # If range like "1-5", take average
        if "-" in s:
            parts = [p for p in s.split("-") if p.strip() != ""]
            nums: List[float] = []
            for p in parts:
                try:
                    nums.append(float(p.strip().replace(",", ".")))
                except Exception:
                    continue
            if nums:
                return sum(nums) / len(nums)
            return None
        # Single value, allow comma decimal
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None

def _to_numeric_km(series: pd.Series) -> pd.Series:
    try:
        parsed = series.map(_parse_km_value)
        return pd.to_numeric(parsed, errors="coerce")
    except Exception:
        return pd.to_numeric(series, errors="coerce")

def _drop_rows_where_blank(df: pd.DataFrame, col_candidates: List[str]) -> pd.DataFrame:
    """Return a copy of df with rows dropped where target column is blank/na."""
    if df is None or df.empty:
        return df
    col = _find_first_present_column(df, col_candidates)
    if col is None or col not in df.columns:
        return df
    s = df[col]
    # Treat NaN or empty/whitespace-only as blank
    mask_keep = s.notna() & s.astype(str).str.strip().ne("") & s.astype(str).str.strip().str.lower().ne("na")
    return df[mask_keep].reset_index(drop=True)


# (double counting helper functions were removed per user request)


def clean_sheets_for_final_output(all_sheets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Apply sheet-specific row filtering at the final stage to avoid early data loss.

    Rules:
      - Scope 1 Fuel Usage Spend: drop where Vehicle Type is blank
      - Scope 1 Fuel Activity: drop where Vehicle Type is blank
      - Scope 3 Cat 4+9 Transport Spend: drop where Purchase Date (...) is blank
      - Scope 3 Cat 5 Waste: drop where Waste Stream is blank or 'na'
      - Scope 3 Cat 12 End of Life: drop where Product is blank
      - Scope 3 Cat 11 Products Indirec: drop where Product Name is blank
      - Scope 3 Cat 15 Pensions: drop where Pension Provider is blank
    """
    cleaned: Dict[str, pd.DataFrame] = {}

    # Per-sheet column removals (exact, case-sensitive column names)
    columns_to_remove: Dict[str, List[str]] = {
        "Scope 1 Fuel Usage Spend": ["mapping_status", "mapped_by", "mapped_date", "ef_source", "emissions_tco2e"],
        "Scope 1 Fuel Activity": ["ef_source", "mapping_status", "mapped_by", "emissions_tco2e"],
        "Scope 2 Electricity": ["ef_unit", "ef_source", "emissions_tco2e", "mapping_status", "mapped_by"],
        "Scope 3 Cat 1 Goods Spend": ["Data Source(invoice tracker)", "ef_source", "emissions_tco2e", "mapping_status", "mapped_by"],
        "Scope 3 Cat 1 Services Spend": ["ef_source", "mapping_status", "mapped_by", "emissions_tco2e"],
        "Scope 3 Cat 1 Services Activity": ["mapping_status", "mapped_by", "ef_source", "emissions_tco2e"],
        "Scope 3 Cat 7 Employee Commute": ["mapping_status", "mapped_by", "ef_unit", "ef_source", "emissions_tco2e"],
        "Scope 3 Cat 5 Waste": [
            "Avergage -data method",
            "mapping_status",
            "mapped_by",
            "0",
            "Annual Waste per Stream (kg) '= Floor Area (m2) x Total Waste Rate (kg/m2/year) x Waste Type %",
            "Unnamed: 3",
            "Unnamed: 10",
            "Unnamed: 4",
            "Unnamed: 6",
            "Unnamed: 10",
        ],
        "Scope 1 Fuel Usage Activity": ["ef_source", "mapping_status", "mapped_by", "emissions_tco2e"],
        "Scope 3 Cat 1 Goods Services": ["mapping_status", "mapped_by", "emissions_tco2e"],
        "Scope 2 Electricity Average": ["ef_id", "ef_unit", "mapping status", "mapped_by", "mapped_date", "ef_value", "ef_source", "emissions_tco2e"],
        "Scope 3 Cat 5 Office Waste": ["mapping status", "mapped_by", "ef_unit", "ef_source", "emissions_tco2e"],
        "Water Tracker Averages": ["ef_id", "ef_name", "ef_unit", "mapping_status", "mapped_by", "ef_value", "ef_source", "emissions_tco2e"],
        "Scope 3 Cat 5 Waste Oslo": ["ef_unit", "ef_source", "emissions_tco2e", "mapping_status", "mapped_by"],
        "Scope 3 Category 9 Activity": ["mapping_status", "mapped_by", "ef_id", "ef_name", "ef_value", "ef_source", "emissions_tco2e", "match_method"],
        "Scope 3 Cat 12 End of Life": ["mapping_status", "mapped_by", "emissions_tco2e"],
        "Scope 3 Category 11 Scenario": ["ef_unit", "ef_value", "ef_name", "ef_source", "emissions_tco2e", "mapping_status", "mapped_by"],
        "Scope 3 Cat 6 Business Travel": ["mapping_status", "mapped_by", "emissions_tco2e", "ef_unit", "ef_source"],
        "Scope 3 Services Spend": ["mapping_status", "mapped_by", "ef_source", "emissions_tco2e"],
        "Scope 3 Cat 11 Product Indirec": ["mapping_status", "mapped_by", "ef_id", "ef_name", "ef_unit", "ef_source", "emissions_tco2e", "ef_value"],
        "Scope 3 Cat 15 Pensions": ["mapping_status", "mapped_by", "ef_unit", "ef_source", "emissions_tco2e"],
    }

    # Build case-insensitive, 31-char truncation-aware lookup for sheet names
    drop_lookup: Dict[str, List[str]] = {}
    for k, v in columns_to_remove.items():
        key_low = str(k).strip().lower()
        drop_lookup[key_low] = v
        drop_lookup[key_low[:31]] = v

    # Mapping from sheet name -> GHGP Category (case-insensitive, supports truncated names)
    GHGP_BY_SHEET: Dict[str, str] = {
        "Scope 1 Fuel Usage Spend": "Scope 1",
        "Scope 1 Fuel Activity": "Scope 1",
        "Scope 2 Electricity": "Scope 2",
        "Scope 3 Cat 1 Goods Spend": "Scope 3 Category 1 Purchased Goods and Services",
        "Scope 3 Cat 1 Services Spend": "Scope 3 Category 1 Purchased Goods and Services",
        "Scope 3 Cat 1 Services Activity": "Scope 3 Category 1 Purchased Goods and Services",
        "Scope 3 Cat 7 Employee Commute": "Scope 3 Category 7 Employee Commuting",
        "Scope 3 Cat 5 Waste": "Scope 3 Category 5 Waste",
        "Scope 1 Fuel Usage Activity": "Scope 1",
        "Scope 3 Cat 1 Goods Services": "Scope 3 Category 1 Purchased Goods and Services",
        "Scope 2 Electricity Average": "Scope 2",
        "Scope 3 Cat 15 Investments": "Scope 3 Category 15 Investments",
        "Scope 3 Cat 5 Office Waste": "Scope 3 Category 5 Waste",
        "Water Tracker 2": "Water",
        "Water Tracker Averages": "Water",
        "Scope 3 Cat 5 Waste Oslo": "Scope 3 Category 5 Waste",
        "Scope 3 Category 9 Activity": "Scope 3 Category 9 Downstream Transportation",
        "Scope 3 Cat 12 End of Life": "Scope 3 Category 12 End of Life of Sold Products",
        "Scope 3 Category 11 Scenario": "Scope 3 Category 11 Use of Sold Products",
        "Scope 3 Cat 6 Business Travel": "Scope 3 Category 6 Business Travel",
        "Scope 3 Services Spend": "Scope 3 Category 1 Purchased Goods and Services",
        "Scope 3 Cat 11 Products Indirec": "Scope 3 Category 11 Use of Sold Products",
        "Scope 3 Cat 15 Pensions": "Scope 3 Category 15 Pensions",
        "Water Tracker": "Water",
        "Transportation %10": "Scope 3 Category 4 Upstream Transportation",
    }

    # Build a lookup with both full and 31-char truncated keys in lowercase
    ghgp_lookup: Dict[str, str] = {}
    for k, v in GHGP_BY_SHEET.items():
        key_full = str(k).strip().lower()
        key_trunc = key_full[:31]
        ghgp_lookup[key_full] = v
        ghgp_lookup[key_trunc] = v
    for name, df in all_sheets.items():
        new_df = df.copy()
        # Drop raw/duplicate/mapping helper columns if present
        for col in [
            "description_raw",
            "unit_raw",
            "amount_raw",
            "ef_id.1",
            "ef_name.1",
            "ef_unit.1",
            "mapping_confidence",
        ]:
            if col in new_df.columns:
                try:
                    new_df = new_df.drop(columns=[col])
                except Exception:
                    pass
        n = name.strip()
        # Drop entire sheet at final stage per user
        if n == "Scope 3 Cat 8 Electricity":
            continue
        # Newly requested full drops at final stage
        if n == "Scope 3 Cat 15 Investments 2":
            continue
        # Drop alternative duplicates/variants
        if n == "Water tracker Averages 2" or n == "Water Tracker Averages 2":
            continue
        if n == "Scope 3 Cat 5 Waste(2)" or n == "Scope 3 Cat 5 Waste (2)":
            continue
        if n == "Scope 3 Cat 5 Waste Oslo 11":
            continue
        # Always drop old summary sheets if present in source workbooks
        if n in {"Company Totals", "Company Totals by Sheet"}:
            continue

        if n == "Scope 1 Fuel Usage Spend":
            new_df = _drop_rows_where_blank(new_df, ["Vehicle Type", "Vehicle type"])
        elif n == "Scope 1 Fuel Usage Activity":
            new_df = _drop_rows_where_blank(new_df, ["Vehicle Type", "Vehicle type", "vehicle type"])
        elif n == "Scope 1 Fuel Activity":
            new_df = _drop_rows_where_blank(new_df, ["Vehicle Type", "Vehicle type", "vehicle type"])
        elif n == "Scope 2 Electricity":
            # Normalize Consumption to numeric (dot-decimal) for consistent output
            try:
                cons_col = _find_first_present_column(new_df, ["Consumption", "consumption"])
                if cons_col is not None and cons_col in new_df.columns:
                    new_df[cons_col] = _to_numeric_mixed(new_df[cons_col])
            except Exception:
                pass
        elif n == "Scope 3 Cat 4+9 Transport Spend":
            new_df = _drop_rows_where_blank(new_df, ["Purchase Date (Purchase order date or invoice date)", "Purchase Date"])
        elif n == "Scope 3 Cat 5 Waste":
            new_df = _drop_rows_where_blank(new_df, ["Waste Stream", "Waste stream"])
        # Removed per user request: do not drop Cat 12 rows based on blank 'Product'
        # Removed per user request: do not drop Cat 11 rows based on blank 'Product Name'
        elif n == "Scope 3 Cat 15 Pensions":
            new_df = _drop_rows_where_blank(new_df, ["Pension Provider"])
        elif n == "Scope 3 Cat 7 Employee Commute":
            # Normalize km travelled one way to numeric (handle ranges like '1–5 km')
            try:
                one_way_col = _find_first_present_column(new_df, [
                    "km travelled one way", "km traveled one way",
                    "km one way", "one way km"
                ])
                if one_way_col and one_way_col in new_df.columns:
                    new_df[one_way_col] = _to_numeric_km(new_df[one_way_col])
            except Exception:
                pass
        elif n == "Scope 3 Category 9 Activity":
            # Copy monthly fixed CO2e from deliveries into 'co2e (t)' when present
            try:
                src_col = _find_first_present_column(new_df, [
                    "monthly fixed CO2e tons from deliveries",
                    "Monthly fixed CO2e tons from deliveries",
                ])
                if src_col is not None and src_col in new_df.columns:
                    fixed_vals = pd.to_numeric(new_df[src_col], errors="coerce")
                    if any(str(c).strip().lower() == "co2e (t)" for c in new_df.columns):
                        co2e_t_col = next(c for c in new_df.columns if str(c).strip().lower() == "co2e (t)")
                        existing = pd.to_numeric(new_df[co2e_t_col], errors="coerce")
                        mask = existing.isna() | (existing.fillna(0.0) == 0.0)
                        new_df.loc[mask, co2e_t_col] = fixed_vals
                    else:
                        new_df["co2e (t)"] = fixed_vals
            except Exception:
                pass

        # Additional numeric normalizations for Cat 1 sheets (Spend values with ',' or spaces)
        try:
            if n in {
                "Scope 3 Cat 1 Goods Spend",
                "Scope 3 Cat 1 Services Spend",
                "Scope 3 Cat 1 Goods Services",
                "Scope 3 Services Spend",
            }:
                spend_col = _find_first_present_column(new_df, [
                    "Spend_Euro", "Spend Euro", "Spend EUR", "Spend", "Amount"
                ])
                if spend_col and spend_col in new_df.columns:
                    new_df[spend_col] = _to_numeric_mixed(new_df[spend_col])
        except Exception:
            pass

        # WT_extracted/Water Tracker: drop unused columns
        if n in {"WT_extracted", "Water Tracker"}:
            wt_drop_cols = [
                "currency",
                "co2e",
                "ef_id",
                "ef_name",
                "ef_unit",
                "Spend_Euro",
                "ef_value",
                "ef_source",
                "emissions_tco2e",
            ]
            for c in wt_drop_cols:
                if c in new_df.columns:
                    try:
                        new_df = new_df.drop(columns=[c])
                    except Exception:
                        pass

        # Global: remove only lowercase 'currency' column if present (preserve 'Currency')
        try:
            cols_to_drop_exact = [c for c in new_df.columns if str(c) == "currency"]
            if cols_to_drop_exact:
                new_df = new_df.drop(columns=cols_to_drop_exact)
        except Exception:
            pass

        # Apply per-sheet column removals (exact, case-sensitive)
        try:
            key_low = n.strip().lower()
            targets = drop_lookup.get(key_low) or drop_lookup.get(key_low[:31])
            if targets:
                cols_to_drop = [c for c in targets if c in new_df.columns]
                if cols_to_drop:
                    new_df = new_df.drop(columns=list(dict.fromkeys(cols_to_drop)))
        except Exception:
            pass
        # Ensure GHGP Category column exists, defaulting to existing ghg_category if present
        try:
            if "GHGP Category" not in new_df.columns:
                if "ghg_category" in new_df.columns:
                    new_df["GHGP Category"] = new_df["ghg_category"].astype("object")
                else:
                    new_df["GHGP Category"] = pd.Series([None] * len(new_df), dtype="object")
        except Exception:
            pass

        # Overwrite GHGP Category based on sheet mapping if applicable
        try:
            key_low = n.strip().lower()
            mapped_val = ghgp_lookup.get(key_low) or ghgp_lookup.get(key_low[:31])
            if mapped_val is not None:
                new_df["GHGP Category"] = mapped_val
        except Exception:
            pass

        # Add Company column from Source_file (strip trailing .xlsx/.xls)
        try:
            src_col = _detect_source_column(new_df)
            if src_col is not None and src_col in new_df.columns:
                def _to_company(val: object) -> Optional[str]:
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return None
                    s = str(val).strip()
                    if s == "":
                        return None
                    s = re.sub(r"\.xlsx?$", "", s, flags=re.IGNORECASE)
                    return s
                new_df["Company"] = new_df[src_col].map(_to_company).astype("object")
        except Exception:
            pass

        # Rename 'co2e' -> 'co2e (t)' uniformly
        try:
            col_map = {str(c).strip().lower(): c for c in new_df.columns}
            if "co2e" in col_map and "co2e (t)" not in col_map:
                new_df = new_df.rename(columns={col_map["co2e"]: "co2e (t)"})
        except Exception:
            pass

        # Double counting rules removed; no final-stage nulling applied
        cleaned[name] = new_df
    return cleaned


def compute_groupwide_company_totals(all_sheets: Dict[str, pd.DataFrame], year_filter: Optional[int] = None) -> pd.DataFrame:
    """Aggregate total co2e per (Company, GHGP Category) across ALL sheets present.

    - Expects each sheet to have columns: 'Company' and 'co2e'.
    - Uses 'GHGP Category' when available; otherwise falls back to existing 'ghg_category' or the sheet name.
    - Rows missing the required fields are skipped.
    """
    records: List[Dict[str, object]] = []
    for name, df in all_sheets.items():
        if df is None or df.empty:
            continue
        # Detect columns
        company_col = None
        for c in df.columns:
            if str(c).strip().lower() == "company":
                company_col = c
                break
        if company_col is None:
            continue
        co2e_col = _find_first_present_column(df, ["co2e (t)", "co2e"]) or None
        if co2e_col is None:
            continue
        ghgp_col = None
        for c in df.columns:
            low = str(c).strip().lower()
            if low == "ghgp category" or low == "ghgp_category" or low == "ghgpcategory":
                ghgp_col = c
                break
        # Fallbacks
        if ghgp_col is None:
            for c in df.columns:
                if str(c).strip().lower() in {"ghg category", "ghg_category", "ghgcategory"}:
                    ghgp_col = c
                    break
        # Optional: year filter (keep only rows where detected month is within year_filter)
        if year_filter is not None:
            try:
                month_series = _detect_month_series(df)
            except Exception:
                month_series = None
            if month_series is not None:
                try:
                    years = pd.to_datetime(month_series, errors="coerce").dt.year
                    df = df[years == year_filter]
                except Exception:
                    # If we cannot parse filter, drop rows (treat as not in the year)
                    df = df.iloc[0:0]
        # Default GHGP category to the sheet name when not found
        if ghgp_col is None:
            # Use cleaned/visible sheet name (truncate to 31 chars like Excel when applicable)
            default_cat = str(name)[:31]
            temp = pd.DataFrame({
                "Company": df[company_col].astype(str),
                "GHGP Category": default_cat,
                "co2e": pd.to_numeric(df[co2e_col], errors="coerce").fillna(0.0),
            })
        else:
            temp = pd.DataFrame({
                "Company": df[company_col].astype(str),
                "GHGP Category": df[ghgp_col].astype(str),
                "co2e": pd.to_numeric(df[co2e_col], errors="coerce").fillna(0.0),
            })
        # Normalize Company (strip .xlsx/.xls) and drop blanks
        try:
            temp["Company"] = (
                temp["Company"].astype(str)
                .str.strip()
                .str.replace(r"(?i)\.xlsx?$", "", regex=True)
            )
        except Exception:
            pass
        temp = temp[temp["Company"].str.strip().ne("")]
        if not temp.empty:
            records.append(temp)

    if not records:
        return pd.DataFrame(columns=["Company", "GHGP Category", "tCO2e_total"]).astype({
            "Company": "object",
            "GHGP Category": "object",
            "tCO2e_total": "float64",
        })

    big = pd.concat(records, ignore_index=True)
    grouped = (
        big.groupby(["Company", "GHGP Category"], dropna=False)["co2e"].sum().reset_index()
        .rename(columns={"co2e": "tCO2e_total"})
        .sort_values(["Company", "tCO2e_total"], ascending=[True, False])
        .reset_index(drop=True)
    )
    # Contribution (share of grand total)
    if not grouped.empty:
        total_all = float(grouped["tCO2e_total"].sum(skipna=True))
        if total_all > 0:
            grouped["Contribution"] = grouped["tCO2e_total"] / total_all
        else:
            grouped["Contribution"] = 0.0
    return grouped


def compute_groupwide_company_totals_by_month(all_sheets: Dict[str, pd.DataFrame], year_filter: Optional[int] = None) -> pd.DataFrame:
    """Aggregate total co2e per (Company, Reporting_Month) across ALL sheets.

    - Uses _detect_month_series to find a month-like column; if not present, the sheet contributes no monthly rows.
    """
    rows: List[Dict[str, object]] = []
    for name, df in all_sheets.items():
        if df is None or df.empty:
            continue
        # company
        company_col = None
        for c in df.columns:
            if str(c).strip().lower() == "company":
                company_col = c
                break
        if company_col is None:
            continue
        # co2e
        co2e_col = _find_first_present_column(df, ["co2e (t)", "co2e"]) or None
        if co2e_col is None:
            continue
        # month detection
        try:
            month_series = _detect_month_series(df)
        except Exception:
            month_series = None
        if month_series is None:
            continue
        tmp = pd.DataFrame({
            "Company": df[company_col].astype(str),
            "Reporting_Month": month_series,
            "co2e": pd.to_numeric(df[co2e_col], errors="coerce").fillna(0.0),
        })
        # Normalize Company (strip .xlsx/.xls)
        try:
            tmp["Company"] = (
                tmp["Company"].astype(str)
                .str.strip()
                .str.replace(r"(?i)\.xlsx?$", "", regex=True)
            )
        except Exception:
            pass
        # Filter target year if requested
        if year_filter is not None:
            try:
                yy = pd.to_datetime(tmp["Reporting_Month"], errors="coerce").dt.year
                tmp = tmp[yy == year_filter]
            except Exception:
                tmp = tmp.iloc[0:0]
        tmp = tmp[tmp["Company"].str.strip().ne("")]
        rows.append(tmp)

    if not rows:
        return pd.DataFrame(columns=["Company", "Reporting_Month", "tCO2e_total"]).astype({
            "Company": "object",
            "Reporting_Month": "object",
            "tCO2e_total": "float64",
        })

    big = pd.concat(rows, ignore_index=True)
    grouped = (
        big.groupby(["Company", "Reporting_Month"], dropna=False)["co2e"].sum().reset_index()
        .rename(columns={"co2e": "tCO2e_total"})
        .sort_values(["Company", "Reporting_Month"], ascending=[True, True])
        .reset_index(drop=True)
    )
    # Contribution per month (share of total that month)
    if not grouped.empty:
        try:
            month_totals = grouped.groupby("Reporting_Month", dropna=False)["tCO2e_total"].transform("sum")
            with pd.option_context('mode.use_inf_as_na', True):
                grouped["Contribution"] = grouped["tCO2e_total"] / month_totals.replace({0: pd.NA})
            grouped["Contribution"] = grouped["Contribution"].fillna(0.0)
        except Exception:
            grouped["Contribution"] = 0.0
    return grouped

def main() -> None:
    base_dir = Path(__file__).resolve().parent
    target = find_latest_mapped_results(base_dir)
    if target is None:
        print("No mapped_results*.xlsx found under output/.")
        return

    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(target, sheet_name=None)
    except Exception:
        print(f"Failed to read workbook: {target}")
        return

    # Build Transportation %10 BEFORE cleaning so GHGP_BY_SHEET mapping applies
    try:
        transportation_10_df = compute_transportation_ten_percent(all_sheets)
        if transportation_10_df is not None and not transportation_10_df.empty:
            trans_enriched = transportation_10_df.copy()
            # Company from Source_file without extension
            if "Source_file" in trans_enriched.columns:
                def _strip_ext(val: object) -> Optional[str]:
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return None
                    s = str(val).strip()
                    if s == "":
                        return None
                    s = re.sub(r"\.xlsx?$", "", s, flags=re.IGNORECASE)
                    return s
                trans_enriched["Company"] = trans_enriched["Source_file"].map(_strip_ext).astype("object")
            else:
                trans_enriched["Company"] = None
            # Rename 10% column to 'co2e (t)' for downstream consistency
            if "Transportation_10_percent_tCO2e" in trans_enriched.columns:
                trans_enriched["co2e (t)"] = pd.to_numeric(trans_enriched["Transportation_10_percent_tCO2e"], errors="coerce").fillna(0.0)
            else:
                trans_enriched["co2e (t)"] = 0.0
            # Inject into all_sheets so cleaning/mapping (including GHGP_BY_SHEET) applies
            all_sheets["Transportation %10"] = trans_enriched
    except Exception:
        pass

    # Apply cleaning at the final stage (to avoid early data loss in mapping/calculation)
    cleaned_sheets = clean_sheets_for_final_output(all_sheets)
    # Rename WT_extracted sheet to Water Tracker in the output
    if "WT_extracted" in cleaned_sheets:
        cleaned_sheets["Water Tracker"] = cleaned_sheets.pop("WT_extracted")

    # Force GHGP Category = "Water" on Water Tracker sheet
    try:
        if "Water Tracker" in cleaned_sheets:
            df_w = cleaned_sheets["Water Tracker"].copy()
            if "GHGP Category" not in df_w.columns:
                df_w["GHGP Category"] = pd.Series([None] * len(df_w), dtype="object")
            df_w["GHGP Category"] = "Water"
            cleaned_sheets["Water Tracker"] = df_w
    except Exception:
        pass

    # Global normalization/creation: ensure Company exists and is free of .xlsx/.xls
    try:
        for k, df in list(cleaned_sheets.items()):
            if df is None or df.empty:
                continue
            comp_col = next((c for c in df.columns if str(c).strip().lower() == "company"), None)
            if comp_col is None:
                # Try to create Company from Source_file variants
                src_col = None
                lowmap = {str(c).strip().lower(): c for c in df.columns}
                for key in ["source_file", "source file", "sourcefile", "source_file_", "source filename"]:
                    if key in lowmap:
                        src_col = lowmap[key]
                        break
                if src_col is not None:
                    try:
                        comp = df[src_col].astype(str).str.strip().str.replace(r"(?i)\.xlsx?$", "", regex=True)
                        df["Company"] = comp.astype("object")
                        cleaned_sheets[k] = df
                    except Exception:
                        pass
            else:
                try:
                    df[comp_col] = df[comp_col].astype(str).str.strip().str.replace(r"(?i)\.xlsx?$", "", regex=True)
                    cleaned_sheets[k] = df
                except Exception:
                    pass
    except Exception:
        pass

    # Global: ensure 'co2e (t)' is numeric across all sheets (avoid text cells breaking Excel calcs)
    try:
        for k, df in list(cleaned_sheets.items()):
            if df is None or df.empty:
                continue
            col_match = None
            for c in df.columns:
                if str(c).strip().lower() == "co2e (t)":
                    col_match = c
                    break
            if col_match is not None:
                try:
                    df[col_match] = pd.to_numeric(df[col_match], errors="coerce")
                    cleaned_sheets[k] = df
                except Exception:
                    pass
    except Exception:
        pass

    # Company totals sheets removed per user; keep only Transportation %10 and Groupwide totals
    # Transportation %10 already injected before cleaning; fetch for output convenience
    transportation_10_out = cleaned_sheets.get("Transportation %10", pd.DataFrame())
    # END: Transportation %10
    YEAR_FILTER = 2025
    gw_totals = compute_groupwide_company_totals(cleaned_sheets, year_filter=YEAR_FILTER)
    gw_totals_company = (
        gw_totals.groupby("Company", dropna=False)["tCO2e_total"].sum().reset_index().sort_values("tCO2e_total", ascending=False).reset_index(drop=True)
    )
    # Contribution for company-only (share of grand total in this table)
    if not gw_totals_company.empty:
        total_all_company = float(gw_totals_company["tCO2e_total"].sum(skipna=True))
        if total_all_company > 0:
            gw_totals_company["Contribution"] = gw_totals_company["tCO2e_total"] / total_all_company
        else:
            gw_totals_company["Contribution"] = 0.0
    gw_totals_month = compute_groupwide_company_totals_by_month(cleaned_sheets, year_filter=YEAR_FILTER)
    GWM_SHEET = "Groupwide Totals by Month"

    # Write back to the same workbook; if locked, write timestamped copy
    try:
        with pd.ExcelWriter(target, engine="xlsxwriter") as writer:
            for name, df in cleaned_sheets.items():
                safe_name = name[:31] if len(name) > 31 else name
                df_out = df.copy()
                if any(str(c).strip().lower() == "co2e" for c in df_out.columns):
                    co2e_col = next(c for c in df_out.columns if str(c).strip().lower() == "co2e")
                    df_out = df_out.rename(columns={co2e_col: "co2e (t)"})
                df_out.to_excel(writer, sheet_name=safe_name, index=False)
                _autosize_and_style(writer, safe_name, df_out)
            # Append summary sheets (Groupwide and Transportation %10)
            trans_out = transportation_10_out.copy()
            if not trans_out.empty and any(str(c).strip().lower() == "co2e" for c in trans_out.columns):
                ccol = next(c for c in trans_out.columns if str(c).strip().lower() == "co2e")
                trans_out = trans_out.rename(columns={ccol: "co2e (t)"})
            trans_out.to_excel(writer, sheet_name="Transportation %10", index=False)
            _autosize_and_style(writer, "Transportation %10", trans_out)
            # Groupwide Company Totals (Company x GHGP Category)
            gw_totals.to_excel(writer, sheet_name="Groupwide Company Totals", index=False)
            _autosize_and_style(writer, "Groupwide Company Totals", gw_totals)
            # Groupwide Company Totals 2 (Company only)
            gw_totals_company.to_excel(writer, sheet_name="Groupwide Company Totals 2", index=False)
            _autosize_and_style(writer, "Groupwide Company Totals 2", gw_totals_company)
            # Groupwide Company Totals by Month
            gw_totals_month.to_excel(writer, sheet_name=GWM_SHEET, index=False)
            _autosize_and_style(writer, GWM_SHEET, gw_totals_month)

            # Add charts
            wb = writer.book
            # 1) Column chart by GHGP Category on 'Groupwide Company Totals'
            try:
                ws_gw = writer.sheets.get("Groupwide Company Totals")
                by_ghgp = gw_totals.groupby("GHGP Category", dropna=False)["tCO2e_total"].sum().reset_index()
                startcol = 5
                startrow = 0
                by_ghgp.to_excel(writer, sheet_name="Groupwide Company Totals", index=False, startrow=startrow, startcol=startcol)
                chart1 = wb.add_chart({"type": "column"})
                # Categories and values ranges
                cat_first_row = startrow + 1
                cat_last_row = startrow + len(by_ghgp)
                chart1.add_series({
                    "name": "GHGP Category Totals",
                    "categories": ["Groupwide Company Totals", cat_first_row, startcol + 0, cat_last_row, startcol + 0],
                    "values":     ["Groupwide Company Totals", cat_first_row, startcol + 1, cat_last_row, startcol + 1],
                })
                chart1.set_title({"name": "Totals by GHGP Category"})
                chart1.set_y_axis({"name": "tCO2e"})
                chart1.set_legend({"position": "bottom"})
                chart1.set_data_labels({"value": True})
                ws_gw.insert_chart(2, startcol + 3, chart1)
            except Exception:
                pass

            # 2) Pie chart on 'Groupwide Company Totals 2'
            try:
                ws_gw2 = writer.sheets.get("Groupwide Company Totals 2")
                if not gw_totals_company.empty:
                    chart2 = wb.add_chart({"type": "pie"})
                    chart2.add_series({
                        "name": "Company Share",
                        "categories": ["Groupwide Company Totals 2", 1, 0, len(gw_totals_company), 0],
                        "values":     ["Groupwide Company Totals 2", 1, 1, len(gw_totals_company), 1],
                    })
                    chart2.set_title({"name": "Company Distribution"})
                    chart2.set_data_labels({"percentage": True, "value": True})
                    ws_gw2.insert_chart("D2", chart2)
            except Exception:
                pass

            # 3) Column chart by month on 'Groupwide Company Totals by Month' (overall totals)
            try:
                ws_gwm = writer.sheets.get(GWM_SHEET)
                if not gw_totals_month.empty:
                    by_month = (
                        gw_totals_month.groupby("Reporting_Month", dropna=False)["tCO2e_total"].sum().reset_index()
                    )
                    startcol_m = 4
                    startrow_m = 0
                    by_month.to_excel(writer, sheet_name=GWM_SHEET, index=False, startrow=startrow_m, startcol=startcol_m)
                    chart3 = wb.add_chart({"type": "column", "subtype": "stacked"})
                    # Build stacked series per top companies
                    top_companies = (
                        gw_totals_month.groupby("Company", dropna=False)["tCO2e_total"].sum().nlargest(10).index.tolist()
                    )
                    # Create a pivot table at the right side for series source
                    pivot_startcol = startcol_m + 3
                    pivot_startrow = startrow_m
                    # header row: Month + companies
                    header = ["Reporting_Month"] + top_companies
                    ws_gwm.write_row(pivot_startrow, pivot_startcol, header)
                    # build month order
                    months = by_month["Reporting_Month"].tolist()
                    for r_idx, month in enumerate(months, start=1):
                        ws_gwm.write(pivot_startrow + r_idx, pivot_startcol + 0, month)
                        for c_idx, comp in enumerate(top_companies, start=1):
                            # compute value for (comp, month)
                            try:
                                val = float(
                                    gw_totals_month[
                                        (gw_totals_month["Company"] == comp) & (gw_totals_month["Reporting_Month"] == month)
                                    ]["tCO2e_total"].sum()
                                )
                            except Exception:
                                val = 0.0
                            ws_gwm.write_number(pivot_startrow + r_idx, pivot_startcol + c_idx, val)

                    # Add a series per company
                    for idx, comp in enumerate(top_companies, start=1):
                        chart3.add_series({
                            "name":       [GWM_SHEET, pivot_startrow, pivot_startcol + idx],
                            "categories": [GWM_SHEET, pivot_startrow + 1, pivot_startcol + 0, pivot_startrow + len(months), pivot_startcol + 0],
                            "values":     [GWM_SHEET, pivot_startrow + 1, pivot_startcol + idx, pivot_startrow + len(months), pivot_startcol + idx],
                        })
                    chart3.set_title({"name": "Totals by Month"})
                    chart3.set_y_axis({"name": "tCO2e"})
                    chart3.set_legend({"position": "bottom"})
                    chart3.set_data_labels({"value": False})
                    ws_gwm.insert_chart("H2", chart3)
            except Exception:
                pass
        written_path = target
    except PermissionError:
        ts_name = target.with_name(f"{target.stem}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}{target.suffix}")
        with pd.ExcelWriter(ts_name, engine="xlsxwriter") as writer:
            for name, df in cleaned_sheets.items():
                safe_name = name[:31] if len(name) > 31 else name
                df_out = df.copy()
                if any(str(c).strip().lower() == "co2e" for c in df_out.columns):
                    co2e_col = next(c for c in df_out.columns if str(c).strip().lower() == "co2e")
                    df_out = df_out.rename(columns={co2e_col: "co2e (t)"})
                df_out.to_excel(writer, sheet_name=safe_name, index=False)
                _autosize_and_style(writer, safe_name, df_out)
            trans_out = transportation_10_out.copy()
            if not trans_out.empty and any(str(c).strip().lower() == "co2e" for c in trans_out.columns):
                ccol = next(c for c in trans_out.columns if str(c).strip().lower() == "co2e")
                trans_out = trans_out.rename(columns={ccol: "co2e (t)"})
            trans_out.to_excel(writer, sheet_name="Transportation %10", index=False)
            _autosize_and_style(writer, "Transportation %10", trans_out)
            gw_totals.to_excel(writer, sheet_name="Groupwide Company Totals", index=False)
            _autosize_and_style(writer, "Groupwide Company Totals", gw_totals)
            gw_totals_company.to_excel(writer, sheet_name="Groupwide Company Totals 2", index=False)
            _autosize_and_style(writer, "Groupwide Company Totals 2", gw_totals_company)
            gw_totals_month.to_excel(writer, sheet_name=GWM_SHEET, index=False)
            _autosize_and_style(writer, GWM_SHEET, gw_totals_month)

            # Charts (best-effort)
            try:
                wb = writer.book
                ws_gw = writer.sheets.get("Groupwide Company Totals")
                by_ghgp = gw_totals.groupby("GHGP Category", dropna=False)["tCO2e_total"].sum().reset_index()
                startcol = 5
                startrow = 0
                by_ghgp.to_excel(writer, sheet_name="Groupwide Company Totals", index=False, startrow=startrow, startcol=startcol)
                chart1 = wb.add_chart({"type": "column"})
                chart1.add_series({
                    "name": "GHGP Category Totals",
                    "categories": ["Groupwide Company Totals", startrow + 1, startcol + 0, startrow + len(by_ghgp), startcol + 0],
                    "values":     ["Groupwide Company Totals", startrow + 1, startcol + 1, startrow + len(by_ghgp), startcol + 1],
                })
                chart1.set_title({"name": "Totals by GHGP Category"})
                chart1.set_y_axis({"name": "tCO2e"})
                ws_gw.insert_chart(2, startcol + 3, chart1)
            except Exception:
                pass
            try:
                wb = writer.book
                ws_gw2 = writer.sheets.get("Groupwide Company Totals 2")
                if not gw_totals_company.empty:
                    chart2 = wb.add_chart({"type": "pie"})
                    chart2.add_series({
                        "name": "Company Share",
                        "categories": ["Groupwide Company Totals 2", 1, 0, len(gw_totals_company), 0],
                        "values":     ["Groupwide Company Totals 2", 1, 1, len(gw_totals_company), 1],
                    })
                    chart2.set_title({"name": "Company Distribution"})
                    ws_gw2.insert_chart("D2", chart2)
            except Exception:
                pass
            try:
                wb = writer.book
                ws_gwm = writer.sheets.get(GWM_SHEET)
                if not gw_totals_month.empty:
                    by_month = (
                        gw_totals_month.groupby("Reporting_Month", dropna=False)["tCO2e_total"].sum().reset_index()
                    )
                    startcol_m = 4
                    startrow_m = 0
                    by_month.to_excel(writer, sheet_name=GWM_SHEET, index=False, startrow=startrow_m, startcol=startcol_m)
                    chart3 = wb.add_chart({"type": "column"})
                    chart3.add_series({
                        "name": "Monthly Totals",
                        "categories": [GWM_SHEET, startrow_m + 1, startcol_m + 0, startrow_m + len(by_month), startcol_m + 0],
                        "values":     [GWM_SHEET, startrow_m + 1, startcol_m + 1, startrow_m + len(by_month), startcol_m + 1],
                    })
                    chart3.set_title({"name": "Totals by Month"})
                    chart3.set_y_axis({"name": "tCO2e"})
                    ws_gwm.insert_chart("H2", chart3)
            except Exception:
                pass
        written_path = ts_name

    print(f"Updated workbook with groupwide totals: {Path(written_path).name}")
    try:
        by_company = gw_totals.groupby("Company", dropna=False)["tCO2e_total"].sum().sort_values(ascending=False).head(10)
        print("Top 10 companies by tCO2e (groupwide):")
        for company, total in by_company.items():
            print(f"- {company}: {total}")
    except Exception:
        pass

    # Final step: also write regrouped workbook by GHGP Category
    try:
        from reorganize_by_ghgp_category import regroup_by_ghgp
        regroup_by_ghgp()
    except Exception:
        pass

 
  
# Simdi burada senden istedigim Company Totals, Company by GHGP Sheet Totals, GHGP Sheet Totals, Company Stacked Data, Company Stacked Data by Months sheetlerini tamamen window dosyasindan cikartman.
# Bu sheetleri cikarttiktan sonra. Yine benzer bir  
  
  

if __name__ == "__main__":
    main()


