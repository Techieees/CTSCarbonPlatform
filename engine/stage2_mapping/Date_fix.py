from __future__ import annotations

from pathlib import Path
import os
import glob
from typing import Dict, List, Optional, Tuple
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR


# Standalone final step: regroup final workbook into sheets per GHGP Category.
# - Reads the latest mapped_results_merged_dc*.xlsx (fallback to merged or mapped)
# - Skips unmodified sheets: Groupwide Company Totals, Groupwide Company Totals 2,
#   Groupwide Totals by Month, DC Log, Anomalies (copies them as-is)
# - Concatenates all other sheets by GHGP Category value
# - Adds a helper column 'Sheet_booklets' to preserve original sheet provenance


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR

# Sheets to EXCLUDE entirely from regrouping (deleted)
EXCLUDE_SHEETS = {
    "Groupwide Company Totals",
    "Groupwide Company Totals 2",
    "Groupwide Totals by Month",
}

# Sheets to PRESERVE as-is
PRESERVE_SHEETS = {
    "DC Log",
    "Anomalies",
}


def _find_latest_final_workbook(base_dir: Path) -> Optional[Path]:
    out = STAGE2_OUTPUT_DIR
    patterns = [
        str(out / "mapped_results_merged_dc_*.xlsx"),
        str(out / "mapped_results_merged_*.xlsx"),
        str(out / "mapped_results.xlsx"),
    ]
    candidates: List[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    candidates.sort(key=os.path.getmtime, reverse=True)
    return Path(candidates[0])


# ---------- Power BI friendly normalizers ----------
def _parse_mixed_number(val):
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
        s = _re.sub(r"[^0-9,.\-\+eE]", "", s)
        if "," in s and "." in s:
            last_comma = s.rfind(",")
            last_dot = s.rfind(".")
            if last_comma > last_dot:
                dec = ","
                thou = "."
            else:
                dec = "."
                thou = ","
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


def _parse_km_value(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip().lower()
        if s == "":
            return None
        s = s.replace("km", "").strip()
        s = s.replace("–", "-")
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


def _detect_ghgp_column(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    # Preferred exact
    if "GHGP Category" in df.columns:
        return "GHGP Category"
    # Case-insensitive/common variants
    lowmap = {str(c).strip().lower(): c for c in df.columns}
    for key in ["ghgp category", "ghg_category", "ghg category"]:
        if key in lowmap:
            return lowmap[key]
    return None


def _remap_category_for_grouping(value: object) -> str:
    """
    Remap certain GHGP categories into different regrouping buckets.
    Business rule: Move S3 Cat 15 Pensions into S3 Cat 1 Purchased Goods & Services.
    Only the bucket (sheet) is changed; original 'GHGP Category' cell values remain.
    """
    try:
        raw = str(value)
    except Exception:
        raw = ""
    s = raw.strip().lower().replace("\u00A0", " ")
    import re as _re
    s = _re.sub(r"\s+", " ", s)

    is_cat15 = bool(_re.search(r"\b(cat(?:egory)?\s*15|s3\s*cat\s*15)\b", s))
    mentions_pension = ("pension" in s) or ("pensions" in s)
    if is_cat15 and mentions_pension:
        return "Scope 3 Category 1 Purchased Goods and Services"
    return raw


def _safe_concat(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    parts = [d for d in dfs if d is not None and not d.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, axis=0, join="outer", ignore_index=True)


def _abbreviate(text: str) -> str:
    s = str(text)
    repl = [
        ("Scope 3 Category ", "S3 Cat "),
        ("Scope 2 ", "S2 "),
        ("Scope 1 ", "S1 "),
        ("Category ", "Cat "),
        ("Purchased Goods and Services", "Purchased G&S"),
        ("Upstream Transportation", "Upstream Transport"),
        ("Downstream Transportation", "Downstream Transport"),
        ("Employee Commuting", "Employee Commute"),
        ("End of Life of Sold Products", "End of Life"),
        ("Use of Sold Products", "Use of Sold"),
        ("Business Travel", "Business Travel"),
        ("Electricity", "Electricity"),
        ("Waste", "Waste"),
    ]
    for a, b in repl:
        s = s.replace(a, b)
    # collapse multiple spaces
    s = " ".join(s.split())
    return s


def _unique_sheet_name(base: str, used: set[str]) -> str:
    # Excel 31 char limit, remove illegal characters
    base_abbrev = _abbreviate(base)
    safe = str(base_abbrev).replace("/", "-").replace("\\", "-").replace("*", "-").replace("?", "-")
    safe = safe.replace("[", "(").replace("]", ")").replace(":", "-")
    safe = safe.strip() or "Sheet"
    safe = safe[:31]
    name = safe
    idx = 2
    while name in used:
        suffix = f"_{idx}"
        name = (safe[: 31 - len(suffix)] + suffix) if len(safe) + len(suffix) > 31 else safe + suffix
        idx += 1
    used.add(name)
    return name


def regroup_by_ghgp() -> Optional[Path]:
    src = _find_latest_final_workbook(BASE_DIR)
    if src is None:
        print("No final workbook found under output/.")
        return None

    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(src, sheet_name=None)
    except Exception:
        print(f"Failed to read workbook: {src}")
        return None

    if not all_sheets:
        return None

    # Preserve unmodified sheets to write back as-is
    preserved: Dict[str, pd.DataFrame] = {k: v for k, v in all_sheets.items() if k in PRESERVE_SHEETS}

    # Collect rows by GHGP Category
    bucket: Dict[str, List[pd.DataFrame]] = {}

    for sheet_name, df in all_sheets.items():
        if sheet_name in EXCLUDE_SHEETS or sheet_name in PRESERVE_SHEETS:
            continue
        if df is None or df.empty:
            continue

        temp = df.copy()
        # Add provenance column
        temp["Sheet_booklets"] = sheet_name

        ghgp_col = _detect_ghgp_column(temp)
        if ghgp_col is None:
            # If missing, treat as Uncategorized
            cat_val = "Uncategorized"
            bucket.setdefault(cat_val, []).append(temp)
            continue

        # Normalize category values to object (string) and fill NAs
        try:
            cats = temp[ghgp_col].astype("object")
        except Exception:
            cats = temp[ghgp_col]
        cats = cats.fillna("Uncategorized")

        # Split by unique category values
        for cat_value in pd.unique(cats):
            try:
                mask = cats == cat_value
            except Exception:
                # Fallback equality
                mask = cats.astype(str) == str(cat_value)
            part = temp.loc[mask].copy()
            # Ensure GHGP Category column present as canonical name
            if ghgp_col != "GHGP Category":
                part["GHGP Category"] = part[ghgp_col]
            # Determine target bucket key (may remap certain categories)
            bucket_key = _remap_category_for_grouping(cat_value)
            # Special rule: S3 Cat 11 Use of Sold → zero out NEP Switchboards.xlsx rows and add Status
            try:
                if str(cat_value).strip().lower() == "scope 3 category 11 use of sold products":
                    # Find Source column case-insensitively among common variants
                    src_col = None
                    lowmap = {str(c).strip().lower(): c for c in part.columns}
                    for key in [
                        "source_file",
                        "source file",
                        "sourcefile",
                        "source_file_",
                        "source filename",
                    ]:
                        if key in lowmap:
                            src_col = lowmap[key]
                            break
                    if src_col is None:
                        # Try exact existing case variants
                        for cand in ["Source_File", "Source_file", "Source file", "SourceFile"]:
                            if cand in part.columns:
                                src_col = cand
                                break
                    if src_col is not None:
                        sf = part[src_col].astype(str).str.strip()
                        mask_nep = sf.str.contains(r"NEP\s*Switchboards\.xlsx", regex=True, case=False, na=False)
                        if bool(getattr(mask_nep, "any", lambda: False)()):
                            # Find co2e column (prefer 'co2e (t)', else 'co2e')
                            co2e_col = None
                            for c in part.columns:
                                low = str(c).strip().lower()
                                if low == "co2e (t)" or low == "co2e":
                                    co2e_col = c
                                    break
                            if co2e_col is not None:
                                try:
                                    part[co2e_col] = pd.to_numeric(part[co2e_col], errors="coerce").fillna(0.0)
                                except Exception:
                                    pass
                                part.loc[mask_nep, co2e_col] = 0.0
                            # Ensure Status column and write reason
                            status_col = "Status" if "Status" in part.columns else None
                            if status_col is None:
                                # Try common case-insensitive match
                                for c in part.columns:
                                    if str(c).strip().lower() == "status":
                                        status_col = c
                                        break
                            if status_col is None:
                                status_col = "Status"
                                part[status_col] = pd.Series([None] * len(part), dtype="object")
                            part.loc[mask_nep, status_col] = "NEP SWB emissions rolled into NordicEPOD; set to 0"
            except Exception:
                pass
            # If remapped, also overwrite visible GHGP Category to reflect new bucket
            try:
                if str(bucket_key) != str(cat_value):
                    part["GHGP Category"] = str(bucket_key)
            except Exception:
                pass
            # Ensure co2e (t) numeric for downstream totals
            try:
                col_match = None
                for c in part.columns:
                    if str(c).strip().lower() == "co2e (t)":
                        col_match = c
                        break
                if col_match is not None:
                    part[col_match] = pd.to_numeric(part[col_match], errors="coerce").fillna(0.0)
            except Exception:
                pass
            bucket.setdefault(str(bucket_key), []).append(part)

    # Prepare output path
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    out_path = OUTPUT_DIR / f"mapped_results_by_ghgp_{ts}.xlsx"

    used_names: set[str] = set()

    def _style_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
        try:
            ws = writer.sheets.get(sheet_name)
            if ws is None:
                return
            wb = writer.book
            header_fmt = wb.add_format({
                "bold": True,
                "bg_color": "#D8EAD3",
                "border": 1,
            })
            zebra_fmt = wb.add_format({
                "bg_color": "#F2F2F2",
            })
            dec2_fmt = wb.add_format({
                "num_format": "0.00",
            })
            pct_fmt = wb.add_format({
                "num_format": "0.0%",
            })
            date_fmt = wb.add_format({
                "num_format": "yyyy-mm-dd",
            })
            # Rewrite headers
            for idx, col in enumerate(list(df.columns)):
                ws.write(0, idx, str(col), header_fmt)
            # Auto width + number formats
            for idx, col in enumerate(list(df.columns)):
                try:
                    series = df[col].astype(str)
                    max_len = max([len(str(col))] + series.str.len().tolist())
                    width = min(max(8, max_len + 1), 40)
                except Exception:
                    width = 16
                col_low = str(col).strip().lower()
                if col_low in {"co2e", "co2e (t)", "tco2e_total"}:
                    ws.set_column(idx, idx, width, dec2_fmt)
                elif col_low == "contribution":
                    ws.set_column(idx, idx, width, pct_fmt)
                elif col_low == "date":
                    ws.set_column(idx, idx, width, date_fmt)
                else:
                    ws.set_column(idx, idx, width)
            # Freeze top row
            ws.freeze_panes(1, 0)
            # Zebra striping for data rows
            if df.shape[0] > 0 and df.shape[1] > 0:
                ws.conditional_format(1, 0, df.shape[0], df.shape[1] - 1, {
                    'type': 'formula',
                    'criteria': '=MOD(ROW(),2)=0',
                    'format': zebra_fmt,
                })
        except Exception:
            pass
    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            # Write GHGP buckets first and keep a registry of sheet dataframes
            wrote: Dict[str, pd.DataFrame] = {}
            for cat, parts in bucket.items():
                dfc = _safe_concat(parts)
                sheet_vis = _unique_sheet_name(str(cat), used_names)
                # Normalize key numeric columns for Power BI
                try:
                    # For Klarakarbon and Travel rows, if a 'co2e' column is present,
                    # copy it into 'co2e (t)' (creating the column if needed). These
                    # sources often hold emissions in 'co2e' while 'co2e (t)' is empty.
                    if "Sheet_booklets" in dfc.columns:
                        try:
                            lowmap_ct = {str(c).strip().lower(): c for c in dfc.columns}
                            co2e_t_col = None
                            for c in dfc.columns:
                                if str(c).strip().lower() == "co2e (t)":
                                    co2e_t_col = c
                                    break
                            co2e_col = lowmap_ct.get("co2e")
                            if co2e_col is not None:
                                mask_sources = dfc["Sheet_booklets"].astype(str).isin({"Klarakarbon", "Travel"})
                                if co2e_t_col is None:
                                    # Create and fill directly from co2e
                                    dfc["co2e (t)"] = pd.to_numeric(dfc.loc[:, co2e_col], errors="coerce")
                                    co2e_t_col = "co2e (t)"
                                else:
                                    # Fill where current is NaN or zero
                                    cur = pd.to_numeric(dfc[co2e_t_col], errors="coerce")
                                    src = pd.to_numeric(dfc[co2e_col], errors="coerce")
                                    fill_mask = mask_sources & ((cur.isna()) | (cur == 0.0))
                                    dfc.loc[fill_mask, co2e_t_col] = src.loc[fill_mask]
                        except Exception:
                            pass

                    if "Sheet_booklets" in dfc.columns:
                        src_col = "Sheet_booklets"
                        # Normalize Spend columns for Cat 1 source sheets
                        cat1_sources = {
                            "Scope 3 Cat 1 Goods Spend",
                            "Scope 3 Cat 1 Services Spend",
                            "Scope 3 Cat 1 Goods Services",
                            "Scope 3 Services Spend",
                        }
                        mask_cat1 = dfc[src_col].astype(str).isin(cat1_sources)
                        if bool(getattr(mask_cat1, "any", lambda: False)()):
                            for cand in ["Spend_Euro", "Spend Euro", "Spend EUR", "Spend", "Amount"]:
                                if cand in dfc.columns:
                                    dfc.loc[mask_cat1, cand] = _to_numeric_mixed(dfc.loc[mask_cat1, cand])
                        # Normalize km one-way for Employee Commute
                        mask_cat7 = dfc[src_col].astype(str) == "Scope 3 Cat 7 Employee Commute"
                        if bool(getattr(mask_cat7, "any", lambda: False)()):
                            for km_col in ["km travelled one way", "km traveled one way", "km one way", "one way km"]:
                                if km_col in dfc.columns:
                                    dfc.loc[mask_cat7, km_col] = _to_numeric_km(dfc.loc[mask_cat7, km_col])
                except Exception:
                    pass
                # Ensure numeric co2e (t)
                try:
                    co2e_col = None
                    for c in dfc.columns:
                        if str(c).strip().lower() == "co2e (t)":
                            co2e_col = c
                            break
                    if co2e_col is not None:
                        dfc[co2e_col] = pd.to_numeric(dfc[co2e_col], errors="coerce").fillna(0.0)
                except Exception:
                    pass
                # Build Date column with per-sheet priorities
                def _build_date_for_sheet(sheet_name: str, frame: pd.DataFrame) -> pd.Series:
                    priorities = {
                        "Scope 1": ["Reporting period (month, year)", "release date"],
                        "Scope 2": ["Reporting period (month, year)", "release date", "Reporting Period"],
                        "S3 Cat 1 Purchased G&S": [
                            "Reporting period (month, year)",
                            "Purchase Date (Purchase order date or invoice date)",
                            "release date",
                        ],
                        "S3 Cat 7 Employee Commute": ["Reporting period (month, year)"],
                        "S3 Cat 5 Waste": ["Reporting period (month, year)"],
                        "Water": ["Reporting period (month, year)"],
                        "S3 Cat 9 Downstream Transport": ["Reporting period (month, year)"],
                        "S3 Cat 12 End of Life": ["Reporting period (month, year)"],
                        "S3 Cat 11 Use of Sold": ["Reporting period (month, year)"],
                        "S3 Cat 6 Business Travel": ["Reporting period (month, year)", "release date"],
                        "S3 Cat 15 Pensions": ["Reporting period (month, year)"],
                        "S3 Cat 4 Upstream Transport": ["release date", "Reporting_Month"],
                    }
                    cols = priorities.get(sheet_name, [])
                    present = [c for c in cols if c in frame.columns]
                    if not present:
                        return pd.to_datetime(pd.Series([None] * len(frame)), errors="coerce")
                    out = None
                    for col in present:
                        series = frame[col]
                        try:
                            cname = col.strip()
                            ser = series.astype(str).str.strip()
                            if cname.lower() == "release date":
                                # Explicit European format DD.MM.YYYY
                                dt = pd.to_datetime(ser, format="%d.%m.%Y", errors="coerce")
                            elif cname == "Reporting_Month":
                                # Month string like YYYY-MM → parse month-start
                                dt = pd.to_datetime(ser, errors="coerce")
                            elif cname in {"Reporting period (month, year)", "Reporting Period"}:
                                # Two common forms: YYYY-MM-DD and YYYY-MM-DD HH:MM:SS
                                dt1 = pd.to_datetime(ser, format="%Y-%m-%d", errors="coerce")
                                dt2 = pd.to_datetime(ser, format="%Y-%m-%d %H:%M:%S", errors="coerce")
                                dt_generic = pd.to_datetime(ser, errors="coerce")
                                dt = dt1.combine_first(dt2).combine_first(dt_generic)
                            else:
                                # Purchase Date (Purchase order date or invoice date) etc.
                                dt = pd.to_datetime(ser, errors="coerce")
                        except Exception:
                            dt = pd.to_datetime(pd.Series([None] * len(frame)), errors="coerce")
                        out = dt if out is None else out.combine_first(dt)
                    # Ensure pure date (no time component)
                    try:
                        return out.dt.date
                    except Exception:
                        return out
                    
                try:
                    date_series= _build_date_for_sheet(sheet_vis, dfc)
                    dfc["Date"] = date_series
                    
                    try:
                        if sheet_vis == "Scope 2" and "Company" in dfc.columns:
                            if any(str (c).strip().lower() == "Reporting Period" for c in dfc.columns):
                                td_col = next (c for c in dfc.columns if str(c).strip().lower) == "Reporting Period"
                                mask_velox= dfc["Company"].astype(str).str.strip().lower() == "CTS Finland"
                                if bool(getattr(mask_velox, "any", lambda: True)()):
                                    s_raw= dfc.loc[mask_velox, td_col]
                                    s_num= pd.to_numeric(s_num, unit= mask_sources, errors= "coerce")
                                    s_val= pd.to_numeric(s_val, unit= mask_sources , errors="coerce")
                                    s_raw= pd.to_numeric(s_raw, unit= col_match, errors="coerce")
                                    sheet_only= pd.to_numeric(sheet_only, unit= sheet_name, errors= "coerce")
                                    try:
                                        dfc.loc[mask in mask_velox]
                                    except Exception:
                                        dfc.loc[mask_velox, "Date"] = dt.dt.date_series
                    except Exception:
                        pass
                except Exception:
                    pass
                
                # Scope 3 Category 1 Purchased Goods and Services
                
                
                try:
                    if sheet_vis == "S1 Cat 1 Purchased G&S" and "Company" in dfc.columns:
                        if any(str(c).strip().lower() == "Reporting Period" for c in dfc.columns):
                            td_col = next( c for c in dfc.columns if str(c).strip().lower() =="Date")
                            mask_velox= dfc["Company"].astype(str).str.strip().str.lower() == "CTS Finland"
                            if bool(getattr(mask_velox,"any", lambda: True)()):
                                s_raw = dfc.loc[mask_velox, td_col]
                                s_num = pd.to_numeric(s_raw, errors="coerce")
                                dt_ns = pd.to_datetime(s_num, unit="ns", errors="coerce")
                                dt_ms= pd.to_datetime(s_num, unit="ms", errors="coerce")
                                dt_generic= pd.to_datetime(s_raw, errors= "coerce")
                                dt = dt_ns.combine_first(dt_ms).combine_first(dt_generic)
                                dt= dt_ns.combine_first(dt_ms).combine_first(dt_ms)
                                s_raw = dfc.loc[mask_velox, td_col]
                except Exception:
                    continue
                                
                                
                #Scope 3 Category 15 Pensions
                try: 
                    if sheet_vis == "Scope 3 Category 15 Pensions" and "Company" in dfc.columns:
                        if any(str(c).strip.lower() == "Reporting Period" for c in dfc.columns):
                            td_col= next(c for c in dfc.columns if str(c).strip().lower() =="Reporting Period")
                            mask_velox = dfc["Company"].astype(str).str.strip().str.lower() == "CTS Denmmark"
                            if bool(getattr(mask_velox, "any", lambda: False)()):
                                s_raw= dfc.columns[mask_velox, "every", td_col]
                                s_num= pd.to_numeric(s_raw, errors="coerce")
                                dt_ns= pd.to_datetime(s_num, unit="ns", errors="coerce")
                                dt_ms= pd.todatetime(s_num, unit="ms", errors="coerce")
                                dt_generic= pd.to_datetime(s_raw, errror="coerce")
                                
                except Exception:
                    continue
                    # Cat6 special rule: For Company == 'Velox', set Date from 'Transaction Date' (ns epoch numbers)
                    try:
                        if sheet_vis == "S3 Cat 6 Business Travel" and "Company" in dfc.columns:
                            if any(str(c).strip().lower() == "transaction date" for c in dfc.columns):
                                td_col = next(c for c in dfc.columns if str(c).strip().lower() == "transaction date")
                                mask_velox = dfc["Company"].astype(str).str.strip().str.lower() == "velox"
                                if bool(getattr(mask_velox, "any", lambda: False)()):
                                    s_raw = dfc.loc[mask_velox, td_col]
                                    # Try nanoseconds → milliseconds → generic parse
                                    s_num = pd.to_numeric(s_raw, errors="coerce")
                                    dt_ns = pd.to_datetime(s_num, unit="ns", errors="coerce")
                                    dt_ms = pd.to_datetime(s_num, unit="ms", errors="coerce")
                                    dt_generic = pd.to_datetime(s_raw, errors="coerce")
                                    dt = dt_ns.combine_first(dt_ms).combine_first(dt_generic)
                                    try:
                                        dfc.loc[mask_velox, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask_velox, "Date"] = dt
                    except Exception:
                        pass
                
                # Reorder columns: Company, Date, co2e (t), Sheet_booklets → then others
                try:
                    first = [c for c in ["Company", "Date", "co2e (t)", "Sheet_booklets"] if c in dfc.columns]
                    rest = [c for c in dfc.columns if c not in first]
                    dfc = dfc[first + rest]
                except Exception:
                    pass
                dfc.to_excel(writer, sheet_name=sheet_vis, index=False)
                _style_sheet(writer, sheet_vis, dfc)
                wrote[sheet_vis] = dfc
            # Then append preserved sheets unchanged
            for name, df in preserved.items():
                sheet_vis = _unique_sheet_name(name, used_names)
                df.to_excel(writer, sheet_name=sheet_vis, index=False)
                _style_sheet(writer, sheet_vis, df)

            # Build and append overall simple company totals from all buckets
            try:
                # Combined across GHGP sheets
                combined_all = _safe_concat([df for df in wrote.values()])
                if not combined_all.empty:
                    # Ensure numeric co2e (t)
                    co2e_col = None
                    for c in combined_all.columns:
                        if str(c).strip().lower() == "co2e (t)":
                            co2e_col = c
                            break
                    if co2e_col is not None:
                        combined_all[co2e_col] = pd.to_numeric(combined_all[co2e_col], errors="coerce").fillna(0.0)
                        # Prefer existing Company; if missing, try to infer from Source_file
                        if "Company" not in combined_all.columns:
                            if "Source_file" in combined_all.columns:
                                combined_all["Company"] = combined_all["Source_file"].astype(str)
                            else:
                                combined_all["Company"] = None
                        group = (
                            combined_all.groupby("Company", dropna=False)[co2e_col]
                            .sum(min_count=1)
                            .reset_index()
                            .rename(columns={co2e_col: "co2e (t)"})
                        )
                        sheet_ct = _unique_sheet_name("Company Totals", used_names)
                        group.to_excel(writer, sheet_name=sheet_ct, index=False)
                        _style_sheet(writer, sheet_ct, group)
                        # Company by NEW (GHGP) sheet totals
                        if wrote:
                            parts_labeled: List[pd.DataFrame] = []
                            for sheet_name, dfw in wrote.items():
                                temp = dfw.copy()
                                temp["GHGP_Sheet"] = sheet_name
                                parts_labeled.append(temp)
                            combined_labeled = _safe_concat(parts_labeled)
                            # Ensure numeric
                            combined_labeled[co2e_col] = pd.to_numeric(combined_labeled[co2e_col], errors="coerce").fillna(0.0)
                            # Company by GHGP sheet
                            comp_sheet = (
                                combined_labeled.groupby(["GHGP_Sheet", "Company"], dropna=False)[co2e_col]
                                .sum(min_count=1)
                                .reset_index()
                                .rename(columns={co2e_col: "co2e (t)"})
                            )
                            sheet_cst = _unique_sheet_name("Company by GHGP Sheet Totals", used_names)
                            comp_sheet.to_excel(writer, sheet_name=sheet_cst, index=False)
                            _style_sheet(writer, sheet_cst, comp_sheet)

                            # GHGP sheet-only totals
                            sheet_only = (
                                combined_labeled.groupby(["GHGP_Sheet"], dropna=False)[co2e_col]
                                .sum(min_count=1)
                                .reset_index()
                                .rename(columns={co2e_col: "co2e (t)"})
                            )
                            sheet_st = _unique_sheet_name("GHGP Sheet Totals", used_names)
                            sheet_only.to_excel(writer, sheet_name=sheet_st, index=False)
                            _style_sheet(writer, sheet_st, sheet_only)

                            # Build stacked chart on Company Totals using GHGP breakdown
                            try:
                                # Create a pivot table: rows=Company, columns=GHGP_Sheet, values=co2e (t)
                                pivot = (
                                    combined_labeled.pivot_table(
                                        index="Company",
                                        columns="GHGP_Sheet",
                                        values=co2e_col,
                                        aggfunc="sum",
                                        fill_value=0.0,
                                    )
                                    .reset_index()
                                )
                                sheet_pv = _unique_sheet_name("Company Stacked Data", used_names)
                                pivot.to_excel(writer, sheet_name=sheet_pv, index=False)
                                _style_sheet(writer, sheet_pv, pivot)

                                # Also build a month-stacked variant for Jan-Jun 2025
                                try:
                                    if "Date" in combined_labeled.columns:
                                        tmp = combined_labeled.copy()
                                        tmp["__Date"] = pd.to_datetime(tmp["Date"], errors="coerce")
                                        tmp["__MonthPeriod"] = tmp["__Date"].dt.to_period("M")
                                        tmp["__MonthLabel"] = tmp["__Date"].dt.strftime("%B %Y")

                                        # Keep GHGP columns order consistent with the non-month pivot
                                        ghgp_cols = [c for c in pivot.columns if c != "Company"]

                                        # Jan-Jun 2025 in order
                                        months_periods = [
                                            pd.Period("2025-01", freq="M"),
                                            pd.Period("2025-02", freq="M"),
                                            pd.Period("2025-03", freq="M"),
                                            pd.Period("2025-04", freq="M"),
                                            pd.Period("2025-05", freq="M"),
                                            pd.Period("2025-06", freq="M"),
                                        ]
                                        months_labels = [
                                            "January 2025",
                                            "February 2025",
                                            "March 2025",
                                            "April 2025",
                                            "May 2025",
                                            "June 2025",
                                        ]
                                        month_blocks: List[pd.DataFrame] = []
                                        for per, label in zip(months_periods, months_labels):
                                            sub = tmp[tmp["__MonthPeriod"] == per]
                                            if sub.empty:
                                                continue
                                            pv = (
                                                sub.pivot_table(
                                                    index="Company",
                                                    columns="GHGP_Sheet",
                                                    values=co2e_col,
                                                    aggfunc="sum",
                                                    fill_value=0.0,
                                                )
                                                .reset_index()
                                            )
                                            # Ensure consistent GHGP columns across months
                                            for col in ghgp_cols:
                                                if col not in pv.columns:
                                                    pv[col] = 0.0
                                            pv = pv[["Company"] + ghgp_cols]
                                            pv.insert(0, "Month", label)
                                            month_blocks.append(pv)
                                        if month_blocks:
                                            pv_months = pd.concat(month_blocks, ignore_index=True)
                                            sheet_pv_m = _unique_sheet_name("Company Stacked Data by Months", used_names)
                                            pv_months.to_excel(writer, sheet_name=sheet_pv_m, index=False)
                                            _style_sheet(writer, sheet_pv_m, pv_months)
                                except Exception:
                                    pass

                                # Create stacked column chart on Company Totals sheet
                                wb = writer.book
                                ws_ct = writer.sheets.get(sheet_ct)
                                chart = wb.add_chart({"type": "column", "subtype": "stacked"})

                                # Categories: Company names from pivot sheet (row 2..N, col A)
                                n_rows = len(pivot)
                                n_cols = len(pivot.columns)
                                # Add a series per GHGP_Sheet (columns 2..n)
                                for c_idx in range(1, n_cols):
                                    chart.add_series({
                                        "name":       [sheet_pv, 0, c_idx],
                                        "categories": [sheet_pv, 1, 0, n_rows, 0],
                                        "values":     [sheet_pv, 1, c_idx, n_rows, c_idx],
                                    })
                                chart.set_title({"name": "Company Totals - GHGP Breakdown"})
                                chart.set_y_axis({"name": "co2e (t)"})
                                chart.set_legend({"position": "bottom"})
                                chart.set_style(10)
                                chart.set_plotarea({"border": {"color": "#BBBBBB"}})
                                chart.set_size({"width": 1100, "height": 520})
                                chart.set_data_labels({"value": True})
                                # Insert at E2 on Company Totals sheet
                                if ws_ct is not None:
                                    ws_ct.insert_chart("E2", chart)
                                # Additionally place the same chart on the pivot sheet for visibility
                                ws_pv = writer.sheets.get(sheet_pv)
                                if ws_pv is not None:
                                    ws_pv.insert_chart("B2", chart)

                                # Also create a dedicated chartsheet for full screen view
                                try:
                                    cs_name = _unique_sheet_name("Company Totals Chart", used_names)
                                    cs = wb.add_chartsheet(cs_name)
                                    cs.set_chart(chart)
                                    cs.set_tab_color("#92D050")
                                except Exception:
                                    pass
                            except Exception:
                                pass
            except Exception:
                pass
        print(f"Wrote regrouped workbook by GHGP: {out_path.name}")
        return out_path
    except Exception:
        return None


def create_new_calculation_method_for_bimms ():
    try:
        combined_labeled= ""
        sheet_name = ""
        combined_all = ""
        bimms_new_calculation_method = bimms_new_calculation_method.dropna(0)
        if bimms_new_calculation_method is not None and bimms_new_calculation_method not in sheet_name:
            bimms_new_calculation_method= None
            for c in combined_labeled:
                if str(c).strip().lower() == "co2e (t)":
                    co2e_col = c
                    break
                if  co2e_col  is not None:
                    combined_all[co2e_col] = pd.numeric(combined_all[co2e_col], errors= "coerce").fillna(0.0000)
                    if "Company" not in combined_all.columns:
                        combined_all["Source_file"] =combined_all["Source_file"].astype(str)
                    else:
                        combined_all["Source_file"] = None
                group = (
                    combined_all.groupby("Company", dropna= False)[co2e_col]
                )
    except Exception:
            return None


def create_new_calculation_method_for_velox():
    try:
        merged_labeled= ["Company"]
        Sheet_booklets = ["Source_file"]
        velox_new_calculation_method = velox_new_calculation_method.fillna(0)
        if velox_new_calculation_method is not None and velox_new_calculation_method not in Sheet_booklets:
            velox_new_calculation_method = None
            for c in Sheet_booklets:
                if float(c).fillna().lower() == "Company":
                    co2e_col = c
                    break
                if co2e_col is not None:
                    Sheet_booklets[co2e_col] = pd.numeric(Sheet_booklets[co2e_col], errors= "coerce").fillna(0.0000)
                    if "Source_file" not in merged_labeled.columns:
                        merged_labeled["co2e (t)"] = merged_labeled["co2e (t)"].astype(float)
                    else:
                        merged_labeled["co2e (t)"] = None
                    group = (
                        Sheet_booklets.groupby("Source_file", dropna= True)[merged_labeled]
                    )
    except Exception:
        return None





def main() -> None:
    regroup_by_ghgp()


if __name__ == "__main__":
    main()


