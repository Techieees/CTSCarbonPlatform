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
from excel_writer_utils import preferred_excel_writer_engine


# This script reads the latest mapped results workbook (output/mapped_results*.xlsx)
# and writes per-row results into the same workbook under a column named 'co2e',
# using ONLY the rules provided by the Sustainability Data Analyst. Rules are strict: no extra headers.


Rule = Tuple[str, Tuple[str, ...]]  # (rule_kind, (colA, colB?, ...))


SHEET_RULES: Dict[str, Rule] = {
    # Scope 1
    "Scope 1 Fuel Usage Spend": ("mul", ("Spend_Euro", "ef_value")),
    "Scope 1 Fuel Activity": ("mul", ("Fuel consumption", "ef_value")),
    "Scope 2 Electricity": ("mul", ("Consumption", "ef_value")),
    "Scope 1 Fuel Usage Activity": ("mul", ("Fuel consumption", "ef_value")),
    "Scope 1 Fugitive Gases": ("zero", ()),  # Directly zero per user
    "Scope 1 Gas Usage": ("zero", ()),       # Directly zero per user

    # Scope 2
    "Scope 2 Electricity Average": ("copy", ("CO2e from electricity",)),

    # Scope 3 Cat 1
    "Scope 3 Cat 1 Goods Spend": ("mul", ("Spend_Euro", "ef_value")),
    "Scope 3 Cat 1 Goods Activity": ("skip", ()),
    "Scope 3 Cat 1 Common Purchases": ("skip", ()),
    "Scope 3 Cat 1 Services Spend": ("mul", ("Spend_Euro", "ef_value")),
    # NOTE: This non-Cat1 sheet is still Bucketed into Purchased G&S downstream
    # (see regrouping logic) and must be spend-based calculated.
    "Scope 3 Services Spend": ("mul", ("Spend_Euro", "ef_value")),
    "Scope 3 Cat 1 Services Activity": ("skip", ()),
    "Scope 3 Cat 1 Supplier Summary": ("skip", ()),
    "Scope 3 Cat 1 Goods Services": ("mul", ("Spend_Euro", "ef_value")),

    # Scope 3 Cat 2
    "Scope 3 Cat 2 Capital Goods Spe": ("zero", ()),
    "Scope 3 Cat 2 Capital Goods Act": ("zero", ()),

    # Scope 3 Cat 4+9
    "Scope 3 Cat 4+9 Transport Spend": ("skip", ()),
    "Scope 3 Cat 4+9 Transport Act": ("skip", ()),

    # Scope 3 Cat 5
    # Weight is stored in mixed units; we always compute in tonnes.
    # Weight(tonnes) = Weight / 1000 when unit is kg or litres; otherwise keep as-is for tn/tonnes/tons.
    "Scope 3 Cat 5 Waste": ("mul_weight_ton", ("Weight", "Weight unit", "ef_value")),
    "Scope 3 Cat 5 Waste 2": ("skip", ()),
    # Do NOT copy Position Green calculated CO2e anymore; always recompute from Weight + ef_value
    "Scope 3 Cat 5 Office Waste": ("mul_weight_ton", ("Weight", "Weight unit", "ef_value")),
    "Scope 3 Cat 5 Office Waste 2": ("skip", ()),
    "Scope 3 Cat 5 Waste Oslo": ("mul_weight_ton", ("Weight", "Weight unit", "ef_value")),
    "Scope 3 Cat 5 Waste Oslo 2": ("skip", ()),

    # Abbreviated names that appear after regrouping/cleaning (safe no-op if not present)
    "S3 Cat 5 Waste": ("mul_weight_ton", ("Weight", "Weight unit", "ef_value")),
    "S3 Cat 5 Office Waste": ("mul_weight_ton", ("Weight", "Weight unit", "ef_value")),
    "S3 Cat 5 Waste Oslo": ("mul_weight_ton", ("Weight", "Weight unit", "ef_value")),

    # Scope 3 Cat 6
    "Scope 3 Cat 6 Business Travel": ("mul", ("Spend_Euro", "ef_value")),
    "Scope 3 Cat 6 Business Travel S": ("skip", ()),
    "Scope 3 Cat 6 Business Travel A": ("skip", ()),

    # Scope 3 Cat 7
    "Scope 3 Cat 7 Employee Commute": ("mul", ("km travelled per month", "ef_value")),

    # Scope 3 Cat 8
    "Scope 3 Cat 8 Electricity": ("skip", ()),
    "Scope 3 Cat 8 District Heating": ("skip", ()),
    "Scope 3 Cat 8 Fuel Usage Spend": ("mul", ("Spend_Euro", "ef_value")),
    "Scope 3 Cat 8 Fuel Usage Activi": ("mul", ("Fuel consumption", "ef_value")),
    "S3C8_Electricity_extracted": ("copy", ("CO2e from electricity",)),
    "Scope 3 Cat 8 District E": ("copy", ("CO2e from electricity",)),
    "Scope 3 Cat 8 District H": ("copy", ("CO2e from district heating",)),

    # Scope 3 Cat 11
    "Scope 3 Cat 11 Products Indirec": (
        "copy",
        ("CO2e emissions from indirect scenarios, Scope 3 (tonnes CO2e)",),
    ),
    "Scope 3 Category 11 Scenario": (
        "copy",
        ("CO2e emissions from electricity consumed from use scenarios (tonnes CO2e)",),
    ),

    # Scope 3 Category 9 Activity (pre-computed) — copy as-is
    "Scope 3 Category 9 Activity": (
        "copy",
        ("Total t CO2e/t-km",),
    ),

    # Scope 3 Cat 12
    # Multiply Product weight (kg/ton aware) by ef_value (tCO2e/t)
    "Scope 3 Cat 12 End of Life": ("mul_weight_ton", ("Product weight (including packaging, if available)", "weight unit", "ef_value")),

    # Scope 3 Cat 15
    "Scope 3 Cat 15 Pensions": ("mul", ("Spend_Euro", "ef_value")),

    # Others
    "Calculation Methods": ("skip", ()),
    "WT_extracted": ("skip", ()),
    "Water Tracker Averages": ("skip", ()),
}


def find_latest_mapped_results(base_dir: Path) -> Optional[Path]:
    output_dir = STAGE2_OUTPUT_DIR

    # 1) Prefer direct children (fast path)
    patterns = [
        str(output_dir / "mapped_results.xlsx"),
        str(output_dir / "mapped_results_*.xlsx"),
    ]
    candidates: List[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))

    # Filter obvious non-targets (window/by_ghgp/clean files)
    def _keep(path_str: str) -> bool:
        name = os.path.basename(path_str).lower()
        if name.startswith("~$"):
            return False
        if "mapped_results_window_" in name:
            return False
        if "mapped_results_by_ghgp" in name:
            return False
        if "mapped_results_by-ghgp" in name:
            return False
        if "clean" in name and "mapped_results" in name:
            return False
        return True

    candidates = [c for c in candidates if _keep(c)]

    # 2) Fallback: search recursively (handles when files are moved into subfolders)
    if not candidates:
        try:
            rec = []
            for p in output_dir.rglob("mapped_results*.xlsx"):
                if _keep(str(p)):
                    rec.append(str(p))
            candidates = rec
        except Exception:
            candidates = []

    if not candidates:
        return None

    candidates.sort(key=os.path.getmtime, reverse=True)
    return Path(candidates[0])


def _parse_mixed_number(val) -> Optional[float]:
    """Parse numbers that may use either ',' or '.' as decimal separator.

    Rules:
      - If both separators appear, the last separator is treated as decimal;
        the other is removed as thousands separator (e.g., '1.234,56' -> 1234.56,
        '1,234.56' -> 1234.56).
      - If only ',' appears, it is treated as decimal (e.g., '0,000581' -> 0.000581).
      - Spaces and non-breaking spaces are removed. Non-numeric trailing text is stripped.
    """
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        # Fast path for real numerics
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if s == "":
            return None
        # Remove spaces and NBSP
        s = s.replace("\u00A0", "").replace(" ", "")
        # Keep only digits, separators, signs and exponent markers
        s = re.sub(r"[^0-9,\.\-\+eE]", "", s)

        if "," in s and "." in s:
            # Determine decimal sep as the last occurrence of either
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
            # Only one or none: treat comma as decimal
            s = s.replace(",", ".")

        # Final coercion
        return float(s)
    except Exception:
        return None


def to_numeric(series: pd.Series) -> pd.Series:
    """Vectorized conversion using _parse_mixed_number with fallback to pandas coercion."""
    try:
        parsed = series.map(_parse_mixed_number)
        return pd.to_numeric(parsed, errors="coerce")
    except Exception:
        return pd.to_numeric(series, errors="coerce")


def _normalize_colname(col: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(col).lower())


def _find_first_present_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    normalized_map = {_normalize_colname(c): c for c in df.columns}
    for candidate in candidates:
        cand_norm = _normalize_colname(candidate)
        if cand_norm in normalized_map:
            return normalized_map[cand_norm]
    return None


def _weight_to_tonnes(weight_series: pd.Series, unit_series: pd.Series) -> pd.Series:
    """Convert weights given unit column to tonnes (t).

    - If unit is 'kg' (case-insensitive), divide by 1000.
    - If unit is litres/liters, divide by 1000. (Requested: treat litres like kg for tonnes conversion)
    - If unit is 't' or 'ton'/'tonne' variants, keep as is.
    - Unknown/empty unit → assume input already in tonnes.
    """
    w = to_numeric(weight_series)
    u = unit_series.astype(str).str.strip().str.lower()
    is_kg = u == "kg"
    is_litres = u.str.contains(r"\blit(er|re)s?\b", regex=True, na=False) | u.isin({"l", "lt", "liter", "litre", "liters", "litres"})
    is_ton = (
        (u == "t")
        | (u == "tn")
        | u.str.contains(r"\btn\b|\bton\b|\btonne\b|\btonnes\b|\btons\b", regex=True, na=False)
    )
    # Default: treat as tonnes
    out = w.copy()
    out[is_kg | is_litres] = w[is_kg | is_litres] / 1000.0
    # is_ton -> unchanged
    return out


def compute_series_for_sheet(df: pd.DataFrame, rule: Rule) -> Optional[pd.Series]:
    kind, cols = rule

    # Respect user's constraints strictly
    if kind == "skip":
        # Do not modify; return None to signal no-change
        return None
    if kind == "zero":
        return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
    if kind == "mul":
        if len(cols) != 2:
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        col_a, col_b = cols
        if col_a not in df.columns or col_b not in df.columns:
            # Column missing → treat as zeros
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        a = to_numeric(df[col_a])
        b = to_numeric(df[col_b])
        return a * b
    if kind == "mul_div1000":
        if len(cols) != 2:
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        col_a, col_b = cols
        if col_a not in df.columns or col_b not in df.columns:
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        a = to_numeric(df[col_a])
        b = to_numeric(df[col_b])
        return (a * b) / 1000.0
    if kind == "mul_weight_ton":
        # cols: (primary_weight_col, unit_col, ef_value_col)
        if len(cols) != 3:
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        w_col_in, u_col_in, ef_col_in = cols
        # Prefer explicit Weight where filled; fallback to Product weight (including packaging)
        weight_pref_col = _find_first_present_column(df, [
            "Weight",
            "weight",
            "Weight (kg)",
            "weight (kg)",
        ])
        product_weight_col = w_col_in if w_col_in in df.columns else _find_first_present_column(df, [
            w_col_in,
            "Product weight (including packaging, if available)",
            "Product weight",
            "Product Weight",
        ])
        u_col = u_col_in if u_col_in in df.columns else _find_first_present_column(df, [
            u_col_in,
            "Weight unit",
            "weight unit",
            "Weight Unit",
            "Unit",
            "Units",
        ])
        ef_col = ef_col_in if ef_col_in in df.columns else _find_first_present_column(df, [
            ef_col_in,
            "EF Value",
            "ef value",
            "Value",
        ])
        if not all([product_weight_col, ef_col]) or any(c not in df.columns for c in [product_weight_col, ef_col]) or (u_col and u_col not in df.columns):
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")

        # Build base weight by preferring Weight where non-empty
        pw = to_numeric(df[product_weight_col])
        if weight_pref_col and weight_pref_col in df.columns:
            w_pref = to_numeric(df[weight_pref_col])
            base_weight = pw.copy()
            use_pref = w_pref.notna() & (w_pref.astype(float) != 0.0)
            base_weight.loc[use_pref] = w_pref.loc[use_pref]
            # Prepare unit series
            unit_series = df[u_col].astype(str) if u_col else pd.Series([""] * len(df), index=df.index, dtype="object")
            # If "Weight (kg)" is used, force 'kg' where we used preferred column
            if weight_pref_col and "(kg)" in str(weight_pref_col).lower():
                unit_series.loc[use_pref] = "kg"
        else:
            base_weight = pw
            unit_series = df[u_col].astype(str) if u_col else pd.Series([""] * len(df), index=df.index, dtype="object")

        weight_t = _weight_to_tonnes(base_weight, unit_series)
        ef = to_numeric(df[ef_col])
        return weight_t * ef
    if kind == "copy":
        if len(cols) != 1:
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        col = cols[0]
        if col not in df.columns:
            # Column missing → zeros
            return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        return to_numeric(df[col])

    return pd.Series([0.0] * len(df), index=df.index, dtype="float64")


def compute_sheet_total(xl_path: Path, sheet_name: str, rule: Rule) -> float:
    try:
        xl = pd.ExcelFile(xl_path)
        if sheet_name not in xl.sheet_names:
            # If sheet not present, contributes 0
            return 0.0
        df = pd.read_excel(xl_path, sheet_name=sheet_name)
    except Exception:
        return 0.0

    series = compute_series_for_sheet(df, rule)
    if series is None:
        return 0.0
    return float(series.sum(skipna=True)) if len(series) else 0.0


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    target = find_latest_mapped_results(base_dir)
    if target is None:
        print("No mapped_results*.xlsx found under output/.")
        return

    # Load entire workbook once
    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(target, sheet_name=None)
    except Exception:
        print(f"Failed to read workbook: {target}")
        return

    # Initialize co2e=0.0 for all sheets
    for name, df in all_sheets.items():
        zeros = pd.Series([0.0] * len(df), index=df.index, dtype="float64")
        df["co2e"] = zeros
        all_sheets[name] = df

    grand_total = 0.0
    per_sheet_totals: List[Tuple[str, float]] = []

    for sheet, rule in SHEET_RULES.items():
        if sheet not in all_sheets:
            continue
        df = all_sheets[sheet].copy()
        kind, cols = rule
        # Special handling: Scope 1 Fuel Usage Spend → branch by Currency
        if sheet == "Scope 1 Fuel Usage Spend":
            cur_col = _find_first_present_column(df, ["currency", "Currency"])
            ef_val_col = _find_first_present_column(df, ["ef_value", "EF Value", "Value"]) 
            spend_col = _find_first_present_column(df, ["Spend_Euro", "Spend EUR", "Spend Euro"]) 
            # Try robust detection for the ratio column (Total litres per spend)
            ratio_candidates = [
                "Total litres / spend",
                "Total litres/spend",
                "Total liters / spend",
                "Total liters/spend",
                "Litres per spend",
                "Liters per spend",
            ]
            litres_ratio_col = _find_first_present_column(df, ratio_candidates)
            if litres_ratio_col is None:
                # Fallback: pick first column whose normalized name contains both 'liter' and 'spend'
                try:
                    for c in df.columns:
                        low = str(c).strip().lower()
                        low_norm = low.replace(" ", "").replace("/", "")
                        if ("liter" in low_norm or "litre" in low_norm) and ("spend" in low_norm):
                            litres_ratio_col = c
                            break
                except Exception:
                    litres_ratio_col = None

            if ef_val_col is not None and (spend_col is not None or litres_ratio_col is not None):
                ef_vals = to_numeric(df[ef_val_col]).fillna(0.0)
                spend_vals = to_numeric(df[spend_col]).fillna(0.0) if spend_col in df.columns else pd.Series([0.0]*len(df), index=df.index, dtype="float64")
                ratio_vals = to_numeric(df[litres_ratio_col]).fillna(0.0) if litres_ratio_col in df.columns else pd.Series([0.0]*len(df), index=df.index, dtype="float64")
                cur_series = df[cur_col].astype(str).str.strip().str.lower() if cur_col in df.columns else pd.Series([""]*len(df), index=df.index, dtype="object")
                liters_mask = cur_series.str.contains(r"lit(er|re)s?\b", regex=True, na=False)
                # Also treat rows as liters-based if ratio column is present and positive
                if litres_ratio_col in df.columns:
                    liters_mask = liters_mask | (ratio_vals > 0)

                series = pd.Series([0.0] * len(df), index=df.index, dtype="float64")
                # liters rows: (Total litres / spend) * ef_value
                series[liters_mask] = (ratio_vals * ef_vals)[liters_mask].fillna(0.0)
                # other rows: spend * ef_value
                series[~liters_mask] = (spend_vals * ef_vals)[~liters_mask].fillna(0.0)
                df["co2e"] = series
                all_sheets[sheet] = df
                total = float(series.sum(skipna=True)) if len(series) else 0.0
                grand_total += total
                per_sheet_totals.append((sheet, total))
                continue
        # Special handling: Scope 1 Fuel Usage Activity can be distance- or fuel-based
        if sheet == "Scope 1 Fuel Usage Activity":
            # Prefer Distance travelled when EF Unit indicates per-km
            ef_unit_col = _find_first_present_column(df, [
                "ef_unit", "EF Unit", "Unit", "Units"
            ])
            ef_val_col = _find_first_present_column(df, [
                "ef_value", "EF Value", "Value"
            ])
            dist_col = _find_first_present_column(df, [
                "Distance travelled", "Distance Travelled", "Distance", "km travelled", "km"
            ])
            fuel_col = _find_first_present_column(df, [
                "Fuel consumption", "Fuel Consumption"
            ])

            if ef_unit_col and ef_val_col and (dist_col or fuel_col):
                ef_unit_series = df[ef_unit_col].astype(str).str.lower()
                use_km_mask = ef_unit_series.str.contains("km", na=False)
                ef_vals = to_numeric(df[ef_val_col]).fillna(0.0)
                dist_vals = (to_numeric(df[dist_col]).fillna(0.0) if dist_col in df.columns else pd.Series([0.0]*len(df), index=df.index, dtype="float64"))
                fuel_vals = (to_numeric(df[fuel_col]).fillna(0.0) if fuel_col in df.columns else pd.Series([0.0]*len(df), index=df.index, dtype="float64"))

                series = pd.Series([0.0] * len(df), index=df.index, dtype="float64")
                if len(series) == len(df):
                    # Distance-based (only where EF per km) → distance * ef_value
                    series[use_km_mask] = (dist_vals * ef_vals)[use_km_mask].fillna(0.0)
                    # Fuel-based (other rows) → fuel_consumption * ef_value
                    series[~use_km_mask] = (fuel_vals * ef_vals)[~use_km_mask].fillna(0.0)
                df["co2e"] = series
                all_sheets[sheet] = df
                total = float(series.sum(skipna=True)) if len(series) else 0.0
                grand_total += total
                per_sheet_totals.append((sheet, total))
                continue

        

        if kind == "mul_weight_ton":
            # cols: (weight_col, unit_col, ef_value_col)
            # Resolve columns robustly (case/space tolerant)
            w_col_in, u_col_in, ef_col_in = cols if len(cols) == 3 else (None, None, None)
            # Prefer explicit Weight where filled; fallback to Product weight (including packaging)
            weight_pref_col = _find_first_present_column(df, [
                "Weight",
                "weight",
                "Weight (kg)",
                "weight (kg)",
            ])
            product_weight_col = w_col_in if (w_col_in and w_col_in in df.columns) else _find_first_present_column(df, [
                w_col_in if w_col_in else "",
                "Product weight (including packaging, if available)",
                "Product weight",
                "Product Weight",
            ])
            u_col = None if u_col_in is None else (u_col_in if u_col_in in df.columns else _find_first_present_column(df, [
                u_col_in,
                "Weight unit",
                "weight unit",
                "Weight Unit",
                "Unit",
                "Units",
            ]))
            ef_col = None if ef_col_in is None else (ef_col_in if ef_col_in in df.columns else _find_first_present_column(df, [
                ef_col_in,
                "ef_value",
                "EF Value",
                "Value",
            ]))

            if not all([product_weight_col, ef_col]) or (u_col and u_col not in df.columns):
                series = pd.Series([0.0] * len(df), index=df.index, dtype="float64")
            else:
                # Build base weight by preferring Weight where non-empty
                pw = to_numeric(df[product_weight_col])
                if weight_pref_col and weight_pref_col in df.columns:
                    w_pref = to_numeric(df[weight_pref_col])
                    base_weight = pw.copy()
                    use_pref = w_pref.notna() & (w_pref.astype(float) != 0.0)
                    base_weight.loc[use_pref] = w_pref.loc[use_pref]
                    unit_series = df[u_col].astype(str) if u_col else pd.Series([""] * len(df), index=df.index, dtype="object")
                    if weight_pref_col and "(kg)" in str(weight_pref_col).lower():
                        unit_series.loc[use_pref] = "kg"
                else:
                    base_weight = pw
                    unit_series = df[u_col].astype(str) if u_col else pd.Series([""] * len(df), index=df.index, dtype="object")

                weight_t = _weight_to_tonnes(base_weight, unit_series)
                # Keep old helper + add user-friendly column name
                df["Weight_tonnes"] = weight_t
                df["Weight(tonnes)"] = weight_t
                ef = to_numeric(df[ef_col])
                series = weight_t * ef
            if len(series) != len(df):
                series = pd.Series([0.0] * len(df), index=df.index, dtype="float64")
            df["co2e"] = series
            total = float(series.sum(skipna=True)) if len(series) else 0.0
        else:
            series = compute_series_for_sheet(df, rule)
            if series is None:
                # keep zeros as initialized
                total = 0.0
            else:
                if len(series) != len(df):
                    series = pd.Series([0.0] * len(df), index=df.index, dtype="float64")
                df["co2e"] = series
                total = float(series.sum(skipna=True)) if len(series) else 0.0

        # Special-case override: Scope 3 Cat 12 End of Life for NordicEPOD.xlsx (NEW VERSION)
        # Uses 3 waste streams only: Recycling, Energy Recovery, To Landfill
        if sheet == "Scope 3 Cat 12 End of Life":

            lowmap = {str(c).strip().lower(): c for c in df.columns}
            sf_col = None
            for key in ["source_file", "source file", "sourcefile", "source_file_"]:
                if key in lowmap:
                    sf_col = lowmap[key]
                    break
            if sf_col is None:
                for cand in ["Source_File", "Source_file", "Source file", "SourceFile"]:
                    if cand in df.columns:
                        sf_col = cand
                        break

            if sf_col is not None and sf_col in df.columns:
                sf_norm = df[sf_col].astype(str).str.strip().str.lower()
                mask_nep = sf_norm == "nordicepod.xlsx"

                if bool(getattr(mask_nep, "any", lambda: False)()):

                    def _find_by_label(label: str) -> str | None:
                        target = _normalize_colname(label)
                        for c in df.columns:
                            if _normalize_colname(str(c)) == target:
                                return c
                        return None

                    # New 3-stream structure
                    c1 = _find_by_label("weight of waste for waste stream 1")  # Recycling
                    c2 = _find_by_label("weight of waste for waste stream 2")  # Energy Recovery
                    c3 = _find_by_label("weight of waste for waste stream 3")  # To Landfill

                    # EF constants
                    EF_305A018 = 0.00641061   # Recycling 
                    EF_305A020 = 0.5203342   # To Landfill
                    EF_30A5A30 = 0.0212808072368763 #Energy Recovery

                    zero = pd.Series([0.0] * len(df), index=df.index, dtype="float64")

                    s_recycling = (to_numeric(df[c1]) * EF_305A018 / 1000.0) if c1 in df.columns else zero
                    s_energy = (to_numeric(df[c2]) * EF_30A5A30 / 1000.0) if c2 in df.columns else zero
                    s_landfill = (to_numeric(df[c3]) * EF_305A020 / 1000.0) if c3 in df.columns else zero

                    total_streams = (s_recycling + s_energy + s_landfill).astype("float64")

                    if "co2e (t)" not in df.columns:
                        df["co2e (t)"] = pd.Series([None] * len(df), dtype="object")

                    df.loc[mask_nep, "co2e (t)"] = total_streams.loc[mask_nep].values
                    df.loc[mask_nep, "co2e"] = total_streams.loc[mask_nep].values

                    try:
                        total = float(pd.to_numeric(df["co2e"], errors="coerce").sum(skipna=True))
                    except Exception:
                        pass

        all_sheets[sheet] = df
        grand_total += total
        per_sheet_totals.append((sheet, total))

    # Write back to the same workbook; if locked, write timestamped copy
    writer_engine = preferred_excel_writer_engine()
    try:
        with pd.ExcelWriter(target, engine=writer_engine) as writer:
            for name, df in all_sheets.items():
                safe_name = name[:31] if len(name) > 31 else name
                df.to_excel(writer, sheet_name=safe_name, index=False)
        written_path = target
    except PermissionError:
        ts_name = target.with_name(f"{target.stem}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}{target.suffix}")
        with pd.ExcelWriter(ts_name, engine=writer_engine) as writer:
            for name, df in all_sheets.items():
                safe_name = name[:31] if len(name) > 31 else name
                df.to_excel(writer, sheet_name=safe_name, index=False)
        written_path = ts_name

    # Print concise summary
    per_sheet_totals.sort(key=lambda x: x[0])
    print(f"Updated workbook: {written_path.name}")
    print("Sample totals (first 10):")
    for sheet, total in per_sheet_totals[:10]:
        print(f"- {sheet}: {total}")
    print(f"GRAND TOTAL tCO2e (modified sheets only): {grand_total}")


if __name__ == "__main__":
    main()


