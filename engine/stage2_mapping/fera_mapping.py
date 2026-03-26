from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import glob
import os
import re
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR

import mapping_utils as mu

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR

# Visible new sheet names
FERA_FUEL_SHEET = "Scope 3 Cat 3 FERA Fuel"
FERA_ELEC_SHEET = "Scope 3 Cat 3 FERA Electricity"

# Target GHGP Category labels for new sheets
FERA_FUEL_GHGP = "Scope 3 Category 3 Fuel and Energy Related Activities - Fuel"
FERA_ELEC_GHGP = "Scope 3 Category 3 Fuel and Energy Related Activities - Electricity"


def _find_latest_workbook_for_mera(base_dir: Path) -> Optional[Path]:
    """
    Pick the most recent mapped workbook, preferring merged/DC variants (including _with_sources copies).
    """
    out = STAGE2_OUTPUT_DIR
    patterns = [
        str(out / "mapped_results_merged_dc_*.xlsx"),
        str(out / "mapped_results_merged_*.xlsx"),
        str(out / "mapped_results_*.xlsx"),
        str(out / "mapped_results.xlsx"),
    ]
    candidates: List[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    # Avoid re-consuming our own derived FERA outputs (would cause recursion/empty sheets)
    filtered: List[str] = []
    for p in candidates:
        name = Path(p).name.lower()
        # Skip non-base artifacts
        if name.startswith("~$"):
            continue
        if "merged_fera" in name:
            continue
        if "by_ghgp" in name or "byghgp" in name:
            continue
        if "window" in name:
            continue
        if "clean" in name:
            continue
        filtered.append(p)
    candidates = filtered if filtered else candidates
    candidates.sort(key=os.path.getmtime, reverse=True)
    # Return the first candidate that is a readable Excel workbook
    for path_str in candidates:
        try:
            # Quick sanity check: open workbook without loading all sheets
            _ = pd.ExcelFile(path_str, engine="openpyxl")
            return Path(path_str)
        except Exception:
            continue
    return Path(candidates[0])


def _get_ci_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Case-insensitive/space-insensitive column matcher. Returns the first present candidate.
    """
    lowmap = {re.sub(r"\s+", " ", str(c).strip().lower()): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r"\s+", " ", str(cand).strip().lower())
        if key in lowmap:
            return lowmap[key]
    # relaxed substring search
    for cand in candidates:
        norm = re.sub(r"[^a-z0-9]", "", str(cand).lower())
        for low, orig in lowmap.items():
            low_norm = re.sub(r"[^a-z0-9]", "", low)
            if norm and norm in low_norm:
                return orig
    return None


def _to_numeric(series: pd.Series) -> pd.Series:
    try:
        s = series.astype(str).str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
        return pd.to_numeric(s, errors="coerce")
    except Exception:
        return pd.to_numeric(series, errors="coerce")


def _is_euro_based_unit(u: Optional[str]) -> Tuple[bool, Optional[str]]:
    if u is None:
        return False, None
    try:
        txt = str(u)
    except Exception:
        return False, None
    euro_based = bool(
        ("€/" in txt)
        or re.search(r"(EUR|€)\s*/", txt, flags=re.IGNORECASE)
        or re.search(r"per\s*(EUR|€)", txt, flags=re.IGNORECASE)
    )
    if not euro_based:
        return False, None
    low = txt.lower().replace("co2e", "")
    if "kg" in low:
        return True, "kg"
    if re.search(r"\bt\b", low) or (" tonne" in low) or (" ton/" in low):
        return True, "t"
    return True, None


def _compute_euro_based_tco2e(spend_eur: Optional[float], ef_value: Optional[float], ef_unit: Optional[str]) -> Optional[float]:
    if spend_eur is None or ef_value is None:
        return None
    try:
        s = float(spend_eur)
        v = float(ef_value)
    except Exception:
        return None
    is_euro, mass = _is_euro_based_unit(ef_unit)
    if not is_euro:
        return None
    if mass == "kg":
        return (s * v) / 1000.0
    return s * v


def _compute_activity_based_tco2e(activity: Optional[float], ef_value: Optional[float], ef_unit: Optional[str]) -> Optional[float]:
    if activity is None or ef_value is None:
        return None
    try:
        a = float(activity)
        v = float(ef_value)
    except Exception:
        return None
    # If EF unit suggests kg-based, convert to tonnes
    if isinstance(ef_unit, str) and ("kg" in ef_unit.lower()):
        return (a * v) / 1000.0
    return a * v


def _map_rows(df: pd.DataFrame, ef_dict: Dict[str, pd.DataFrame], visible_sheet_name: str) -> pd.DataFrame:
    """
    Apply EF mapping for each row using mapping_utils.map_emission_factor by forcing 'Sheet' to visible_sheet_name.
    Adds columns: ef_id, ef_name, ef_unit, ef_value, ef_source, match_method, status
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    out["Sheet"] = visible_sheet_name
    cols = ["ef_id", "ef_name", "ef_unit", "ef_value", "ef_source", "match_method", "status"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    records: List[Dict[str, Any]] = []
    for _, row in out.iterrows():
        res = mu.map_emission_factor(row, ef_dict)
        records.append(res)
    # Assign back with safe conversions
    def _assign(col_out: str, col_res: str) -> None:
        vals: List[Any] = []
        for r in records:
            try:
                vals.append(r.get(col_res))
            except Exception:
                vals.append(None)
        out[col_out] = pd.Series(vals, dtype="object")
    _assign("ef_id", "EF ID")
    _assign("ef_name", "EF Name")
    _assign("ef_unit", "EF Unit")
    _assign("ef_value", "EF Value")
    _assign("ef_source", "Source")
    _assign("match_method", "Match Method")
    # status may already exist; if not, fill
    vals_status = []
    for r in records:
        vals_status.append(r.get("status"))
    if "status" not in out.columns:
        out["status"] = pd.Series(vals_status, dtype="object")
    else:
        # only fill NAs
        try:
            mask_na = out["status"].isna() | (out["status"].astype(str).str.strip() == "")
            out.loc[mask_na, "status"] = pd.Series(vals_status, dtype="object")
        except Exception:
            pass
    return out


def _compute_emissions_for_fera(df: pd.DataFrame, is_electricity: bool) -> pd.DataFrame:
    """
    Compute 'co2e (t)' for FERA sheets with flexible logic:
     - Prefer activity-based multiplication using:
         Electricity: 'Consumption' or 'activity volume'
         Fuel: 'Fuel consumption' or 'activity volume'
     - If EF is €/based and Spend_Euro exists, compute via €/based helper
     - Klarakarbon special: if 'activity volume' exists, use it (overrides others)
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Detect key columns
    col_spend = _get_ci_col(out, ["Spend_Euro", "Spend Euro", "Spend EUR", "Amount", "Spend"])
    col_act_kbk = _get_ci_col(out, ["activity volume", "activity_volume", "activity amount"])
    col_consumption = _get_ci_col(out, ["Consumption", "consumption", "kwh", "kwh consumed"])
    col_fuel_cons = _get_ci_col(out, ["Fuel consumption", "fuel consumption", "liters", "litres", "volume"])
    col_distance = _get_ci_col(out, ["Distance travelled", "Distance Travelled", "Distance (km)", "Distance", "km travelled", "km", "Total Km"])
    col_ef_unit = _get_ci_col(out, ["ef_unit", "EF Unit"])
    col_ef_val = _get_ci_col(out, ["ef_value", "EF Value"])
    # Allow either legacy 'Sheet_booklets' or the explicit 'Data Source sheet'
    col_src = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])

    # Normalize numeric sources
    series_spend = _to_numeric(out[col_spend]) if col_spend and col_spend in out.columns else pd.Series([None] * len(out))
    series_act_kbk = _to_numeric(out[col_act_kbk]) if col_act_kbk and col_act_kbk in out.columns else pd.Series([None] * len(out))
    series_cons = _to_numeric(out[col_consumption]) if col_consumption and col_consumption in out.columns else pd.Series([None] * len(out))
    series_fuel = _to_numeric(out[col_fuel_cons]) if col_fuel_cons and col_fuel_cons in out.columns else pd.Series([None] * len(out))
    series_dist = _to_numeric(out[col_distance]) if col_distance and col_distance in out.columns else pd.Series([None] * len(out))
    series_ef_val = pd.to_numeric(out[col_ef_val], errors="coerce") if col_ef_val and col_ef_val in out.columns else pd.Series([None] * len(out))
    ef_units = out[col_ef_unit] if col_ef_unit and col_ef_unit in out.columns else pd.Series([None] * len(out))

    co2e_vals: List[Optional[float]] = []
    for i in range(len(out)):
        efv = series_ef_val.iloc[i] if i < len(series_ef_val) else None
        efu = ef_units.iloc[i] if i < len(ef_units) else None
        # Klarakarbon priority on activity volume
        is_kbk = False
        try:
            if col_src and col_src in out.columns:
                is_kbk = str(out.loc[out.index[i], col_src]).strip().lower() == "klarakarbon"
        except Exception:
            is_kbk = False
        # choose activity source
        act_val = None
        if is_kbk and col_act_kbk:
            act_val = series_act_kbk.iloc[i]
        else:
            if is_electricity:
                act_val = series_cons.iloc[i] if col_consumption else None
                if act_val is None and col_act_kbk:
                    act_val = series_act_kbk.iloc[i]
            else:
                # Fuel: prefer Fuel consumption, fallback to Distance travelled, then activity volume
                act_val = series_fuel.iloc[i] if col_fuel_cons else None
                if (act_val is None or (hasattr(pd, "isna") and pd.isna(act_val))) and col_distance:
                    act_val = series_dist.iloc[i]
                if (act_val is None or (hasattr(pd, "isna") and pd.isna(act_val))) and col_act_kbk:
                    act_val = series_act_kbk.iloc[i]
        # compute
        val: Optional[float] = None
        if act_val is not None and pd.notna(act_val):
            val = _compute_activity_based_tco2e(act_val, efv, efu if isinstance(efu, str) else str(efu) if efu is not None else None)
        if val is None and col_spend:
            sp = series_spend.iloc[i]
            if sp is not None and pd.notna(sp):
                val = _compute_euro_based_tco2e(sp, efv, efu if isinstance(efu, str) else str(efu) if efu is not None else None)
        co2e_vals.append(val)
    out["co2e (t)"] = pd.Series(co2e_vals, dtype="float64")
    return out


def _pick_diesel_row_from_ef(ef_df: pd.DataFrame) -> Optional[pd.Series]:
    """Pick the best 'Diesel' EF row from an EF sheet."""
    if ef_df is None or ef_df.empty:
        return None
    name_col = mu._find_first_present_column(ef_df, ["ef_name", "EF Name", "name", "ef_description", "description"])
    if name_col and name_col in ef_df.columns:
        try:
            names = ef_df[name_col].astype(str).str.strip().str.lower()
            # Prefer exact 'diesel'
            exact = ef_df[names == "diesel"]
            if not exact.empty:
                return exact.iloc[0]
            # Then word-boundary 'diesel'
            hits = ef_df[names.str.contains(r"\bdiesel\b", regex=True, na=False)]
            if not hits.empty:
                return hits.iloc[0]
        except Exception:
            pass
    # Fallback: first row
    try:
        return ef_df.iloc[0]
    except Exception:
        return None


def _map_fera_fuel_cts_diesel_distance_or_activity(df: pd.DataFrame, ef_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    For Scope 3 Cat 3 FERA Fuel:
    - Only CTS Denmark / CTS Nordics rows
    - If Distance travelled exists (numeric) -> map Diesel from 'Scope 3 Category 3 FERA Fuel D'
    - Else if Fuel consumption exists (numeric) -> map Diesel from 'Scope 3 Category 3 FERA Fuel'
    Fills ef_id/ef_name/ef_unit/ef_value/ef_source/match_method and keeps existing values if already filled.
    """
    if df is None or df.empty or not ef_dict:
        return df
    out = df.copy()

    # Identify rows
    comp_col = _get_ci_col(out, ["Company", "company", "subsidiary_name", "subsidiary name", "subsidiary"])
    if comp_col is None or comp_col not in out.columns:
        return out
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    dist_col = _get_ci_col(out, ["Distance travelled", "Distance Travelled", "Distance (km)", "Distance", "km travelled", "km", "Total Km"])
    fuel_col = _get_ci_col(out, ["Fuel consumption", "fuel consumption", "liters", "litres", "volume"])

    ef_val_col_out = _get_ci_col(out, ["ef_value", "EF Value"])
    if ef_val_col_out is None:
        ef_val_col_out = "ef_value"
        out[ef_val_col_out] = None

    # Ensure output columns exist
    for c in ["ef_id", "ef_name", "ef_unit", "ef_source", "match_method"]:
        if c not in out.columns:
            out[c] = None

    # Load EF sheets
    key_dist = next((k for k in ef_dict.keys() if str(k).strip().lower() == "scope 3 category 3 fera fuel d"), None)
    key_act = next((k for k in ef_dict.keys() if str(k).strip().lower() == "scope 3 category 3 fera fuel"), None)
    df_dist = ef_dict.get(key_dist) if key_dist else None
    df_act = ef_dict.get(key_act) if key_act else None

    diesel_dist = _pick_diesel_row_from_ef(df_dist) if df_dist is not None else None
    diesel_act = _pick_diesel_row_from_ef(df_act) if df_act is not None else None

    # EF field columns in EF sheets
    def _cols(efdf: pd.DataFrame) -> Dict[str, Optional[str]]:
        return {
            "id": mu._find_first_present_column(efdf, ["ef_id", "EF ID", "EFID", "id"]),
            "name": mu._find_first_present_column(efdf, ["ef_name", "EF Name", "name"]),
            "unit": mu._find_first_present_column(efdf, ["ef_unit", "EF Unit", "unit", "units"]),
            "val": mu._find_first_present_column(efdf, ["ef_value", "EF Value", "value"]),
            "src": mu._find_first_present_column(efdf, ["ef_source", "EF Source", "source", "Reference", "Publication", "Provider"]),
        }

    cols_dist = _cols(df_dist) if df_dist is not None and not df_dist.empty else {}
    cols_act = _cols(df_act) if df_act is not None and not df_act.empty else {}

    def _safe_get(r: pd.Series, col: Optional[str]) -> Any:
        return r[col] if col and col in r else None

    # Numeric series
    dist_num = _to_numeric(out[dist_col]) if dist_col and dist_col in out.columns else pd.Series([None] * len(out))
    fuel_num = _to_numeric(out[fuel_col]) if fuel_col and fuel_col in out.columns else pd.Series([None] * len(out))

    comp_low = out[comp_col].astype(str).str.strip().str.lower()
    is_cts = comp_low.isin({"cts denmark", "cts nordics"})

    # Distance-based mask (has numeric distance)
    has_dist = dist_num.notna() & (dist_num.astype(float) != 0.0)
    # Activity-based mask (has numeric fuel)
    has_fuel = fuel_num.notna() & (fuel_num.astype(float) != 0.0)

    # Optional: only apply to Booklets rows if the data source column exists
    if ds_col and ds_col in out.columns:
        ds_low = out[ds_col].astype(str).str.strip().str.lower()
        is_booklets = ds_low == "booklets"
    else:
        is_booklets = pd.Series([True] * len(out))

    # Prefer distance mapping when both exist; override even if already mapped (existing mapping may be spend-based)
    mask_dist = is_cts & is_booklets & has_dist
    mask_act = is_cts & is_booklets & (~has_dist) & has_fuel

    if diesel_dist is not None and bool(getattr(mask_dist, "any", lambda: False)()):
        out.loc[mask_dist, "ef_id"] = _safe_get(diesel_dist, cols_dist.get("id"))
        out.loc[mask_dist, "ef_name"] = _safe_get(diesel_dist, cols_dist.get("name")) or "Diesel"
        # This is the distance-based sheet; present it as per-km even if the EF sheet unit is inconsistent.
        out.loc[mask_dist, "ef_unit"] = "t CO2e/km"
        out.loc[mask_dist, ef_val_col_out] = _safe_get(diesel_dist, cols_dist.get("val"))
        out.loc[mask_dist, "ef_source"] = _safe_get(diesel_dist, cols_dist.get("src"))
        out.loc[mask_dist, "match_method"] = "FERA Fuel CTS: Diesel distance (Scope 3 Category 3 FERA Fuel D)"

    if diesel_act is not None and bool(getattr(mask_act, "any", lambda: False)()):
        out.loc[mask_act, "ef_id"] = _safe_get(diesel_act, cols_act.get("id"))
        out.loc[mask_act, "ef_name"] = _safe_get(diesel_act, cols_act.get("name")) or "Diesel"
        out.loc[mask_act, "ef_unit"] = _safe_get(diesel_act, cols_act.get("unit"))
        out.loc[mask_act, ef_val_col_out] = _safe_get(diesel_act, cols_act.get("val"))
        out.loc[mask_act, "ef_source"] = _safe_get(diesel_act, cols_act.get("src"))
        out.loc[mask_act, "match_method"] = "FERA Fuel CTS: Diesel activity (Scope 3 Category 3 FERA Fuel)"

    return out


def _fill_missing_co2e_for_fera_fuel_cts(df: pd.DataFrame) -> pd.DataFrame:
    """
    After EF mapping, fill missing co2e(t) for CTS Denmark / CTS Nordics Booklets rows
    using either distance or fuel consumption.
    - Prefer distance if present, else fuel.
    - Only fills where current co2e is empty/zero.
    """
    if df is None or df.empty:
        return df
    out = df.copy()

    comp_col = _get_ci_col(out, ["Company", "company", "subsidiary_name", "subsidiary name", "subsidiary"])
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if comp_col is None or comp_col not in out.columns:
        return out

    # Find a co2e(t) column to write into (prefer exact)
    co2e_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "co2e (t)":
            co2e_col = c
            break
    if co2e_col is None:
        # fallback: first co2e(t)* variant
        for c in list(out.columns):
            low = str(c).strip().lower().replace(" ", "")
            if low == "co2e(t)" or low.startswith("co2e(t).") or low.startswith("co2e(t)"):
                co2e_col = c
                break
    if co2e_col is None:
        co2e_col = "co2e (t)"
        out[co2e_col] = None

    dist_col = _get_ci_col(out, ["Distance travelled", "Distance Travelled", "Distance (km)", "Distance", "km travelled", "km", "Total Km"])
    fuel_col = _get_ci_col(out, ["Fuel consumption", "fuel consumption", "liters", "litres", "volume"])
    ef_unit_col = _get_ci_col(out, ["ef_unit", "EF Unit"])
    ef_val_col = _get_ci_col(out, ["ef_value", "EF Value"])
    if ef_val_col is None or ef_val_col not in out.columns:
        return out

    dist_num = _to_numeric(out[dist_col]) if dist_col and dist_col in out.columns else pd.Series([None] * len(out))
    fuel_num = _to_numeric(out[fuel_col]) if fuel_col and fuel_col in out.columns else pd.Series([None] * len(out))
    ef_vals = pd.to_numeric(out[ef_val_col], errors="coerce")
    ef_units = out[ef_unit_col] if ef_unit_col and ef_unit_col in out.columns else pd.Series([None] * len(out))

    comp_low = out[comp_col].astype(str).str.strip().str.lower()
    is_cts = comp_low.isin({"cts denmark", "cts nordics"})
    if ds_col and ds_col in out.columns:
        is_booklets = out[ds_col].astype(str).str.strip().str.lower() == "booklets"
    else:
        is_booklets = pd.Series([True] * len(out))

    has_dist = dist_num.notna() & (dist_num.astype(float) != 0.0)
    has_fuel = fuel_num.notna() & (fuel_num.astype(float) != 0.0)

    # Convert fuel activity to litres if unit is m3
    fuel_unit_col = _get_ci_col(out, ["Unit of fuel Consumption", "Unit of fuel consumption", "fuel unit", "Fuel unit"])
    if fuel_unit_col and fuel_unit_col in out.columns:
        u = out[fuel_unit_col].astype(str).str.strip().str.lower()
        is_m3 = u.isin({"m3", "m^3", "cubic meter", "cubic metre", "cbm"})
        fuel_num = fuel_num.copy()
        fuel_num[is_m3] = fuel_num[is_m3] * 1000.0

    # Recompute for CTS Booklets rows that have distance or fuel and EF present (override old/wrong values)
    mask_base = is_cts & is_booklets & ef_vals.notna() & (has_dist | has_fuel)

    new_vals: list[Optional[float]] = []
    for i in range(len(out)):
        if not bool(mask_base.iloc[i]):
            new_vals.append(None)
            continue
        efv = float(ef_vals.iloc[i])
        efu = ef_units.iloc[i]
        efu_s = str(efu) if efu is not None else ""
        act = None
        if bool(has_dist.iloc[i]):
            act = dist_num.iloc[i]
        elif bool(has_fuel.iloc[i]):
            act = fuel_num.iloc[i]
        if act is None or (hasattr(pd, "isna") and pd.isna(act)):
            new_vals.append(None)
            continue
        val = _compute_activity_based_tco2e(float(act), efv, efu_s)
        new_vals.append(val)

    new_ser = pd.Series(new_vals, index=out.index, dtype="float64")
    fill_mask = mask_base & new_ser.notna()
    if bool(getattr(fill_mask, "any", lambda: False)()):
        out.loc[fill_mask, co2e_col] = new_ser.loc[fill_mask].values
    return out


def _drop_booklets_rows_missing_vehicle_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    For Booklets rows, drop records where Vehicle Type is missing/empty.
    Applies only to Fuel sheet context.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    col_src = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    vt_col = _get_ci_col(out, ["Vehicle Type", "Vehicle type", "vehicle type"])
    if col_src is None or vt_col is None:
        return out
    try:
        src = out[col_src].astype(str).str.strip().str.lower()
        vt_raw = out[vt_col]
        # Normalize to string and check common NA tokens
        vt_str = vt_raw.astype(str).str.strip()
        vt_low = vt_str.str.lower()
        tokens_na = {"", "na", "n/a", "none", "nan", "-", "--", "null"}
        is_na_val = vt_low.isin(tokens_na)
        # Also treat real NaN as NA
        is_nan = vt_raw.isna() if hasattr(vt_raw, "isna") else False
        drop_mask = (src == "booklets") & (is_na_val | is_nan)
        if bool(getattr(drop_mask, "any", lambda: False)()):
            out = out.loc[~drop_mask].copy()
    except Exception:
        return out
    return out


def _standardize_company_for_klarakarbon(df: pd.DataFrame) -> pd.DataFrame:
    """
    For rows marked as Klarakarbon in 'Data Source sheet' (or Sheet_booklets),
    set Company from source_company using the provided mapping rules.
    Applies to both Fuel and Electricity consolidated frames.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    col_src = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if col_src is None or col_src not in out.columns:
        return out
    # Find a source_company-like column (prefer cleaned variants)
    prefer_cols = [
        "source_company_clean",
        "source company clean",
        "source_company_cleaned",
        "source_company",
        "source company",
    ]
    sc_col = _get_ci_col(out, prefer_cols)
    # Fallback: any column that contains 'source_company' token (very relaxed)
    if sc_col is None:
        for c in list(out.columns):
            low = str(c).strip().lower().replace(" ", "_")
            if "source_company" in low:
                sc_col = c
                break
    if sc_col is None or sc_col not in out.columns:
        return out
    # Ensure Company column exists
    if "Company" not in out.columns:
        out["Company"] = pd.Series([None] * len(out), dtype="object")
    # Build mask for Klarakarbon rows
    try:
        src = out[col_src].astype(str).str.strip().str.lower()
        mask_kbk = src == "klarakarbon"
    except Exception:
        return out
    # Mapping rules (case-insensitive)
    def _canon(name: Optional[str]) -> Optional[str]:
        if name is None:
            return None
        raw = str(name).strip()
        low = raw.lower()
        # Treat common 'none' strings as empty
        if low in {"none", "nan", ""}:
            return None
        # Startswith/contains matching to be robust
        if low.startswith("gapit nordics"):
            return "Gapit"
        if low.startswith("gt nordics"):
            return "GT Nordics"
        if "nordicepod" in low:
            return "NordicEPOD"
        if low.startswith("nep switchboards"):
            return "NEP Switchboards"
        # Default: return trimmed as-is
        return raw
    try:
        src_vals = out[sc_col].map(lambda v: None if (v is None or str(v).strip() == "") else str(v).strip())
        mapped = src_vals.map(_canon).astype("object")
        # Assign for all Klarakarbon rows when a mapped company is available
        assign_mask = mask_kbk & mapped.notna()
        if bool(getattr(assign_mask, "any", lambda: False)()):
            out.loc[assign_mask, "Company"] = mapped.loc[assign_mask]
        # Finally, clean literal "None" strings in Company
        try:
            comp = out["Company"].astype(str)
            mask_none_literal = comp.str.strip().str.lower().isin({"none", "nan"})
            if bool(getattr(mask_none_literal, "any", lambda: False)()):
                out.loc[mask_none_literal, "Company"] = None
        except Exception:
            pass
    except Exception:
        # Best-effort; keep as-is on error
        return out
    return out


def _drop_old_co2e_and_add_new(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all 'co2e (t)' columns (including duplicates like 'co2e (t).1')
    and create a fresh empty column named 'co2e (tonnes)'.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    try:
        # Find columns to drop: exact lower == 'co2e (t)' or suffix variants 'co2e (t).N'
        to_drop: list[str] = []
        for c in list(out.columns):
            s = str(c).strip()
            low = s.lower()
            # normalize multiple spaces
            low = re.sub(r"\s+", " ", low)
            if (low == "co2e (t)") or low.startswith("co2e (t).") or re.match(r"^co2e\s*\(t\)", low) is not None:
                to_drop.append(c)
        if to_drop:
            out = out.drop(columns=to_drop, errors="ignore")
    except Exception:
        pass
    # Add fresh column
    try:
        out["co2e (tonnes)"] = pd.Series([None] * len(out), dtype="object")
    except Exception:
        out["co2e (tonnes)"] = None
    return out


def _fill_company_from_source_when_empty(df: pd.DataFrame) -> pd.DataFrame:
    """
    If Company is empty/None/'None', fill from source_company* using mapping, regardless of data source.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    sc_col = _get_ci_col(out, [
        "source_company_clean",
        "source company clean",
        "source_company_cleaned",
        "source_company",
        "source company",
    ])
    # Relaxed fallback search
    if sc_col is None:
        for c in list(out.columns):
            low = str(c).strip().lower().replace(" ", "_")
            if "source_company" in low:
                sc_col = c
                break
    if sc_col is None or sc_col not in out.columns:
        return out
    if "Company" not in out.columns:
        out["Company"] = pd.Series([None] * len(out), dtype="object")
    # Build mask for empty Company
    try:
        comp = out["Company"].astype(str)
    except Exception:
        comp = out["Company"]
    is_empty = comp.isna() if hasattr(comp, "isna") else pd.Series([False] * len(out))
    try:
        mask_none_literal = comp.astype(str).str.strip().str.lower().isin({"none", ""})
    except Exception:
        mask_none_literal = pd.Series([False] * len(out))
    need_fill = (is_empty) | (mask_none_literal)
    # Map values
    def _canon(name: Optional[str]) -> Optional[str]:
        if name is None:
            return None
        raw = str(name).strip()
        low = raw.lower()
        if low in {"none", "nan", ""}:
            return None
        if low.startswith("gapit nordics"):
            return "Gapit"
        if low.startswith("gt nordics"):
            return "GT Nordics"
        if "nordicepod" in low:
            return "NordicEPOD"
        if low.startswith("nep switchboards"):
            return "NEP Switchboards"
        return raw
    try:
        src_vals = out[sc_col].map(lambda v: None if (v is None or str(v).strip() == "") else str(v).strip())
        mapped = src_vals.map(_canon).astype("object")
        assign_mask = need_fill & mapped.notna()
        if bool(getattr(assign_mask, "any", lambda: False)()):
            out.loc[assign_mask, "Company"] = mapped.loc[assign_mask]
        # Clean remaining literal 'None'/'nan'
        try:
            comp2 = out["Company"].astype(str)
            mask_clean = comp2.str.strip().str.lower().isin({"none", "nan"})
            if bool(getattr(mask_clean, "any", lambda: False)()):
                out.loc[mask_clean, "Company"] = None
        except Exception:
            pass
    except Exception:
        return out
    return out


def _drop_mapping_related_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapping'e ait kolonları (ef_*, mapped_*, mapping_status, match_method, ef_source vb.)
    ve tüm 'co2e (t)' varyantlarını kaldırır. Yalnız hazırlık için kullanılır.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    to_drop: list[str] = []
    for c in list(out.columns):
        s = str(c).strip()
        low = s.lower()
        norm = re.sub(r"[^a-z0-9]", "", low)
        # Remove all ef_* and mapping meta columns
        if norm.startswith("efid") or norm.startswith("efname") or norm.startswith("efunit") or norm.startswith("efvalue") or norm.startswith("efsource"):
            to_drop.append(c)
            continue
        if norm.startswith("mappingstatus") or norm.startswith("mappedby") or norm.startswith("mappeddate") or norm.startswith("matchmethod"):
            to_drop.append(c)
            continue
        # Remove any 'co2e (t)' variants
        norm_co2e = re.sub(r"\s+", " ", low)
        if (norm_co2e == "co2e (t)") or norm_co2e.startswith("co2e (t).") or re.match(r"^co2e\s*\(t\)", norm_co2e) is not None:
            to_drop.append(c)
            continue
        # Also drop columns normalized to 'co2et' (to catch Excel duplicate headers)
        if norm.startswith("co2et"):
            to_drop.append(c)
            continue
    if to_drop:
        try:
            out = out.drop(columns=to_drop, errors="ignore")
        except Exception:
            pass
    return out


def _copy_company_from_source_for_empty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Company sütununda None/boş/'None' olan hücreleri, doğrudan source_company benzeri
    kolondan (hiçbir yeniden adlandırma/haritalama YAPMADAN) kopyalar.
    Mevcut Company değeri dolu ise dokunmaz.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Company sütunu yoksa oluştur
    if "Company" not in out.columns:
        out["Company"] = pd.Series([None] * len(out), dtype="object")
    # source_company aday kolonlarını bul
    sc_col = _get_ci_col(out, [
        "source_company_clean",
        "source company clean",
        "source_company_cleaned",
        "source_company",
        "source company",
    ])
    if sc_col is None:
        # gevşek arama
        for c in list(out.columns):
            low = str(c).strip().lower().replace(" ", "_")
            if "source_company" in low:
                sc_col = c
                break
    if sc_col is None or sc_col not in out.columns:
        return out
    # Company boş olanları bul (NaN, '', 'None', 'nan')
    comp_str = out["Company"].astype(str)
    empty_mask = comp_str.str.strip().str.lower().isin({"", "none", "nan"})
    # Kaynaktan direkt kopyala (trimle)
    src_vals = out[sc_col].map(lambda v: None if (v is None or str(v).strip() == "") else str(v).strip())
    assign_mask = empty_mask & src_vals.notna()
    if bool(getattr(assign_mask, "any", lambda: False)()):
        out.loc[assign_mask, "Company"] = src_vals.loc[assign_mask].astype("object")
    # Son temizlik: literal 'None'/'nan' metinlerini kaldır
    try:
        comp2 = out["Company"].astype(str)
        mask_clean = comp2.str.strip().str.lower().isin({"none", "nan"})
        if bool(getattr(mask_clean, "any", lambda: False)()):
            out.loc[mask_clean, "Company"] = None
    except Exception:
        pass
    return out


def _clear_company_none_to_empty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Company sütununda 'None'/'none' gibi stringleri ve 'nan' stringlerini
    boş stringe çevirir. Gerçek NaN'lere dokunmaz (boş kalır).
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "Company" not in out.columns:
        return out
    try:
        comp = out["Company"].astype(str)
        mask = comp.str.strip().str.lower().isin({"none", "nan"})
        if bool(getattr(mask, "any", lambda: False)()):
            out.loc[mask, "Company"] = ""
    except Exception:
        pass
    return out


def _strip_mapping_artifacts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapping'e ait yardımcı kolonları kaldırır: ef_* alanları, mapping/mapped alanları,
    match_method, status vb. (GHGP Category, Scope vb. korunur).
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    to_drop: list[str] = []
    patterns = [
        r"^ef[\s_]*id(\.\d+)?$",
        r"^ef[\s_]*name(\.\d+)?$",
        r"^ef[\s_]*unit(\.\d+)?$",
        r"^ef[\s_]*value(\.\d+)?$",
        r"^ef[\s_]*source(\.\d+)?$",
        r"^match[\s_]*method(\.\d+)?$",
        r"^mapping[\s_]*status(\.\d+)?$",
        r"^mapped[\s_]*by(\.\d+)?$",
        r"^mapped[\s_]*date(\.\d+)?$",
        r"^status(\.\d+)?$",
        r"^emissions?_?t?co2e(\.\d+)?$",
        r"^co2e\s*\(kg\)(\.\d+)?$",
        r"^co2e(\.\d+)?$",  # bazı kaynaklarda sadece 'co2e' olabilir
    ]
    try:
        import re as _re
        for c in list(out.columns):
            low = str(c).strip().lower()
            low = _re.sub(r"\s+", " ", low)
            for pat in patterns:
                if _re.match(pat, low):
                    to_drop.append(c)
                    break
        if to_drop:
            out = out.drop(columns=to_drop, errors="ignore")
    except Exception:
        pass
    return out


def _override_source_file_from_source_company_where_sheet1(df: pd.DataFrame) -> pd.DataFrame:
    """
    'Sheet' == 'Sheet1' satırlarında 'Source_File' kolonunu source_company benzeri
    kolondan kopyalar ve '.xlsx' uzantısını ekler.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    sheet_col = _get_ci_col(out, ["Sheet", "sheet"])
    if sheet_col is None or sheet_col not in out.columns:
        return out
    sc_col = _get_ci_col(out, [
        "source_company_clean",
        "source company clean",
        "source_company_cleaned",
        "source_company",
        "source company",
    ])
    if sc_col is None:
        for c in list(out.columns):
            low = str(c).strip().lower().replace(" ", "_")
            if "source_company" in low:
                sc_col = c
                break
    if sc_col is None or sc_col not in out.columns:
        return out
    if "Source_File" not in out.columns:
        out["Source_File"] = pd.Series([None] * len(out), dtype="object")
    try:
        mask = out[sheet_col].astype(str).str.strip().str.lower() == "sheet1"
        src_vals = out[sc_col].map(lambda v: "" if (v is None or str(v).strip() == "") else str(v).strip())
        def _ensure_xlsx(name: str) -> str:
            low = name.lower()
            return name if not name or low.endswith(".xlsx") else (name + ".xlsx")
        out.loc[mask, "Source_File"] = src_vals.loc[mask].map(_ensure_xlsx).astype("object")
    except Exception:
        return out
    return out


def _remap_source_file_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Source_File içinde aşağıdaki birebir değerleri düzeltir:
      - 'gapit nordics.xlsx'      -> 'Gapit.xlsx'
      - 'gt nordics.xlsx'         -> 'GT Nordics.xlsx'
      - 'nordicepod.xlsx'         -> 'NordicEPOD.xlsx'
      - 'nep switchboards.xlsx'   -> 'NEP Switchboards.xlsx'
    Diğer değerleri olduğu gibi bırakır.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "Source_File" not in out.columns:
        return out
    try:
        src = out["Source_File"].astype(str).str.strip()
        low = src.str.lower()
        mapping = {
            "gapit nordics.xlsx": "Gapit.xlsx",
            "gt nordics.xlsx": "GT Nordics.xlsx",
            "nordicepod.xlsx": "NordicEPOD.xlsx",
            "nep switchboards.xlsx": "NEP Switchboards.xlsx",
        }
        for k, v in mapping.items():
            mask = low == k
            if bool(getattr(mask, "any", lambda: False)()):
                out.loc[mask, "Source_File"] = v
    except Exception:
        return out
    return out


def _fill_country_from_source_file(df: pd.DataFrame) -> pd.DataFrame:
    """
    Country sütunu boş olan satırlarda, Source_File değerine göre ülkeyi doldurur.
    Şu anda istenen kurallar:
      - Gapit.xlsx            -> Norway
      - GT Nordics.xlsx       -> Norway
      - NordicEPOD.xlsx       -> Norway
      - NEP Switchboards.xlsx -> Norway
    Diğer değerlerde doldurma yapmaz. Country doluysa dokunmaz.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Country sütununu bul (case-insensitive; gerekirse 'country' içeren ilk sütun)
    country_col = _get_ci_col(out, ["Country", "country"])
    if country_col is None:
        for c in list(out.columns):
            if "country" in str(c).strip().lower():
                country_col = c
                break
    if country_col is None:
        # Kolon yoksa oluştur
        country_col = "Country"
        out[country_col] = pd.Series([None] * len(out), dtype="object")
    if "Source_File" not in out.columns:
        return out
    try:
        # Boş country (NaN veya boş string) maskesi
        cser = out[country_col]
        try:
            is_na = cser.isna()
        except Exception:
            is_na = pd.Series([False] * len(out))
        is_empty = cser.astype(str).str.strip().isin({"", "none", "nan"})
        need = is_na | is_empty
        if not bool(getattr(need, "any", lambda: False)()):
            return out
        src = out["Source_File"].astype(str).str.strip()
        src_low = src.str.lower()
        mapping = {
            "gapit.xlsx": "Norway",
            "gt nordics.xlsx": "Norway",
            "nordicepod.xlsx": "Norway",
            "nep switchboards.xlsx": "Norway",
        }
        # Eşleşenleri bul ve ata (sadece need maskesi içinde)
        for key_low, country_val in mapping.items():
            m = src_low == key_low
            assign_mask = need & m
            if bool(getattr(assign_mask, "any", lambda: False)()):
                out.loc[assign_mask, country_col] = country_val
    except Exception:
        return out
    return out


def _copy_release_date_to_reporting_period_for_klarakarbon(df: pd.DataFrame) -> pd.DataFrame:
    """
    'Data Source sheet' == 'Klarakarbon' olan satırlarda
    'release date' sütunundaki tarihi 'Reporting period (month, year)' sütununa kopyalar.
    Tarih parse sırası: DD.MM.YYYY -> genel parse.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Data Source sütunu
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    sheet_col = _get_ci_col(out, ["Sheet", "sheet"])
    # Release date sütunu
    rel_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "release date":
            rel_col = c
            break
    if rel_col is None:
        return out
    # Hedef sütun
    target_col = _get_ci_col(out, ["Reporting period (month, year)", "reporting period (month, year)"])
    if target_col is None:
        target_col = "Reporting period (month, year)"
        out[target_col] = pd.Series([None] * len(out), dtype="object")
    try:
        mask_kbk = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
        # Stringe çevir, non‑breaking space ve baştaki/sondaki boşlukları temizle
        raw = (
            out.loc[mask_kbk, rel_col]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
        # parse DD.MM.YYYY öncelikli; olmuyorsa genel parse
        dt1 = pd.to_datetime(raw, format="%d.%m.%Y", errors="coerce")
        dtg = pd.to_datetime(raw, errors="coerce")
        dt = dt1.combine_first(dtg)
        # Hedef kolonu object yap
        try:
            out[target_col] = out[target_col].astype("object")
        except Exception:
            pass
        # Sadece YYYY-MM-DD string yaz (saat olmadan)
        try:
            as_str = dt.dt.strftime("%Y-%m-%d")
        except Exception:
            # dt serisi değilse generik parse edilmiş olabilir
            as_str = pd.to_datetime(dt, errors="coerce").dt.strftime("%Y-%m-%d")
        out.loc[mask_kbk, target_col] = pd.Series(as_str, index=out.index[mask_kbk]).astype("object")
    except Exception:
        return out
    return out


def _apply_klarakarbon_manual_ef_map_fuel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yalnızca Klarakarbon satırları için, 'emission factor name' değerine göre
    manuel EF eşlemesini yeni sütunlarda gösterir:
      - kbk_map_ef_id
      - kbk_map_ef_category
      - kbk_map_ef_value
      - kbk_map_ef_unit
      - kbk_map_note
    Orijinal ef_* kolonlarına dokunmaz.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if ds_col is None or ds_col not in out.columns:
        return out
    # EF name benzeri kolon
    name_col = _get_ci_col(out, [
        "emission factor name",
        "Emission factor name",
        "ef_name",
        "EF Name",
        "emission factor",
        "name",
    ])
    if name_col is None or name_col not in out.columns:
        return out
    # Hedef sütunları oluştur
    for c in ["kbk_map_ef_id", "kbk_map_ef_category", "kbk_map_ef_value", "kbk_map_ef_unit", "kbk_map_note"]:
        if c not in out.columns:
            out[c] = None
    # Eşleşme sözlüğü (case-insensitive exact)
    mapping = {
        "diesel - 100% mineral diesel": ("303F004", "Diesel", 0.00061101, "t CO2e/L", "Direct match"),
        "gas/diesel oil": ("303F004", "Diesel", 0.00061101, "t CO2e/L", "Same category – Diesel"),
        "gasoline": ("303F006", "Petrol", 0.00058094, "t CO2e/L", "Gasoline equals Petrol"),
        "average passenger car average distance": ("303F007", "Internal combustion vehicle", 0.00058094, "t CO2e/L", "Best available match"),
        "gasoline - 5% bioethanol blend": ("303F002", "Bioethanol", 0.00051906, "t CO2e/L", "Closest blend → Bioethanol"),
        "air transport services": (None, None, None, None, "Table does not include flight EF"),
        "other petroleum gas": ("303F005", "LPG", 0.00018551, "t CO2e/L", "Gas → closest to LPG"),
        "natural gas": ("303F003", "CNG", 0.00009289, "t CO2e/L", "Natural gas → CNG"),
        "propane": ("303F005", "LPG", 0.00018551, "t CO2e/L", "Propane equals LPG"),
        "non-residential maintenance and repair": (None, None, None, None, "Not a fuel; no match"),
    }
    try:
        mask_kbk = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
        names = out.loc[mask_kbk, name_col].astype(str).str.strip().str.lower()
        # Atama
        ids = []
        cats = []
        vals = []
        units = []
        notes = []
        for v in names.tolist():
            rec = mapping.get(v)
            if rec is None:
                ids.append(None); cats.append(None); vals.append(None); units.append(None); notes.append("No match")
            else:
                ids.append(rec[0]); cats.append(rec[1]); vals.append(rec[2]); units.append(rec[3]); notes.append(rec[4])
        out.loc[mask_kbk, "kbk_map_ef_id"] = pd.Series(ids, index=out.index[mask_kbk]).astype("object")
        out.loc[mask_kbk, "kbk_map_ef_category"] = pd.Series(cats, index=out.index[mask_kbk]).astype("object")
        out.loc[mask_kbk, "kbk_map_ef_value"] = pd.Series(vals, index=out.index[mask_kbk])
        out.loc[mask_kbk, "kbk_map_ef_unit"] = pd.Series(units, index=out.index[mask_kbk]).astype("object")
        out.loc[mask_kbk, "kbk_map_note"] = pd.Series(notes, index=out.index[mask_kbk]).astype("object")
    except Exception:
        return out
    return out


def _compute_kbk_co2e_from_activity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yalnızca Klarakarbon satırları için:
      co2e (t) = activity volume * kbk_map_ef_value
    Her iki değer de sayıya çevrilir; sayı değilse atlanır.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if ds_col is None or ds_col not in out.columns:
        return out
    ef_val_col = _get_ci_col(out, ["kbk_map_ef_value"])
    act_col = _get_ci_col(out, ["activity volume", "activity_volume", "activity amount"])
    if ef_val_col is None or act_col is None:
        return out
    # Target co2e column: write into the first existing co2e-family column (left-most).
    def _is_co2e_family(name: str) -> bool:
        import re as _re
        s = _re.sub(r"[^a-z0-9]", "", str(name).lower())
        return s.startswith("co2et")
    co2e_candidates = [c for c in list(out.columns) if _is_co2e_family(c)]
    if co2e_candidates:
        co2e_col = co2e_candidates[0]
    else:
        co2e_col = "co2e (t)"
        out[co2e_col] = None
    try:
        mask = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
        # numerik çeviri (virgül/nbs boşluk temizliği)
        def _to_num(s: pd.Series) -> pd.Series:
            s2 = s.astype(str).str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
            return pd.to_numeric(s2, errors="coerce")
        ef_vals = _to_num(out.loc[mask, ef_val_col])
        acts = _to_num(out.loc[mask, act_col])
        prod = ef_vals * acts
        out.loc[mask, co2e_col] = prod.astype("float64")
    except Exception:
        return out
    return out


def _overwrite_primary_co2e_with_last_duplicate_for_kbk(df: pd.DataFrame) -> pd.DataFrame:
    """
    If there are multiple columns named exactly 'co2e (t)', overwrite the first
    one with the last one's values ONLY for Klarakarbon rows, then drop the
    duplicate columns keeping the first.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Find duplicates of exact label 'co2e (t)' using get_loc (can return slice)
    try:
        loc = out.columns.get_loc("co2e (t)")
    except KeyError:
        return out
    # If single int → nothing to merge
    if isinstance(loc, int):
        return out
    # It's a slice -> at least 2 duplicates
    try:
        start = loc.start or 0
        stop = loc.stop or start + 1
        if stop - start < 2:
            return out
        left_idx = start
        right_idx = stop - 1
        # Klarakarbon mask
        ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
        if ds_col is None or ds_col not in out.columns:
            return out
        mask_kbk = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
        # Overwrite first duplicate with last duplicate values only for Klarakarbon rows
        out.iloc[mask_kbk.values, left_idx] = out.iloc[mask_kbk.values, right_idx].values
        # Drop duplicate columns keeping the first occurrence
        out = out.loc[:, ~out.columns.duplicated(keep="first")]
    except Exception:
        return out
    return out

def _compute_kbk_co2e_electricity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Electricity sheet'inde yalnızca Klarakarbon satırları için:
      co2e (t) = activity volume * ef_value
    'co2e (t)' kolonu yoksa oluşturulur.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if ds_col is None or ds_col not in out.columns:
        return out
    ef_val_col = _get_ci_col(out, ["ef_value", "EF Value", "value"])
    act_col = _get_ci_col(out, ["activity volume", "activity_volume", "activity amount"])
    if ef_val_col is None or act_col is None:
        return out
    # co2e (t) hedefi: mevcut tam isimli sütunu tercih et; yoksa oluştur
    co2e_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "co2e (t)":
            co2e_col = c
            break
    if co2e_col is None:
        co2e_col = "co2e (t)"
        out[co2e_col] = None
    try:
        mask = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
        def _to_num(s: pd.Series) -> pd.Series:
            s2 = s.astype(str).str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
            return pd.to_numeric(s2, errors="coerce")
        ef_vals = _to_num(out.loc[mask, ef_val_col])
        acts = _to_num(out.loc[mask, act_col])
        prod = ef_vals * acts
        out.loc[mask, co2e_col] = prod.astype("float64")
    except Exception:
        return out
    return out


def _compute_booklets_co2e_electricity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Electricity sheet'inde yalnızca Booklets satırları için:
      co2e (t) = (Electricity Consumption * ef_value) + (Heating Consumption * ef_value)
    Hangi tüketim varsa onu kullanır; ikisi de varsa toplamını yazar.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if ds_col is None or ds_col not in out.columns:
        return out
    ef_val_col = _get_ci_col(out, ["ef_value", "EF Value", "value"])
    elc_col = _get_ci_col(out, ["Electricity Consumption", "electricity consumption"])
    heat_col = _get_ci_col(out, ["Heating Consumption", "heating consumption"])
    if ef_val_col is None or (elc_col is None and heat_col is None):
        return out
    # co2e (t) hedefi: mevcut tam isimli sütunu tercih et; yoksa oluştur
    co2e_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "co2e (t)":
            co2e_col = c
            break
    if co2e_col is None:
        co2e_col = "co2e (t)"
        out[co2e_col] = None
    try:
        mask = out[ds_col].astype(str).str.strip().str.lower() == "booklets"
        # numerik çeviriciler
        def _to_num(s: pd.Series) -> pd.Series:
            s2 = s.astype(str).str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
            return pd.to_numeric(s2, errors="coerce")
        ef_vals = _to_num(out.loc[mask, ef_val_col])
        elc_vals = _to_num(out.loc[mask, elc_col]) if elc_col else 0.0
        heat_vals = _to_num(out.loc[mask, heat_col]) if heat_col else 0.0
        # NaN'ları 0 kabul ederek toplam
        import numpy as _np
        if isinstance(elc_vals, (int, float)):
            elc_vals = _np.zeros(sum(mask))
        if isinstance(heat_vals, (int, float)):
            heat_vals = _np.zeros(sum(mask))
        prod = ef_vals.fillna(0.0) * (elc_vals.fillna(0.0) + heat_vals.fillna(0.0))
        out.loc[mask, co2e_col] = prod.astype("float64")
    except Exception:
        return out
    return out


def _compute_consumption_co2e_electricity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Electricity sheet'inde 'Consumption' kolonu bulunan satırlar için:
      co2e (t) = Consumption * ef_value
    Sadece 'Consumption' değeri mevcut olan satırlara uygular; diğerlerini değiştirmez.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    cons_col = _get_ci_col(out, ["Consumption", "consumption"])
    ef_val_col = _get_ci_col(out, ["ef_value", "EF Value", "value"])
    if cons_col is None or ef_val_col is None:
        return out
    # co2e (t) hedefi: mevcut tam isimli sütunu tercih et; yoksa oluştur
    co2e_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "co2e (t)":
            co2e_col = c
            break
    if co2e_col is None:
        co2e_col = "co2e (t)"
        out[co2e_col] = None
    try:
        # Only where Consumption present (non-empty numeric)
        def _to_num(s: pd.Series) -> pd.Series:
            s2 = s.astype(str).str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
            return pd.to_numeric(s2, errors="coerce")
        cons_vals = _to_num(out[cons_col])
        ef_vals = _to_num(out[ef_val_col])
        mask = cons_vals.notna() & ef_vals.notna()
        prod = cons_vals[mask] * ef_vals[mask]
        out.loc[mask, co2e_col] = prod.astype("float64")
    except Exception:
        return out
    return out


def _overlay_kbk_co2e_from_secondary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Scope 3 Cat 3 FERA Fuel içinde (genel amaçlı da çalışır):
    Aynı isimli birden fazla 'co2e (t)' kolonu varsa, yalnızca Klarakarbon satırlarında
    en sağdaki 'co2e (t)' değerlerini en soldaki 'co2e (t)' kolonu üzerine yazar.
    Diğer satırlara dokunmaz. İkincil kolonu silmez.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # 'co2e (t)' sütunlarını sırayla topla (boşlukları yok sayarak)
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    co2e_cols: list[str] = []
    for c in list(out.columns):
        low_compact = _norm_name(c)
        if low_compact == "co2e(t)" or low_compact.startswith("co2e(t)."):
            co2e_cols.append(c)
    if len(co2e_cols) < 2:
        return out  # overlay yapacak ikincil bir sütun yok
    base_col = co2e_cols[0]           # en soldaki
    secondary_col = co2e_cols[-1]     # en sağdaki (ör. CH gibi)
    # Klarakarbon maskesi
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    sheet_col = _get_ci_col(out, ["Sheet", "sheet"])
    if ds_col is None or ds_col not in out.columns:
        return out
    try:
        mask_ds = (out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon") if (ds_col and ds_col in out.columns) else False
        mask_sheet = (out[sheet_col].astype(str).str.strip().str.lower() == "klarakarbon") if (sheet_col and sheet_col in out.columns) else False
        # Klarakarbon maskesi: Data Source veya Sheet alanından herhangi biri
        mask_kbk = mask_ds | mask_sheet
        # sadece Klarakarbon satırları için ikincil değeri temel kolona kopyala (boş olmayanları)
        sec_vals = out.loc[mask_kbk, secondary_col]
        out.loc[mask_kbk, base_col] = sec_vals.values
    except Exception:
        return out
    return out


def _mirror_booklets_co2e_to_secondary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Booklets satırları için ana 'co2e (t)' kolonundaki değeri,
    ikinci 'co2e (t)' kolonuna kopyalar. İkinci kolon yoksa oluşturur
    (ör. 'co2e (t).1').
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if ds_col is None or ds_col not in out.columns:
        return out
    # co2e (t) kolonlarını bul
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    co2e_cols: list[str] = []
    for c in list(out.columns):
        low_compact = _norm_name(c)
        if low_compact == "co2e(t)" or low_compact.startswith("co2e(t)."):
            co2e_cols.append(c)
    if not co2e_cols:
        # hiç yoksa ana kolonu oluştur
        base_col = "co2e (t)"
        out[base_col] = None
        co2e_cols = [base_col]
    base_col = co2e_cols[0]
    if len(co2e_cols) >= 2:
        secondary_col = co2e_cols[-1]
    else:
        # ikinci kolonu oluştur
        secondary_col = "co2e (t).1"
        out[secondary_col] = None
    try:
        mask_book = out[ds_col].astype(str).str.strip().str.lower() == "booklets"
        out.loc[mask_book, secondary_col] = out.loc[mask_book, base_col].values
    except Exception:
        return out
    return out


def _copy_main_co2e_to_secondary_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ana 'co2e (t)' değerlerini tüm satırlar için ikinci 'co2e (t).1' sütununa kopyalar.
    İkinci sütun yoksa oluşturur. İsimleri aynı olsa dahi pozisyona göre çalışır.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    idxs: list[int] = []
    for j, c in enumerate(list(out.columns)):
        low_compact = _norm_name(c)
        if low_compact == "co2e(t)" or low_compact.startswith("co2e(t)."):
            idxs.append(j)
    # Ana sütun
    if not idxs:
        base_col = "co2e (t)"
        out[base_col] = None
        base_idx = list(out.columns).index(base_col)
    else:
        base_idx = idxs[0]
    # İkincil sütun adı
    secondary_col = None
    for c in list(out.columns):
        if _norm_name(c).startswith("co2e(t)."):
            secondary_col = c
            break
    if secondary_col is None:
        secondary_col = "co2e (t).1"
        out[secondary_col] = None
    # Kopyala (tüm satırlar)
    try:
        out.loc[:, secondary_col] = out.iloc[:, base_idx].values
    except Exception:
        return out
    return out


def _rename_duplicate_co2e_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    'co2e (t)' ailesindeki sütun adlarını benzersiz yapar.
    Kural: İlk (tercihen tam adı 'co2e (t)' olan) sütun aynı kalır,
    sonraki kopyalar sırasıyla 'co2e (t) 2', 'co2e (t) 3', ... olur.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    co2e_cols: list[str] = []
    for c in list(out.columns):
        low_compact = _norm_name(c)
        if low_compact == "co2e(t)" or low_compact.startswith("co2e(t)."):
            co2e_cols.append(c)
    if len(co2e_cols) <= 1:
        return out
    # Base column: prefer exact 'co2e (t)' if present
    base_col = None
    for c in co2e_cols:
        if str(c).strip().lower() == "co2e (t)":
            base_col = c
            break
    if base_col is None:
        base_col = co2e_cols[0]
    # Build rename mapping for the rest (start from 2)
    rename_map = {}
    next_idx = 2
    for c in co2e_cols:
        if c == base_col:
            continue
        # avoid collision with existing name
        new_name = f"co2e (t) {next_idx}"
        while new_name in out.columns or new_name == base_col:
            next_idx += 1
            new_name = f"co2e (t) {next_idx}"
        rename_map[c] = new_name
        next_idx += 1
    try:
        out = out.rename(columns=rename_map)
    except Exception:
        pass
    return out


def _force_ghgp_category(df: pd.DataFrame, value: str) -> pd.DataFrame:
    """
    Verilen DataFrame'de 'GHGP Category' sütununu sabit bir değere set eder.
    Sütun yoksa oluşturur.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    try:
        out["GHGP Category"] = value
    except Exception:
        out["GHGP Category"] = pd.Series([value] * len(out), dtype="object")
    return out


def _copy_booklets_co2e1_to_co2e2(df: pd.DataFrame) -> pd.DataFrame:
    """
    FERA Fuel çıktısında (genel amaçlı da çalışır):
    'Data Source sheet' = 'Booklets' olan satırlarda,
    'co2e (t) 1' değerlerini 'co2e (t) 2' sütununa kopyalar.
    Eğer yeniden adlandırma henüz yapılmadıysa, mevcut ilk co2e kolonu -> ikinci co2e kolonu kuralıyla çalışır.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if ds_col is None or ds_col not in out.columns:
        return out
    # co2e kolonlarını sıraya göre bul
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    co2e_cols: list[str] = []
    for c in list(out.columns):
        low_compact = _norm_name(c)
        if low_compact == "co2e(t)" or low_compact.startswith("co2e(t)"):
            co2e_cols.append(c)
    if not co2e_cols:
        return out
    # Tercihen 'co2e (t) 1' ve 'co2e (t) 2' isimleri
    base_col = None
    tgt_col = None
    if "co2e (t) 1" in out.columns:
        base_col = "co2e (t) 1"
    if "co2e (t) 2" in out.columns:
        tgt_col = "co2e (t) 2"
    # Fallback: ilk co2e kolonu -> ikinci co2e kolonu
    if base_col is None:
        base_col = co2e_cols[0]
    if tgt_col is None:
        if len(co2e_cols) >= 2:
            tgt_col = co2e_cols[1]
        else:
            tgt_col = "co2e (t) 2"
            out[tgt_col] = None
    try:
        mask_book = out[ds_col].astype(str).str.strip().str.lower() == "booklets"
        out.loc[mask_book, tgt_col] = out.loc[mask_book, base_col].values
    except Exception:
        return out
    return out


def _ensure_booklets_co2e2_filled(df: pd.DataFrame) -> pd.DataFrame:
    """
    Booklets satırlarında 'co2e (t) 2' boş ise, 'co2e (t) 1' değerleriyle doldurur.
    Sütunlar yoksa oluşturur ve mevcut ilk co2e kolonu ile çalışır.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
    if ds_col is None or ds_col not in out.columns:
        return out
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    # co2e kolonları
    co2e_cols: list[str] = []
    for c in list(out.columns):
        low_compact = _norm_name(c)
        if low_compact == "co2e(t)" or low_compact.startswith("co2e(t)"):
            co2e_cols.append(c)
    if not co2e_cols:
        # Ana kolonu oluştur
        out["co2e (t) 1"] = None
        co2e_cols = ["co2e (t) 1"]
    base_col = "co2e (t) 1" if "co2e (t) 1" in out.columns else co2e_cols[0]
    tgt_col = "co2e (t) 2"
    if tgt_col not in out.columns:
        out[tgt_col] = None
    try:
        mask_book = out[ds_col].astype(str).str.strip().str.lower() == "booklets"
        # boş tanımı: NaN veya trimlenmiş boş string
        tgt = out[tgt_col]
        if tgt.dtype == "object":
            is_empty = tgt.isna() | (tgt.astype(str).str.strip() == "")
        else:
            is_empty = tgt.isna()
        mask = mask_book & is_empty
        out.loc[mask, tgt_col] = out.loc[mask_book, base_col].values
    except Exception:
        return out
    return out
def _copy_reporting_period_for_electricity_extracted(df: pd.DataFrame) -> pd.DataFrame:
    """
    Electricity sheet'i için:
      source_id 'S3C8_Electricity_extracted' ile başlayan satırlarda,
      'Reporting Period' sütunundaki tarihi 'Reporting period (month, year)' sütununa kopyalar.
      Zaman bilgisini kaldırır, 'YYYY-MM-DD' string olarak yazar.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    sid_col = _get_ci_col(out, ["source_id", "Source_ID", "source id"])
    if sid_col is None or sid_col not in out.columns:
        return out
    # Kaynak tarih kolonu
    rp_src_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "reporting period":
            rp_src_col = c
            break
    if rp_src_col is None:
        return out
    # Hedef
    rp_tgt_col = _get_ci_col(out, ["Reporting period (month, year)", "reporting period (month, year)"])
    if rp_tgt_col is None:
        rp_tgt_col = "Reporting period (month, year)"
        out[rp_tgt_col] = pd.Series([None] * len(out), dtype="object")
    try:
        sid_low = out[sid_col].astype(str).str.strip().str.lower()
        mask = sid_low.str.startswith("s3c8_electricity_extracted")
        raw = (
            out.loc[mask, rp_src_col]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
        dt = pd.to_datetime(raw, errors="coerce")
        as_str = dt.dt.strftime("%Y-%m-%d")
        # Hedefi object yap ve ata
        try:
            out[rp_tgt_col] = out[rp_tgt_col].astype("object")
        except Exception:
            pass
        out.loc[mask, rp_tgt_col] = pd.Series(as_str, index=out.index[mask]).astype("object")
    except Exception:
        return out
    return out


def _compose_co2e3_from_main_and_two(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yeni 'co2e (t) 3' sütununu üretir.
    Mantık: önce ana 'co2e (t)' (veya 'co2e (t) 1') değerlerini kopyala,
    ardından mevcutsa 'co2e (t) 2' değerleri ile üstüne yaz.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    # co2e kolonlarını sırayla bul
    co2e_cols: list[str] = []
    for c in list(out.columns):
        low = _norm_name(c)
        if low == "co2e(t)" or low.startswith("co2e(t)"):
            co2e_cols.append(c)
    if not co2e_cols and "co2e (t)" not in out.columns:
        # hiçbir co2e yoksa direkt oluşturup boş bırak
        out["co2e (t) 3"] = None
        return out
    # ana kolon: tercihen 'co2e (t) 1', yoksa 'co2e (t)', yoksa ilk bulunan
    if "co2e (t) 1" in out.columns:
        base_col = "co2e (t) 1"
    elif "co2e (t)" in out.columns:
        base_col = "co2e (t)"
    else:
        base_col = co2e_cols[0]
    # ikinci kolon: tercihen 'co2e (t) 2', yoksa ikinci bulunan
    if "co2e (t) 2" in out.columns:
        two_col = "co2e (t) 2"
    elif len(co2e_cols) >= 2:
        two_col = co2e_cols[1]
    else:
        two_col = None
    # co2e (t) 3'ü oluştur
    out["co2e (t) 3"] = out[base_col].values
    # üzerine yazma: two_col var ve boş olmayan değerler için
    if two_col is not None and two_col in out.columns:
        two_vals = out[two_col]
        if two_vals.dtype == "object":
            mask_non_empty = two_vals.notna() & (two_vals.astype(str).str.strip() != "")
        else:
            mask_non_empty = two_vals.notna()
        out.loc[mask_non_empty, "co2e (t) 3"] = out.loc[mask_non_empty, two_col].values
    return out


def _copy_base_co2e_to_new_co2e(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yeni 'co2e' adlı bir sütun oluşturur ve tabandaki 'co2e (t)' değerlerini aynen kopyalar.
    Taban olarak öncelik sırası: 'co2e (t)' → 'co2e (t) 1' → ilk bulunan co2e(t)* sütunu.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    # Önceliklendirilmiş seçim
    candidates = []
    if "co2e (t)" in out.columns:
        candidates.append("co2e (t)")
    if "co2e (t) 1" in out.columns:
        candidates.append("co2e (t) 1")
    # Fallback: ilk co2e(t)* sütunu
    if not candidates:
        for c in list(out.columns):
            low = _norm_name(c)
            if low == "co2e(t)" or low.startswith("co2e(t)"):
                candidates.append(c)
                break
    if not candidates:
        # hiç yoksa 'co2e' oluştur ama boş bırak
        out["co2e"] = None
        return out
    base_col = candidates[0]
    try:
        out["co2e"] = out[base_col].values
    except Exception:
        out["co2e"] = out[base_col]
    return out


def _overwrite_co2e_with_two_where_source_company(df: pd.DataFrame) -> pd.DataFrame:
    """
    'co2e' sütununu, yalnızca 'source_company' değeri dolu olan satırlarda
    'co2e (t) 2' değerleriyle üzerine yazar. 'co2e (t) 2' boşsa mevcut 'co2e' korunur.
    'co2e' yoksa oluşturulur. 'co2e (t) 2' yoksa ikinci co2e kolonu tahmin edilmeye çalışılır.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # source_company kolonunu bul
    sc_col = _get_ci_col(out, ["source_company", "Source_Company", "source company"])
    if sc_col is None or sc_col not in out.columns:
        return out
    # co2e hedef kolonunu hazırla
    if "co2e" not in out.columns:
        out["co2e"] = None
    # 'co2e (t) 2' kolonunu bul
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    two_col = "co2e (t) 2" if "co2e (t) 2" in out.columns else None
    if two_col is None:
        # ikinci co2e(t)* kolonunu tahmin et
        co2e_cols: list[str] = []
        for c in list(out.columns):
            low = _norm_name(c)
            if low == "co2e(t)" or low.startswith("co2e(t)"):
                co2e_cols.append(c)
        if len(co2e_cols) >= 2:
            two_col = co2e_cols[1]
    if two_col is None or two_col not in out.columns:
        return out
    try:
        # source_company dolu maskesi
        sc = out[sc_col].astype(str)
        mask_sc = sc.notna() & (sc.str.strip() != "") & (sc.str.strip().str.lower() != "none")
        # co2e (t) 2 dolu maskesi
        two = out[two_col]
        if two.dtype == "object":
            mask_two = two.notna() & (two.astype(str).str.strip() != "")
        else:
            mask_two = two.notna()
        mask = mask_sc & mask_two
        out.loc[mask, "co2e"] = out.loc[mask, two_col].values
    except Exception:
        return out
    return out
def _add_total_co2e_concat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a new column 'total co2e' by concatenating 'co2e (t)' and 'co2e (t) 2'
    with a dash in between. Missing values are rendered as empty strings.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Prefer explicit requested columns
    col_main = "co2e (t)" if "co2e (t)" in out.columns else None
    col_two = "co2e (t) 2" if "co2e (t) 2" in out.columns else None
    # Collect all co2e(t) family columns in left-to-right order (fallbacks)
    def _norm_name(s: str) -> str:
        return str(s).lower().replace(" ", "")
    co2e_cols: list[str] = []
    for c in list(out.columns):
        low = _norm_name(c)
        if low == "co2e(t)" or low.startswith("co2e(t)"):
            co2e_cols.append(c)
    # Fallbacks: if requested ones missing, use first and second co2e(t) variants
    if not col_main:
        if "co2e (t) 1" in out.columns:
            col_main = "co2e (t) 1"
        elif co2e_cols:
            col_main = co2e_cols[0]
    if not col_two:
        if len(co2e_cols) >= 2:
            # choose second variant different from col_main
            for cand in co2e_cols:
                if cand != col_main:
                    col_two = cand
                    break
    # Ensure destination column exists
    if "total co2e" not in out.columns:
        out["total co2e"] = pd.Series([None] * len(out), dtype="object")
    if not col_main or not col_two or col_main not in out.columns or col_two not in out.columns:
        # If any source column missing, leave 'total co2e' as None
        return out
    def _to_str(series: pd.Series) -> pd.Series:
        try:
            return series.map(lambda v: "" if (v is None or (hasattr(pd, "isna") and pd.isna(v))) else str(v))
        except Exception:
            try:
                s = series.astype("object")
                return s.map(lambda v: "" if (v is None or (hasattr(pd, "isna") and pd.isna(v))) else str(v))
            except Exception:
                return series.astype(str)
    try:
        s1 = _to_str(out[col_main])
        s2 = _to_str(out[col_two])
        out["total co2e"] = (s1 + "-" + s2).astype("object")
    except Exception:
        # Best-effort; keep as-is on error
        return out
    return out
def _copy_reporting_period_for_district_prefixes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Electricity sheet'i için:
      source_id 'Scope_3_Cat_8_District_H*' veya 'Scope_3_Cat_8_District_E*' ile başlayan satırlarda
      'Reporting Period' -> 'Reporting period (month, year)' kopyalar (YYYY-MM-DD, saatsiz).
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    sid_col = _get_ci_col(out, ["source_id", "Source_ID", "source id"])
    if sid_col is None or sid_col not in out.columns:
        return out
    rp_src_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "reporting period":
            rp_src_col = c
            break
    if rp_src_col is None:
        return out
    rp_tgt_col = _get_ci_col(out, ["Reporting period (month, year)", "reporting period (month, year)"])
    if rp_tgt_col is None:
        rp_tgt_col = "Reporting period (month, year)"
        out[rp_tgt_col] = pd.Series([None] * len(out), dtype="object")
    try:
        sid_low = out[sid_col].astype(str).str.strip().str.lower()
        mask = sid_low.str.startswith("scope_3_cat_8_district_h") | sid_low.str.startswith("scope_3_cat_8_district_e")
        raw = (
            out.loc[mask, rp_src_col]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
        dt = pd.to_datetime(raw, errors="coerce")
        as_str = dt.dt.strftime("%Y-%m-%d")
        try:
            out[rp_tgt_col] = out[rp_tgt_col].astype("object")
        except Exception:
            pass
        out.loc[mask, rp_tgt_col] = pd.Series(as_str, index=out.index[mask]).astype("object")
    except Exception:
        return out
    return out


def _copy_reporting_period_for_s2_average(df: pd.DataFrame) -> pd.DataFrame:
    """
    Electricity sheet'i için:
      source_id 'Scope_2_Electricity_Average*' ile başlayan satırlarda
      'Reporting Period' -> 'Reporting period (month, year)' kopyalar (YYYY-MM-DD).
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    sid_col = _get_ci_col(out, ["source_id", "Source_ID", "source id"])
    if sid_col is None or sid_col not in out.columns:
        return out
    rp_src_col = None
    for c in list(out.columns):
        if str(c).strip().lower() == "reporting period":
            rp_src_col = c
            break
    if rp_src_col is None:
        return out
    rp_tgt_col = _get_ci_col(out, ["Reporting period (month, year)", "reporting period (month, year)"])
    if rp_tgt_col is None:
        rp_tgt_col = "Reporting period (month, year)"
        out[rp_tgt_col] = pd.Series([None] * len(out), dtype="object")
    try:
        sid_low = out[sid_col].astype(str).str.strip().str.lower()
        mask = sid_low.str.startswith("scope_2_electricity_average")
        raw = (
            out.loc[mask, rp_src_col]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
        dt = pd.to_datetime(raw, errors="coerce")
        as_str = dt.dt.strftime("%Y-%m-%d")
        try:
            out[rp_tgt_col] = out[rp_tgt_col].astype("object")
        except Exception:
            pass
        out.loc[mask, rp_tgt_col] = pd.Series(as_str, index=out.index[mask]).astype("object")
    except Exception:
        return out
    return out


def _map_fera_electricity_by_country(df: pd.DataFrame, ef_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Scope 3 Cat 3 FERA Electricity için country -> EF eşlemesi.
    EF kaynağı: 'Scope 3 Category 3 FERA Electri'
    Eşleşme: EF sheet'te 'ef_name' (veya 'country') == satırdaki Country (case-insensitive, trim)
    Yazılacak kolonlar: ef_category, ef_id, ef_value, ef_unit, ef_source, Emission Factor Category
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Country column in spend df
    country_col = _get_ci_col(out, ["Country", "country"])
    if country_col is None:
        # try any column containing 'country'
        for c in list(out.columns):
            if "country" in str(c).strip().lower():
                country_col = c
                break
    if country_col is None:
        return out
    # Find EF sheet key
    ef_key = None
    for k in ef_dict.keys():
        if str(k).strip().lower() == "scope 3 category 3 fera electri":
            ef_key = k
            break
    if ef_key is None:
        return out
    ef_df = ef_dict.get(ef_key)
    if ef_df is None or ef_df.empty:
        return out
    # EF columns
    ef_name_col = mu._find_first_present_column(ef_df, ["ef_name", "EF Name", "name"])
    ef_cat_col = mu._find_first_present_column(ef_df, ["ef_category", "EF Category", "category"])
    ef_id_col = mu._find_first_present_column(ef_df, ["ef_id", "EF ID", "EFID", "id"])
    ef_val_col = mu._find_first_present_column(ef_df, ["ef_value", "EF Value", "value"])
    ef_unit_col = mu._find_first_present_column(ef_df, ["ef_unit", "EF Unit", "unit", "units"])
    ef_src_col = mu._find_first_present_column(ef_df, ["ef_source", "EF Source", "source"])
    ef_em_cat_col = mu._find_first_present_column(ef_df, ["Emission Factor Category", "emission factor category"])
    # Optional country column in EF
    ef_country_col = mu._find_first_present_column(ef_df, ["country", "Country"])
    # Prepare lookup by normalized country
    def _norm(s: Optional[str]) -> str:
        return "" if s is None else str(s).strip().lower()
    ef_lookup: Dict[str, int] = {}
    if ef_name_col and ef_name_col in ef_df.columns:
        for idx, val in ef_df[ef_name_col].items():
            key = _norm(val)
            if key and key not in ef_lookup:
                ef_lookup[key] = idx
    if ef_country_col and ef_country_col in ef_df.columns:
        for idx, val in ef_df[ef_country_col].items():
            key = _norm(val)
            if key and key not in ef_lookup:
                ef_lookup[key] = idx
    # Ensure output columns exist
    for col in ["ef_category", "ef_id", "ef_value", "ef_unit", "ef_source", "Emission Factor Category"]:
        if col not in out.columns:
            out[col] = None
    # Map per row
    for i in range(len(out)):
        ctry = _norm(out.iloc[i][country_col])
        if not ctry:
            continue
        hit_idx = ef_lookup.get(ctry)
        if hit_idx is None:
            continue
        row = ef_df.loc[hit_idx]
        def _g(col: Optional[str]):
            return row[col] if col and col in row else None
        out.at[i, "ef_category"] = _g(ef_cat_col)
        out.at[i, "ef_id"] = _g(ef_id_col)
        out.at[i, "ef_value"] = _g(ef_val_col)
        out.at[i, "ef_unit"] = _g(ef_unit_col)
        out.at[i, "ef_source"] = _g(ef_src_col)
        out.at[i, "Emission Factor Category"] = _g(ef_em_cat_col)
    return out


def _map_fera_fuel_by_vehicle(df: pd.DataFrame, ef_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Scope 3 Cat 3 FERA Fuel için Vehicle Type -> EF eşlemesi.
    EF kaynağı: 'Scope 3 Category 3 FERA Fuel'
    Eşleşme: EF sheet'in 'ef_name' kolonu, satırdaki vehicle type metnini içeriyorsa (case-insensitive).
    Özel token kuralları: diesel -> 'diesel', petrol/gasoline/hybrid -> 'petrol', cng -> 'cng'
    Yazılacak kolonlar: ef_category, ef_id, ef_value, ef_unit, ef_source, Emission Factor Category
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Vehicle Type column in spend df
    vt_col = _get_ci_col(out, ["Vehicle Type", "Vehicle type", "vehicle type"])
    if vt_col is None:
        return out
    # Find EF sheet key
    ef_key = None
    for k in ef_dict.keys():
        if str(k).strip().lower() == "scope 3 category 3 fera fuel":
            ef_key = k
            break
    if ef_key is None:
        return out
    ef_df = ef_dict.get(ef_key)
    if ef_df is None or ef_df.empty:
        return out
    # EF columns
    ef_name_col = mu._find_first_present_column(ef_df, ["ef_name", "EF Name", "name", "ef_description", "description"])
    ef_cat_col = mu._find_first_present_column(ef_df, ["ef_category", "EF Category", "category"])
    ef_id_col = mu._find_first_present_column(ef_df, ["ef_id", "EF ID", "EFID", "id"])
    ef_val_col = mu._find_first_present_column(ef_df, ["ef_value", "EF Value", "value"])
    ef_unit_col = mu._find_first_present_column(ef_df, ["ef_unit", "EF Unit", "unit", "units"])
    ef_src_col = mu._find_first_present_column(ef_df, ["ef_source", "EF Source", "source"])
    ef_em_cat_col = mu._find_first_present_column(ef_df, ["Emission Factor Category", "emission factor category"])
    if ef_name_col is None or ef_name_col not in ef_df.columns:
        return out
    # Ensure output columns exist
    for col in ["ef_category", "ef_id", "ef_value", "ef_unit", "ef_source", "Emission Factor Category"]:
        if col not in out.columns:
            out[col] = None
    # helper
    def _norm(s: Optional[str]) -> str:
        return "" if s is None else str(s).strip().lower()
    def _pick_token(vt_text: str) -> str:
        t = vt_text.strip().lower()
        if "diesel" in t:
            return "diesel"
        if "cng" in t:
            return "cng"
        if "petrol" in t or "gasoline" in t or "hybrid" in t:
            return "petrol"
        # fallback: first word
        return t.split()[0] if t else ""
    # Map per row
    names_low = ef_df[ef_name_col].astype(str).str.strip().str.lower()
    for i in range(len(out)):
        vt_raw = out.iloc[i][vt_col]
        vt = _norm(vt_raw)
        if not vt:
            continue
        token = _pick_token(vt)
        if not token:
            continue
        try:
            mask = names_low.str.contains(rf"\b{re.escape(token)}\b", regex=True, na=False)
            hits = ef_df[mask]
        except Exception:
            hits = ef_df.head(0)
        if hits.empty:
            continue
        best = hits.iloc[0]
        def _g(row: pd.Series, col: Optional[str]):
            return row[col] if col and col in row else None
        out.at[i, "ef_category"] = _g(best, ef_cat_col)
        out.at[i, "ef_id"] = _g(best, ef_id_col)
        out.at[i, "ef_value"] = _g(best, ef_val_col)
        out.at[i, "ef_unit"] = _g(best, ef_unit_col)
        out.at[i, "ef_source"] = _g(best, ef_src_col)
        out.at[i, "Emission Factor Category"] = _g(best, ef_em_cat_col)
    return out


def _backfill_fera_fuel_ef_from_scope1(all_sheets: Dict[str, pd.DataFrame], df_fuel: pd.DataFrame) -> pd.DataFrame:
    """
    Scope 3 Cat 3 FERA Fuel içinde, source_id si
      - 'Scope_3_Cat_8_Fuel_Usage_Spend_'
      - 'Scope_3_Cat_8_Fuel_Usage_Activi_'
    ile başlayan satırlar için EF bilgilerini, daha önce mapping yapılmış
    Scope 1 sheet'lerinden (Spend/Activity) source_id eşleşmesi ile kopyalar.
    Kopyalanan kolonlar: ef_id, ef_value, ef_unit, ef_source ve ef_name (→ ef_category/Emission Factor Category).
    """
    if df_fuel is None or df_fuel.empty:
        return df_fuel
    out = df_fuel.copy()
    # hedef source_id kolonu
    sid_col_dest = _get_ci_col(out, ["source_id", "Source_ID", "source id"])
    if sid_col_dest is None or sid_col_dest not in out.columns:
        return out
    # Kaynak sheetler
    candidates = [
        "Scope 1 Fuel Usage Spend",
        "Scope 1 Fuel Activity",
        "Scope 1 Fuel Usage Activity",
    ]
    # ef alanlarını toplayan map
    ef_by_source_id: Dict[str, Dict[str, object]] = {}
    for name in candidates:
        src_df = all_sheets.get(name)
        if src_df is None or src_df.empty:
            continue
        sid_col_src = _get_ci_col(src_df, ["source_id", "Source_ID", "source id"])
        if sid_col_src is None or sid_col_src not in src_df.columns:
            continue
        col_ef_id = mu._find_first_present_column(src_df, ["ef_id", "EF ID", "EFID", "id"])
        col_ef_val = mu._find_first_present_column(src_df, ["ef_value", "EF Value", "value"])
        col_ef_unit = mu._find_first_present_column(src_df, ["ef_unit", "EF Unit", "unit", "units"])
        col_ef_src = mu._find_first_present_column(src_df, ["ef_source", "EF Source", "source"])
        col_ef_name = mu._find_first_present_column(src_df, ["ef_name", "EF Name", "name", "Emission Factor Category"])
        for _, r in src_df.iterrows():
            sid = str(r.get(sid_col_src, "")).strip()
            if not sid:
                continue
            key = sid.lower()
            if key not in ef_by_source_id:
                ef_by_source_id[key] = {
                    "ef_id": r.get(col_ef_id) if col_ef_id else None,
                    "ef_value": r.get(col_ef_val) if col_ef_val else None,
                    "ef_unit": r.get(col_ef_unit) if col_ef_unit else None,
                    "ef_source": r.get(col_ef_src) if col_ef_src else None,
                    "ef_name": r.get(col_ef_name) if col_ef_name else None,
                }
    # Hedefe uygula
    for col in ["ef_id", "ef_value", "ef_unit", "ef_source", "ef_category", "Emission Factor Category"]:
        if col not in out.columns:
            out[col] = None
    try:
        sid_series = out[sid_col_dest].astype(str)
    except Exception:
        sid_series = out[sid_col_dest]
    prefixes = (
        "scope_3_cat_8_fuel_usage_spend_",
        "scope_3_cat_8_fuel_usage_activi_",
    )
    for i in range(len(out)):
        sid = str(sid_series.iloc[i]).strip()
        low = sid.lower()
        if not sid or not any(low.startswith(p) for p in prefixes):
            continue
        rec = ef_by_source_id.get(low)
        if not rec:
            continue
        out.at[i, "ef_id"] = rec.get("ef_id")
        out.at[i, "ef_value"] = rec.get("ef_value")
        out.at[i, "ef_unit"] = rec.get("ef_unit")
        out.at[i, "ef_source"] = rec.get("ef_source")
        name_val = rec.get("ef_name")
        if name_val is not None and str(name_val).strip() != "":
            out.at[i, "ef_category"] = name_val
            out.at[i, "Emission Factor Category"] = name_val
    return out


def _map_fera_fuel_using_core_logic(df: pd.DataFrame, ef_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Scope 3 Cat 3 FERA Fuel için ana mapping mantığını (mapping_utils.map_emission_factor)
    yeniden kullanır. Her satırın 'Sheet' değerini görünür FERA Fuel adıyla set edip
    ef_id, ef_value, ef_unit, ef_source ve isim alanlarını doldurur.
    Ayrıca 'ef_category' ve 'Emission Factor Category' alanlarını EF Name ile senkronlar.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    # Visible sheet name
    visible = FERA_FUEL_SHEET
    out["Sheet"] = visible
    # Ensure target columns
    for col in ["ef_category", "ef_id", "ef_value", "ef_unit", "ef_source", "Emission Factor Category"]:
        if col not in out.columns:
            out[col] = None
    # Map row-by-row with core logic
    records = []
    for _, row in out.iterrows():
        try:
            res = mu.map_emission_factor(row, ef_dict)
        except Exception:
            res = {"status": "error"}
        records.append(res)
    def _col(vals_key: str):
        vals = []
        for r in records:
            vals.append(r.get(vals_key))
        return pd.Series(vals, dtype="object")
    # Assign
    ef_name_series = _col("EF Name")
    out["ef_id"] = _col("EF ID")
    out["ef_unit"] = _col("EF Unit")
    out["ef_value"] = _col("EF Value")
    out["ef_source"] = _col("Source")
    # Mirror EF Name into category fields (uyum için)
    try:
        out["ef_category"] = ef_name_series
        out["Emission Factor Category"] = ef_name_series
    except Exception:
        pass
    return out


def _concat_align(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    parts = [d for d in dfs if d is not None and not d.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, axis=0, join="outer", ignore_index=True)

#Purchased goods ve service mappingi yaparken Mecwide Nordics'ten gelen veriler icin Product description sutununa bakman gerkiyor cunku Product type sutunundaki bilgiler cok genel kalmis.


def _filter_klarakarbon_subset(df_klar: pd.DataFrame, scope_flag: str) -> pd.DataFrame:
    """
    Filter Klarakarbon rows per requested scope.
    scope_flag: "scope 1" or "scope 2"
    """
    if df_klar is None or df_klar.empty:
        return pd.DataFrame()
    temp = df_klar.copy()
    lowmap = {str(c).strip().lower(): c for c in temp.columns}
    ghg_col = None
    for key in ["ghgp category", "ghg category", "ghg_category"]:
        if key in lowmap:
            ghg_col = lowmap[key]
            break
    scope_col = None
    for key in ["scope"]:
        if key in lowmap:
            scope_col = lowmap[key]
            break
    mask = pd.Series([False] * len(temp))
    if ghg_col and ghg_col in temp.columns:
        try:
            ghg = temp[ghg_col].astype(str).str.strip().str.lower()
            mask = ghg.str.startswith(scope_flag)
        except Exception:
            pass
    if not bool(getattr(mask, "any", lambda: False)()):
        if scope_col and scope_col in temp.columns:
            try:
                sc = temp[scope_col].astype(str).str.strip().str.lower()
                mask = sc == scope_flag
            except Exception:
                pass
    try:
        return temp.loc[mask].copy()
    except Exception:
        return pd.DataFrame()


def _build_fera_sheets(all_sheets: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Construct FERA Fuel and Electricity input frames from existing sheets + Klarakarbon subsets.
    """
    def _mark_source(df_in: Optional[pd.DataFrame], source_label: str) -> Optional[pd.DataFrame]:
        if df_in is None or df_in.empty:
            return df_in
        temp = df_in.copy()
        # Mark explicit data source
        try:
            temp["Data Source sheet"] = source_label
        except Exception:
            temp["Data Source sheet"] = pd.Series([source_label] * len(temp), dtype="object")
        # Klarakarbon için Company üzerinde herhangi bir eşleştirme/atama YAPMA
        return temp

    # Fuel: Scope 1 sheetlerinden belirli 'Sheet' değerleri ile gelen satırları taşı + Klarakarbon(Scope 1) ekle
    def _find_sheet_ci(prefix: str) -> Optional[pd.DataFrame]:
        pref_low = prefix.strip().lower()
        # exact first
        for key, df in all_sheets.items():
            if str(key).strip().lower() == pref_low:
                return df
        # startswith fallback (31-char truncated isimler için)
        for key, df in all_sheets.items():
            if str(key).strip().lower().startswith(pref_low):
                return df
        return None

    # Helper: aynı isimle key'i de bul
    def _find_sheet_and_key_ci(prefix: str) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
        pref_low = prefix.strip().lower()
        # exact
        for key, df in all_sheets.items():
            if str(key).strip().lower() == pref_low:
                return key, df
        # startswith
        for key, df in all_sheets.items():
            if str(key).strip().lower().startswith(pref_low):
                return key, df
        return None, None

    # Scope 1 kaynaklarını bul
    s1_spend_key, s1_spend_df = _find_sheet_and_key_ci("Scope 1 Fuel Usage Spend")
    s1_act_key, s1_act_df = _find_sheet_and_key_ci("Scope 1 Fuel Activity")

    # Sheet kolonunda bu değerleri taşı
    def _pick_rows(df: Optional[pd.DataFrame], wanted_prefix: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df is None or df.empty:
            return pd.DataFrame(), pd.DataFrame()
        sheet_col = _get_ci_col(df, ["Sheet", "sheet"])
        if sheet_col is None or sheet_col not in df.columns:
            return pd.DataFrame(), df
        low = df[sheet_col].astype(str).str.strip().str.lower()
        wanted_low = wanted_prefix.strip().lower()
        mask = low == wanted_low
        moved = df.loc[mask].copy()
        remain = df.loc[~mask].copy()
        return moved, remain

    # Taşınacak satırlar
    moved_spend, remain_spend = _pick_rows(s1_spend_df, "Scope 3 Cat 8 Fuel Usage Spend")
    moved_act, remain_act = _pick_rows(s1_act_df, "Scope 3 Cat 8 Fuel Usage Activi")

    klar = all_sheets.get("Klarakarbon")
    klar_s1 = _filter_klarakarbon_subset(klar, "scope 1") if klar is not None else pd.DataFrame()
    # Tag as sources
    moved_spend = _mark_source(moved_spend, "Booklets")
    moved_act = _mark_source(moved_act, "Booklets")
    klar_s1 = _mark_source(klar_s1, "Klarakarbon")
    # Klarakarbon'dan gelen satırlarda 'co2e (t)' kolonunu (ve varyantlarını) hariç tut
    try:
        if klar_s1 is not None and not klar_s1.empty:
            drop_cols: list[str] = []
            for c in list(klar_s1.columns):
                low = str(c).strip().lower().replace(" ", "")
                if low == "co2e(t)" or low.startswith("co2e(t)."):
                    drop_cols.append(c)
            if drop_cols:
                klar_s1 = klar_s1.drop(columns=drop_cols, errors="ignore")
    except Exception:
        pass
    fuel_df = _concat_align([moved_spend, moved_act, klar_s1])

    # Not cutting rows from original Scope 1 sheets anymore.
    # Keep S1 sheets intact so the moved records also remain visible under Scope 1.

    # Electricity: Scope 2 + district + Klarakarbon S2
    s2_elec = all_sheets.get("Scope 2 Electricity")
    s2_avg = all_sheets.get("Scope 2 Electricity Average")
    s3_d_e = all_sheets.get("Scope 3 Cat 8 District E")
    s3_d_h = all_sheets.get("Scope 3 Cat 8 District H")
    klar_s2 = _filter_klarakarbon_subset(klar, "scope 2") if klar is not None else pd.DataFrame()
    # Tag sources
    s2_elec = _mark_source(s2_elec, "Booklets")
    s2_avg = _mark_source(s2_avg, "Booklets")
    s3_d_e = _mark_source(s3_d_e, "Booklets")
    s3_d_h = _mark_source(s3_d_h, "Booklets")
    klar_s2 = _mark_source(klar_s2, "Klarakarbon")
    elec_df = _concat_align([s2_elec, s2_avg, s3_d_e, s3_d_h, klar_s2])
    return fuel_df, elec_df


def apply_fera_mapping(target_workbook: Optional[Path] = None) -> Optional[Path]:
    """
    Create or replace two sheets in the mapped workbook:
      - Scope 3 Cat 3 FERA Fuel
      - Scope 3 Cat 3 FERA Electricity
    Using EF mapping from:
      - 'Scope 3 Category 3 FERA Fuel'
      - 'Scope 3 Category 3 FERA Electri'
    respectively. Emissions computed as 'co2e (t)'.
    """
    if target_workbook is None:
        target_workbook = _find_latest_workbook_for_mera(BASE_DIR)
    if target_workbook is None:
        print("FERA: No mapped workbook found.")
        return None

    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(target_workbook, sheet_name=None, engine="openpyxl")
    except Exception as exc:
        print(f"FERA: Failed to read workbook: {exc}")
        return None
    if not all_sheets:
        print("FERA: Workbook is empty.")
        return None

    # Build inputs
    fuel_src, elec_src = _build_fera_sheets(all_sheets)
    if fuel_src is None:
        fuel_src = pd.DataFrame()
    if elec_src is None:
        elec_src = pd.DataFrame()

    # NO MAPPING: sadece sheet'leri hazırla ve Company/co2e sütunlarını düzenle
    def _ensure_cols(df: pd.DataFrame, ghgp_val: str) -> pd.DataFrame:
        out = df.copy()
        try:
            out["GHGP Category"] = ghgp_val
        except Exception:
            out["GHGP Category"] = pd.Series([ghgp_val] * len(out), dtype="object")
        try:
            out["Scope"] = 3
        except Exception:
            out["Scope"] = pd.Series([3] * len(out), dtype="Int64")
        return out

    # Load EF dictionary once (used for both Fuel and Electricity)
    try:
        ef_dict = mu.load_emission_factors(BASE_DIR)
    except Exception:
        ef_dict = {}

    # FUEL: EN BAŞTAKİ HALİ (S3C8 Spend + S3C8 Activity + Klarakarbon S1)
    fuel_final = fuel_src.copy()
    # İstenen temizlik adımları:
    # 1) Vehicle Type boş/NA satırları sil (Booklets için)
    fuel_final = _drop_booklets_rows_missing_vehicle_type(fuel_final)
    # 2) 'Sheet' == 'Sheet1' satırlarında Source_File = source_company + '.xlsx'
    fuel_final = _override_source_file_from_source_company_where_sheet1(fuel_final)
    # 3) Source_File remap (Gapit.xlsx, GT Nordics.xlsx, NordicEPOD.xlsx, NEP Switchboards.xlsx)
    fuel_final = _remap_source_file_names(fuel_final)
    # 4) Klarakarbon satırlarında release date → Reporting period (month, year) (YYYY-MM-DD)
    fuel_final = _copy_release_date_to_reporting_period_for_klarakarbon(fuel_final)

    # --- FUEL: CTS Denmark/Nordics Diesel mapping (distance vs activity) ---
    if ef_dict:
        fuel_final = _map_fera_fuel_cts_diesel_distance_or_activity(fuel_final, ef_dict)
        fuel_final = _fill_missing_co2e_for_fera_fuel_cts(fuel_final)
    # 5) Klarakarbon için manuel EF eşlemesini yeni sütunlarda göster
    fuel_final = _apply_klarakarbon_manual_ef_map_fuel(fuel_final)
    # 6) Klarakarbon satırları için co2e (t) = activity volume * kbk_map_ef_value
    fuel_final = _compute_kbk_co2e_from_activity(fuel_final)
    # 6b) Eğer aynı isimli birden fazla 'co2e (t)' oluştuyse, Klarakarbon satırları için ilk sütunu son sütunla güncelle
    fuel_final = _overwrite_primary_co2e_with_last_duplicate_for_kbk(fuel_final)
    # 7) Klarakarbon satırlarında ikincil 'co2e (t)' ile üzerine yazma ADIMINI KALDIRDIK (geri alındı)
    # 8) Booklets satırlarında ana 'co2e (t)' → ikinci 'co2e (t)' kopyası
    fuel_final = _mirror_booklets_co2e_to_secondary(fuel_final)
    # 9) GENEL KOPYA ADIMI KALDIRILDI: Klarakarbon değerlerini korumak için yalnızca Booklets'e uygulanır
    # 10) co2e (t) sütunlarının adlarını benzersiz yap (co2e (t) 1,2,3...)
    fuel_final = _rename_duplicate_co2e_columns(fuel_final)
    # 11) Booklets satırları için 'co2e (t) 1' -> 'co2e (t) 2' kopyası (istenen çıktı adıyla)
    fuel_final = _copy_booklets_co2e1_to_co2e2(fuel_final)
    # 12) 'co2e (t) 2' hâlâ boş kalan Booklets satırlarını garanti doldur
    fuel_final = _ensure_booklets_co2e2_filled(fuel_final)
    # 13) Her iki FERA sheet'inde istenen tekil GHGP Category metnini kullan
    common_ghgp = "Scope 3 Category 3 Fuel and Energy Related Activities"
    fuel_final = _force_ghgp_category(fuel_final, common_ghgp)
    # 13b) Fuel sheet: add 'total co2e' = 'co2e (t)' + '-' + 'co2e (t) 2'
    fuel_final = _add_total_co2e_concat(fuel_final)
    # 14-15) Ek 'co2e (t) 3' ve 'co2e' üretim adımları GERİ ALINDI (çağrılmıyor)

    # ELECTRICITY: kategoriyi ve scope'u ekle (elektrik için dönüşümler devam ediyor)
    elec_mapped = _ensure_cols(elec_src, FERA_ELEC_GHGP)

    # Company: 'None' stringlerini boşluğa çevir (elektrik için)
    elec_mapped = _clear_company_none_to_empty(elec_mapped)

    # 'Sheet' == 'Sheet1' satırlarında Source_File'ı source_company'den kopyala + '.xlsx' (elektrik)
    elec_mapped = _override_source_file_from_source_company_where_sheet1(elec_mapped)

    # Source_File remap istekleri (elektrik)
    elec_mapped = _remap_source_file_names(elec_mapped)

    # Country doldurma (elektrik)
    elec_mapped = _fill_country_from_source_file(elec_mapped)

    # Klarakarbon satırlarında release date → Reporting period (elektrik)
    elec_mapped = _copy_release_date_to_reporting_period_for_klarakarbon(elec_mapped)

    # Electricity: source_id 'S3C8_Electricity_extracted*' için Reporting Period -> Reporting period (month, year)
    elec_mapped = _copy_reporting_period_for_electricity_extracted(elec_mapped)
    # Electricity: District H/E kaynakları için aynı kural
    elec_mapped = _copy_reporting_period_for_district_prefixes(elec_mapped)
    # Electricity: Scope_2_Electricity_Average* için aynı kural
    elec_mapped = _copy_reporting_period_for_s2_average(elec_mapped)

    # Mapping artefact kolonlarını kaldır ve co2e (t) sütunlarını temizle (elektrik)
    elec_mapped = _strip_mapping_artifacts(elec_mapped)
    elec_mapped = _drop_old_co2e_and_add_new(elec_mapped)

    # --- ELECTRICITY: Country -> EF mapping (FERA Electri) ---
    if ef_dict:
        elec_mapped = _map_fera_electricity_by_country(elec_mapped, ef_dict)

    # Electricity (Klarakarbon): co2e (t) = activity volume * ef_value
    elec_mapped = _compute_kbk_co2e_electricity(elec_mapped)
    # Electricity (Booklets): co2e (t) = (Electricity Consumption + Heating Consumption) * ef_value
    elec_mapped = _compute_booklets_co2e_electricity(elec_mapped)
    # Electricity (Consumption present): co2e (t) = Consumption * ef_value
    elec_mapped = _compute_consumption_co2e_electricity(elec_mapped)
    # GHGP Category'yi aynı tekil metne sabitle
    elec_mapped = _force_ghgp_category(elec_mapped, common_ghgp)
    # Always write out a NEW timestamped workbook to avoid Excel locks and make result explicit
    try:
        out_dir = STAGE2_OUTPUT_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"mapped_results_merged_fera_{ts}.xlsx"
        new_path = out_dir / new_name
        with pd.ExcelWriter(new_path, engine="openpyxl") as writer:
            # Copy all original sheets first
            for name, df in all_sheets.items():
                safe = name[:31] if len(name) > 31 else name
                # Skip our two FERA targets, they will be written from mapped versions
                if str(safe).strip().lower() in {
                    FERA_FUEL_SHEET.strip().lower(),
                    FERA_ELEC_SHEET.strip().lower(),
                }:
                    continue
                df.to_excel(writer, sheet_name=safe, index=False)
            # Write the two updated FERA sheets
            fuel_final.to_excel(writer, sheet_name=FERA_FUEL_SHEET[:31], index=False)
            elec_mapped.to_excel(writer, sheet_name=FERA_ELEC_SHEET[:31], index=False)
        print(f"FERA: Wrote new workbook: {new_path.name}")
        return new_path
    except Exception as exc:
        print(f"FERA: Failed to write new workbook: {exc}")
        return None


def main() -> None:
    apply_fera_mapping()


if __name__ == "__main__":
    main()

