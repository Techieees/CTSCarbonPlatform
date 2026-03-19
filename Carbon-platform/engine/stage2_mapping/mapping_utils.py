from __future__ import annotations

from enum import nonmember
from inspect import isframe
from pickletools import read_long1
import os
import re
import difflib
from pathlib import Path
from sqlite3 import SQLITE_CANTOPEN_DIRTYWAL
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd
from pandas.core.accessor import CachedAccessor
from pandas.core.methods.describe import describe_categorical_1d


# -----------------------------
# Feature flags (runtime)
# -----------------------------
def _env_flag(name: str, default: bool = False) -> bool:
    """
    Parse a boolean-like environment variable.
    Truthy: 1, true, yes, y, on
    Falsy:  0, false, no, n, off, (empty)
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    v = str(raw).strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


# This dictionary will be provided by the Sustainability Data Analyst.
# Paste the final mapping provided by the Sustainability Data Analyst to replace the empty dict below.
sheet_mapping: Dict[str, Union[str, List[str]]] = {
    # Scope 1
    "Scope 1 Fuel Usage Spend": "Scope 1 Fuel Spend",
    "Scope 1 Fuel Usage Activity": "Scope 1 Fuel Distance, Scope 1 Fuel Activity",
    "Scope 1 Fuel Activity": "Scope 1 Fuel Activity",
    "Scope 1 Fugitive Gases": "Scope 1 Fugitive Gas",

    # Scope 2
    "Scope 2 Electricity": "Scope 2 Electricity",
    "Scope 2 Electricity Average": "Scope 2 Electricity",

    # Scope 3 Category 1 (Purchased Goods & Services)
    # Prefer integrated 'All together' sheets (contain consolidated manual mappings)
    "Scope 3 Cat 1 Goods Spend": "All together, All together 2, Scope 3 Purchased Goods Spend",
    "Scope 3 Cat 1 Services Spend": "All together, All together 2, Scope 3 Purchased Service Spend",
    # New non-Cat1 sheet that should follow Services logic
    "Scope 3 Services Spend": "All together, All together 2, Scope 3 Purchased Service Spend",
    "Scope 3 Cat 1 Common Purchases": "All together, All together 2, Scope 3 Purchased Goods Spend, Scope 3 Purchased Service Spend",
    "Scope 3 Cat 1 Goods Activity": "All together, All together 2, Scope 3 Purchased Goods Spend",
    "Scope 3 Cat 1 Services Activity": "All together, All together 2, Scope 3 Purchased Service Spend",
    "Scope 3 Cat 1 Supplier Summary": "All together, All together 2, Scope 3 Purchased Goods Spend, Scope 3 Purchased Service Spend",
    # Allow both singular/plural key variants
    "Scope 3 Cat 1 Goods Service": "Scope 3 Purchased Goods Spend, Scope 3 Purchased Service Spend",
    "Scope 3 Cat 1 Goods Services": "Scope 3 Purchased Goods Spend, Scope 3 Purchased Service Spend",

    # Scope 3 Category 2 (Capital Goods)
    "Scope 3 Cat 2 Capital Goods Spe": "Scope 3 Category 2 Capital Good",
    "Scope 3 Cat 2 Capital Goods Act": "Scope 3 Category 2 Capital Good",

    # Scope 3 Category 4+9 (Transport)
    "Scope 3 Cat 4+9 Transport Spend": "Scope 3 Category 4 Transport",

    # Scope 3 Category 5 (Waste)
    "Scope 3 Cat 5 Waste": "Scope 3 Cat 5 Waste",
    "Scope 3 Cat 5 Office Waste": "Scope 3 Cat 5 Waste",
    "Scope 3 Cat 5 Waste Oslo": "Scope 3 Cat 5 Waste",

    # Scope 3 Category 6 (Business Travel)
    # Map to the dedicated EF sheet for business travel if available
    "Scope 3 Cat 6 Business Travel": "Scope 3 Cat 6 Business Travel",

    # Scope 3 Category 7 (Employee Commute)
    "Scope 3 Cat 7 Employee Commute": "Scope 3 Cat 7 Employee Commutin",

    # Scope 3 Category 8 (Upstream T&D, Fuel/Electricity)
    "Scope 3 Cat 8 Fuel Usage Spend": "Scope 3 Cat 3 FERA Fuel S",
    "Scope 3 Cat 8 Fuel Activity": "Scope 3 Category 3 FERA Fuel",
    "Scope 3 Cat 8 Fuel Usage Activit": "Scope 3 Category 3 FERA Fuel",
    # Some workbooks truncate differently (missing trailing 't' in Activity)
    "Scope 3 Cat 8 Fuel Usage Activi": "Scope 3 Category 3 FERA Fuel",
    "Scope 3 Cat 8 Electricity": "Scope 3 Category 3 FERA Electri",
    "S3C8_Electricity_extracted": "Scope 3 Category 3 FERA Electri",
    "Scope 3 Cat 8 District E": "Scope 3 Cat 8 District E",
    "Scope 3 Cat 8 District H": "Scope 3 Cat 8 District H",

    # NEW: Scope 3 Cat 3 FERA consolidated sheets (created post-mapping)
    # Map directly to the corresponding EF sheets
    "Scope 3 Cat 3 FERA Fuel": "Scope 3 Category 3 FERA Fuel",
    "Scope 3 Cat 3 FERA Electricity": "Scope 3 Category 3 FERA Electri",

    # Scope 3 Category 11 (Products Use)
    "Scope 3 Cat 11 Products Indirec": "Scope 3 Cat 11 Products Indirec",

    # Scope 3 Category 12 (End of Life)
    "Scope 3 Cat 12 End of Life": "Scope 3 Cat 12 End of Life",

    # Scope 3 Category 15 (Investments/Pensions)
    "Scope 3 Cat 15 Pensions": "Scope 3 Cat 15 Pensions",
}


def load_emission_factors(path: Union[str, Path]) -> Dict[str, pd.DataFrame]:
    """Load all sheets from the CTS_Emission_factors_short_list.xlsx workbook.

    Args:
        path: Either the directory that contains the workbook or the full path
              to the CTS_Emission_factors_short_list.xlsx file.

    Returns:
        A dictionary mapping sheet name (str) to pandas DataFrame.
    """
    base_path = Path(path)
    if base_path.is_dir():
        file_path = base_path / "CTS_Emission_factors_short_list.xlsx"
    else:
        file_path = base_path

    if file_path.suffix.lower() != ".xlsx":
        raise ValueError(
            "Expected path to 'CTS_Emission_factors_short_list.xlsx' or its directory."
        )

    # Read all sheets in one shot. Returns Dict[str, DataFrame]
    sheets_dict = pd.read_excel(file_path, sheet_name=None)
    # Normalize sheet names by stripping whitespace
    return {str(name).strip(): df for name, df in sheets_dict.items()}


def get_ef_sheet(spend_sheet_name: Optional[str]) -> Optional[Union[str, List[str]]]:
    """Resolve EF sheet name(s) from spend sheet name using sheet_mapping.

    - Returns "EMPTY YET" or "NO NEED" if mapping dictates so.
    - May return a single sheet name (str) or multiple (List[str]).
    - Returns None if no mapping exists.
    """
    if not spend_sheet_name:
        return None

    key_norm = str(spend_sheet_name).strip().lower()
    lowered_map: Dict[str, Union[str, List[str]]] = {
        str(k).strip().lower(): v for k, v in sheet_mapping.items()
    }
    mapped = lowered_map.get(key_norm)

    if isinstance(mapped, str):
        mapped_clean = mapped.strip()
        if mapped_clean.upper() in {"EMPTY YET", "NO NEED"}:
            return mapped_clean
        # Allow simple delimited lists in string values
        if any(sep in mapped_clean for sep in (",", "|", ";")):
            parts: List[str] = []
            for sep in (",", "|", ";"):
                # Split progressively; subsequent splits will act on already-split tokens
                if not parts:
                    parts = [p.strip() for p in mapped_clean.split(sep)]
                else:
                    next_parts: List[str] = []
                    for token in parts:
                        next_parts.extend(p.strip() for p in token.split(sep))
                    parts = next_parts
            parts = [p for p in parts if p]
            # De-duplicate preserving order
            seen: set[str] = set()
            ordered: List[str] = []
            for p in parts:
                if p not in seen:
                    seen.add(p)
                    ordered.append(p)
            return ordered
    return mapped


def normalize_country(name: Any) -> Optional[str]:
    """Return country as-is (no normalization, no Global mapping)."""
    if name is None:
        return None
    text = str(name).strip()
    return text if text else None


# -------------------------
# Matching helper utilities
# -------------------------

def _normalize_colname(col: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(col).lower())


def _find_first_present_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    normalized_map = {_normalize_colname(c): c for c in df.columns}
    for candidate in candidates:
        cand_norm = _normalize_colname(candidate)
        if cand_norm in normalized_map:
            return normalized_map[cand_norm]
    return None


def _get_first_present(row: pd.Series, keys: Sequence[str]) -> Any:
    for k in keys:
        if k in row and pd.notna(row[k]):
            return row[k]
    return None


def _find_country_column(df: pd.DataFrame) -> Optional[str]:
    """Resolve the Country column with robust matching.

    Tries common names; if not found, falls back to any column whose
    normalized name contains 'country'.
    """
    # First, try standard candidates
    standard = [
        "Country",
        "Country Name",
        "Country/Territory",
        "Country/Region",
        "Geography",
        "Region",
        "Location",
    ]
    found = _find_first_present_column(df, standard)
    if found:
        2
        return found
    # Fallback: any column that looks like a country field
    normalized_map = {_normalize_colname(c): c for c in df.columns}
    for norm, original in normalized_map.items():
        if "country" in norm:
            return original
    return None


def _get_country_from_spend_row(row: pd.Series) -> Optional[str]:
    """Extract country from a spend row with flexible header matching.

    Tries common names, then any column whose normalized name contains 'country'.
    Returns normalized text via normalize_country.
    """
    # Try standard headers first (both cases)
    standard_headers = [
        "Country",
        "country",
        "Country Name",
        "country name",
        "Country/Territory",
        "Country/Region",
        "Geography",
        "Location",
        "Region",
    ]
    for h in standard_headers:
        if h in row and pd.notna(row[h]):
            return normalize_country(row[h])

    # Fallback: scan columns containing 'country' case-insensitively
    for col in list(row.index):
        if re.search(r"country", str(col), flags=re.IGNORECASE):
            val = row[col]
            if pd.notna(val):
                return normalize_country(val)
    return None


def _prepare_text_for_match(text: Any) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    s = str(text).lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _tokenize_keywords(text: str) -> List[str]:
    # Keep words with length >= 3 to avoid too generic tokens
    return [t.lower() for t in re.findall(r"[a-zA-Z]{3,}", text)]


def _get_text_columns(df: pd.DataFrame) -> List[str]:
    # Columns likely to contain descriptive text useful for keyword matching
    candidates = [
        "EF Name",
        "Name",
        "Description",
        "Activity",
        "Category",
        "Subcategory",
        "Sub-category",
        "Product",
        "Service",
        "Sector",
    ]
    cols: List[str] = []
    for c in candidates:
        found = _find_first_present_column(df, [c])
        if found:
            cols.append(found)
    return cols


def _best_row_by_scores(
    df: pd.DataFrame,
    country_match_mask: pd.Series,
    global_match_mask: pd.Series,
    keyword_scores: Optional[pd.Series],
) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None

    # Base score: 2 for country match, 1 for global, 0 otherwise
    base_score = (
        country_match_mask.astype(int) * 2 + global_match_mask.astype(int) * 1
    )
    if keyword_scores is not None:
        total_score = base_score + keyword_scores.fillna(0).astype(int)
    else:
        total_score = base_score

    if total_score.max() <= 0:
        return None

    idx = total_score.idxmax()
    return df.loc[idx]


def _find_code_column(df: pd.DataFrame) -> Optional[str]:
    """Find a likely product/TFM code column in EF data."""
    return _find_first_present_column(
        df,
        [
            "Product Code",
            "TFM Code",
            "TFM",
            "Code",
            "ProductCode",
            "Product_Code",
        ],
    )


def _find_waste_stream_column(df: pd.DataFrame) -> Optional[str]:
    """Find a likely Waste Stream column in EF or input data."""
    return _find_first_present_column(
        df,
        [
            "Waste Stream",
            "Waste stream",
            "Waste Category",
            "Waste Type",
            "Waste",
        ],
    )


def _find_vehicle_type_column(df: pd.DataFrame) -> Optional[str]:
    """Find a likely Vehicle Type column in EF data."""
    return _find_first_present_column(
        df,
        [
            "Vehicle Type",
            "Vehicle type",
            "Vehicle",
            "Transport Type",
            "Mode",
            "Name",
            "Description",
        ],
    )

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


def _get_vehicle_type_from_row(row: pd.Series) -> Optional[str]:
    vt = _get_first_present(row, [
        "Vehicle Type",
        "Vehicle type",
        "vehicle type",
        # common variants seen in S3C8 source data
        "Fuel Type",
        "fuel type",
        "Miles Fuel Type",
        "miles Fuel Type",
        "miles fuel type",
    ])
    if vt is None:
        return None
    text = str(vt).strip()
    if text == "":
        return None
    return text


def _find_power_consumption_in_row(row: pd.Series) -> Optional[str]:
    """Extract a power consumption indicator from row if available (as text)."""
    val = _get_first_present(row, [
        "Power Consumption",
        "Power",
        "Watt",
        "kW",
        "kWh",
    ])
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _find_ef_name_column(df: pd.DataFrame) -> Optional[str]:
    """Find the column that stores Emission Factor names for fuzzy matching."""
    return _find_first_present_column(
        df,
        [
            "ef_name",
            "Emission Factor Name",
            "EF Name",
            "Name",
        ],
    )


def _string_similarity(a: Any, b: Any) -> float:
    if a is None or b is None:
        return 0.0
    sa = _prepare_text_for_match(a)
    sb = _prepare_text_for_match(b)
    if not sa or not sb:
        return 0.0
    return difflib.SequenceMatcher(None, sa, sb).ratio()


def map_emission_factor(
    row: pd.Series,
    ef_data_dict: Dict[str, pd.DataFrame],
) -> Dict[str, Any]:
    """Find the best matching EF record for a spend row.

    Logic:
        1) Resolve EF sheet(s) by spend sheet name via get_ef_sheet.
        2) If mapping is "EMPTY YET" -> {"status": "EF not available"}
           If mapping is "NO NEED"  -> {"status": "Skipped"}
        3) Within the EF sheet(s):
           - Prefer country match, fallback to Global
           - If Purchased Goods or Services: also use keywords from row description

    Returns on match:
        {
          "EF ID": ..., "EF Name": ..., "EF Unit": ..., "EF Value": ...,
          "Source": ..., "Match Method": ...
        }

    Returns on special cases:
        {"status": "EF not available"} or {"status": "Skipped"}

    Returns if no match found:
        {"status": "No match"}
    """
    # Prefer the explicit 'Sheet' column, but fall back to the regrouping provenance column
    # so post-merge/regrouped rows still map correctly.
    spend_sheet = _get_first_present(
        row,
        [
            "Sheet",
            "sheet",
            "Spend Sheet",
            "Spend_Sheet",
            "Sheet_booklets",
            "Sheet booklets",
            "Sheet Booklets",
        ],
    )  # type: ignore[index]
    mapping = get_ef_sheet(spend_sheet if spend_sheet is not None else None)

    # Early anomaly rule for specific sheets:
    # If Fuel consumption value is 0 or 1 → mark status and skip mapping (EF Value = 0)
    spend_sheet_low_early = str(spend_sheet).strip().lower() if spend_sheet is not None else ""
    if spend_sheet_low_early in {
        "scope 1 fuel usage activity",
        "scope 3 cat 8 fuel usage activi",
        "scope 3 cat 8 fuel usage activit",
    }:
        fuel_cons_val = _get_first_present(
            row,
            [
                "Fuel consumption",
                "Fuel Consumption",
                "fuel consumption",
            ],
        )
        try:
            v = float(str(fuel_cons_val).strip()) if fuel_cons_val is not None and str(fuel_cons_val).strip() != "" else None
        except Exception:
            v = None
        if v in {0.0, 1.0}:
            return {
                "EF ID": None,
                "EF Name": None,
                "EF Unit": None,
                "EF Value": 0.0,
                "Source": None,
                "Match Method": None,
                "status": "Anomaly Detected",
            }

    if isinstance(mapping, str) and mapping.strip().upper() == "EMPTY YET":
        return {"status": "EF not available"}
    if isinstance(mapping, str) and mapping.strip().upper() == "NO NEED":
        return {"status": "Skipped"}

    # Normalize mapping to a list of candidate sheet names
    if mapping is None:
        ef_sheet_names: List[str] = []
    elif isinstance(mapping, str):
        ef_sheet_names = [mapping]
    else:
        ef_sheet_names = list(mapping)

    # Build a case-insensitive lookup for available EF sheet names
    ef_lookup: Dict[str, str] = {str(k).lower(): str(k) for k in ef_data_dict.keys()}

    # Scope 1 Fuel Usage Activity: use distance-based EF ONLY when Distance travelled is numeric
    spend_sheet_low_guard = str(_get_first_present(row, ["Sheet", "sheet", "Spend Sheet", "Spend_Sheet"])) .strip().lower() if _get_first_present(row, ["Sheet", "sheet", "Spend Sheet", "Spend_Sheet"]) is not None else ""
    if spend_sheet_low_guard == "scope 1 fuel usage activity" and ef_sheet_names:
        # Try to detect distance value
        dist_raw = _get_first_present(
            row,
            [
                "Distance travelled",
                "Distance Travelled",
                "Distance",
                "km travelled",
                "km",
            ],
        )
        def _is_numeric_distance(v: Any) -> bool:
            if v is None:
                return False
            s = str(v).strip()
            if s == "":
                return False
            # tolerant parse: replace comma with dot
            try:
                s2 = s.replace("\u00A0", " ").replace(" ", "").replace(",", ".")
                val = float(s2)
                return not pd.isna(val)
            except Exception:
                return False
        has_distance = _is_numeric_distance(dist_raw)
        if has_distance:
            # Ensure distance sheet is first in priority
            def _is_distance_sheet(name: str) -> bool:
                return str(name).strip().lower() == "scope 1 fuel distance".lower()
            ef_sheet_names = sorted(list(ef_sheet_names), key=lambda n: (0 if _is_distance_sheet(n) else 1, str(n)))
        else:
            # Remove distance EF candidates so we fall back to fuel-activity EFs
            ef_sheet_names = [n for n in ef_sheet_names if str(n).strip().lower() != "scope 1 fuel distance".lower()]

    # Scope 1 Fuel Usage Spend: when Currency indicates liters, prefer EF sheet 'Scope 1 Fuel Activity'
    if spend_sheet_low_guard == "scope 1 fuel usage spend" and ef_sheet_names:
        currency_val = _get_first_present(row, [
            "currency", "Currency"
        ])
        cur = str(currency_val).strip().lower() if currency_val is not None else ""
        is_liters = cur in {"liter", "liters", "litre", "litres"}
        if is_liters:
            def _is_fuel_activity(name: str) -> bool:
                return str(name).strip().lower() == "scope 1 fuel activity"
            # ensure Fuel Activity comes first; keep others as fallback
            ef_sheet_names = sorted(list(ef_sheet_names) + ["Scope 1 Fuel Activity"], key=lambda n: (0 if _is_fuel_activity(n) else 1, str(n)))

    # Determine if keyword-based matching should be applied
    spend_sheet_text = str(spend_sheet).lower() if spend_sheet is not None else ""
    use_keywords = any(
        term in spend_sheet_text for term in (
            "purchased goods",
            "services",
            "goods service",
        )
    )

    # Extract country and description-like fields from the spend row
    country_norm = _get_country_from_spend_row(row)

    description_text = _get_first_present(
        row,
        [
            "Product type",
            "Product Type",
            "Description",
            "Item Description",
            "Item",
            "Material",
            "Service",
            "Spend Description",
            "Name",
            "Billofquantity",
            "Bill of quantity",
        ],
    )
    description_text_norm = _prepare_text_for_match(description_text)
    keywords = _tokenize_keywords(description_text_norm) if use_keywords else []

    # Early handling: District Energy by country (avoid generic fallbacks)
    spend_sheet_low = str(spend_sheet).strip().lower() if spend_sheet is not None else ""

    # -----------------------------
    # Special handling: Scope 1 electric rows should use Scope 2 Electricity EF by country
    # -----------------------------
    if spend_sheet_low in {
        "scope 1 fuel usage spend",
        "scope 1 fuel usage activity",
        "scope 1 fuel activity",
        # These rows can be merged into Scope 1 sheets later, but still need electricity EF
        "scope 3 cat 8 fuel usage spend",
        "scope 3 cat 8 fuel activity",
        "scope 3 cat 8 fuel usage activit",
        "scope 3 cat 8 fuel usage activi",
    }:
        vt = _get_vehicle_type_from_row(row)
        vt_low = str(vt).strip().lower() if vt is not None else ""
        # catch: "Electric", "Electrical vehicle", "ELECTRICITY", etc.
        is_electric = ("electric" in vt_low) or ("electricity" in vt_low)
        if is_electric:
            # Extract country from spend row (do NOT rely on outer variable ordering)
            country_norm_el = _get_country_from_spend_row(row)
            if not country_norm_el:
                return {"status": "Scope1 electric: Country not provided"}

            # Resolve Scope 2 Electricity EF sheet
            df_el = None
            key = ef_lookup.get("scope 2 electricity")
            if key is not None:
                df_el = ef_data_dict.get(key)
            if df_el is None or df_el.empty:
                return {"status": "Scope1 electric: EF sheet 'Scope 2 Electricity' not found"}

            # Match by country (required)
            country_col = _find_country_column(df_el)
            if country_col is None:
                return {"status": "Scope1 electric: EF has no country column"}

            ef_country_norm_all = df_el[country_col].map(normalize_country).str.lower()
            subset = df_el[ef_country_norm_all == str(country_norm_el).lower()]
            if subset.empty:
                return {"status": f"Scope1 electric: Country not found ({country_norm_el})"}
            best_row = subset.iloc[0]

            # Prefer exact CTS EF columns (snake_case), fallback to common variants
            ef_id_col = _find_first_present_column(df_el, ["ef_id", "EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"])
            ef_name_col = _find_first_present_column(df_el, ["ef_name", "Emission Factor Name", "EF Name", "Name"])
            ef_unit_col = _find_first_present_column(df_el, ["ef_unit", "EF Unit", "Unit", "Units"])
            ef_value_col = _find_first_present_column(df_el, ["ef_value", "EF Value", "Value", "Emission", "Emission Factor Value"])
            source_col = _find_first_present_column(df_el, ["ef_source", "EF Source", "Source", "Reference", "Publication", "Provider"])

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            return {
                "EF ID": _safe_get(ef_id_col),
                "EF Name": _safe_get(ef_name_col),
                "EF Unit": _safe_get(ef_unit_col),
                "EF Value": ef_value,
                "Source": _safe_get(source_col),
                "Match Method": "Scope1 electric: Scope2 Electricity by country",
            }

    # Early handling for Cat 11 removed per user instruction; this sheet will be excluded from mapping
    # and handled by duplicating its precomputed CO2e in the calculator phase.

    # Early handling: Scope 3 Cat 6 Business Travel
    if spend_sheet_low == "scope 3 cat 6 business travel":
        # User rule: If 'Travel Type' has any value, force mapping to
        # EF sheet 'Scope 3 Category 6 Air' with EF Name 'Airplane' and value 0.0011718
        travel_type_val = _get_first_present(row, ["Travel Type", "travel type", "Travel type"])
        has_travel_type = travel_type_val is not None and str(travel_type_val).strip() != ""
        if has_travel_type:
            ef_air_key = None
            for k in ef_data_dict.keys():
                if str(k).strip().lower() == "scope 3 category 6 air".lower():
                    ef_air_key = k
                    break
            df_air = ef_data_dict.get(ef_air_key) if ef_air_key else None
            if df_air is None or df_air.empty:
                return {"status": "Cat6: Air EF sheet not found"}

            ef_name_col = _find_ef_name_column(df_air)
            ef_value_col = _find_first_present_column(df_air, ["EF Value", "Value", "Emission", "Emission Factor Value"])
            ef_id_col = _find_first_present_column(df_air, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"])
            ef_unit_col = _find_first_present_column(df_air, ["Unit", "Units"])
            source_col = _find_first_present_column(df_air, ["Source", "Reference", "Publication", "Provider"])

            best_row = None
            # Prefer EF Name == 'Airplane'
            if ef_name_col is not None and ef_name_col in df_air.columns:
                try:
                    mask_name = df_air[ef_name_col].astype(str).str.strip().str.lower() == "airplane"
                    hits = df_air[mask_name]
                    if not hits.empty:
                        best_row = hits.iloc[0]
                except Exception:
                    best_row = None
            # Fallback by EF Value exact 0.0011718
            if best_row is None and ef_value_col is not None and ef_value_col in df_air.columns:
                try:
                    vals = pd.to_numeric(df_air[ef_value_col], errors="coerce")
                    mask_val = vals == 0.0011718
                    hits2 = df_air[mask_val]
                    if not hits2.empty:
                        best_row = hits2.iloc[0]
                except Exception:
                    best_row = None
            # Final fallback: first row
            if best_row is None:
                best_row = df_air.iloc[0]

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            name_out = _safe_get(ef_name_col)
            return {
                "EF ID": _safe_get(ef_id_col),
                "EF Name": name_out,
                "ef_name": name_out,
                "EF Unit": _safe_get(ef_unit_col),
                "EF Value": ef_value if ef_value is not None else 0.0011718,
                "Source": _safe_get(source_col),
                "Match Method": "Cat6: Travel Type → fixed Airplane EF",
            }

        # Default Cat6 behavior: by Mode of Transport → match EF Name
        # Resolve EF sheet (mapping points to 'Scope 3 Cat 6 Business Travel')
        target_df: Optional[pd.DataFrame] = None
        for cand in ef_sheet_names:
            if cand is None:
                continue
            key = ef_lookup.get(str(cand).lower())
            if key:
                target_df = ef_data_dict[key]
                break
        if target_df is None or target_df.empty:
            return {"status": "Cat6: EF sheet not found"}

        # Extract mode from spend row (specifically 'Mode of Transport')
        mode_raw = _get_first_present(
            row,
            [
                "Mode of Transport",
            ],
        )
        if mode_raw is None or str(mode_raw).strip() == "":
            return {"status": "Cat6: Missing Mode of Transport"}

        # Match against EF Name column exactly (case-insensitive)
        ef_name_col_bt = _find_ef_name_column(target_df)
        if ef_name_col_bt is None:
            return {"status": "Cat6: EF sheet missing EF Name"}

        mask_bt = (
            target_df[ef_name_col_bt]
            .astype(str)
            .str.strip()
            .str.lower()
            == str(mode_raw).strip().lower()
        )
        hits_bt = target_df[mask_bt]
        if hits_bt.empty:
            return {"status": "Cat6: No EF Name match"}

        best_row_bt = hits_bt.iloc[0]

        ef_id_col_bt = _find_first_present_column(target_df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
        ef_unit_col_bt = _find_first_present_column(target_df, ["Unit", "Units"]) 
        ef_value_col_bt = _find_first_present_column(target_df, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
        source_col_bt = _find_first_present_column(target_df, ["Source", "Reference", "Publication", "Provider"]) 

        def _safe_get_bt(col: Optional[str]) -> Any:
            return best_row_bt[col] if col and col in best_row_bt else None

        ef_value_bt = _safe_get_bt(ef_value_col_bt)
        try:
            ef_value_bt = float(ef_value_bt) if ef_value_bt is not None and ef_value_bt != "" else None
        except Exception:
            pass

        name_out_bt = _safe_get_bt(ef_name_col_bt)
        return {
            "EF ID": _safe_get_bt(ef_id_col_bt),
            "EF Name": name_out_bt,
            "ef_name": name_out_bt,
            "EF Unit": _safe_get_bt(ef_unit_col_bt),
            "EF Value": ef_value_bt,
            "Source": _safe_get_bt(source_col_bt),
            "Match Method": "Cat6: mode match by EF Name",
        }

    # Early handling: Scope 3 Cat 7 Employee Commute by Mode of Transport → ef_description
    if spend_sheet_low == "scope 3 cat 7 employee commute":
        # Resolve EF sheet (mapping points to 'Scope 3 Cat 7 Employee Commutin')
        target_df: Optional[pd.DataFrame] = None
        for cand in ef_sheet_names:
            if cand is None:
                continue
            key = ef_lookup.get(str(cand).lower())
            if key:
                target_df = ef_data_dict[key]
                break
        if target_df is None or target_df.empty:
            return {"status": "Cat7: EF sheet not found"}

        # Extract mode from spend row
        mode_raw = _get_first_present(
            row,
            [
                "Mode of Transport",
                "Mode",
                "Transport Mode",
                "Commuting Mode",
            ],
        )
        if mode_raw is None or str(mode_raw).strip() == "":
            return {"status": "Cat7: Missing Mode of Transport"}

        # Normalize with alias rules
        def norm_mode(val: Any) -> str:
            s = str(val).strip().lower()
            # If contains separators ; or , and not in explicit list, treat as Mixed
            if (";" in s or "," in s) and not any(
                token in s for token in [
                    "walk/bicycle",
                    "train or bus",
                    "metro and walk",
                    "car, train, walk",
                ]
            ):
                return "mixed"
            aliases = {
                "car (petrol)": "petrol",
                "car (electric)": "electric",
                "car (hybrid)": "hybrid",
                "electric car": "electric",
                "walk": "walking",
                "bicycle": "bike",
                "walk/bicycle": "walking",
                "walk/bicycle ect": "walking",
                "train or bus": "bus",
                "metro and walk": "metro",
                "car, train, walk": "mixed",
                "metro;bus;walk": "bus",
                "metro;train;bus": "bus",
                "car;train;metro;walk": "mixed",
                "car;walk;metro": "mixed",
                "car;mixed;walk": "mixed",
                "moto": "motorbike",
                "taxi": "car",
                "luas": "tram",
            }
            return aliases.get(s, s)

        mode_val = norm_mode(mode_raw)

        # Find ef_description column in EF sheet
        ef_desc_col = _find_first_present_column(
            target_df,
            ["ef_description", "EF Description", "Description", "Name"],
        )
        if ef_desc_col is None:
            return {"status": "Cat7: EF sheet missing ef_description"}

        mask = (
            target_df[ef_desc_col]
            .astype(str)
            .str.strip()
            .str.lower()
            == str(mode_val).strip().lower()
        )
        hits = target_df[mask]
        if hits.empty:
            return {"status": "Cat7: No mode match"}

        best_row = hits.iloc[0]

        ef_id_col = _find_first_present_column(target_df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
        ef_name_col = _find_ef_name_column(target_df)
        ef_unit_col = _find_first_present_column(target_df, ["Unit", "Units"]) 
        ef_value_col = _find_first_present_column(target_df, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
        source_col = _find_first_present_column(target_df, ["Source", "Reference", "Publication", "Provider"]) 

        def _safe_get(col: Optional[str]) -> Any:
            return best_row[col] if col and col in best_row else None

        ef_value = _safe_get(ef_value_col)
        try:
            ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
        except Exception:
            pass

        return {
            "EF ID": _safe_get(ef_id_col),
            "EF Name": _safe_get(ef_name_col),
            "EF Unit": _safe_get(ef_unit_col),
            "EF Value": ef_value,
            "Source": _safe_get(source_col),
            "Match Method": "Cat7: mode match",
        }

    # Early handling: Scope 3 Cat 2 Capital Goods (single EF with value 0)
    if spend_sheet_low in {"scope 3 cat 2 capital goods act", "scope 3 cat 2 capital goods spe"}:
        ef_source_key = None
        for k in ef_data_dict.keys():
            if str(k).strip().lower() == "scope 3 category 2 capital good".lower():
                ef_source_key = k
                break
        df_cap = ef_data_dict.get(ef_source_key) if ef_source_key else None
        if df_cap is None or df_cap.empty:
            return {"status": "Cat2: EF sheet not found"}

        ef_id_col = _find_first_present_column(df_cap, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
        ef_name_col = _find_ef_name_column(df_cap)
        ef_unit_col = _find_first_present_column(df_cap, ["Unit", "Units"]) 
        ef_value_col = _find_first_present_column(df_cap, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
        source_col = _find_first_present_column(df_cap, ["Source", "Reference", "Publication", "Provider"]) 

        # Use first row; per requirement factor is single and equals 0
        best_row = df_cap.iloc[0]

        def _safe_get_cap(col: Optional[str]) -> Any:
            return best_row[col] if col and col in best_row else None

        # Force EF name/value if missing
        name_out = _safe_get_cap(ef_name_col) if ef_name_col else "Capital goods"
        value_out = 0.0
        try:
            v = _safe_get_cap(ef_value_col)
            if v is not None and str(v).strip() != "":
                value_out = float(v)
        except Exception:
            value_out = 0.0

        return {
            "EF ID": _safe_get_cap(ef_id_col),
            "EF Name": name_out if name_out else "Capital goods",
            "EF Unit": _safe_get_cap(ef_unit_col),
            "EF Value": value_out,
            "Source": _safe_get_cap(source_col),
            "Match Method": "Cat2: fixed factor",
        }

    # Early handling: Scope 3 Cat 12 End of Life → fixed EF 305A019 from Cat 5 Waste
    if spend_sheet_low == "scope 3 cat 12 end of life":
        # Try to take EF from 'Scope 3 Category 5 Waste' sheet
        ef_source_key = None
        for k in ef_data_dict.keys():
            if str(k).strip().lower() == "scope 3 category 5 waste".lower():
                ef_source_key = k
                break
        df_waste = ef_data_dict.get(ef_source_key) if ef_source_key else None

        target_id_norm = "305a019"

        def _extract_from_row(df_any: pd.DataFrame, row: pd.Series) -> Dict[str, Any]:
            ef_id_col = _find_first_present_column(df_any, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
            ef_name_col = _find_ef_name_column(df_any)
            ef_unit_col = _find_first_present_column(df_any, ["EF Unit", "Unit", "Units"]) 
            ef_value_col = _find_first_present_column(df_any, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
            source_col = _find_first_present_column(df_any, ["Source", "Reference", "Publication", "Provider"]) 

            def _safe_get(col: Optional[str]) -> Any:
                return row[col] if col and col in row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass
            return {
                "EF ID": _safe_get(ef_id_col) or "305A019",
                "EF Name": _safe_get(ef_name_col) or "Pure magnetic metal — Construction waste",
                "EF Unit": _safe_get(ef_unit_col) or "t CO2e/t",
                "EF Value": ef_value if ef_value is not None else 0.00098485,
                "Source": _safe_get(source_col) or "DEFRA (2024)",
                "Match Method": "Cat12: fixed EF 305A019",
            }

        if df_waste is not None and not df_waste.empty:
            id_col = _find_first_present_column(df_waste, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
            if id_col is not None and id_col in df_waste.columns:
                try:
                    ids_norm = df_waste[id_col].astype(str).str.strip().str.lower()
                    mask = ids_norm == target_id_norm
                    hits = df_waste[mask]
                    if not hits.empty:
                        best_row = hits.iloc[0]
                        return _extract_from_row(df_waste, best_row)
                except Exception:
                    pass
            # Fallback by EF Value exact match
            val_col = _find_first_present_column(df_waste, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
            if val_col is not None and val_col in df_waste.columns:
                try:
                    vals = pd.to_numeric(df_waste[val_col], errors="coerce")
                    mask2 = vals == 0.00098485
                    hits2 = df_waste[mask2]
                    if not hits2.empty:
                        best_row2 = hits2.iloc[0]
                        return _extract_from_row(df_waste, best_row2)
                except Exception:
                    pass

        # Ultimate fallback: hard-coded constants
        return {
            "EF ID": "305A019",
            "EF Name": "Pure magnetic metal — Construction waste",
            "EF Unit": "t CO2e/t",
            "EF Value": 0.00098485,
            "Source": "DEFRA (2024)",
            "Match Method": "Cat12: fixed EF 305A019",
        }



 #Do forecasting with this rules
 #1 if we buy or open new 10 companies what's the impact on the carbon footprint?
 #2 if we close 10 companies what's the impact on the carbon footprint?
 #3 different options for the same company
 #4 Spend based model 
 #5 Option A and Option B compare it
 # Use the data from LCA database to compare the impact of the different options
 #Concreate and steel industry
 #Steel and Concreate industry
 

    # Early handling: Cat 1 sheets with consolidated 'All together' EF data
    # Input key columns per sheet (exact):
    # - Goods Spend: Product type
    # - Goods Activity: Product type
    # - Common Purchases: Product Type
    # - Services Spend: Service Provided
    # - Services Activity: Service Provided
    # - Goods Services: Product type
    if spend_sheet_low in {
        "scope 3 cat 1 goods spend",
        "scope 3 cat 1 goods activity",
        "scope 3 cat 1 common purchases",
        "scope 3 cat 1 services spend",
        "scope 3 cat 1 services activity",
        "scope 3 cat 1 supplier summary",
        "scope 3 cat 1 goods services",
        # New non-Cat1 sheet following Cat1-Services logic
        "scope 3 services spend",
    }:
        # Only use the precise column per sheet definition; match to EF 'Product type'
        def _norm_alnum(v: Any) -> Optional[str]:
            if v is None:
                return None
            s = str(v).strip().lower()
            if s == "":
                return None
            return re.sub(r"[^a-z0-9]", "", s)

        def _pick_input_key(sheet_name_low: str) -> Optional[str]:
            # Special-case: Mecwide Nordics rows need Product description as key
            # because Product type is often too generic in their source workbook.
            src_val_any = _get_first_present(
                row,
                [
                    "Source_File",
                    "Source file",
                    "source_file",
                    "Source file name",
                    "Source",
                ],
            )
            src_low_any = str(src_val_any).strip().lower() if src_val_any is not None else ""
            is_mecwide_nordics = "mecwide nordics" in src_low_any
            if is_mecwide_nordics:
                desc_any = _get_first_present(
                    row,
                    [
                        "Product description",
                        "Product Description",
                        "Item Description",
                        "Description",
                    ],
                )
                if desc_any is not None and str(desc_any).strip() != "":
                    return desc_any

            if sheet_name_low in {"scope 3 cat 1 goods spend", "scope 3 cat 1 goods activity", "scope 3 cat 1 goods services"}:
                return _get_first_present(row, ["Product type", "Product Type"])  # type: ignore[return-value]
            if sheet_name_low == "scope 3 cat 1 common purchases":
                return _get_first_present(row, ["Product Type", "Product type"])  # type: ignore[return-value]
            if sheet_name_low == "scope 3 cat 1 services spend":
                # Special-case: for CTS Nordics.xlsx use 'Service Provider Function' if present
                src_val = _get_first_present(
                    row,
                    [
                        "Source_File",
                        "Source file",
                        "source_file",
                        "Source file name",
                        "Source",
                    ],
                )
                src_low = str(src_val).strip().lower() if src_val is not None else ""
                if src_low == "cts nordics.xlsx":
                    spf = _get_first_present(row, ["Service Provider Function", "Service provider function"])  # type: ignore[return-value]
                    if spf is not None and str(spf).strip() != "":
                        return spf
                # Default behavior
                return _get_first_present(row, ["Service Provided"])  # type: ignore[return-value]
            if sheet_name_low == "scope 3 cat 1 services activity":
                return _get_first_present(row, ["Service Provided"])  # type: ignore[return-value]
            # Supplier Summary is not specified; fall back to Product type
            if sheet_name_low == "scope 3 cat 1 supplier summary":
                return _get_first_present(row, ["Product type", "Product Type"])  # type: ignore[return-value]
            # New: Scope 3 Services Spend → use Service Provider Function exclusively
            if sheet_name_low == "scope 3 services spend":
                return _get_first_present(row, ["Service Provider Function", "Service provider function"])  # type: ignore[return-value]
            return None

        key_raw = _pick_input_key(spend_sheet_low)
        key_norm = _norm_alnum(key_raw)
        if not key_norm:
            return {"status": "Cat1: Missing key"}

        preferred_sheets = [
            "All together",
            "All together 2",
        ]
        # Search only in preferred sheets if available among the mapping candidates
        for cand in ef_sheet_names:
            if cand is None:
                continue
            if str(cand).strip().lower() not in {s.lower() for s in preferred_sheets}:
                continue
            key = ef_lookup.get(str(cand).lower())
            if not key:
                continue
            df_all = ef_data_dict.get(key)
            if df_all is None or df_all.empty:
                continue

            # Prepare normalized columns for Product type and BoQ and Codes if present
            def _pick(df: pd.DataFrame, cands: list[str]) -> Optional[str]:
                return _find_first_present_column(df, cands)

            col_prod = _pick(df_all, ["Product type", "Product Type"])  # type: ignore[list-item]

            def _norm_series(df: pd.DataFrame, col: Optional[str]) -> Optional[pd.Series]:
                if col is None or col not in df:
                    return None
                s = df[col].astype(str).str.strip().str.lower()
                s = s.str.replace(r"[^a-z0-9]", "", regex=True)
                return s

            s_prod = _norm_series(df_all, col_prod)

            hit_idx = None
            match_note = None
            if s_prod is not None:
                # 1) Exact match
                m = (s_prod == key_norm)
                if m.any():
                    hit_idx = m.idxmax()
                    match_note = "Cat1: All together exact"
                else:
                    # 2) Contains (both directions); prefer longest candidate
                    # NOTE: This step can produce false positives. It is disabled by default.
                    # Enable when needed via: ENABLE_CAT1_ALL_TOGETHER_CONTAINS=1
                    if _env_flag("ENABLE_CAT1_ALL_TOGETHER_CONTAINS", default=False):
                        contains_mask = s_prod.str.contains(re.escape(key_norm), na=False) | s_prod.map(
                            lambda v: (key_norm.find(v) >= 0) if isinstance(v, str) and len(v) > 0 else False
                        )
                        if contains_mask.any():
                            # pick the longest product string among hits
                            hit_idx = s_prod[contains_mask].map(lambda v: len(str(v))).idxmax()
                            match_note = "Cat1: All together contains"

                    # 3) Fuzzy with high threshold (runs even if 'contains' is disabled)
                    if hit_idx is None:
                        try:
                            sims = s_prod.map(lambda v: difflib.SequenceMatcher(None, str(v), key_norm).ratio())
                            max_score = float(sims.max()) if len(sims) else 0.0
                            if max_score >= 0.92:
                                hit_idx = sims.idxmax()
                                match_note = f"Cat1: All together fuzzy:{max_score:.2f}"
                        except Exception:
                            pass

            if hit_idx is None:
                continue

            best_row = df_all.loc[hit_idx]

            ef_id_col = _find_first_present_column(df_all, ["ef_id", "EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
            ef_name_col = _find_first_present_column(df_all, ["Emission Factor Category", "EF Category", "Category", "ef_name", "EF Name"]) 
            ef_unit_col = _find_first_present_column(df_all, ["EF Unit", "Unit", "Units"]) 
            ef_value_col = _find_first_present_column(df_all, ["ef_value", "EF Value", "Value", "Emission", "Emission Factor Value"]) 
            source_col = _find_first_present_column(df_all, ["Source", "Reference", "Publication", "Provider"]) 

            def _safe_get_cat1(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get_cat1(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            return {
                "EF ID": _safe_get_cat1(ef_id_col),
                "EF Name": _safe_get_cat1(ef_name_col),
                "EF Unit": _safe_get_cat1(ef_unit_col),
                "EF Value": ef_value,
                "Source": _safe_get_cat1(source_col),
                "Match Method": match_note or "Cat1: All together",
            }

    # Early handling: Scope 1 Fugitive Gases (single EF with fixed value 0)
    if spend_sheet_low == "scope 1 fugitive gases":
        # Resolve EF sheet explicitly named 'Scope 1 Fugitive Gas' in EF workbook
        ef_source_key = None
        for k in ef_data_dict.keys():
            if str(k).strip().lower() == "scope 1 fugitive gas".lower():
                ef_source_key = k
                break
        df_fug = ef_data_dict.get(ef_source_key) if ef_source_key else None
        if df_fug is None or df_fug.empty:
            # Fallback: try any mapped candidate sheets
            target_df: Optional[pd.DataFrame] = None
            for cand in ef_sheet_names:
                if cand is None:
                    continue
                key = ef_lookup.get(str(cand).lower())
                if key:
                    target_df = ef_data_dict.get(key)
                    if target_df is not None and not target_df.empty:
                        df_fug = target_df
                        break
        if df_fug is None or df_fug.empty:
            return {"status": "Fugitive: EF sheet not found"}

        best_row = df_fug.iloc[0]

        ef_id_col = _find_first_present_column(df_fug, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"])
        ef_name_col = _find_ef_name_column(df_fug)
        ef_unit_col = _find_first_present_column(df_fug, ["Unit", "Units"])
        source_col = _find_first_present_column(df_fug, ["Source", "Reference", "Publication", "Provider"])

        def _safe_get_fug(col: Optional[str]) -> Any:
            return best_row[col] if col and col in best_row else None

        # Force EF Value to 0.0 per requirement
        value_out = 0.0

        return {
            "EF ID": _safe_get_fug(ef_id_col),
            "EF Name": _safe_get_fug(ef_name_col) or "Fugitive gases",
            "EF Unit": _safe_get_fug(ef_unit_col),
            "EF Value": value_out,
            "Source": _safe_get_fug(source_col),
            "Match Method": "Fugitive: fixed factor",
        }
    # Early handling: Scope 3 Cat 15 Pensions (single fixed EF, no country logic)
    if spend_sheet_low == "scope 3 cat 15 pensions":
        # Resolve EF sheet referenced by mapping (expected single-row fixed factor)
        target_df: Optional[pd.DataFrame] = None
        for cand in ef_sheet_names:
            if cand is None:
                continue
            key = ef_lookup.get(str(cand).lower())
            if key:
                df_candidate = ef_data_dict.get(key)
                if df_candidate is not None and not df_candidate.empty:
                    target_df = df_candidate
                    break
        if target_df is None or target_df.empty:
            return {"status": "Cat15: EF sheet not found"}

        best_row = target_df.iloc[0]

        ef_id_col = _find_first_present_column(target_df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"])
        ef_name_col = _find_ef_name_column(target_df)
        ef_unit_col = _find_first_present_column(target_df, ["Unit", "Units"])
        ef_value_col = _find_first_present_column(target_df, ["EF Value", "Value", "Emission", "Emission Factor Value"])
        source_col = _find_first_present_column(target_df, ["Source", "Reference", "Publication", "Provider"])

        def _safe_get_cat15(col: Optional[str]) -> Any:
            return best_row[col] if col and col in best_row else None

        ef_value = _safe_get_cat15(ef_value_col)
        try:
            ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
        except Exception:
            pass

        return {
            "EF ID": _safe_get_cat15(ef_id_col),
            "EF Name": _safe_get_cat15(ef_name_col),
            "EF Unit": _safe_get_cat15(ef_unit_col),
            "EF Value": ef_value,
            "Source": _safe_get_cat15(source_col),
            "Match Method": "Cat15: single EF",
        }

    for candidate in ef_sheet_names:
        if candidate is None:
            continue
        df = None
        # Try direct key, then case-insensitive fallback
        if candidate in ef_data_dict:
            df = ef_data_dict[candidate]
        else:
            df_key = ef_lookup.get(str(candidate).lower())
            if df_key is not None:
                df = ef_data_dict[df_key]
        if df is None or df.empty:
            continue


        # Sheet-specific rules
        spend_sheet_low = str(spend_sheet).strip().lower() if spend_sheet is not None else ""

        # Scope 2 Electricity: choose EF by row Country (no Global fallback)
        if spend_sheet_low in {
            "scope 2 electricity",
            "scope 2 electricity average",
            "s3c8_electricity_extracted",
        }:
            country_col = _find_country_column(df)
            if country_col is None:
                # No country column in EF sheet; cannot map reliably
                return {"status": "Electricity: EF has no country column"}
            else:
                # Try exact country match (normalized), then Global
                ef_country_norm_all = df[country_col].map(normalize_country).str.lower()
                if not country_norm:
                    return {"status": "Electricity: Country not provided"}
                subset = df[ef_country_norm_all == str(country_norm).lower()]
                if subset.empty:
                    return {"status": f"Electricity: Country not found ({country_norm})"}
                best_row = subset.iloc[0]

            ef_id_col = _find_first_present_column(df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
            ef_name_col = _find_ef_name_column(df)
            ef_unit_col = _find_first_present_column(df, ["ef_unit", "EF Unit", "Unit", "Units"]) 
            ef_value_col = _find_first_present_column(df, ["ef_value", "EF Value", "Value", "Emission", "Emission Factor Value"]) 
            source_col = _find_first_present_column(df, ["ef_source", "EF Source", "Source", "Reference", "Publication", "Provider"]) 

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            method_note = "Country match" if country_norm else "Country not provided"
            return {
                "EF ID": _safe_get(ef_id_col),
                "EF Name": _safe_get(ef_name_col),
                "EF Unit": _safe_get(ef_unit_col),
                "EF Value": ef_value,
                "Source": _safe_get(source_col),
                "Match Method": f"Electricity: {method_note}",
            }

        # Scope 3 Cat 8 District Heating/Cooling: choose EF by row Country (no Global fallback)
        if spend_sheet_low in {
            "scope 3 cat 8 district e",
            "scope 3 cat 8 district h",
        }:
            country_col = _find_country_column(df)
            if country_col is None:
                return {"status": "District Energy: EF has no country column"}
            if not country_norm:
                return {"status": "District Energy: Country not provided"}

            ef_country_norm_all = df[country_col].map(normalize_country).str.lower()
            subset = df[ef_country_norm_all == str(country_norm).lower()]
            if subset.empty:
                return {"status": f"District Energy: Country not found ({country_norm})"}
            best_row = subset.iloc[0]

            ef_id_col = _find_first_present_column(df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
            ef_name_col = _find_ef_name_column(df)
            ef_unit_col = _find_first_present_column(df, ["ef_unit", "EF Unit", "Unit", "Units"]) 
            ef_value_col = _find_first_present_column(df, ["ef_value", "EF Value", "Value", "Emission", "Emission Factor Value"]) 
            source_col = _find_first_present_column(df, ["ef_source", "EF Source", "Source", "Reference", "Publication", "Provider"]) 

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            return {
                "EF ID": _safe_get(ef_id_col),
                "EF Name": _safe_get(ef_name_col),
                "EF Unit": _safe_get(ef_unit_col),
                "EF Value": ef_value,
                "Source": _safe_get(source_col),
                "Match Method": "District Energy: Country match",
            }

        # Vehicle-type driven sheets
        if spend_sheet_low in {
            "scope 1 fuel usage spend",
            "scope 1 fuel usage activity",
            "scope 1 fuel activity",
            "scope 3 cat 8 fuel usage spend",
            "scope 3 cat 8 fuel activity",
            "scope 3 cat 8 fuel usage activit",
            "scope 3 cat 8 fuel usage activi",
        }:
            vehicle_type_text = _get_vehicle_type_from_row(row)
            if vehicle_type_text is None:
                return {"status": "Missing Vehicle Type"}

            ef_name_col_generic = _find_ef_name_column(df)
            vt_col = _find_vehicle_type_column(df)
            id_col = _find_first_present_column(df, ["EF ID", "ID", "EFID"]) 
            unit_col = _find_first_present_column(df, ["EF Unit", "Unit", "Units"])
            value_col = _find_first_present_column(df, ["EF Value", "Emission Factor", "Value"]) or _find_first_present_column(df, ["Value"]) 
            source_col = _find_first_present_column(df, ["Source", "Reference"]) 

            df_hits = pd.DataFrame()
            if vt_col is not None:
                df_hits = df[df[vt_col].astype(str).str.strip().str.lower() == vehicle_type_text.strip().lower()]

            if df_hits.empty and ef_name_col_generic is not None:
                # Heuristic mapping for tokens like Diesel/Petrol and custom aliases
                token_low = vehicle_type_text.strip().lower()
                # custom alias normalization
                alias_to_petrol = (
                    "miles 95" in token_low
                    or "miles95" in token_low
                    or "milesplus 95" in token_low
                    or "milesplus95" in token_low
                    or "miles 98" in token_low
                    or "miles98" in token_low
                    or "milesplus 98" in token_low
                    or "milesplus98" in token_low
                    or "bildrift" in token_low
                    or "vask" in token_low
                    or "selvvask" in token_low
                    or "ad-blue" in token_low
                    or "ad blue" in token_low
                    or "adblue" in token_low
                    or "moraren" in token_low
                )
                if "miles diesel" in token_low or "diesel" in token_low or "machine" in token_low:
                    lookup_token = "diesel"
                elif "electrical vehicle" in token_low or "electric vehicle" in token_low or "electric" in token_low:
                    lookup_token = "electricity"
                elif alias_to_petrol or "premium" in token_low or "petrol" in token_low or "gasoline" in token_low or "hybrid" in token_low:
                    lookup_token = "petrol"
                elif "cng" in token_low:
                    lookup_token = "cng"
                else:
                    # fallback: first token
                    lookup_token = token_low.split("-")[0].split(" ")[0]
                mask = df[ef_name_col_generic].astype(str).str.strip().str.lower().str.contains(rf"\b{re.escape(lookup_token)}\b")
                df_hits = df[mask]

            if df_hits.empty:
                return {"status": "No EF match by Vehicle Type"}

            best_row = df_hits.iloc[0]

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            return {
                "EF ID": _safe_get(id_col),
                "EF Name": _safe_get(ef_name_col_generic if ef_name_col_generic else vt_col),
                "EF Unit": _safe_get(unit_col),
                "EF Value": ef_value,
                "Source": _safe_get(source_col),
                "Match Method": "Vehicle Type match",
            }



        # Waste-stream driven sheets
        if spend_sheet_low in {
            "scope 3 cat 5 waste",
            "scope 3 cat 5 office waste",
            "scope 3 cat 5 waste oslo",
        }:
            # Special case: exact EF match by EF Name using Waste Stream value (no fuzzy)
            if spend_sheet_low == "scope 3 cat 5 waste":
                ef_ws = _get_first_present(row, ["Waste Stream", "Waste stream", "Waste Type", "Waste"])  # type: ignore[index]
                if ef_ws is None:
                    return {"status": "Missing Waste Stream"}
                ws_norm = str(ef_ws).strip().lower()
                ef_name_col_exact = _find_ef_name_column(df)
                if ef_name_col_exact is None:
                    return {"status": "EF sheet missing EF Name"}
                mask_exact = df[ef_name_col_exact].astype(str).str.strip().str.lower() == ws_norm
                hits_exact = df[mask_exact]
                if hits_exact.empty:
                    return {"status": "No EF match by EF Name"}
                best_row = hits_exact.iloc[0]
                def _safe_get_ef(col: Optional[str]) -> Any:
                    return best_row[col] if col and col in best_row else None
                ef_id_col = _find_first_present_column(df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
                ef_unit_col = _find_first_present_column(df, ["Unit", "Units"]) 
                ef_value_col = _find_first_present_column(df, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
                source_col = _find_first_present_column(df, ["Source", "Reference", "Publication", "Provider"]) 
                ef_value = _safe_get_ef(ef_value_col)
                try:
                    ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
                except Exception:
                    pass
                name_out = _safe_get_ef(ef_name_col_exact)
                return {
                    "EF ID": _safe_get_ef(ef_id_col),
                    "EF Name": name_out,
                    "ef_name": name_out,
                    "EF Unit": _safe_get_ef(ef_unit_col),
                    "EF Value": ef_value,
                    "Source": _safe_get_ef(source_col),
                    "Match Method": "Waste EF Name exact",
                }
        
            # Prefer exact matches only (no fuzzy/no token overlap)
            waste_val = _get_first_present(row, [
                "Waste Stream", "Waste stream", "Waste Type", "Waste"
            ])
            if waste_val is None:
                return {"status": "Missing Waste Stream"}

            ws_norm = str(waste_val).strip().lower()
            ef_name_col = _find_ef_name_column(df)
            ef_desc_col = _find_first_present_column(
                df, ["ef_description", "EF Description", "Description", "Name"]
            )

            # Search order: exact by EF Name, exact by Description, contains (Description), fuzzy (Description)
            best_row = None
            match_method = None

            # 0) High-priority keyword rules to prefer specific landfill categories
            # Skip keyword rules for simple categories handled by EF Name exact matching
            if ef_name_col is not None and ws_norm and ws_norm not in {"organic", "glass", "general", "recycling"}:
                def _pick_by_ef_name_contains(needle: str) -> Optional[pd.Series]:
                    mask = df[ef_name_col].astype(str).str.lower().str.contains(rf"\b{re.escape(needle)}\b", na=False)
                    hits = df[mask]
                    return hits.iloc[0] if not hits.empty else None

                keyword_rules = [
                    ({"plasterboard", "gypsum"}, "landfill plasterboard"),
                    ({"paper board", "paperboard", "cardboard", "carton"}, "landfill paper board"),
                    ({"wood", "timber", "pallet", "pallets"}, "landfill wood"),
                    ({"metal", "steel", "aluminium", "aluminum", "iron", "copper"}, "landfill metal"),
                    ({"household", "residual"}, "landfill household"),
                    ({"organic", "food", "kitchen"}, "landfill organic"),
                    ({"construction", "demolition"}, "landfill construction"),
                    ({"electrical", "electric", "weee", "appliance", "electronics", "e-waste", "ewaste"}, "electric plastic"),
                    
                    
                ]

                for tokens_set, target_phrase in keyword_rules:
                    if any(tok in ws_norm for tok in tokens_set):
                        candidate = _pick_by_ef_name_contains(target_phrase)
                        if candidate is not None:
                            best_row = candidate
                            match_method = f"Waste Stream keyword rule → {target_phrase}"
                            break

            def _pick_first_exact(col_name: Optional[str], label: str) -> Optional[pd.Series]:
                if col_name is None:
                    return None
                mask = df[col_name].astype(str).str.strip().str.lower() == ws_norm
                hits = df[mask]
                if not hits.empty:
                    nonlocal match_method
                    match_method = f"Waste Stream exact by {label}"
                    return hits.iloc[0]
                return None

            # 1) Exact equality on EF Name (only for simple categories)
            if best_row is None:
                whitelist = {"organic", "glass", "general", "recycling"}
                if ws_norm in whitelist:
                    best_row = _pick_first_exact(ef_name_col, "EF Name")
            # 2) Exact equality on Description
            if best_row is None:
                best_row = _pick_first_exact(ef_desc_col, "Description")

            # No token-overlap / contains / fuzzy steps

            if best_row is None:
                return {"status": "No EF match by Waste Stream"}

            ef_id_col = _find_first_present_column(df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
            ef_name_out_col = _find_ef_name_column(df)
            ef_unit_col = _find_first_present_column(df, ["Unit", "Units"]) 
            ef_value_col = _find_first_present_column(df, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
            source_col = _find_first_present_column(df, ["Source", "Reference", "Publication", "Provider"]) 

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            name_out = _safe_get(ef_name_out_col)
            return {
                "EF ID": _safe_get(ef_id_col),
                "EF Name": name_out,
                "ef_name": name_out,
                "EF Unit": _safe_get(ef_unit_col),
                "EF Value": ef_value,
                "Source": _safe_get(source_col),
                "Match Method": match_method or "Waste Stream match",
            }

        # Products Indirect Use (power consumption)
        if spend_sheet_low == "scope 3 cat 11 products indirec":
            power_text = _find_power_consumption_in_row(row)
            country_col = _find_country_column(df)
            subset = df
            if country_col and country_norm:
                ef_country_norm_all = df[country_col].map(normalize_country).str.lower()
                subset = df[ef_country_norm_all == str(country_norm).lower()]
                if subset.empty:
                    subset = df[ef_country_norm_all == "global"]

            ef_name_col = _find_ef_name_column(subset if not subset.empty else df)
            search_df = subset if not subset.empty else df
            if power_text and ef_name_col is not None:
                mask = search_df[ef_name_col].astype(str).str.lower().str.contains(power_text.strip().lower())
                df_hits = search_df[mask]
                if not df_hits.empty:
                    best_row = df_hits.iloc[0]
                else:
                    best_row = search_df.iloc[0]
            else:
                if search_df.empty:
                    return {"status": "No match"}
                best_row = search_df.iloc[0]

            ef_id_col = _find_first_present_column(df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]) 
            ef_unit_col = _find_first_present_column(df, ["Unit", "Units"]) 
            ef_value_col = _find_first_present_column(df, ["EF Value", "Value", "Emission", "Emission Factor Value"]) 
            source_col = _find_first_present_column(df, ["Source", "Reference", "Publication", "Provider"]) 

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            return {
                "EF ID": _safe_get(ef_id_col),
                "EF Name": _safe_get(ef_name_col),
                "EF Unit": _safe_get(ef_unit_col),
                "EF Value": ef_value,
                "Source": _safe_get(source_col),
                "Match Method": "Power Consumption heuristic + Country/Global",
            }

        # Identify subset: by design country is not used unless explicitly required below
        df_subset = df
        subset_kind = ""

        # Special handling for Purchased Goods/Service EF sheets within the subset
        special_sheet = str(candidate).strip().lower() in {
            "scope 3 purchased goods spend".lower(),
            "scope 3 purchased service spend".lower(),
        }

        if special_sheet:
            # 1) Try Product Code / TFM Code exact match
            row_code = _get_first_present(
                row,
                [
                    "Product Code",
                    "TFM Code",
                    "TFM code",
                    "ProductCode",
                    "Product_Code",
                    "Code",
                ],
            )
            code_col = _find_code_column(df_subset)
            if row_code is not None and code_col is not None:
                row_code_norm = str(row_code).strip().lower()
                df_code = df_subset[
                    df[code_col].astype(str).str.strip().str.lower() == row_code_norm
                ]
                if not df_code.empty:
                    best_row = df_code.iloc[0]
                    if best_row is not None:
                        ef_id_col = _find_first_present_column(
                            df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]
                        )
                        ef_name_col = _find_ef_name_column(df)
                        ef_unit_col = _find_first_present_column(df, ["Unit", "Units"])
                        ef_value_col = _find_first_present_column(
                            df, ["EF Value", "Value", "Emission", "Emission Factor Value"]
                        )
                        source_col = _find_first_present_column(
                            df, ["Source", "Reference", "Publication", "Provider"]
                        )

                        def _safe_get(col: Optional[str]) -> Any:
                            return best_row[col] if col and col in best_row else None

                        ef_value = _safe_get(ef_value_col)
                        try:
                            ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
                        except Exception:
                            pass
                        return {
                            "EF ID": _safe_get(ef_id_col),
                            "EF Name": _safe_get(ef_name_col),
                            "EF Unit": _safe_get(ef_unit_col),
                            "EF Value": ef_value,
                            "Source": _safe_get(source_col),
                            "Match Method": f"{subset_kind} + Product Code exact ({code_col})",
                        }

            # No fuzzy or generic fallbacks for Cat 1 sheets

            # If special handling did not return, continue with generic logic below
        # Generic: choose first row (no country/global logic)
        best_row = None
        if best_row is not None:
            ef_id_col = _find_first_present_column(
                df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"]
            )
            ef_name_col = _find_ef_name_column(df)
            ef_unit_col = _find_first_present_column(df, ["Unit", "Units"])
            ef_value_col = _find_first_present_column(
                df, ["EF Value", "Value", "Emission", "Emission Factor Value"]
            )
            source_col = _find_first_present_column(
                df, ["Source", "Reference", "Publication", "Provider"]
            )

            def _safe_get(col: Optional[str]) -> Any:
                return best_row[col] if col and col in best_row else None

            ef_value = _safe_get(ef_value_col)
            try:
                ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
            except Exception:
                pass

            return {
                "EF ID": _safe_get(ef_id_col),
                "EF Name": _safe_get(ef_name_col),
                "EF Unit": _safe_get(ef_unit_col),
                "EF Value": ef_value,
                "Source": _safe_get(source_col),
                "Match Method": subset_kind,
            }

    # If we got here, nothing matched in any candidate sheet
    return {"status": "No match"}



# -------------------------------
# TFM-only mapping for Cat 1 rows
# -------------------------------

def map_emission_factor_tfm_only(
    row: pd.Series,
    ef_data_dict: Dict[str, pd.DataFrame],
) -> Dict[str, Any]:
    """Map EF using ONLY an exact TFM/Product code match for Cat 1 sheets.

    The function resolves EF sheet name(s) via get_ef_sheet based on the row's
    'Sheet' field, then performs a strict, case-insensitive equality match of
    the spend row code against the EF sheet code column. No fuzzy or fallback
    logic is applied.

    Returns the same structure as map_emission_factor on success; otherwise a
    status dict indicating the reason for failure.
    """
    spend_sheet = _get_first_present(row, ["Sheet", "sheet", "Spend Sheet", "Spend_Sheet"])  # type: ignore[index]
    mapping = get_ef_sheet(spend_sheet if spend_sheet is not None else None)

    if isinstance(mapping, str) and mapping.strip().upper() == "EMPTY YET":
        return {"status": "EF not available"}
    if isinstance(mapping, str) and mapping.strip().upper() == "NO NEED":
        return {"status": "Skipped"}

    if mapping is None:
        candidate_sheets: List[str] = []
    elif isinstance(mapping, str):
        candidate_sheets = [mapping]
    else:
        candidate_sheets = list(mapping)

    row_code = _get_first_present(
        row,
        [
            "Product Code",
            "TFM Code",
            "TFM code",
            "TFM",
            "ProductCode",
            "Product_Code",
            "Code",
        ],
    )
    if row_code is None or str(row_code).strip() == "":
        return {"status": "Missing TFM code"}
    row_code_norm = str(row_code).strip().lower()

    ef_lookup: Dict[str, str] = {str(k).lower(): str(k) for k in ef_data_dict.keys()}

    for candidate in candidate_sheets:
        if candidate is None:
            continue
        df = None
        if candidate in ef_data_dict:
            df = ef_data_dict[candidate]
        else:
            df_key = ef_lookup.get(str(candidate).lower())
            if df_key is not None:
                df = ef_data_dict[df_key]
        if df is None or df.empty:
            continue

        code_col = _find_code_column(df)
        if code_col is None:
            continue

        mask = df[code_col].astype(str).str.strip().str.lower() == row_code_norm
        hits = df[mask]
        if hits.empty:
            continue

        best_row = hits.iloc[0]

        ef_id_col = _find_first_present_column(df, ["EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"])
        ef_name_col = _find_ef_name_column(df)
        ef_unit_col = _find_first_present_column(df, ["Unit", "Units"])
        ef_value_col = _find_first_present_column(df, ["EF Value", "Value", "Emission", "Emission Factor Value"])
        source_col = _find_first_present_column(df, ["Source", "Reference", "Publication", "Provider"])

        def _safe_get(col: Optional[str]) -> Any:
            return best_row[col] if col and col in best_row else None

        ef_value = _safe_get(ef_value_col)
        try:
            ef_value = float(ef_value) if ef_value is not None and ef_value != "" else None
        except Exception:
            pass

        return {
            "EF ID": _safe_get(ef_id_col),
            "EF Name": _safe_get(ef_name_col),
            "EF Unit": _safe_get(ef_unit_col),
            "EF Value": ef_value,
            "Source": _safe_get(source_col),
            "Match Method": "TFM code exact",
        }

    return {"status": "No TFM code match"}
