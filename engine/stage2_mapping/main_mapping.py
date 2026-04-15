from __future__ import annotations

import math
import re
import sys
import time
from pathlib import Path
from datetime import datetime
import difflib
from typing import Dict, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_DIR, STAGE2_HEADCOUNT_CSV, STAGE2_INPUT_DIR, STAGE2_MANUAL_MAPPING_DIR, STAGE2_OUTPUT_DIR
from excel_writer_utils import preferred_excel_writer_engine

import mapping_utils as mu


# ----------------------------
# Configuration and constants
# ----------------------------

# Primary input workbook (will fallback to latest matching if not found).
# The file is resolved from the centralized DATA_DIR configuration.
INPUT_WORKBOOK_NAME = "normalized_emission_factor_mapping_translated.xlsx"

# Optional override workbook to manually provide EF mapping for specific sheets
OVERRIDE_WORKBOOK_NAME = "Correct mapping example.xlsx"
# Additional manual mapping workbook (BIMMS and CTS Denmark)
EXTRA_OVERRIDE_WORKBOOK_NAMES = [
    "Correct Mapping Example Bimms and CTS Denmark.xlsx",
]

# Sheet names that should use the override workbook when available
OVERRIDE_SHEET_NAMES = {
    "Scope 3 Cat 1 Services Spend",
    "Scope 3 Cat 1 Goods Services",
    "Scope 3 Cat 1 Goods Spend",
}

# Cat 7 preprocessor configuration
CAT7_NATIONAL_AVERAGES_ENABLED = True
CAT7_LEGACY_SURVEY_PREPROCESS_ENABLED = False
CAT7_HEADCOUNT_CSV = STAGE2_HEADCOUNT_CSV
CAT7_SHEET_NAME = "Scope 3 Cat 7 Employee Commute"
CAT7_NATIONAL_AVERAGES_XLSX = (
    PROJECT_ROOT
    / "engine"
    / "stage2_mapping"
    / "Employee Commuting National Averages"
    / "Employee_headcount_national_averages.xlsx"
)

# BillOfQuantity header mapping per input sheet (where to read BoQ from)
BOQ_INPUT_HEADERS_PER_SHEET: Dict[str, List[str]] = {
    # Goods
    "Scope 3 Cat 1 Goods Spend": ["Product type", "Product Type"],
    "Scope 3 Cat 1 Goods Activity": ["Product type", "Product Type"],
    "Scope 3 Cat 1 Common Purchases": ["Product Type", "Product type"],
    # Services
    "Scope 3 Cat 1 Services Spend": ["Service Provided"],
    "Scope 3 Cat 1 Services Activity": ["Service Provided"],
    # Goods Services: try specific column then fallback to Product type
    "Scope 3 Cat 1 Goods Services": ["Scope 3 Cat 1 Goods Services", "Product type", "Product Type"],
}

# Emission Factors workbook is loaded via mapping_utils.load_emission_factors
# from the same directory where this script resides.

# Sheets excluded from mapping
EXCLUDED_SHEETS = {
    "Scope 3 Cat 4+9 Transport Spend",
    "Scope 3 Cat 4+9 Transport Act",
    "Scope 3 Cat 1 Supplier Summary",
    "Scope 3 Cat 6 Business Travel S",
    "Scope 3 Cat 6 Business Travel A",
    "Scope 3 Cat 8 Electricity",
    "Scope 3 Cat 8 District Heating",
    "Scope 2 Electricity Average",
    "Calculation Methods",
    "Scope 3 Cat 5 Office Waste 2",
    "Water Tracker Averages",
    "Scope 3 Cat 5 Waste Oslo 2",
    "Scope 3 Cat 12 End of Life 2",
    "WT_extracted",
    "Scope 3 Cat 11 Products Indirec",
}

# Sheets to drop entirely from the output (do not write these sheets at all)
DROP_SHEETS = {
    "Scope 3 Cat 4+9 Transport Spend",
    "Scope 3 Cat 4+9 Transport Act",
    "Scope 3 Cat 1 Common purchases",
    "Scope 3 Cat 1 Common Purchases",
    "Scope3 Cat 1 Supplier Summary",
    "Scope 3 Cat 1 Supplier Summary",
    "Scope 3 Cat 1 Goods Activity",
    "Scope 3 Cat 5 Waste 2",
    "Scope 1 Fugitive Gases",
    "Scope 3 Cat 8 District Heating",
    "Calculation Methods",
    "Scope 3 Cat 5 Office Waste 2",
    "Scope 3 Cat 5  Waste Oslo 2",
    "Scope 3 Cat 5 Waste Oslo 2",
    "Scope 1 Gas Usage",
    "Scope 3 Cat 12 End of Life 2",
    "Scope 3 Cat 2 Capital Goods Spe",
    "Scope 3 Cat 2 Capital Goods Act",
    # Remove Cat 6 Business Travel sheets entirely
    "Scope 3 Cat 6 Business Travel S",
    "Scope 3 Cat 6 Business Travel A",
    "Scope 3 Waste with calculations",
}

def _canon_sheet_name(name: str) -> str:
    # Lowercase and collapse internal whitespace for robust matching
    return re.sub(r"\s+", " ", str(name).strip().lower())

DROP_SHEETS_NORMALIZED = {_canon_sheet_name(n) for n in DROP_SHEETS}


def _build_external_manual_lookups(base_dir: Path) -> Tuple[
    Dict[str, Dict[str, Optional[object]]],
    Dict[str, Dict[str, Optional[object]]],
    Dict[str, Dict[str, Optional[object]]],
]:
    """Load external manual EF mappings and build lookups by Product type and BillOfQuantity.

    Sources (if present) under 'Emission factors' directory:
      - mapped_emission_factors.xlsx
      - Marta_manual_mapping_with_category.xlsx

    Expected columns (case/space tolerant):
      - Product type / Product Type
      - BillOfQuantity / Bill Of Quantity / BoQ
      - Emission Factor Category (-> ef_name)
      - ef_value
      - ef_id
      - EF Unit / Unit / Units (optional -> ef_unit)
    """
    dir_path = STAGE2_MANUAL_MAPPING_DIR / "Emission factors"
    files = []
    try:
        if dir_path.exists():
            # Accept any Excel extension; filter by base name fragments (case-insensitive)
            for p in dir_path.glob("*.xls*"):
                name_low = p.name.lower()
                if ("mapped_emission_factors" in name_low) or ("marta_manual_mapping_with_category" in name_low):
                    files.append(p)
    except Exception:
        files = []

    # Keep a secondary repo-local fallback for older layouts during migration.
    explicit_paths = [
        base_dir / "Emission factors" / "mapped_emission_factors.xlsx",
        base_dir / "Emission factors" / "Marta_manual_mapping_with_category.xlsx",
    ]
    for p in explicit_paths:
        try:
            if p.exists() and p.suffix.lower().startswith('.xls'):
                if p not in files:
                    files.append(p)
        except Exception:
            pass

    def _norm_map(cols: List[str]) -> Dict[str, str]:
        return {re.sub(r"[^a-z0-9]", "", str(c).lower()): str(c) for c in cols}

    def _pick(norm_map: Dict[str, str], *cands: str) -> Optional[str]:
        for c in cands:
            k = re.sub(r"[^a-z0-9]", "", c.lower())
            if k in norm_map:
                return norm_map[k]
        return None

    def _norm_key(v: Optional[object]) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return re.sub(r"\s+", " ", s).lower()

    by_product: Dict[str, Dict[str, Optional[object]]] = {}
    by_boq: Dict[str, Dict[str, Optional[object]]] = {}
    by_code: Dict[str, Dict[str, Optional[object]]] = {}

    for path in files:
        if not path.exists():
            continue
        try:
            xls = pd.ExcelFile(path)
        except Exception:
            continue
        for sh in xls.sheet_names:
            try:
                df = pd.read_excel(xls, sheet_name=sh)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            nm = _norm_map(list(df.columns))
            col_prod = _pick(nm, "Product type", "Product Type")
            col_boq = _pick(nm, "BillOfQuantity", "Bill Of Quantity", "BoQ", "Bill of quantity")
            col_cat = _pick(nm, "Emission Factor Category", "EF Category", "Category", "ef_name", "EF Name")
            col_val = _pick(nm, "ef_value", "EF Value", "Value")
            col_id = _pick(nm, "ef_id", "EF ID", "id")
            col_unit = _pick(nm, "EF Unit", "Unit", "Units")
            col_code = _pick(nm, "TFM Code", "TFMCode", "Product Code", "Code")

            def norm_alnum(v: Optional[object]) -> Optional[str]:
                if v is None:
                    return None
                s = str(v).strip().lower()
                if s == "":
                    return None
                return re.sub(r"[^a-z0-9]", "", s)

            for _, r in df.iterrows():
                item = {
                    "ef_id": r.get(col_id) if col_id else None,
                    "ef_name": r.get(col_cat) if col_cat else None,
                    "ef_value": r.get(col_val) if col_val else None,
                    "ef_unit": r.get(col_unit) if col_unit else None,
                    "ef_source": str(path.name),
                }
                if col_prod:
                    k = norm_alnum(r.get(col_prod))
                    if k and k not in by_product:
                        by_product[k] = item
                if col_boq:
                    k2 = norm_alnum(r.get(col_boq))
                    if k2 and k2 not in by_boq:
                        by_boq[k2] = item
                if col_code:
                    k3 = norm_alnum(r.get(col_code))
                    if k3 and k3 not in by_code:
                        by_code[k3] = item

    return by_product, by_boq, by_code


def _find_spend_column(df: pd.DataFrame) -> Optional[str]:
    """Find a spend column (in EUR) with several likely aliases.

    Prefers exact 'Spend_Euro' but will fallback to common alternatives.
    """
    candidates: List[str] = [
        "Spend_Euro",
        "Spend Euro",
        "Spend (Euro)",
        "Spend_EUR",
        "Spend EUR",
        "Total Spend",
        "Amount EUR",
        "Amount_EUR",
        "EUR",
        "Spend",
    ]
    df_cols_norm = {re.sub(r"[^a-z0-9]", "", str(c).lower()): str(c) for c in df.columns}
    for cand in candidates:
        key = re.sub(r"[^a-z0-9]", "", cand.lower())
        if key in df_cols_norm:
            return df_cols_norm[key]
    return None


def _is_euro_based_unit(unit: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Check if EF unit is per-Euro based and infer mass unit ('kg' or 't').

    Returns (is_euro_based, mass_unit) where mass_unit is 'kg' or 't' when inferable.
    """
    if unit is None:
        return False, None
    u = str(unit).strip()
    if u == "":
        return False, None
    euro_based = bool(
        ("€/" in u)
        or re.search(r"(EUR|€)\s*/", u, flags=re.IGNORECASE)
        or re.search(r"per\s*(EUR|€)", u, flags=re.IGNORECASE)
    )
    if not euro_based:
        return False, None

    # Infer mass unit
    u_low = u.lower().replace("co2e", "")
    if "kg" in u_low:
        return True, "kg"
    if any(tok in u_low for tok in [" t/", " tonne", " ton/"]):
        return True, "t"
    if re.search(r"\bt\b", u_low):
        return True, "t"
    # Unknown mass unit but still €/based
    return True, None


def _compute_emissions_tco2e(spend_eur: Optional[float], ef_value: Optional[float], ef_unit: Optional[str]) -> Optional[float]:
    """Compute emissions in tCO2e when EF unit is Euro-based.

    If EF unit is kgCO2e/EUR -> tCO2e = spend * ef_value / 1000
    If EF unit is tCO2e/EUR  -> tCO2e = spend * ef_value
    Otherwise returns None.
    """
    if spend_eur is None or ef_value is None:
        return None
    try:
        spend_f = float(spend_eur)
        ef_f = float(ef_value)
    except Exception:
        return None

    is_euro_based, mass_unit = _is_euro_based_unit(ef_unit)
    if not is_euro_based:
        return None

    if mass_unit == "kg":
        return (spend_f * ef_f) / 1000.0
    # Default to tonnes if unknown but €/based
    return spend_f * ef_f


def _load_input_workbook(base_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load input workbook. If the configured file is missing, fallback to latest matching file."""
    search_roots = [STAGE2_INPUT_DIR, DATA_DIR, base_dir]
    # Resolve explicit absolute path or filename (with/without .xlsx)
    inp = str(INPUT_WORKBOOK_NAME)
    try:
        p = Path(inp)
        if p.is_absolute():
            target = p
        else:
            target = next((root / inp for root in search_roots if (root / inp).exists()), search_roots[0] / inp)
        if not target.exists() and target.suffix.lower() != ".xlsx":
            for root in search_roots:
                target_with_ext = root / f"{Path(inp).name}.xlsx"
                if target_with_ext.exists():
                    target = target_with_ext
                    break
        if not target.exists():
            pattern = "normalized_emission_factor_mapping_translated_*.xlsx"
            candidates: List[Path] = []
            for root in search_roots:
                candidates.extend(root.glob(pattern))
            candidates = sorted(candidates, key=lambda pp: pp.stat().st_mtime, reverse=True)
            if not candidates:
                raise FileNotFoundError(f"Input workbook not found: {target.name}")
            target = candidates[0]
    except Exception:
        pattern = "normalized_emission_factor_mapping_translated_*.xlsx"
        candidates: List[Path] = []
        for root in search_roots:
            candidates.extend(root.glob(pattern))
        candidates = sorted(candidates, key=lambda pp: pp.stat().st_mtime, reverse=True)
        if not candidates:
            raise FileNotFoundError("Input workbook not found.")
        target = candidates[0]

    sheets = pd.read_excel(target, sheet_name=None)
    # Normalize sheet names: trim spaces
    return {str(k).strip(): v for k, v in sheets.items()}


def _load_override_workbook(base_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load optional override mapping workbooks. Merge sheets from all present files."""
    merged: Dict[str, pd.DataFrame] = {}
    candidates = [STAGE2_MANUAL_MAPPING_DIR / OVERRIDE_WORKBOOK_NAME] + [
        STAGE2_MANUAL_MAPPING_DIR / name for name in EXTRA_OVERRIDE_WORKBOOK_NAMES
    ]
    candidates.extend([base_dir / OVERRIDE_WORKBOOK_NAME] + [base_dir / name for name in EXTRA_OVERRIDE_WORKBOOK_NAMES])
    for path in candidates:
        if not path.exists():
            continue
        try:
            sheets = pd.read_excel(path, sheet_name=None)
        except Exception:
            continue
        for k, v in sheets.items():
            merged[str(k).strip()] = v
    return merged


def _norm_key(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    return re.sub(r"\s+", " ", s).lower()


def _get_single_override_df(override_sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Return the single override sheet if present; otherwise an empty DataFrame."""
    if not override_sheets:
        return pd.DataFrame()
    # Return the first (and expected only) sheet
    for _name, _df in override_sheets.items():
        return _df if _df is not None else pd.DataFrame()
    return pd.DataFrame()


def _build_boq_lookup(df_override: pd.DataFrame) -> Dict[str, Dict[str, Optional[object]]]:
    """Create a lookup dict from BillOfQuantity -> mapping fields from override sheet.

    Expected columns (case/space tolerant):
      - BillOfQuantity (key)
      - Emission Factor Category -> ef_name
      - ef_value -> ef_value
      - TFM Code (optional)
      - EF Unit / Unit / Units (optional) -> ef_unit
      - Source/Reference (optional) -> ef_source
    """
    if df_override is None or df_override.empty:
        return {}

    # Normalize column names map
    norm_map = {re.sub(r"[^a-z0-9]", "", str(c).lower()): str(c) for c in df_override.columns}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            k = re.sub(r"[^a-z0-9]", "", c.lower())
            if k in norm_map:
                return norm_map[k]
        return None

    col_boq = pick("BillOfQuantity", "Bill Of Quantity", "BoQ", "Billofquantity")
    col_cat = pick("Emission Factor Category", "EF Category", "Category")
    col_val = pick("ef_value", "EF Value", "Value")
    col_unit = pick("EF Unit", "Unit", "Units")
    col_src = pick("Source", "Reference", "Publication", "Provider")
    col_tfm = pick("TFM Code", "Product Code", "Code")

    if col_boq is None:
        return {}

    lookup: Dict[str, Dict[str, Optional[object]]] = {}
    for _, r in df_override.iterrows():
        key = _norm_key(r.get(col_boq))
        if key is None:
            continue
        item: Dict[str, Optional[object]] = {
            "ef_name": r.get(col_cat) if col_cat else None,
            "ef_value": r.get(col_val) if col_val else None,
            "ef_unit": r.get(col_unit) if col_unit else None,
            "ef_source": r.get(col_src) if col_src else None,
            "tfm_code": r.get(col_tfm) if col_tfm else None,
        }
        # First occurrence wins
        if key not in lookup:
            lookup[key] = item
    return lookup


def _build_service_provided_lookup(df_bimms: pd.DataFrame) -> Dict[str, Dict[str, Optional[object]]]:
    """Create a lookup from Service Provided -> {ef_id, ef_name} for BIMMS sheet."""
    if df_bimms is None or df_bimms.empty:
        return {}
    norm_map = {re.sub(r"[^a-z0-9]", "", str(c).lower()): str(c) for c in df_bimms.columns}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            k = re.sub(r"[^a-z0-9]", "", c.lower())
            if k in norm_map:
                return norm_map[k]
        return None

    col_sp = pick("Service Provided", "Service", "Provided")
    col_name = pick("ef_name", "EF Name", "name")
    col_id = pick("ef_id", "EF ID", "id")
    if col_sp is None:
        return {}
    out: Dict[str, Dict[str, Optional[object]]] = {}
    for _, r in df_bimms.iterrows():
        key = _norm_key(r.get(col_sp))
        if key is None:
            continue
        item = {
            "ef_id": r.get(col_id) if col_id else None,
            "ef_name": r.get(col_name) if col_name else None,
        }
        if key not in out:
            out[key] = item
    return out


def _build_cts_denmark_lookups(df_cts: pd.DataFrame) -> Tuple[Dict[str, Dict[str, Optional[object]]], Dict[str, Dict[str, Optional[object]]]]:
    """Build lookups for CTS Denmark: by BillOfQuantity and by TFM Code."""
    if df_cts is None or df_cts.empty:
        return {}, {}
    norm_map = {re.sub(r"[^a-z0-9]", "", str(c).lower()): str(c) for c in df_cts.columns}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            k = re.sub(r"[^a-z0-9]", "", c.lower())
            if k in norm_map:
                return norm_map[k]
        return None

    col_boq = pick("BillOfQuantity", "Bill Of Quantity", "BoQ", "Bill of quantity")
    col_code = pick("TFM Code", "TFMCode", "Product Code / TFM Code", "Product Code", "Code")
    col_name = pick("ef_name", "EF Name", "name")
    col_id = pick("ef_id", "EF ID", "id")

    by_boq: Dict[str, Dict[str, Optional[object]]] = {}
    by_code: Dict[str, Dict[str, Optional[object]]] = {}
    for _, r in df_cts.iterrows():
        if col_boq:
            key_boq = _norm_key(r.get(col_boq))
            if key_boq:
                item = {
                    "ef_id": r.get(col_id) if col_id else None,
                    "ef_name": r.get(col_name) if col_name else None,
                }
                if key_boq not in by_boq:
                    by_boq[key_boq] = item
        if col_code:
            code_val = r.get(col_code)
            if code_val is not None and str(code_val).strip() != "":
                code_key = str(code_val).strip().lower()
                item = {
                    "ef_id": r.get(col_id) if col_id else None,
                    "ef_name": r.get(col_name) if col_name else None,
                }
                if code_key not in by_code:
                    by_code[code_key] = item
    return by_boq, by_code


def _extract_boq_from_row(row: pd.Series, header_candidates: List[str]) -> Optional[str]:
    for h in header_candidates:
        if h in row and pd.notna(row[h]):
            v = str(row[h]).strip()
            if v != "":
                return v
    return None


def _match_boq_key(key_norm: str, candidates: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """Pick best matching BoQ key among candidates using exact, contains, then fuzzy.

    Returns (matched_key, method_label)
    """
    if not key_norm:
        return None, None
    # Exact
    if key_norm in candidates:
        return key_norm, "boq_exact"
    # Contains (prefer longest candidate)
    contains_hits = [c for c in candidates if (key_norm in c) or (c in key_norm)]
    if contains_hits:
        best = max(contains_hits, key=len)
        return best, "boq_contains"
    # Fuzzy similarity
    best_key = None
    best_score = 0.0
    for c in candidates:
        score = difflib.SequenceMatcher(None, key_norm, c).ratio()
        if score > best_score:
            best_key = c
            best_score = score
    if best_key is not None and best_score >= 0.8:
        return best_key, f"boq_fuzzy:{best_score:.2f}"
    return None, None


def _build_results_from_any_columns(df_any: pd.DataFrame, out_cols: List[str]) -> pd.DataFrame:
    """Construct results DataFrame with required columns from a source that may use various headers.

    Accepts both Title Case (e.g., 'EF ID') and snake_case (e.g., 'ef_id').
    Unknown/missing columns are filled with None.
    """
    # Normalize column lookup map (alnum lower)
    norm_map = {re.sub(r"[^a-z0-9]", "", str(c).lower()): str(c) for c in df_any.columns}

    def pick(candidates: List[str]) -> Optional[str]:
        for cand in candidates:
            key = re.sub(r"[^a-z0-9]", "", cand.lower())
            if key in norm_map:
                return norm_map[key]
        return None

    candidate_names: Dict[str, List[str]] = {
        "ef_id": ["ef_id", "EF ID", "ef id", "id"],
        "ef_name": ["ef_name", "EF Name", "ef name", "name"],
        "ef_unit": ["ef_unit", "EF Unit", "ef unit", "unit"],
        "ef_value": ["ef_value", "EF Value", "ef value", "value"],
        "ef_source": ["ef_source", "Source", "source", "ef source"],
        "match_method": ["match_method", "Match Method", "match method"],
        "status": ["status", "Status"],
    }

    out: Dict[str, List[Optional[object]]] = {c: [] for c in out_cols}
    col_map: Dict[str, Optional[str]] = {k: pick(v) for k, v in candidate_names.items()}

    for _, row in df_any.iterrows():
        for c in out_cols:
            src_col = col_map.get(c)
            out[c].append(row[src_col] if src_col in row else None)
    return pd.DataFrame(out)


def _preprocess_cat7_proportional(
    in_sheets: Dict[str, pd.DataFrame],
    headcount_csv: Path,
) -> Dict[str, pd.DataFrame]:
    """Generate proportional 12-month Cat 7 dataset per headcount before mapping.

    - Drops rows with empty Mode of Transport.
    - Computes mode ratios from the most common month per company.
    - Synthesizes records up to expected headcount and duplicates for 12 months.
    - Does NOT copy ef_id/ef_name/ef_value; mapping will populate later.
    """
    try:
        if CAT7_SHEET_NAME not in in_sheets:
            return in_sheets
        df = in_sheets[CAT7_SHEET_NAME]
        if df is None or df.empty:
            return in_sheets

        df_proc = df.copy()

        # Drop rows with empty Mode of Transport
        mode_col = mu._find_first_present_column(
            df_proc,
            [
                "Mode of Transport",
                "Mode of transport",
                "Transport Mode",
                "Transport mode",
                "Mode",
                "mode",
            ],
        )
        if mode_col is None:
            return in_sheets
        vals = df_proc[mode_col]
        df_proc = df_proc[vals.notna() & vals.astype(str).str.strip().ne("")].reset_index(drop=True)

        # Parse reporting month if present
        if "Reporting period (month, year)" in df_proc.columns:
            rep = pd.to_datetime(df_proc["Reporting period (month, year)"], errors="coerce").dt.to_period("M").astype(str)
            df_proc = df_proc.assign(Reporting_Month=rep)

        # Helper to parse km (handles ranges like "1–5 km")
        def _parse_km(value):
            if isinstance(value, (int, float)):
                try:
                    return float(value)
                except Exception:
                    return float("nan")
            if isinstance(value, str):
                s = value.strip()
                if "–" in s or "-" in s:
                    parts = s.replace("km", "").replace(" ", "").replace("–", "-").split("-")
                    try:
                        nums = [float(p) for p in parts if p != ""]
                        return sum(nums) / len(nums) if nums else float("nan")
                    except Exception:
                        return float("nan")
                try:
                    return float(s.replace("km", "").strip())
                except Exception:
                    return float("nan")
            return float("nan")

        # Detect common columns
        src_col = mu._find_first_present_column(
            df_proc,
            ["Source_File", "Source file", "source_file", "Source file name", "Source"],
        )
        if src_col is None:
            return in_sheets
        mot_col = mode_col
        km_one_way_col = mu._find_first_present_column(df_proc, ["km travelled one way"]) or "km travelled one way"

        # Load headcount
        if not headcount_csv.exists():
            return in_sheets
        hc = pd.read_csv(headcount_csv)

        # Target months (calendar year); allow company-specific overrides
        months_all = pd.date_range(start="2025-01-01", end="2025-12-01", freq="MS")

        final_pieces: List[pd.DataFrame] = []
        def _norm_company(val: object) -> str:
            try:
                s = str(val).strip()
            except Exception:
                return ""
            s = s.replace("\u00A0", " ")
            s = s.replace(".xlsx", "").replace(".xls", "")
            s = " ".join(s.split())
            return s.lower()

        # Precompute normalized source column for robust matching
        try:
            df_proc["_src_norm"] = df_proc[src_col].apply(_norm_company)
        except Exception:
            df_proc["_src_norm"] = ""

        for _, hr in hc.iterrows():
            company = hr.get("Company_Name")
            expected = hr.get("Expected_Headcount")
            try:
                expected = int(expected) if expected is not None and str(expected).strip() != "" else None
            except Exception:
                expected = None
            if company is None or expected is None:
                continue

            comp_norm = _norm_company(company)
            comp_data = df_proc[df_proc["_src_norm"] == comp_norm].copy()
            if comp_data.empty:
                continue

            # Most common month subset
            if "Reporting_Month" in comp_data.columns:
                most_common_month = comp_data["Reporting_Month"].value_counts().idxmax()
                base_month = comp_data[comp_data["Reporting_Month"] == most_common_month].copy()
            else:
                most_common_month = None
                base_month = comp_data.copy()

            mode_counts = base_month[mot_col].value_counts()
            if mode_counts.sum() == 0:
                continue
            mode_ratios = (mode_counts / mode_counts.sum()).to_dict()

            # Real records
            real_records = base_month.copy()
            real_count = real_records.shape[0]
            if real_count > expected:
                real_records = real_records.sample(expected, random_state=42)
                synthetic_needed = 0
            else:
                synthetic_needed = expected - real_count

            real_records["Synthetic"] = False
            real_records["Synthetic_Record_Note"] = "Original record from most common month"
            real_records["Data_Type"] = "Primary Data"

            # Synthetic
            synth_rows: List[Dict[str, object]] = []
            if synthetic_needed > 0 and mode_ratios:
                # Determine integer counts per mode that sum exactly to synthetic_needed
                modes_sorted = sorted(mode_ratios.items(), key=lambda kv: kv[1], reverse=True)
                base_counts = {m: int(synthetic_needed * r) for m, r in modes_sorted}
                allocated = sum(base_counts.values())
                remainder = max(0, synthetic_needed - allocated)
                # Give remaining +1 to top modes
                for i in range(remainder):
                    m, _ = modes_sorted[i % len(modes_sorted)]
                    base_counts[m] = base_counts.get(m, 0) + 1

                for mode, _ratio in modes_sorted:
                    count_for_mode = base_counts.get(mode, 0)
                    if count_for_mode <= 0:
                        continue
                    mode_data = base_month[base_month[mot_col] == mode].copy()
                    if km_one_way_col in mode_data.columns:
                        mode_data["km_one_way_cleaned"] = mode_data[km_one_way_col].apply(_parse_km)
                        avg_one_way = float(mode_data["km_one_way_cleaned"].mean()) if not mode_data["km_one_way_cleaned"].empty else float("nan")
                    else:
                        avg_one_way = float("nan")
                    if pd.isna(avg_one_way):
                        continue
                    avg_per_day = round(avg_one_way * 2, 2)
                    avg_per_month = round(avg_per_day * 20, 2)

                    for _ in range(count_for_mode):
                        row_obj = {
                            src_col: company,
                            "Reporting period (month, year)": (str(most_common_month) + "-01") if most_common_month else datetime.now().strftime("%Y-%m-01"),
                            mot_col: mode,
                            km_one_way_col: avg_one_way,
                            "km travelled per day": avg_per_day,
                            "km travelled per month": avg_per_month,
                            "Synthetic": True,
                            "Synthetic_Record_Note": f"Generated for {company} using proportional mode '{mode}'",
                            "Data_Type": "Extrapolated",
                        }
                        synth_rows.append(row_obj)

            one_month_df = pd.concat([real_records, pd.DataFrame(synth_rows)], ignore_index=True)

            # Duplicate to months (Fortica: Oct, Nov and Dec only)
            months = months_all
            try:
                comp_name_norm = str(company).strip().lower()
            except Exception:
                comp_name_norm = str(company).lower() if company is not None else ""
            if comp_name_norm == "fortica":
                months = pd.date_range(start="2025-10-01", end="2025-12-01", freq="MS")
            for m in months:
                copy = one_month_df.copy()
                copy["Reporting period (month, year)"] = m.strftime("%Y-%m-%d")
                final_pieces.append(copy)

        if final_pieces:
            cat7_new = pd.concat(final_pieces, ignore_index=True)
            in_sheets[CAT7_SHEET_NAME] = cat7_new
        return in_sheets
    except Exception:
        return in_sheets


def _preprocess_cat7_national_averages(
    in_sheets: Dict[str, pd.DataFrame],
    national_averages_xlsx: Path,
) -> Dict[str, pd.DataFrame]:
    """Generate Cat 7 rows from national averages + headcount source data."""
    try:
        if not national_averages_xlsx.exists():
            return in_sheets

        source_df = pd.read_excel(national_averages_xlsx)
        if source_df is None or source_df.empty:
            return in_sheets

        months_all = pd.date_range(start="2025-01-01", end="2025-12-01", freq="MS")

        def _clean_company(value: object) -> str:
            try:
                s = str(value or "").strip()
            except Exception:
                return ""
            s = s.replace("\u00A0", " ")
            s = s.replace(".xlsx", "").replace(".xls", "")
            return " ".join(s.split())

        def _num(value: object) -> float:
            try:
                parsed = pd.to_numeric(value, errors="coerce")
                return float(parsed) if not pd.isna(parsed) else float("nan")
            except Exception:
                return float("nan")

        def _allocate_counts(headcount: int, ratios: Dict[str, float]) -> Dict[str, int]:
            if headcount <= 0 or not ratios:
                return {}
            raw = {mode: (headcount * max(0.0, float(ratio)) / 100.0) for mode, ratio in ratios.items()}
            counts = {mode: int(math.floor(val)) for mode, val in raw.items()}
            remainder = max(0, int(headcount - sum(counts.values())))
            order = sorted(
                raw.keys(),
                key=lambda mode: (raw[mode] - counts[mode], raw[mode], mode),
                reverse=True,
            )
            for idx in range(remainder):
                counts[order[idx % len(order)]] += 1
            return counts

        final_pieces: List[pd.DataFrame] = []
        for _, row in source_df.iterrows():
            company_name = _clean_company(row.get("Company_Name"))
            country = str(row.get("Country") or "").strip()
            km_one_way = _num(row.get("Average one day"))
            headcount_value = _num(row.get("Headcount"))
            if not company_name or not country or pd.isna(km_one_way) or pd.isna(headcount_value):
                continue

            headcount = int(round(headcount_value))
            if headcount <= 0:
                continue

            mode_counts = _allocate_counts(
                headcount,
                {
                    "Car": _num(row.get("Car %")),
                    "Bus": _num(row.get("Bus %")),
                    "Walking and Cycling": _num(row.get("Walking and Cycling %")),
                    "Mixed": _num(row.get("Mixed %")),
                },
            )
            if not mode_counts:
                continue

            km_per_day = round(float(km_one_way) * 2, 2)
            km_per_month = round(km_per_day * 20, 2)

            month_rows: List[Dict[str, object]] = []
            for mode_name, person_count in mode_counts.items():
                if person_count <= 0:
                    continue
                for _ in range(person_count):
                    month_rows.append(
                        {
                            "Source_File": company_name,
                            "Country": country,
                            "Reporting period (month, year)": datetime.now().strftime("%Y-%m-01"),
                            "Mode of Transport": mode_name,
                            "km travelled one way": round(float(km_one_way), 2),
                            "km travelled per day": km_per_day,
                            "km travelled per month": km_per_month,
                            "Synthetic": True,
                            "Synthetic_Record_Note": (
                                f"Generated from national averages for {company_name} "
                                f"using headcount and transport share '{mode_name}'"
                            ),
                            "Data_Type": "National Average",
                        }
                    )

            if not month_rows:
                continue

            one_month_df = pd.DataFrame(month_rows)
            months = months_all
            if company_name.strip().lower() == "fortica":
                months = pd.date_range(start="2025-10-01", end="2025-12-01", freq="MS")
            for month in months:
                copy_df = one_month_df.copy()
                copy_df["Reporting period (month, year)"] = month.strftime("%Y-%m-%d")
                final_pieces.append(copy_df)

        if final_pieces:
            in_sheets[CAT7_SHEET_NAME] = pd.concat(final_pieces, ignore_index=True)
        return in_sheets
    except Exception:
        return in_sheets

def process_all_sheets() -> None:
    base_dir = Path(__file__).resolve().parent
    output_dir = STAGE2_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load input sheets (with optional Cat7 preprocessing)
    if CAT7_NATIONAL_AVERAGES_ENABLED and CAT7_NATIONAL_AVERAGES_XLSX.exists():
        try:
            raw_sheets = _load_input_workbook(base_dir)
            in_sheets = _preprocess_cat7_national_averages(raw_sheets, CAT7_NATIONAL_AVERAGES_XLSX)
        except Exception:
            in_sheets = _load_input_workbook(base_dir)
    elif CAT7_LEGACY_SURVEY_PREPROCESS_ENABLED and Path.exists(CAT7_HEADCOUNT_CSV):
        try:
            raw_sheets = _load_input_workbook(base_dir)
            in_sheets = _preprocess_cat7_proportional(raw_sheets, CAT7_HEADCOUNT_CSV)
        except Exception:
            in_sheets = _load_input_workbook(base_dir)
    else:
        in_sheets = _load_input_workbook(base_dir)

    # Load EF dictionary
    ef_dict = mu.load_emission_factors(base_dir)

    # Build restricted lookups EF ID -> EF metadata for specified EF sheets only
    # Used as a safe backfill when we already have an EF ID (e.g., manual "boq_exact" overrides).
    efid_to_value_services_goods: Dict[str, float] = {}
    efid_to_meta_services_goods: Dict[str, Dict[str, Optional[object]]] = {}
    def _norm_efid(val: Optional[object]) -> Optional[str]:
        try:
            s = str(val).strip()
        except Exception:
            return None
        if not s:
            return None
        # Uppercase and keep only letters/numbers
        import re as _re
        s2 = _re.sub(r"[^A-Za-z0-9]", "", s).upper()
        return s2 if s2 else None
    try:
        allowed_ef_sheets = {"Scope 3 Purchased Service Spend", "Scope 3 Purchased Goods Spend"}
        # Case-insensitive name map
        ef_name_map = {str(k).strip().lower(): (k, v) for k, v in ef_dict.items()}
        for wanted in allowed_ef_sheets:
            key = ef_name_map.get(str(wanted).strip().lower())
            if not key:
                continue
            _sheet_name, _df = key[0], key[1]
            if _df is None or _df.empty:
                continue
            id_col = mu._find_first_present_column(_df, [
                "ef_id", "EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"
            ])
            val_col = mu._find_first_present_column(_df, [
                "ef_value", "EF Value", "Value", "Emission", "Emission Factor Value"
            ])
            unit_col = mu._find_first_present_column(_df, ["EF Unit", "Unit", "Units"])
            source_col = mu._find_first_present_column(_df, ["Source", "Reference", "Publication", "Provider"])
            if id_col is None or val_col is None:
                continue
            try:
                cols = [id_col, val_col]
                if unit_col is not None and unit_col not in cols:
                    cols.append(unit_col)
                if source_col is not None and source_col not in cols:
                    cols.append(source_col)
                for _, rr in _df[cols].iterrows():
                    raw_id = rr.get(id_col)
                    nid = _norm_efid(raw_id)
                    if nid is None:
                        continue
                    v = rr.get(val_col)
                    if v is None or str(v).strip() == "":
                        continue
                    try:
                        fv = float(v)
                    except Exception:
                        continue
                    if nid not in efid_to_value_services_goods:
                        efid_to_value_services_goods[nid] = fv
                    if nid not in efid_to_meta_services_goods:
                        efid_to_meta_services_goods[nid] = {
                            "ef_value": fv,
                            "ef_unit": rr.get(unit_col) if unit_col else None,
                            "ef_source": rr.get(source_col) if source_col else None,
                        }
            except Exception:
                continue
    except Exception:
        efid_to_value_services_goods = {}
        efid_to_meta_services_goods = {}

    # Load optional override workbook
    override_sheets = _load_override_workbook(base_dir)
    # External manual EF lookups disabled for this run
    ext_by_prod, ext_by_boq, ext_by_code = ({}, {}, {})

    mapped_results: Dict[str, pd.DataFrame] = {}
    # unmapped_results removed per requirements

    # Progress header (stdout is redirected in web runs; keep ASCII-only, flush=True)
    try:
        total_sheets = len(in_sheets)
    except Exception:
        total_sheets = -1
    print(f"[stage2] process_all_sheets: loaded sheets={total_sheets}", flush=True)

    done = 0

    for sheet_name, df in in_sheets.items():
        # Drop unwanted sheets entirely (robust to case/spacing)
        if _canon_sheet_name(sheet_name) in DROP_SHEETS_NORMALIZED:
            continue
        if df is None or df.empty:
            continue

        done += 1
        t0 = time.time()
        try:
            nrows = len(df)
        except Exception:
            nrows = -1
        print(f"[stage2] ({done}) mapping sheet='{sheet_name}' rows={nrows}", flush=True)

        df_proc = df.copy()
        df_proc["Sheet"] = sheet_name

        # Clean Cat 7: drop rows with empty Mode of Transport
        if sheet_name == "Scope 3 Cat 7 Employee Commute":
            mode_col = mu._find_first_present_column(
                df_proc,
                [
                    "Mode of Transport",
                    "Mode of transport",
                    "Transport Mode",
                    "Transport mode",
                    "Mode",
                    "mode",
                ],
            )
            if mode_col is not None:
                col_vals = df_proc[mode_col]
                mask = col_vals.notna() & col_vals.astype(str).str.strip().ne("")
                df_proc = df_proc[mask].reset_index(drop=True)

        # If sheet is excluded, write status/match_method and skip mapping logic
        if sheet_name in EXCLUDED_SHEETS:
            out_cols = [
                "ef_id",
                "ef_name",
                "ef_unit",
                "ef_value",
                "ef_source",
                "emissions_tco2e",
                "match_method",
                "status",
            ]
            # create empty result frame with required columns
            results_df = pd.DataFrame({c: [None] * len(df_proc) for c in out_cols})
            results_df["status"] = "Excluded from mapping"
            results_df["match_method"] = "Not applicable"
            # Attach and continue
            df_out = pd.concat([df_proc.reset_index(drop=True), results_df[out_cols].reset_index(drop=True)], axis=1)
            mapped_results[sheet_name] = df_out
            continue

        spend_col = _find_spend_column(df_proc)
        # Prepare target columns
        out_cols = [
            "ef_id",
            "ef_name",
            "ef_unit",
            "ef_value",
            "ef_source",
            "emissions_tco2e",
            "match_method",
            "status",
        ]

        use_override = (sheet_name in OVERRIDE_SHEET_NAMES) and (sheet_name in override_sheets)

        if use_override:
            # Attempt to build results from override sheet columns
            ovr_df = override_sheets.get(sheet_name)
            if ovr_df is None or ovr_df.empty:
                use_override = False
            else:
                tmp_results = _build_results_from_any_columns(ovr_df, out_cols)
                # Only accept override if row counts match to ensure safe alignment
                if len(tmp_results) == len(df_proc):
                    results_df = tmp_results.copy()
                    # Mark method to reflect manual override origin when not provided
                    if "match_method" in results_df.columns:
                        results_df["match_method"] = results_df["match_method"].where(
                            results_df["match_method"].notna(), other="manual_override_example"
                        )
                else:
                    # Fallback to automatic mapping if sizes differ
                    use_override = False

        if not use_override:
            # 1) Try BoQ-based matching using Correct mapping example for selected sheets
            results_df = pd.DataFrame()
            manual_applied = False
            if sheet_name in BOQ_INPUT_HEADERS_PER_SHEET:
                # Always use the single sheet from Correct mapping example.xlsx
                override_df = _get_single_override_df(override_sheets)
                boq_lookup = _build_boq_lookup(override_df)

                if boq_lookup:
                    def map_row_boq(r: pd.Series) -> Dict[str, Optional[object]]:
                        key_raw = _extract_boq_from_row(r, BOQ_INPUT_HEADERS_PER_SHEET[sheet_name])
                        key_norm = _norm_key(key_raw) if key_raw is not None else None
                        if key_norm is None:
                            return {"status": "No BoQ match"}
                        # Only exact match (no fuzzy/contains)
                        if key_norm not in boq_lookup:
                            return {"status": "No BoQ match"}
                        item = boq_lookup[key_norm]
                        return {
                            "ef_id": None,
                            "ef_name": item.get("ef_name"),
                            "ef_unit": item.get("ef_unit"),
                            "ef_value": item.get("ef_value"),
                            "ef_source": item.get("ef_source"),
                            "match_method": "boq_exact",
                            "status": None,
                        }

                    mapped = df_proc.apply(map_row_boq, axis=1)
                    tmp_df = pd.DataFrame(mapped.tolist()) if len(mapped) else pd.DataFrame()
                    # If we have any BoQ hits, merge with automatic engine for non-hits
                    if not tmp_df.empty and ("ef_name" in tmp_df.columns or "ef_value" in tmp_df.columns or "ef_id" in tmp_df.columns):
                        # Compute automatic mapping for all rows
                        auto_results = df_proc.apply(lambda r: mu.map_emission_factor(r, ef_dict), axis=1)
                        auto_df = pd.DataFrame(auto_results.tolist()) if len(auto_results) else pd.DataFrame()
                        auto_df = auto_df.rename(columns={
                            "EF ID": "ef_id",
                            "EF Name": "ef_name",
                            "EF Unit": "ef_unit",
                            "EF Value": "ef_value",
                            "Source": "ef_source",
                            "Match Method": "match_method",
                        })
                        # Ensure columns
                        if auto_df.empty:
                            auto_df = pd.DataFrame({c: [] for c in out_cols})
                        for col in out_cols:
                            if col not in auto_df.columns:
                                auto_df[col] = None
                            if col not in tmp_df.columns:
                                tmp_df[col] = None
                        # Identify BoQ hits
                        hit_mask = (
                            (tmp_df.get("ef_id").notna() if "ef_id" in tmp_df.columns else False)
                            | (tmp_df.get("ef_name").notna() if "ef_name" in tmp_df.columns else False)
                            | (tmp_df.get("ef_value").notna() if "ef_value" in tmp_df.columns else False)
                        )
                        if isinstance(hit_mask, pd.Series) and hit_mask.any():
                            # Reset indices to avoid reindexing errors
                            auto_df = auto_df.reset_index(drop=True)
                            tmp_df = tmp_df.reset_index(drop=True)
                            hit_mask = hit_mask.reset_index(drop=True)
                            # Pandas can infer "string" dtype for these columns, which rejects
                            # non-string setitem values (e.g. numeric ef_value). Use object dtype
                            # to make the override assignment robust.
                            for col_cast in ["ef_id", "ef_name", "match_method", "status", "ef_unit", "ef_value", "ef_source"]:
                                if col_cast in auto_df.columns:
                                    auto_df[col_cast] = auto_df[col_cast].astype(object)
                            for col in ["ef_id", "ef_name", "match_method", "status", "ef_unit", "ef_value", "ef_source"]:
                                if col in tmp_df.columns and col in auto_df.columns:
                                    auto_df.loc[hit_mask, col] = tmp_df.loc[hit_mask, col]
                        results_df = auto_df

            # 1b) External manual EF lookups disabled; skip

            # 1c) Try BIMMS and CTS Denmark overrides if present (only for specified Cat 1 sheets)
            MANUAL_OVERRIDE_SHEETS = {
                "Scope 3 Cat 1 Goods Spend",
                "Scope 3 Cat 1 Goods Activity",
                "Scope 3 Cat 1 Common Purchases",
                "Scope 3 Cat 1 Services Spend",
                "Scope 3 Cat 1 Services Activity",
                "Scope 3 Cat 1 Supplier Summary",
            }
            if override_sheets and sheet_name in MANUAL_OVERRIDE_SHEETS:
                bimms_df = override_sheets.get("BIMMS")
                cts_df = override_sheets.get("CTS Denmark") if "CTS Denmark" in override_sheets else override_sheets.get("CTS Denmark ")
                bimms_lookup = _build_service_provided_lookup(bimms_df) if bimms_df is not None else {}
                cts_boq_lookup, cts_code_lookup = _build_cts_denmark_lookups(cts_df) if cts_df is not None else ({}, {})

                if bimms_lookup or cts_boq_lookup or cts_code_lookup:
                    def map_row_manual(r: pd.Series) -> Dict[str, Optional[object]]:
                        # Only for the explicitly listed Cat 1 sheets
                        # 1) CTS Denmark by TFM Code exact
                        code_val = r.get("TFM Code") or r.get("TFMCode") or r.get("Product Code / TFM Code") or r.get("Product Code") or r.get("Code")
                        if code_val is not None and str(code_val).strip() != "":
                            hit = cts_code_lookup.get(str(code_val).strip().lower())
                            if hit:
                                return {
                                    "ef_id": hit.get("ef_id"),
                                    "ef_name": hit.get("ef_name"),
                                    "match_method": "boq_exact",
                                    "status": None,
                                }
                        # 2) CTS Denmark by BoQ exact
                        boq_val = r.get("BillOfQuantity") or r.get("Bill Of Quantity") or r.get("Bill of quantity") or r.get("BoQ")
                        if boq_val is not None:
                            hit = cts_boq_lookup.get(_norm_key(boq_val))
                            if hit:
                                return {
                                    "ef_id": hit.get("ef_id"),
                                    "ef_name": hit.get("ef_name"),
                                    "match_method": "boq_exact",
                                    "status": None,
                                }
                        # 3) BIMMS by Service Provided OR Product type exact
                        sp_val = (
                            r.get("Service Provided")
                            if r.get("Service Provided") is not None else (
                                r.get("Product type") if r.get("Product type") is not None else r.get("Product Type")
                            )
                        )
                        if sp_val is None:
                            # fallback: some sheets use a generic description
                            sp_val = r.get("Product description") or r.get("Most common purchases from supplier")
                        if sp_val is not None:
                            hit = bimms_lookup.get(_norm_key(sp_val))
                            if hit:
                                return {
                                    "ef_id": hit.get("ef_id"),
                                    "ef_name": hit.get("ef_name"),
                                    "match_method": "boq_exact",
                                    "status": None,
                                }
                        return {}

                    mapped2 = df_proc.apply(map_row_manual, axis=1)
                    tmp_df2 = pd.DataFrame(mapped2.tolist()) if len(mapped2) else pd.DataFrame()
                    # Determine rows where manual mapping produced values
                    if not tmp_df2.empty and ("ef_id" in tmp_df2.columns or "ef_name" in tmp_df2.columns):
                        hit_mask = (
                            (tmp_df2.get("ef_id").notna() if "ef_id" in tmp_df2.columns else False)
                            | (tmp_df2.get("ef_name").notna() if "ef_name" in tmp_df2.columns else False)
                        )



                        if isinstance(hit_mask, pd.Series) and hit_mask.any():
                            # Compute automatic mapping then override only the hits
                            auto_results = df_proc.apply(lambda r: mu.map_emission_factor(r, ef_dict), axis=1)
                            auto_df = pd.DataFrame(auto_results.tolist()) if len(auto_results) else pd.DataFrame()
                            auto_df = auto_df.rename(columns={
                                "EF ID": "ef_id",
                                "EF Name": "ef_name",
                                "EF Unit": "ef_unit",
                                "EF Value": "ef_value",
                                "Source": "ef_source",
                                "Match Method": "match_method",
                            })
                            # Pandas 2.x can infer "string" dtype for these columns, which rejects
                            # non-string setitem values (e.g. numeric ef_id from overrides).
                            # Use object dtype to make the override assignment robust.
                            for col_cast in ["ef_id", "ef_name", "match_method", "status", "ef_unit", "ef_value", "ef_source"]:
                                if col_cast in auto_df.columns:
                                    auto_df[col_cast] = auto_df[col_cast].astype(object)
                            for col in ["ef_id", "ef_name", "match_method", "status", "ef_unit", "ef_value", "ef_source"]:
                                if col in tmp_df2.columns:
                                    if col not in auto_df.columns:
                                        auto_df[col] = None
                                    auto_df.loc[hit_mask, col] = tmp_df2.loc[hit_mask, col]
                            results_df = auto_df
                            manual_applied = True

            # 2) If no manual pipeline ran, fallback to automatic engine (no TFM-only)
            if (not manual_applied) and results_df.empty:
                results = df_proc.apply(lambda r: mu.map_emission_factor(r, ef_dict), axis=1)
                results_df = pd.DataFrame(results.tolist()) if len(results) else pd.DataFrame()

        # Normalize keys from mapping into snake_case columns
        if not results_df.empty:
            rename_map = {
                "EF ID": "ef_id",
                "EF Name": "ef_name",
                "EF Unit": "ef_unit",
                "EF Value": "ef_value",
                "Source": "ef_source",
                "Match Method": "match_method",
                "status": "status",
            }
            results_df = results_df.rename(columns=rename_map)
            for col in out_cols:
                if col not in results_df.columns:
                    results_df[col] = None
            # Ensure string-capable dtypes for manual fills
            for col_cast in ["ef_id", "ef_name", "match_method", "status", "ef_source", "ef_unit"]:
                if col_cast in results_df.columns:
                    results_df[col_cast] = results_df[col_cast].astype(object)

            # Disable ef_id->ef_value backfill for Services to avoid unintended fixed values (e.g., 0.396)
            # Instead, handle Services Spend explicit fallback via 'All together' using 'Service Provided' below.
            #
            # However: for manual "boq_exact" overrides (notably BIMMS), we can end up with ef_id/ef_name filled
            # but ef_value/ef_unit missing. In that case, it is safe to backfill by EF ID (unique key) from the
            # restricted Purchased Goods/Service EF sheets only.
            if sheet_name == "Scope 3 Cat 1 Services Spend":
                try:
                    comp_col = mu._find_first_present_column(df_proc, ["Company"])
                    comp_series = (
                        df_proc[comp_col].astype(str).str.strip().str.lower()
                        if comp_col is not None and comp_col in df_proc.columns
                        else None
                    )
                    mm = results_df["match_method"].astype(str).str.strip().str.lower()
                    is_boq_exact = mm == "boq_exact"
                    is_bimms = (comp_series == "bimms") if comp_series is not None else False
                    need_val = results_df["ef_value"].isna() | (results_df["ef_value"].astype(str).str.strip() == "")
                    has_id = results_df["ef_id"].notna() & (results_df["ef_id"].astype(str).str.strip() != "")
                    mask_fill = is_boq_exact & has_id & need_val & (is_bimms if isinstance(is_bimms, pd.Series) else True)

                    if isinstance(mask_fill, pd.Series) and bool(getattr(mask_fill, "any", lambda: False)()):
                        for idx in results_df[mask_fill].index.tolist():
                            nid = _norm_efid(results_df.at[idx, "ef_id"])
                            if nid is None:
                                continue
                            meta = efid_to_meta_services_goods.get(nid)
                            if not meta:
                                continue
                            if meta.get("ef_value") is not None:
                                results_df.at[idx, "ef_value"] = meta.get("ef_value")
                            if (results_df.at[idx, "ef_unit"] is None) or (str(results_df.at[idx, "ef_unit"]).strip() == ""):
                                if meta.get("ef_unit") is not None:
                                    results_df.at[idx, "ef_unit"] = meta.get("ef_unit")
                            if (results_df.at[idx, "ef_source"] is None) or (str(results_df.at[idx, "ef_source"]).strip() == ""):
                                if meta.get("ef_source") is not None:
                                    results_df.at[idx, "ef_source"] = meta.get("ef_source")
                except Exception:
                    # Never break mapping due to backfill issues
                    pass

            # Extra fallback for Cat 1 Goods Services: if status == "No match",
            # map via EF 'All together' by exact Product type match
            if sheet_name == "Scope 3 Cat 1 Goods Services":
                try:
                    # Locate EF 'All together' sheet in ef_dict (case-insensitive)
                    ef_key = None
                    for k in ef_dict.keys():
                        if str(k).strip().lower() == "all together":
                            ef_key = k
                            break
                    if ef_key is not None:
                        df_all = ef_dict.get(ef_key)
                    else:
                        df_all = None

                    if df_all is not None and not df_all.empty:
                        # Helpers
                        def _find_first(df_any, candidates):
                            return mu._find_first_present_column(df_any, candidates)

                        def _norm_alnum_val(v):
                            if v is None:
                                return None
                            s = str(v).strip().lower()
                            if s == "":
                                return None
                            return re.sub(r"[^a-z0-9]", "", s)

                        col_prod_all = _find_first(df_all, ["Product type", "Product Type"])  # EF key
                        col_ef_name = _find_first(df_all, ["Emission Factor Category", "EF Category", "Category", "ef_name", "EF Name"])  # type: ignore[list-item]
                        col_ef_id = _find_first(df_all, ["ef_id", "EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"])  # type: ignore[list-item]
                        col_ef_unit = _find_first(df_all, ["EF Unit", "Unit", "Units"])  # type: ignore[list-item]
                        col_ef_value = _find_first(df_all, ["ef_value", "EF Value", "Value", "Emission", "Emission Factor Value"])  # type: ignore[list-item]
                        col_source = _find_first(df_all, ["Source", "Reference", "Publication", "Provider"])  # type: ignore[list-item]

                        if col_prod_all is not None:
                            s_prod_all = df_all[col_prod_all].astype(str).str.strip().str.lower()
                            s_prod_all = s_prod_all.str.replace(r"[^a-z0-9]", "", regex=True)

                            # Find indices with "No match" status
                            no_match_mask = results_df["status"].astype(str).str.strip().str.lower() == "no match"
                            if bool(getattr(no_match_mask, "any", lambda: False)()):
                                for idx in results_df[no_match_mask].index.tolist():
                                    # Read Product type from input row
                                    prod_raw = None
                                    for key in ["Scope 3 Cat 1 Goods Services", "Product type", "Product Type"]:
                                        if key in df_proc.columns and pd.notna(df_proc.loc[idx, key]):
                                            prod_raw = df_proc.loc[idx, key]
                                            break
                                    prod_norm = _norm_alnum_val(prod_raw)
                                    if not prod_norm:
                                        continue
                                    m = (s_prod_all == prod_norm)
                                    if not bool(getattr(m, "any", lambda: False)()):
                                        continue
                                    hit_idx = m[m].index[0]
                                    best_row = df_all.loc[hit_idx]

                                    def _safe_get(col_name):
                                        return best_row[col_name] if col_name and col_name in best_row else None

                                    ef_value = _safe_get(col_ef_value)
                                    try:
                                        ef_value = float(ef_value) if ef_value is not None and str(ef_value) != "" else None
                                    except Exception:
                                        pass

                                    results_df.at[idx, "ef_id"] = _safe_get(col_ef_id)
                                    results_df.at[idx, "ef_name"] = _safe_get(col_ef_name)
                                    results_df.at[idx, "ef_unit"] = _safe_get(col_ef_unit)
                                    results_df.at[idx, "ef_value"] = ef_value
                                    results_df.at[idx, "ef_source"] = _safe_get(col_source)
                                    results_df.at[idx, "match_method"] = "Cat1 Goods Services fallback: All together exact"
                                    results_df.at[idx, "status"] = None
                except Exception:
                    # Silent fallback: do not break entire pipeline
                    pass

            # Extra fallback for Cat 1 Services Spend: if status == "No match",
            # map via EF 'All together' by exact Service Provided -> Product type match
            if sheet_name == "Scope 3 Cat 1 Services Spend":
                try:
                    ef_key = None
                    for k in ef_dict.keys():
                        if str(k).strip().lower() == "all together":
                            ef_key = k
                            break
                    df_all = ef_dict.get(ef_key) if ef_key is not None else None
                    if df_all is not None and not df_all.empty:
                        # Helpers
                        def _find_first(df_any, candidates):
                            return mu._find_first_present_column(df_any, candidates)
                        def _norm_alnum_val(v):
                            if v is None:
                                return None
                            s = str(v).strip().lower()
                            if s == "":
                                return None
                            return re.sub(r"[^a-z0-9]", "", s)

                        col_prod_all = _find_first(df_all, ["Product type", "Product Type"])  # EF key
                        col_ef_name = _find_first(df_all, ["Emission Factor Category", "EF Category", "Category", "ef_name", "EF Name"])
                        col_ef_id = _find_first(df_all, ["ef_id", "EF ID", "EFID", "ID", "Factor ID", "Emission Factor ID"])
                        col_ef_unit = _find_first(df_all, ["EF Unit", "Unit", "Units"])
                        col_ef_value = _find_first(df_all, ["ef_value", "EF Value", "Value", "Emission", "Emission Factor Value"])
                        col_source = _find_first(df_all, ["Source", "Reference", "Publication", "Provider"])
                        if col_prod_all is not None:
                            s_prod_all = df_all[col_prod_all].astype(str).str.strip().str.lower()
                            s_prod_all = s_prod_all.str.replace(r"[^a-z0-9]", "", regex=True)

                            # Input key column for Services: Service Provided
                            svc_col = mu._find_first_present_column(
                                df_proc,
                                ["Service Provided", "Service provided", "Service", "Service Name"]
                            )

                            no_match_mask = results_df["status"].astype(str).str.strip().str.lower() == "no match"
                            if svc_col is not None and bool(getattr(no_match_mask, "any", lambda: False)()):
                                for idx in results_df[no_match_mask].index.tolist():
                                    svc_raw = df_proc.loc[idx, svc_col] if (svc_col in df_proc.columns and pd.notna(df_proc.loc[idx, svc_col])) else None
                                    svc_norm = _norm_alnum_val(svc_raw)
                                    if not svc_norm:
                                        continue
                                    m = (s_prod_all == svc_norm)
                                    if not bool(getattr(m, "any", lambda: False)()):
                                        continue
                                    hit_idx = m[m].index[0]
                                    best_row = df_all.loc[hit_idx]
                                    def _safe_get(col_name):
                                        return best_row[col_name] if col_name and col_name in best_row else None
                                    ef_value = _safe_get(col_ef_value)
                                    try:
                                        ef_value = float(ef_value) if ef_value is not None and str(ef_value) != "" else None
                                    except Exception:
                                        pass
                                    results_df.at[idx, "ef_id"] = _safe_get(col_ef_id)
                                    results_df.at[idx, "ef_name"] = _safe_get(col_ef_name)
                                    results_df.at[idx, "ef_unit"] = _safe_get(col_ef_unit)
                                    results_df.at[idx, "ef_value"] = ef_value
                                    results_df.at[idx, "ef_source"] = _safe_get(col_source)
                                    results_df.at[idx, "match_method"] = "Cat1 Services Spend fallback: All together exact"
                                    results_df.at[idx, "status"] = None
                except Exception:
                    pass
        else:
            # Ensure columns exist even if no rows
            results_df = pd.DataFrame({c: [] for c in out_cols})

        # External manual fallbacks disabled (respect user's request to run without fuzzy/fallbacks)

        # Compute emissions
        if spend_col is not None and not results_df.empty:
            spend_series = pd.to_numeric(df_proc[spend_col], errors="coerce")
            ef_val_series = pd.to_numeric(results_df["ef_value"], errors="coerce")
            ef_unit_series = results_df["ef_unit"].astype(str)

            emissions_vals: List[Optional[float]] = []
            for spend, ef_val, ef_unit in zip(spend_series.tolist(), ef_val_series.tolist(), ef_unit_series.tolist()):
                emissions_vals.append(_compute_emissions_tco2e(spend, ef_val, ef_unit))
            results_df["emissions_tco2e"] = emissions_vals
        else:
            results_df["emissions_tco2e"] = None

        # Attach outputs to original data (remove any pre-existing result columns to avoid duplicates like ef_name.1)
        for col in out_cols:
            if col in df_proc.columns:
                try:
                    df_proc = df_proc.drop(columns=[col])
                except Exception:
                    pass
        df_out = pd.concat([df_proc.reset_index(drop=True), results_df[out_cols].reset_index(drop=True)], axis=1)
        mapped_results[sheet_name] = df_out
        elapsed = time.time() - t0
        print(f"[stage2] done sheet='{sheet_name}' in {elapsed:0.1f}s", flush=True)

        # No unmapped collection per requirements

    # Write mapped results
    mapped_path = output_dir / "mapped_results.xlsx"
    writer_engine = preferred_excel_writer_engine()
    try:
        with pd.ExcelWriter(mapped_path, engine=writer_engine) as writer:
            for name, dfm in mapped_results.items():
                safe_name = name[:31] if len(name) > 31 else name
                dfm.to_excel(writer, sheet_name=safe_name, index=False)
    except PermissionError:
        # If file is open/locked (e.g., by Excel), write to a timestamped file instead
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        mapped_path = output_dir / f"mapped_results_{ts}.xlsx"
        with pd.ExcelWriter(mapped_path, engine=writer_engine) as writer:
            for name, dfm in mapped_results.items():
                safe_name = name[:31] if len(name) > 31 else name
                dfm.to_excel(writer, sheet_name=safe_name, index=False)

    # No unmapped file output per requirements

    # Run calculation script before post-merge, then apply consolidation
    try:
        # Run calculations that populate/normalize totals and per-sheet values
        import calculate_me_the_chosen_one as calc  # type: ignore
        calc.main()
    except Exception:
        # Continue even if calculation step fails; keep original mapped workbook
        pass

    # Post-mapping: apply consolidation of specified sheets into targets
    try:
        from post_merge_sheets import apply_post_mapping_merges  # type: ignore
        merged_path = apply_post_mapping_merges(mapped_path)
        if merged_path is not None:
            print(f"Post-merge workbook written: {Path(merged_path).name}")
        else:
            print("Post-merge skipped: no mapped workbook found or merge failed.")
    except Exception:
        # Continue silently if post-merge fails; original mapped workbook is preserved
        pass


if __name__ == "__main__":
    process_all_sheets()


