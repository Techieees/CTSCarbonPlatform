from __future__ import annotations

from pathlib import Path
import os
import glob
from typing import Dict, List, Optional, Tuple
import sys

import pandas as pd

TEMPLATE_MODE_2026 = "2026"


def _is_2026_mode() -> bool:
    return str(os.getenv("CTS_TEMPLATE_MODE") or "").strip() == TEMPLATE_MODE_2026


def _normalize_2026_category_label(value: object) -> str:
    text = str(value or "").strip()
    low = text.lower()
    if "category 9" in low or "cat 9" in low:
        return "Scope 3 Category 9 Downstream Transportation"
    if "category 11" in low or "cat 11" in low:
        return "Scope 3 Category 11 Use of Sold Product"
    if "category 12" in low or "cat 12" in low:
        return "Scope 3 Category 12 End of Life"
    return text

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

# Comp




def _find_latest_final_workbook(base_dir: Path) -> Optional[Path]:
    out = STAGE2_OUTPUT_DIR
    patterns = [
        # Prefer windowed outputs if present (filtered by a specific date range)
        str(out / "mapped_results_window_*.xlsx"),
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


def _add_calculation_method_from_efid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yeni sütun: 'Calculation Method'.
    Kural: ef_id alanının 4. karakterine göre:
      - 'A' → 'Activity Based'
      - 'S' → 'Spend Based'
      - 'D' → 'Distance Based'
    Diğer/eksik durumlarda None bırakılır.
    """
    if df is None or df.empty:
        return df

    # Kolon adını esnek (case-insensitive, boşluk varyantları) yakala
    lowmap = {str(c).strip().lower(): c for c in df.columns}
    ef_col = None
    for key in ["ef_id", "ef id", "efid"]:
        if key in lowmap:
            ef_col = lowmap[key]
            break
    if ef_col is None or ef_col not in df.columns:
        # ef_id yoksa yine de sütunu oluştur ama boş bırak
        df["Calculation Method"] = None
        return df

    def _map_method(val):
        try:
            s = str(val).strip()
            if len(s) < 4:
                return None
            ch = s[3].upper()
            if ch == "A":
                return "Activity Based"
            if ch == "S":
                return "Spend Based"
            if ch == "D":
                return "Distance Based"
            return None
        except Exception:
            return None

    try:
        df["Calculation Method"] = df[ef_col].map(_map_method)
    except Exception:
        # Her ihtimale karşı, hatada sütunu yine de oluştur
        df["Calculation Method"] = None
    return df


def _assign_data_type(sheet_vis: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    'Data Type' sütununu oluşturup kurallara göre doldurur.
    Kurallar:
      - Scope 1: hepsi 'Primary'
      - Scope 2: 'Sheet_booklets' içinde 'Average' geçiyorsa 'Estimate', aksi halde 'Primary'
      - S3 Cat 1 Purchased G&S: hepsi 'Primary'
      - S3 Cat 7 Employee Commute: 'Synthetic' TRUE ise 'Estimate', FALSE ise 'Primary'
      - S3 Cat 5 Waste: hepsi 'Estimation'
      - Water: 'Data Source (utilities report or water meter)' içinde 'Averages' geçiyorsa 'Estimate', aksi halde 'Primary'
      - S3 Cat 9 Downstream Transport: hepsi 'Estimate'
      - S3 Cat 12 End of Life: hepsi 'Estimate'
      - S3 Cat 6 Business Travel: hepsi 'Primary'
      - S3 Cat 4 Upstream Transport: 'Sheet_booklets' == 'Klarakarbon' ise 'Primary', 'Transportation %10' ise 'Estimate'
    """
    if df is None or df.empty:
        return df
    try:
        # Varsayılan 'Primary'
        df["Data Type"] = pd.Series(["Primary"] * len(df), dtype="object")
    except Exception:
        df["Data Type"] = "Primary"
    try:
        name_l = (sheet_vis or "").strip().lower()
        # Yardımcı kısayollar
        def _col_ci(target: str) -> Optional[str]:
            lowmap = {str(c).strip().lower(): c for c in df.columns}
            return lowmap.get(target.strip().lower())
        def _col_contains_ci(substr: str) -> Optional[str]:
            sub = substr.strip().lower()
            lowmap = {str(c).strip().lower(): c for c in df.columns}
            for low, orig in lowmap.items():
                if sub in low:
                    return orig
            return None
        # Scope 1
        if name_l.startswith("scope 1"):
            df["Data Type"] = "Primary"
            return df
        # Scope 2
        if name_l.startswith("scope 2"):
            sb_col = _col_ci("sheet_booklets") or _col_contains_ci("sheet_booklets")
            if sb_col and sb_col in df.columns:
                sb = df[sb_col].astype(str).str.lower()
                mask_avg = sb.str.contains("average", na=False)
                df.loc[mask_avg, "Data Type"] = "Estimate"
                df.loc[~mask_avg, "Data Type"] = "Primary"
            else:
                df["Data Type"] = "Primary"
            return df
        # S3 Cat 1 Purchased G&S
        if name_l.startswith("s3 cat 1"):
            df["Data Type"] = "Primary"
            return df
        # S3 Cat 7 Employee Commute
        if name_l.startswith("s3 cat 7"):
            syn_col = _col_ci("synthetic") or _col_contains_ci("synthetic")
            if syn_col and syn_col in df.columns:
                syn = df[syn_col]
                # Boole benzeri değerlere dayan
                syn_bool = syn
                try:
                    syn_bool = syn.astype(bool)
                except Exception:
                    syn_bool = syn.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
                df.loc[syn_bool.fillna(False), "Data Type"] = "Estimate"
                df.loc[~syn_bool.fillna(False), "Data Type"] = "Primary"
            else:
                df["Data Type"] = "Primary"
            return df
        # S3 Cat 5 Waste
        if name_l.startswith("s3 cat 5"):
            fac_col = (
                _col_ci("facility/ site label")
                or _col_contains_ci("facility/ site label")
                or _col_contains_ci("facility")
            )
            if fac_col and fac_col in df.columns:
                fac = df[fac_col].astype(str).str.lower()
                mask_office = fac.str.contains("office", na=False)
                df.loc[mask_office, "Data Type"] = "Estimate"
                df.loc[~mask_office, "Data Type"] = "Primary"
            else:
                # Kolon yoksa varsayılan Primary
                df["Data Type"] = "Primary"
            return df
        # Water
        if name_l == "water":
            ds_col = _col_ci("data source (utilities report or water meter)") or _col_contains_ci("data source")
            if ds_col and ds_col in df.columns:
                dsrc = df[ds_col].astype(str).str.lower()
                mask_avg = dsrc.str.contains("average", na=False)
                df.loc[mask_avg, "Data Type"] = "Estimate"
                df.loc[~mask_avg, "Data Type"] = "Primary"
            else:
                df["Data Type"] = "Primary"
            return df
        # S3 Cat 9 Downstream Transport
        if name_l.startswith("s3 cat 9"):
            df["Data Type"] = "Estimate"
            return df
        # S3 Cat 12 End of Life
        if name_l.startswith("s3 cat 12"):
            df["Data Type"] = "Estimate"
            return df
        # S3 Cat 6 Business Travel
        if name_l.startswith("s3 cat 6"):
            df["Data Type"] = "Primary"
            return df
        # S3 Cat 4 Upstream Transport
        if name_l.startswith("s3 cat 4"):
            sb_col = _col_ci("sheet_booklets") or _col_contains_ci("sheet_booklets")
            if sb_col and sb_col in df.columns:
                sb = df[sb_col].astype(str).str.strip()
                mask_kbk = sb.str.lower() == "klarakarbon"
                mask_t10 = sb.str.lower() == "transportation %10"
                df.loc[mask_kbk, "Data Type"] = "Primary"
                df.loc[mask_t10, "Data Type"] = "Estimate"
                # Diğerleri varsayılan Primary olarak kalır
            else:
                df["Data Type"] = "Primary"
            return df
        # Varsayılan: Primary
        df["Data Type"] = "Primary"
        return df
    except Exception:
        # Herhangi bir hata durumunda sütunu en azından oluşturalım
        if "Data Type" not in df.columns:
            df["Data Type"] = "Primary"
        return df


def _remap_category_for_grouping(value: object) -> str:
    """
    Revert to the requested previous behavior: remap specific GHGP categories
    into different regrouping buckets.

    Business rule: S3 Cat 15 Pensions → S3 Cat 1 Purchased Goods & Services
    (Only the target sheet bucket changes; when needed, visible
    'GHGP Category' is also aligned with the new bucket).
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

    # NEW: Merge FERA Electricity/Fuel categories into single bucket "S3 Cat 3 FERA"
    # Matches both full GHGP texts:
    #  - Scope 3 Category 3 Fuel and Energy Related Activities - Electricity
    #  - Scope 3 Category 3 Fuel and Energy Related Activities - Fuel
    if ("category 3" in s or "cat 3" in s) and ("fuel and energy related activities" in s):
        return "S3 Cat 3 FERA"
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


def _canonical_bucket_for(cat_value: object, source_sheet: str) -> str:
    """
    Map arbitrary GHGP/ghg_category text into one of the canonical sheet names
    requested by the user. Falls back to the original text when not recognized.
    """
    # Explicit source-sheet → canonical bucket hints provided by user
    BUCKET_SOURCES = {
        "Scope 1": {
            "scope 1 fuel usage spend",
            "scope 1 fuel activity",
            "scope 1 fuel usage activity",
            "klarakarbon",
        },
        "Scope 2": {
            "scope 2 electricity",
            "scope 2 electricity average",
            "klarakarbon",
        },
        "S3 Cat 1 Purchased G&S": {
            "scope 3 cat 1 goods spend",
            "scope 3 cat 1 services spend",
            "scope 3 cat 1 services activity",
            "scope 3 cat 1 goods services",
            "scope 3 services spend",
            "scope 3 cat 15 pensions",
            "klarakarbon",
            "travel",
        },
        "S3 Cat 7 Employee Commute": {
            "scope 3 cat 7 employee commute",
        },
        "Water": {
            "water tracker 2",
            "water tracker averages",
            "water tracker",
        },
        "S3 Cat 9 Downstream Transport": {
            "scope 3 category 9 activity",
        },
        "S3 Cat 12 End of Life": {
            "scope 3 cat 12 end of life",
        },
        "S3 Cat 11 Use of Sold": {
            "scope 3 category 11 scenario",
            "scope 3 cat 11 products indirec",
        },
        "S3 Cat 6 Business Travel": {
            "scope 3 cat 6 business travel",
            "klarakarbon",
            "travel",
        },
        "S3 Cat 4 Upstream Transport": {
            "transportation %10",
            "klarakarbon",
        },
    }
    CANONICAL = set(BUCKET_SOURCES.keys())

    try:
        raw = str(cat_value)
    except Exception:
        raw = ""
    s = raw.strip().lower().replace("\u00A0", " ")
    s = " ".join(s.split())

    # Also use the original sheet name as a hint when category text is weak/missing
    try:
        sh = str(source_sheet).strip().lower()
    except Exception:
        sh = ""

    def is_in(txt: str, *needles: str) -> bool:
        txt = txt or ""
        return any(n in txt for n in needles)

    # Scope 1 / Scope 2
    if is_in(s, "scope 1") or is_in(sh, "scope 1"):
        return "Scope 1"
    if is_in(s, "scope 2") or is_in(sh, "scope 2"):
        return "Scope 2"

    # Scope 3 buckets
    if is_in(s, "category 1", "cat 1", "purchased goods", "purchased g&s", "goods spend", "services spend") or is_in(sh, "cat 1"):
        return "S3 Cat 1 Purchased G&S"
    if is_in(s, "employee commute", "employee commuting", "cat 7") or is_in(sh, "cat 7"):
        return "S3 Cat 7 Employee Commute"
    if is_in(s, "waste", "cat 5") or is_in(sh, "cat 5"):
        return "S3 Cat 5 Waste"
    if is_in(s, "water"):
        return "Water"
    if is_in(s, "downstream transport", "downstream transportation", "cat 9") or is_in(sh, "cat 9"):
        return "S3 Cat 9 Downstream Transport"
    if is_in(s, "end of life", "end-of-life", "cat 12") or is_in(sh, "cat 12"):
        return "S3 Cat 12 End of Life"
    if is_in(s, "use of sold", "cat 11") or is_in(sh, "cat 11"):
        return "S3 Cat 11 Use of Sold"
    if is_in(s, "business travel", "cat 6") or is_in(sh, "cat 6"):
        return "S3 Cat 6 Business Travel"
    if is_in(s, "pension", "pensions", "cat 15") or is_in(sh, "cat 15"):
        return "S3 Cat 15 Pensions"
    if is_in(s, "upstream transport", "upstream transportation", "cat 4") or is_in(sh, "cat 4"):
        return "S3 Cat 4 Upstream Transport"

    # Fallback to given category text, will be shortened by _unique_sheet_name if needed
    # If GHGP text couldn't map to canonical bucket, try explicit sheet-name hints
    sh_l = sh
    for bucket, sources in BUCKET_SOURCES.items():
        if sh_l in sources:
            return bucket
    return raw or "Uncategorized"


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
        # Add Calculation Method derived from ef_id (A/S/D rule)
        temp = _add_calculation_method_from_efid(temp)

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
            if _is_2026_mode():
                part["GHGP Category"] = part["GHGP Category"].map(_normalize_2026_category_label)
            # KATEGORİYE GÖRE GRUPLA: remap kuralını uygula (ör. Cat 15 Pensions → Cat 1 PGS)
            bucket_key = _remap_category_for_grouping(cat_value)
            # Görünen GHGP Category metnini de yeni bucket ile hizala
            try:
                if str(bucket_key) != str(cat_value):
                    part["GHGP Category"] = str(bucket_key)
                    if _is_2026_mode():
                        part["GHGP Category"] = part["GHGP Category"].map(_normalize_2026_category_label)
            except Exception:
                pass
            # Water sheet: exclude rows whose provenance is 'Water Tracker 2'
            try:
                if str(bucket_key).strip().lower() == "water" and "Sheet_booklets" in part.columns:
                    sb_lower = part["Sheet_booklets"].astype(str).str.strip().str.lower()
                    part = part.loc[~(sb_lower == "water tracker 2")].copy()
            except Exception:
                pass
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
                        mask_srcfile = sf.str.contains(r"NEP\s*Switchboards\.xlsx", regex=True, case=False, na=False)

                        # Requested business rule: Company == "NEP Switchboards" (avoid double counting)
                        comp_col = None
                        for c in part.columns:
                            if str(c).strip().lower() == "company":
                                comp_col = c
                                break
                        mask_company = None
                        if comp_col is not None:
                            comp = part[comp_col].astype(str).str.strip().str.lower()
                            mask_company = comp == "nep switchboards"

                        # Status-based signal (sometimes already present)
                        status_col = None
                        for c in part.columns:
                            if str(c).strip().lower() == "status":
                                status_col = c
                                break
                        mask_status = None
                        if status_col is not None:
                            st = part[status_col].astype(str)
                            mask_status = st.str.contains(
                                r"NEP\s*SWB\s*emissions\s*rolled\s*into\s*NordicEPOD;\s*set\s*to\s*0",
                                regex=True,
                                case=False,
                                na=False,
                            )

                        mask_nep = mask_srcfile
                        if mask_company is not None:
                            mask_nep = mask_nep | mask_company
                        if mask_status is not None:
                            mask_nep = mask_nep | mask_status

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
                            if status_col is None:
                                status_col = "Status"
                                part[status_col] = pd.Series([None] * len(part), dtype="object")
                            part.loc[mask_nep, status_col] = "NEP SWB emissions rolled into NordicEPOD; set to 0"
            except Exception:
                pass
            # Keep original 'GHGP Category' values; do not overwrite with bucket label
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
                "border": 1,
            })
            base_fmt = wb.add_format({"border": 1})
            dec2_fmt = wb.add_format({"num_format": "0.00", "border": 1})
            dec5_fmt = wb.add_format({"num_format": "0.00000", "border": 1})
            pct_fmt = wb.add_format({
                "num_format": "0.0%",
                "border": 1,
            })
            date_fmt = wb.add_format({
                "num_format": "yyyy-mm-dd",
                "border": 1,
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
                # Apply formats selectively (no blanket numeric formatting)
                #  - co2e/t columns: 2 decimals (or 5 decimals for Waste sheet)
                #  - date-like columns: date format
                #  - scope: no numeric format (show plain 1/2/3)
                #  - contribution: percentage
                sheet_low = str(sheet_name).strip().lower()
                if col_low in {"co2e", "co2e (t)", "tco2e_total"}:
                    if "waste" in sheet_low:  # e.g., "s3 cat 5 waste"
                        ws.set_column(idx, idx, width, dec5_fmt)
                    else:
                        ws.set_column(idx, idx, width, dec2_fmt)
                elif col_low == "contribution":
                    ws.set_column(idx, idx, width, pct_fmt)
                elif col_low == "date":
                    ws.set_column(idx, idx, width, date_fmt)
                else:
                    # Do not force decimals on generic numeric columns.
                    # Keep 'scope' as integer-like (no format).
                    if col_low in {"scope"}:
                        ws.set_column(idx, idx, width, base_fmt)
                    else:
                        # For other columns, just set width (no number format)
                        ws.set_column(idx, idx, width, base_fmt)
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

                        # Travel kuralı: Company boşsa, Source_file'dan doldur ve CTS-Nordics'i normalize et
                        try:
                            mask_travel = dfc["Sheet_booklets"].astype(str).str.strip().str.lower() == "travel"
                            if bool(getattr(mask_travel, "any", lambda: False)()):
                                # Source_file benzeri sütunu bul (case-insensitive yaygın varyantlar)
                                src_col = None
                                lowmap = {str(c).strip().lower(): c for c in dfc.columns}
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
                                    for cand in ["Source_File", "Source_file", "Source file", "SourceFile"]:
                                        if cand in dfc.columns:
                                            src_col = cand
                                            break
                                # Company sütunu yoksa oluştur
                                if "Company" not in dfc.columns:
                                    dfc["Company"] = pd.Series([None] * len(dfc), dtype="object")
                                if src_col is not None:
                                    comp_series = dfc["Company"]
                                    # Boş/NA Company hücreleri
                                    comp_is_empty = comp_series.isna() | (comp_series.astype(str).str.strip() == "")
                                    assign_mask = mask_travel & comp_is_empty
                                    if bool(getattr(assign_mask, "any", lambda: False)()):
                                        dfc.loc[assign_mask, "Company"] = dfc.loc[assign_mask, src_col].astype(str)

                                # Travel özel isim standardizasyonu: CTS-Nordics → CTS Nordics
                                try:
                                    if "Company" in dfc.columns:
                                        comp_str = dfc["Company"].astype(str)
                                        # Tam eşleşme, tire veya boşluk varyantlarını kapsa
                                        mask_cts = comp_str.str.contains(r"^\s*cts[-\s]?nordics\s*$", case=False, regex=True, na=False)
                                        fix_mask = mask_travel & mask_cts
                                        if bool(getattr(fix_mask, "any", lambda: False)()):
                                            dfc.loc[fix_mask, "Company"] = "CTS Nordics"
                                except Exception:
                                    pass
                        except Exception:
                            pass

                        # Company normalization for S3 Cat 3 FERA: lowercase to proper case
                        try:
                            if sheet_vis == "S3 Cat 3 FERA" and "Company" in dfc.columns:
                                comp_lower = dfc["Company"].astype(str).str.strip().str.lower()
                                mask_fortica = comp_lower == "fortica"
                                mask_gapit = comp_lower == "gapit"
                                if bool(getattr(mask_fortica, "any", lambda: False)()):
                                    dfc.loc[mask_fortica, "Company"] = "Fortica"
                                if bool(getattr(mask_gapit, "any", lambda: False)()):
                                    dfc.loc[mask_gapit, "Company"] = "Gapit"
                        except Exception:
                            pass

                        # Company normalization for S3 Cat 5 Waste: "Gapit Nordics" -> "Gapit"
                        try:
                            if sheet_vis == "S3 Cat 5 Waste" and "Company" in dfc.columns:
                                comp_series = dfc["Company"].astype(str).str.strip()
                                mask_gapit_n = comp_series.str.contains(r"^\s*gapit\s*nordics\s*$", case=False, regex=True, na=False)
                                if bool(getattr(mask_gapit_n, "any", lambda: False)()):
                                    dfc.loc[mask_gapit_n, "Company"] = "Gapit"
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
                # Data Type sütununu kurallara göre ata
                try:
                    dfc = _assign_data_type(sheet_vis, dfc)
                except Exception:
                    pass
                # Build Date column with per-sheet priorities
                def _build_date_for_sheet(sheet_name: str, frame: pd.DataFrame) -> pd.Series:
                    priorities = {
                        "Scope 1": ["Reporting period (month, year)", "release date"],
                        "Scope 2": ["Reporting period (month, year)", "release date", "Reporting Period"],
                        "S3 Cat 3 FERA": ["Reporting period (month, year)"],
                        "S3 Cat 1 Purchased G&S": [
                            "Reporting period (month, year)",
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
                    if not _is_2026_mode():
                        priorities["S3 Cat 1 Purchased G&S"].extend([
                            "Purchase Date (Purchase order date or invoice date)",
                            "release date",
                        ])
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
                    date_series = _build_date_for_sheet(sheet_vis, dfc)
                    dfc["Date"] = date_series

                    # Scope 2 + GT Nordics özel kuralı:
                    # 'Reporting period (month, year)' sütunu YYYY-DD-MM formatında → Date'e YYYY-MM-DD olarak yaz.
                    try:
                        if sheet_vis.strip().lower().startswith("scope 2") and "Company" in dfc.columns:
                            comp_lower = dfc["Company"].astype(str).str.strip().str.lower()
                            mask_gt = comp_lower == "gt nordics"
                            if bool(getattr(mask_gt, "any", lambda: False)()):
                                # Kaynak sütunu case-insensitive bul
                                rp_col = None
                                for c in dfc.columns:
                                    if str(c).strip().lower() == "reporting period (month, year)":
                                        rp_col = c
                                        break
                                if rp_col is not None:
                                    s_raw = dfc.loc[mask_gt, rp_col].astype(str).str.strip()
                                    # Gelen format YYYY-DD-MM → parse format="%Y-%d-%m"
                                    dt = pd.to_datetime(s_raw, format="%Y-%d-%m", errors="coerce")
                                    try:
                                        dfc.loc[mask_gt, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask_gt, "Date"] = dt
                    except Exception:
                        pass

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

                    # DC Piping özel kuralı: Date, 'Purchase Date (Purchase order date or invoice date)' sütunundan
                    try:
                        if _is_2026_mode():
                            raise StopIteration
                        if "Company" in dfc.columns:
                            mask_dc = dfc["Company"].astype(str).str.strip().str.lower() == "dc piping"
                            if bool(getattr(mask_dc, "any", lambda: False)()):
                                # İlgili satın alma tarihi sütununu bul (case-insensitive)
                                purchase_col = None
                                target_name = "purchase date (purchase order date or invoice date)"
                                for c in dfc.columns:
                                    if str(c).strip().lower() == target_name:
                                        purchase_col = c
                                        break
                                # Alternatif kısa isim dene
                                if purchase_col is None:
                                    for alt in ["purchase date", "purchase_date"]:
                                        for c in dfc.columns:
                                            if str(c).strip().lower() == alt:
                                                purchase_col = c
                                                break
                                        if purchase_col is not None:
                                            break
                                if purchase_col is not None:
                                    s_raw = dfc.loc[mask_dc, purchase_col]
                                    try:
                                        dt = pd.to_datetime(s_raw, errors="coerce")
                                    except Exception:
                                        dt = pd.to_datetime(s_raw.astype(str), errors="coerce")
                                    try:
                                        dfc.loc[mask_dc, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask_dc, "Date"] = dt
                    except Exception:
                        pass

                    # MC Prefab özel kuralı: Date, 'Purchase Date (Purchase order date or invoice date)' sütunundan
                    try:
                        if _is_2026_mode():
                            raise StopIteration
                        if "Company" in dfc.columns:
                            mask_mcp = dfc["Company"].astype(str).str.strip().str.lower() == "mc prefab"
                            if bool(getattr(mask_mcp, "any", lambda: False)()):
                                purchase_col = None
                                target_name = "purchase date (purchase order date or invoice date)"
                                for c in dfc.columns:
                                    if str(c).strip().lower() == target_name:
                                        purchase_col = c
                                        break
                                if purchase_col is None:
                                    for alt in ["purchase date", "purchase_date"]:
                                        for c in dfc.columns:
                                            if str(c).strip().lower() == alt:
                                                purchase_col = c
                                                break
                                        if purchase_col is not None:
                                            break
                                if purchase_col is not None:
                                    s_raw = dfc.loc[mask_mcp, purchase_col]
                                    try:
                                        dt = pd.to_datetime(s_raw, errors="coerce")
                                    except Exception:
                                        dt = pd.to_datetime(s_raw.astype(str), errors="coerce")
                                    try:
                                        dfc.loc[mask_mcp, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask_mcp, "Date"] = dt
                    except Exception:
                        pass

                    # Velox özel kuralı: Date, 'Purchase Date (Purchase order date or invoice date)' sütunundan
                    try:
                        if _is_2026_mode():
                            raise StopIteration
                        if "Company" in dfc.columns:
                            mask_velox_c = dfc["Company"].astype(str).str.strip().str.lower() == "velox"
                            if bool(getattr(mask_velox_c, "any", lambda: False)()):
                                purchase_col = None
                                target_name = "purchase date (purchase order date or invoice date)"
                                for c in dfc.columns:
                                    if str(c).strip().lower() == target_name:
                                        purchase_col = c
                                        break
                                if purchase_col is None:
                                    for alt in ["purchase date", "purchase_date"]:
                                        for c in dfc.columns:
                                            if str(c).strip().lower() == alt:
                                                purchase_col = c
                                                break
                                        if purchase_col is not None:
                                            break
                                if purchase_col is not None:
                                    s_raw = dfc.loc[mask_velox_c, purchase_col]
                                    try:
                                        dt = pd.to_datetime(s_raw, errors="coerce")
                                    except Exception:
                                        dt = pd.to_datetime(s_raw.astype(str), errors="coerce")
                                    try:
                                        dfc.loc[mask_velox_c, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask_velox_c, "Date"] = dt
                    except Exception:
                        pass

                    # CTS EU özel kuralı: Date, 'Purchase Date (Purchase order date or invoice date)' sütunundan
                    try:
                        if _is_2026_mode():
                            raise StopIteration
                        if "Company" in dfc.columns:
                            comp_lower = dfc["Company"].astype(str).str.strip().str.lower()
                            mask_cts_eu = comp_lower.isin({"cts eu", "cts-eu"})
                            if bool(getattr(mask_cts_eu, "any", lambda: False)()):
                                purchase_col = None
                                target_name = "purchase date (purchase order date or invoice date)"
                                for c in dfc.columns:
                                    if str(c).strip().lower() == target_name:
                                        purchase_col = c
                                        break
                                if purchase_col is None:
                                    for alt in ["purchase date", "purchase_date"]:
                                        for c in dfc.columns:
                                            if str(c).strip().lower() == alt:
                                                purchase_col = c
                                                break
                                        if purchase_col is not None:
                                            break
                                if purchase_col is not None:
                                    s_raw = dfc.loc[mask_cts_eu, purchase_col]
                                    try:
                                        dt = pd.to_datetime(s_raw, errors="coerce")
                                    except Exception:
                                        dt = pd.to_datetime(s_raw.astype(str), errors="coerce")
                                    try:
                                        dfc.loc[mask_cts_eu, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask_cts_eu, "Date"] = dt
                    except Exception:
                        pass

                    # DC Piping + Scope 3 Cat 15 Pensions: 'Reporting period (month, year)' → Date
                    try:
                        if "Company" in dfc.columns and "Sheet_booklets" in dfc.columns:
                            mask_dc = dfc["Company"].astype(str).str.strip().str.lower() == "dc piping"
                            mask_pension = dfc["Sheet_booklets"].astype(str).str.strip().str.lower() == "scope 3 cat 15 pensions"
                            mask = mask_dc & mask_pension
                            if bool(getattr(mask, "any", lambda: False)()):
                                rp_col = None
                                for c in dfc.columns:
                                    if str(c).strip().lower() == "reporting period (month, year)":
                                        rp_col = c
                                        break
                                if rp_col is None:
                                    for alt in ["reporting period", "reporting_period"]:
                                        for c in dfc.columns:
                                            if str(c).strip().lower() == alt:
                                                rp_col = c
                                                break
                                        if rp_col is not None:
                                            break
                                if rp_col is not None:
                                    s_raw = dfc.loc[mask, rp_col]
                                    try:
                                        dt = pd.to_datetime(s_raw, errors="coerce")
                                    except Exception:
                                        dt = pd.to_datetime(s_raw.astype(str), errors="coerce")
                                    # Date boşsa doldur
                                    date_empty = dfc["Date"].isna() if "Date" in dfc.columns else mask
                                    assign_mask = mask & date_empty
                                    if bool(getattr(assign_mask, "any", lambda: False)()):
                                        try:
                                            dfc.loc[assign_mask, "Date"] = dt.dt.date
                                        except Exception:
                                            dfc.loc[assign_mask, "Date"] = dt
                    except Exception:
                        pass

                    # Velox + Scope 3 Cat 15 Pensions: 'Reporting period (month, year)' → Date
                    try:
                        if "Company" in dfc.columns and "Sheet_booklets" in dfc.columns:
                            mask_velox_p = dfc["Company"].astype(str).str.strip().str.lower() == "velox"
                            mask_pension = dfc["Sheet_booklets"].astype(str).str.strip().str.lower() == "scope 3 cat 15 pensions"
                            mask = mask_velox_p & mask_pension
                            if bool(getattr(mask, "any", lambda: False)()):
                                rp_col = None
                                for c in dfc.columns:
                                    if str(c).strip().lower() == "reporting period (month, year)":
                                        rp_col = c
                                        break
                                if rp_col is None:
                                    for alt in ["reporting period", "reporting_period"]:
                                        for c in dfc.columns:
                                            if str(c).strip().lower() == alt:
                                                rp_col = c
                                                break
                                        if rp_col is not None:
                                            break
                                if rp_col is not None:
                                    s_raw = dfc.loc[mask, rp_col]
                                    try:
                                        dt = pd.to_datetime(s_raw, errors="coerce")
                                    except Exception:
                                        dt = pd.to_datetime(s_raw.astype(str), errors="coerce")
                                    date_empty = dfc["Date"].isna() if "Date" in dfc.columns else mask
                                    assign_mask = mask & date_empty
                                    if bool(getattr(assign_mask, "any", lambda: False)()):
                                        try:
                                            dfc.loc[assign_mask, "Date"] = dt.dt.date
                                        except Exception:
                                            dfc.loc[assign_mask, "Date"] = dt
                    except Exception:
                        pass

                    # S3 Cat 11 Use of Sold: For Company == 'GT Nordics', copy 'Reporting Period' → 'Date'
                    try:
                        if sheet_vis == "S3 Cat 11 Use of Sold" and "Company" in dfc.columns:
                            comp_lower = dfc["Company"].astype(str).str.strip().str.lower()
                            mask_gt = comp_lower == "gt nordics"
                            if bool(getattr(mask_gt, "any", lambda: False)()):
                                rp_col = None
                                for c in dfc.columns:
                                    if str(c).strip().lower() == "reporting period":
                                        rp_col = c
                                        break
                                if rp_col is not None:
                                    s_raw = dfc.loc[mask_gt, rp_col]
                                    try:
                                        dt = pd.to_datetime(s_raw, errors="coerce")
                                    except Exception:
                                        dt = pd.to_datetime(s_raw.astype(str), errors="coerce")
                                    try:
                                        dfc.loc[mask_gt, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask_gt, "Date"] = dt
                    except Exception:
                        pass

                    # S3 Cat 11 Use of Sold: For Company in {'GT Nordics','NEP Switchboards'},
                    # fill missing 'co2e (t)' from
                    # 'CO2e emissions from electricity consumed from use scenarios (tonnes CO2e)'
                    try:
                        if sheet_vis == "S3 Cat 11 Use of Sold" and "Company" in dfc.columns:
                            comp_lower = dfc["Company"].astype(str).str.strip().str.lower()
                            mask_targets = comp_lower.isin({"gt nordics", "nep switchboards"})
                            if bool(getattr(mask_targets, "any", lambda: False)()):
                                dest_col = None
                                src_col = None
                                for c in dfc.columns:
                                    low = str(c).strip().lower()
                                    if low == "co2e (t)" and dest_col is None:
                                        dest_col = c
                                    if low == "co2e emissions from electricity consumed from use scenarios (tonnes co2e)":
                                        src_col = c
                                if dest_col is not None and src_col is not None:
                                    cur = pd.to_numeric(dfc[dest_col], errors="coerce")
                                    src = pd.to_numeric(dfc[src_col], errors="coerce")
                                    assign_mask = mask_targets & ((cur.isna()) | (cur == 0.0)) & src.notna()
                                    dfc.loc[assign_mask, dest_col] = src.loc[assign_mask]
                    except Exception:
                        pass

                    # S3 Cat 9 Downstream Transport: For Company == 'GT Nordics',
                    # fill missing 'co2e (t)' from 'co2e (t).1'
                    try:
                        if sheet_vis == "S3 Cat 9 Downstream Transport" and "Company" in dfc.columns:
                            comp_lower = dfc["Company"].astype(str).str.strip().str.lower()
                            mask_gt = comp_lower == "gt nordics"
                            if bool(getattr(mask_gt, "any", lambda: False)()):
                                co2e_t_col = None
                                co2e_t1_col = None
                                for c in dfc.columns:
                                    low = str(c).strip().lower()
                                    if low == "co2e (t)":
                                        co2e_t_col = c
                                    if low == "co2e (t).1":
                                        co2e_t1_col = c
                                if co2e_t_col is not None and co2e_t1_col is not None:
                                    cur = pd.to_numeric(dfc[co2e_t_col], errors="coerce")
                                    src = pd.to_numeric(dfc[co2e_t1_col], errors="coerce")
                                    # Copy only where current is NaN or zero
                                    assign_mask = mask_gt & ((cur.isna()) | (cur == 0.0))
                                    dfc.loc[assign_mask, co2e_t_col] = src.loc[assign_mask]
                    except Exception:
                        pass

                    # Scope 2: For Company == 'GT Nordics' and Sheet_booklets == 'Klarakarbon',
                    # copy 'release date' into 'Date' (parse as DD.MM.YYYY first, then generic).
                    try:
                        if (sheet_vis.strip().lower().startswith("scope 2")
                            and "Company" in dfc.columns
                            and "Sheet_booklets" in dfc.columns):
                            comp_lower = dfc["Company"].astype(str).str.strip().str.lower()
                            src_lower = dfc["Sheet_booklets"].astype(str).str.strip().str.lower()
                            mask = (comp_lower == "gt nordics") & (src_lower == "klarakarbon")
                            if bool(getattr(mask, "any", lambda: False)()):
                                rel_col = None
                                for c in dfc.columns:
                                    if str(c).strip().lower() == "release date":
                                        rel_col = c
                                        break
                                if rel_col is not None:
                                    raw = dfc.loc[mask, rel_col]
                                    # Try explicit DD.MM.YYYY, then generic
                                    dt1 = pd.to_datetime(raw, format="%d.%m.%Y", errors="coerce")
                                    dtg = pd.to_datetime(raw, errors="coerce")
                                    dt = dt1.combine_first(dtg)
                                    try:
                                        dfc.loc[mask, "Date"] = dt.dt.date
                                    except Exception:
                                        dfc.loc[mask, "Date"] = dt
                    except Exception:
                        pass
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
                        try:
                            total_sum = pd.to_numeric(group["co2e (t)"], errors="coerce").sum(min_count=1)
                            if total_sum and total_sum != 0:
                                group["Share (%)"] = (pd.to_numeric(group["co2e (t)"], errors="coerce") / float(total_sum)) * 100.0
                            else:
                                group["Share (%)"] = 0.0
                        except Exception:
                            group["Share (%)"] = 0.0
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
                            try:
                                # Share within GHGP_Sheet (category-based)
                                cat_tot = comp_sheet.groupby("GHGP_Sheet", dropna=False)["co2e (t)"].transform("sum")
                                comp_sheet["Share in GHGP (%)"] = (
                                    pd.to_numeric(comp_sheet["co2e (t)"], errors="coerce") / cat_tot.replace(0, pd.NA)
                                ) * 100.0
                            except Exception:
                                comp_sheet["Share in GHGP (%)"] = 0.0
                            try:
                                # Share within Company (company-based)
                                comp_tot = comp_sheet.groupby("Company", dropna=False)["co2e (t)"].transform("sum")
                                comp_sheet["Share in Company (%)"] = (
                                    pd.to_numeric(comp_sheet["co2e (t)"], errors="coerce") / comp_tot.replace(0, pd.NA)
                                ) * 100.0
                            except Exception:
                                comp_sheet["Share in Company (%)"] = 0.0
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
                            try:
                                total_cat = pd.to_numeric(sheet_only["co2e (t)"], errors="coerce").sum(min_count=1)
                                if total_cat and total_cat != 0:
                                    sheet_only["Share (%)"] = (
                                        pd.to_numeric(sheet_only["co2e (t)"], errors="coerce") / float(total_cat)
                                    ) * 100.0
                                else:
                                    sheet_only["Share (%)"] = 0.0
                            except Exception:
                                sheet_only["Share (%)"] = 0.0
                            sheet_st = _unique_sheet_name("GHGP Sheet Totals", used_names)
                            sheet_only.to_excel(writer, sheet_name=sheet_st, index=False)
                            _style_sheet(writer, sheet_st, sheet_only)

                            # Data Volume Summary: total rows and monthly/company counts
                            try:
                                dv_sheet = _unique_sheet_name("Data Volume Summary", used_names)
                                # Prepare base
                                base_df = combined_all.copy()
                                # Ensure Company column exists
                                if "Company" not in base_df.columns:
                                    base_df["Company"] = None
                                # Derive Month
                                month_series = None
                                try:
                                    if "Date" in base_df.columns:
                                        dt = pd.to_datetime(base_df["Date"], errors="coerce")
                                        month_series = dt.dt.to_period("M").dt.to_timestamp().dt.strftime("%B %Y")
                                except Exception:
                                    month_series = None
                                if month_series is None:
                                    try:
                                        if "Reporting_Month" in base_df.columns:
                                            rm = pd.to_datetime(base_df["Reporting_Month"], errors="coerce")
                                            month_series = rm.dt.to_period("M").dt.to_timestamp().dt.strftime("%B %Y")
                                    except Exception:
                                        month_series = None
                                if month_series is None:
                                    try:
                                        if "Reporting period (month, year)" in base_df.columns:
                                            rp = pd.to_datetime(base_df["Reporting period (month, year)"], errors="coerce")
                                            month_series = rp.dt.to_period("M").dt.to_timestamp().dt.strftime("%B %Y")
                                    except Exception:
                                        month_series = None
                                if month_series is None:
                                    month_series = pd.Series([None] * len(base_df), index=base_df.index)
                                base_df["Month"] = month_series

                                # Tables
                                total_rows = int(len(base_df))
                                df_total = pd.DataFrame([{"Metric": "Total rows", "Value": total_rows}])

                                by_month = (
                                    base_df.groupby("Month", dropna=False)
                                    .size()
                                    .reset_index(name="Record Count")
                                )
                                # Sort Month chronologically when possible
                                try:
                                    _m = pd.to_datetime(by_month["Month"], format="%B %Y", errors="coerce")
                                    by_month = by_month.assign(_sort=_m).sort_values(["_sort", "Month"]).drop(columns=["_sort"])
                                except Exception:
                                    pass

                                by_company = (
                                    base_df.groupby("Company", dropna=False)
                                    .size()
                                    .reset_index(name="Record Count")
                                    .sort_values("Record Count", ascending=False)
                                )

                                by_company_month = (
                                    base_df.groupby(["Company", "Month"], dropna=False)
                                    .size()
                                    .reset_index(name="Record Count")
                                )
                                try:
                                    _m2 = pd.to_datetime(by_company_month["Month"], format="%B %Y", errors="coerce")
                                    by_company_month = by_company_month.assign(_sort=_m2).sort_values(
                                        ["Company", "_sort", "Month"]
                                    ).drop(columns=["_sort"])
                                except Exception:
                                    by_company_month = by_company_month.sort_values(["Company", "Month"])

                                # Write in one sheet with sections
                                start = 0
                                df_total.to_excel(writer, sheet_name=dv_sheet, index=False, startrow=start, startcol=0)
                                start += len(df_total) + 2

                                # Section titles
                                ws = writer.sheets.get(dv_sheet)
                                try:
                                    wb = writer.book
                                    title_fmt = wb.add_format({"bold": True, "bg_color": "#E2EFDA", "border": 1})
                                except Exception:
                                    title_fmt = None

                                try:
                                    if ws is not None and title_fmt is not None:
                                        ws.write(start, 0, "By Month", title_fmt)
                                except Exception:
                                    pass
                                start += 1
                                by_month.to_excel(writer, sheet_name=dv_sheet, index=False, startrow=start, startcol=0)
                                start += len(by_month) + 2

                                try:
                                    if ws is not None and title_fmt is not None:
                                        ws.write(start, 0, "By Company", title_fmt)
                                except Exception:
                                    pass
                                start += 1
                                by_company.to_excel(writer, sheet_name=dv_sheet, index=False, startrow=start, startcol=0)
                                start += len(by_company) + 2

                                try:
                                    if ws is not None and title_fmt is not None:
                                        ws.write(start, 0, "By Company and Month", title_fmt)
                                except Exception:
                                    pass
                                start += 1
                                by_company_month.to_excel(writer, sheet_name=dv_sheet, index=False, startrow=start, startcol=0)

                                # Basic autosize for this sheet (apply once on the last table for column widths)
                                try:
                                    _style_sheet(writer, dv_sheet, by_company_month)
                                except Exception:
                                    pass
                            except Exception:
                                pass

                            # Data Type Summary (overall)
                            try:
                                if "Data Type" in combined_all.columns:
                                    dt_series = combined_all["Data Type"].astype(str).str.strip()
                                    total_n = len(dt_series)
                                    if total_n > 0:
                                        counts = dt_series.value_counts(dropna=False).reset_index()
                                        counts.columns = ["Data Type", "Count"]
                                        counts["Share"] = counts["Count"] / float(total_n)
                                        sheet_dt = _unique_sheet_name("Data Type Summary", used_names)
                                        counts.to_excel(writer, sheet_name=sheet_dt, index=False)
                                        _style_sheet(writer, sheet_dt, counts)
                            except Exception:
                                pass

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
                                try:
                                    ghgp_cols = [c for c in pivot.columns if c != "Company"]
                                    pivot["Row Total (t)"] = pd.to_numeric(pivot[ghgp_cols], errors="coerce").sum(axis=1, min_count=1)
                                    grand_total = pd.to_numeric(pivot["Row Total (t)"], errors="coerce").sum(min_count=1)
                                    if grand_total and grand_total != 0:
                                        pivot["Company Share in Total (%)"] = (
                                            pd.to_numeric(pivot["Row Total (t)"], errors="coerce") / float(grand_total)
                                        ) * 100.0
                                    else:
                                        pivot["Company Share in Total (%)"] = 0.0
                                except Exception:
                                    pivot["Row Total (t)"] = 0.0
                                    pivot["Company Share in Total (%)"] = 0.0
                                sheet_pv = _unique_sheet_name("Company Stacked Data", used_names)
                                pivot.to_excel(writer, sheet_name=sheet_pv, index=False)
                                _style_sheet(writer, sheet_pv, pivot)

                                # Also build a month-stacked variant based on available months in data
                                try:
                                    if "Date" in combined_labeled.columns:
                                        tmp = combined_labeled.copy()
                                        tmp["__Date"] = pd.to_datetime(tmp["Date"], errors="coerce")
                                        tmp["__MonthPeriod"] = tmp["__Date"].dt.to_period("M")
                                        tmp["__MonthLabel"] = tmp["__Date"].dt.strftime("%B %Y")

                                        # Keep GHGP columns order consistent with the non-month pivot
                                        ghgp_cols = [c for c in pivot.columns if c != "Company"]

                                        # Build the month list dynamically from data (sorted)
                                        months_periods = [p for p in tmp["__MonthPeriod"].dropna().unique().tolist()]
                                        try:
                                            # sort periods chronologically
                                            months_periods = sorted(months_periods, key=lambda p: p.start_time)
                                        except Exception:
                                            months_periods = sorted(months_periods)
                                        months_labels = [
                                            (p.to_timestamp() if hasattr(p, "to_timestamp") else pd.Period(p, freq="M").to_timestamp()).strftime("%B %Y")
                                            for p in months_periods
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
                                            try:
                                                ghgp_cols_m = [c for c in pv_months.columns if c not in {"Month", "Company"}]
                                                pv_months["Row Total (t)"] = pd.to_numeric(pv_months[ghgp_cols_m], errors="coerce").sum(axis=1, min_count=1)
                                                # Company share within each month
                                                def _share_in_month(s):
                                                    tot = pd.to_numeric(s, errors="coerce").sum(min_count=1)
                                                    return (pd.to_numeric(s, errors="coerce") / float(tot) * 100.0) if tot else 0.0
                                                pv_months["Company Share in Month (%)"] = (
                                                    pv_months.groupby("Month", dropna=False)["Row Total (t)"]
                                                    .transform(lambda s: (s / (pd.to_numeric(s, errors="coerce").sum(min_count=1) or 1.0)) * 100.0)
                                                )
                                            except Exception:
                                                pv_months["Row Total (t)"] = 0.0
                                                pv_months["Company Share in Month (%)"] = 0.0
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


def main() -> None:
    regroup_by_ghgp()


if __name__ == "__main__":
    main()


