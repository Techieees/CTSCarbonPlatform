# Python 3.10+
# Requirements: pandas, os, datetime
# Author: Florian Demir (Sustainability Data Analyst)

import pandas as pd
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_KLARAKARBON_OUTPUT_DIR

# =============================================================================
# HELPERS
# =============================================================================

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.lower()
         .str.strip()
         .str.replace(".xlsx", "", regex=False)   # <<< FIX BURADA
         .str.replace(r"[.,/]", "", regex=True)
         .str.replace("a/s", "as")
         .str.replace("ltd", "")
         .str.replace("limited", "")
         .str.replace(r"\s+", " ", regex=True)
    )


def contains_any(series: pd.Series, keywords: list[str]) -> pd.Series:
    pattern = "|".join(keywords)
    return series.str.contains(pattern, na=False)

# =============================================================================
# PATHS
# =============================================================================

base_folder = str(STAGE1_KLARAKARBON_OUTPUT_DIR)

input_file = os.path.join(
    base_folder,
    "combined_klarakarbon_data_20260129_170025.xlsx"
)

now_str = datetime.now().strftime("%Y%m%d_%H%M")

output_file = os.path.join(
    base_folder,
    f"klarakarbon_double_counting_{now_str}.xlsx"
)

# =============================================================================
# LOAD DATA
# =============================================================================

if not os.path.exists(input_file):
    raise FileNotFoundError(f"Input file not found:\n{input_file}")

df = pd.read_excel(input_file)

# =============================================================================
# STANDARDIZE COLUMNS (BASED ON YOUR HEADERS)
# =============================================================================

df["supplier_clean"] = normalize_text(df["Supplier"])
df["source_company_clean"] = normalize_text(df["source_file"])
df["category_clean"] = normalize_text(df["GHG Category"])

df["match_method"] = ""

# =============================================================================
# RULE SET 1 – NEP SWITCHBOARDS
# =============================================================================

nep_mask = (
    df["source_company_clean"].str.contains("nep", na=False) &
    contains_any(
        df["supplier_clean"],
        ["gt nordics", "gapit nordics", "qec nordics", "nordicepod"]
    ) &
    ~contains_any(
        df["supplier_clean"],
        ["cts nordics", "qec pt"]
    )
)

df.loc[nep_mask, ["CO2e (kg)", "match_method"]] = [
    0,
    "Double counting – NEP Switchboards rule"
]

# =============================================================================
# RULE SET 2 – GT NORDICS
# =============================================================================

gt_mask = (
    df["source_company_clean"].str.contains("gt nordics", na=False) &
    df["supplier_clean"].str.contains("cts nordics", na=False) &
    contains_any(
        df["category_clean"],
        [
            "restaurant",
            "accommodation",
            "transport",
            "building",
            "infrastructure",
            "organisational"
        ]
    )
)

df.loc[gt_mask, ["CO2e (kg)", "match_method"]] = [
    0,
    "Double counting – GT Nordics rule"
]

# =============================================================================
# RULE SET 3 – NORDICEPOD
# =============================================================================

epod_mask = (
    df["source_company_clean"].str.contains("epod", na=False) &
    contains_any(
        df["supplier_clean"],
        [
            "cts nordics",
            "gapit nordics",
            "gt nordics",
            "nep",
            "bimms",
            "navitas",
            "caerus",
            "sd nordics"
        ]
    )
)

df.loc[epod_mask, ["CO2e (kg)", "match_method"]] = [
    0,
    "Double counting – NordicEPOD rule"
]

# =============================================================================
# RULE SET 4 – GAPIT
# =============================================================================

gapit_mask = (
    df["source_company_clean"].str.contains("gapit", na=False) &
    contains_any(
        df["supplier_clean"],
        [
            "cts nordics",
            "gt nordics",
            "epod",
            "nep",
            "fortica",
            "volox"
        ]
    ) &
    contains_any(
        df["category_clean"],
        [
            "accommodation",
            "electronics",
            "electrical",
            "professional",
            "construction",
            "machinery",
            "road"
        ]
    )
)

df.loc[gapit_mask, ["CO2e (kg)", "match_method"]] = [
    0,
    "Double counting – Gapit rule"
]

# =============================================================================
# OPTIONAL DEBUG (UNCOMMENT IF NEEDED)
# =============================================================================
# print("NEP matches:", nep_mask.sum())
# print("GT matches:", gt_mask.sum())
# print("EPOD matches:", epod_mask.sum())
# print("Gapit matches:", gapit_mask.sum())

# =============================================================================
# SAVE OUTPUT
# =============================================================================

df.to_excel(output_file, index=False)

print(f"File saved successfully to:\n{output_file}")
