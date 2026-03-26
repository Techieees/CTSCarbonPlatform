# Python 3.10+
# Requirements: pandas, os
# Author: Florian Demir (Sustainability Data Analyst)

import pandas as pd
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import ENGINE_STAGE1_KLARAKARBON_OUTPUT_WORK_DIR

# =============================================================================
# PATHS
# =============================================================================
base_folder = str(ENGINE_STAGE1_KLARAKARBON_OUTPUT_WORK_DIR)

input_file = os.path.join(
    base_folder,
    "klarakarbon_double_counting_20260129_1702.xlsx"
)

output_file = os.path.join(
    base_folder,
    "klarakarbon_categories_mapped_FINAL.xlsx"
)

# =============================================================================
# LOAD DATA
# =============================================================================
if not os.path.exists(input_file):
    raise FileNotFoundError(f"Input file not found:\n{input_file}")

df = pd.read_excel(input_file)

# =============================================================================
# NORMALIZE COLUMN NAMES
# =============================================================================
df.columns = df.columns.astype(str).str.strip().str.lower()

# =============================================================================
# TEXT NORMALIZATION (TEMP ONLY)
# =============================================================================
def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.lower()
         .str.strip()
         .str.replace(".xlsx", "", regex=False)
         .str.replace("_", " ", regex=False)
         .str.replace(r"[(),./]", "", regex=True)
         .str.replace(r"\s+", " ", regex=True)
    )

# temp normalized series (DO NOT SAVE AS COLUMN)
ghg_norm = normalize_text(df["ghg category"])

# =============================================================================
# KEYWORD-BASED SCOPE RULES (EXTENDED)
# =============================================================================
scope_rules = [
    # ---------------- Scope 1 ----------------
    (
        [
            "emissions related to energy production",
            "mobile combustion",
            "mobile incineration",
            "stationary combustion",
            "fugitive",
            "mobil forbrenning",
            "leaks and emissions of gases"
        ],
        "Scope 1"
    ),

    # ---------------- Scope 2 ----------------
    (
        [
            "purchased electricity",
            "purchased steam",
            "steam heating",
            "steam cooling",
            "heating",
            "cooling"
        ],
        "Scope 2"
    ),

    # ---------------- Scope 3 – Category 1 ----------------
    (
        [
            "purchasing of goods and services",
            "purchased goods and services",
            "capital goods",
            "fixed assets",
            "fuel and energy related activities",
            "leased assets",
            "rental of premises",
            "rental of equipment",
            "waste generated in operations",
            "waste management",
            "insurance",
            "investments",
            "use of sold products",
            "anleggsmidler",
            "purchase of goods and services"
        ],
        "Scope 3 Category 1 Purchased Goods and Services"
    ),

    # ---------------- Scope 3 – Category 4 ----------------
    (
        [
            "transportation",
            "distribution"
        ],
        "Scope 3 Category 4 Upstream Transportation"
    ),

    # ---------------- Scope 3 – Category 6 ----------------
    (
        [
            "business travel",
            "business trip"
        ],
        "Scope 3 Category 6 Business Travel"
    ),

    # ---------------- Scope 3 – Category 7 ----------------
    (
        [
            "employee commuting",
            "commuting",
            "microsoft forms"
        ],
        "Scope 3 Category 7 Employee Commuting"
    ),

    # ---------------- Scope 3 – Category 8 ----------------
    (
        [
            "leased assets upstream",
            "electricity averages applied per m2"
        ],
        "Scope 3 Category 8 Leased Assets"
    ),
]

# =============================================================================
# APPLY RULES (IN-PLACE OVERWRITE)
# =============================================================================
mapped_mask = pd.Series(False, index=df.index)

for keywords, target_scope in scope_rules:
    pattern = "|".join(keywords)
    mask = ghg_norm.str.contains(pattern, na=False) & (~mapped_mask)

    df.loc[mask, "ghg category"] = target_scope
    mapped_mask |= mask

# =============================================================================
# QA OUTPUT (OPTIONAL BUT RECOMMENDED)
# =============================================================================
print("\nTop GHG Category values after mapping:")
print(df["ghg category"].value_counts().head(20))

unmapped = (~mapped_mask).sum()
print(f"\nUnmapped rows remaining: {unmapped}")

# =============================================================================
# SAVE OUTPUT
# =============================================================================
df.to_excel(output_file, index=False)

print(f"\nFinal file saved to:\n{output_file}")
