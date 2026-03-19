# Python 3.13.3
# Requirements:
# Pip install pandas openpyxl
# This code has been written by Florian Demir (Sustainability Data Analyst)
# This code is used to normalize the merged WT, ELEC, DH emission factor mapping file with improved date parsing and currency normalization

from __future__ import annotations

import argparse
import re
from datetime import datetime

import pandas as pd
from pandas import ExcelWriter


# Inputs (defaults preserved; can be overridden via CLI)
DEFAULT_INPUT = r"engine/stage1_preprocess/Datas\merged_WT_ELEC_DH_S3C15_20260225_145121.xlsx"
DEFAULT_OUTPUT = f"normalized_emission_factor_mapping_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


# Currency normalization dictionary
currency_map = {
    "eur": "EUR",
    "euro": "EUR",
    "€": "EUR",
    "nok": "NOK",
    "kr": "NOK",
    "sek": "SEK",
    "dkk": "DKK",
    "usd": "USD",
    "us dollar": "USD",
    "$": "USD",
}


# Columns considered as dates
date_patterns = [
    "reporting period",
    "when supplier onboarded",
    "when product purchased",
    "purchase date",  # NEW: covers Purchase Date (Purchase order date or invoice date)
]


def parse_date_value(val, default_year=2025):
    if pd.isna(val):
        return None

    # Handle numbers, including scientific notation as string
    try:
        if isinstance(val, str) and re.search(r"e\+\d+", val.lower()):
            val_num = float(val)
        elif isinstance(val, (int, float)):
            val_num = val
        else:
            val_num = None

        if val_num is not None:
            # Unix timestamp in nanoseconds (1e17–1e19 range)
            if 1e17 < val_num < 1e19:
                return datetime.utcfromtimestamp(val_num / 1e9).strftime("%Y-%m-%d")
            # Unix timestamp in milliseconds (1e12–1e15 range)
            if 1e12 < val_num < 1e15:
                return datetime.utcfromtimestamp(val_num / 1e3).strftime("%Y-%m-%d")
            # Excel serial number (days since 1899-12-30)
            if 10000 < val_num < 1e7:
                return pd.to_datetime(val_num, origin="1899-12-30", unit="D").strftime("%Y-%m-%d")
    except Exception:
        pass

    val_str = str(val).strip()

    # Try normal pandas parser
    try:
        dt = pd.to_datetime(val_str, errors="raise")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # Handle Jan'-2025 style
    m = re.match(r"^([A-Za-z]{3})'?-?(\d{4})$", val_str)
    if m:
        month_str, year_str = m.groups()
        try:
            dt = datetime.strptime(f"{month_str} {year_str}", "%b %Y")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    # Handle just a year
    if re.match(r"^\d{4}$", val_str):
        return f"{val_str}-01-01"

    # Handle full month names like "January"
    try:
        dt = datetime.strptime(f"{val_str} {default_year}", "%B %Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_scope_category(sheet_name: str):
    if sheet_name.startswith("Scope 1"):
        scope = 1
    elif sheet_name.startswith("Scope 2"):
        scope = 2
    elif sheet_name.startswith("Scope 3"):
        scope = 3
    else:
        scope = None
    category = sheet_name.replace("Scope", "").strip()
    return scope, category


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Stage1 normalize: normalize dates/currency and add metadata columns."
    )
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Input merged workbook (.xlsx)")
    ap.add_argument("--output", default=DEFAULT_OUTPUT, help="Output normalized workbook (.xlsx)")
    args = ap.parse_args(argv)

    file_path = args.input
    output_path = args.output

    # Load sheet names
    sheets = pd.ExcelFile(file_path).sheet_names

    # Company → Country mapping (unpivot)
    company_info_raw = pd.read_excel(file_path, sheet_name="Company Information")
    company_info_melted = company_info_raw.melt(
        id_vars=["Source_File"], var_name="subsidiary_name", value_name="country"
    )
    company_info_filtered = (
        company_info_melted[
            (company_info_melted["subsidiary_name"] != "Name of company gathering data")
            & (company_info_melted["country"] != "Geographic location of company")
        ]
        .dropna(subset=["country"])
        .reset_index(drop=True)
    )

    ignore_sheets = ["Readme", "Company Information", "Water Tracker"]
    process_sheets = [s for s in sheets if s not in ignore_sheets]

    with ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet in process_sheets:
            df = pd.read_excel(file_path, sheet_name=sheet)

            if "Source_File" not in df.columns:
                df.to_excel(writer, sheet_name=sheet[:31], index=False)
                continue

            original_cols = df.columns.tolist()

            # --- Date normalization ---
            date_cols = [c for c in df.columns if any(p in c.lower() for p in date_patterns)]
            for col in date_cols:
                df[col] = df[col].apply(parse_date_value)

            # --- Currency normalization ---
            currency_cols = [c for c in df.columns if "currency" in c.lower()]
            for col in currency_cols:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .map(currency_map)
                    .fillna(df[col])
                )

            # Join with company info
            merged = df.merge(company_info_filtered, on="Source_File", how="left")

            # Metadata
            scope, category = parse_scope_category(sheet)
            merged["scope"] = scope
            merged["scope_category"] = category
            merged["ghg_category"] = category

            if "description_raw" not in merged.columns:
                merged["description_raw"] = ""

            if "currency" not in merged.columns:
                merged["currency"] = ""

            # Always create co2e column as empty
            merged["co2e"] = ""

            if "unit_raw" not in merged.columns:
                unit_cols = [c for c in merged.columns if re.search(r"\bunit\b", str(c), flags=re.I)]
                merged["unit_raw"] = merged[unit_cols[0]] if unit_cols else ""

            if "amount_raw" not in merged.columns:
                amt_cols = [
                    c
                    for c in df.columns
                    if any(
                        k in str(c)
                        for k in [
                            "Consumption",
                            "Payment",
                            "Spend",
                            "Headcount",
                            "Usage",
                            "Amount",
                            "Quantity",
                            "Volume",
                        ]
                    )
                ]
                merged["amount_raw"] = merged[amt_cols[0]] if amt_cols else ""

            for col, val in [
                ("ef_id", ""),
                ("ef_name", ""),
                ("ef_unit", ""),
                ("mapping_status", "pending"),
                ("mapping_confidence", ""),
                ("mapped_by", "system"),
                ("mapped_date", pd.Timestamp.now().strftime("%Y-%m-%d")),
            ]:
                if col not in merged.columns:
                    merged[col] = val

            if "source_id" not in merged.columns:
                merged["source_id"] = [f"{sheet.replace(' ', '_')}_{i}" for i in range(len(merged))]

            new_cols_order = [
                "subsidiary_name",
                "country",
                "scope",
                "scope_category",
                "ghg_category",
                "description_raw",
                "currency",
                "co2e",
                "unit_raw",
                "amount_raw",
                "ef_id",
                "ef_name",
                "ef_unit",
                "mapping_status",
                "mapping_confidence",
                "mapped_by",
                "mapped_date",
                "source_id",
            ]
            final_cols = original_cols + [c for c in new_cols_order if c not in original_cols]

            merged[final_cols].to_excel(writer, sheet_name=sheet[:31], index=False)

    print(f" Done: {output_path}")


if __name__ == "__main__":
    main()
