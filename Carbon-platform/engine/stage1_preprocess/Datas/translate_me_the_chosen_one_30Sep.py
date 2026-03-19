# This code has been written by Florian Demir (Sustainability Data Analyst)
# Translates only specific rows & specific columns in multiple sheets, keeps all sheets in output
# Python version: 3.13.3

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
from deep_translator import GoogleTranslator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_OUTPUT_DIR, pick_first_existing


# Defaults remain CLI-overridable, but now resolve from the shared data
# directory configured through the environment.
DEFAULT_INPUT = str(
    pick_first_existing(
        STAGE1_OUTPUT_DIR / "stage1_04_currency.xlsx",
        STAGE1_OUTPUT_DIR / "normalized_emission_factor_mapping_with_spend_euro.xlsx",
    )
)
DEFAULT_OUTPUT = str(
    STAGE1_OUTPUT_DIR / f"normalized_emission_factor_mapping_translated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
)


# Target sheets, files, and columns
target_sheet1 = "Scope 3 Cat 1 Services Spend"
target_sheet2 = "Scope 3 Cat 1 Goods Spend"
target_sheet3 = "Scope 1 Fuel Usage Activity"
target_sheet4 = "Scope 3 Cat 8 Fuel Usage Activi"

target_files1 = ["MC Prefab.xlsx", "Mecwide Nordics.xlsx", "DC Piping.xlsx"]
target_files2 = ["MC Prefab.xlsx", "Mecwide Nordics.xlsx", "DC Piping.xlsx", "Velox.xlsx"]
target_files3 = ["Velox.xlsx"]
target_files4 = ["Velox.xlsx"]

columns_to_translate1 = ["Service Provided", "Service Provider Function"]
columns_to_translate2 = ["Product type", "Product description"]
columns_to_translate3 = ["Vehicle Type"]
columns_to_translate4 = ["Vehicle Type"]


def main(argv=None):
    ap = argparse.ArgumentParser(description="Stage1 translate: translate selected columns for selected files/sheets.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Input workbook (.xlsx)")
    ap.add_argument("--output", default=DEFAULT_OUTPUT, help="Output translated workbook (.xlsx)")
    args = ap.parse_args(argv)

    input_path = args.input
    output_path = args.output

    xls = pd.ExcelFile(input_path)
    translator = GoogleTranslator(source="auto", target="en")

    translated_sheets: dict[str, pd.DataFrame] = {}

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)

        # --- Scope 3 Cat 1 Services Spend ---
        if sheet_name == target_sheet1 and "Source_File" in df.columns:
            df_translated = df.copy()
            condition = df["Source_File"].isin(target_files1)
            for col in columns_to_translate1:
                if col in df.columns:
                    mask = condition & df[col].notna()
                    if mask.any():
                        values_to_translate = df.loc[mask, col].astype(str).tolist()
                        try:
                            translated = translator.translate_batch(values_to_translate)
                            df_translated.loc[mask, col] = translated
                        except Exception:
                            pass
            translated_sheets[sheet_name] = df_translated

        # --- Scope 3 Cat 1 Goods Spend ---
        elif sheet_name == target_sheet2 and "Source_File" in df.columns:
            df_translated = df.copy()
            condition = df["Source_File"].isin(target_files2)
            for col in columns_to_translate2:
                if col in df.columns:
                    mask = condition & df[col].notna()
                    if mask.any():
                        values_to_translate = df.loc[mask, col].astype(str).tolist()
                        try:
                            translated = translator.translate_batch(values_to_translate)
                            df_translated.loc[mask, col] = translated
                        except Exception:
                            pass
            translated_sheets[sheet_name] = df_translated

        # --- Scope 1 Fuel Usage Activity ---
        elif sheet_name == target_sheet3 and "Source_File" in df.columns:
            df_translated = df.copy()
            condition = df["Source_File"].isin(target_files3)
            for col in columns_to_translate3:
                if col in df.columns:
                    mask = condition & df[col].notna()
                    if mask.any():
                        values_to_translate = df.loc[mask, col].astype(str).tolist()
                        try:
                            translated = translator.translate_batch(values_to_translate)
                            df_translated.loc[mask, col] = translated
                        except Exception:
                            pass
            translated_sheets[sheet_name] = df_translated

        # --- Scope 3 Cat 8 Fuel Usage Activity ---
        elif sheet_name == target_sheet4 and "Source_File" in df.columns:
            df_translated = df.copy()
            condition = df["Source_File"].isin(target_files4)
            for col in columns_to_translate4:
                if col in df.columns:
                    mask = condition & df[col].notna()
                    if mask.any():
                        values_to_translate = df.loc[mask, col].astype(str).tolist()
                        try:
                            translated = translator.translate_batch(values_to_translate)
                            df_translated.loc[mask, col] = translated
                        except Exception:
                            pass
            translated_sheets[sheet_name] = df_translated

        else:
            translated_sheets[sheet_name] = df

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sh, dff in translated_sheets.items():
            dff.to_excel(writer, sheet_name=sh, index=False)

    print(f" Translation completed. File saved to: {output_path}")


if __name__ == "__main__":
    main()
