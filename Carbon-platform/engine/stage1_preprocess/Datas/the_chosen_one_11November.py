# Python 3.10+
# Requirements: pandas, openpyxl, os, datetime
# Author: Florian Demir (Sustainability Data Analyst)


import os
import re
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import argparse

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_INPUT_DIR, STAGE1_OUTPUT_DIR


# Stage 1 defaults now resolve from DATA_DIR so the same script works in every
# environment while keeping the existing CLI override behaviour.
INPUT_FOLDER = str(STAGE1_INPUT_DIR)
OUTPUT_FILE = os.path.join(
    str(STAGE1_OUTPUT_DIR),
    f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
)

MAX_ROWS_PER_SHEET = 1_048_576
SHEET_NAME_MAXLEN = 31

# Soft cleaning settings  
EMPTY_COL_THRESHOLD = 1.0  # keep column if it is NOT 100% empty  
MIN_NON_NULL_PER_ROW = 1   # keep row if it has at least 1 non-null cell

INVALID_SHEET_CHARS = r'[:\\/\?\*\[\]]'


def safe_sheet_name(name: str) -> str:
    name = re.sub(INVALID_SHEET_CHARS, "", str(name))
    name = name.strip()
    if not name:
        name = "Sheet"
    return name[:SHEET_NAME_MAXLEN]


def write_in_chunks(df: pd.DataFrame, writer: pd.ExcelWriter, base_name: str) -> None:
    if df.empty:
        df.to_excel(writer, sheet_name=base_name, index=False)
        return
    total = len(df)
    if total <= MAX_ROWS_PER_SHEET:
        df.to_excel(writer, sheet_name=base_name, index=False)
        return

    start = 0
    part = 1
    while start < total:
        end = min(start + MAX_ROWS_PER_SHEET, total)
        chunk = df.iloc[start:end]
        suffix = "" if part == 1 else f"_p{part}"
        sheet_name = safe_sheet_name(f"{base_name}{suffix}")
        chunk.to_excel(writer, sheet_name=sheet_name, index=False)
        start = end
        part += 1


def normalize_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string")
            df[col] = df[col].str.strip()
            df[col] = df[col].replace("", pd.NA)
    df = df.infer_objects(copy=False)
    return df


def clean_sheet_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_whitespace(df)
    df = df.dropna(how='all')
    df = df.dropna(axis=1, how='all')
    return df


def post_merge_tidy(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df.columns = [str(c).strip() for c in df.columns]

    # Keep columns that are not 100% empty
    keep_cols = []
    for c in df.columns:
        if c == "Source_File":
            keep_cols.append(c)
            continue
        null_ratio = df[c].isna().mean()
        if null_ratio < EMPTY_COL_THRESHOLD:
            keep_cols.append(c)
    df = df[keep_cols]

    # Keep rows with at least 1 non-null value
    non_null_counts = df.notna().sum(axis=1)
    df = df[non_null_counts >= MIN_NON_NULL_PER_ROW]

    df = normalize_whitespace(df)

    # Do NOT remove duplicates at this stage
    # df = df.drop_duplicates(ignore_index=True)

    # Move Source_File to the left
    if "Source_File" in df.columns:
        other = [c for c in df.columns if c != "Source_File"]
        df = df[["Source_File"] + other]

    # Attempt numeric conversion
    for c in df.columns:
        if c == "Source_File":
            continue
        try:
            df[c] = pd.to_numeric(df[c], errors="ignore")
        except Exception:
            pass

    return df


def main(argv=None):
    global INPUT_FOLDER, OUTPUT_FILE
    ap = argparse.ArgumentParser(description="Stage1 merge: merge input Excel files into one workbook.")
    ap.add_argument("--input-folder", default=INPUT_FOLDER, help="Folder containing input .xlsx/.xlsm files")
    ap.add_argument("--output-file", default=OUTPUT_FILE, help="Output .xlsx path")
    args = ap.parse_args(argv)
    INPUT_FOLDER = args.input_folder
    OUTPUT_FILE = args.output_file

    all_sheets: dict[str, list[pd.DataFrame]] = {}

    for file in os.listdir(INPUT_FOLDER):
        if not file.lower().endswith((".xlsx", ".xlsm")):
            continue
        file_path = os.path.join(INPUT_FOLDER, file)
        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            print(f"Skip file (read error): {file} -> {e}")
            continue

        for sheet in xls.sheet_names:
            try:
                if "scope 3 cat 11 products" in sheet.lower():
                    df = xls.parse(sheet, dtype=object, skiprows=9)
                else:
                    df = xls.parse(sheet, dtype=object)
            except Exception as e:
                print(f"Skip sheet (read error): {file} | {sheet} -> {e}")
                continue

            df = clean_sheet_raw(df)
            if df.empty:
                continue

            df["Source_File"] = file
            all_sheets.setdefault(sheet, []).append(df)

    if not all_sheets:
        print("No data found to merge. Nothing written.")
        return

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for raw_sheet_name, df_list in all_sheets.items():
            merged_df = pd.concat(df_list, ignore_index=True)

            print(f"{raw_sheet_name}: {len(merged_df)} rows before tidy")
            merged_df = post_merge_tidy(merged_df)
            print(f"{raw_sheet_name}: {len(merged_df)} rows after tidy")

            base_name = safe_sheet_name(raw_sheet_name)
            write_in_chunks(merged_df, writer, base_name)

    print(f"Merged Excel file created: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
