# This code has been written by Florian Demir (Sustainability Data Analyst)
# Translates only specific rows & specific columns in multiple sheets, keeps all sheets in output
# Python version: 3.13.3

from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd
from deep_translator import GoogleTranslator


# Defaults preserved; can be overridden via CLI
DEFAULT_INPUT = r"C:\Users\FlorianDemir\Desktop\Desktop- August\normalized_emission_factor_mapping_with_spend_euro_20260225_145241.xlsx"
DEFAULT_OUTPUT = rf"engine/stage1_preprocess/Datas\normalized_emission_factor_mapping_translated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


TARGET_COMPANIES = {"MC Prefab", "Mecwide Nordics", "DC Piping", "Velox"}

TRANSLATION_COLUMNS_BY_SHEET = {
    "Scope 3 Category 1 Purchased Goods & Services": ("Description", "Category"),
    "Scope 1 Fuel Usage": ("Vehicle Type",),
}

COMPANY_COLUMNS = (
    "Company",
    "company",
    "Company_Name",
    "company_name",
    "subsidiary_name",
    "Subsidiary",
    "subsidiary",
)
SOURCE_FILE_COLUMNS = ("Source_File", "Source file", "source_file", "Source file name", "Source")


def _clean_company_identifier(value: object) -> str:
    raw = str(value or "").strip()
    raw = raw.replace("\u00A0", " ")
    for suffix in (".xlsx", ".xls"):
        if raw.lower().endswith(suffix):
            raw = raw[: -len(suffix)]
            break
    return " ".join(raw.split())


def _find_first_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> object | None:
    lookup = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        col = lookup.get(candidate.strip().lower())
        if col is not None:
            return col
    return None


def _company_mask(df: pd.DataFrame, company_name: str) -> pd.Series:
    wanted = _clean_company_identifier(company_name)
    company_col = _find_first_column(df, COMPANY_COLUMNS)
    if company_col is not None:
        return df[company_col].map(_clean_company_identifier).eq(wanted)

    source_col = _find_first_column(df, SOURCE_FILE_COLUMNS)
    if source_col is not None:
        return df[source_col].map(_clean_company_identifier).eq(wanted)

    return pd.Series([True] * len(df), index=df.index)


def run_translation(df: pd.DataFrame, sheet_name: str, company_name: str) -> pd.DataFrame:
    sheet_key = str(sheet_name or "").strip()
    resolved_company = _clean_company_identifier(company_name)
    columns_to_translate = TRANSLATION_COLUMNS_BY_SHEET.get(sheet_key)

    if resolved_company not in TARGET_COMPANIES or not columns_to_translate:
        out = df.copy()
        out.attrs["translation_rows_affected"] = 0
        return out

    df_translated = df.copy()
    translator = GoogleTranslator(source="auto", target="en")
    condition = _company_mask(df, resolved_company)
    affected_rows = pd.Series([False] * len(df), index=df.index)

    for col in columns_to_translate:
        if col in df.columns:
            mask = condition & df[col].notna() & (df[col].astype(str).str.strip() != "")
            if mask.any():
                values_to_translate = df.loc[mask, col].astype(str).tolist()
                try:
                    translated = translator.translate_batch(values_to_translate)
                    df_translated.loc[mask, col] = translated
                    affected_rows = affected_rows | mask
                except Exception:
                    pass

    df_translated.attrs["translation_rows_affected"] = int(affected_rows.sum())
    return df_translated


def main(argv=None):
    ap = argparse.ArgumentParser(description="Stage1 translate: translate selected columns for selected files/sheets.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Input workbook (.xlsx)")
    ap.add_argument("--output", default=DEFAULT_OUTPUT, help="Output translated workbook (.xlsx)")
    args = ap.parse_args(argv)

    input_path = args.input
    output_path = args.output

    xls = pd.ExcelFile(input_path)
    translated_sheets: dict[str, pd.DataFrame] = {}

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df_translated = df.copy()
        for company in TARGET_COMPANIES:
            df_translated = run_translation(df_translated, sheet_name, company)
        translated_sheets[sheet_name] = df_translated

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sh, dff in translated_sheets.items():
            dff.to_excel(writer, sheet_name=sh, index=False)

    print(f" Translation completed. File saved to: {output_path}")


if __name__ == "__main__":
    main()
