from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


# === SETTINGS (defaults preserved; can be overridden via CLI) ===
DEFAULT_INPUT = "normalized_emission_factor_mapping_final_20260225_145153.xlsx"
DEFAULT_OUTPUT = f"normalized_emission_factor_mapping_with_spend_euro_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


# === ECB 2026 AVERAGE EXCHANGE RATES (1 unit currency -> EUR) ===
exchange_rates = {
    "USD": 0.854446,
    "JPY": 0.005428,
    # "BGN": 0.511300,  # unchanged (not provided in 2026 list)
    "CZK": 0.041082,
    "DKK": 0.133846,
    "GBP": 1.151485,
    "HUF": 0.002630,
    "PLN": 0.235911,
    "RON": 0.196217,
    "SEK": 0.093199,
    "CHF": 1.089446,
    "ISK": 0.006911,
    "NOK": 0.088556,
    "TRY": 0.019421,
    "AUD": 0.596991,
    "BRL": 0.164307,
    "CAD": 0.622703,
    "CNY": 0.123802,
    "HKD": 0.109286,
    "IDR": 0.000050,
    "ILS": 0.275919,
    "INR": 0.009283,
    "KRW": 0.000581,
    "MXN": 0.048751,
    "MYR": 0.215452,
    "NZD": 0.502931,
    "PHP": 0.014397,
    "SGD": 0.669800,
    "THB": 0.026880,
    "ZAR": 0.052100,

    # === EUR SAFETY ===
    "EUR": 1.0,
    "EURO": 1.0,
    "€": 1.0,
    "EUROS": 1.0,
}

# === HELPERS ===
def to_numeric_spend(x):
    try:
        if pd.isna(x):
            return None
        if isinstance(x, (int, float)):
            return float(x)

        s = str(x).strip().replace(" ", "")

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]

        if s.count(",") == 1 and s.count(".") > 0 and s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        elif s.count(".") == 1 and s.count(",") > 0 and s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        elif s.count(",") == 1 and s.count(".") == 0:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")

        return float(s)
    except Exception:
        return None



def normalize_currency(raw):
    if raw is None or pd.isna(raw):
        return None

    s = str(raw).upper()

    if "€" in s:
        return "EUR"
    if "£" in s:
        return "GBP"

    m = re.search(
        r"\b(USD|JPY|BGN|CZK|DKK|GBP|HUF|PLN|RON|SEK|CHF|ISK|NOK|TRY|AUD|BRL|CAD|CNY|HKD|IDR|ILS|INR|KRW|MXN|MYR|NZD|PHP|SGD|THB|ZAR|EUR|EURO|EUROS|€|)\b",
        s,
    )
    return m.group(1) if m else None


def main(argv=None):
    ap = argparse.ArgumentParser(description="Stage1 currency: add Spend_Euro using fixed FX rates.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Input normalized workbook (.xlsx)")
    ap.add_argument("--output", default=DEFAULT_OUTPUT, help="Output workbook (.xlsx)")
    args = ap.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)

    print("Using fixed exchange rates (per 1 unit):")
    for k, v in exchange_rates.items():
        if k != "€":
            print(f"1 {k} = {v:.6f} EUR")

    xls = pd.ExcelFile(input_path)
    writer = pd.ExcelWriter(output_path, engine="openpyxl")

    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        sheet_key = sheet.strip().lower()

        # === SPECIAL CASE: Scope 3 Cat 15 Pensions ===
        if sheet_key == "scope 3 cat 15 pensions":
            spend_col = next(
                (c for c in df.columns if "employer payment to pension provider" in str(c).lower()),
                None,
            )
            currency_col = next((c for c in df.columns if str(c).strip().lower() == "currency"), None)

            if not spend_col or not currency_col:
                print(f"{sheet}: required columns not found")
                df["Spend_Euro"] = pd.NA
                df.to_excel(writer, sheet_name=sheet, index=False)
                continue

        # === SPECIAL CASE: Scope 3 Cat 6 Business Travel ===
        elif sheet_key == "scope 3 cat 6 business travel":
            spend_col = next((c for c in df.columns if str(c).strip().lower() == "spend"), None)
            currency_col = next((c for c in df.columns if str(c).strip().lower() == "spend currency"), None)

            if not spend_col or not currency_col:
                print(f"{sheet}: Spend or Spend Currency not found")
                df["Spend_Euro"] = pd.NA
                df.to_excel(writer, sheet_name=sheet, index=False)
                continue

        # === DEFAULT CASE ===
        else:
            spend_col = next(
                (c for c in df.columns if "spend" in str(c).lower() and "euro" not in str(c).lower()),
                None,
            )
            currency_col = next((c for c in df.columns if "currency" in str(c).lower()), None)

            if not spend_col or not currency_col:
                print(f"{sheet}: Spend or Currency column not found -> skipped")
                df["Spend_Euro"] = pd.NA
                df.to_excel(writer, sheet_name=sheet, index=False)
                continue

        print(f"{sheet} -> using spend_col='{spend_col}', currency_col='{currency_col}'")

        spend_euro_vals = []
        for _, row in df.iterrows():
            spend = to_numeric_spend(row[spend_col])
            curr_code = normalize_currency(row[currency_col])

            if spend is None or curr_code is None:
                spend_euro_vals.append(pd.NA)
                continue

            rate = exchange_rates.get(curr_code)
            spend_euro_vals.append(spend * rate if rate else pd.NA)

        df["Spend_Euro"] = spend_euro_vals
        df.to_excel(writer, sheet_name=sheet, index=False)
        print(f"Processed sheet: {sheet}")

    writer.close()
    print(f"\nDone. Output saved to {output_path}")


if __name__ == "__main__":
    main()
