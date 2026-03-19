import pandas as pd
from pathlib import Path

# === FILE PATH ===
file_path = Path(
    r"C:\Users\FlorianDemir\Desktop\Desktop- August\Datas\Exchange Rates- European Central Bank\Exchange_Rates_European_Central_Bank.xlsx"
)

# === LOAD EXCEL ===
df = pd.read_excel(file_path)

# === PARSE DATE COLUMN ===
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# === FILTER ONLY 2026 ===
df_2026 = df[df["Date"].dt.year == 2026].copy()

print(f"Total rows for 2026: {len(df_2026)}")

# === IDENTIFY CURRENCY COLUMNS ===
currency_cols = [c for c in df_2026.columns if c != "Date"]

# === CONVERT TO NUMERIC (SAFETY) ===
df_2026[currency_cols] = df_2026[currency_cols].apply(
    pd.to_numeric, errors="coerce"
)

# === CALCULATE 2026 DAILY AVERAGE (ECB FORMAT: 1 EUR = X CUR) ===
avg_2026_rates = (
    df_2026[currency_cols]
    .mean()
    .reset_index()
    .rename(columns={"index": "Currency", 0: "ECB_Avg_2026"})
)

# === INVERT RATES (1 CUR = X EUR) ===
avg_2026_rates["Rate_To_EUR_2026"] = 1 / avg_2026_rates["ECB_Avg_2026"]

# === DROP INVALID / LEGACY CURRENCIES ===
avg_2026_rates = avg_2026_rates.dropna(subset=["Rate_To_EUR_2026"])

# === PRINT RESULTS ===
print("\n2026 Average Exchange Rates (1 unit of currency → EUR):\n")
for _, row in avg_2026_rates.iterrows():
    print(f"{row['Currency']}: {row['Rate_To_EUR_2026']:.6f}")

# === FINAL DICT FOR SPEND CONVERSION ===
exchange_rates_2026 = dict(
    zip(
        avg_2026_rates["Currency"],
        avg_2026_rates["Rate_To_EUR_2026"]
    )
)

# === EUR SAFETY ===
exchange_rates_2026["EUR"] = 1.0
exchange_rates_2026["EURO"] = 1.0
exchange_rates_2026["€"] = 1.0

print("\nFinal exchange_rates_2026 dictionary ready for use.")
