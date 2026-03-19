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

# === FILTER ONLY 2025 ===
df_2025 = df[df["Date"].dt.year == 2025].copy()

print(f"Total rows for 2025: {len(df_2025)}")

# === IDENTIFY CURRENCY COLUMNS ===
currency_cols = [c for c in df_2025.columns if c != "Date"]

# === CONVERT TO NUMERIC (SAFETY) ===
df_2025[currency_cols] = df_2025[currency_cols].apply(
    pd.to_numeric, errors="coerce"
)

# === CALCULATE 2025 DAILY AVERAGE (ECB FORMAT: 1 EUR = X CUR) ===
avg_2025_rates = (
    df_2025[currency_cols]
    .mean()
    .reset_index()
    .rename(columns={"index": "Currency", 0: "ECB_Avg_2025"})
)

# === INVERT RATES (1 CUR = X EUR) ===
avg_2025_rates["Rate_To_EUR_2025"] = 1 / avg_2025_rates["ECB_Avg_2025"]

# === DROP INVALID / LEGACY CURRENCIES ===
avg_2025_rates = avg_2025_rates.dropna(subset=["Rate_To_EUR_2025"])

# === PRINT RESULTS ===
print("\n2025 Average Exchange Rates (1 unit of currency → EUR):\n")
for _, row in avg_2025_rates.iterrows():
    print(f"{row['Currency']}: {row['Rate_To_EUR_2025']:.6f}")

# === FINAL DICT FOR SPEND CONVERSION ===
exchange_rates_2025 = dict(
    zip(
        avg_2025_rates["Currency"],
        avg_2025_rates["Rate_To_EUR_2025"]
    )
)

# === EUR SAFETY ===
exchange_rates_2025["EUR"] = 1.0
exchange_rates_2025["EURO"] = 1.0
exchange_rates_2025["€"] = 1.0

print("\nFinal exchange_rates_2025 dictionary ready for use.")
