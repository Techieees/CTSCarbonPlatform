import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_EXCHANGE_RATE_WORKBOOK

# === FILE PATH ===
file_path = STAGE1_EXCHANGE_RATE_WORKBOOK

# === LOAD FILE ===
df = pd.read_excel(file_path)

# === PARSE DATE COLUMN ===
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# === FILTER ONLY 2026 ===
df_2026 = df[df["Date"].dt.year == 2026]

print(f"Total rows for 2026: {len(df_2026)}")

# === IDENTIFY CURRENCY COLUMNS ===
currency_cols = [c for c in df_2026.columns if c != "Date"]

# === CALCULATE 2026 AVERAGE PER CURRENCY ===
avg_2026_rates = (
    df_2026[currency_cols]
    .mean(numeric_only=True)
    .reset_index()
    .rename(columns={"index": "Currency", 0: "Avg_Rate_2026"})
)

# === PRINT RESULTS ===
print("\n2026 Average ECB Exchange Rates (EUR base):\n")
for _, row in avg_2026_rates.iterrows():
    print(f"{row['Currency']}: {row['Avg_Rate_2026']:.6f}")

# === OPTIONAL: DICT FOR LATER USE ===
exchange_rates_2026 = dict(
    zip(avg_2026_rates["Currency"], avg_2026_rates["Avg_Rate_2026"])
)

# EUR safety
exchange_rates_2026["EUR"] = 1.0
exchange_rates_2026["EURO"] = 1.0
exchange_rates_2026["€"] = 1.0
