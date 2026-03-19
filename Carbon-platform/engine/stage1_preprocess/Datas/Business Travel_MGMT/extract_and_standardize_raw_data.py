import xlwings as xw
import pandas as pd
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_BUSINESS_TRAVEL_DIR

# Input and output file paths
input_path = os.path.join(str(STAGE1_BUSINESS_TRAVEL_DIR), "CTS Nordics Travel Mgmt Report_travellers_2025_whole_year_source.xlsb")
output_dir = str(STAGE1_BUSINESS_TRAVEL_DIR)
output_path = os.path.join(output_dir, "source Raw Data.xlsx")

# Sheet name (change if necessary)
sheet_name = "source"

# Read the .xlsb file using xlwings
app = xw.App(visible=False)
wb = xw.Book(input_path)
sheet = wb.sheets[sheet_name]
data = sheet.range('A1').expand().options(pd.DataFrame, header=1, index=False).value
wb.close()
app.quit()

df = data.copy()

# Show shape and sample rows for verification
print("Original data shape:", df.shape)
print("First 5 rows:")
print(df.head())
print("Last 5 rows:")
print(df.tail())

# Remove only completely empty rows and columns (do not touch partially filled ones)
df = df.dropna(how='all')
df = df.dropna(axis=1, how='all')
df = df.reset_index(drop=True)

# Add two new columns
df["Scope"] = "Scope 3"
df["GHG Category"] = "Business Travel"

# Create output directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the new file
df.to_excel(output_path, index=False, engine='openpyxl')

print(f"Data successfully saved to: {output_path}")
print("Final data shape:", df.shape) 