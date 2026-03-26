import xlwings as xw
import pandas as pd
import os

# Input and output file paths
input_path = r"C:\Users\FlorianDemir\Desktop\Business Travel_MGMT\January 2025(WholeYear)\CTS Nordics Travel Mgmt Report_travellers_2025_whole_year_source.xlsb"
output_dir = r"C:\Users\FlorianDemir\Desktop\Business Travel_MGMT\January 2025(WholeYear)"
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