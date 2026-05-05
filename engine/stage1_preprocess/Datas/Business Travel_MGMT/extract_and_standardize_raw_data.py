import pandas as pd
import os
import traceback

# Input and output file paths
input_path = r"C:\Users\FlorianDemir\Desktop\Business Travel_MGMT\January 2025(WholeYear)\CTS Nordics Travel Mgmt Report_travellers_2025_whole_year_source.xlsb"
output_dir = r"C:\Users\FlorianDemir\Desktop\Business Travel_MGMT\January 2025(WholeYear)"
output_path = os.path.join(output_dir, "source Raw Data.xlsx")

# Sheet name (change if necessary)
sheet_name = "source"

required_columns = [
    'Inv Date', 'Area', 'Invoice Number', 'Booking Number', 'Product group', 'Article Name ',
    'Customer Number', 'Month', 'Quarter', 'Year', 'Number of One Way Tickets', 'Number of Products',
    'Number of Travel Days', 'Service Fee', 'Sales Amount Incl. Tax', 'Km Total', 'VAT',
    'Total Amount Incl. VAT', 'Total Amount', 'Tax Amount', 'Number of Nights', 'Hotel EQ Amount',
    'Number of Segments', 'Number of Days (Car)', 'Car EQ Amount', 'Kg CO2', 'Cost Center',
    'Airline Name', 'Travel Route', 'Order Time', 'Departure date', 'Return Date',
    'Origin Name', 'Origin Country', 'Destination Name', 'Destination Country', 'Service Class Group',
    'Booking Channel', 'From - To City Grouped', 'Check In Date', 'Check Out Date', 'Hotel Name',
    'Hotel Brand', 'Hotel Chain', 'Rate Code', 'Hotel Destination', 'Hotel Country', 'Order by',
    'Days booked in adv.', 'Car Chain', 'Car Country', 'Car Destination', 'Booking Class Flexibility',
    'Traveler Name',
]


def _normalize_header(value):
    return " ".join(str(value or "").strip().lower().split())


def _read_xlsb_source(path):
    if not str(path).lower().endswith(".xlsb"):
        raise RuntimeError("Only .xlsb files are allowed for Travel uploads.")
    try:
        return pd.read_excel(path, sheet_name=sheet_name, engine="pyxlsb")
    except Exception as exc:
        print(f"[TRAVEL] Failed to read .xlsb file: {exc}")
        print(traceback.format_exc())
        raise RuntimeError("Failed to read .xlsb file. Ensure file is valid or convert to .xlsx.") from exc


def _validate_required_headers(df):
    actual = {_normalize_header(col) for col in df.columns}
    missing = [col for col in required_columns if _normalize_header(col) not in actual]
    if missing:
        raise RuntimeError("Travel source is missing required columns: " + ", ".join(missing))


data = _read_xlsb_source(input_path)
_validate_required_headers(data)

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