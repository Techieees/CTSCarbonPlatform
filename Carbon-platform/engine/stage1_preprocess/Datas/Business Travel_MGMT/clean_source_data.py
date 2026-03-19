# THIS CODE HAS BEEN WRITTEN BY FLORIAN DEMIR (SUSTAINABILITY DATA ANALYST)
# THIS CODE IS USED TO CLEAN THE SOURCE DATA FOR THE BUSINESS TRAVEL MANAGEMENT REPORT
# PYTHON VERSION 3.13.3


import pandas as pd
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_BUSINESS_TRAVEL_DIR

input_path = os.path.join(str(STAGE1_BUSINESS_TRAVEL_DIR), "source Raw Data.xlsx")
output_path = os.path.join(str(STAGE1_BUSINESS_TRAVEL_DIR), "cleaned_source_Raw_Data.xlsx")

# Only keep the following columns
keep_columns = [
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
    'Traveler Name', 'Scope', 'GHG Category'
]

# Read the file
df = pd.read_excel(input_path)

# Only keep the required columns
df_clean = df[keep_columns].copy()

# Cost Center merge
cost_center_replace = {'CTS DUBLIN': 'CTS-VDC SERVICES', 'CTS IRELAND': 'CTS-VDC SERVICES'}
df_clean['Cost Center'] = df_clean['Cost Center'].replace(cost_center_replace)

# Convert 'Inv Date' to DD/MM/YYYY format
df_clean['Inv Date'] = pd.to_datetime(df_clean['Inv Date'], errors='coerce').dt.strftime('%d/%m/%Y')

# Check if 'Km Total' has negative values
df_clean['Negative Km Total Exists'] = df_clean['Km Total'].apply(lambda x: 'Yes' if pd.notnull(x) and x < 0 else 'No')

# Convert 'Kg CO2' to numeric type
df_clean['Kg CO2'] = pd.to_numeric(df_clean['Kg CO2'], errors='coerce')
# Add CO2 Exists column
df_clean['CO2 Exists'] = df_clean['Kg CO2'].apply(lambda x: 'Yes' if pd.notnull(x) and x > 0 else 'No')

df_clean.to_excel(output_path, index=False)






print(f"Cleaned data saved to: {output_path}") 