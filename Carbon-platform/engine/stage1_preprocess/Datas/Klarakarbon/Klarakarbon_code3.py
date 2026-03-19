# Python 3.10+
# Requirements: pandas, openpyxl, os, datetime
# Author: Florian Demir (Sustainability Data Analyst)

import pandas as pd
import os
import sys
from datetime import datetime
from openpyxl import load_workbook
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_KLARAKARBON_INPUT_DIR, STAGE1_KLARAKARBON_OUTPUT_DIR


# Define folders
input_folder = str(STAGE1_KLARAKARBON_INPUT_DIR)
output_folder = str(STAGE1_KLARAKARBON_OUTPUT_DIR)
output_file = os.path.join(output_folder, f"combined_klarakarbon_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")



#NordicEPOD - YTD 2 December
#"C:\Users\FlorianDemir\Desktop\Klarakarbon 15 December\NordicEPOD Klarakarbon export YTD 2DEC2025.xlsx"
#GAPIT YTD 
#"C:\Users\FlorianDemir\Desktop\Klarakarbon 15 December\Emissions - Gapit - 18.11.25 (1).xlsx"
#NEP Switchboards
#"C:\Users\FlorianDemir\Desktop\Klarakarbon 15 December\Klarakarbon NEP SWB 171125 (2).xlsx"
#Fortica
# Fortica has to be done 
#"C:\Users\FlorianDemir\Desktop\Klarakarbon 15 December\Fortica Klara karbon report October (1).xlsx"
#"C:\Users\FlorianDemir\Desktop\Klarakarbon 15 December\Fortica Klarakarbon export received 22OCT2025 (1).xlsx"

# Fortica Merged and cleaned
# "C:\Users\FlorianDemir\Desktop\Klarakarbon 15 December\Fortica Merged 15 December_DEDUPED_20251215_114931.xlsx"

#GT Nordics Merged and cleaned
#"C:\Users\FlorianDemir\Desktop\Klarakarbon 15 December\Klarakarbon YTD october GTN (1)_DEDUPED_20251215_115700.xlsx"




# List Excel files
file_list = [f for f in os.listdir(input_folder) if f.endswith(".xlsx")]

combined_dataframes = []

for file_name in file_list:
    file_path = os.path.join(input_folder, file_name)
    xls = pd.ExcelFile(file_path)
    
    for sheet_name in xls.sheet_names:
        # Read entire sheet as raw data without headers
        df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        
        # Find the first row that looks like a real header
        for i in range(len(df_raw)):
            row = df_raw.iloc[i]
            if row.notna().sum() > 5:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=i)
                df["source_file"] = file_name
                df["sheet_name"] = sheet_name
                combined_dataframes.append(df)
                break  # go to next sheet
            
            
        for i in reversed(range(len(df_raw))):
            row = df_raw.iloc[i]
            if row.notna().sum() > 3:
                df_end = pd.read_excel(file_path, sheet_name=sheet_name, header= None, skiprows= i+1)
                if not df_end.empty:
                    df_end["source_file"] = file_name
                    df_end["sheet_name"] = sheet_name
                    combined_dataframes.append(df_end)
                break # check the next sheet
        
        
        
    final_df = pd.concat(combined_dataframes, ignore_index=True)
    
    # Save intermediate result after each file
    os.makedirs(output_folder, exist_ok=True)
    final_df.to_excel(output_file, index= False)
    print(f"Processed {file_name}, saved intermediate result.")
    print("Saved to:", output_file)
    print("Merge with the next file")
    
        
            

# Merge all
final_df = pd.concat(combined_dataframes, ignore_index=True)

# Save
os.makedirs(output_folder, exist_ok=True)
final_df.to_excel(output_file, index=False)

print("✓ Merged with advanced header detection.")
print("Saved to:", output_file)
