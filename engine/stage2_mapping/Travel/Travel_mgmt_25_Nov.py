# This code has been written by Florian Demir (Sustainability Data Analyst)
# Python version 3.13.3
# This code has been written for travel MGMT data from all the companies.

import os
from pathlib import Path
import sys
from typing import List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_TRAVEL_DIR

INPUT_FOLDER = os.path.join(str(STAGE2_TRAVEL_DIR), "analysis_summary.xlsx")
OUTPUT_FOLDER = os.path.join(str(STAGE2_TRAVEL_DIR), "analysis_summary_without_hotels.xlsx")

KEEP_COLUMNS = [
    "Source_file",
    "Travel Date",
    "Reporting Period",
    "Number of Days",
    "Hotel Chain",
    "Hotel Brand",
    "Car Chain",
    "GHG category",
]


def pull_the_correct_data() -> pd.DataFrame:
    try:
        collect = pd.ExcelFile(INPUT_FOLDER)
    except Exception:
        return pd.DataFrame()

    correct_data: List[pd.DataFrame] = []
    for sheet_name in collect.sheet_names:
        try:
            df_sheet = pd.read_excel(collect, sheet_name=sheet_name)
        except Exception:
            continue
        if df_sheet is None or df_sheet.empty:
            continue
        df_sheet["Source_file"] = sheet_name
        correct_data.append(df_sheet)

    if not correct_data:
        return pd.DataFrame()
    return pd.concat(correct_data, ignore_index=True)


def main() -> None:
    df = pull_the_correct_data()
    if df.empty:
        print("No travel data found to process.")
        return

    available_columns = [col for col in KEEP_COLUMNS if col in df.columns]
    df_clean = df[available_columns].copy()

    subsidiary_replace = {"CTS Dublin": "CTS-VDC Services"}
    if "Source_file" in df_clean.columns:
        df_clean["Source_file"] = df_clean["Source_file"].replace(subsidiary_replace)

    os.makedirs(Path(OUTPUT_FOLDER).parent, exist_ok=True)
    df_clean.to_excel(OUTPUT_FOLDER, index=False)
    print(f"Cleaned travel data saved to: {OUTPUT_FOLDER}")


if __name__ == "__main__":
    main()




