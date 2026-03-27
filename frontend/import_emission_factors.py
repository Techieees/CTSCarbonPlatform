import pandas as pd
from app import app, db, EmissionFactor
import json
import numpy as np
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_IMPORT_EMISSION_FACTORS_XLSX


EXCEL_PATH = STAGE2_IMPORT_EMISSION_FACTORS_XLSX


def safe_value(val, default=None):
    if isinstance(val, (pd.Series, np.ndarray)):
        return default
    if pd.isna(val):
        return default
    return val


def find_column(columns, keywords):
    for col in columns:
        for kw in keywords:
            if kw in col:
                return col
    return None


def import_emission_factors():

    xls = pd.ExcelFile(EXCEL_PATH)

    with app.app_context():

        total_inserted = 0

        for sheet_name in xls.sheet_names:

            df = xls.parse(sheet_name)

            if not isinstance(df, pd.DataFrame):
                print(f"Skipping {sheet_name} (not a dataframe)")
                continue

            # normalize column names
            df.columns = [str(c).strip().lower() for c in df.columns]

            factor_col = find_column(df.columns, ["ef_value", "factor", "value"])
            name_col = find_column(df.columns, ["ef_name", "name", "fuel", "type"])
            unit_col = find_column(df.columns, ["ef_unit", "unit"])
            desc_col = find_column(df.columns, ["ef_description", "description", "desc"])
            source_col = find_column(df.columns, ["ef_source", "source"])
            scope_col = find_column(df.columns, ["scope"])
            category_col = find_column(df.columns, ["emission factor category", "category"])

            if not factor_col or not name_col:
                print(f"Skipping {sheet_name} (required columns not found)")
                continue

            inserted_this_sheet = 0

            for _, row in df.iterrows():

                factor_val = safe_value(row[factor_col])

                if factor_val is None:
                    continue

                try:
                    factor = float(factor_val)
                except (ValueError, TypeError):
                    continue

                subcategory = str(safe_value(row[name_col], "")).strip()

                if not subcategory or subcategory.lower() == "nan":
                    continue

                unit = str(safe_value(row[unit_col], "")).strip() if unit_col else None
                description = str(safe_value(row[desc_col], "")).strip() if desc_col else None
                source = str(safe_value(row[source_col], "")).strip() if source_col else None

                try:
                    scope = int(safe_value(row[scope_col], 3)) if scope_col else 3
                except:
                    scope = 3

                ef_category = str(safe_value(row[category_col], sheet_name)).strip() if category_col else sheet_name

                extra_data = json.dumps({
                    col: safe_value(row[col])
                    for col in df.columns
                })

                ef = EmissionFactor(
                    category=ef_category,
                    subcategory=subcategory,
                    factor=factor,
                    unit=unit,
                    year=2025,
                    description=description,
                    extra_data=extra_data
                )

                db.session.add(ef)

                inserted_this_sheet += 1
                total_inserted += 1

            db.session.commit()

            print(f"{sheet_name}: {inserted_this_sheet} factors inserted")

        print("")
        print(f"TOTAL INSERTED: {total_inserted}")


if __name__ == "__main__":

    import_emission_factors()

    print("Emission factors import completed successfully.")