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


# The workbook location is now environment-driven so the same script can run
# locally and in production without editing source code.
EXCEL_PATH = STAGE2_IMPORT_EMISSION_FACTORS_XLSX

def safe_value(val, default=None):
    if isinstance(val, (pd.Series, np.ndarray)):
        return default
    if pd.isna(val):
        return default
    return val

# Automatically detects sheets and column names

def import_emission_factors():
    xls = pd.ExcelFile(EXCEL_PATH)
    with app.app_context():
        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            if not isinstance(df, pd.DataFrame):
                print(f"Sheet {sheet_name} atlandı (DataFrame değil)")
                continue
            # Normalize column names
            df.columns = [str(c).strip().lower() for c in df.columns]
            factor_col = next((c for c in df.columns if 'factor' in c or 'faktor' in c), None)
            subcat_col = next((c for c in df.columns if 'fuel' in c or 'type' in c or 'subcategory' in c or 'mode' in c), None)
            unit_col = next((c for c in df.columns if 'unit' in c), None)
            year_col = next((c for c in df.columns if 'year' in c), None)
            desc_col = next((c for c in df.columns if 'desc' in c or 'açıklama' in c), None)
            if not factor_col or not subcat_col:
                print(f"Sheet {sheet_name} atlandı (gerekli kolonlar yok)")
                continue
            for _, row in df.iterrows():
                factor_val = safe_value(row[factor_col])
                if factor_val is None:
                    continue
                try:
                    factor = float(factor_val)
                    if pd.isna(factor):
                        continue
                except (ValueError, TypeError):
                    continue
                subcat = str(safe_value(row[subcat_col], '')).strip()
                if not subcat or subcat.lower() == 'nan':
                    continue
                unit = str(safe_value(row[unit_col], '')).strip() if unit_col in df.columns else None
                year_val = safe_value(row[year_col], 2025) if year_col in df.columns else 2025
                try:
                    year = int(year_val) if year_val is not None else 2025
                except Exception:
                    year = 2025
                desc = str(safe_value(row[desc_col], '')).strip() if desc_col in df.columns else None
                # Tüm satırı JSON olarak kaydet
                extra_data = json.dumps({col: safe_value(row[col]) for col in df.columns})
                ef = EmissionFactor(
                    category=sheet_name,
                    subcategory=subcat,
                    factor=factor,
                    unit=unit,
                    year=year,
                    description=desc,
                    extra_data=extra_data
                )
                db.session.add(ef)
            db.session.commit()
            print(f"Sheet {sheet_name} için faktörler eklendi.")

if __name__ == "__main__":
    import_emission_factors()
    print("Tüm emisyon faktörleri başarıyla yüklendi.") 