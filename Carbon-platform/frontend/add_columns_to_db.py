import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import FRONTEND_DB_PATH

db_path = str(FRONTEND_DB_PATH)
columns = [
    ('scope1_emission', 'REAL', 0.0),
    ('scope2_emission', 'REAL', 0.0),
    ('scope3_emission', 'REAL', 0.0),
    ('total_emission', 'REAL', 0.0),
]

table_name = 'form_submission'

conn = sqlite3.connect(db_path)
c = conn.cursor()

for col, typ, default in columns:
    try:
        c.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {typ} DEFAULT {default}")
        print(f"Added column: {col}")
    except Exception as e:
        print(f"Column {col} may already exist or error: {e}")

conn.commit()
conn.close()
print("Done.") 