import sqlite3
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import FRONTEND_DB_PATH

db_path = str(FRONTEND_DB_PATH)
conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute('SELECT id, category, subcategory, factor, unit, year, description FROM emission_factor')
rows = c.fetchall()


for row in rows:
    id_, category, subcategory, factor, unit, year, description = row
    extra_data = json.dumps({
        'category': category,
        'subcategory': subcategory,
        'factor': factor,
        'unit': unit,
        'year': year,
        'description': description
    })
    c.execute('UPDATE emission_factor SET extra_data = ? WHERE id = ?', (extra_data, id_))

conn.commit()
conn.close()
print('All emission_factor extra_data fields updated.') 