import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR

p = STAGE2_OUTPUT_DIR / "mapped_results.xlsx"
xl = pd.ExcelFile(p)
rows = []
for sh in xl.sheet_names:
    try:
        df = pd.read_excel(p, sheet_name=sh)
    except Exception:
        continue
    # mapping detection
    n = len(df)
    if n == 0:
        continue
    has_any_ref = pd.Series([False]*n)
    for c in ['ef_id','ef_name','match_method','ef_source']:
        if c in df.columns:
            has_any_ref = has_any_ref | df[c].notna()
    if 'ef_value' in df.columns:
        has_any_ref = has_any_ref | df['ef_value'].notna()
    mapped = int(has_any_ref.sum())
    if mapped == 0:
        continue  # skip zero-mapped sheets per request
    pct = mapped / n * 100.0
    rows.append((sh, mapped, n, pct))

# print results preserving sheet order
for sh, mapped, n, pct in rows:
    print(f"{sh}: {mapped}/{n} = {pct:.1f}%")

# overall excluding zero-mapped sheets
if rows:
    tot_mapped = sum(m for _, m, _, _ in rows)
    tot_n = sum(n for _, _, n, _ in rows)
    overall = tot_mapped / tot_n * 100.0
    print(f"\nOVERALL (excluding zero-mapped sheets): {tot_mapped}/{tot_n} = {overall:.1f}%")
else:
    print('No sheets with any mapped rows.')
