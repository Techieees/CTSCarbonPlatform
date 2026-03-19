import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR

p = STAGE2_OUTPUT_DIR / "mapped_results.xlsx"
xl = pd.ExcelFile(p)
all_rows = []
for sh in xl.sheet_names:
    try:
        df = pd.read_excel(p, sheet_name=sh)
    except Exception:
        continue
    df['__sheet'] = sh
    # Normalize scope string
    if 'scope' in df.columns:
        sc = df['scope'].astype(str).str.strip()
    else:
        sc = pd.Series([None]*len(df))
    sc = sc.where(~sc.isna() & (sc!='nan'))
    sc = sc.where(~sc.isna(), pd.Series([
        (sh.split()[1] if sh.lower().startswith('scope') and len(sh.split())>1 and sh.split()[1].isdigit() else None)
        for _ in range(len(df))
    ]))
    def norm_scope(x):
        if pd.isna(x):
            return None
        xs = str(x).strip()
        if xs in ['1','2','3']:
            return f'Scope {xs}'
        if xs.lower().startswith('scope'):
            parts = xs.split()
            if len(parts)>=2 and parts[1].isdigit():
                return f'Scope {parts[1]}'
        return xs
    df['__scope'] = sc.map(norm_scope)
    # Numeric fields
    for col in ['emissions_tco2e','Spend_Euro','ef_value']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = pd.Series([pd.NA]*len(df), dtype='float64')
    # Mapping coverage heuristic
    has_any_ref = pd.Series([False]*len(df))
    for c in ['ef_id','ef_name','match_method','ef_source']:
        if c in df.columns:
            has_any_ref = has_any_ref | df[c].notna()
    df['__is_mapped'] = has_any_ref | df['ef_value'].notna()
    all_rows.append(df[['__sheet','__scope','emissions_tco2e','Spend_Euro','__is_mapped']])

if not all_rows:
    print('No data')
    raise SystemExit

d = pd.concat(all_rows, ignore_index=True)
# Filter out rows where both metrics are NaN
nonempty = d.dropna(how='all', subset=['emissions_tco2e','Spend_Euro'])
# Totals
total_emis = nonempty['emissions_tco2e'].sum(skipna=True)
total_spend = nonempty['Spend_Euro'].sum(skipna=True)
# Scope breakdown
by_scope = nonempty.groupby('__scope', dropna=False).agg(
    emissions_tco2e=('emissions_tco2e','sum'),
    Spend_Euro=('Spend_Euro','sum'),
    rows=('__scope','size'),
    mapped=('__is_mapped','sum')
).reset_index().sort_values('emissions_tco2e', ascending=False)
# Percentages
by_scope['emis_pct'] = (by_scope['emissions_tco2e']/total_emis*100) if total_emis not in [0, None, float('nan')] else pd.NA
by_scope['spend_pct'] = (by_scope['Spend_Euro']/total_spend*100) if total_spend not in [0, None, float('nan')] else pd.NA
# Sheet breakdown (top 8 by emissions)
by_sheet = nonempty.groupby('__sheet').agg(
    emissions_tco2e=('emissions_tco2e','sum'),
    Spend_Euro=('Spend_Euro','sum'),
    rows=('__sheet','size')
).reset_index().sort_values('emissions_tco2e', ascending=False)
by_sheet['emis_pct'] = (by_sheet['emissions_tco2e']/total_emis*100) if total_emis not in [0, None, float('nan')] else pd.NA
# Mapping coverage overall
covered = int(d['__is_mapped'].sum())
coverage_pct = covered/len(d)*100 if len(d)>0 else float('nan')

print('TOTALS')
print(f"Total emissions (tCO2e): {total_emis:,.4f}")
print(f"Total spend (EUR): {total_spend:,.2f}")
print(f"Mapping coverage: {covered}/{len(d)} = {coverage_pct:.2f}%")
print('\nBY SCOPE (emissions desc)')
print(by_scope[['__scope','rows','mapped','emissions_tco2e','emis_pct','Spend_Euro','spend_pct']].to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
print('\nTOP SHEETS (by emissions)')
print(by_sheet.head(8)[['__sheet','rows','emissions_tco2e','emis_pct','Spend_Euro']].to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
