from pathlib import Path
import pandas as pd
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR

def main() -> None:
    out_dir = STAGE2_OUTPUT_DIR
    files = sorted(out_dir.glob("mapped_results*.xlsx"), key=lambda p: p.stat().st_mtime)
    if not files:
        print("No mapped_results file found")
        return
    p = files[-1]
    xls = pd.ExcelFile(p)
    names = [n for n in xls.sheet_names if n.lower().startswith("scope 3 cat 12 end of life")]
    print(f"Workbook: {p.name}")
    print(f"Cat12 sheet(s): {names}")
    if not names:
        return
    name = names[0]
    df = pd.read_excel(p, sheet_name=name)
    cols = [c for c in [
        "Product weight (including packaging, if available)",
        "weight unit",
        "Weight_tonnes",
        "ef_value",
        "co2e",
    ] if c in df.columns]
    print("Columns present:", cols)
    print(df[cols].head(10))

if __name__ == "__main__":
    main()


