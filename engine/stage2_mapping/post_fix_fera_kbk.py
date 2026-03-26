from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional
import glob
import os
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR


BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = STAGE2_OUTPUT_DIR
TARGET_SHEET = "S3 Cat 3 FERA"


def _find_target_clean(base_dir: Path, explicit: Optional[str] = None) -> Optional[Path]:
    if explicit:
        p = Path(explicit)
        if p.suffix.lower() != ".xlsx":
            p = p.with_suffix(".xlsx")
        return p if p.exists() else None
    out = STAGE2_OUTPUT_DIR
    patterns = [
        str(out / "mapped_results_by_ghgp_clean_*.xlsx"),
    ]
    candidates: list[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    candidates.sort(key=os.path.getmtime, reverse=True)
    return Path(candidates[0])


def _get_col(df: pd.DataFrame, name: str) -> Optional[str]:
    # exact match first
    if name in df.columns:
        return name
    # case-insensitive fallback
    lowmap = {str(c).strip().lower(): c for c in df.columns}
    return lowmap.get(name.strip().lower())


def apply_post_fix(explicit_path: Optional[str] = None) -> Optional[Path]:
    src = _find_target_clean(BASE_DIR, explicit_path)
    if src is None:
        print("PostFix: No GHGP clean workbook found.")
        return None
    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(src, sheet_name=None, engine="openpyxl")
    except Exception as exc:
        print(f"PostFix: Failed to read workbook: {exc}")
        return None
    if not all_sheets or TARGET_SHEET not in all_sheets:
        print(f"PostFix: Target sheet '{TARGET_SHEET}' not found.")
        return None
    df = all_sheets[TARGET_SHEET].copy()
    # Columns (exact names as requested)
    col_sheet_booklets = _get_col(df, "Sheet_booklets")
    col_data_source = _get_col(df, "Data Source sheet")
    col_co2e = _get_col(df, "co2e (t)")
    col_co2e1 = _get_col(df, "co2e (t).1")
    col_co2e2 = _get_col(df, "co2e (t) 2")
    if not all([col_sheet_booklets, col_data_source, col_co2e, col_co2e1]):
        print("PostFix: Required columns are missing; no changes applied.")
    else:
        try:
            # 1) Keep the previous behavior (Klarakarbon overlay) but also
            # 2) Fix the common issue: co2e (t) is 0 while values exist in co2e (t).1 / co2e (t) 2
            sb = df[col_sheet_booklets].astype(str).str.strip()
            ds = df[col_data_source].astype(str).str.strip()

            # Numeric coercions (treat non-numeric as NaN)
            base = pd.to_numeric(df[col_co2e], errors="coerce")
            src1 = pd.to_numeric(df[col_co2e1], errors="coerce")
            src2 = pd.to_numeric(df[col_co2e2], errors="coerce") if col_co2e2 else None

            # Fill where base is missing/zero using co2e (t) 2 first, then co2e (t).1
            need = base.isna() | (base == 0)
            if col_co2e2 and src2 is not None:
                take2 = need & src2.notna() & (src2 != 0)
                if bool(getattr(take2, "any", lambda: False)()):
                    df.loc[take2, col_co2e] = src2.loc[take2].values
                need = pd.to_numeric(df[col_co2e], errors="coerce").isna() | (pd.to_numeric(df[col_co2e], errors="coerce") == 0)
            take1 = need & src1.notna() & (src1 != 0)
            if bool(getattr(take1, "any", lambda: False)()):
                df.loc[take1, col_co2e] = src1.loc[take1].values

            # Preserve the old special-case (still useful if only Klarakarbon should override)
            mask_kbk = (sb == "Scope 3 Cat 3 FERA Fuel") & (ds == "Klarakarbon")
            if bool(getattr(mask_kbk, "any", lambda: False)()):
                df.loc[mask_kbk, col_co2e] = df.loc[mask_kbk, col_co2e1].values
        except Exception as exc:
            print(f"PostFix: Failed to apply overlay: {exc}")
    all_sheets[TARGET_SHEET] = df
    # Write new file
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        out_path = OUT_DIR / f"mapped_results_by_ghgp_clean_postfix_{ts}.xlsx"
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for name, sdf in all_sheets.items():
                safe = name[:31] if len(name) > 31 else name
                sdf.to_excel(writer, sheet_name=safe, index=False)
        print(f"PostFix: Wrote workbook: {out_path.name}")
        return out_path
    except Exception as exc:
        print(f"PostFix: Failed to write workbook: {exc}")
        return None


def main() -> None:
    # You can pass an explicit filename here if needed
    apply_post_fix()


if __name__ == "__main__":
    main()


