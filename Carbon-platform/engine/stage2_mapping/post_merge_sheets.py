from __future__ import annotations

from pathlib import Path
import os
import glob
from typing import Dict, List, Optional, Tuple
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR


# Post-mapping consolidation rules
# Each tuple is (sources_list, target_sheet)
POST_MERGE_RULES: List[Tuple[List[str], str]] = [
    # Scope 3 Cat 8 Fuel into Scope 1 sheets (corrected per user)
    (["Scope 3 Cat 8 Fuel Usage Activi"], "Scope 1 Fuel Activity"),
    (["Scope 3 Cat 8 Fuel Usage Spend"], "Scope 1 Fuel Usage Spend"),
    # Electricity and District Energy into Scope 2 Electricity
    ([
        "S3C8_Electricity_extracted",
        "Scope 3 Cat 8 District H",
        "Scope 3 Cat 8 District E",
    ], "Scope 2 Electricity"),
]


def _find_latest_mapped_results(base_dir: Path) -> Optional[Path]:
    output_dir = STAGE2_OUTPUT_DIR
    patterns = [str(output_dir / "mapped_results_*.xlsx"), str(output_dir / "mapped_results.xlsx")]
    candidates: List[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    candidates.sort(key=os.path.getmtime, reverse=True)
    return Path(candidates[0])


def _resolve_present_sheet_key(all_sheets: Dict[str, pd.DataFrame], desired_name: str) -> Optional[str]:
    """Resolve actual present sheet key considering Excel's 31-char truncation and case differences.

    Preference order:
      1) Exact key
      2) Case-insensitive exact key
      3) Truncated desired name (<=31) exact match (case-sensitive then insensitive)
    """
    if desired_name in all_sheets:
        return desired_name
    # Case-insensitive exact
    desired_low = desired_name.strip().lower()
    for k in all_sheets.keys():
        if k.strip().lower() == desired_low:
            return k
    # Truncation equality
    trunc = desired_name[:31]
    if trunc in all_sheets:
        return trunc
    trunc_low = trunc.strip().lower()
    for k in all_sheets.keys():
        if k.strip().lower() == trunc_low:
            return k
    return None


def _concat_align(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate DataFrames row-wise with outer-join on columns and reset index."""
    frames = [df for df in dfs if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=0, join="outer", ignore_index=True)


def _apply_scope_fields(df: pd.DataFrame, target_sheet: str) -> pd.DataFrame:
    """Set scope-related columns to match the target sheet category for all rows.

    Columns affected: 'scope', 'scope_category', 'ghg_category'.
    """
    if df is None or df.empty:
        return df

    scope_val = None
    scope_cat = None
    ghg_cat = None

    t = target_sheet.strip().lower()
    if t.startswith("scope 1"):
        scope_val = "1"
    elif t.startswith("scope 2"):
        scope_val = "2"
    elif t.startswith("scope 3"):
        scope_val = "3"

    # Explicit categories for known targets
    if t == "scope 1 fuel activity":
        scope_cat = "Scope 1 Fuel Activity"
        ghg_cat = "Scope 1 Fuel Activity"
    elif t == "scope 1 fuel usage spend":
        scope_cat = "Scope 1 Fuel Usage Spend"
        ghg_cat = "Scope 1 Fuel Usage Spend"
    elif t == "scope 2 electricity":
        scope_cat = "Scope 2 Electricity"
        ghg_cat = "Scope 2 Electricity"
    else:
        # Fallback: use the suffix after the scope number as category text
        # e.g. "Scope 1 Fuel Something" -> "Fuel Something"
        parts = target_sheet.split(maxsplit=2)
        if len(parts) >= 3:
            suffix = parts[2].strip()
        elif len(parts) == 2:
            suffix = parts[1].strip()
        else:
            suffix = target_sheet
        scope_cat = suffix
        ghg_cat = suffix

    for col, val in (("scope", scope_val), ("scope_category", scope_cat), ("ghg_category", ghg_cat)):
        if val is not None:
            # Ensure column exists and set all rows to the value
            if col not in df.columns:
                df[col] = None
            df[col] = val
    return df


def apply_post_mapping_merges(target_workbook: Optional[Path] = None) -> Optional[Path]:
    """Apply post-mapping sheet consolidations and write a new merged workbook.

    - Never mutates the original workbook. Writes to output/mapped_results_merged.xlsx
      (or a timestamped variant if locked), so reverting simply means ignoring/deleting the merged copy.
    - If a source or target sheet does not exist, it is skipped gracefully.
    - Rows are appended as-is (no transformation). Column set is the union of involved sheets.
    """
    base_dir = Path(__file__).resolve().parent
    if target_workbook is None:
        target_workbook = _find_latest_mapped_results(base_dir)
        if target_workbook is None:
            return None

    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(target_workbook, sheet_name=None)
    except Exception:
        return None

    if not all_sheets:
        return None

    updated: Dict[str, pd.DataFrame] = {k: v for k, v in all_sheets.items()}

    for sources, target in POST_MERGE_RULES:
        # Resolve real present target key (create empty if not present)
        target_key = _resolve_present_sheet_key(updated, target)
        target_df = updated.get(target_key) if target_key else None
        if target_df is None:
            target_df = pd.DataFrame()

        pieces: List[pd.DataFrame] = [target_df]
        removed_keys: List[str] = []

        for src in sources:
            src_key = _resolve_present_sheet_key(updated, src)
            if src_key is None:
                continue
            src_df = updated.get(src_key)
            if src_df is None or src_df.empty:
                # Remove empty source sheets too
                removed_keys.append(src_key)
                continue
            pieces.append(src_df)
            removed_keys.append(src_key)

        merged_df = _concat_align(pieces)
        if merged_df is None or merged_df.empty:
            # Nothing to write; still remove empty sources if any
            for rk in removed_keys:
                if rk in updated:
                    updated.pop(rk, None)
            continue

        # Ensure target entry uses the intended visible name (respecting Excel 31-char limit)
        visible_target_name = target[:31]
        # Apply scope fields for the merged target
        merged_df = _apply_scope_fields(merged_df, visible_target_name)
        updated[visible_target_name] = merged_df

        # Remove merged source sheets
        for rk in removed_keys:
            if rk in updated and rk != visible_target_name:
                updated.pop(rk, None)

    # Write out a new workbook under output/
    out_dir = STAGE2_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    # Always use timestamped filename
    ts = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    merged_path = out_dir / f"mapped_results_merged_{ts}.xlsx"

    try:
        with pd.ExcelWriter(merged_path, engine="xlsxwriter") as writer:
            for name, df in updated.items():
                safe_name = name[:31] if len(name) > 31 else name
                df.to_excel(writer, sheet_name=safe_name, index=False)
        return merged_path
    except PermissionError:
        ts_fallback = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        ts_name = out_dir / f"mapped_results_merged_{ts_fallback}.xlsx"
        with pd.ExcelWriter(ts_name, engine="xlsxwriter") as writer:
            for name, df in updated.items():
                safe_name = name[:31] if len(name) > 31 else name
                df.to_excel(writer, sheet_name=safe_name, index=False)
        return ts_name


if __name__ == "__main__":
    # Manual run helper
    result = apply_post_mapping_merges()
    if result is None:
        print("No mapped workbook found or failed to merge.")
    else:
        print(f"Wrote merged workbook: {Path(result).name}")



