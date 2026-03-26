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
OUTPUT_DIR = STAGE2_OUTPUT_DIR
FUEL_SHEET_NAME = "Scope 3 Cat 3 FERA Fuel"


def _find_latest_fera_output(base_dir: Path) -> Optional[Path]:
	"""
	Find the most recent 'mapped_results_merged_fera_*.xlsx' under output/.
	Fallback to any 'mapped_results_merged_*.xlsx' if none is found.
	"""
	out = STAGE2_OUTPUT_DIR
	patterns = [
		str(out / "mapped_results_merged_fera_*.xlsx"),
		str(out / "mapped_results_merged_*.xlsx"),
	]
	candidates: list[str] = []
	for pat in patterns:
		candidates.extend(glob.glob(pat))
	if not candidates:
		return None
	candidates.sort(key=os.path.getmtime, reverse=True)
	return Path(candidates[0])


def _get_ci_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
	lowmap = {str(c).strip().lower(): c for c in df.columns}
	for cand in candidates:
		key = str(cand).strip().lower()
		if key in lowmap:
			return lowmap[key]
	# relaxed contains
	for cand in candidates:
		key = str(cand).strip().lower()
		for low, orig in lowmap.items():
			if key in low:
				return orig
	return None


def _overwrite_primary_co2e_for_kbk(df: pd.DataFrame) -> pd.DataFrame:
	"""
	On FERA Fuel sheet:
	- If there are multiple exact 'co2e (t)' columns, overwrite the first
	  with the last for rows where 'Data Source sheet' == 'Klarakarbon'.
	- Do not touch Booklets rows.
	- Keep structure otherwise unchanged.
	"""
	if df is None or df.empty:
		return df
	out = df.copy()
	# Find all columns belonging to the co2e(t) family (robust to '.1' and ' 2' variants)
	def _is_co2e_family(name: str) -> bool:
		import re as _re
		s = _re.sub(r"[^a-z0-9]", "", str(name).lower())
		return s.startswith("co2et")
	co2e_cols = [c for c in list(out.columns) if _is_co2e_family(c)]
	if len(co2e_cols) < 2:
		# not enough columns to overlay
		return out
	left_col = co2e_cols[0]
	right_col = co2e_cols[-1]
	# Prefer exact 'co2e (t)' as left if present
	for c in co2e_cols:
		if str(c).strip().lower() == "co2e (t)":
			left_col = c
			break
	# Klarakarbon mask
	ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
	if ds_col is None or ds_col not in out.columns:
		return out
	try:
		mask_kbk = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
		out.loc[mask_kbk, left_col] = out.loc[mask_kbk, right_col].values
	except Exception:
		return out
	return out

def _clear_kbk_co2e_and_drop_empty(df: pd.DataFrame) -> pd.DataFrame:
	"""
	For Klarakarbon rows: clear values in all co2e(t) family columns.
	Then drop any co2e(t) family column that becomes entirely empty (all NaN/empty string).
	"""
	if df is None or df.empty:
		return df
	out = df.copy()
	# Klarakarbon mask
	ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
	if ds_col is None or ds_col not in out.columns:
		return out
	mask_kbk = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
	# Find co2e family columns
	def _is_co2e_family(name: str) -> bool:
		import re as _re
		s = _re.sub(r"[^a-z0-9]", "", str(name).lower())
		return s.startswith("co2et")
	co2e_cols = [c for c in list(out.columns) if _is_co2e_family(c)]
	if co2e_cols:
		for c in co2e_cols:
			try:
				out.loc[mask_kbk, c] = None
			except Exception:
				pass
	# Drop fully-empty co2e columns
	def _is_col_empty(series: pd.Series) -> bool:
		try:
			if series.isna().all():
				return True
		except Exception:
			pass
		try:
			s = series.astype(str).str.strip().str.lower()
			return s.isin({"", "nan", "none"}).all()
		except Exception:
			return False
	try:
		drop_list = [c for c in co2e_cols if _is_col_empty(out[c])]
		if drop_list:
			out = out.drop(columns=drop_list, errors="ignore")
	except Exception:
		return out
	return out

def _write_kbk_product_to_primary(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Compute for Klarakarbon rows:
	  co2e (t) = activity volume * kbk_map_ef_value
	Write the result into the remaining primary 'co2e (t)' column.
	If no co2e column exists, create 'co2e (t)'.
	"""
	if df is None or df.empty:
		return df
	out = df.copy()
	# Locate columns
	ds_col = _get_ci_col(out, ["Data Source sheet", "Sheet_booklets", "sheet_booklets"])
	if ds_col is None or ds_col not in out.columns:
		return out
	val_col = _get_ci_col(out, ["kbk_map_ef_value"])
	act_col = _get_ci_col(out, ["activity volume", "activity_volume", "activity amount"])
	if val_col is None or act_col is None:
		return out
	# Primary co2e column
	co2e_col = None
	for c in list(out.columns):
		if str(c).strip().lower() == "co2e (t)":
			co2e_col = c
			break
	if co2e_col is None:
		co2e_col = "co2e (t)"
		out[co2e_col] = None
	# Compute for Klarakarbon rows
	mask_kbk = out[ds_col].astype(str).str.strip().str.lower() == "klarakarbon"
	def _to_num(s: pd.Series) -> pd.Series:
		return pd.to_numeric(
			s.astype(str)
			 .str.replace("\u00A0", "", regex=False)
			 .str.replace(" ", "", regex=False)
			 .str.replace(",", ".", regex=False),
			errors="coerce",
		)
	try:
		prod = _to_num(out.loc[mask_kbk, act_col]) * _to_num(out.loc[mask_kbk, val_col])
		out.loc[mask_kbk, co2e_col] = prod.astype("float64")
	except Exception:
		return out
	return out
def apply_fix(target_workbook: Optional[Path] = None) -> Optional[Path]:
	if target_workbook is None:
		target_workbook = _find_latest_fera_output(BASE_DIR)
	if target_workbook is None:
		print("FERA Fuel Fix: No FERA workbook found.")
		return None
	try:
		all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(target_workbook, sheet_name=None, engine="openpyxl")
	except Exception as exc:
		print(f"FERA Fuel Fix: Failed to read workbook: {exc}")
		return None
	if not all_sheets:
		print("FERA Fuel Fix: Workbook is empty.")
		return None
	df = all_sheets.get(FUEL_SHEET_NAME)
	if df is None or df.empty:
		print(f"FERA Fuel Fix: Fuel sheet '{FUEL_SHEET_NAME}' not found or empty.")
		return None
	# 1) Overwrite primary with last duplicate values for KBK (if duplicates exist)
	df = _overwrite_primary_co2e_for_kbk(df)
	# 2) Then clear KBK co2e values and drop fully-empty co2e columns as requested
	df = _clear_kbk_co2e_and_drop_empty(df)
	# 3) Finally compute KBK co2e = activity volume * kbk_map_ef_value into primary column
	df = _write_kbk_product_to_primary(df)
	all_sheets[FUEL_SHEET_NAME] = df
	# Write a fresh timestamped workbook so downstream picks the latest
	try:
		OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
		ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
		out_path = OUTPUT_DIR / f"mapped_results_merged_fera_fuel_{ts}.xlsx"
		with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
			for name, sdf in all_sheets.items():
				safe = name[:31] if len(name) > 31 else name
				sdf.to_excel(writer, sheet_name=safe, index=False)
		print(f"FERA Fuel Fix: Wrote workbook: {out_path.name}")
		return out_path
	except Exception as exc:
		print(f"FERA Fuel Fix: Failed to write workbook: {exc}")
		return None


def main() -> None:
	apply_fix()


if __name__ == "__main__":
	main()

# Yeni bir kod yazacagiz seninle. Bu kodun ismi column harmonization olacak.
# Bu kodun amaci bazi columnlari birlestirmek ve bazi columnlarin ismini degistirmek olacak.
# Sheet bazli gidecegiz. Sheetleri okuyacagiz ve harmonize edecegiz.
# Scope 1 icin:
# Company, Country (Normalde country isimli bir column var bunun ismini Country olarak degistirecegiz.) , Date, Calculation Method, Activity Volume, Activity Unit, Emission Factor Name, Emission Factor Unit
# Emission Factor Value
