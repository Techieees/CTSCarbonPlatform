from __future__ import annotations

from pathlib import Path
import os
import glob
from typing import Dict, List, Optional, Tuple
import re
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR


# Purpose
# Apply manual double-counting null rules ONLY to the following sheets/columns:
# - Sheet: "Scope 3 Cat 1 Goods Services" → Column: "Supplier"
# - Sheet: "Scope 3 Cat 1 Services Spend" → Column: "Service Provider Name"
# Source company is read from column variants of Source_file.
#
# Safety & Reversibility
# - Never modify the original workbook. Writes a new timestamped copy: mapped_results_merged_dc_YYYYMMDD_HHMMSS.xlsx
# - Do NOT delete rows. Preserve original values in column 'co2e_dc_original', set 'co2e' to 0.0 for nulled rows
#   and mark with 'double_counting_flag' + 'double_counting_reason'.


TARGET_SHEETS_TO_COL = {
    "Scope 3 Cat 1 Goods Services": ["Supplier", "supplier"],
    "Scope 3 Cat 1 Services Spend": ["Service Provider Name", "Service provider name"],
}


def _autosize_and_style(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Apply header format, zebra striping, auto-fit columns, freeze top row."""
    try:
        ws = writer.sheets.get(sheet_name)
        if ws is None:
            return
        wb = writer.book
        header_fmt = wb.add_format({
            "bold": True,
            "bg_color": "#D8EAD3",
            "border": 1,
        })
        zebra_fmt = wb.add_format({
            "bg_color": "#F2F2F2",
        })
        # Re-write headers with style
        for idx, col in enumerate(list(df.columns)):
            ws.write(0, idx, str(col), header_fmt)
        # Auto width
        for idx, col in enumerate(list(df.columns)):
            try:
                series = df[col].astype(str)
                max_len = max([len(str(col))] + series.str.len().tolist())
                width = min(max(8, max_len + 1), 40)
                ws.set_column(idx, idx, width)
            except Exception:
                ws.set_column(idx, idx, 16)
        # Freeze top row
        ws.freeze_panes(1, 0)
        # Zebra
        if df.shape[0] > 0 and df.shape[1] > 0:
            ws.conditional_format(1, 0, df.shape[0], df.shape[1] - 1, {
                'type': 'formula',
                'criteria': '=MOD(ROW(),2)=0',
                'format': zebra_fmt,
            })
    except Exception:
        # best effort styling
        pass


CTS_SOURCES: List[str] = [
    "CTS Nordics",
    "CTS Denmark",
    "CTS Sweden",
    "CTS Finland",
]

#Waste datasi icin yaptigimiz mappingi degistirelim.  Oncelikle weight unit kismina bakacaksin. Burada kg, litres, tn, Tonnes, tons var). Biz her zaman ton ile calisiyoruz. kg ve Litres olanlari 1000 e boleceksin. Digerleri ayni sekilde kalacak. Sonrasinda ef_value degerleriyle bunu carpacaksin. Istersen yeni bir sutun yarat Weight(tonnes) olarak ve donusturdugun degerleri buraya yaz. 



# Internal suppliers list per manual (normalized, punctuation/spacing-insensitive)
INTERNAL_SUPPLIERS: List[str] = [
    "Nordic EPOD",
    "Nordicepod AS",
    "Nordicepod",
    "NEP Switchboards",
    "G. T Nordics",
    "G. T Nordics AS",
    "G.T Nordics As",
    "Gapit",
    "Gapit AS",
    "Gapit As",
    "Gapit Nordics As",
    "DC Piping",
    "MC Prefab",
    "MC Prefab Nordics AS",
    "Mc Prefab Nordics AS",
    "Velox Electro Nordics AS",
    "Velox Electro Nordics OY",
    "Mecwide Nordics Finland OY",
    "Mecwide Nordics AS",
    "Mecwide Nordics Denmark ApS",
    "Mecwide Nordics`",
    "Comissioning Services",
    "Comissioning Services AS",
    "Qec Nordics AS",
    "Qec Nordics",
    "PORVELOX Electro Europe Lda",
    "MC Prefab Sweden AB",
    "Nordic Crane AS",
    "Commissioning Services Nordics AS",
    "Mecwide Nordics Sweden AB",   
    "CTS-VDC Services LTD",
    "CTS NORDICS AS",
    "Velox Electro Nordics AB",
    "CS Nordics",
    "Fortica Sweden AB",
    "101",
    "102",
    "103",
    "104",
    "105",
    "106",
    "107",
    "108",
    "109",
    "110",
    "111",
    "112",
    "113",
    "114",
    "115",
    "116",
    "117",
]

# Special null rules (explicit pairs): (source_company, provider_should_be)
SPECIAL_NULL_PAIRS: List[Tuple[str, List[str], str]] = [
    ("Mecwide Nordics", ["DC Piping", "DC Piping, S.A."], "Mecwide x DC Piping per manual"),
    ("MC Prefab", ["DC Piping", "DC Piping, S.A."], "MC Prefab x DC Piping per manual"),
]

# Custom supplier-based nulling for Klarakarbon-like cases on Goods Services
CUSTOM_NULL_SUPPLIERS_GOODS_SERVICES: List[str] = [
    "G.T Automasjon & Elektro service AS",
    "EST G.T Nordics AS",
    "EST.G.T & Elektroservice AS",
    "G.T Automasjon AS",
]


def _normalize_token(text: Optional[str]) -> str:
    if text is None:
        return ""
    s = str(text).strip().lower()
    if not s:
        return ""
    s = s.replace(".xlsx", "").replace(".xls", "")
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    # keep alnum and spaces; strip punctuation/accents-like
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return s


def _detect_source_column(df: pd.DataFrame) -> Optional[str]:
    # Prefer explicit source_company if present, then source_file variants
    preferred_exact = {"source_company", "source company", "source_file", "source file", "sourcefile"}
    for col in df.columns:
        low = str(col).lower()
        if low in preferred_exact:
            return col
    for col in df.columns:
        low = str(col).lower()
        compact = low.replace(" ", "").replace("_", "")
        if low in {"source_company"} or compact == "sourcecompany":
            return col
        if ("source" in low and "file" in low) or compact == "sourcefile":
            return col
    return None


def _pick_provider_column(sheet_name: str, df: pd.DataFrame) -> Optional[str]:
    cols = TARGET_SHEETS_TO_COL.get(sheet_name)
    if not cols:
        return None
    lowmap = {str(c).strip().lower(): c for c in df.columns}
    for cand in cols:
        k = str(cand).strip().lower()
        if k in lowmap:
            return lowmap[k]
    return None


def _find_latest_merged(base_dir: Path) -> Optional[Path]:
    out_dir = STAGE2_OUTPUT_DIR
    patterns = [
        str(out_dir / "mapped_results_merged_*.xlsx"),
        str(out_dir / "mapped_results_merged.xlsx"),
    ]
    candidates: List[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    candidates.sort(key=os.path.getmtime, reverse=True)
    return Path(candidates[0])


def _build_internal_sets() -> Tuple[set[str], set[str], Dict[str, set[str]]]:
    cts_set = {_normalize_token(x) for x in CTS_SOURCES}
    internal_set = {_normalize_token(x) for x in INTERNAL_SUPPLIERS}
    special_pairs: Dict[str, set[str]] = {}
    for src, providers, _reason in SPECIAL_NULL_PAIRS:
        key = _normalize_token(src)
        specials = { _normalize_token(p) for p in providers }
        if key in special_pairs:
            special_pairs[key].update(specials)
        else:
            special_pairs[key] = specials
    return cts_set, internal_set, special_pairs


def apply_double_counting(df_dict: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    cts_set, internal_set, special_pairs = _build_internal_sets()

    def _pick_co2e_column(df_any: pd.DataFrame) -> Optional[str]:
        if df_any is None or df_any.empty:
            return None
        # try exact common names first (prefer kg if exists, then tons)
        for name in ["co2e (kg)", "co2e (t)", "co2e", "emissions_tco2e"]:
            if name in df_any.columns:
                return name
        # case-insensitive fallback
        lowmap = {str(c).strip().lower(): c for c in df_any.columns}
        for key in ["co2e (kg)", "co2e (t)", "co2e", "emissions_tco2e"]:
            if key in lowmap:
                return lowmap[key]
        return None

    log_rows: List[Dict[str, object]] = []
    updated: Dict[str, pd.DataFrame] = {}

    target_names = set(TARGET_SHEETS_TO_COL.keys())
    for sheet_name, df in df_dict.items():
        if sheet_name not in target_names or df is None or df.empty:
            updated[sheet_name] = df
            continue

        df_mod = df.copy()
        src_col = _detect_source_column(df_mod)
        prov_col = _pick_provider_column(sheet_name, df_mod)
        if src_col is None or prov_col is None:
            updated[sheet_name] = df_mod
            continue

        co2e_col = _pick_co2e_column(df_mod)
        if "co2e_dc_original" not in df_mod.columns:
            try:
                if co2e_col is not None and co2e_col in df_mod.columns:
                    df_mod["co2e_dc_original"] = df_mod[co2e_col]
                else:
                    df_mod["co2e_dc_original"] = None
            except Exception:
                df_mod["co2e_dc_original"] = None
        if "double_counting_flag" not in df_mod.columns:
            df_mod["double_counting_flag"] = False
        if "double_counting_reason" not in df_mod.columns:
            df_mod["double_counting_reason"] = pd.Series([None] * len(df_mod), dtype="object")
        else:
            # ensure object dtype to avoid assignment FutureWarning
            try:
                df_mod["double_counting_reason"] = df_mod["double_counting_reason"].astype("object")
            except Exception:
                pass

        src_norm = df_mod[src_col].astype(str).map(_normalize_token)
        prov_norm = df_mod[prov_col].astype(str).map(_normalize_token)


        # Rule 1: CTS* sources null providers in internal list
        mask_cts = src_norm.isin(cts_set) & prov_norm.isin(internal_set)
        if bool(getattr(mask_cts, "any", lambda: False)()) and co2e_col is not None:
            co2e_before = pd.to_numeric(df_mod.loc[mask_cts, co2e_col], errors="coerce").sum(skipna=True)
            df_mod.loc[mask_cts, co2e_col] = 0.0
            df_mod.loc[mask_cts, "double_counting_flag"] = True
            df_mod.loc[mask_cts, "double_counting_reason"] = "CTS internal CCC per manual"
            log_rows.append({
                "sheet": sheet_name,
                "rule": "CTS internal CCC",
                "rows_nulled": int(mask_cts.sum()),
                "co2e_nulled_sum": float(co2e_before),
            })

        # Rule 2: Special pairs (Mecwide/MC Prefab with DC Piping)
        for src_raw, _providers, reason in SPECIAL_NULL_PAIRS:
            src_key = _normalize_token(src_raw)
            pset = special_pairs.get(src_key, set())
            if not pset:
                continue
            mask = (src_norm == src_key) & (prov_norm.isin(pset))
            if bool(getattr(mask, "any", lambda: False)()) and co2e_col is not None:
                co2e_before = pd.to_numeric(df_mod.loc[mask, co2e_col], errors="coerce").sum(skipna=True)
                df_mod.loc[mask, co2e_col] = 0.0
                df_mod.loc[mask, "double_counting_flag"] = True
                df_mod.loc[mask, "double_counting_reason"] = reason
                log_rows.append({
                    "sheet": sheet_name,
                    "rule": reason,
                    "rows_nulled": int(mask.sum()),
                    "co2e_nulled_sum": float(co2e_before),
                })

        # Rule 3: Custom suppliers on Goods Services sheet -> null
        try:
            if sheet_name == "Scope 3 Cat 1 Goods Services" and prov_col is not None:
                targets = { _normalize_token(x) for x in CUSTOM_NULL_SUPPLIERS_GOODS_SERVICES }
                mask_custom = prov_norm.isin(targets)
                if bool(getattr(mask_custom, "any", lambda: False)()) and co2e_col is not None:
                    co2e_before = pd.to_numeric(df_mod.loc[mask_custom, co2e_col], errors="coerce").sum(skipna=True)
                    df_mod.loc[mask_custom, co2e_col] = 0.0
                    df_mod.loc[mask_custom, "double_counting_flag"] = True
                    df_mod.loc[mask_custom, "double_counting_reason"] = "Supplier DC per manual (Goods Services)"
                    log_rows.append({
                        "sheet": sheet_name,
                        "rule": "Supplier DC: Goods Services",
                        "rows_nulled": int(mask_custom.sum()),
                        "co2e_nulled_sum": float(co2e_before),
                    })
        except Exception:
            pass

        updated[sheet_name] = df_mod

    log_df = pd.DataFrame(log_rows)
    if not log_df.empty:
        log_df = log_df.sort_values(["sheet", "co2e_nulled_sum"], ascending=[True, False]).reset_index(drop=True)

    return updated, log_df



def _collect_anomalies(df_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Collect rows across sheets where status == 'Anomaly Detected'.

    - Adds a 'Sheet' column with the source sheet name.
    - Outer-concats different schemas.
    """
    parts: List[pd.DataFrame] = []
    for name, df in df_dict.items():
        if df is None or df.empty:
            continue
        # find 'status' case-insensitively
        status_col = None
        for c in df.columns:
            if str(c).strip().lower() == "status":
                status_col = c
                break
        if status_col is None:
            continue
        try:
            mask = df[status_col].astype(str).str.strip().str.lower() == "anomaly detected"
        except Exception:
            continue
        hits = df[mask]
        if hits is None or hits.empty:
            continue
        temp = hits.copy()
        temp["Sheet"] = name
        parts.append(temp)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, axis=0, join="outer", ignore_index=True)


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    src = _find_latest_merged(base_dir)
    if src is None:
        print("No mapped_results_merged*.xlsx found under output/.")
        return

    try:
        all_sheets: Dict[str, pd.DataFrame] = pd.read_excel(src, sheet_name=None)
    except Exception:
        print(f"Failed to read workbook: {src}")
        return

    updated, log_df = apply_double_counting(all_sheets)
    anomalies_df = _collect_anomalies(updated)

    out_dir = STAGE2_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    out_path = out_dir / f"mapped_results_merged_dc_{ts}.xlsx"

    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            for name, df in updated.items():
                safe_name = name[:31] if len(name) > 31 else name
                df.to_excel(writer, sheet_name=safe_name, index=False)
                _autosize_and_style(writer, safe_name, df)
            # Append log sheet
            if log_df is not None and not log_df.empty:
                log_df.to_excel(writer, sheet_name="DC Log", index=False)
                _autosize_and_style(writer, "DC Log", log_df)
            # Append anomalies sheet
            if anomalies_df is not None and not anomalies_df.empty:
                anomalies_df.to_excel(writer, sheet_name="Anomalies", index=False)
                _autosize_and_style(writer, "Anomalies", anomalies_df)
        print(f"Wrote DC-adjusted workbook: {out_path.name}")
    except PermissionError:
        # Try with a different timestamped name
        ts2 = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        out_path2 = out_dir / f"mapped_results_merged_dc_{ts2}.xlsx"
        with pd.ExcelWriter(out_path2, engine="xlsxwriter") as writer:
            for name, df in updated.items():
                safe_name = name[:31] if len(name) > 31 else name
                df.to_excel(writer, sheet_name=safe_name, index=False)
                _autosize_and_style(writer, safe_name, df)
            if log_df is not None and not log_df.empty:
                log_df.to_excel(writer, sheet_name="DC Log", index=False)
                _autosize_and_style(writer, "DC Log", log_df)
            if anomalies_df is not None and not anomalies_df.empty:
                anomalies_df.to_excel(writer, sheet_name="Anomalies", index=False)
                _autosize_and_style(writer, "Anomalies", anomalies_df)
        print(f"Wrote DC-adjusted workbook: {out_path2.name}")


if __name__ == "__main__":
    main()


