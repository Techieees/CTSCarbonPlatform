# Python 3.10+
# Requirements: pandas, openpyxl
# WT (Water Tracker, dual extractor),
# S3C8 Electricity, S3C8 District Heating (E/H split),
# S3C11 Products, S3C15 Pensions (custom mapping with CTS Denmark & Velox fix)
# Author: Florian Demir (Sustainability Data Analyst)

import pandas as pd
import re
import sys
from datetime import datetime
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE1_OUTPUT_DIR, pick_first_existing

# ============================= PATHS ==========================================
INPUT_FILE = str(
    pick_first_existing(
        STAGE1_OUTPUT_DIR / "stage1_01_merged.xlsx",
        STAGE1_OUTPUT_DIR / "merged.xlsx",
    )
)
OUTPUT_FILE = str(
    STAGE1_OUTPUT_DIR / f"merged_WT_ELEC_DH_S3C15_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
)

# ============================= COMMON HELPERS =================================
def ncomp(x):
    """
    Normalize to a compact comparison string:
    - lower-case
    - fix common unicode variants
    - remove whitespace
    - strip non-alphanumeric
    """
    if pd.isna(x):
        return ""
    s = str(x).lower()
    s = s.replace("’", "'").replace("³", "3")
    s = re.sub(r"\s+", "", s)
    return re.sub(r"[^a-z0-9]", "", s)


MONTH_RE = re.compile(
    r"(?ix)("
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[\s'._-]*\d{4}"
    r"|"
    r"\d{4}[\s./_-]*(0?[1-9]|1[0-2])"
    r"|"
    r"(0?[1-9]|1[0-2])[\s./_-]*\d{4}"
    r")"
)

def month_year_like(x):
    if pd.isna(x):
        return False
    return bool(MONTH_RE.match(str(x).strip()))


def looks_numeric(x):
    return not pd.to_numeric(pd.Series([x]), errors="coerce").isna().iloc[0]


def is_energy_unit(x):
    return ncomp(x) in {"kwh", "mwh", "m3", "l"}


def find_sheet(xl, prefer_exact, fuzzy_terms):
    """
    Prefer an exact sheet name; otherwise return the first sheet whose
    lowercase name includes all fuzzy_terms.
    """
    for n in xl.sheet_names:
        if n.strip().lower() == prefer_exact.lower():
            return n
    for n in xl.sheet_names:
        t = n.strip().lower()
        if all(term in t for term in fuzzy_terms):
            return n
    return None


def header_hits_row(norm_row, canon_headers):
    """
    Given a normalized row (already ncomp'ed), return {header:col_index}
    if the row contains all canon headers (normalized).
    """
    need = {ncomp(h): h for h in canon_headers}
    got = {}
    for j, val in enumerate(norm_row):
        if val in need and need[val] not in got:
            got[need[val]] = j
    return got if len(got) == len(canon_headers) else None


def cut_by_empty(body, target_cols):
    """
    Trim body where all target cols become empty (common in exported sheets:
    blank rows at the bottom).
    """
    tmp = body[target_cols].astype(str).applymap(lambda x: x.strip())
    stop_mask = tmp.replace("", pd.NA).isna().all(axis=1)
    if stop_mask.any():
        body = body.loc[: stop_mask.idxmax() - 1]
    return body


# ============================= WATER TRACKER (GOLD) ===========================
WT_HEADERS = [
    "Reporting period (month, year)",
    "Consumption of water",
    "Unit",
    "Data Source (utilities report or water meter)",
]

def wt_clean_unit(x):
    c = ncomp(x)
    if c == "l":
        return "L"
    if c == "m3":
        return "m3"
    return None


def wt_validate_row_dict(row: dict) -> bool:
    """
    Validation used by the WATER TRACKER extractors (dict-based).
    Mutates row in-place when valid.
    """
    if not month_year_like(row["Reporting period (month, year)"]):
        return False

    unit = wt_clean_unit(row["Unit"])
    if unit is None:
        return False

    val = pd.to_numeric(row["Consumption of water"], errors="coerce")
    if pd.isna(val):
        return False

    row["Consumption of water"] = val
    row["Unit"] = unit
    row["Data Source (utilities report or water meter)"] = (
        "" if pd.isna(row["Data Source (utilities report or water meter)"])
        else str(row["Data Source (utilities report or water meter)"]).strip()
    )
    return True


def extract_wt_structured(df: pd.DataFrame) -> pd.DataFrame:
    """
    Structured extractor:
    - Searches for header rows that contain WT_HEADERS.
    - Reads downwards until the end of the sheet.
    """
    norm = df.astype(str).applymap(ncomp)
    blocks = []

    for i in range(norm.shape[0]):
        # check if the row contains the first 3 headers (robust enough to detect tables)
        if all(ncomp(h) in norm.iloc[i].values for h in WT_HEADERS[:3]):
            col_map = {}
            for h in WT_HEADERS:
                for j, v in enumerate(norm.iloc[i]):
                    if ncomp(h) == v:
                        col_map[h] = j
            start = i + 1
            body = []

            for r in range(start, df.shape[0]):
                row = {}
                for h in WT_HEADERS:
                    row[h] = df.iloc[r, col_map.get(h)] if h in col_map else pd.NA
                row["Source_File"] = df.iloc[r, 0]

                if wt_validate_row_dict(row):
                    body.append(row)

            if body:
                blocks.append(pd.DataFrame(body))

    return pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame(columns=["Source_File"] + WT_HEADERS)


def extract_wt_rowwise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Row-wise extractor:
    - Expects rows of the form
      [Source_File, Reporting period, Consumption, Unit, Data Source?]
    - Uses the same validation as the structured extractor.
    """
    rows = []

    for i in range(df.shape[0]):
        src = df.iloc[i, 0]
        if not isinstance(src, str) or not src.lower().endswith(".xlsx"):
            continue

        rp = df.iloc[i, 1]
        val = df.iloc[i, 2]
        unit = df.iloc[i, 3]
        ds = df.iloc[i, 4] if df.shape[1] > 4 else ""

        row = {
            "Source_File": src,
            "Reporting period (month, year)": rp,
            "Consumption of water": val,
            "Unit": unit,
            "Data Source (utilities report or water meter)": ds,
        }

        if wt_validate_row_dict(row):
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["Source_File"] + WT_HEADERS)

    return pd.DataFrame(rows)


# ============================= GENERIC EXTRACTOR ===============================
def extract_generic(df, canon_headers, validate_row, source_col_hint="source_file", span_limit=12):
    """
    Generic header-based extractor used for:
    - S3C8 Electricity
    - (Any future table with classic header rows)

    validate_row is a function(row: pd.Series) -> bool
    """
    norm = df.astype(str).applymap(ncomp)
    header_rows = []

    for i in range(norm.shape[0]):
        got = header_hits_row(norm.iloc[i, :], canon_headers)
        if got and (max(got.values()) - min(got.values())) <= span_limit:
            header_rows.append((i, got))

    blocks = []
    for k, (r, pos) in enumerate(header_rows):
        r0 = r + 1
        r1 = header_rows[k + 1][0] if k + 1 < len(header_rows) else df.shape[0]

        body = df.iloc[r0:r1, [pos[h] for h in canon_headers]].copy()
        body.columns = canon_headers

        # detect source file column if present, else fall back to first column
        if source_col_hint in norm.columns:
            sf = df.iloc[r0:r1, norm.columns.get_loc(source_col_hint)]
        else:
            sf = df.iloc[r0:r1, 0]
        body.insert(0, "Source_File", sf.values)

        body = cut_by_empty(body, canon_headers)
        body = body.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all")

        if not body.empty:
            mask_ok = body.apply(validate_row, axis=1)
            body = body[mask_ok]
            if not body.empty:
                blocks.append(body)

    if not blocks:
        return pd.DataFrame(columns=["Source_File"] + canon_headers)

    out = pd.concat(blocks, ignore_index=True)
    out["Source_File"] = out["Source_File"].ffill().bfill().astype(str).str.strip()
    out = out.drop_duplicates(ignore_index=True)
    return out[["Source_File"] + canon_headers]


# ============================= S3C8 ELECTRICITY ================================
ELEC_HEADERS = [
    "Facility",
    "Reporting Period",
    "Electricity Consumption",
    "Consumption unit",
    "CO2e from electricity",
    "CO2e unit",
]
ELEC_UNIT_WHITELIST = {"kwh", "mwh"}

def elec_unit_ok(x):
    return ncomp(x) in ELEC_UNIT_WHITELIST if pd.notna(x) else False


def elec_co2e_unit_ok(x):
    if pd.isna(x):
        return False
    s = str(x).strip()
    return 0 < len(s) <= 12


def elec_validate_row(row: pd.Series) -> bool:
    fac, per, kwh, u_cons, co2e, u_co2e = [row.get(c) for c in ELEC_HEADERS]

    if pd.isna(fac) or str(fac).strip() == "":
        return False
    if not month_year_like(per):
        return False

    kwh_num = pd.to_numeric(pd.Series([kwh]), errors="coerce").iloc[0]
    co2_num = pd.to_numeric(pd.Series([co2e]), errors="coerce").iloc[0]

    if pd.isna(kwh_num):
        return False
    if not elec_unit_ok(u_cons):
        return False
    if pd.isna(co2_num):
        return False
    if not elec_co2e_unit_ok(u_co2e):
        return False

    # normalize numeric fields
    row["Electricity Consumption"] = kwh_num
    row["CO2e from electricity"] = co2_num
    return True


# ============================= S3C8 DISTRICT HEATING ===========================
DH_HEADERS = [
    "Facility",
    "Reporting Period",
    "Electricity Consumption",
    "Consumption unit",
    "CO2e from electricity",
    "CO2e unit",
]
DH_UNIT_WHITELIST = {"kwh", "mwh"}

def dh_valid_row(row: pd.Series) -> bool:
    if not month_year_like(row.get("Reporting Period")):
        return False
    if ncomp(row.get("Consumption unit")) not in DH_UNIT_WHITELIST:
        return False
    if pd.to_numeric(pd.Series([row.get("Electricity Consumption")]), errors="coerce").isna().iloc[0]:
        return False
    if pd.to_numeric(pd.Series([row.get("CO2e from electricity")]), errors="coerce").isna().iloc[0]:
        return False

    bad_words = ["type of building", "average", "unit", "location", "facility", "reporting period"]
    if any(b in str(row.get("Facility")).lower() for b in bad_words):
        return False
    return True


def extract_dh_blocks(df: pd.DataFrame, headers) -> pd.DataFrame:
    """
    Locate the first header-like row for District Heating / Electricity split blocks,
    then:
    - treat next rows as [Source_File] + headers
    - keep only valid data rows
    - preserve original index for later E/H slicing (12+12 rows).
    """
    norm = df.astype(str).applymap(lambda x: re.sub(r"\s+", "", str(x).lower()))
    header_idx = None

    for i in range(norm.shape[0]):
        hits = 0
        for h in headers:
            pat = re.sub(r"\s+", "", h.lower())
            if any(norm.iloc[i, :].str.contains(pat, regex=False)):
                hits += 1
        if hits >= 3:
            header_idx = i
            break

    if header_idx is None:
        return pd.DataFrame(columns=["Source_File"] + headers + ["__orig_idx__"])

    body = df.iloc[header_idx + 1 :, : len(headers) + 1].copy()
    body.columns = ["Source_File"] + headers
    body["__orig_idx__"] = body.index

    body = body.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all")
    body = body[body.apply(dh_valid_row, axis=1)]
    return body.reset_index(drop=True)


# ============================= S3C15 PENSIONS =================================
def extract_pensions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Custom pensions extractor with:
    - per-file column index mapping
    - auto-detection of 'Employee Headcount' column as fallback
    - 'same as payment' notes pushed into Data Source when needed
    """
    # mark source file per row
    src_col = df[0].astype(str)
    is_filename = src_col.str.contains(r"\.xls[xm]?$", case=False, na=False)
    source_ffill = src_col.where(is_filename).where(src_col != "Source_File").ffill()
    df["Source_File"] = source_ffill

    mapping = {
        "BIMMS.xlsx": {"rp": 1, "headcount": 2, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "Caerus Nordics.xlsx": {"rp": 1, "headcount": 6, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "CTS Finland.xlsx": {"rp": 1, "headcount": 7, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "CTS Nordics.xlsx": {"rp": 1, "headcount": 7, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "CTS Sweden.xlsx": {"rp": 1, "headcount": 8, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "CTS-VDC.xlsx": {"rp": 1, "headcount": 9, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "DC Piping.xlsx": {"rp": 1, "headcount": 7, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "GT Nordics.xlsx": {"rp": 1, "headcount": 10, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "MC Prefab.xlsx": {"rp": 1, "headcount": 7, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "Mecwide Nordics.xlsx": {"rp": 1, "headcount": 10, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "Navitas Norway.xlsx": {"rp": 1, "headcount": 7, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "Navitas Portugal.xlsx": {"rp": 1, "headcount": 7, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "SD Nordics.xlsx": {"rp": 1, "headcount": 10, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "Velox.xlsx": {"rp": 1, "headcount": 12, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
        "CTS Denmark.xlsx": {"rp": 1, "headcount": 7, "provider": 3, "payment": 4, "currency": 5, "datasrc": 6},
    }

    records = []

    for src, cols in mapping.items():
        subset = df[df["Source_File"] == src]
        if subset.empty:
            continue

        # try to auto-detect the "Employee Headcount" column inside this block
        norm_subset = subset.astype(str).applymap(ncomp)
        hc_candidate_cols = [c for c in subset.columns if (norm_subset[c] == "employeeheadcount").any()]
        auto_hc_col = None
        if hc_candidate_cols:
            best_count = -1
            for c in hc_candidate_cols:
                mask_header = norm_subset[c] == "employeeheadcount"
                num_count = pd.to_numeric(subset.loc[~mask_header, c], errors="coerce").notna().sum()
                if num_count > best_count:
                    best_count = num_count
                    auto_hc_col = c

        def coerce_headcount(value):
            if pd.isna(value):
                return None, None  # (headcount, datasrc_add)
            s = str(value).strip()
            s_l = s.lower()
            if "same as" in s_l or "payment" in s_l:
                return None, s
            if ncomp(s) == "employeeheadcount":
                return None, None
            num = pd.to_numeric(pd.Series([s]), errors="coerce").iloc[0]
            if pd.notna(num):
                try:
                    return int(float(num)), None
                except Exception:
                    return None, None
            return None, s

        for _, row in subset.iterrows():
            rp = row.get(cols["rp"], None)
            provider = row.get(cols["provider"], None)
            payment = row.get(cols["payment"], None)
            currency = row.get(cols["currency"], None) if "currency" in cols else None
            datasrc = row.get(cols["datasrc"], None) if "datasrc" in cols else None

            # base headcount from mapping
            headcount_raw = row.get(cols["headcount"], None)
            hc_val, ds_from_hc = coerce_headcount(headcount_raw)
            if ds_from_hc and (pd.isna(datasrc) or str(datasrc).strip() == ""):
                datasrc = ds_from_hc

            # if still missing, try auto-detected headcount column
            if hc_val is None and auto_hc_col is not None:
                alt_val = row.get(auto_hc_col, None)
                hc_val2, ds_from_alt = coerce_headcount(alt_val)
                if hc_val2 is not None:
                    hc_val = hc_val2
                if ds_from_alt and (pd.isna(datasrc) or str(datasrc).strip() == ""):
                    datasrc = ds_from_alt

            headcount = hc_val

            # skip obvious header/guide rows
            row_str = " ".join(
                [
                    str(x)
                    for x in [rp, headcount, provider, payment, currency, datasrc]
                    if pd.notna(x)
                ]
            )
            if any(
                keyword in row_str
                for keyword in [
                    "Employee Headcount",
                    "Employer Pension Scheme",
                    "Employer Contribution %",
                    "Reporting period",
                    "Pension Provider",
                    "Employer Payment",
                    "Currency",
                    "Geographic location",
                ]
            ):
                continue

            if pd.isna(rp) and pd.isna(provider) and pd.isna(payment) and pd.isna(headcount):
                continue

            records.append(
                {
                    "Source_File": src,
                    "Reporting period (month, year)": rp,
                    "Employee Headcount": headcount,
                    "Pension Provider": provider,
                    "Employer Payment to Pension Provider": payment,
                    "Currency": currency,
                    "Data Source (pension scheme policy documentation trail)": datasrc,
                }
            )

    out_cols = [
        "Source_File",
        "Reporting period (month, year)",
        "Employee Headcount",
        "Pension Provider",
        "Employer Payment to Pension Provider",
        "Currency",
        "Data Source (pension scheme policy documentation trail)",
    ]
    out = pd.DataFrame.from_records(records, columns=out_cols)

    out = out.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all")
    out = out.drop_duplicates(ignore_index=True)
    return out[out_cols]


# ============================= MAIN ===========================================
def main(argv=None):
    global INPUT_FILE, OUTPUT_FILE
    ap = argparse.ArgumentParser(description="Stage1 clean/extract: WT + Elec + DH + S3C11 + S3C15 cleaning/extraction.")
    ap.add_argument("--input", default=INPUT_FILE, help="Input merged workbook (.xlsx)")
    ap.add_argument("--output", default=OUTPUT_FILE, help="Output cleaned/extracted workbook (.xlsx)")
    args = ap.parse_args(argv)
    INPUT_FILE = args.input
    OUTPUT_FILE = args.output

    # load workbook once, keep raw sheets header=None (preserve everything)
    xl = pd.ExcelFile(INPUT_FILE)
    sheets = {name: xl.parse(name, header=None, dtype=object) for name in xl.sheet_names}

    # ---- WATER TRACKER (dual extractor)
    wt_sheet_name = next(s for s in xl.sheet_names if "water" in s.lower())
    df_wt = sheets[wt_sheet_name]

    wt_structured = extract_wt_structured(df_wt)
    wt_rowwise = extract_wt_rowwise(df_wt)

    wt_final = pd.concat([wt_structured, wt_rowwise], ignore_index=True)
    wt_final = wt_final.drop_duplicates(
        subset=["Source_File", "Reporting period (month, year)", "Consumption of water", "Unit"]
    )

    # ---- S3C8 Electricity
    s3c8_elec_name = find_sheet(xl, "Scope 3 Cat 8 Electricity", ["scope", "3", "cat", "8", "electricity"])
    elec_result = extract_generic(
        sheets[s3c8_elec_name],
        ELEC_HEADERS,
        elec_validate_row,
        source_col_hint="source_file",
    )

    # ---- S3C8 District Heating (E/H split)
    s3c8_dh_name = find_sheet(xl, "Scope 3 Cat 8 District Heating", ["scope", "3", "cat", "8", "district", "heating"])
    dh_clean = extract_dh_blocks(sheets[s3c8_dh_name], DH_HEADERS)

    elec_out, heat_out = [], []
    if not dh_clean.empty:
        for (sf, fac), sub in dh_clean.groupby(["Source_File", "Facility"], sort=False):
            sub = sub.sort_values("__orig_idx__").reset_index(drop=True)
            elec = sub.iloc[:12].copy()
            heat = sub.iloc[12:24].copy()

            heat = heat.rename(
                columns={
                    "Electricity Consumption": "Heating Consumption",
                    "CO2e from electricity": "CO2e from district heating",
                }
            )

            elec_out.append(elec.drop(columns="__orig_idx__"))
            heat_out.append(heat.drop(columns="__orig_idx__"))

    dh_elec = (
        pd.concat(elec_out, ignore_index=True)
        if elec_out
        else pd.DataFrame(columns=["Source_File"] + DH_HEADERS)
    )
    dh_heat_cols = [
        "Source_File",
        "Facility",
        "Reporting Period",
        "Heating Consumption",
        "Consumption unit",
        "CO2e from district heating",
        "CO2e unit",
    ]
    dh_heat = (
        pd.concat(heat_out, ignore_index=True)
        if heat_out
        else pd.DataFrame(columns=dh_heat_cols)
    )

    # ---- S3C11 Products (already pre-cleaned in merge step)
    s3c11_name = find_sheet(xl, "Scope 3 Cat 11 Products Indirec", ["scope", "3", "cat", "11", "product"])
    s3c11_clean = xl.parse(s3c11_name, header=0, dtype=object)

    # ---- S3C15 Pensions
    s3c15_name = find_sheet(xl, "Scope 3 Cat 15 Pensions", ["scope", "3", "cat", "15", "pension"])
    pensions_clean = extract_pensions(sheets[s3c15_name])

    # ============================= WRITE OUTPUT ================================
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as w:
        # original sheets, untouched (except S3C11 & S3C15 replaced with cleaned versions)
        for name, df_orig in sheets.items():
            if name in {s3c11_name, s3c15_name}:
                continue
            df_orig.to_excel(w, sheet_name=name[:31], index=False, header=False)

        # WATER: extracted (gold-standard dual logic)
        wt_final.to_excel(w, sheet_name="WT_extracted", index=False)

        # S3C8 Electricity
        elec_result[["Source_File"] + ELEC_HEADERS].to_excel(
            w, sheet_name="S3C8_Electricity_extracted", index=False
        )

        # S3C8 District Heating (Electricity part, Heating part)
        dh_elec.to_excel(w, sheet_name="Scope 3 Cat 8 District E", index=False)
        dh_heat.to_excel(w, sheet_name="Scope 3 Cat 8 District H", index=False)

        # S3C11: cleaned
        s3c11_clean.to_excel(w, sheet_name=s3c11_name[:31], index=False)

        # S3C15: pensions cleaned
        pensions_clean.to_excel(w, sheet_name=s3c15_name[:31], index=False)

    # small summary in console (ASCII-only to avoid Windows console encoding crashes)
    print("FINAL EXTRACTION COMPLETED")
    print("  Water rows       :", len(wt_final))
    print("  Elec rows        :", len(elec_result))
    print("  DH Elec rows     :", len(dh_elec))
    print("  DH Heat rows     :", len(dh_heat))
    print("  Pensions rows    :", len(pensions_clean))
    print("  Output file      :", OUTPUT_FILE)


if __name__ == "__main__":
    main()
