from __future__ import annotations

import json
import shutil
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd

from company_slug import company_slug
from config import (
    ENGINE_STAGE1_KLARAKARBON_ALL_TOGETHER_DIR,
    ENGINE_STAGE1_KLARAKARBON_OUTPUT_WORK_DIR,
    FRONTEND_DIR,
    STAGE1_KLARAKARBON_OUTPUT_DIR,
    STAGE2_TRAVEL_DIR,
)


_KLARAKARBON_LOCK = threading.Lock()
_TRAVEL_LOCK = threading.Lock()

_KLARAKARBON_TEMPLATES_PATH = FRONTEND_DIR / "data" / "klarakarbon_header_templates.json"
_TRAVEL_TEMPLATE_PATH = FRONTEND_DIR / "data" / "travel_header_template.json"

_LEGACY_KLARAKARBON_INPUT_DIR = ENGINE_STAGE1_KLARAKARBON_ALL_TOGETHER_DIR
_LEGACY_KLARAKARBON_OUTPUT_DIR = ENGINE_STAGE1_KLARAKARBON_OUTPUT_WORK_DIR
_LEGACY_KLARAKARBON_COMBINED_INPUT = _LEGACY_KLARAKARBON_OUTPUT_DIR / "combined_klarakarbon_data_20260129_170025.xlsx"
_LEGACY_KLARAKARBON_DC_INPUT = _LEGACY_KLARAKARBON_OUTPUT_DIR / "klarakarbon_double_counting_20260129_1702.xlsx"

_LEGACY_TRAVEL_DIR = Path(
    r"C:\Users\FlorianDemir\Desktop\Business Travel_MGMT\January 2025(WholeYear)"
)
_LEGACY_TRAVEL_INPUT = _LEGACY_TRAVEL_DIR / "CTS Nordics Travel Mgmt Report_travellers_2025_whole_year_source.xlsb"

_KLARAKARBON_SCRIPT_DIR = Path(__file__).resolve().parent / "engine" / "stage1_preprocess" / "Datas" / "Klarakarbon"
_TRAVEL_SCRIPT_DIR = Path(__file__).resolve().parent / "engine" / "stage1_preprocess" / "Datas" / "Business Travel_MGMT"

_KLARAKARBON_SCRIPT_1 = _KLARAKARBON_SCRIPT_DIR / "Klarakarbon_code3.py"
_KLARAKARBON_SCRIPT_2 = _KLARAKARBON_SCRIPT_DIR / "Klarakarbon_double_counting_15_December.py"
_KLARAKARBON_SCRIPT_3 = _KLARAKARBON_SCRIPT_DIR / "Klarakarbon_category_change_15_December.py"

_TRAVEL_SCRIPT_1 = _TRAVEL_SCRIPT_DIR / "extract_and_standardize_raw_data.py"
_TRAVEL_SCRIPT_2 = _TRAVEL_SCRIPT_DIR / "clean_source_data.py"
_TRAVEL_SCRIPT_3 = _TRAVEL_SCRIPT_DIR / "analysis_report.py"


def _read_klarakarbon_templates() -> dict[str, dict[str, list[str]]]:
    try:
        with _KLARAKARBON_TEMPLATES_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _read_travel_template() -> dict[str, list[str]]:
    try:
        with _TRAVEL_TEMPLATE_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def klarakarbon_company_supported(company_name: str) -> bool:
    return str(company_name or "").strip() in _read_klarakarbon_templates()


def klarakarbon_required_headers(company_name: str) -> list[str]:
    raw = _read_klarakarbon_templates().get(str(company_name or "").strip()) or {}
    headers = raw.get("required_headers") if isinstance(raw, dict) else []
    return [str(h).strip() for h in headers if str(h).strip()]


def klarakarbon_entry_headers(company_name: str) -> list[str]:
    return klarakarbon_required_headers(company_name)


def _normalize_header(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def travel_required_headers() -> list[str]:
    raw = _read_travel_template()
    headers = raw.get("required_columns") if isinstance(raw, dict) else []
    return [str(h).strip() for h in headers if str(h).strip()]


def _read_travel_source_headers(upload_path: Path) -> list[str]:
    try:
        df = pd.read_excel(upload_path, sheet_name="source", nrows=0, engine="pyxlsb")
        return [str(col).strip() for col in df.columns if str(col).strip()]
    except Exception:
        pass

    try:
        import importlib

        xw = importlib.import_module("xlwings")

        app = xw.App(visible=False)
        wb = None
        try:
            wb = xw.Book(str(upload_path))
            sheet = wb.sheets["source"]
            values = sheet.range("A1").expand().value
            if isinstance(values, list) and values:
                first_row = values[0] if isinstance(values[0], list) else values
                return [str(v).strip() for v in first_row if str(v).strip()]
            return []
        finally:
            try:
                wb.close()
            except Exception:
                pass
            app.quit()
    except Exception as exc:
        raise RuntimeError(f"could not read Travel source headers ({exc})") from exc


def validate_travel_upload(upload_path: Path) -> list[str]:
    required = travel_required_headers()
    if not required:
        return ["Travel template is not configured."]

    try:
        actual_headers = _read_travel_source_headers(upload_path)
    except Exception as exc:
        return [f"{upload_path.name}: {exc}"]

    actual_norm = {_normalize_header(h) for h in actual_headers}
    missing = [h for h in required if _normalize_header(h) not in actual_norm]
    if missing:
        return [f"{upload_path.name}: missing required columns: {', '.join(missing)}"]
    return []


def _write_status(run_dir: Path, status: str, **extra: object) -> None:
    payload = {
        "status": status,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        **extra,
    }
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "status.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


def _append_log(run_dir: Path, message: str) -> None:
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        with (run_dir / "preprocess.log").open("a", encoding="utf-8") as f:
            f.write(message.rstrip() + "\n")
    except Exception:
        pass


def validate_klarakarbon_uploads(company_name: str, upload_paths: list[Path]) -> list[str]:
    required = klarakarbon_required_headers(company_name)
    if not required:
        return [f"Klarakarbon template is not configured for {company_name}."]

    required_norm = {_normalize_header(h) for h in required}
    errors: list[str] = []

    for upload_path in upload_paths:
        matched = False
        best_headers: set[str] = set()
        try:
            xls = pd.ExcelFile(upload_path)
            for sheet_name in xls.sheet_names:
                df_raw = pd.read_excel(upload_path, sheet_name=sheet_name, header=None)
                threshold = max(3, len(required_norm))
                for i in range(len(df_raw)):
                    row = df_raw.iloc[i]
                    values = [str(v).strip() for v in row.tolist() if str(v).strip()]
                    if len(values) < threshold:
                        continue
                    normalized = {_normalize_header(v) for v in values if _normalize_header(v)}
                    if len(normalized) > len(best_headers):
                        best_headers = normalized
                    if required_norm.issubset(normalized):
                        matched = True
                        break
                if matched:
                    break
        except Exception as exc:
            errors.append(f"{upload_path.name}: could not read workbook ({exc})")
            continue

        if matched:
            continue

        missing = [h for h in required if _normalize_header(h) not in best_headers]
        if missing:
            errors.append(f"{upload_path.name}: missing required headers: {', '.join(missing)}")
        else:
            errors.append(f"{upload_path.name}: required Klarakarbon headers were not found.")

    return errors


def _run_script(script_path: Path, run_dir: Path) -> None:
    _append_log(run_dir, f"RUN {script_path.name}")
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(script_path.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.stdout:
        _append_log(run_dir, proc.stdout)
    if proc.stderr:
        _append_log(run_dir, proc.stderr)
    if proc.returncode != 0:
        raise RuntimeError(f"{script_path.name} failed with exit code {proc.returncode}")


def _clear_directory_files(target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for child in target_dir.iterdir():
        if child.is_file():
            child.unlink()


def _copy_latest(pattern: str, target_path: Path) -> None:
    matches = sorted(target_path.parent.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No file matched {pattern} in {target_path.parent}")
    shutil.copy2(matches[0], target_path)


def run_klarakarbon_preprocess(company_name: str, run_dir: Path, upload_paths: list[Path]) -> None:
    slug = company_slug(company_name)
    publish_dir = STAGE1_KLARAKARBON_OUTPUT_DIR / slug
    publish_path = publish_dir / "klarakarbon_categories_mapped_FINAL.xlsx"

    _write_status(run_dir, "running", company_name=company_name, company_slug=slug)
    with _KLARAKARBON_LOCK:
        try:
            _clear_directory_files(_LEGACY_KLARAKARBON_INPUT_DIR)
            _clear_directory_files(_LEGACY_KLARAKARBON_OUTPUT_DIR)

            for upload_path in upload_paths:
                shutil.copy2(upload_path, _LEGACY_KLARAKARBON_INPUT_DIR / upload_path.name)

            _run_script(_KLARAKARBON_SCRIPT_1, run_dir)
            _copy_latest("combined_klarakarbon_data_*.xlsx", _LEGACY_KLARAKARBON_COMBINED_INPUT)

            _run_script(_KLARAKARBON_SCRIPT_2, run_dir)
            _copy_latest("klarakarbon_double_counting_*.xlsx", _LEGACY_KLARAKARBON_DC_INPUT)

            _run_script(_KLARAKARBON_SCRIPT_3, run_dir)

            final_source = _LEGACY_KLARAKARBON_OUTPUT_DIR / "klarakarbon_categories_mapped_FINAL.xlsx"
            if not final_source.exists():
                raise FileNotFoundError(f"Expected Klarakarbon output was not found: {final_source}")

            publish_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(final_source, publish_path)

            archive_path = run_dir / "klarakarbon_categories_mapped_FINAL.xlsx"
            shutil.copy2(final_source, archive_path)
            _write_status(
                run_dir,
                "succeeded",
                company_name=company_name,
                company_slug=slug,
                publish_path=str(publish_path),
            )
        except Exception as exc:
            _append_log(run_dir, f"ERROR {exc}")
            _write_status(run_dir, "failed", company_name=company_name, company_slug=slug, error=str(exc))


def run_travel_preprocess(run_dir: Path, upload_path: Path) -> None:
    _write_status(run_dir, "running")
    with _TRAVEL_LOCK:
        try:
            _LEGACY_TRAVEL_DIR.mkdir(parents=True, exist_ok=True)
            for name in [
                "source Raw Data.xlsx",
                "cleaned_source_Raw_Data.xlsx",
                "analysis_summary.xlsx",
                "negative_km_rows.xlsx",
                _LEGACY_TRAVEL_INPUT.name,
            ]:
                candidate = _LEGACY_TRAVEL_DIR / name
                if candidate.exists():
                    candidate.unlink()

            shutil.copy2(upload_path, _LEGACY_TRAVEL_INPUT)

            _run_script(_TRAVEL_SCRIPT_1, run_dir)
            _run_script(_TRAVEL_SCRIPT_2, run_dir)
            _run_script(_TRAVEL_SCRIPT_3, run_dir)

            final_source = _LEGACY_TRAVEL_DIR / "analysis_summary.xlsx"
            if not final_source.exists():
                raise FileNotFoundError(f"Expected Travel output was not found: {final_source}")

            STAGE2_TRAVEL_DIR.mkdir(parents=True, exist_ok=True)
            publish_path = STAGE2_TRAVEL_DIR / "analysis_summary.xlsx"
            shutil.copy2(final_source, publish_path)
            shutil.copy2(final_source, run_dir / "analysis_summary.xlsx")
            _write_status(run_dir, "succeeded", publish_path=str(publish_path))
        except Exception as exc:
            _append_log(run_dir, f"ERROR {exc}")
            _write_status(run_dir, "failed", error=str(exc))
