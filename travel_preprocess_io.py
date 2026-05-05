from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

import pandas as pd


TRAVEL_ALLOWED_EXTENSIONS = frozenset({".xlsb", ".xlsx"})


def travel_excel_engine(path: str | Path) -> str:
    file_path = Path(path)
    ext = file_path.suffix.lower()
    if ext == ".xlsb":
        return "pyxlsb"
    if ext == ".xlsx":
        return "openpyxl"
    raise RuntimeError("Only .xlsb or .xlsx files are allowed")


def read_travel_excel(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    file_path = Path(path)
    engine = travel_excel_engine(file_path)
    print(f"[TRAVEL] Reading file: {file_path.name}")
    print(f"[TRAVEL] Detected extension: {file_path.suffix.lower()}")
    print(f"[TRAVEL] Selected engine: {engine}")
    try:
        return pd.read_excel(file_path, engine=engine, **kwargs)
    except Exception as exc:
        print(f"[TRAVEL] Failed to read file {file_path}: {exc}")
        print(traceback.format_exc())
        raise RuntimeError("Failed to read file. Ensure file is valid (.xlsb or .xlsx).") from exc

