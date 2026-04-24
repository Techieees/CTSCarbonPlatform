from __future__ import annotations

import argparse
import importlib.util
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_ROOT / "config.py"
_CONFIG_SPEC = importlib.util.spec_from_file_location("project_config_file", CONFIG_PATH)
if _CONFIG_SPEC is None or _CONFIG_SPEC.loader is None:
    raise RuntimeError(f"Unable to load config from {CONFIG_PATH}")
_CONFIG = importlib.util.module_from_spec(_CONFIG_SPEC)
_CONFIG_SPEC.loader.exec_module(_CONFIG)

FRONTEND_DB_PATH = _CONFIG.FRONTEND_DB_PATH
FRONTEND_DIR = _CONFIG.FRONTEND_DIR
FRONTEND_INSTANCE_DIR = _CONFIG.FRONTEND_INSTANCE_DIR
FRONTEND_UPLOAD_DIR = _CONFIG.FRONTEND_UPLOAD_DIR
PIPELINE_RUNS_DIR = _CONFIG.PIPELINE_RUNS_DIR
STAGE1_KLARAKARBON_OUTPUT_DIR = _CONFIG.STAGE1_KLARAKARBON_OUTPUT_DIR
STAGE1_OUTPUT_DIR = _CONFIG.STAGE1_OUTPUT_DIR
STAGE2_OUTPUT_DIR = _CONFIG.STAGE2_OUTPUT_DIR


@dataclass
class TableResetResult:
    table_name: str
    existed: bool
    deleted_rows: int


@dataclass
class FolderResetResult:
    folder: Path
    existed: bool
    deleted_files: int
    deleted_dirs: int


TABLES_TO_CLEAR = (
    "data_entry",
    "responses",
    "mapping_run",
    "mapping_run_summary",
    "pipeline_run",
)

FOLDERS_TO_CLEAR = (
    FRONTEND_UPLOAD_DIR,
    FRONTEND_DIR / "run_logs",
    FRONTEND_INSTANCE_DIR / "mapping_runs",
    PIPELINE_RUNS_DIR,
    STAGE2_OUTPUT_DIR,
    STAGE1_OUTPUT_DIR,
    STAGE1_KLARAKARBON_OUTPUT_DIR,
)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def clear_tables(db_path: Path) -> list[TableResetResult]:
    results: list[TableResetResult] = []
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        for table_name in TABLES_TO_CLEAR:
            if not _table_exists(conn, table_name):
                results.append(TableResetResult(table_name=table_name, existed=False, deleted_rows=0))
                continue
            deleted_rows = int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
            conn.execute(f"DELETE FROM {table_name}")
            results.append(TableResetResult(table_name=table_name, existed=True, deleted_rows=deleted_rows))
        conn.commit()
    finally:
        conn.close()
    return results


def clear_folder_contents(folder: Path) -> FolderResetResult:
    deleted_files = 0
    deleted_dirs = 0
    if not folder.exists():
        return FolderResetResult(folder=folder, existed=False, deleted_files=0, deleted_dirs=0)

    for child in list(folder.iterdir()):
        if child.is_dir():
            shutil.rmtree(child)
            deleted_dirs += 1
        else:
            child.unlink(missing_ok=True)
            deleted_files += 1
    return FolderResetResult(folder=folder, existed=True, deleted_files=deleted_files, deleted_dirs=deleted_dirs)


def clear_folders() -> list[FolderResetResult]:
    results: list[FolderResetResult] = []
    for folder in FOLDERS_TO_CLEAR:
        folder.mkdir(parents=True, exist_ok=True)
        results.append(clear_folder_contents(folder))
    return results


def print_summary(table_results: list[TableResetResult], folder_results: list[FolderResetResult]) -> None:
    print("Tables cleaned:")
    for result in table_results:
        if result.existed:
            print(f"- {result.table_name}: deleted {result.deleted_rows} rows")
        else:
            print(f"- {result.table_name}: not found")

    print("\nFolders cleaned:")
    for result in folder_results:
        rel = result.folder
        if result.existed:
            print(f"- {rel}: removed {result.deleted_files} files and {result.deleted_dirs} directories")
        else:
            print(f"- {rel}: not found (created empty)")

    print("\nUntouched:")
    print("- user table")
    print("- authentication data")
    print("- profile data")
    print("- profile photos")
    print("- company logos")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset app data for 2026 testing without touching users or schema.")
    parser.add_argument("--yes", action="store_true", help="Execute the reset.")
    args = parser.parse_args()

    if not args.yes:
        print("Dry run only. Re-run with --yes to execute.")
        print("Tables that would be cleaned:", ", ".join(TABLES_TO_CLEAR))
        print("Folders that would be cleaned:")
        for folder in FOLDERS_TO_CLEAR:
            print(f"- {folder}")
        return 0

    table_results = clear_tables(FRONTEND_DB_PATH)
    folder_results = clear_folders()
    print_summary(table_results, folder_results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
