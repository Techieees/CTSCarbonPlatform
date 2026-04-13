#!/usr/bin/env python3
"""
Purge all users (and dependent user-owned rows) from the SQLite database.

This is intended for local/dev recovery when you are locked out of admin.
It will create a timestamped backup of the DB before deleting anything.
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import FRONTEND_DB_PATH


def _default_db_path() -> Path:
    return FRONTEND_DB_PATH


def _backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_suffix(db_path.suffix + f".bak_{ts}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None


def purge_users(db_path: Path, keep_user_owned_data: bool) -> None:
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    backup_path = _backup_db(db_path)
    print(f"Backup created: {backup_path}")

    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA foreign_keys=ON;")
        cur = con.cursor()

        # Dependent tables (must be deleted first if FK enforcement is on)
        dependent_tables = ["form_submission", "pipeline_run"]

        with con:
            deleted = []

            if not keep_user_owned_data:
                for t in dependent_tables:
                    if _table_exists(cur, t):
                        cur.execute(f'DELETE FROM "{t}"')
                        deleted.append(t)

            if _table_exists(cur, "user"):
                cur.execute('DELETE FROM "user"')
                deleted.append("user")

            # Reset AUTOINCREMENT counters where applicable
            if _table_exists(cur, "sqlite_sequence"):
                for t in deleted:
                    cur.execute("DELETE FROM sqlite_sequence WHERE name=?", (t,))

        print("Cleared tables:", ", ".join(deleted) if deleted else "(none)")
        print("Done. You can create an admin account with create_admin.py.")
    finally:
        con.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Remove all users (and dependent rows) from the SQLite database.")
    p.add_argument(
        "--db",
        default=str(_default_db_path()),
        help="Path to the database file (default: SQLite path from central config)",
    )
    p.add_argument(
        "--keep-user-owned-data",
        action="store_true",
        help="Do not delete user-owned mapping rows (only clear the user table).",
    )
    args = p.parse_args()

    purge_users(Path(args.db), keep_user_owned_data=bool(args.keep_user_owned_data))


if __name__ == "__main__":
    main()
