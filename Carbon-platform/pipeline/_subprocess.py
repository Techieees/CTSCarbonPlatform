from __future__ import annotations

import subprocess
import sys
import os
from pathlib import Path
from typing import Iterable, Sequence


def run_python_script(
    script_path: Path,
    *,
    cwd: Path | None = None,
    args: Sequence[str] = (),
    dry_run: bool = False,
) -> int:
    script_path = script_path.resolve()
    if cwd is None:
        cwd = script_path.parent
    else:
        cwd = cwd.resolve()

    cmd: list[str] = [sys.executable, str(script_path), *list(args)]
    if dry_run:
        print("[dry-run]", " ".join(cmd))
        print("[dry-run] cwd:", str(cwd))
        return 0

    # Ensure UTF-8 stdio for child scripts (avoids Windows cp1252 UnicodeEncodeError
    # when scripts print non-ASCII characters and stdout is redirected).
    env = dict(os.environ)
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUNBUFFERED", "1")

    completed = subprocess.run(cmd, cwd=str(cwd), env=env)
    return int(completed.returncode)


def ensure_exists(path: Path, *, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")

