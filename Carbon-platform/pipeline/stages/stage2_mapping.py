from __future__ import annotations

from pathlib import Path
from typing import Sequence

from pipeline._subprocess import ensure_exists, run_python_script


def run(
    *,
    project_root: Path,
    args: Sequence[str] = (),
    dry_run: bool = False,
) -> int:
    """
    Run Stage 2 mapping/scenario engine via its existing entry script.

    We intentionally invoke it as a script (not as an import) to preserve:
    - its working-directory assumptions
    - its internal imports like `import main_mapping`
    """
    stage2_dir = project_root / "engine" / "stage2_mapping"
    entry = stage2_dir / "Run_Everything.py"
    ensure_exists(entry, label="Stage2 entry script")

    print("\n=== Stage2: mapping/scenario engine ===")
    return run_python_script(entry, cwd=stage2_dir, args=args, dry_run=dry_run)

