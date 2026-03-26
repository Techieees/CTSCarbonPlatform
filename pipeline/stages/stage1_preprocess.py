from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

from pipeline._subprocess import ensure_exists, run_python_script


@dataclass(frozen=True)
class Stage1Scripts:
    """
    Stage 1 is currently a set of standalone scripts (some with hardcoded paths).
    We keep them untouched and just orchestrate their execution order.
    """

    merge: Path
    clean_extract: Path
    normalize: Path
    currency: Path
    translate: Path

    @staticmethod
    def default(project_root: Path) -> "Stage1Scripts":
        base = project_root / "engine" / "stage1_preprocess" / "Datas"
        return Stage1Scripts(
            merge=base / "the_chosen_one_11November.py",
            clean_extract=base / "Clean_me_the_chosen_one_2Dec.py",
            normalize=base / "normalized10.py",
            currency=base / "Currency_converter_17Dec.py",
            translate=base / "translate_me_the_chosen_one_30Sep.py",
        )


def run(
    *,
    project_root: Path,
    input_folder: Path | None = None,
    work_dir: Path | None = None,
    dry_run: bool = False,
    continue_on_error: bool = False,
    only: Sequence[str] | None = None,
) -> Path:
    """
    Run Stage 1 preprocess pipeline.

    Parameters
    - only: optional subset of steps in order, e.g. ("merge", "clean_extract")
    """
    scripts = Stage1Scripts.default(project_root)

    if work_dir is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        work_dir = project_root / "storage" / "pipeline_runs" / ts / "stage1"
    work_dir.mkdir(parents=True, exist_ok=True)

    # Pre-compute chained output paths
    merged_path = work_dir / "stage1_01_merged.xlsx"
    cleaned_path = work_dir / "stage1_02_cleaned.xlsx"
    normalized_path = work_dir / "stage1_03_normalized.xlsx"
    currency_path = work_dir / "stage1_04_currency.xlsx"
    translated_path = work_dir / "stage1_05_translated.xlsx"

    steps: list[tuple[str, Path]] = [
        ("merge", scripts.merge),
        ("clean_extract", scripts.clean_extract),
        ("normalize", scripts.normalize),
        ("currency", scripts.currency),
        ("translate", scripts.translate),
    ]

    if only is not None:
        only_set = set(only)
        steps = [s for s in steps if s[0] in only_set]

    for name, path in steps:
        ensure_exists(path, label=f"Stage1 step '{name}' script")
        print(f"\n=== Stage1: {name} ===")

        args: list[str] = []
        if name == "merge":
            if input_folder is not None:
                args += ["--input-folder", str(input_folder)]
            args += ["--output-file", str(merged_path)]
        elif name == "clean_extract":
            args += ["--input", str(merged_path), "--output", str(cleaned_path)]
        elif name == "normalize":
            args += ["--input", str(cleaned_path), "--output", str(normalized_path)]
        elif name == "currency":
            args += ["--input", str(normalized_path), "--output", str(currency_path)]
        elif name == "translate":
            args += ["--input", str(currency_path), "--output", str(translated_path)]

        rc = run_python_script(path, args=args, dry_run=dry_run)
        if rc != 0:
            msg = f"Stage1 step failed: {name} (exit_code={rc})"
            if continue_on_error:
                print("[WARN]", msg)
            else:
                raise RuntimeError(msg)

    print(f"\n[info] Stage1 final output: {translated_path}")
    return translated_path

