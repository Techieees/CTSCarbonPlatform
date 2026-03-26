from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from pipeline.stages import stage1_preprocess, stage2_mapping


@dataclass(frozen=True)
class OrchestratorResult:
    stage1_exit_code: int | None = None
    stage2_exit_code: int | None = None
    stage1_output: Path | None = None

    @property
    def ok(self) -> bool:
        codes = [c for c in [self.stage1_exit_code, self.stage2_exit_code] if c is not None]
        return all(c == 0 for c in codes)


def run_all(
    *,
    project_root: Path,
    stage2_args: Sequence[str] = (),
    stage1_input_folder: Path | None = None,
    stage1_work_dir: Path | None = None,
    dry_run: bool = False,
    continue_on_error: bool = False,
) -> OrchestratorResult:
    stage1_out = stage1_preprocess.run(
        project_root=project_root,
        input_folder=stage1_input_folder,
        work_dir=stage1_work_dir,
        dry_run=dry_run,
        continue_on_error=continue_on_error,
    )
    # Auto-wire Stage2 input from Stage1 final output unless user already provided --input.
    args2 = list(stage2_args)
    if "--input" not in args2:
        args2 = ["--input", str(stage1_out), *args2]

    s2 = stage2_mapping.run(project_root=project_root, args=args2, dry_run=dry_run)
    return OrchestratorResult(stage1_exit_code=0, stage2_exit_code=s2, stage1_output=stage1_out)


def run_stage1(
    *,
    project_root: Path,
    stage1_input_folder: Path | None = None,
    stage1_work_dir: Path | None = None,
    dry_run: bool = False,
    continue_on_error: bool = False,
) -> OrchestratorResult:
    stage1_out = stage1_preprocess.run(
        project_root=project_root,
        input_folder=stage1_input_folder,
        work_dir=stage1_work_dir,
        dry_run=dry_run,
        continue_on_error=continue_on_error,
    )
    return OrchestratorResult(stage1_exit_code=0, stage1_output=stage1_out)


def run_stage2(
    *,
    project_root: Path,
    stage2_args: Sequence[str] = (),
    dry_run: bool = False,
) -> OrchestratorResult:
    s2 = stage2_mapping.run(project_root=project_root, args=stage2_args, dry_run=dry_run)
    return OrchestratorResult(stage2_exit_code=s2)

