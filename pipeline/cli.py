from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline import orchestrator


def _project_root_from_this_file() -> Path:
    # Carbon-platform/pipeline/cli.py -> Carbon-platform
    return Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_pipeline",
        description=(
            "Master entry point to orchestrate Stage1 (preprocess) and Stage2 (mapping/scenario) "
            "without rewriting business logic."
        ),
    )
    p.add_argument(
        "--stage1-input-folder",
        help="Folder containing Stage1 input workbooks (company templates filled with data).",
    )
    p.add_argument(
        "--stage1-work-dir",
        help="Directory to write Stage1 chained outputs (defaults to storage/pipeline_runs/<ts>/stage1).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue Stage1 even if a step fails (Stage2 will still run).",
    )

    sub = p.add_subparsers(dest="cmd", required=False)

    p_all = sub.add_parser("all", help="Run Stage1 then Stage2 (default).")
    p_all.add_argument(
        "stage2_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to engine/stage2_mapping/Run_Everything.py (prefix with --).",
    )
    sub.add_parser("stage1", help="Run Stage1 preprocess only.")

    p2 = sub.add_parser("stage2", help="Run Stage2 mapping/scenario only.")
    p2.add_argument(
        "stage2_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to engine/stage2_mapping/Run_Everything.py (prefix with --).",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    # Allow global flags to appear after the subcommand (friendlier CLI UX).
    # Example supported:
    #   python run_pipeline.py all --dry-run
    global_flags = {"--dry-run", "--continue-on-error", "--stage1-input-folder", "--stage1-work-dir"}
    extracted: list[str] = []
    remaining: list[str] = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in {"--stage1-input-folder", "--stage1-work-dir"}:
            extracted.append(a)
            if i + 1 < len(argv):
                extracted.append(argv[i + 1])
                i += 2
                continue
        if a in global_flags:
            extracted.append(a)
        else:
            remaining.append(a)
        i += 1
    argv = extracted + remaining

    parser = build_parser()
    ns = parser.parse_args(argv)

    project_root = _project_root_from_this_file()
    cmd = ns.cmd or "all"

    if cmd == "all":
        stage2_args = list(getattr(ns, "stage2_args", []) or [])
        if stage2_args and stage2_args[0] == "--":
            stage2_args = stage2_args[1:]

        res = orchestrator.run_all(
            project_root=project_root,
            stage2_args=stage2_args,
            stage1_input_folder=Path(ns.stage1_input_folder) if getattr(ns, "stage1_input_folder", None) else None,
            stage1_work_dir=Path(ns.stage1_work_dir) if getattr(ns, "stage1_work_dir", None) else None,
            dry_run=bool(ns.dry_run),
            continue_on_error=bool(ns.continue_on_error),
        )
        return 0 if res.ok else 1

    if cmd == "stage1":
        res = orchestrator.run_stage1(
            project_root=project_root,
            stage1_input_folder=Path(ns.stage1_input_folder) if getattr(ns, "stage1_input_folder", None) else None,
            stage1_work_dir=Path(ns.stage1_work_dir) if getattr(ns, "stage1_work_dir", None) else None,
            dry_run=bool(ns.dry_run),
            continue_on_error=bool(ns.continue_on_error),
        )
        return 0 if res.ok else 1

    if cmd == "stage2":
        stage2_args = list(getattr(ns, "stage2_args", []) or [])
        # argparse.REMAINDER includes the leading "--" if user used it; strip it.
        if stage2_args and stage2_args[0] == "--":
            stage2_args = stage2_args[1:]
        res = orchestrator.run_stage2(project_root=project_root, stage2_args=stage2_args, dry_run=bool(ns.dry_run))
        return 0 if res.ok else 1

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

