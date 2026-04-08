from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _contains(value: Any, needle: str) -> bool:
    return needle in str(value or "").strip().lower()


def _safe_limit(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return items[: max(1, int(limit))]


def _search_companies(CompanyModel: Any, query: str, limit: int) -> list[dict[str, Any]]:
    rows = (
        CompanyModel.query.order_by(CompanyModel.company_name.asc()).limit(500).all()
        if CompanyModel is not None
        else []
    )
    return _safe_limit(
        [
            {
                "title": row.company_name,
                "subtitle": getattr(row, "company_country", None) or "Company",
                "link": "/profile",
                "kind": "company",
            }
            for row in rows
            if _contains(getattr(row, "company_name", ""), query)
            or _contains(getattr(row, "company_country", ""), query)
        ],
        limit,
    )


def _search_emission_factors(EmissionFactorModel: Any, query: str, limit: int) -> list[dict[str, Any]]:
    rows = (
        EmissionFactorModel.query.order_by(EmissionFactorModel.category.asc()).limit(800).all()
        if EmissionFactorModel is not None
        else []
    )
    results: list[dict[str, Any]] = []
    for row in rows:
        text = " ".join(
            [
                str(getattr(row, "category", "") or ""),
                str(getattr(row, "subcategory", "") or ""),
                str(getattr(row, "description", "") or ""),
                str(getattr(row, "unit", "") or ""),
            ]
        ).lower()
        if query not in text:
            continue
        results.append(
            {
                "title": getattr(row, "description", None) or getattr(row, "subcategory", None) or "Emission factor",
                "subtitle": " | ".join(
                    [
                        str(getattr(row, "category", "") or "").strip(),
                        str(getattr(row, "subcategory", "") or "").strip(),
                        str(getattr(row, "unit", "") or "").strip(),
                    ]
                ).strip(" |"),
                "link": "/emission_factors",
                "kind": "emission_factor",
            }
        )
    return _safe_limit(results, limit)


def _recent_excel_files(*directories: Path, limit: int = 8) -> list[Path]:
    files: list[Path] = []
    for directory in directories:
        if not directory.exists():
            continue
        files.extend(directory.glob("*.xlsx"))
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return files[:limit]


def _search_excel_columns(files: list[Path], query: str, *, column_hints: tuple[str, ...], kind: str, limit: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for file_path in files:
        try:
            workbook = pd.ExcelFile(file_path)
        except Exception:
            continue
        for sheet_name in workbook.sheet_names[:4]:
            try:
                frame = pd.read_excel(file_path, sheet_name=sheet_name, nrows=200)
            except Exception:
                continue
            frame.columns = [str(col) for col in frame.columns]
            for column in frame.columns:
                lowered = column.lower()
                if not any(hint in lowered for hint in column_hints):
                    continue
                for value in frame[column].dropna().astype(str).head(100):
                    if query not in value.lower():
                        continue
                    key = f"{kind}:{value.strip().lower()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(
                        {
                            "title": value.strip(),
                            "subtitle": f"{file_path.name} | {sheet_name}",
                            "link": None,
                            "kind": kind,
                        }
                    )
                    if len(results) >= limit:
                        return results
    return results


def _search_api_history(run_logs_dir: Path, query: str, limit: int) -> list[dict[str, Any]]:
    log_path = run_logs_dir / "ccc_api_sync.json"
    if not log_path.exists():
        return []
    try:
        payload = json.loads(log_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    results: list[dict[str, Any]] = []
    for row in reversed(payload[-100:]):
        haystack = " ".join(
            [
                str(row.get("endpoint", "") or ""),
                str(row.get("output_file", "") or row.get("output_filename", "") or ""),
                str(row.get("base_url", "") or ""),
                str(row.get("message", "") or ""),
            ]
        ).lower()
        if query not in haystack:
            continue
        results.append(
            {
                "title": row.get("endpoint") or "CCC API import",
                "subtitle": row.get("output_file") or row.get("output_filename") or row.get("message") or "CCC API history entry",
                "link": "/data-sources/ccc-api",
                "kind": "api_import",
            }
        )
        if len(results) >= limit:
            break
    return results


def search_all(
    query: str,
    *,
    CompanyModel: Any = None,
    UserModel: Any = None,
    EmissionFactorModel: Any = None,
    stage1_input_dir: str | Path | None = None,
    stage2_output_dir: str | Path | None = None,
    run_logs_dir: str | Path | None = None,
    per_group_limit: int = 8,
) -> dict[str, list[dict[str, Any]]]:
    needle = str(query or "").strip().lower()
    if not needle:
        return {}

    stage1_dir = Path(stage1_input_dir) if stage1_input_dir else Path(".")
    stage2_dir = Path(stage2_output_dir) if stage2_output_dir else Path(".")
    logs_dir = Path(run_logs_dir) if run_logs_dir else Path(".")
    excel_files = _recent_excel_files(stage1_dir, stage2_dir)

    groups = {
        "Companies": _search_companies(CompanyModel, needle, per_group_limit),
        "Emission factors": _search_emission_factors(EmissionFactorModel, needle, per_group_limit),
        "Suppliers": _search_excel_columns(
            excel_files,
            needle,
            column_hints=("supplier", "vendor"),
            kind="supplier",
            limit=per_group_limit,
        ),
        "Projects": _search_excel_columns(
            excel_files,
            needle,
            column_hints=("project", "cost center"),
            kind="project",
            limit=per_group_limit,
        ),
        "Datasets": [
            {
                "title": path.name,
                "subtitle": str(path.parent.name),
                "link": "/analytics/emissions-totals" if path.parent.name == "output" else "/data-sources/ccc-api",
                "kind": "dataset",
            }
            for path in excel_files
            if needle in path.name.lower()
        ][:per_group_limit],
        "API imports": _search_api_history(logs_dir, needle, per_group_limit),
    }
    return {name: rows for name, rows in groups.items() if rows}
