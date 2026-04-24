from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TEMPLATE_MODE_LEGACY = "legacy"
TEMPLATE_MODE_2026 = "2026"
VALID_TEMPLATE_MODES = frozenset({TEMPLATE_MODE_LEGACY, TEMPLATE_MODE_2026})

DEPRECATED_2026_SHEETS = frozenset({
    "Scope 1 Fugitive Gases",
    "Scope 1 Gas Usage",
})

CATEGORY_11_TEMPLATE_NAME = "Scope 3 Category 11 Use of Sold Product"

_CATEGORY_SHEET_NAMES: dict[str, str] = {
    "9": "Scope 3 Category 9 Downstream Transportation",
    "11": CATEGORY_11_TEMPLATE_NAME,
    "12": "Scope 3 Category 12 End of Life",
}

_PIPING_ALIASES = {
    "piping": "Piping",
    "dc piping": "Piping",
    "pipe": "Piping",
    "pipes": "Piping",
}


def normalize_template_mode(value: object) -> str:
    raw = str(value or "").strip()
    return raw if raw in VALID_TEMPLATE_MODES else TEMPLATE_MODE_LEGACY


def _normalize_key(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_product_type(value: object) -> str:
    raw = " ".join(str(value or "").strip().split())
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered == "nordicepod":
        return "NordicEPOD"
    return _PIPING_ALIASES.get(lowered, raw)


def _load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


@dataclass(frozen=True)
class MetadataIssue:
    sheet_name: str
    missing_fields: list[str]
    severity: str

    def to_dict(self) -> dict[str, object]:
        return {
            "sheet_name": self.sheet_name,
            "missing_fields": list(self.missing_fields),
            "severity": self.severity,
        }


class TemplateRegistry:
    def __init__(self, *, templates2026_path: Path) -> None:
        self.templates2026_path = Path(templates2026_path)
        self.templates_2026 = self._load_2026_templates()

    def _load_2026_templates(self) -> dict[str, dict[str, object]]:
        raw = _load_json(self.templates2026_path)
        templates: dict[str, dict[str, object]] = {}
        if not isinstance(raw, dict):
            return templates
        for sheet_name, cfg in raw.items():
            if not isinstance(sheet_name, str) or not isinstance(cfg, dict):
                continue
            clean_sheet = sheet_name.strip()
            if not clean_sheet:
                continue
            templates[clean_sheet] = dict(cfg)
        return templates

    def resolve_sheet_name(self, company_name: str, sheet_name: str, *, template_mode: str) -> str | None:
        target = str(sheet_name or "").strip()
        if not target:
            return None
        for template in self.get_visible_templates(template_mode=template_mode):
            if _normalize_key(template["sheet_name"]) == _normalize_key(target):
                return str(template["sheet_name"])
        return None

    def _category_enablement(self, profile: dict[str, object] | None = None) -> dict[str, object]:
        payload = profile or {}
        business_type = str(payload.get("business_type") or "").strip()
        product_type = normalize_product_type(payload.get("product_type"))
        enabled: list[str] = []
        disabled: list[str] = []
        reasons: dict[str, str] = {}

        if business_type == "Manufacturer":
            if product_type == "NordicEPOD":
                enabled = ["9", "11", "12"]
                for category in enabled:
                    reasons[category] = "manufacturer_product_type_nordicepod"
            elif product_type == "Piping":
                enabled = ["9", "12"]
                disabled = ["11"]
                reasons["9"] = "manufacturer_product_type_piping"
                reasons["12"] = "manufacturer_product_type_piping"
                reasons["11"] = "manufacturer_product_type_piping_disabled"

        return {
            "enabled_categories": enabled,
            "disabled_categories": disabled,
            "reason_by_category": reasons,
            "normalized_product_type": product_type,
        }

    def _metadata_issues_for_sheet(self, sheet_name: str, cfg: dict[str, object]) -> list[MetadataIssue]:
        issues: list[MetadataIssue] = []
        missing_required: list[str] = []

        if not isinstance(cfg.get("columns"), list):
            missing_required.append("columns")
        if "mapping_keys" not in cfg or not isinstance(cfg.get("mapping_keys"), list):
            missing_required.append("mapping_keys")
        if not str(cfg.get("calculation_type") or "").strip():
            missing_required.append("calculation_type")

        calc_type = str(cfg.get("calculation_type") or "").strip()
        if calc_type == "conditional":
            if not isinstance(cfg.get("calculation_rules"), dict):
                missing_required.append("calculation_rules")
        elif calc_type and "calculation_fields" not in cfg:
            missing_required.append("calculation_fields")

        if missing_required:
            issues.append(MetadataIssue(sheet_name=sheet_name, missing_fields=missing_required, severity="error"))

        override_fields = cfg.get("override_fields")
        override_logic = str(cfg.get("override_logic") or "").strip()
        override_missing: list[str] = []
        if override_fields is not None and not isinstance(override_fields, list):
            override_missing.append("override_fields")
        if override_fields and not override_logic:
            override_missing.append("override_logic")
        if override_logic and not isinstance(override_fields, list):
            override_missing.append("override_fields")
        if override_missing:
            issues.append(MetadataIssue(sheet_name=sheet_name, missing_fields=override_missing, severity="warning"))

        return issues

    def _collect_metadata_validation(
        self,
        *,
        template_mode: str,
        visible_templates: list[dict[str, object]],
        enabled_categories: list[str],
    ) -> list[dict[str, object]]:
        if normalize_template_mode(template_mode) != TEMPLATE_MODE_2026:
            return []

        issues: list[dict[str, object]] = []
        for template in visible_templates:
            sheet_name = str(template.get("sheet_name") or "")
            cfg = self.templates_2026.get(sheet_name)
            if not isinstance(cfg, dict):
                issues.append(
                    MetadataIssue(
                        sheet_name=sheet_name,
                        missing_fields=["template_definition"],
                        severity="error",
                    ).to_dict()
                )
                continue
            issues.extend(issue.to_dict() for issue in self._metadata_issues_for_sheet(sheet_name, cfg))

        if "11" in enabled_categories and CATEGORY_11_TEMPLATE_NAME not in self.templates_2026:
            issues.append(
                MetadataIssue(
                    sheet_name=CATEGORY_11_TEMPLATE_NAME,
                    missing_fields=["template_definition"],
                    severity="warning",
                ).to_dict()
            )
        return issues

    def _category_code_for_sheet(self, sheet_name: str) -> str | None:
        compact = re.sub(r"\s+", " ", str(sheet_name or "").strip()).lower()
        if "category 9" in compact:
            return "9"
        if "category 11" in compact:
            return "11"
        if "category 12" in compact:
            return "12"
        return None

    def get_visible_templates(
        self,
        *,
        template_mode: str,
        company_name: str = "",
        profile: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        enablement = self._category_enablement(profile)
        enabled_categories = set(enablement["enabled_categories"])
        visible_templates: list[dict[str, object]] = []
        for sheet_name, cfg in self.templates_2026.items():
            if sheet_name in DEPRECATED_2026_SHEETS:
                continue
            category_code = self._category_code_for_sheet(sheet_name)
            if category_code in {"9", "11", "12"} and category_code not in enabled_categories:
                continue
            visible_templates.append(
                {
                    "sheet_name": sheet_name,
                    "headers": list(cfg.get("columns") or []),
                    "mapping_keys": list(cfg.get("mapping_keys") or []),
                    "calculation_type": str(cfg.get("calculation_type") or ""),
                    "category_code": category_code,
                    "template_mode": TEMPLATE_MODE_2026,
                }
            )
        return visible_templates

    def get_bundle(
        self,
        *,
        template_mode: str,
        company_name: str = "",
        profile: dict[str, object] | None = None,
    ) -> dict[str, object]:
        mode = normalize_template_mode(template_mode)
        enablement = self._category_enablement(profile)
        visible_templates = self.get_visible_templates(
            template_mode=mode,
            company_name=company_name,
            profile=profile,
        )
        metadata_validation = self._collect_metadata_validation(
            template_mode=mode,
            visible_templates=visible_templates,
            enabled_categories=list(enablement["enabled_categories"]),
        )
        return {
            "template_mode": mode,
            "visible_templates": visible_templates,
            "enabled_categories": list(enablement["enabled_categories"]),
            "disabled_categories": list(enablement["disabled_categories"]),
            "category_reasons": dict(enablement["reason_by_category"]),
            "metadata_validation": metadata_validation,
            "deprecated_sheets_ignored": sorted(DEPRECATED_2026_SHEETS) if mode == TEMPLATE_MODE_2026 else [],
        }

