"""
Endpoint-aware workspace context panels (continue / related / status / recent).
Pure layout logic — URLs and stats are supplied by the Flask app context processor.
"""

from __future__ import annotations

from typing import Any

# Dense operational pages: keep rail visually light.
_COMPACT_ENDPOINTS = frozenset(
    {
        "home",
        "feed",
        "carbon_accounting",
        "mapping_runs",
        "admin_mapping_runs",
        "mapping_review_page",
        "emissions_records_page",
        "data_sources_klarakarbon",
        "locations",
        "settings_profile_page",
    }
)

# Pages that ship their own dense chrome; no rail.
_SUPPRESS_ENDPOINTS = frozenset(
    {
        "dashboard",
        "mapping_preview_detail",
        "mapping_previews_page",
        "admin_mapping_panel",
        "admin",
        "profile_setup",
    }
)


def build_workspace_panels(
    endpoint: str | None,
    *,
    is_admin: bool,
    can_suppliers: bool,
    stats: dict[str, Any] | None,
    urls: dict[str, str],
) -> dict[str, Any] | None:
    ep = (endpoint or "").strip()
    if not ep or ep in _SUPPRESS_ENDPOINTS:
        return None

    stats = stats or {}
    continue_items: list[dict[str, str]] = []
    related_items: list[dict[str, str]] = []
    status_items: list[dict[str, str]] = []
    recent_items: list[dict[str, str]] = []

    def link(key: str, label: str, meta: str = "") -> dict[str, str]:
        href = urls.get(key) or "#"
        item: dict[str, str] = {"label": label, "href": href}
        if meta:
            item["meta"] = meta
        return item

    if ep == "home":
        continue_items = [
            link("dashboard", "Data entry & uploads"),
            link("mapping_runs", "Mapping runs", stats.get("mapping_runs_meta", "")),
            link("mapping_review", "Mapping review"),
        ]
        related_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("emissions_records", "Emissions records"),
            link("analytics_totals", "Totals & outputs"),
            link("feed", "Team feed"),
            link("methodology", "Methodology reference"),
            link("locations", "Climate risk map"),
        ]
        if stats.get("last_run_label"):
            status_items.append(
                {
                    "label": "Latest mapping run",
                    "value": stats["last_run_label"],
                    "href": urls.get("mapping_runs", "#"),
                }
            )

    elif ep == "feed":
        continue_items = [
            link("dashboard", "Data entry"),
            link("reports", "Reports"),
            link("mapping_runs", "Mapping runs"),
        ]
        related_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("mapping_runs", "Mapping runs"),
            link("events", "Events"),
            link("newsletters", "Newsletters"),
            link("audit_2025", "Audit 2025 workspace"),
        ]

    elif ep in ("mapping_runs", "admin_mapping_runs"):
        continue_items = [
            link("dashboard", "Data entry"),
            link("mapping_previews", "Mapping previews"),
            link("mapping_review", "Mapping review"),
        ]
        related_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("analytics_mapped", "Mapped window output"),
            link("emissions_records", "Emissions records"),
        ]
        if stats.get("runs_summary"):
            status_items.append(
                {"label": "Runs", "value": stats["runs_summary"], "href": urls.get("mapping_runs", "#")}
            )

    elif ep == "mapping_review_page":
        continue_items = [
            link("mapping_previews", "Mapping previews"),
            link("mapping_runs", "Mapping runs"),
            link("emissions_records", "Emissions records"),
        ]
        related_items = [
            link("mapping_review_pending", "Rows needing review", "Filter: Pending / Needs review"),
            link("governance_audit", "Audit-ready output"),
            link("analytics_mapped", "Mapped window output"),
            link("dashboard", "Data entry"),
        ]
        if stats.get("review_queue_label"):
            status_items.append(
                {
                    "label": "Review queue",
                    "value": stats["review_queue_label"],
                    "href": urls.get("mapping_review_pending", urls.get("mapping_review", "#")),
                }
            )
        if stats.get("last_run_label"):
            status_items.append(
                {
                    "label": "Latest run",
                    "value": stats["last_run_label"],
                    "href": urls.get("mapping_preview_latest", urls.get("mapping_previews", "#")),
                }
            )

    elif ep == "carbon_accounting":
        continue_items = [
            link("emissions_records", "Emissions records"),
            link("scope1", "Scope 1"),
            link("scope3", "Scope 3"),
        ]
        related_items = [
            link("mapping_runs", "Mapping runs"),
            link("analytics_totals", "Totals tables"),
            link("analytics_map", "Emissions map"),
            link("governance_audit", "Audit-ready output"),
            link("home", "Executive overview"),
        ]

    elif ep == "emissions_records_page":
        continue_items = [
            link("mapping_review", "Mapping review"),
            link("mapping_runs", "Mapping runs"),
            link("governance_audit", "Audit-ready output"),
        ]
        related_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("analytics_totals", "Totals tables"),
            link("analytics_map", "Emissions map"),
            link("dashboard", "Data entry"),
        ]
        if is_admin:
            related_items.append(link("mapping_reconciliation", "Mapping reconciliation"))
        if stats.get("records_status_label"):
            status_items.append(
                {
                    "label": "Ledger",
                    "value": stats["records_status_label"],
                    "href": urls.get("emissions_records", "#"),
                }
            )

    elif ep in ("scope1_detail", "scope2_detail", "scope3_detail"):
        continue_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("emissions_records", "Emissions records"),
            link("dashboard", "Data entry"),
        ]
        related_items = [
            link("methodology_scope", "Methodology for this scope"),
            link("analytics_totals", "Analytics outputs"),
            link("mapping_runs", "Mapping runs"),
        ]

    elif ep == "data_sources_klarakarbon":
        continue_items = [
            link("mapping_runs", "Mapping runs"),
            link("dashboard", "Data entry"),
        ]
        if not stats.get("kk_output_ready"):
            continue_items.insert(0, link("data_output_klarakarbon", "Klarakarbon output"))
        related_items = [
            link("mapping_previews", "Mapping previews"),
            link("analytics_mapped", "Mapped window output"),
            link("carbon_accounting", "Carbon accounting"),
            link("admin_jobs", "Background jobs", "Preprocess job status"),
        ]
        if stats.get("last_run_label"):
            status_items.append(
                {
                    "label": "Latest mapping run",
                    "value": stats["last_run_label"],
                    "href": urls.get("mapping_runs", "#"),
                }
            )

    elif ep in (
        "data_sources_ccc_api",
        "engage_waste_api_page",
        "data_sources_employee_commuting",
        "sustainability.sustainability_estimation_hub",
        "sustainability.sustainability_products_page",
        "products_input_page",
    ):
        continue_items = [
            link("dashboard", "Data entry uploads"),
            link("mapping_runs", "Run mapping"),
        ]
        related_items = [
            link("mapping_previews", "Mapping previews"),
            link("analytics_mapped", "Mapped outputs"),
            link("carbon_accounting", "Carbon accounting"),
        ]
        if ep == "sustainability.sustainability_estimation_hub":
            related_items.insert(0, link("sustainability_products", "Products workflow"))

    elif ep in (
        "analytics_forecasting",
        "analytics_decarbonization",
        "analytics_emissions_map",
        "analytics_mapped_window_output",
        "analytics_emissions_totals",
        "analytics_share_analysis",
        "data_output_travel",
        "data_output_klarakarbon",
    ):
        continue_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("emissions_records", "Emissions records"),
        ]
        related_items = [
            link("mapping_runs", "Mapping runs"),
            link("governance_audit", "Audit-ready output"),
            link("home", "Executive overview"),
        ]

    elif ep in ("reports_page", "report_detail", "newsletters_page", "events_page", "audit_2025_page"):
        continue_items = [
            link("feed", "Share on feed"),
            link("carbon_accounting", "Carbon accounting"),
        ]
        related_items = [
            link("dashboard", "Data entry"),
            link("mapping_review", "Mapping review"),
            link("governance_audit", "Audit-ready output"),
        ]

    elif ep in ("csrd", "csrd_policies", "lca", "lca_tool"):
        continue_items = [
            link("governance_audit", "Audit-ready output"),
            link("carbon_accounting", "Carbon accounting"),
        ]
        related_items = [
            link("reports", "Reports library"),
            link("methodology", "Methodology"),
            link("feed", "Team feed"),
        ]

    elif ep == "locations":
        continue_items = [
            link("analytics_map", "Emissions map", "Operational emissions geography"),
            link("home", "Emissions overview"),
        ]
        related_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("methodology", "Methodology reference"),
            link("dashboard", "Data entry"),
            link("audit_2025", "Audit workspace"),
        ]

    elif ep == "settings_profile_page":
        continue_items = [
            link("dashboard", "Data entry"),
            link("mapping_runs", "Mapping runs"),
            link("carbon_accounting", "Carbon accounting"),
            link("feed", "Feed"),
        ]
        related_items = [
            link("emissions_records", "Emissions records"),
            link("mapping_review", "Mapping review"),
            link("reports", "Reports"),
            link("home", "Overview"),
        ]
        for item in stats.get("recent_pages") or []:
            if isinstance(item, dict) and item.get("label") and item.get("href"):
                recent_items.append(
                    {"label": str(item["label"]), "href": str(item["href"])}
                )

    elif ep == "public_profile":
        continue_items = [
            link("settings_profile", "Profile settings"),
            link("dashboard", "Your data entry"),
        ]
        related_items = [
            link("feed", "Feed"),
            link("reports", "Reports"),
        ]

    elif ep == "sustainability.sustainability_estimation_run_detail":
        continue_items = [
            link("sustainability_estimation", "Estimation hub"),
            link("dashboard", "Data entry"),
        ]
        related_items = [
            link("sustainability_products", "Products"),
            link("mapping_runs", "Mapping runs"),
        ]

    else:
        continue_items = [
            link("home", "Overview"),
            link("dashboard", "Data entry"),
            link("feed", "Feed"),
        ]
        related_items = [
            link("carbon_accounting", "Carbon accounting"),
            link("mapping_runs", "Mapping runs"),
        ]

    if not continue_items and not related_items and not status_items and not recent_items:
        return None

    panels: dict[str, Any] = {
        "continue": continue_items[:3],
        "related": related_items[:4],
        "status": status_items[:1],
    }
    if recent_items:
        panels["recent"] = recent_items[:4]
    if ep in _COMPACT_ENDPOINTS:
        panels["compact"] = True
    return panels
