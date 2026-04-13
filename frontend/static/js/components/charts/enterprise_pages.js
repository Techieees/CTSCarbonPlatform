/**
 * Enterprise-style dashboard overlays: home, admin analytics, admin report.
 * Uses only client-side aggregation of existing template JSON / DOM meta.
 */

import { readJsonScript, buildAnalyticsRows, pickAvailablePayloadId } from "../../charts/analytics_rows.js";
import {
  monthlyTotals,
  scopeTotals,
  companyTotals,
  categoryTotals,
  monthlyScopeStackSeries,
  monthlyTopCategoryStack,
  scopeCategoryHeatmap
} from "../../charts/row_aggregates.js";
import { whenVisible, showEmptyState } from "./BaseChart.js";
import { applyKpiValues } from "./KpiCard.js";
import { mountStackedBarChart } from "./StackedBarChart.js";
import { mountDonutChart } from "./DonutChart.js";
import { mountCompanyRankingChart } from "./CompanyRankingChart.js";
import { mountMonthlyTrendChart } from "./MonthlyTrendChart.js";
import { mountSunburstChart } from "./SunburstChart.js";
import { mountHeatmapChart } from "./HeatmapChart.js";
import { mountCategoryContributionChart } from "./CategoryContributionChart.js";
import { mountMultiLineChart } from "./MultiLineChart.js";
import { mountHorizontalBarChart } from "./HorizontalBarChart.js";
import { formatFull, formatCompact, getAxisLabel, getTooltipBase, initChart, withOpacity } from "../../charts/echarts_theme.js";
import { getColorByKey } from "../../charts/chart_colors.js";

function nf(n) {
  return formatFull(Number(n || 0));
}

function computeHomeKpis(payload) {
  const t = payload.totals || {};
  let categories = "—";
  let months = "—";
  let companies = "—";
  if (payload.isAdmin && Array.isArray(payload.companyRows)) {
    companies = String(payload.companyRows.length);
  } else if (!payload.isAdmin) {
    companies = "1";
  }
  if (Array.isArray(payload.breakdown) && payload.breakdown.length) {
    categories = String(payload.breakdown.length);
    const u = new Set();
    payload.breakdown.forEach((r) => {
      const m = String(r.updated_at || "").slice(0, 7);
      if (m && m.length === 7) {
        u.add(m);
      }
    });
    months = u.size ? String(u.size) : "—";
  }
  return {
    kpi_total: `${nf(t.total)} tCO₂e`,
    kpi_s1: `${nf(t.scope1)} tCO₂e`,
    kpi_s2: `${nf(t.scope2)} tCO₂e`,
    kpi_s3: `${nf(t.scope3)} tCO₂e`,
    kpi_companies: companies,
    kpi_categories: categories,
    kpi_months: months
  };
}

function mountFlatTreemap(container, items, height = 320) {
  if (!items.length) {
    showEmptyState(container, "No hierarchy data for this view.");
    return null;
  }
  const chart = initChart(container);
  if (!chart) {
    return null;
  }
  chart.setOption({
    animation: true,
    animationDuration: 880,
    tooltip: {
      ...getTooltipBase((params) => `<div><strong>${params.name}</strong><div>${formatFull(params.value)} tCO₂e</div></div>`),
      trigger: "item"
    },
    series: [
      {
        type: "treemap",
        roam: false,
        breadcrumb: { show: false },
        nodeClick: false,
        label: { show: true, fontSize: 11, fontWeight: 600 },
        itemStyle: { borderColor: "#fff", borderWidth: 1, gapWidth: 2 },
        data: items.map((item) => ({
          name: item.name,
          value: item.value,
          itemStyle: { color: withOpacity(getColorByKey(item.name, item.colorKind || "company"), 0.92) }
        }))
      }
    ]
  });
  chart.resize({ height });
  return chart;
}

function heatmapCellsScopeTinted(scopes, rawData, maxValue) {
  const maxV = maxValue || 1;
  return rawData.map(([cx, sy, v]) => {
    const val = Number(v || 0);
    const sc = scopes[sy] || "";
    const base = getColorByKey(sc, "scope");
    const t = maxV > 0 ? Math.min(1, val / maxV) : 0;
    const color = val <= 0 ? "rgba(148,163,184,0.14)" : withOpacity(base, 0.18 + t * 0.82);
    return {
      value: [cx, sy, val],
      itemStyle: { color }
    };
  });
}

function mountScopeCategoryMatrix(container, rows, height = 340) {
  const catRank = categoryTotals(rows);
  const allow = new Set(catRank.slice(0, 24).map(([c]) => c));
  const filtered = rows.filter((r) => allow.has(r.category || r.template));
  const { scopes, categories, data, maxValue } = scopeCategoryHeatmap(filtered.length ? filtered : rows);
  if (!categories.length) {
    showEmptyState(container, "No category dimension for matrix view.");
    return null;
  }
  const chart = initChart(container);
  if (!chart) {
    return null;
  }
  const styledData = heatmapCellsScopeTinted(scopes, data, maxValue);
  chart.setOption({
    animation: true,
    tooltip: {
      ...getTooltipBase((params) => {
        const [cx, sy, val] = params.value || [];
        const cat = categories[cx] || "";
        const sc = scopes[sy] || "";
        return `<div><div style="font-weight:600">${sc}</div><div>${cat}</div><strong>${formatFull(val)} tCO₂e</strong></div>`;
      }),
      trigger: "item"
    },
    grid: { top: 16, right: 16, bottom: 48, left: 120, containLabel: false },
    xAxis: {
      type: "category",
      data: categories,
      splitArea: { show: false },
      axisLabel: { ...getAxisLabel((v) => v), rotate: 35, width: 90, overflow: "truncate" }
    },
    yAxis: {
      type: "category",
      data: scopes,
      splitArea: { show: false },
      axisLabel: getAxisLabel((v) => v)
    },
    series: [
      {
        type: "heatmap",
        data: styledData,
        label: { show: false },
        emphasis: { itemStyle: { shadowBlur: 12, shadowColor: "rgba(0,0,0,.12)" } }
      }
    ]
  });
  chart.resize({ height });
  return chart;
}

function portfolioMonthRowsFromBreakdown(breakdown) {
  const m = new Map();
  (breakdown || []).forEach((row) => {
    const key = String(row.updated_at || "").slice(0, 7);
    if (!key || key.length < 7) {
      return;
    }
    m.set(key, (m.get(key) || 0) + Number(row.tco2e || 0));
  });
  return Array.from(m.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([dateLabel, emissions]) => ({
      company: "Portfolio",
      dateLabel,
      sortKey: dateLabel,
      emissions
    }));
}

function initHomeEnterprise() {
  const payload = readJsonScript("home-dashboard-payload");
  if (!payload) {
    return;
  }

  const kpiRoot = document.getElementById("homeAnalyticsKpis");
  if (kpiRoot) {
    applyKpiValues(kpiRoot, computeHomeKpis(payload));
  }

  const bind = (id, fn) => {
    const el = document.getElementById(id);
    if (!el) {
      return;
    }
    whenVisible(el, () => fn(el));
  };

  const { isAdmin, totals, companyRows = [], breakdown = [], reportingRows = [] } = payload;

  bind("homeChartScopeStackedBar", (el) => {
    if (!isAdmin || !companyRows.length) {
      showEmptyState(el, "Company-level scope stacks appear when aggregated company data is available.");
      return;
    }
    const top = companyRows.slice(0, 12);
    mountStackedBarChart(el, {
      labels: top.map((r) => r.company),
      seriesColorKind: "scope",
      series: [
        { name: "Scope 1 Direct Emissions", data: top.map((r) => r.scope1) },
        { name: "Scope 2 Indirect Emissions", data: top.map((r) => r.scope2) },
        { name: "Scope 3 Value Chain Emissions", data: top.map((r) => r.scope3) }
      ],
      height: 300
    });
  });

  bind("homeChartMonthlyTrend", (el) => {
    let rows = [];
    if (Array.isArray(reportingRows) && reportingRows.length) {
      rows = reportingRows.map((r) => ({
        company: r.company || "Portfolio",
        dateLabel: r.dateLabel || r.sortKey,
        sortKey: r.sortKey,
        emissions: Number(r.emissions || 0)
      }));
    } else {
      rows = portfolioMonthRowsFromBreakdown(breakdown);
    }
    if (!rows.length) {
      showEmptyState(el, "Monthly emission trend needs time-indexed reporting rows in mapped workbooks.");
      return;
    }
    const mt = monthlyTotals(rows);
    mountMonthlyTrendChart(el, {
      labels: mt.map((x) => x.dateLabel),
      values: mt.map((x) => x.value),
      height: 280,
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("homeChartCompanyRank", (el) => {
    if (!isAdmin || !companyRows.length) {
      showEmptyState(el, "Company ranking uses administrator company aggregates.");
      return;
    }
    const sorted = [...companyRows].sort((a, b) => Number(b.total) - Number(a.total)).slice(0, 12);
    mountCompanyRankingChart(el, {
      labels: sorted.map((r) => r.company),
      values: sorted.map((r) => r.total),
      height: 300,
      categoryColorKind: "company",
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("homeChartCategoryStack", (el) => {
    if (!breakdown.length) {
      showEmptyState(el, "Category contribution needs mapped category rows.");
      return;
    }
    const labels = breakdown.map((r) => r.sheet || "Category");
    mountStackedBarChart(el, {
      labels,
      categoryColorKind: "category",
      series: [{ name: "Category Contribution", data: breakdown.map((r) => Number(r.tco2e || 0)) }],
      height: 280,
      showLegend: false
    });
  });

  bind("homeChartScopeDonut", (el) => {
    mountDonutChart(el, {
      labels: ["Scope 1 Direct Emissions", "Scope 2 Indirect Emissions", "Scope 3 Value Chain Emissions"],
      values: [totals.scope1, totals.scope2, totals.scope3],
      height: 280,
      totalLabel: "Total emissions",
      pieColorKind: "scope",
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("homeChartTopCategoriesH", (el) => {
    if (!breakdown.length) {
      showEmptyState(el, "Top categories appear with category breakdown data.");
      return;
    }
    const sorted = [...breakdown].sort((a, b) => Number(b.tco2e || 0) - Number(a.tco2e || 0)).slice(0, 10);
    mountHorizontalBarChart(el, {
      labels: sorted.map((r) => r.sheet),
      values: sorted.map((r) => r.tco2e),
      height: 320,
      categoryColorKind: "category",
      seriesName: "Category Contribution",
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("homeChartMonthHeat", (el) => {
    let rows = [];
    if (Array.isArray(reportingRows) && reportingRows.length) {
      rows = reportingRows.map((r) => ({
        company: r.company || "Portfolio",
        dateLabel: r.dateLabel || r.sortKey,
        sortKey: r.sortKey,
        emissions: Number(r.emissions || 0)
      }));
    } else {
      rows = portfolioMonthRowsFromBreakdown(breakdown);
    }
    if (!rows.length) {
      showEmptyState(el, "Monthly pattern heatmap needs dated rows in mapped workbooks.");
      return;
    }
    mountHeatmapChart({ container: el, rows, height: 260 });
  });

  bind("homeChartCompanyTreemap", (el) => {
    if (!isAdmin || !companyRows.length) {
      showEmptyState(el, "Company treemap uses administrator company totals.");
      return;
    }
    mountFlatTreemap(
      el,
      companyRows.map((r) => ({ name: r.company, value: Number(r.total || 0), colorKind: "company" })),
      300
    );
  });

  bind("homeChartScopeCatHeat", (el) => {
    const rows = [];
    breakdown.forEach((r) => {
      const scopeLabel =
        Number(r.scope) === 1
          ? "Scope 1"
          : Number(r.scope) === 2
            ? "Scope 2"
            : Number(r.scope) === 3
              ? "Scope 3"
              : "Other";
      rows.push({
        company: "—",
        template: r.sheet,
        scope: scopeLabel,
        category: r.sheet,
        emissions: Number(r.tco2e || 0),
        dateLabel: "static",
        sortKey: "0",
        year: 0,
        monthIndex: 0
      });
    });
    if (!rows.length) {
      showEmptyState(el, "Scope vs category matrix needs category breakdown.");
      return;
    }
    mountScopeCategoryMatrix(el, rows, 300);
  });
}

function initDashboardEnterprise() {
  if (!document.getElementById("dashEntMonthlyScopeStack")) {
    return;
  }
  const extra = readJsonScript("dashboard-admin-enterprise-payload") || {};
  const grand = extra.grand || {};
  const companyRows = Array.isArray(extra?.companyRows) ? extra.companyRows : [];
  const monthRows = Array.isArray(extra?.monthRows) ? extra.monthRows : [];
  const categoryRows = Array.isArray(extra?.categoryRows) ? extra.categoryRows : [];

  const bind = (id, fn) => {
    const el = document.getElementById(id);
    if (!el) {
      return;
    }
    whenVisible(el, () => fn(el));
  };

  bind("dashEntMonthlyScopeStack", (el) => {
    const labels = monthRows.map((r) => r.month);
    if (!labels.length) {
      showEmptyState(el, "No monthly series in the current filter.");
      return;
    }
    const approx = grand.total > 0;
    if (!approx) {
      showEmptyState(el, "Scope split per month requires detailed time series.");
      return;
    }
    const ratio1 = grand.scope1 / grand.total;
    const ratio2 = grand.scope2 / grand.total;
    const ratio3 = grand.scope3 / grand.total;
    mountStackedBarChart(el, {
      labels,
      seriesColorKind: "scope",
      series: [
        { name: "Scope 1 Direct Emissions", data: monthRows.map((r) => r.total * ratio1) },
        { name: "Scope 2 Indirect Emissions", data: monthRows.map((r) => r.total * ratio2) },
        { name: "Scope 3 Value Chain Emissions", data: monthRows.map((r) => r.total * ratio3) }
      ],
      height: 320
    });
  });

  bind("dashEntCategoryTreemap", (el) => {
    if (!categoryRows.length) {
      showEmptyState(el, "Enable category breakdown in filters to load this view.");
      return;
    }
    mountFlatTreemap(
      el,
      categoryRows.map((r) => ({ name: r.category, value: Number(r.total || 0), colorKind: "category" })),
      320
    );
  });

  bind("dashEntMonthIntensity", (el) => {
    if (!monthRows.length) {
      showEmptyState(el, "No months in filtered data.");
      return;
    }
    const rows = monthRows.map((r) => ({
      company: "All companies",
      dateLabel: r.month,
      emissions: Number(r.total || 0)
    }));
    mountHeatmapChart({ container: el, rows, height: 220 });
  });

  bind("dashEntCategoryMonthStack", (el) => {
    if (!categoryRows.length || !monthRows.length) {
      showEmptyState(el, "Requires categories and monthly totals.");
      return;
    }
    const weights = categoryRows.map((c) => Number(c.total || 0));
    const sumW = weights.reduce((a, b) => a + b, 0) || 1;
    const top = categoryRows.slice(0, 6);
    const series = top.map((c) => ({
      name: c.category,
      type: "bar",
      stack: "contrib",
      emphasis: { focus: "series" },
      data: monthRows.map((m) => (Number(m.total || 0) * Number(c.total || 0)) / sumW)
    }));
    mountCategoryContributionChart(el, {
      labels: monthRows.map((m) => m.month),
      series,
      height: 320
    });
  });

  bind("dashEntScopeLines", (el) => {
    if (!monthRows.length || !grand.total) {
      showEmptyState(el, "No data for scope evolution.");
      return;
    }
    const r1 = grand.scope1 / grand.total;
    const r2 = grand.scope2 / grand.total;
    const r3 = grand.scope3 / grand.total;
    mountMultiLineChart(el, {
      labels: monthRows.map((m) => m.month),
      seriesColorKind: "scope",
      series: [
        { name: "Scope 1 Direct Emissions", data: monthRows.map((m) => m.total * r1) },
        { name: "Scope 2 Indirect Emissions", data: monthRows.map((m) => m.total * r2) },
        { name: "Scope 3 Value Chain Emissions", data: monthRows.map((m) => m.total * r3) }
      ],
      height: 320,
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("dashEntCompanyShare", (el) => {
    if (!companyRows.length) {
      showEmptyState(el, "No companies in filtered data.");
      return;
    }
    const top = companyRows.slice(0, 12);
    const sum = top.reduce((a, r) => a + Number(r.total || 0), 0) || 1;
    mountHorizontalBarChart(el, {
      labels: top.map((r) => r.company),
      values: top.map((r) => (Number(r.total || 0) / sum) * 100),
      height: 320,
      categoryColorKind: "company",
      seriesName: "Company Share",
      tooltipSuffix: " %",
      axisValueFormatter: (v) => `${formatCompact(v)}%`
    });
  });
}

function initAdminReportEnterprise() {
  const payloadId = pickAvailablePayloadId("admin-report-chart-data", "report-chart-data");
  if (!payloadId || !document.getElementById("admEntTrendLine")) {
    return;
  }
  const rows = buildAnalyticsRows(payloadId, ".report-analytics-source");
  if (!rows.length) {
    return;
  }
  const bind = (id, fn) => {
    const el = document.getElementById(id);
    if (!el) {
      return;
    }
    whenVisible(el, () => fn(el));
  };
  initAdminReportEnterpriseBindings(rows, bind);
}

function initAdminReportEnterpriseBindings(rows, bind) {
  bind("admEntTrendLine", (el) => {
    const mt = monthlyTotals(rows);
    mountMonthlyTrendChart(el, {
      labels: mt.map((x) => x.dateLabel),
      values: mt.map((x) => x.value),
      height: 300,
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("admEntMonthlyScopeStack", (el) => {
    const { labels, series } = monthlyScopeStackSeries(rows);
    if (!labels.length) {
      showEmptyState(el, "No monthly scope stacks for this filter.");
      return;
    }
    mountStackedBarChart(el, { labels, series, seriesColorKind: "scope", height: 320 });
  });

  bind("admEntScopeDonut", (el) => {
    const st = scopeTotals(rows);
    mountDonutChart(el, {
      labels: st.map((s) => s.name),
      values: st.map((s) => s.value),
      height: 280,
      totalLabel: "Emission Distribution",
      pieColorKind: "scope",
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("admEntCompanyCompare", (el) => {
    const ct = companyTotals(rows).slice(0, 12);
    mountCompanyRankingChart(el, {
      labels: ct.map(([name]) => name),
      values: ct.map(([, v]) => v),
      height: 320,
      categoryColorKind: "company",
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("admEntTopCatH", (el) => {
    const cat = categoryTotals(rows).slice(0, 12);
    mountHorizontalBarChart(el, {
      labels: cat.map(([name]) => name),
      values: cat.map(([, v]) => v),
      height: 320,
      categoryColorKind: "category",
      seriesName: "Category Contribution",
      tooltipSuffix: " tCO₂e"
    });
  });

  bind("admEntCatStackMonth", (el) => {
    const { labels, series } = monthlyTopCategoryStack(rows, 5);
    if (!labels.length) {
      showEmptyState(el, "Not enough monthly category points.");
      return;
    }
    mountCategoryContributionChart(el, { labels, series, height: 340 });
  });

  bind("admEntMonthHeat2", (el) => {
    mountHeatmapChart({ container: el, rows, height: 360 });
  });

  bind("admEntCompanyTreemap2", (el) => {
    const agg = companyTotals(rows).map(([name, value]) => ({ name, value, colorKind: "company" }));
    mountFlatTreemap(el, agg, 360);
  });

  bind("admEntScopeCatMatrix", (el) => {
    mountScopeCategoryMatrix(el, rows, 380);
  });

  bind("admEntSunburst2", (el) => {
    mountSunburstChart({ container: el, rows, height: 380 });
  });

  bind("admEntSankey2", (el) => {
    import("./SankeyChart.js").then(({ mountSankeyChart }) => {
      mountSankeyChart({ container: el, rows, height: 420 });
    });
  });
}

export function initEnterpriseDashboards() {
  initHomeEnterprise();
  initDashboardEnterprise();
  initAdminReportEnterprise();
}
