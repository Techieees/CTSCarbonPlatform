/**
 * Per-scope analytics pages — reuses home JSON payload + enterprise chart mounts.
 */
import { readJsonScript } from "./analytics_rows.js";
import { monthlyTotals, monthlyTopCategoryStack } from "./row_aggregates.js";
import { whenVisible, showEmptyState } from "../components/charts/BaseChart.js";
import { mountStackedBarChart } from "../components/charts/StackedBarChart.js";
import { mountDonutChart } from "../components/charts/DonutChart.js";
import { mountMonthlyTrendChart } from "../components/charts/MonthlyTrendChart.js";
import { mountHorizontalBarChart } from "../components/charts/HorizontalBarChart.js";
import { mountCategoryContributionChart } from "../components/charts/CategoryContributionChart.js";
import { initChart, formatFull, getAxisLabel, getTooltipBase } from "./echarts_theme.js";
import { getColorByKey } from "./chart_colors.js";

function inferScopeFromSheetName(sheet) {
  const s = String(sheet || "").toLowerCase();
  if (/\bscope\s*3\b/.test(s)) {
    return 3;
  }
  if (/\bscope\s*2\b/.test(s)) {
    return 2;
  }
  if (/\bscope\s*1\b/.test(s)) {
    return 1;
  }
  return 0;
}

function filterBreakdown(breakdown, scopeNum) {
  return (breakdown || []).filter((r) => {
    const n = Number(r.scope);
    if (n === scopeNum) {
      return true;
    }
    if (n === 0 || Number.isNaN(n)) {
      return inferScopeFromSheetName(r.sheet) === scopeNum;
    }
    return false;
  });
}

function rowMatchesScope(r, scopeNum) {
  const n = Number(r.scope);
  if (n === scopeNum) {
    return true;
  }
  if (n === 0 || Number.isNaN(n)) {
    return inferScopeFromSheetName(r.sheet || r.category) === scopeNum;
  }
  return false;
}

function portfolioMonthRowsFromScopeBreakdown(breakdown, scopeNum) {
  const fb = filterBreakdown(breakdown, scopeNum);
  const m = new Map();
  fb.forEach((row) => {
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

function breakdownToRows(breakdown, scopeNum) {
  return filterBreakdown(breakdown, scopeNum).map((r) => {
    const ym = String(r.updated_at || "").slice(0, 7) || "2000-01";
    return {
      company: "—",
      template: r.sheet,
      scope: `Scope ${scopeNum}`,
      category: r.sheet,
      emissions: Number(r.tco2e || 0),
      dateLabel: ym,
      sortKey: ym,
      year: 0,
      monthIndex: 0
    };
  });
}

/** Row-level rows from server: reporting period from workbook + per-row tCO₂e */
function reportingRowsToChartRows(reportingRows, scopeNum) {
  return (reportingRows || [])
    .filter((r) => rowMatchesScope(r, scopeNum))
    .map((r) => ({
      company: "—",
      template: r.sheet || r.category,
      scope: `Scope ${scopeNum}`,
      category: r.category || r.sheet,
      emissions: Number(r.emissions || 0),
      dateLabel: r.dateLabel,
      sortKey: r.sortKey,
      year: 0,
      monthIndex: 0
    }));
}

function mountEmissionsShareCombo(container, labels, values, height, scopeNum) {
  const emissions = values.map((v) => Number(v || 0));
  const total = emissions.reduce((a, b) => a + b, 0) || 1;
  const shares = emissions.map((v) => (100 * v) / total);
  const chart = initChart(container);
  if (!chart) {
    return null;
  }
  const barKey = scopeNum === 2 ? "Scope 2" : "Scope 1";
  const lineKey = scopeNum === 2 ? "Scope 3" : "Scope 2";
  chart.setOption({
    animation: true,
    tooltip: {
      trigger: "axis",
      ...getTooltipBase((params) => {
        const list = Array.isArray(params) ? params : [params];
        return list
          .map((p) => `<div>${p.seriesName}: <strong>${p.seriesName === "Share %" ? formatFull(p.value) + "%" : formatFull(p.value) + " tCO₂e"}</strong></div>`)
          .join("");
      })
    },
    legend: { data: ["tCO₂e", "Share %"], top: 0 },
    grid: { top: 48, right: 56, bottom: 64, left: 56 },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: { ...getAxisLabel((v) => v), rotate: 28, interval: 0, width: 120, overflow: "truncate" }
    },
    yAxis: [
      { type: "value", name: "tCO₂e", splitLine: { show: true } },
      { type: "value", name: "% of scope", max: 100, splitLine: { show: false } }
    ],
    series: [
      {
        name: "tCO₂e",
        type: "bar",
        data: emissions,
        itemStyle: { color: getColorByKey(barKey, "scope") }
      },
      {
        name: "Share %",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        data: shares,
        itemStyle: { color: getColorByKey(lineKey, "scope") }
      }
    ]
  });
  chart.resize({ height });
  return chart;
}

function partitionScope3ByKeyword(breakdown) {
  const fb = filterBreakdown(breakdown, 3);
  const buckets = { travel: [], transport: [], waste: [], other: [] };
  fb.forEach((r) => {
    const s = String(r.sheet || "").toLowerCase();
    if (/travel|flight|hotel|mile/.test(s)) {
      buckets.travel.push(r);
    } else if (/transport|logistics|freight|vehicle|shipping/.test(s)) {
      buckets.transport.push(r);
    } else if (/waste|disposal|recycl/.test(s)) {
      buckets.waste.push(r);
    } else {
      buckets.other.push(r);
    }
  });
  return buckets;
}

function sumSheets(rows) {
  return rows.reduce((a, r) => a + Number(r.tco2e || 0), 0);
}

function mountMiniDonut(container, label, value, totalScope, height = 240) {
  const rest = Math.max(0, Number(totalScope || 0) - Number(value || 0));
  const chart = initChart(container);
  if (!chart) {
    return null;
  }
  chart.setOption({
    title: { text: label, left: "center", top: 8, textStyle: { fontSize: 12, fontWeight: 600 } },
    tooltip: { trigger: "item" },
    series: [
      {
        type: "pie",
        radius: ["42%", "68%"],
        label: { formatter: "{b}\n{d}%" },
        data: [
          { name: "Matched", value: Number(value || 0) },
          { name: "Other Scope 3", value: rest }
        ]
      }
    ]
  });
  chart.resize({ height });
  return chart;
}

function initScopeAdmin(scopeNum, payload) {
  const { totals, companyRows = [] } = payload;
  const key = scopeNum === 1 ? "scope1" : scopeNum === 2 ? "scope2" : "scope3";
  const total = Number(totals[key] || 0);

  const kpi = document.getElementById("scopeDetailKpiTotal");
  if (kpi) {
    kpi.textContent = `${formatFull(total)} tCO₂e`;
  }

  if (!companyRows.length) {
    whenVisible(document.getElementById("scopeDetailMonthly"), (el) => showEmptyState(el, "Company-level time series requires user portfolio mapping rows."));
    return;
  }

  const labels = companyRows.map((r) => r.company);
  const values = companyRows.map((r) => Number(r[key] || 0));

  whenVisible(document.getElementById("scopeDetailMonthly"), (el) => {
    mountHorizontalBarChart(el, {
      labels,
      values,
      height: 300,
      categoryColorKind: "company",
      seriesName: "tCO₂e",
      tooltipSuffix: " tCO₂e"
    });
  });

  whenVisible(document.getElementById("scopeDetailDonut"), (el) => {
    mountDonutChart(el, {
      labels,
      values,
      height: 280,
      totalLabel: `Scope ${scopeNum} by company`,
      pieColorKind: "company",
      tooltipSuffix: " tCO₂e"
    });
  });

  whenVisible(document.getElementById("scopeDetailCategoryH"), (el) => {
    if (!el) {
      return;
    }
    mountHorizontalBarChart(el, {
      labels,
      values,
      height: 300,
      categoryColorKind: "company",
      seriesName: "tCO₂e",
      tooltipSuffix: " tCO₂e"
    });
  });

  const combo = document.getElementById("scopeDetailCombo");
  if (combo) {
    whenVisible(combo, (el) => mountEmissionsShareCombo(el, labels, values, 320, scopeNum));
  }

  const stackEl = document.getElementById("scopeDetailStackMonth");
  if (stackEl) {
    whenVisible(stackEl, (el) => showEmptyState(el, "Stacked monthly categories need dated sheet rows (user view)."));
  }

  ["scopeDetailMini1", "scopeDetailMini2", "scopeDetailMini3"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) {
      whenVisible(el, (node) => showEmptyState(node, "Keyword split available when category sheets are mapped."));
    }
  });
}

function initScopeUser(scopeNum, payload) {
  const { totals, breakdown = [], reporting_rows: reportingRows = [] } = payload;
  const key = scopeNum === 1 ? "scope1" : scopeNum === 2 ? "scope2" : "scope3";
  const total = Number(totals[key] || 0);

  const kpi = document.getElementById("scopeDetailKpiTotal");
  if (kpi) {
    kpi.textContent = `${formatFull(total)} tCO₂e`;
  }

  const fb = filterBreakdown(breakdown, scopeNum);
  if (!fb.length) {
    ["scopeDetailMonthly", "scopeDetailDonut", "scopeDetailCategoryH", "scopeDetailCombo", "scopeDetailStackMonth", "scopeDetailMini1", "scopeDetailMini2", "scopeDetailMini3"].forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        whenVisible(el, (node) => showEmptyState(node, "No mapped categories for this scope yet."));
      }
    });
    return;
  }

  const monthRowsRaw = (reportingRows || [])
    .filter((r) => rowMatchesScope(r, scopeNum))
    .map((r) => ({
      company: "Portfolio",
      dateLabel: r.dateLabel || r.sortKey,
      sortKey: r.sortKey,
      emissions: Number(r.emissions || 0)
    }));
  const sortedSheets = [...fb].sort((a, b) => Number(b.tco2e || 0) - Number(a.tco2e || 0));
  const catLabels = sortedSheets.map((r) => r.sheet || "Category");
  const catVals = sortedSheets.map((r) => Number(r.tco2e || 0));

  whenVisible(document.getElementById("scopeDetailMonthly"), (el) => {
    if (!monthRowsRaw.length) {
      showEmptyState(
        el,
        "No time series from mapped files. Ensure “Reporting period (month, year)” or “Purchase Date” has values in the mapped workbook, or re-run mapping after saving data."
      );
      return;
    }
    const mt = monthlyTotals(monthRowsRaw);
    mountMonthlyTrendChart(el, {
      labels: mt.map((x) => x.dateLabel),
      values: mt.map((x) => x.value),
      height: 300,
      tooltipSuffix: " tCO₂e"
    });
  });

  whenVisible(document.getElementById("scopeDetailDonut"), (el) => {
    mountDonutChart(el, {
      labels: catLabels,
      values: catVals,
      height: 280,
      totalLabel: "Category mix",
      pieColorKind: "category",
      tooltipSuffix: " tCO₂e"
    });
  });

  whenVisible(document.getElementById("scopeDetailCategoryH"), (el) => {
    mountHorizontalBarChart(el, {
      labels: catLabels.slice(0, 14),
      values: catVals.slice(0, 14),
      height: 320,
      categoryColorKind: "category",
      seriesName: "tCO₂e",
      tooltipSuffix: " tCO₂e"
    });
  });

  whenVisible(document.getElementById("scopeDetailCombo"), (el) => {
    mountEmissionsShareCombo(el, catLabels.slice(0, 12), catVals.slice(0, 12), scopeNum === 2 ? 360 : 320, scopeNum);
  });

  const stackRows = reportingRowsToChartRows(reportingRows, scopeNum);
  whenVisible(document.getElementById("scopeDetailStackMonth"), (el) => {
    if (!stackRows.length) {
      showEmptyState(el, "Stacked monthly trend needs reporting-period rows per category in mapped files.");
      return;
    }
    const { labels, series } = monthlyTopCategoryStack(stackRows, 6);
    if (!labels.length) {
      showEmptyState(el, "Not enough monthly category points for a stacked trend.");
      return;
    }
    mountCategoryContributionChart(el, { labels, series, height: 360 });
  });

  if (scopeNum === 3) {
    const buckets = partitionScope3ByKeyword(breakdown);
    const t3 = sumSheets(fb);
    const pairs = [
      ["scopeDetailMini1", sumSheets(buckets.travel), "Travel & mobility"],
      ["scopeDetailMini2", sumSheets(buckets.transport), "Transport & logistics"],
      ["scopeDetailMini3", sumSheets(buckets.waste), "Waste & disposal"]
    ];
    pairs.forEach(([id, val, title]) => {
      const el = document.getElementById(id);
      if (!el) {
        return;
      }
      whenVisible(el, (node) => {
        if (val <= 0) {
          showEmptyState(node, "No sheets matched this keyword group.");
          return;
        }
        mountMiniDonut(node, title, val, t3, 260);
      });
    });
  }
}

export function initScopeDetailCharts() {
  const payload = readJsonScript("scope-dashboard-payload");
  if (!payload || typeof payload.scope !== "number") {
    return;
  }

  const scopeNum = payload.scope;
  const fb = filterBreakdown(payload.breakdown || [], scopeNum);

  // Prefer category / time-series charts whenever this scope has mapped breakdown rows.
  // (Previously admin + companyRows always used initScopeAdmin, which left Scope 3 mini/stack/month slots empty.)
  if (fb.length > 0) {
    initScopeUser(scopeNum, payload);
    return;
  }

  if (payload.isAdmin && (payload.companyRows || []).length > 0) {
    initScopeAdmin(scopeNum, payload);
  } else {
    initScopeUser(scopeNum, payload);
  }
}
