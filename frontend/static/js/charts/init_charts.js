import { renderCategoryChart } from "./category_chart.js";
import { renderCategoryTreemap } from "./category_treemap.js";
import { renderCompanyChart } from "./company_chart.js";
import { renderEmissionsHeatmap } from "./emissions_heatmap.js";
import { renderEmissionsRaceChart } from "./emissions_race_chart.js";
import { renderEmissionsSankey } from "./emissions_sankey.js";
import { formatFull } from "./echarts_theme.js";
import { renderScopeSunburst } from "./scope_sunburst.js";
import { renderScopeChart } from "./scope_chart.js";
import { renderTrendChart } from "./trend_chart.js";
import { readJsonScript, buildAnalyticsRows, pickAvailablePayloadId } from "./analytics_rows.js";
import { initEnterpriseDashboards } from "../components/charts/enterprise_pages.js";
import { initScopeDetailCharts } from "./scope_detail_charts.js";
import { initScope3CategoryCharts } from "./scope3_category_charts.js";

let themeChangeBindingReady = false;

function updateReportMetrics(rows) {
  if (!rows.length) {
    return;
  }

  const total = rows.reduce((sum, row) => sum + Number(row.emissions || 0), 0);
  const months = new Set(rows.map((row) => row.dateLabel));
  const categories = new Set(rows.map((row) => row.template || row.category));
  const peakRow = rows.reduce((peak, row) => (Number(row.emissions || 0) > Number(peak?.emissions || 0) ? row : peak), null);

  const elTotal = document.getElementById("reportMetricTotal");
  const elMonths = document.getElementById("reportMetricMonths");
  const elCategories = document.getElementById("reportMetricCategories");
  const elPeak = document.getElementById("reportMetricPeak");

  if (elTotal) {
    elTotal.textContent = `${formatFull(total)} tCO₂e`;
  }
  if (elMonths) {
    elMonths.textContent = String(months.size);
  }
  if (elCategories) {
    elCategories.textContent = String(categories.size);
  }
  if (elPeak) {
    elPeak.textContent = peakRow ? `${peakRow.dateLabel} · ${formatFull(peakRow.emissions)} tCO₂e` : "—";
  }
}

function initDashboardAdminCharts() {
  const payload = readJsonScript("dashboard-admin-chart-payload");
  if (!payload) {
    return;
  }

  const ent = readJsonScript("dashboard-admin-enterprise-payload");
  let trendLabels = payload.months?.labels || [];
  let trendValues = payload.months?.values || [];
  if (ent?.monthRows?.length) {
    const mr = [...ent.monthRows].sort((a, b) => String(a.month).localeCompare(String(b.month)));
    trendLabels = mr.map((r) => r.month);
    trendValues = mr.map((r) => Number(r.total || 0));
  }

  renderCompanyChart({
    container: "#companyChart",
    labels: payload.companies?.labels || [],
    values: payload.companies?.values || [],
    categoryColorKind: "company",
    seriesName: "tCO₂e",
    height: 320,
    tooltipSuffix: " tCO₂e"
  });

  renderTrendChart({
    container: "#trendChart",
    labels: trendLabels,
    values: trendValues,
    seriesName: "tCO₂e",
    height: 320,
    tooltipSuffix: " tCO₂e"
  });

  const scopeData = readJsonScript("dashboard-admin-scope-chart-data");
  if (scopeData) {
    renderScopeChart({
      container: "#scopeChart",
      labels: scopeData.labels || [],
      values: scopeData.values || [],
      seriesName: scopeData.seriesName || "tCO₂e",
      height: scopeData.height || 320,
      tooltipSuffix: scopeData.tooltipSuffix || ""
    });
  }

  if (document.getElementById("categoryChart")) {
    renderCategoryChart({
      container: "#categoryChart",
      labels: payload.categories?.labels || [],
      values: payload.categories?.values || [],
      seriesName: "Category Total",
      height: 320,
      tooltipSuffix: " tCO₂e"
    });
  }
}

function initScopeSummaryChart(payloadId) {
  const payload = readJsonScript(payloadId);
  if (!payload) {
    return;
  }

  renderScopeChart({
    container: "#scopeChart",
    labels: payload.labels || [],
    values: payload.values || [],
    seriesName: payload.seriesName || "tCO₂e",
    height: payload.height || 280,
    tooltipSuffix: payload.tooltipSuffix || ""
  });
}

function initAdminCoverageChart() {
  const chartData = readJsonScript("admin-chart-data");
  if (!Array.isArray(chartData) || !chartData.length) {
    return;
  }

  const labels = chartData.map((row) => row.company);
  const submitted = chartData.map((row) => Number(row.submitted || 0));
  const expected = chartData.map((row) => Number(row.expected || 0));
  const rates = chartData.map((row) => Number(row.rate || 0));

  renderCompanyChart({
    container: "#companyChart",
    labels,
    series: [
      { name: "Mapped Categories", data: submitted },
      { name: "Expected", data: expected }
    ],
    height: 320,
    showLegend: true,
    axisValueFormatter: (value) => formatFull(value),
    tooltipFormatter: (params) => {
      const entries = Array.isArray(params) ? params : [params];
      const index = entries[0]?.dataIndex ?? 0;
      const title = entries[0]?.axisValueLabel || labels[index] || "";
      const rows = entries
        .map((entry) => `<div style="display:flex;justify-content:space-between;gap:16px;margin-top:6px;"><span>${entry.seriesName}</span><strong>${formatFull(entry.value)}</strong></div>`)
        .join("");

      return `
        <div>
          <div style="font-size:12px;font-weight:600;color:rgba(226,232,240,.82);margin-bottom:2px;">${title}</div>
          ${rows}
          <div style="font-size:12px;color:rgba(226,232,240,.82);margin-top:8px;">Coverage: ${formatFull(rates[index])}%</div>
        </div>
      `;
    }
  });
}

function initReportCards(payloadId) {
  const chartData = readJsonScript(payloadId);
  if (!Array.isArray(chartData) || !chartData.length) {
    return;
  }

  chartData.forEach((item) => {
    if (item.bar_id && document.getElementById(item.bar_id)) {
      renderCompanyChart({
        container: `#${item.bar_id}`,
        labels: item.labels || [],
        values: item.values || [],
        seriesName: "Emission (tCO₂e)",
        height: 260,
        tooltipSuffix: " tCO₂e"
      });
    }

    if (item.line_id && document.getElementById(item.line_id)) {
      renderTrendChart({
        container: `#${item.line_id}`,
        labels: item.labels || [],
        values: item.values || [],
        seriesName: "Emission (tCO₂e)",
        height: 260,
        tooltipSuffix: " tCO₂e"
      });
    }

    if (item.pie_id && document.getElementById(item.pie_id)) {
      renderCategoryChart({
        container: `#${item.pie_id}`,
        variant: "donut",
        labels: item.labels || [],
        values: item.values || [],
        height: 280,
        tooltipSuffix: " tCO₂e",
        totalLabel: "Total"
      });
    }
  });
}

function initAdvancedReportAnalytics() {
  if (!document.getElementById("emissionsRaceChart")) {
    return;
  }

  const payloadId = pickAvailablePayloadId("report-chart-data", "admin-report-chart-data");
  if (!payloadId) {
    return;
  }

  const rows = buildAnalyticsRows(payloadId, ".report-analytics-source");
  if (!rows.length) {
    return;
  }

  updateReportMetrics(rows);

  renderEmissionsRaceChart({
    container: "#emissionsRaceChart",
    rows,
    height: 440
  });

  renderEmissionsHeatmap({
    container: "#emissionsHeatmap",
    rows,
    height: 380
  });

  renderScopeSunburst({
    container: "#scopeSunburst",
    rows,
    height: 380
  });

  renderCategoryTreemap({
    container: "#categoryTreemap",
    rows,
    height: 380
  });

  renderEmissionsSankey({
    container: "#emissionsSankey",
    rows,
    height: 440
  });
}

function initAllCharts() {
  initDashboardAdminCharts();
  initAdminCoverageChart();
  initScopeSummaryChart("home-scope-chart-data");
  initScopeSummaryChart("carbon-accounting-scope-chart-data");
  initAdvancedReportAnalytics();
  initReportCards("report-chart-data");
  initReportCards("admin-report-chart-data");
  initEnterpriseDashboards();
  initScopeDetailCharts();
  initScope3CategoryCharts();
}

function ensureThemeChangeBinding() {
  if (themeChangeBindingReady) {
    return;
  }

  themeChangeBindingReady = true;
  window.addEventListener("themechange", () => {
    window.requestAnimationFrame(() => bootCharts());
  });
}

function bootCharts(attempt = 0) {
  if (typeof window.echarts === "undefined") {
    if (attempt < 60) {
      window.requestAnimationFrame(() => bootCharts(attempt + 1));
    }
    return;
  }

  try {
    ensureThemeChangeBinding();
    initAllCharts();
  } catch (error) {
    console.error("Chart initialization failed", error);
  }
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => bootCharts(), { once: true });
} else {
  bootCharts();
}
