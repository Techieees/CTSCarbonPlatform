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

let themeChangeBindingReady = false;

function readJsonScript(id) {
  const element = document.getElementById(id);
  if (!element?.textContent) {
    return null;
  }

  try {
    return JSON.parse(element.textContent);
  } catch {
    return null;
  }
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const MONTH_LOOKUP = MONTHS.reduce((acc, month, index) => {
  acc[month.toLowerCase()] = index;
  return acc;
}, {});

function parseTemplateMeta(templateName) {
  const raw = String(templateName || "").trim() || "Uncategorized";
  const scopeMatch = raw.match(/scope\s*([123])/i);
  const scope = scopeMatch ? `Scope ${scopeMatch[1]}` : "Other";
  const category = raw.replace(/^\s*scope\s*[123]\s*/i, "").trim() || raw;
  return { scope, category };
}

function normalizeMonthLabel(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) {
    return null;
  }

  let match = value.match(/^([A-Za-z]{3})\s+(\d{4})$/);
  if (match) {
    const monthIndex = MONTH_LOOKUP[match[1].slice(0, 3).toLowerCase()];
    const year = Number(match[2]);
    if (monthIndex >= 0) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  match = value.match(/^(\d{4})-(\d{2})$/);
  if (match) {
    const year = Number(match[1]);
    const monthIndex = Number(match[2]) - 1;
    if (monthIndex >= 0 && monthIndex < 12) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) {
    const year = Number(match[1]);
    const monthIndex = Number(match[2]) - 1;
    if (monthIndex >= 0 && monthIndex < 12) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  match = value.match(/^(\d{4})-([A-Za-z]{3})$/);
  if (match) {
    const year = Number(match[1]);
    const monthIndex = MONTH_LOOKUP[match[2].slice(0, 3).toLowerCase()];
    if (monthIndex >= 0) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  return null;
}

function readAnalyticsMeta(selector) {
  const elements = Array.from(document.querySelectorAll(selector));
  const deduped = new Map();

  elements.forEach((element) => {
    const index = Number(element.dataset.index || 0);
    if (!deduped.has(index)) {
      deduped.set(index, {
        index,
        company: element.dataset.company || "Company",
        template: element.dataset.template || "Category",
        period: element.dataset.period || "",
        total: Number(element.dataset.total || 0)
      });
    }
  });

  return Array.from(deduped.values()).sort((a, b) => a.index - b.index);
}

function pickAvailablePayloadId(...ids) {
  return ids.find((id) => document.getElementById(id)) || null;
}

function buildAnalyticsRows(payloadId, metaSelector) {
  const chartData = readJsonScript(payloadId);
  const meta = readAnalyticsMeta(metaSelector);
  if (!Array.isArray(chartData) || !meta.length) {
    return [];
  }

  return meta.flatMap((item, index) => {
    const chartItem = chartData[index] || {};
    const labels = Array.isArray(chartItem.labels) ? chartItem.labels : [];
    const values = Array.isArray(chartItem.values) ? chartItem.values : [];
    const { scope, category } = parseTemplateMeta(item.template);

    if (!labels.length || !values.length) {
      const normalized = normalizeMonthLabel(item.period);
      if (!normalized) {
        return [];
      }
      return [{
        company: item.company,
        template: item.template,
        scope,
        category,
        emissions: Number(item.total || 0),
        ...normalized
      }];
    }

    return labels.map((label, pointIndex) => {
      const normalized = normalizeMonthLabel(label);
      if (!normalized) {
        return null;
      }
      return {
        company: item.company,
        template: item.template,
        scope,
        category,
        emissions: Number(values[pointIndex] || 0),
        ...normalized
      };
    }).filter(Boolean);
  }).sort((a, b) => String(a.sortKey).localeCompare(String(b.sortKey)) || String(a.company).localeCompare(String(b.company)) || String(a.category).localeCompare(String(b.category)));
}

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

  renderCompanyChart({
    container: "#companyChart",
    labels: payload.companies?.labels || [],
    values: payload.companies?.values || [],
    seriesName: "tCO₂e",
    height: 320,
    tooltipSuffix: " tCO₂e"
  });

  renderTrendChart({
    container: "#trendChart",
    labels: payload.months?.labels || [],
    values: payload.months?.values || [],
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
