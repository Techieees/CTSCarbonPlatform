/**
 * Scope 3 GHG category methodology pages — mapped workbook analytics.
 */
import { readJsonScript } from "./analytics_rows.js";
import { whenVisible, showEmptyState } from "../components/charts/BaseChart.js";
import { mountMonthlyTrendChart } from "../components/charts/MonthlyTrendChart.js";
import { mountDonutChart } from "../components/charts/DonutChart.js";
import { mountHorizontalBarChart } from "../components/charts/HorizontalBarChart.js";
import { formatFull } from "./echarts_theme.js";

export function initScope3CategoryCharts() {
  const wrap = document.getElementById("scope3CategoryDashboard");
  if (!wrap) {
    return;
  }

  const authenticated = wrap.getAttribute("data-authenticated") === "true";
  const payload = readJsonScript("scope3-category-chart-payload");

  if (!authenticated || !payload || !payload.has_data) {
    return;
  }

  const kpiTotal = document.getElementById("s3catKpiTotal");
  const kpiRecords = document.getElementById("s3catKpiRecords");
  if (kpiTotal) {
    kpiTotal.textContent = `${formatFull(payload.total_tco2e)} tCO₂e`;
  }
  if (kpiRecords) {
    kpiRecords.textContent = String(payload.record_count ?? 0);
  }

  whenVisible(document.getElementById("s3catMonthly"), (el) => {
    const labels = payload.monthly_labels || [];
    const values = payload.monthly_values || [];
    if (!labels.length || !values.length) {
      showEmptyState(el, "No data available yet");
      return;
    }
    mountMonthlyTrendChart(el, {
      labels,
      values,
      height: 300,
      tooltipSuffix: " tCO₂e"
    });
  });

  whenVisible(document.getElementById("s3catEfDonut"), (el) => {
    const labels = payload.ef_labels || [];
    const values = payload.ef_values || [];
    if (!labels.length) {
      showEmptyState(el, "No data available yet");
      return;
    }
    mountDonutChart(el, {
      labels,
      values,
      height: 280,
      totalLabel: "Emission factors",
      pieColorKind: "category",
      tooltipSuffix: " tCO₂e"
    });
  });

  whenVisible(document.getElementById("s3catSourceBar"), (el) => {
    const labels = payload.source_labels || [];
    const values = payload.source_values || [];
    if (!labels.length) {
      showEmptyState(el, "No data available yet");
      return;
    }
    mountHorizontalBarChart(el, {
      labels,
      values,
      height: 320,
      categoryColorKind: "category",
      seriesName: "tCO₂e",
      tooltipSuffix: " tCO₂e"
    });
  });
}
