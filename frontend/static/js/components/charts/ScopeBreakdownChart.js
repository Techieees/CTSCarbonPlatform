import { mountDonutChart } from "./DonutChart.js";

export function mountScopeBreakdownChart(container, { labels, values, height } = {}) {
  return mountDonutChart(container, {
    labels,
    values,
    height,
    seriesName: "Emission Distribution",
    totalLabel: "Total emissions",
    tooltipSuffix: " tCO₂e"
  });
}
