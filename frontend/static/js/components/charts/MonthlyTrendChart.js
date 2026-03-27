import { mountLineTrendChart } from "./LineTrendChart.js";

export function mountMonthlyTrendChart(container, options) {
  return mountLineTrendChart(container, {
    seriesName: "Emission Trend",
    ...options
  });
}
