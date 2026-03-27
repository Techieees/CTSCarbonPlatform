import { renderTrendChart } from "../../charts/trend_chart.js";

export function mountMultiLineChart(container, options) {
  return renderTrendChart({ showLegend: true, ...options, container });
}
