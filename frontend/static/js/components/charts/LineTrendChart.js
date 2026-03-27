import { renderTrendChart } from "../../charts/trend_chart.js";

export function mountLineTrendChart(container, options) {
  return renderTrendChart({ ...options, container });
}
