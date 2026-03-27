import { renderTrendChart } from "../../charts/trend_chart.js";

/** Area fill is already enabled in shared trend renderer. */
export function mountAreaTrendChart(container, options) {
  return renderTrendChart({ ...options, container });
}
