import { renderCategoryChart } from "../../charts/category_chart.js";

export function mountDonutChart(container, options) {
  return renderCategoryChart({ variant: "donut", container, ...options });
}
