import { renderCompanyChart } from "../../charts/company_chart.js";

export function mountHorizontalBarChart(container, options) {
  return renderCompanyChart({ horizontal: true, ...options, container });
}
