import { renderCompanyChart } from "../../charts/company_chart.js";

export function mountStackedBarChart(container, options) {
  const { tooltipSuffix, showLegend, ...rest } = options || {};
  const seriesCount = Array.isArray(rest.series) ? rest.series.length : 0;
  return renderCompanyChart({
    stacked: true,
    tooltipSuffix: tooltipSuffix || " tCO₂e",
    showLegend: showLegend !== undefined ? showLegend : seriesCount > 1,
    ...rest,
    container
  });
}
