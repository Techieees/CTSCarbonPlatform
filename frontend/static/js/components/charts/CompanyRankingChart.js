import { mountHorizontalBarChart } from "./HorizontalBarChart.js";

export function mountCompanyRankingChart(container, options) {
  return mountHorizontalBarChart(container, {
    seriesName: "Company Share",
    ...options
  });
}
