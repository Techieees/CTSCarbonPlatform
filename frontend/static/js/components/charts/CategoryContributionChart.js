import { initChart, formatFull, getAxisLabel, getGrid, getLegend, getTooltipBase } from "../../charts/echarts_theme.js";
import { getColorByKey } from "../../charts/chart_colors.js";

/** Stacked bars along category axis — pass pre-built `series` (stacked bar). */
export function mountCategoryContributionChart(container, { labels = [], series = [], height = 320, tooltipSuffix = " tCO₂e" } = {}) {
  const chart = initChart(container);
  if (!chart) {
    return null;
  }
  const coloredSeries = series.map((s) => {
    const c = getColorByKey(s.name, "category");
    return {
      ...s,
      itemStyle: {
        ...s.itemStyle,
        color: c
      }
    };
  });
  chart.setOption({
    animation: true,
    animationDuration: 900,
    animationEasing: "cubicOut",
    grid: getGrid(false, coloredSeries.length > 1),
    legend: getLegend(coloredSeries.length > 1, coloredSeries.length),
    tooltip: {
      ...getTooltipBase((params) => {
        const entries = Array.isArray(params) ? params : [params];
        const title = entries[0]?.axisValueLabel || "";
        const rows = entries
          .map(
            (e) =>
              `<div style="display:flex;justify-content:space-between;gap:12px;margin-top:4px;"><span>${e.seriesName}</span><strong>${formatFull(e.value)}${tooltipSuffix}</strong></div>`
          )
          .join("");
        return `<div><div style="font-weight:600;margin-bottom:4px;">${title}</div>${rows}</div>`;
      }),
      trigger: "axis"
    },
    xAxis: {
      type: "category",
      data: labels,
      axisTick: { show: false },
      axisLine: { lineStyle: { color: "rgba(148,163,184,0.18)" } },
      axisLabel: getAxisLabel((v) => v)
    },
    yAxis: {
      type: "value",
      splitLine: { lineStyle: { color: "rgba(148,163,184,0.1)" } },
      axisLabel: getAxisLabel((v) => v)
    },
    series: coloredSeries
  });
  chart.resize({ height });
  return chart;
}
