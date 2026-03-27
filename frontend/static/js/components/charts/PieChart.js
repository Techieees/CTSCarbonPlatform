import { formatFull, getLegend, getTooltipBase, initChart, getPalette, makeGradient, withOpacity } from "../../charts/echarts_theme.js";

export function mountPieChart(container, { labels = [], values = [], height = 300, tooltipSuffix = " tCO₂e", seriesName = "Emission Distribution" } = {}) {
  const chart = initChart(container);
  if (!chart) {
    return null;
  }
  const seriesData = labels.map((label, index) => {
    const palette = getPalette(index);
    return {
      name: label,
      value: values[index] || 0,
      itemStyle: {
        color: makeGradient(index, false, 0.98, 0.76),
        shadowBlur: 12,
        shadowColor: withOpacity(palette.from, 0.12)
      }
    };
  });
  chart.setOption({
    animation: true,
    animationDuration: 800,
    animationEasing: "cubicOut",
    legend: getLegend(true),
    tooltip: {
      ...getTooltipBase((params) => {
        const pct = Number(params.percent || 0).toFixed(1);
        return `<div><div style="font-weight:600;margin-bottom:4px;">${params.name}</div><strong>${formatFull(params.value)}${tooltipSuffix}</strong><div style="margin-top:4px;opacity:.85;">Share: ${pct}%</div></div>`;
      }),
      trigger: "item"
    },
    series: [
      {
        name: seriesName,
        type: "pie",
        radius: "68%",
        center: ["50%", "48%"],
        data: seriesData,
        minAngle: 2,
        label: { color: "inherit", fontWeight: 600, fontSize: 11 }
      }
    ]
  });
  chart.resize({ height });
  return chart;
}
