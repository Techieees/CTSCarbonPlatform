import {
  formatCompact,
  formatFull,
  getAxisLabel,
  getGrid,
  getLegend,
  getPalette,
  getPerformanceOptions,
  getTooltipBase,
  initChart,
  makeGradient,
  withOpacity
} from "./echarts_theme.js";

export function renderCompanyChart(config) {
  const {
    container,
    labels = [],
    values = [],
    series,
    height = 320,
    horizontal = false,
    stacked = false,
    inverse = false,
    showLegend = false,
    tooltipSuffix = "",
    axisValueFormatter = formatCompact,
    tooltipFormatter
  } = config;

  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const normalizedSeries = Array.isArray(series) && series.length
    ? series
    : [{ name: config.seriesName || "Value", data: values }];

  const largestSeries = Math.max(labels.length, ...normalizedSeries.map((item) => item.data?.length || 0));
  const perf = getPerformanceOptions(largestSeries, "bar");

  const defaultTooltip = (params) => {
    const entries = Array.isArray(params) ? params : [params];
    const title = entries[0]?.axisValueLabel || entries[0]?.name || "";
    const rows = entries
      .map((entry) => {
        const marker = `<span style="display:inline-block;width:10px;height:10px;border-radius:999px;margin-right:8px;background:${entry.color};"></span>`;
        return `<div style="display:flex;align-items:center;justify-content:space-between;gap:16px;margin-top:6px;">${marker}<span style="flex:1;">${entry.seriesName}</span><strong>${formatFull(entry.value)}${tooltipSuffix}</strong></div>`;
      })
      .join("");

    return `<div><div style="font-size:12px;font-weight:600;color:rgba(226,232,240,.82);margin-bottom:2px;">${title}</div>${rows}</div>`;
  };

  const categoryAxis = {
    type: "category",
    data: labels,
    inverse,
    axisTick: { show: false },
    axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.18)" } },
    splitLine: { show: false },
    axisLabel: getAxisLabel((value) => value)
  };

  const valueAxis = {
    type: "value",
    splitNumber: 4,
    axisLine: { show: false },
    axisTick: { show: false },
    splitLine: {
      lineStyle: {
        color: "rgba(148, 163, 184, 0.10)"
      }
    },
    axisLabel: getAxisLabel((value) => axisValueFormatter(value))
  };

  const buildSeries = (dataset, animate = true) => normalizedSeries.map((item, index) => {
    const palette = getPalette(index);

    return {
      name: item.name || `Series ${index + 1}`,
      type: "bar",
      data: dataset[index] || [],
      stack: stacked ? "total" : undefined,
      large: perf.large,
      largeThreshold: perf.largeThreshold,
      progressive: perf.progressive,
      progressiveThreshold: perf.progressiveThreshold,
      barMaxWidth: horizontal ? 18 : 28,
      barMinHeight: 3,
      animationDuration: animate && perf.animation ? 1150 : 0,
      animationDurationUpdate: animate && perf.animation ? 1150 : 0,
      animationEasing: "quarticOut",
      animationDelay: animate && perf.animation
        ? (dataIndex) => Math.min(dataIndex * 75 + index * 45, 540)
        : 0,
      animationDelayUpdate: animate && perf.animation
        ? (dataIndex) => Math.min(dataIndex * 75 + index * 45, 540)
        : 0,
      emphasis: {
        focus: "series"
      },
      itemStyle: {
        borderRadius: horizontal ? [0, 12, 12, 0] : [12, 12, 0, 0],
        color: makeGradient(index, horizontal, 0.98, 0.72),
        shadowBlur: 18,
        shadowColor: withOpacity(palette.from, 0.16),
        shadowOffsetY: 8
      }
    };
  });

  const actualDataset = normalizedSeries.map((item) => item.data || []);
  const zeroDataset = actualDataset.map((seriesData) => seriesData.map(() => 0));

  const baseOption = {
    animation: perf.animation,
    animationDuration: 0,
    animationDurationUpdate: perf.animation ? 1150 : 0,
    animationEasing: "quarticOut",
    animationThreshold: 2000,
    grid: getGrid(horizontal),
    legend: getLegend(showLegend || normalizedSeries.length > 1),
    tooltip: getTooltipBase(tooltipFormatter || defaultTooltip),
    xAxis: horizontal ? valueAxis : categoryAxis,
    yAxis: horizontal ? categoryAxis : valueAxis
  };

  chart.setOption({
    ...baseOption,
    series: buildSeries(perf.animation ? zeroDataset : actualDataset, false)
  });

  if (perf.animation) {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        chart.setOption({
          series: buildSeries(actualDataset, true)
        });
      });
    });
  }

  chart.resize({ height });
  return chart;
}
