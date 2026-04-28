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
import { getColorByKey } from "./chart_colors.js";

function scopeBarEntries(values, labelList, valueFn) {
  return values.map((v, di) => {
    const lab = labelList[di] ?? "";
    const c = getColorByKey(lab, "scope");
    return {
      value: valueFn(v, di),
      itemStyle: {
        borderRadius: [12, 12, 0, 0],
        color: c,
        shadowBlur: 16,
        shadowColor: withOpacity(c, 0.14),
        shadowOffsetY: 8
      }
    };
  });
}

export function renderScopeChart(config) {
  const {
    container,
    labels = [],
    values = [],
    series,
    height = 300,
    stacked = false,
    showLegend = false,
    tooltipSuffix = "",
    axisValueFormatter = formatCompact
  } = config;

  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const normalizedSeries = Array.isArray(series) && series.length
    ? series
    : [{ name: config.seriesName || "Scope", data: values }];

  const largestSeries = Math.max(labels.length, ...normalizedSeries.map((item) => item.data?.length || 0));
  const perf = getPerformanceOptions(largestSeries, "bar");

  const tooltip = getTooltipBase((params) => {
    const entries = Array.isArray(params) ? params : [params];
    const title = entries[0]?.axisValueLabel || entries[0]?.name || "";
    const rows = entries
      .map((entry) => `<div style="display:flex;justify-content:space-between;gap:16px;margin-top:6px;"><span>${entry.seriesName}</span><strong>${formatFull(entry.value)}${tooltipSuffix}</strong></div>`)
      .join("");

    return `<div><div style="font-size:12px;font-weight:600;color:rgba(226,232,240,.82);margin-bottom:2px;">${title}</div>${rows}</div>`;
  });

  const xAxis = {
    type: "category",
    data: labels,
    axisTick: { show: false },
    axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.18)" } },
    splitLine: { show: false },
    axisLabel: getAxisLabel((value) => value)
  };

  const yAxis = {
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

  const colorScopeBars = !stacked && normalizedSeries.length === 1;

  const buildSeries = (dataset, animate = true) =>
    normalizedSeries.map((item, index) => {
      const palette = getPalette(index);
      const isLast = index === normalizedSeries.length - 1;
      const name = item.name || `Scope ${index + 1}`;

      return {
        name,
        type: "bar",
        data: dataset[index] || [],
        stack: stacked ? "scope-total" : undefined,
        large: perf.large,
        largeThreshold: perf.largeThreshold,
        progressive: perf.progressive,
        progressiveThreshold: perf.progressiveThreshold,
        barMaxWidth: 28,
        barMinHeight: 3,
        animationDuration: animate && perf.animation ? 1150 : 0,
        animationDurationUpdate: animate && perf.animation ? 1150 : 0,
        animationEasing: "quarticOut",
        animationDelay: animate && perf.animation
          ? (dataIndex) => Math.min(dataIndex * 85 + index * 45, 580)
          : 0,
        animationDelayUpdate: animate && perf.animation
          ? (dataIndex) => Math.min(dataIndex * 85 + index * 45, 580)
          : 0,
        itemStyle: colorScopeBars
          ? undefined
          : {
              borderRadius: stacked
                ? isLast
                  ? [12, 12, 0, 0]
                  : [0, 0, 0, 0]
                : [12, 12, 0, 0],
              color: makeGradient(index, false, 0.98, 0.74),
              shadowBlur: 16,
              shadowColor: withOpacity(palette.from, 0.14),
              shadowOffsetY: 8
            }
      };
    });

  const rawDataset = normalizedSeries.map((item) => item.data || []);

  let actualDataset = rawDataset;
  let zeroDataset = rawDataset.map((seriesData) => seriesData.map(() => 0));

  if (colorScopeBars) {
    actualDataset = rawDataset.map((seriesData) =>
      scopeBarEntries(seriesData, labels, (v) => v)
    );
    zeroDataset = rawDataset.map((seriesData) => scopeBarEntries(seriesData, labels, () => 0));
  }

  chart.setOption({
    animation: perf.animation,
    animationDuration: 0,
    animationDurationUpdate: perf.animation ? 1150 : 0,
    animationEasing: "quarticOut",
    animationThreshold: 2000,
    grid: getGrid(false, showLegend || normalizedSeries.length > 1),
    legend: getLegend(showLegend || normalizedSeries.length > 1, normalizedSeries.length),
    tooltip,
    xAxis,
    yAxis,
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
