import {
  formatCompact,
  formatFull,
  getAxisLabel,
  getGrid,
  getLegend,
  getPerformanceOptions,
  getTooltipBase,
  initChart,
  makeGradient,
  withOpacity
} from "./echarts_theme.js";
import { getColorByKey } from "./chart_colors.js";

function inferLineColorKind(seriesName) {
  const n = String(seriesName || "");
  if (/scope\s*[123]|direct emissions|indirect emissions|value chain/i.test(n)) {
    return "scope";
  }
  return "company";
}

function colorsForLineSeries(name, index, total, seriesColorKind) {
  if (seriesColorKind) {
    const c = getColorByKey(name, seriesColorKind);
    return {
      line: withOpacity(c, 0.96),
      itemColor: c,
      area: withOpacity(c, 0.22)
    };
  }
  if (total === 1) {
    return {
      line: withOpacity("#3b82f6", 0.96),
      itemColor: "#3b82f6",
      area: makeGradient(0, false, 0.28, 0.02)
    };
  }
  const c = getColorByKey(name, inferLineColorKind(name));
  return {
    line: withOpacity(c, 0.96),
    itemColor: c,
    area: withOpacity(c, 0.2)
  };
}

export function renderTrendChart(config) {
  const {
    container,
    labels = [],
    values = [],
    series,
    height = 320,
    showLegend = false,
    tooltipSuffix = "",
    axisValueFormatter = formatCompact,
    seriesColorKind = null
  } = config;

  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const normalizedSeries = Array.isArray(series) && series.length
    ? series
    : [{ name: config.seriesName || "Trend", data: values }];

  const largestSeries = Math.max(labels.length, ...normalizedSeries.map((item) => item.data?.length || 0));
  const perf = getPerformanceOptions(largestSeries, "line");

  chart.setOption({
    animation: perf.animation,
    animationDuration: perf.animation ? 1100 : 0,
    animationDurationUpdate: perf.animation ? 420 : 0,
    animationEasing: "cubicInOut",
    animationThreshold: 2500,
    grid: getGrid(false),
    legend: getLegend(showLegend || normalizedSeries.length > 1),
    tooltip: {
      ...getTooltipBase((params) => {
        const entries = Array.isArray(params) ? params : [params];
        const title = entries[0]?.axisValueLabel || entries[0]?.name || "";
        const rows = entries
          .map((entry) => `<div style="display:flex;justify-content:space-between;gap:16px;margin-top:6px;"><span>${entry.seriesName}</span><strong>${formatFull(entry.value)}${tooltipSuffix}</strong></div>`)
          .join("");

        return `<div><div style="font-size:12px;font-weight:600;color:rgba(226,232,240,.82);margin-bottom:2px;">${title}</div>${rows}</div>`;
      }),
      axisPointer: {
        type: "line",
        lineStyle: {
          color: "rgba(99, 102, 241, 0.24)",
          width: 1
        }
      }
    },
    xAxis: {
      type: "category",
      boundaryGap: false,
      data: labels,
      axisTick: { show: false },
      axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.18)" } },
      splitLine: { show: false },
      axisLabel: getAxisLabel((value) => value)
    },
    yAxis: {
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
    },
    dataZoom: labels.length > 60
      ? [
          {
            type: "inside",
            zoomOnMouseWheel: false,
            moveOnMouseMove: true,
            moveOnMouseWheel: true,
            throttle: 50
          }
        ]
      : [],
    series: normalizedSeries.map((seriesItem, index) => {
      const name = seriesItem.name || `Trend ${index + 1}`;
      const pal = colorsForLineSeries(
        name,
        index,
        normalizedSeries.length,
        seriesColorKind
      );
      return {
        name,
        type: "line",
        smooth: true,
        showSymbol: largestSeries <= 24,
        symbol: "circle",
        symbolSize: 7,
        sampling: perf.sampling,
        progressive: perf.progressive,
        progressiveThreshold: perf.progressiveThreshold,
        animationDuration: perf.animation ? 1100 : 0,
        animationEasing: "cubicInOut",
        animationDelay: perf.animation
          ? (dataIndex) => Math.min(dataIndex * 28 + index * 40, 360)
          : 0,
        data: seriesItem.data || [],
        lineStyle: {
          width: 3,
          color: pal.line
        },
        itemStyle: {
          color: pal.itemColor,
          borderColor: "#ffffff",
          borderWidth: 2
        },
        areaStyle: {
          color: pal.area
        },
        emphasis: {
          focus: "series"
        }
      };
    })
  });

  chart.resize({ height });
  return chart;
}
