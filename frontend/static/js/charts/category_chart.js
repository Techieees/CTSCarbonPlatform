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

export function renderCategoryChart(config) {
  const variant = config.variant || "bar";
  return variant === "donut" ? renderDonutCategoryChart(config) : renderHorizontalCategoryChart(config);
}

function renderHorizontalCategoryChart(config) {
  const {
    container,
    labels = [],
    values = [],
    height = 320,
    tooltipSuffix = "",
    axisValueFormatter = formatCompact
  } = config;

  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const perf = getPerformanceOptions(values.length || labels.length, "bar");
  const actualValues = values;
  const zeroValues = actualValues.map(() => 0);

  chart.setOption({
    animation: perf.animation,
    animationDuration: 0,
    animationDurationUpdate: perf.animation ? 1100 : 0,
    animationEasing: "quarticOut",
    grid: getGrid(true),
    legend: getLegend(false),
    tooltip: getTooltipBase((params) => {
      const entry = Array.isArray(params) ? params[0] : params;
      return `
        <div>
          <div style="font-size:12px;font-weight:600;color:rgba(226,232,240,.82);margin-bottom:4px;">${entry.name}</div>
          <div style="font-size:13px;font-weight:700;">${formatFull(entry.value)}${tooltipSuffix}</div>
        </div>
      `;
    }),
    xAxis: {
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
    yAxis: {
      type: "category",
      data: labels,
      inverse: true,
      axisTick: { show: false },
      axisLine: { show: false },
      splitLine: { show: false },
      axisLabel: {
        ...getAxisLabel((value) => value),
        width: 140,
        overflow: "truncate"
      }
    },
    series: [
      {
        name: config.seriesName || "Category",
        type: "bar",
        data: perf.animation ? zeroValues : actualValues,
        large: perf.large,
        largeThreshold: perf.largeThreshold,
        progressive: perf.progressive,
        progressiveThreshold: perf.progressiveThreshold,
        barMaxWidth: 16,
        animationDuration: perf.animation ? 1100 : 0,
        animationDurationUpdate: perf.animation ? 1100 : 0,
        animationEasing: "quarticOut",
        animationDelay: perf.animation ? (dataIndex) => Math.min(dataIndex * 70, 520) : 0,
        animationDelayUpdate: perf.animation ? (dataIndex) => Math.min(dataIndex * 70, 520) : 0,
        itemStyle: {
          borderRadius: [0, 12, 12, 0],
          color: makeGradient(3, true, 0.98, 0.74),
          shadowBlur: 18,
          shadowColor: withOpacity("#8b5cf6", 0.16),
          shadowOffsetY: 8
        }
      }
    ]
  });

  if (perf.animation) {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        chart.setOption({
          series: [
            {
              data: actualValues
            }
          ]
        });
      });
    });
  }

  chart.resize({ height });
  return chart;
}

function renderDonutCategoryChart(config) {
  const {
    container,
    labels = [],
    values = [],
    height = 280,
    tooltipSuffix = "",
    totalLabel = "Total"
  } = config;

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
        shadowBlur: 14,
        shadowColor: withOpacity(palette.from, 0.14)
      }
    };
  });

  chart.setOption({
    animation: true,
    animationDuration: 860,
    animationDurationUpdate: 420,
    animationEasing: "cubicOut",
    legend: getLegend(true),
    tooltip: {
      ...getTooltipBase((params) => {
        const percent = Number(params.percent || 0).toFixed(1);
        return `
          <div>
            <div style="font-size:12px;font-weight:600;color:rgba(226,232,240,.82);margin-bottom:4px;">${params.name}</div>
            <div style="font-size:13px;font-weight:700;">${formatFull(params.value)}${tooltipSuffix}</div>
            <div style="font-size:12px;color:rgba(226,232,240,.82);margin-top:4px;">Share: ${percent}%</div>
          </div>
        `;
      }),
      trigger: "item"
    },
    series: [
      {
        name: config.seriesName || "Category Share",
        type: "pie",
        radius: ["56%", "76%"],
        center: ["50%", "50%"],
        avoidLabelOverlap: true,
        minAngle: 3,
        label: { show: false },
        labelLine: { show: false },
        emphasis: { scale: false },
        itemStyle: {
          borderColor: "#ffffff",
          borderWidth: 3
        },
        data: seriesData
      }
    ],
    graphic: [
      {
        type: "text",
        left: "center",
        top: "42%",
        silent: true,
        style: {
          text: totalLabel,
          fill: "#94a3b8",
          font: "600 12px Inter, sans-serif"
        }
      },
      {
        type: "text",
        left: "center",
        top: "49%",
        silent: true,
        style: {
          text: formatCompact(values.reduce((sum, value) => sum + Number(value || 0), 0)),
          fill: "#0f172a",
          font: "700 22px Inter, sans-serif"
        }
      }
    ]
  });

  chart.resize({ height });
  return chart;
}
