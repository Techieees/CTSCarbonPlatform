import {
  formatCompact,
  formatFull,
  getAxisLabel,
  getTooltipBase,
  getPalette,
  initChart,
  withOpacity
} from "./echarts_theme.js";

export function renderEmissionsRaceChart(config) {
  const { container, rows = [], height = 440 } = config;
  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const chartDom = chart.getDom();
  if (chartDom.__ctsRaceFrame) {
    window.cancelAnimationFrame(chartDom.__ctsRaceFrame);
    chartDom.__ctsRaceFrame = null;
  }
  if (chartDom.__ctsRaceStartTimeout) {
    window.clearTimeout(chartDom.__ctsRaceStartTimeout);
    chartDom.__ctsRaceStartTimeout = null;
  }

  const aggregated = new Map();
  rows
    .filter((row) => row && row.company && row.dateLabel)
    .forEach((row) => {
      const company = String(row.company || "").trim();
      const date = String(row.dateLabel || "").trim();
      const sortKey = String(row.sortKey || date);
      const emissions = Number(row.emissions || 0);
      const key = `${company}__${sortKey}`;
      const current = aggregated.get(key);

      if (current) {
        current.Emissions += emissions;
      } else {
        aggregated.set(key, {
          Company: company,
          Date: date,
          DateSort: sortKey,
          Emissions: emissions
        });
      }
    });

  const normalizedRows = Array.from(aggregated.values()).sort(
    (a, b) => String(a.DateSort).localeCompare(String(b.DateSort)) || String(a.Company).localeCompare(String(b.Company))
  );

  if (!normalizedRows.length) {
    return chart;
  }

  const dates = Array.from(new Set(normalizedRows.map((row) => row.Date)));
  const companies = Array.from(new Set(normalizedRows.map((row) => row.Company)));
  const monthIndexByLabel = new Map(dates.map((label, index) => [label, index]));
  const monthlyValueMatrix = new Map(
    companies.map((company) => [company, Array.from({ length: dates.length }, () => 0)])
  );

  normalizedRows.forEach((row) => {
    const values = monthlyValueMatrix.get(row.Company);
    const index = monthIndexByLabel.get(row.Date);
    if (values && typeof index === "number") {
      values[index] = Number(row.Emissions || 0);
    }
  });

  const cumulativeValueMatrix = new Map(
    companies.map((company) => {
      const monthlyValues = monthlyValueMatrix.get(company) || [];
      const cumulativeValues = Array.from({ length: monthlyValues.length }, () => 0);
      let runningTotal = 0;

      for (let i = 0; i < monthlyValues.length; i += 1) {
        runningTotal += Number(monthlyValues[i] || 0);
        cumulativeValues[i] = runningTotal;
      }

      return [company, cumulativeValues];
    })
  );

  const totalDuration = Math.min(12000, Math.max(8000, dates.length * 420));
  const startDelayMs = 0;

  function buildAnimatedSeries(progress) {
    const clamped = Math.max(0, Math.min(progress, Math.max(dates.length - 1, 0)));
    const whole = Math.floor(clamped);
    const fraction = clamped - whole;

    return companies.map((company, index) => {
      const palette = getPalette(index);
      const values = cumulativeValueMatrix.get(company) || [];
      const points = [];

      if (values.length && clamped >= 0) {
        points.push([0, Number(values[0] || 0)]);
      }

      for (let i = 1; i <= whole && i < values.length; i += 1) {
        points.push([i, Number(values[i] || 0)]);
      }

      if (whole < values.length - 1) {
        const fromX = Math.max(0, whole);
        const toX = whole + fraction;
        const fromY = Number(values[fromX] || 0);
        const toY = Number(values[Math.min(values.length - 1, whole + 1)] || 0);
        const interpolatedY = fromY + ((toY - fromY) * fraction);
        if (!points.length || points[points.length - 1][0] !== fromX) {
          points.push([fromX, fromY]);
        }
        points.push([toX, interpolatedY]);
      }

      return {
        type: "line",
        name: company,
        smooth: true,
        showSymbol: false,
        symbol: "circle",
        symbolSize: 6,
        data: points,
        animation: false,
        endLabel: {
          show: points.length > 0,
          valueAnimation: true,
          color: withOpacity(palette.from, 0.98),
          fontSize: 12,
          fontWeight: 700,
          distance: 8,
          formatter: (params) => {
            const currentValue = Array.isArray(params.value) ? params.value[1] : 0;
            return `${params.seriesName}: ${formatFull(currentValue)} tCO₂e`;
          }
        },
        labelLayout: {
          moveOverlap: "shiftY"
        },
        emphasis: {
          focus: "series"
        },
        lineStyle: {
          width: 3,
          color: withOpacity(palette.from, 0.95),
          shadowBlur: 18,
          shadowColor: withOpacity(palette.from, 0.18)
        },
        itemStyle: {
          color: withOpacity(palette.from, 1),
          borderColor: "#ffffff",
          borderWidth: 2
        }
      };
    });
  }

  chart.setOption({
    animation: false,
    color: companies.map((_, index) => getPalette(index).from),
    grid: {
      top: 24,
      right: 140,
      bottom: 26,
      left: 12,
      containLabel: true
    },
    tooltip: {
      ...getTooltipBase((params) => {
        const entries = Array.isArray(params) ? params : [params];
        const axisValue = Number(entries[0]?.axisValue ?? entries[0]?.value?.[0] ?? 0);
        const title = dates[Math.min(dates.length - 1, Math.max(0, Math.round(axisValue)))] || "";
        const rowsHtml = entries
          .filter((entry) => entry.value != null)
          .map((entry) => {
            const marker = `<span style="display:inline-block;width:10px;height:10px;border-radius:999px;margin-right:8px;background:${entry.color};"></span>`;
            const emissionValue = Array.isArray(entry.value) ? entry.value[1] : entry.value?.[1] ?? 0;
            return `<div style="display:flex;align-items:center;justify-content:space-between;gap:18px;margin-top:6px;">${marker}<span style="flex:1;">${entry.seriesName}</span><strong>${formatFull(emissionValue)} tCO₂e</strong></div>`;
          })
          .join("");
        return `<div><div style="font-size:12px;font-weight:700;color:rgba(226,232,240,.82);margin-bottom:4px;">${title}</div>${rowsHtml}</div>`;
      }),
      order: "valueDesc",
      trigger: "axis",
      axisPointer: {
        type: "line",
        lineStyle: {
          color: "rgba(59,130,246,0.18)",
          width: 1
        }
      }
    },
    legend: {
      top: 0,
      left: 0,
      icon: "roundRect",
      itemWidth: 10,
      itemHeight: 10,
      textStyle: {
        color: "#64748b",
        fontSize: 12,
        fontWeight: 600
      }
    },
    xAxis: {
      type: "value",
      min: 0,
      max: Math.max(dates.length - 1, 0),
      axisTick: { show: false },
      axisLine: { lineStyle: { color: "rgba(148,163,184,0.18)" } },
      splitLine: { show: false },
      axisLabel: getAxisLabel((value) => {
        const rounded = Math.round(Number(value));
        if (Math.abs(Number(value) - rounded) > 0.001) {
          return "";
        }
        return dates[rounded] || "";
      })
    },
    yAxis: {
      type: "value",
      splitNumber: 4,
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: {
        lineStyle: {
          color: "rgba(148,163,184,0.08)"
        }
      },
      axisLabel: getAxisLabel((value) => formatCompact(value))
    },
    series: []
  }, true);

  chartDom.__ctsRaceStartTimeout = window.setTimeout(() => {
    const startedAt = performance.now();

    const step = (now) => {
      const elapsed = now - startedAt;
      const progress = (elapsed / totalDuration) * Math.max(dates.length - 1, 0);

      chart.setOption({
        series: buildAnimatedSeries(progress)
      }, false, false);

      if (elapsed < totalDuration) {
        chartDom.__ctsRaceFrame = window.requestAnimationFrame(step);
      } else {
        chartDom.__ctsRaceFrame = null;
        chart.setOption({
          series: buildAnimatedSeries(dates.length - 1)
        }, false, false);
      }
    };

    chartDom.__ctsRaceFrame = window.requestAnimationFrame(step);
    chartDom.__ctsRaceStartTimeout = null;
  }, startDelayMs);

  chart.resize({ height });
  return chart;
}
