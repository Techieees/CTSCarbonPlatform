import {
  formatCompact,
  formatFull,
  getAxisLabel,
  getTooltipBase,
  initChart
} from "./echarts_theme.js";
import { getColorByKey } from "./chart_colors.js";
import { rowReportingSortKey } from "./row_aggregates.js";

export function renderEmissionsRaceChart(config) {
  const { container, rows = [], height = 440 } = config;
  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const aggregated = new Map();
  rows
    .filter((row) => row && row.company && (row.dateLabel || row.sortKey))
    .forEach((row) => {
      const company = String(row.company || "").trim();
      const sortKey = rowReportingSortKey(row);
      if (!sortKey) {
        return;
      }
      const date = String(row.dateLabel || sortKey).trim();
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
    (a, b) =>
      String(a.DateSort).localeCompare(String(b.DateSort)) || String(a.Company).localeCompare(String(b.Company))
  );

  if (!normalizedRows.length) {
    chart.resize({ height });
    return chart;
  }

  const slotMap = new Map();
  normalizedRows.forEach((row) => {
    if (!slotMap.has(row.DateSort)) {
      slotMap.set(row.DateSort, row.Date);
    }
  });
  const timelineKeys = Array.from(slotMap.keys()).sort((a, b) => String(a).localeCompare(String(b)));
  const displayLabels = timelineKeys.map((sk) => slotMap.get(sk));

  const companies = Array.from(new Set(normalizedRows.map((row) => row.Company))).sort((a, b) =>
    String(a).localeCompare(String(b))
  );
  const monthIndexBySort = new Map(timelineKeys.map((sk, index) => [sk, index]));
  const monthlyValueMatrix = new Map(
    companies.map((company) => [company, Array.from({ length: timelineKeys.length }, () => 0)])
  );

  normalizedRows.forEach((row) => {
    const values = monthlyValueMatrix.get(row.Company);
    const index = monthIndexBySort.get(row.DateSort);
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

  let globalMax = 0;
  companies.forEach((c) => {
    const arr = cumulativeValueMatrix.get(c) || [];
    const last = arr.length ? Number(arr[arr.length - 1] || 0) : 0;
    globalMax = Math.max(globalMax, last);
  });
  if (globalMax <= 0) {
    globalMax = 1;
  }

  const options = timelineKeys.map((_, t) => ({
    series: companies.map((company) => {
      const cum = cumulativeValueMatrix.get(company) || [];
      const lineColor = getColorByKey(company, "company");
      return {
        name: company,
        type: "line",
        smooth: true,
        showSymbol: true,
        symbol: "circle",
        symbolSize: 6,
        connectNulls: false,
        data: timelineKeys.map((__, i) => (i <= t ? Number(cum[i] || 0) : null)),
        lineStyle: {
          width: 3,
          color: lineColor
        },
        itemStyle: {
          color: lineColor,
          borderColor: "#ffffff",
          borderWidth: 2
        },
        endLabel: {
          show: true,
          valueAnimation: true,
          color: lineColor,
          fontSize: 12,
          fontWeight: 700,
          distance: 8,
          formatter: (params) => {
            const v = params.value;
            const num = typeof v === "number" ? v : 0;
            return `${params.seriesName}: ${formatFull(num)} tCO₂e`;
          }
        },
        labelLayout: {
          moveOverlap: "shiftY"
        },
        emphasis: {
          focus: "series"
        }
      };
    })
  }));

  chart.setOption(
    {
      baseOption: {
        animation: true,
        animationDuration: 1000,
        animationDurationUpdate: 800,
        animationEasing: "linear",
        animationEasingUpdate: "linear",
        color: companies.map((c) => getColorByKey(c, "company")),
        timeline: {
          axisType: "category",
          autoPlay: true,
          playInterval: 1000,
          loop: true,
          rewind: false,
          show: true,
          data: displayLabels,
          label: {
            color: "#64748b"
          },
          controlStyle: {
            color: "#64748b",
            borderColor: "rgba(148,163,184,0.35)"
          }
        },
        tooltip: {
          ...getTooltipBase((params) => {
            const entries = Array.isArray(params) ? params : [params];
            const month =
              entries[0]?.axisValueLabel ||
              entries[0]?.name ||
              "";
            const rowsHtml = entries
              .filter((entry) => entry.value != null && !Number.isNaN(Number(entry.value)))
              .map((entry) => {
                const marker = `<span style="display:inline-block;width:10px;height:10px;border-radius:999px;margin-right:8px;background:${entry.color};"></span>`;
                return `<div style="display:flex;align-items:center;justify-content:space-between;gap:18px;margin-top:6px;">${marker}<span style="flex:1;">${entry.seriesName}</span><strong>${formatFull(entry.value)} tCO₂e</strong></div>`;
              })
              .join("");
            return `<div><div style="font-size:12px;font-weight:700;color:rgba(226,232,240,.82);margin-bottom:4px;">${month}</div>${rowsHtml}</div>`;
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
        grid: {
          top: 56,
          right: 140,
          bottom: 26,
          left: 12,
          containLabel: true
        },
        xAxis: {
          type: "category",
          data: displayLabels,
          axisTick: { show: false },
          axisLine: { lineStyle: { color: "rgba(148,163,184,0.18)" } },
          splitLine: { show: false },
          axisLabel: getAxisLabel((value) => value)
        },
        yAxis: {
          type: "value",
          max: globalMax * 1.02,
          splitNumber: 4,
          axisLine: { show: false },
          axisTick: { show: false },
          splitLine: {
            lineStyle: {
              color: "rgba(148,163,184,0.08)"
            }
          },
          axisLabel: getAxisLabel((value) => formatCompact(value))
        }
      },
      options
    },
    true
  );

  chart.resize({ height });
  return chart;
}
