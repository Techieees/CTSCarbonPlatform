import {
  formatFull,
  getAxisLabel,
  getTooltipBase,
  initChart,
  withOpacity
} from "./echarts_theme.js";
import { getColorByKey } from "./chart_colors.js";
import { sortedReportingMonthSlots, rowReportingSortKey } from "./row_aggregates.js";

export function renderEmissionsHeatmap(config) {
  const { container, rows = [], height = 380 } = config;
  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const normalizedRows = rows.filter((row) => row && row.company && (row.dateLabel || row.sortKey));
  const slots = sortedReportingMonthSlots(normalizedRows);
  const monthLabels = slots.map((s) => s.dateLabel);
  const sortKeyToMonthIndex = new Map(slots.map((s, i) => [s.sortKey, i]));

  const companies = Array.from(new Set(normalizedRows.map((row) => row.company))).sort((a, b) =>
    String(a).localeCompare(String(b))
  );
  const valueMap = new Map();

  normalizedRows.forEach((row) => {
    const sk = rowReportingSortKey(row);
    const mi = sortKeyToMonthIndex.get(sk);
    if (mi === undefined) {
      return;
    }
    const key = `${row.company}__${mi}`;
    valueMap.set(key, (valueMap.get(key) || 0) + Number(row.emissions || 0));
  });

  const rawCells = [];
  let maxValue = 0;
  monthLabels.forEach((_, monthIndex) => {
    companies.forEach((company, companyIndex) => {
      const value = Number(valueMap.get(`${company}__${monthIndex}`) || 0);
      maxValue = Math.max(maxValue, value);
      rawCells.push({ monthIndex, companyIndex, value, company });
    });
  });

  const maxV = maxValue || 1;
  const seriesData = rawCells.map(({ monthIndex, companyIndex, value, company }) => {
    const base = getColorByKey(company, "company");
    const t = maxV > 0 ? Math.min(1, value / maxV) : 0;
    const color = value <= 0 ? "rgba(148,163,184,0.12)" : withOpacity(base, 0.15 + t * 0.82);
    return {
      value: [monthIndex, companyIndex, value],
      itemStyle: {
        borderRadius: 10,
        borderColor: "rgba(255,255,255,0.9)",
        borderWidth: 1,
        color
      }
    };
  });

  chart.setOption({
    animation: companies.length * monthLabels.length < 6000,
    animationDuration: 900,
    animationEasing: "cubicOut",
    grid: {
      top: 18,
      right: 22,
      bottom: 18,
      left: 18,
      containLabel: true
    },
    tooltip: {
      ...getTooltipBase((params) => {
        const raw = params.value || [];
        const monthIndex = raw[0];
        const companyIndex = raw[1];
        const value = raw[2];
        const month = monthLabels[monthIndex] || "";
        const company = companies[companyIndex] || "";
        return `
          <div>
            <div style="font-size:12px;font-weight:700;color:rgba(226,232,240,.82);margin-bottom:4px;">${company}</div>
            <div style="display:flex;justify-content:space-between;gap:18px;">
              <span>${month}</span>
              <strong>${formatFull(value || 0)} tCO₂e</strong>
            </div>
          </div>
        `;
      }),
      trigger: "item"
    },
    xAxis: {
      type: "category",
      data: monthLabels,
      splitArea: { show: false },
      axisTick: { show: false },
      axisLine: { lineStyle: { color: "rgba(148,163,184,0.16)" } },
      splitLine: { show: false },
      axisLabel: getAxisLabel((value) => value)
    },
    yAxis: {
      type: "category",
      data: companies,
      splitArea: { show: false },
      axisTick: { show: false },
      axisLine: { show: false },
      splitLine: { show: false },
      axisLabel: getAxisLabel((value) => value)
    },
    series: [
      {
        name: "Emission Intensity",
        type: "heatmap",
        progressive: seriesData.length > 4000 ? 3000 : 0,
        progressiveThreshold: 8000,
        data: seriesData,
        label: { show: false },
        emphasis: {
          itemStyle: {
            shadowBlur: 22,
            shadowColor: "rgba(37,99,235,0.22)"
          }
        }
      }
    ]
  });

  chart.resize({ height });
  return chart;
}
