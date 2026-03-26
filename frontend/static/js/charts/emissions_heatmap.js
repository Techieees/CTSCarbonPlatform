import {
  formatFull,
  getAxisLabel,
  getTooltipBase,
  initChart
} from "./echarts_theme.js";

export function renderEmissionsHeatmap(config) {
  const { container, rows = [], height = 380 } = config;
  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const normalizedRows = rows.filter((row) => row && row.company && row.dateLabel);
  const months = Array.from(new Set(normalizedRows.map((row) => row.dateLabel)));
  const companies = Array.from(new Set(normalizedRows.map((row) => row.company)));
  const valueMap = new Map();

  normalizedRows.forEach((row) => {
    const key = `${row.company}__${row.dateLabel}`;
    valueMap.set(key, (valueMap.get(key) || 0) + Number(row.emissions || 0));
  });

  const seriesData = [];
  let maxValue = 0;

  months.forEach((month, monthIndex) => {
    companies.forEach((company, companyIndex) => {
      const value = Number(valueMap.get(`${company}__${month}`) || 0);
      maxValue = Math.max(maxValue, value);
      seriesData.push([monthIndex, companyIndex, value]);
    });
  });

  chart.setOption({
    animation: companies.length * months.length < 6000,
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
        const [monthIndex, companyIndex, value] = params.value || [];
        const month = months[monthIndex] || "";
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
      data: months,
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
    visualMap: {
      min: 0,
      max: maxValue || 1,
      orient: "horizontal",
      left: "center",
      bottom: 0,
      calculable: true,
      itemWidth: 120,
      itemHeight: 12,
      textStyle: {
        color: "#64748b",
        fontSize: 12,
        fontWeight: 600
      },
      inRange: {
        color: ["#eff6ff", "#bfdbfe", "#60a5fa", "#4f46e5", "#7c3aed"]
      }
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
        },
        itemStyle: {
          borderRadius: 10,
          borderColor: "rgba(255,255,255,0.9)",
          borderWidth: 1
        }
      }
    ]
  });

  chart.resize({ height });
  return chart;
}
