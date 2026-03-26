import {
  formatFull,
  getPalette,
  getTooltipBase,
  initChart,
  withOpacity
} from "./echarts_theme.js";

function buildTreemapData(rows) {
  const scopeMap = new Map();

  rows.forEach((row) => {
    const scopeName = row.scope || "Other";
    const categoryName = row.category || "Uncategorized";
    const value = Number(row.emissions || 0);
    if (!value) {
      return;
    }

    if (!scopeMap.has(scopeName)) {
      scopeMap.set(scopeName, { name: scopeName, children: [], value: 0 });
    }
    const scope = scopeMap.get(scopeName);
    scope.value += value;

    let category = scope.children.find((item) => item.name === categoryName);
    if (!category) {
      category = { name: categoryName, value: 0 };
      scope.children.push(category);
    }
    category.value += value;
  });

  return Array.from(scopeMap.values()).map((scope, index) => {
    const palette = getPalette(index);
    return {
      ...scope,
      itemStyle: {
        color: withOpacity(palette.from, 0.9)
      }
    };
  });
}

export function renderCategoryTreemap(config) {
  const { container, rows = [], height = 380 } = config;
  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  chart.setOption({
    animation: true,
    animationDuration: 1000,
    animationEasing: "cubicOut",
    tooltip: {
      ...getTooltipBase((params) => {
        const path = Array.isArray(params.treePathInfo)
          ? params.treePathInfo.slice(1).map((item) => item.name).join(" / ")
          : params.name || "";
        return `
          <div>
            <div style="font-size:12px;font-weight:700;color:rgba(226,232,240,.82);margin-bottom:4px;">${path}</div>
            <strong>${formatFull(params.value || 0)} tCO₂e</strong>
          </div>
        `;
      }),
      trigger: "item"
    },
    series: [
      {
        type: "treemap",
        roam: false,
        breadcrumb: { show: false },
        nodeClick: false,
        visibleMin: 1,
        label: {
          show: true,
          color: "#0f172a",
          fontSize: 12,
          fontWeight: 700,
          formatter: "{b}"
        },
        upperLabel: {
          show: true,
          height: 24,
          color: "#0f172a",
          fontSize: 12,
          fontWeight: 700
        },
        itemStyle: {
          borderColor: "rgba(255,255,255,0.92)",
          borderWidth: 3,
          gapWidth: 3,
          borderRadius: 14
        },
        levels: [
          {
            itemStyle: {
              borderColor: "rgba(255,255,255,0.96)",
              borderWidth: 4,
              gapWidth: 4,
              borderRadius: 16
            }
          },
          {
            colorSaturation: [0.35, 0.75],
            itemStyle: {
              gapWidth: 3,
              borderRadius: 12
            }
          }
        ],
        data: buildTreemapData(rows)
      }
    ]
  });

  chart.resize({ height });
  return chart;
}
