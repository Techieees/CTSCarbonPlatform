import {
  formatFull,
  getTooltipBase,
  initChart,
  withOpacity
} from "./echarts_theme.js";
import { getColorByKey } from "./chart_colors.js";

function buildHierarchy(rows) {
  const companyMap = new Map();

  rows.forEach((row) => {
    const companyName = row.company || "Company";
    const scopeName = row.scope || "Other";
    const categoryName = row.category || "Uncategorized";
    const value = Number(row.emissions || 0);
    if (!value) {
      return;
    }

    if (!companyMap.has(companyName)) {
      companyMap.set(companyName, { name: companyName, children: [], value: 0 });
    }
    const company = companyMap.get(companyName);
    company.value += value;

    let scope = company.children.find((item) => item.name === scopeName);
    if (!scope) {
      scope = { name: scopeName, children: [], value: 0 };
      company.children.push(scope);
    }
    scope.value += value;

    let category = scope.children.find((item) => item.name === categoryName);
    if (!category) {
      category = { name: categoryName, value: 0 };
      scope.children.push(category);
    }
    category.value += value;
  });

  return Array.from(companyMap.values()).map((company) => ({
    ...company,
    itemStyle: {
      color: withOpacity(getColorByKey(company.name, "company"), 0.95)
    },
    children: company.children.map((scope) => ({
      ...scope,
      itemStyle: {
        color: withOpacity(getColorByKey(scope.name, "scope"), 0.92)
      },
      children: scope.children.map((cat) => ({
        ...cat,
        itemStyle: {
          color: withOpacity(getColorByKey(cat.name, "category"), 0.88)
        }
      }))
    }))
  }));
}

export function renderScopeSunburst(config) {
  const { container, rows = [], height = 380 } = config;
  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const data = buildHierarchy(rows);

  chart.setOption({
    animation: true,
    animationDuration: 1100,
    animationEasing: "cubicOut",
    tooltip: {
      ...getTooltipBase((params) => {
        const name = params.name || "";
        const value = params.value || 0;
        const path = Array.isArray(params.treePathInfo)
          ? params.treePathInfo.slice(1).map((item) => item.name).join(" / ")
          : name;
        return `
          <div>
            <div style="font-size:12px;font-weight:700;color:rgba(226,232,240,.82);margin-bottom:4px;">${name}</div>
            <div style="font-size:12px;color:rgba(226,232,240,.72);margin-bottom:6px;">${path}</div>
            <strong>${formatFull(value)} tCO₂e</strong>
          </div>
        `;
      }),
      trigger: "item"
    },
    series: [
      {
        type: "sunburst",
        radius: [16, "92%"],
        sort: (a, b) => (b?.getValue?.() || 0) - (a?.getValue?.() || 0),
        emphasis: {
          focus: "ancestor"
        },
        data,
        itemStyle: {
          borderRadius: 8,
          borderColor: "rgba(255,255,255,0.9)",
          borderWidth: 2
        },
        label: {
          color: "#0f172a",
          fontWeight: 600
        },
        levels: [
          {},
          {
            r0: "0%",
            r: "28%",
            label: { rotate: 0 }
          },
          {
            r0: "30%",
            r: "60%",
            label: { rotate: "radial" }
          },
          {
            r0: "62%",
            r: "92%",
            label: { rotate: "tangential", fontSize: 11 }
          }
        ]
      }
    ]
  });

  chart.resize({ height });
  return chart;
}
