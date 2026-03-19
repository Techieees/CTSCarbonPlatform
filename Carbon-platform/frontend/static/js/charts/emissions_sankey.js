import {
  formatFull,
  getPalette,
  getTooltipBase,
  initChart,
  withOpacity
} from "./echarts_theme.js";

function cleanNodeLabel(value) {
  return String(value || "")
    .replace(/^company:/, "")
    .replace(/^scope:/, "")
    .replace(/^category:[^:]*:/, "");
}

function buildSankey(rows) {
  const nodes = new Map();
  const links = new Map();

  const ensureNode = (id, name, color) => {
    if (!nodes.has(id)) {
      nodes.set(id, {
        id,
        name,
        itemStyle: color ? { color } : undefined
      });
    }
  };

  const addLink = (source, target, value) => {
    const key = `${source}__${target}`;
    links.set(key, (links.get(key) || 0) + Number(value || 0));
  };

  rows.forEach((row, index) => {
    const company = row.company || "Company";
    const scope = row.scope || "Other";
    const category = row.category || "Uncategorized";
    const value = Number(row.emissions || 0);
    if (!value) {
      return;
    }

    const companyId = `company:${company}`;
    const scopeId = `scope:${scope}`;
    const categoryId = `category:${scope}:${category}`;
    const palette = getPalette(index);

    ensureNode(companyId, company, withOpacity(palette.from, 0.95));
    ensureNode(scopeId, scope, withOpacity("#64748b", 0.88));
    ensureNode(categoryId, category, withOpacity(palette.to, 0.84));

    addLink(companyId, scopeId, value);
    addLink(scopeId, categoryId, value);
  });

  return {
    nodes: Array.from(nodes.values()),
    links: Array.from(links.entries()).map(([key, value]) => {
      const [source, target] = key.split("__");
      return { source, target, value };
    })
  };
}

export function renderEmissionsSankey(config) {
  const { container, rows = [], height = 440 } = config;
  const chart = initChart(container);
  if (!chart) {
    return null;
  }

  const sankey = buildSankey(rows);

  chart.setOption({
    animation: true,
    animationDuration: 1100,
    animationEasing: "cubicOut",
    tooltip: {
      ...getTooltipBase((params) => {
        if (params.dataType === "edge") {
          return `
            <div>
              <div style="font-size:12px;font-weight:700;color:rgba(226,232,240,.82);margin-bottom:4px;">${cleanNodeLabel(params.data.source)} → ${cleanNodeLabel(params.data.target)}</div>
              <strong>${formatFull(params.data.value || 0)} tCO₂e</strong>
            </div>
          `;
        }
        return `
          <div>
            <div style="font-size:12px;font-weight:700;color:rgba(226,232,240,.82);margin-bottom:4px;">${params.name}</div>
            <div>Emission flow node</div>
          </div>
        `;
      }),
      trigger: "item"
    },
    series: [
      {
        type: "sankey",
        data: sankey.nodes,
        links: sankey.links,
        left: 8,
        top: 12,
        right: 8,
        bottom: 12,
        draggable: false,
        emphasis: {
          focus: "adjacency"
        },
        lineStyle: {
          color: "source",
          curveness: 0.52,
          opacity: 0.32
        },
        itemStyle: {
          borderColor: "rgba(255,255,255,0.9)",
          borderWidth: 1
        },
        label: {
          color: "#0f172a",
          fontSize: 12,
          fontWeight: 700
        },
        levels: [
          {
            depth: 0,
            itemStyle: {
              color: withOpacity("#3b82f6", 0.92)
            }
          },
          {
            depth: 1,
            itemStyle: {
              color: withOpacity("#6366f1", 0.88)
            }
          },
          {
            depth: 2,
            itemStyle: {
              color: withOpacity("#8b5cf6", 0.84)
            }
          }
        ]
      }
    ]
  });

  chart.resize({ height });
  return chart;
}
