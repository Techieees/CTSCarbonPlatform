const THEME_NAME = "cts-enterprise-saas";
let themeRegistered = false;
let resizeBindingReady = false;

const chartRegistry = new Set();
const resizeObservers = new WeakMap();

function getCssVar(name, fallback) {
  const styles = window.getComputedStyle(document.body || document.documentElement);
  const value = styles.getPropertyValue(name).trim();
  return value || fallback;
}

const themeDefinition = {
  color: ["#3b82f6", "#8b5cf6", "#14b8a6", "#f59e0b", "#ec4899", "#06b6d4"],
  backgroundColor: "transparent",
  textStyle: {
    fontFamily: 'ui-sans-serif, system-ui, -apple-system, "Segoe UI", Inter, Roboto, "Helvetica Neue", Arial, sans-serif'
  },
  grid: {
    top: 24,
    right: 20,
    bottom: 24,
    left: 20,
    containLabel: true
  },
  categoryAxis: {
    axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.22)" } },
    axisTick: { show: false },
    splitLine: { show: false },
    axisLabel: { color: "#94a3b8", fontSize: 12, fontWeight: 500 }
  },
  valueAxis: {
    axisLine: { show: false },
    axisTick: { show: false },
    splitLine: { lineStyle: { color: "rgba(148, 163, 184, 0.10)" } },
    axisLabel: { color: "#94a3b8", fontSize: 12, fontWeight: 500 }
  },
  legend: {
    textStyle: { color: "#64748b", fontSize: 12, fontWeight: 600 }
  }
};

const palettes = [
  ["#3b82f6", "#8b5cf6"],
  ["#14b8a6", "#22c55e"],
  ["#f59e0b", "#f97316"],
  ["#ec4899", "#8b5cf6"],
  ["#06b6d4", "#3b82f6"],
  ["#64748b", "#334155"]
];

export function formatCompact(value) {
  return new Intl.NumberFormat("en", {
    notation: "compact",
    maximumFractionDigits: 1
  }).format(Number(value || 0));
}

export function formatFull(value) {
  return new Intl.NumberFormat("en", {
    maximumFractionDigits: 2
  }).format(Number(value || 0));
}

export function getPalette(index = 0) {
  const [from, to] = palettes[index % palettes.length];
  return { from, to };
}

export function makeGradient(index = 0, horizontal = false, opacityFrom = 1, opacityTo = 0.78) {
  const { from, to } = getPalette(index);
  return new window.echarts.graphic.LinearGradient(
    horizontal ? 0 : 0,
    horizontal ? 0 : 1,
    horizontal ? 1 : 0,
    horizontal ? 0 : 0,
    [
      { offset: 0, color: withOpacity(from, opacityFrom) },
      { offset: 1, color: withOpacity(to, opacityTo) }
    ]
  );
}

export function withOpacity(hex, opacity) {
  if (typeof hex !== "string") {
    return `rgba(100, 116, 139, ${opacity})`;
  }

  if (/^hsla?\(/i.test(hex)) {
    const parts = hex
      .replace(/^hsla?\(/i, "")
      .replace(/\)$/, "")
      .split(",")
      .map((part) => part.trim());
    const [h, s, l] = parts;
    return `hsla(${h}, ${s}, ${l}, ${opacity})`;
  }

  if (/^rgba?\(/i.test(hex)) {
    const parts = hex
      .replace(/^rgba?\(/i, "")
      .replace(/\)$/, "")
      .split(",")
      .map((part) => part.trim());
    const [r, g, b] = parts;
    return `rgba(${r}, ${g}, ${b}, ${opacity})`;
  }

  const normalized = hex.replace("#", "");
  const value = normalized.length === 3
    ? normalized.split("").map((char) => char + char).join("")
    : normalized;

  const r = Number.parseInt(value.slice(0, 2), 16);
  const g = Number.parseInt(value.slice(2, 4), 16);
  const b = Number.parseInt(value.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${opacity})`;
}

export function getPerformanceOptions(dataSize, kind = "bar") {
  const count = Math.max(0, Number(dataSize || 0));
  const veryLarge = count >= 100000;
  const large = count >= 2000;

  return {
    animation: count < 12000,
    progressive: veryLarge ? 12000 : large ? 4000 : 0,
    progressiveThreshold: veryLarge ? 30000 : large ? 8000 : 0,
    large: kind === "bar" && count >= 1500,
    largeThreshold: kind === "bar" ? 1500 : 0,
    sampling: kind === "line" && count >= 1000 ? "lttb" : undefined
  };
}

export function getGrid(horizontal = false, withLegend = false) {
  return {
    top: 24,
    right: horizontal ? 20 : 12,
    bottom: withLegend ? 72 : 18,
    left: horizontal ? 12 : 8,
    containLabel: true
  };
}

export function getAxisLabel(formatter) {
  return {
    color: getCssVar("--chart-axis-color", "#94a3b8"),
    fontSize: 12,
    fontWeight: 500,
    formatter
  };
}

export function getTooltipBase(customFormatter) {
  return {
    trigger: "axis",
    axisPointer: {
      type: "shadow",
      shadowStyle: {
        color: getCssVar("--chart-grid-color", "rgba(148, 163, 184, 0.08)")
      }
    },
    backgroundColor: getCssVar("--chart-tooltip-bg", "rgba(15, 23, 42, 0.94)"),
    borderWidth: 0,
    textStyle: {
      color: getCssVar("--chart-tooltip-text", "#f8fafc"),
      fontSize: 12,
      fontWeight: 500
    },
    padding: [12, 14],
    extraCssText: "border-radius:14px;box-shadow:0 18px 42px rgba(2,6,23,.28);backdrop-filter:blur(14px);",
    formatter: customFormatter
  };
}

export function getLegend(show, itemCount = 0) {
  return show
    ? {
        type: itemCount > 10 ? "scroll" : "plain",
        bottom: 0,
        left: 0,
        right: 0,
        itemWidth: 10,
        itemHeight: 10,
        icon: "roundRect",
        selectedMode: true,
        pageIconColor: getCssVar("--chart-axis-strong", "#475569"),
        pageIconInactiveColor: "rgba(148, 163, 184, 0.4)",
        pageTextStyle: {
          color: getCssVar("--muted", "#64748b"),
          fontSize: 11,
          fontWeight: 600
        },
        textStyle: {
          color: getCssVar("--muted", "#64748b"),
          fontSize: 12,
          fontWeight: 600
        },
        tooltip: {
          show: true
        }
      }
    : { show: false };
}

function ensureResizeBinding() {
  if (resizeBindingReady) {
    return;
  }

  resizeBindingReady = true;
  window.addEventListener(
    "resize",
    () => {
      chartRegistry.forEach((chart) => chart.resize());
    },
    { passive: true }
  );
}

function bindResizeObserver(element, chart) {
  if (!("ResizeObserver" in window)) {
    return;
  }

  const existing = resizeObservers.get(element);
  if (existing) {
    existing.disconnect();
  }

  const observer = new ResizeObserver(() => {
    chart.resize();
  });

  observer.observe(element);
  resizeObservers.set(element, observer);
}

export function ensureTheme() {
  if (themeRegistered || typeof window.echarts === "undefined") {
    return;
  }

  window.echarts.registerTheme(THEME_NAME, themeDefinition);
  themeRegistered = true;
}

export function resolveContainer(container) {
  if (!container) {
    return null;
  }

  return typeof container === "string" ? document.querySelector(container) : container;
}

export function initChart(container) {
  const element = resolveContainer(container);
  if (!element || typeof window.echarts === "undefined") {
    return null;
  }

  ensureTheme();
  ensureResizeBinding();

  const existing = window.echarts.getInstanceByDom(element);
  if (existing) {
    chartRegistry.delete(existing);
    existing.dispose();
  }

  const chart = window.echarts.init(element, THEME_NAME, {
    renderer: "canvas",
    useDirtyRect: true
  });

  chartRegistry.add(chart);
  bindResizeObserver(element, chart);

  const rect = element.getBoundingClientRect();
  if (!rect.width || !rect.height) {
    // IntersectionObserver + flex/sidebar layouts can run before the host has a non-zero box.
    // Defer resize so Scope / Home charts still mount (otherwise blank white surfaces).
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        try {
          chart.resize();
        } catch {
          /* ignore */
        }
      });
    });
  }

  return chart;
}
