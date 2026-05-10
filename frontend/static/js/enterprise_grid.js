/**
 * Shared helpers for enterprise grid pages (resize, debounce).
 * Safe no-op when ECharts is absent.
 */

export function debounce(fn, wait = 120) {
  let t = null;
  return function debounced(...args) {
    const ctx = this;
    clearTimeout(t);
    t = setTimeout(() => fn.apply(ctx, args), wait);
  };
}

/**
 * Resize ECharts instances mounted inside `.eg-chart-canvas-host` under root.
 */
export function resizeHostedCharts(root) {
  if (!root || typeof window === "undefined" || !window.echarts) {
    return;
  }
  const hosts = root.querySelectorAll(".eg-chart-canvas-host");
  if (!hosts.length) {
    return;
  }
  window.requestAnimationFrame(() => {
    hosts.forEach((el) => {
      try {
        const chart = window.echarts.getInstanceByDom(el);
        if (chart && typeof chart.resize === "function") {
          chart.resize();
        }
      } catch {
        /* ignore */
      }
    });
  });
}

/**
 * Observe layout size changes (sidebar, flex reflow) and debounce chart resize.
 * Returns disconnect function.
 */
export function bindChartHostResizeObserver(root, options = {}) {
  if (!root || typeof ResizeObserver === "undefined") {
    return () => {};
  }
  const delay = typeof options.wait === "number" ? options.wait : 80;
  let t = null;
  const schedule = () => {
    clearTimeout(t);
    t = setTimeout(() => resizeHostedCharts(root), delay);
  };
  const obs = new ResizeObserver(schedule);
  obs.observe(root);
  return () => {
    clearTimeout(t);
    try {
      obs.disconnect();
    } catch {
      /* ignore */
    }
  };
}

const api = {
  debounce,
  resizeHostedCharts,
  bindChartHostResizeObserver
};

if (typeof window !== "undefined") {
  window.CtsEnterpriseGrid = api;
}
