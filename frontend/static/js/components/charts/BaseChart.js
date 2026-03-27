import { initChart } from "../../charts/echarts_theme.js";

export function showEmptyState(container, message = "No data for this view") {
  const el = typeof container === "string" ? document.querySelector(container) : container;
  if (!el) {
    return;
  }
  el.innerHTML = `<div class="cts-chart-empty d-flex align-items-center justify-content-center text-muted small p-4 text-center" style="min-height:220px;">${message}</div>`;
}

export function mountEChart(container, buildOption, { height, showLoadingFirst = false } = {}) {
  const el = typeof container === "string" ? document.querySelector(container) : container;
  if (!el) {
    return null;
  }
  if (height) {
    el.style.minHeight = `${height}px`;
  }
  const chart = initChart(el);
  if (!chart) {
    return null;
  }
  if (showLoadingFirst) {
    chart.showLoading("default", { text: "Loading…", color: "#64748b", textColor: "#94a3b8", maskColor: "rgba(15,23,42,0.06)" });
  }
  const opt = buildOption();
  chart.setOption(opt, true);
  chart.hideLoading();
  return chart;
}

export function whenVisible(element, callback, { rootMargin = "140px" } = {}) {
  if (!element) {
    return;
  }
  if (!("IntersectionObserver" in window)) {
    callback();
    return;
  }
  const io = new IntersectionObserver(
    (entries) => {
      if (entries.some((e) => e.isIntersecting)) {
        io.disconnect();
        callback();
      }
    },
    { rootMargin, threshold: 0.01 }
  );
  io.observe(element);
}
