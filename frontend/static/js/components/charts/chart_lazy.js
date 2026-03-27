import { whenVisible } from "./BaseChart.js";

export function lazyMountChart(hostSelector, initFn) {
  const host = document.querySelector(hostSelector);
  if (!host) {
    return;
  }
  whenVisible(host, () => {
    try {
      initFn(host);
    } catch (e) {
      console.error("Chart init failed", hostSelector, e);
    }
  });
}
