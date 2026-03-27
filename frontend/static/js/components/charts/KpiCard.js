/**
 * Fill KPI card slots from data attributes (no new markup contract).
 * Expects: .cts-kpi with data-kpi-key matching keys in values object.
 */
export function applyKpiValues(root, values) {
  const scope = root || document;
  scope.querySelectorAll("[data-kpi-key]").forEach((el) => {
    const key = el.getAttribute("data-kpi-key");
    if (!key || !(key in values)) {
      return;
    }
    const v = values[key];
    el.textContent = v === null || v === undefined ? "—" : String(v);
  });
}
