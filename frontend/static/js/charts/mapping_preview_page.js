import { renderTrendChart } from "./trend_chart.js";
import { renderCompanyChart } from "./company_chart.js";

function esc(s) {
  const d = document.createElement("div");
  d.textContent = s == null ? "" : String(s);
  return d.innerHTML;
}

function renderChartSpec(container, spec, height) {
  if (!container || !spec) {
    return null;
  }
  container.innerHTML = "";
  const inner = document.createElement("div");
  inner.style.height = `${height}px`;
  inner.style.width = "100%";
  container.appendChild(inner);

  const suf = spec.tooltip_suffix || "";
  const name = spec.series_name || "Series";

  if (spec.renderer === "trend") {
    return renderTrendChart({
      container: inner,
      labels: spec.labels || [],
      values: spec.values || [],
      seriesName: name,
      height,
      tooltipSuffix: suf,
    });
  }

  return renderCompanyChart({
    container: inner,
    labels: spec.labels || [],
    values: spec.values || [],
    horizontal: !!spec.horizontal,
    height,
    tooltipSuffix: suf,
    seriesName: name,
  });
}

function applyAnalytics(payload) {
  const card = document.getElementById("mpAnalyticsCard");
  const head = document.getElementById("mpAnalyticsHeading");
  const sub = document.getElementById("mpAnalyticsSub");
  const sampled = document.getElementById("mpAnalyticsSampled");
  const ta = document.getElementById("mpChartTitleA");
  const tb = document.getElementById("mpChartTitleB");
  const hostA = document.getElementById("mpChartA");
  const hostB = document.getElementById("mpChartB");

  if (!card || !hostA || !hostB) {
    return;
  }

  const mode = payload.preview_mode || "emissions";
  const specs = Array.isArray(payload.charts) ? payload.charts : [];
  const cap = payload.rows_cap ?? "";
  const n = payload.rows_sampled ?? 0;

  if (head) {
    head.textContent =
      mode === "water" ? "Water usage analytics" : "Preview analytics (audit)";
  }
  if (sub) {
    sub.textContent =
      mode === "water"
        ? "Operational usage totals — no emission-factor mapping context."
        : "Aggregates from this mapping snapshot sample — does not replace formal inventory QA.";
  }
  if (sampled) {
    sampled.textContent =
      cap !== "" ? `Sample: ${n.toLocaleString()} / ≤${Number(cap).toLocaleString()} rows` : "";
  }

  if (!specs.length) {
    card.classList.remove("d-none");
    if (ta) ta.textContent = "";
    if (tb) tb.textContent = "";
    hostA.innerHTML =
      '<div class="text-muted small p-3 text-center">No chart — insufficient numeric columns for this sheet profile.</div>';
    hostB.innerHTML =
      '<div class="text-muted small p-3 text-center text-secondary">—</div>';
    return;
  }

  card.classList.remove("d-none");

  const s0 = specs[0];
  const s1 = specs[1];
  if (ta) {
    ta.textContent = s0 ? `${s0.title || ""}${s0.subtitle ? ` · ${s0.subtitle}` : ""}` : "";
  }
  if (tb) {
    tb.textContent = s1 ? `${s1.title || ""}${s1.subtitle ? ` · ${s1.subtitle}` : ""}` : "";
  }

  renderChartSpec(hostA, s0, 260);
  if (s1) {
    renderChartSpec(hostB, s1, 260);
  } else {
    hostB.innerHTML =
      '<div class="text-muted small p-3 text-center">No secondary chart for this snapshot.</div>';
  }

  const elStats = document.getElementById("mpStats");
  if (mode === "water" && payload.summary && elStats) {
    const t = payload.summary.total_usage_sampled;
    const u = payload.summary.unit_hint ? ` ${payload.summary.unit_hint}` : "";
    if (t != null && t !== "") {
      elStats.textContent += ` · Σ usage (sampled): ${t}${u}`;
    }
  }
}

export function bootMappingPreviewPage(cfg) {
  const runId = cfg.runId;
  const maxPageSize = cfg.maxPageSize || 100;
  const metaUrl = `/api/mapping-preview/${encodeURIComponent(runId)}/meta`;
  const rowsUrl = `/api/mapping-preview/${encodeURIComponent(runId)}/rows`;
  const analyticsUrl = `/api/mapping-preview/${encodeURIComponent(runId)}/analytics`;

  const root = document.getElementById("mpRoot");
  const elAlert = document.getElementById("mpAlert");
  const elSearch = document.getElementById("mpSearch");
  const elPageSize = document.getElementById("mpPageSize");
  const elStats = document.getElementById("mpStats");
  const elHeadRow = document.getElementById("mpHeadRow");
  const elBody = document.getElementById("mpBody");
  const elPrev = document.getElementById("mpPrev");
  const elNext = document.getElementById("mpNext");
  const elPageLabel = document.getElementById("mpPageLabel");
  const elTitle = document.getElementById("mpPageTitle");
  const elSubtitle = document.getElementById("mpPageSubtitle");

  let columns = [];
  let page = 1;
  let total = 0;
  let searchDebounce = null;

  function showAlert(msg) {
    if (!elAlert) return;
    if (!msg) {
      elAlert.classList.add("d-none");
      elAlert.textContent = "";
      return;
    }
    elAlert.textContent = msg;
    elAlert.classList.remove("d-none");
  }

  function renderHead() {
    if (!elHeadRow) return;
    elHeadRow.innerHTML = columns.map((c) => `<th scope="col">${esc(c)}</th>`).join("");
  }

  function renderRows(rows) {
    if (!elBody) return;
    elBody.innerHTML = "";
    for (const r of rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = columns.map((c) => `<td>${esc(r[c] ?? "")}</td>`).join("");
      elBody.appendChild(tr);
    }
  }

  async function fetchJson(url) {
    const res = await fetch(url, { headers: { Accept: "application/json" } });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || res.statusText || "Request failed");
    return data;
  }

  async function loadMeta() {
    const m = await fetchJson(metaUrl);
    const mode = m.preview_mode === "water" ? "water" : "emissions";
    if (root) {
      root.classList.remove("mp-root--water", "mp-root--emissions");
      root.classList.add(mode === "water" ? "mp-root--water" : "mp-root--emissions");
    }
    if (elTitle) {
      elTitle.textContent = mode === "water" ? "Water usage preview" : "Mapping preview";
    }
    if (elSubtitle) {
      elSubtitle.textContent =
        mode === "water"
          ? "Operational water usage snapshot — read-only."
          : "Mapped output snapshot — read-only audit view.";
    }

    const disp = Array.isArray(m.display_columns) ? m.display_columns.map(String) : [];
    const fallback = Array.isArray(m.columns) ? m.columns.map(String) : [];
    columns = disp.length ? disp : fallback;
    renderHead();

    const tc =
      m.totals_summary && typeof m.totals_summary.tco2e_total === "number"
        ? m.totals_summary.tco2e_total
        : null;

    if (elStats) {
      if (mode === "water") {
        const parts = [`${m.data_row_count ?? 0} data rows (snapshot)`];
        parts.push(`${m.mapped_row_count ?? ""} rows in snapshot`);
        elStats.textContent = parts.join(" · ");
      } else {
        const parts = [
          `${m.data_row_count ?? 0} preview rows`,
          `${m.mapped_row_count ?? ""} mapped rows recorded`,
          `${m.unmapped_row_count ?? ""} unmapped (No match)`,
        ];
        if (tc != null) parts.push(`Σ tCO₂e (df): ${tc}`);
        elStats.textContent = parts.join(" · ");
      }
    }

    if (!columns.length) showAlert("Column metadata missing for this snapshot.");

    try {
      const analytics = await fetchJson(analyticsUrl);
      applyAnalytics(analytics);
    } catch {
      const card = document.getElementById("mpAnalyticsCard");
      const hA = document.getElementById("mpChartA");
      if (card && hA) {
        card.classList.remove("d-none");
        hA.innerHTML = '<div class="text-muted small p-2">Analytics unavailable.</div>';
      }
    }
  }

  async function loadRows() {
    showAlert("");
    const q = (elSearch && elSearch.value) || "";
    const qs = q.trim();
    const ps = Math.min(maxPageSize, Math.max(1, parseInt(elPageSize.value || "50", 10) || 50));
    const u = new URL(rowsUrl, window.location.origin);
    u.searchParams.set("page", String(page));
    u.searchParams.set("page_size", String(ps));
    if (qs) u.searchParams.set("q", qs);
    const data = await fetchJson(u.toString());
    total = Number(data.total || 0);
    if (data.filter_scan_capped) {
      showAlert(
        "Search scanned the maximum number of rows for this snapshot; some matches may be omitted."
      );
    }
    renderRows(Array.isArray(data.rows) ? data.rows : []);
    const pages = Math.max(1, Math.ceil(total / ps));
    if (elPageLabel) elPageLabel.textContent = `Page ${page} / ${pages} · ${total} row(s)`;
    if (elPrev) elPrev.disabled = page <= 1;
    if (elNext) elNext.disabled = page >= pages;
  }

  async function init() {
    try {
      await loadMeta();
      page = 1;
      await loadRows();
    } catch (e) {
      if (elStats) elStats.textContent = "";
      showAlert(e.message || "Failed to load preview.");
    }
  }

  if (elPrev) {
    elPrev.addEventListener("click", async () => {
      if (page <= 1) return;
      page -= 1;
      try {
        await loadRows();
      } catch (e) {
        showAlert(e.message);
      }
    });
  }
  if (elNext) {
    elNext.addEventListener("click", async () => {
      page += 1;
      try {
        await loadRows();
      } catch (e) {
        showAlert(e.message);
      }
    });
  }
  if (elPageSize) {
    elPageSize.addEventListener("change", async () => {
      page = 1;
      try {
        await loadRows();
      } catch (e) {
        showAlert(e.message);
      }
    });
  }
  if (elSearch) {
    elSearch.addEventListener("input", () => {
      clearTimeout(searchDebounce);
      searchDebounce = setTimeout(async () => {
        page = 1;
        try {
          await loadRows();
        } catch (e) {
          showAlert(e.message);
        }
      }, 350);
    });
  }

  init();
}
