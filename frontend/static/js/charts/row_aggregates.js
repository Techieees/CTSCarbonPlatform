/**
 * Client-side aggregations for analytics rows (tCO₂e points).
 */

export function rowReportingSortKey(r) {
  const sk = String(r?.sortKey || "").trim();
  if (sk) {
    return sk;
  }
  return String(r?.dateLabel || "").trim();
}

export function sortedReportingMonthSlots(rows) {
  const m = new Map();
  rows.forEach((r) => {
    const sk = rowReportingSortKey(r);
    if (!sk) {
      return;
    }
    const dl = String(r.dateLabel || sk).trim();
    if (!m.has(sk)) {
      m.set(sk, dl);
    }
  });
  const keys = Array.from(m.keys()).sort((a, b) => String(a).localeCompare(String(b)));
  return keys.map((sortKey) => ({ sortKey, dateLabel: m.get(sortKey) }));
}

export function sumBy(rows, keyFn) {
  const map = new Map();
  rows.forEach((row) => {
    const k = keyFn(row);
    if (k == null || k === "") {
      return;
    }
    map.set(k, (map.get(k) || 0) + Number(row.emissions || 0));
  });
  return map;
}

export function sortedEntries(map, desc = true) {
  return Array.from(map.entries()).sort((a, b) => (desc ? b[1] - a[1] : a[1] - b[1]));
}

/** Chronological monthly totals by reporting period (sortKey). */
export function monthlyTotals(rows) {
  const slotValues = new Map();
  rows.forEach((r) => {
    const sk = rowReportingSortKey(r);
    if (!sk) {
      return;
    }
    const dl = String(r.dateLabel || sk).trim();
    const prev = slotValues.get(sk) || { sortKey: sk, dateLabel: dl, value: 0 };
    prev.value += Number(r.emissions || 0);
    prev.dateLabel = dl || prev.dateLabel;
    slotValues.set(sk, prev);
  });
  return Array.from(slotValues.values()).sort((a, b) => a.sortKey.localeCompare(b.sortKey));
}

export function scopeTotals(rows) {
  const labels = ["Scope 1 Direct Emissions", "Scope 2 Indirect Emissions", "Scope 3 Value Chain Emissions", "Other"];
  const m = sumBy(rows, (r) => {
    const s = String(r.scope || "");
    if (s.includes("1")) {
      return labels[0];
    }
    if (s.includes("2")) {
      return labels[1];
    }
    if (s.includes("3")) {
      return labels[2];
    }
    return labels[3];
  });
  return labels.map((name) => ({ name, value: m.get(name) || 0 }));
}

export function companyTotals(rows) {
  return sortedEntries(sumBy(rows, (r) => r.company), true).slice(0, 16);
}

export function categoryTotals(rows) {
  return sortedEntries(sumBy(rows, (r) => r.category || r.template), true).slice(0, 16);
}

function rowScopeKey(r) {
  const s = String(r.scope || "");
  if (s.includes("1")) {
    return "Scope 1";
  }
  if (s.includes("2")) {
    return "Scope 2";
  }
  if (s.includes("3")) {
    return "Scope 3";
  }
  return "Other";
}

/** For stacked bar: months x scope stacks (ordered by reporting sortKey). */
export function monthlyScopeStackSeries(rows) {
  const slots = sortedReportingMonthSlots(rows);
  const scopeKeys = ["Scope 1", "Scope 2", "Scope 3", "Other"];
  const series = scopeKeys.map((sk) => ({
    name: sk,
    type: "bar",
    stack: "emissions",
    emphasis: { focus: "series" },
    data: slots.map(({ sortKey }) =>
      rows
        .filter((r) => rowReportingSortKey(r) === sortKey && rowScopeKey(r) === sk)
        .reduce((acc, r) => acc + Number(r.emissions || 0), 0)
    )
  }));
  return { labels: slots.map((s) => s.dateLabel), series };
}

/** Top N categories per month → stacked series (month on x) */
export function monthlyTopCategoryStack(rows, topN = 6) {
  const slots = sortedReportingMonthSlots(rows);
  const catTotals = categoryTotals(rows);
  const topCats = catTotals.slice(0, topN).map(([c]) => c);
  const series = topCats.map((cat) => ({
    name: cat,
    type: "bar",
    stack: "total",
    emphasis: { focus: "series" },
    data: slots.map(({ sortKey }) => {
      const v = rows
        .filter((r) => rowReportingSortKey(r) === sortKey && (r.category || r.template) === cat)
        .reduce((s, r) => s + Number(r.emissions || 0), 0);
      return v;
    })
  }));
  series.push({
    name: "Other categories",
    type: "bar",
    stack: "total",
    emphasis: { focus: "series" },
    data: slots.map(({ sortKey }) =>
      rows
        .filter((r) => rowReportingSortKey(r) === sortKey && !topCats.includes(r.category || r.template))
        .reduce((s, r) => s + Number(r.emissions || 0), 0)
    )
  });
  return { labels: slots.map((s) => s.dateLabel), series };
}

/** Heatmap: scope x category (aggregated) */
export function scopeCategoryHeatmap(rows) {
  const scopes = ["Scope 1", "Scope 2", "Scope 3", "Other"];
  const cats = Array.from(new Set(rows.map((r) => r.category || r.template))).sort();
  const map = new Map();
  rows.forEach((r) => {
    const sc = rowScopeKey(r);
    const cat = r.category || r.template;
    const key = `${sc}__${cat}`;
    map.set(key, (map.get(key) || 0) + Number(r.emissions || 0));
  });
  const data = [];
  let maxV = 0;
  scopes.forEach((s, i) => {
    cats.forEach((c, j) => {
      const v = Number(map.get(`${s}__${c}`) || 0);
      maxV = Math.max(maxV, v);
      data.push([j, i, v]);
    });
  });
  return { scopes, categories: cats, data, maxValue: maxV || 1 };
}
