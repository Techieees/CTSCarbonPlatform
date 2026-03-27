/**
 * Build normalized analytics rows from report card payloads + DOM meta.
 * Shared by admin report and extended enterprise dashboards (frontend only).
 */

export const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const MONTH_LOOKUP = MONTHS.reduce((acc, month, index) => {
  acc[month.toLowerCase()] = index;
  return acc;
}, {});

export function readJsonScript(id) {
  const element = document.getElementById(id);
  if (!element?.textContent) {
    return null;
  }
  try {
    return JSON.parse(element.textContent);
  } catch {
    return null;
  }
}

export function parseTemplateMeta(templateName) {
  const raw = String(templateName || "").trim() || "Uncategorized";
  const scopeMatch = raw.match(/scope\s*([123])/i);
  const scope = scopeMatch ? `Scope ${scopeMatch[1]}` : "Other";
  const category = raw.replace(/^\s*scope\s*[123]\s*/i, "").trim() || raw;
  return { scope, category };
}

export function normalizeMonthLabel(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) {
    return null;
  }

  let match = value.match(/^([A-Za-z]{3})\s+(\d{4})$/);
  if (match) {
    const monthIndex = MONTH_LOOKUP[match[1].slice(0, 3).toLowerCase()];
    const year = Number(match[2]);
    if (monthIndex >= 0) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  match = value.match(/^(\d{4})-(\d{2})$/);
  if (match) {
    const year = Number(match[1]);
    const monthIndex = Number(match[2]) - 1;
    if (monthIndex >= 0 && monthIndex < 12) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) {
    const year = Number(match[1]);
    const monthIndex = Number(match[2]) - 1;
    if (monthIndex >= 0 && monthIndex < 12) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  match = value.match(/^(\d{4})-([A-Za-z]{3})$/);
  if (match) {
    const year = Number(match[1]);
    const monthIndex = MONTH_LOOKUP[match[2].slice(0, 3).toLowerCase()];
    if (monthIndex >= 0) {
      return {
        year,
        monthIndex,
        dateLabel: `${year}-${MONTHS[monthIndex]}`,
        sortKey: `${year}-${String(monthIndex + 1).padStart(2, "0")}`
      };
    }
  }

  return null;
}

export function readAnalyticsMeta(selector) {
  const elements = Array.from(document.querySelectorAll(selector));
  const deduped = new Map();

  elements.forEach((element) => {
    const index = Number(element.dataset.index || 0);
    if (!deduped.has(index)) {
      deduped.set(index, {
        index,
        company: element.dataset.company || "Company",
        template: element.dataset.template || "Category",
        period: element.dataset.period || "",
        total: Number(element.dataset.total || 0)
      });
    }
  });

  return Array.from(deduped.values()).sort((a, b) => a.index - b.index);
}

export function pickAvailablePayloadId(...ids) {
  return ids.find((id) => document.getElementById(id)) || null;
}

export function buildAnalyticsRows(payloadId, metaSelector) {
  const chartData = readJsonScript(payloadId);
  const meta = readAnalyticsMeta(metaSelector);
  if (!Array.isArray(chartData) || !meta.length) {
    return [];
  }

  return meta
    .flatMap((item, index) => {
      const chartItem = chartData[index] || {};
      const labels = Array.isArray(chartItem.labels) ? chartItem.labels : [];
      const values = Array.isArray(chartItem.values) ? chartItem.values : [];
      const { scope, category } = parseTemplateMeta(item.template);

      if (!labels.length || !values.length) {
        const normalized = normalizeMonthLabel(item.period);
        if (!normalized) {
          return [];
        }
        return [
          {
            company: item.company,
            template: item.template,
            scope,
            category,
            emissions: Number(item.total || 0),
            ...normalized
          }
        ];
      }

      return labels
        .map((label, pointIndex) => {
          const normalized = normalizeMonthLabel(label);
          if (!normalized) {
            return null;
          }
          return {
            company: item.company,
            template: item.template,
            scope,
            category,
            emissions: Number(values[pointIndex] || 0),
            ...normalized
          };
        })
        .filter(Boolean);
    })
    .sort(
      (a, b) =>
        String(a.sortKey).localeCompare(String(b.sortKey)) ||
        String(a.company).localeCompare(String(b.company)) ||
        String(a.category).localeCompare(String(b.category))
    );
}
