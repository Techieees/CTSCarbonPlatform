/**
 * Deterministic colors for dashboards: same key → same color across all charts.
 */

const companyColorMap = new Map();
const categoryColorMap = new Map();

export function generateColors(count) {
  const total = Math.max(1, Number(count || 0));
  const colors = [];
  for (let index = 0; index < total; index += 1) {
    const hue = Math.round((360 / total) * index);
    colors.push(`hsl(${hue}, 65%, 55%)`);
  }
  return colors;
}

const COMPANY_PALETTE = generateColors(96);

const CATEGORY_PALETTE = [
  "#3b82f6",
  "#22c55e",
  "#f97316",
  "#a855f7",
  "#ec4899",
  "#14b8a6",
  "#eab308",
  "#6366f1",
  "#ef4444",
  "#0ea5e9",
  "#d946ef",
  "#84cc16",
  "#f59e0b",
  "#06b6d4",
  "#e11d48",
  "#64748b",
  "#475569",
  "#334155"
];

function hashString(s) {
  let h = 2166136261;
  const str = String(s);
  for (let i = 0; i < str.length; i += 1) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function normalizeKey(value) {
  return String(value || "").trim().toLowerCase();
}

function allocateMappedColor(registry, key, palette) {
  if (registry.has(key)) {
    return registry.get(key);
  }
  const color = palette[registry.size % palette.length];
  registry.set(key, color);
  return color;
}

/**
 * @param {string} key
 * @param {"company" | "category" | "scope"} kind
 * @returns {string} hex color
 */
export function getColorByKey(key, kind = "company") {
  const k = String(key || "").trim();
  if (!k) {
    return "#64748b";
  }

  if (kind === "scope") {
    if (/scope\s*1/i.test(k) || /direct\s+emissions/i.test(k)) {
      return "#2563eb";
    }
    if (/scope\s*2/i.test(k) || /indirect\s+emissions/i.test(k)) {
      return "#16a34a";
    }
    if (/scope\s*3/i.test(k) || /value\s+chain/i.test(k)) {
      return "#ea580c";
    }
    const m = k.match(/scope\s*([123])/i);
    if (m) {
      if (m[1] === "1") {
        return "#2563eb";
      }
      if (m[1] === "2") {
        return "#16a34a";
      }
      if (m[1] === "3") {
        return "#ea580c";
      }
    }
    return "#64748b";
  }

  const normalized = normalizeKey(k);

  if (kind === "company") {
    return allocateMappedColor(companyColorMap, normalized, COMPANY_PALETTE);
  }

  if (kind === "category") {
    const categoryPalette = CATEGORY_PALETTE.length >= 48 ? CATEGORY_PALETTE : generateColors(48);
    return allocateMappedColor(categoryColorMap, normalized, categoryPalette);
  }

  const pal = COMPANY_PALETTE;
  return pal[hashString(`generic:${normalized}`) % pal.length];
}
