/**
 * Deterministic colors for dashboards: same key → same color across all charts.
 * CTS ecosystem: deep blue, cyan, teal, and sustainability greens (no rainbow).
 */

const companyColorMap = new Map();
const categoryColorMap = new Map();

/** CTS-aligned hues only (blue → cyan → teal → green). Used when synthesizing steps. */
export function generateColors(count) {
  const total = Math.max(1, Number(count || 0));
  const colors = [];
  for (let index = 0; index < total; index += 1) {
    const t = total <= 1 ? 0.5 : index / (total - 1);
    const hue = 218 - t * 88;
    const sat = 54 + (index % 3) * 5;
    const light = 36 + (index % 4) * 3;
    colors.push(`hsl(${Math.round(hue)}, ${Math.min(62, sat)}%, ${Math.min(48, light)}%)`);
  }
  return colors;
}

/** Canonical CTS visualization hues for multi-company charts (cycled deterministically). */
const CTS_COMPANY_BASE = [
  "#1e3a5f",
  "#1e4d8c",
  "#2f5fb3",
  "#2563eb",
  "#1d4ed8",
  "#3b82f6",
  "#0369a1",
  "#0c4a6e",
  "#155e75",
  "#0f766e",
  "#115e59",
  "#14b8a6",
  "#2dd4bf",
  "#0891b2",
  "#06b6d4",
  "#0ea5e9",
  "#22d3ee",
  "#14532d",
  "#166534",
  "#15803d",
  "#1f9d55",
  "#22c55e",
  "#312e81",
  "#164e63"
];

const COMPANY_PALETTE = Array.from({ length: 96 }, (_, i) => CTS_COMPANY_BASE[i % CTS_COMPANY_BASE.length]);

/** Named CTS identities — still within the same hue family as the platform palette. */
const COMPANY_COLOR_OVERRIDES = new Map(
  Object.entries({
    "cts finland": "#1e3a5f",
    "cts finland oy": "#1e3a5f",
    "cts nordics": "#2f5fb3",
    "cts nordics ab": "#2f5fb3",
    bimms: "#0f766e",
    "bimms ab": "#0f766e",
    "carbon transparency solutions": "#1e4d8c",
    cts: "#2f5fb3",
    "cts ab": "#2f5fb3"
  })
);

const CATEGORY_PALETTE = [
  "#1e3a5f",
  "#1e40af",
  "#2563eb",
  "#2f5fb3",
  "#3b82f6",
  "#60a5fa",
  "#0369a1",
  "#0c4a6e",
  "#155e75",
  "#164e63",
  "#0e7490",
  "#0891b2",
  "#0ea5e9",
  "#06b6d4",
  "#22d3ee",
  "#67e8f9",
  "#134e4a",
  "#115e59",
  "#0f766e",
  "#14b8a6",
  "#2dd4bf",
  "#5eead4",
  "#14532d",
  "#166534",
  "#15803d",
  "#166534",
  "#1f9d55",
  "#22c55e",
  "#4ade80",
  "#312e81",
  "#1d4ed8",
  "#475569",
  "#334155",
  "#64748b"
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
      return "#0f766e";
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
        return "#0f766e";
      }
    }
    return "#64748b";
  }

  const normalized = normalizeKey(k);

  if (kind === "company") {
    if (companyColorMap.has(normalized)) {
      return companyColorMap.get(normalized);
    }
    const branded = COMPANY_COLOR_OVERRIDES.get(normalized);
    if (branded) {
      companyColorMap.set(normalized, branded);
      return branded;
    }
    return allocateMappedColor(companyColorMap, normalized, COMPANY_PALETTE);
  }

  if (kind === "category") {
    const categoryPalette = CATEGORY_PALETTE.length >= 48 ? CATEGORY_PALETTE : generateColors(48);
    return allocateMappedColor(categoryColorMap, normalized, categoryPalette);
  }

  const pal = COMPANY_PALETTE;
  return pal[hashString(`generic:${normalized}`) % pal.length];
}
