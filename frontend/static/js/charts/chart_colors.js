/**
 * Deterministic CTS enterprise colors: same company → same primary + secondary everywhere.
 * Expanded muted palette (navy/cyan/teal/green + indigo/slate/steel/copper/amber/violet/rosewood/lime).
 */

const companyVisualRegistry = new Map();
const categoryColorMap = new Map();

/**
 * Curated [primary, secondary] pairs — premium, low-neon, dark-mode friendly.
 * Order does not affect assignment (hash picks index); list length scales distinction for 24+ tenants.
 */
const CTS_ENTERPRISE_COMPANY_PAIRS = [
  ["#1e3a5f", "#38bdf8"],
  ["#2f5fb3", "#93c5fd"],
  ["#1e40af", "#7dd3fc"],
  ["#2563eb", "#67e8f9"],
  ["#312e81", "#a5b4fc"],
  ["#3730a3", "#818cf8"],
  ["#4338ca", "#c7d2fe"],
  ["#4c1d95", "#ddd6fe"],
  ["#5b21b6", "#c4b5fd"],
  ["#6b21a8", "#e9d5ff"],
  ["#0c4a6e", "#bae6fd"],
  ["#0369a1", "#7dd3fc"],
  ["#0e7490", "#99f6e4"],
  ["#155e75", "#67e8f9"],
  ["#164e63", "#a5f3fc"],
  ["#115e59", "#5eead4"],
  ["#0f766e", "#6ee7b7"],
  ["#134e4a", "#99f6e4"],
  ["#065f46", "#6ee7b7"],
  ["#047857", "#34d399"],
  ["#14532d", "#86efac"],
  ["#166534", "#65a30d"],
  ["#365314", "#73a314"],
  ["#57534e", "#22d3ee"],
  ["#44403c", "#38bdf8"],
  ["#334155", "#94a3b8"],
  ["#475569", "#cbd5e1"],
  ["#64748b", "#38bdf8"],
  ["#52525b", "#a78bfa"],
  ["#71717a", "#22d3ee"],
  ["#78350f", "#fcd34d"],
  ["#92400e", "#fdba74"],
  ["#9a3412", "#93c5fd"],
  ["#b45309", "#57534e"],
  ["#a16207", "#64748b"],
  ["#854d0e", "#14b8a6"],
  ["#881337", "#94a3b8"],
  ["#9d174d", "#cbd5e1"],
  ["#86198f", "#64748b"],
  ["#172554", "#38bdf8"],
  ["#1e293b", "#22d3ee"],
  ["#0f172a", "#60a5fa"],
  ["#164e63", "#818cf8"],
  ["#134e4a", "#c4b5fd"],
  ["#365314", "#22d3ee"],
  ["#3f6212", "#94a3b8"],
  ["#3f3f46", "#22d3ee"],
  ["#1d4ed8", "#86efac"],
  ["#1e3a8a", "#fcd34d"]
];

/** Optional anchor accents for known CTS ecosystem names (still scalable via pairs above). */
const COMPANY_BRAND_PAIR_OVERRIDES = new Map(
  Object.entries({
    "cts nordics": ["#2f5fb3", "#38bdf8"],
    "cts nordics ab": ["#2f5fb3", "#38bdf8"],
    "cts finland": ["#1e3a5f", "#14b8a6"],
    "cts finland oy": ["#1e3a5f", "#14b8a6"],
    bimms: ["#0f766e", "#6ee7b7"],
    "bimms ab": ["#0f766e", "#6ee7b7"],
    "dc piping": ["#475569", "#22d3ee"],
    "cts security": ["#334155", "#3b82f6"],
    gapit: ["#047857", "#0d9488"],
    "mc prefab": ["#3730a3", "#64748b"],
    fortica: ["#b45309", "#57534e"],
    "cts sweden": ["#1e40af", "#64748b"],
    "caerus nordics": ["#5b21b6", "#475569"],
    "cts eu": ["#0e7490", "#059669"],
    mecwide: ["#9a3412", "#52525b"],
    velox: ["#65a30d", "#166534"],
    "carbon transparency solutions": ["#1e4d8c", "#22d3ee"],
    cts: ["#2f5fb3", "#7dd3fc"],
    "cts ab": ["#2f5fb3", "#7dd3fc"]
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
  "#1f9d55",
  "#22c55e",
  "#4ade80",
  "#312e81",
  "#1d4ed8",
  "#475569",
  "#334155",
  "#64748b"
];

/** CTS-biased synthesis for large category sets (no rainbow). */
export function generateColors(count) {
  const total = Math.max(1, Number(count || 0));
  const colors = [];
  for (let index = 0; index < total; index += 1) {
    const t = total <= 1 ? 0.5 : index / (total - 1);
    const hue = 238 - t * 118;
    const sat = 38 + (index % 5) * 3;
    const light = 34 + (index % 6) * 2;
    colors.push(`hsl(${Math.round(hue)}, ${Math.min(52, sat)}%, ${Math.min(46, light)}%)`);
  }
  return colors;
}

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

function pairFromIndex(index) {
  const i = Math.abs(index) % CTS_ENTERPRISE_COMPANY_PAIRS.length;
  const [primary, secondary] = CTS_ENTERPRISE_COMPANY_PAIRS[i];
  return { primary, secondary };
}

/**
 * Deterministic primary + secondary for a company name (hash-stable, no encounter-order coupling).
 * @param {string} key raw company label
 * @returns {{ primary: string, secondary: string }}
 */
export function getCompanyColorPair(key) {
  const normalized = normalizeKey(key);
  if (!normalized) {
    return { primary: "#64748b", secondary: "#94a3b8" };
  }
  if (companyVisualRegistry.has(normalized)) {
    return companyVisualRegistry.get(normalized);
  }
  const branded = COMPANY_BRAND_PAIR_OVERRIDES.get(normalized);
  let pair;
  if (branded) {
    pair = { primary: branded[0], secondary: branded[1] };
  } else {
    pair = pairFromIndex(hashString(`cts-enterprise-company:${normalized}`));
  }
  companyVisualRegistry.set(normalized, pair);
  return pair;
}

/**
 * @param {string} key
 * @param {"company" | "category" | "scope"} kind
 * @returns {string} hex (primary anchor for companies)
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
    return getCompanyColorPair(normalized).primary;
  }

  if (kind === "category") {
    const categoryPalette = CATEGORY_PALETTE.length >= 48 ? CATEGORY_PALETTE : generateColors(48);
    return allocateMappedColor(categoryColorMap, normalized, categoryPalette);
  }

  const idx = hashString(`generic:${normalized}`) % CTS_ENTERPRISE_COMPANY_PAIRS.length;
  return CTS_ENTERPRISE_COMPANY_PAIRS[idx][0];
}
