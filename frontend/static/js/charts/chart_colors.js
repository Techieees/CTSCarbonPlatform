/**
 * Deterministic colors for dashboards: same key → same color across all charts.
 */

const COMPANY_PALETTE = [
  "#2563eb",
  "#16a34a",
  "#ea580c",
  "#8b5cf6",
  "#ec4899",
  "#14b8a6",
  "#f59e0b",
  "#6366f1",
  "#ef4444",
  "#84cc16",
  "#06b6d4",
  "#d946ef",
  "#f97316",
  "#0ea5e9",
  "#22c55e",
  "#a855f7",
  "#e11d48",
  "#0891b2"
];

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

  const pal = kind === "category" ? CATEGORY_PALETTE : COMPANY_PALETTE;
  const salt = kind === "category" ? "cat:" : "co:";
  return pal[hashString(salt + k.toLowerCase()) % pal.length];
}
