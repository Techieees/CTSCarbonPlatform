import { formatFull, initChart } from "./echarts_theme.js";

const FLAGS_URL = "/static/data/country_flags.json";
const COORDS_URL = "/static/data/country_coordinates.json";

function readPayload() {
  const el = document.getElementById("emissions-map-payload");
  if (!el) return [];
  try {
    return JSON.parse(el.textContent || "[]");
  } catch (_err) {
    return [];
  }
}

function flagFromIso(code, flagsMap) {
  const key = String(code || "").trim().toUpperCase();
  if (!key) return "";
  if (flagsMap && typeof flagsMap[key] === "string") return flagsMap[key];
  if (!/^[A-Z]{2}$/.test(key)) return "";
  return String.fromCodePoint(...Array.from(key).map((char) => 127397 + char.charCodeAt(0)));
}

function sizeForEmission(value, maxEmission) {
  const safe = Math.max(0, Number(value || 0));
  const max = Math.max(1, Number(maxEmission || 1));
  return 10 + Math.sqrt(safe / max) * 34;
}

async function loadMapConfig() {
  const [flagsRes, coordsRes] = await Promise.all([
    fetch(FLAGS_URL, { headers: { Accept: "application/json" } }),
    fetch(COORDS_URL, { headers: { Accept: "application/json" } })
  ]);
  const flags = flagsRes.ok ? await flagsRes.json().catch(() => ({})) : {};
  const coords = coordsRes.ok ? await coordsRes.json().catch(() => ({})) : {};
  return { flags, coords };
}

function buildSeriesData(rows, flagsMap, coordsMap) {
  const valid = [];
  rows.forEach((row) => {
    const country = String(row.country_name || "");
    const coord = coordsMap[country];
    if (!coord) return;
    valid.push({
      name: String(row.company_name || country),
      value: [Number(coord.lon || 0), Number(coord.lat || 0), Number(row.emissions || 0)],
      country: country,
      countryCode: String(row.country_code || ""),
      sharePct: Number(row.share_pct || 0),
      flag: flagFromIso(row.country_code, flagsMap)
    });
  });
  return valid;
}

function renderMap(rows, flagsMap, coordsMap) {
  const chart = initChart("#emissionsMapChart");
  if (!chart) return;

  const data = buildSeriesData(rows, flagsMap, coordsMap);
  const maxEmission = Math.max(1, ...data.map((item) => Number(item.value?.[2] || 0)));
  const muted = getComputedStyle(document.body).getPropertyValue("--muted").trim() || "#64748b";

  chart.setOption({
    backgroundColor: "transparent",
    tooltip: {
      trigger: "item",
      backgroundColor: "rgba(15,23,42,0.94)",
      borderWidth: 0,
      textStyle: { color: "#f8fafc", fontSize: 12, fontWeight: 500 },
      padding: [12, 14],
      extraCssText: "border-radius:14px;box-shadow:0 18px 42px rgba(2,6,23,.28);backdrop-filter:blur(14px);",
      formatter: (params) => {
        const d = params.data || {};
        return `
          <div>
            <div style="font-size:13px;font-weight:700;margin-bottom:6px;">${d.name || "Company"}</div>
            <div>${d.country || ""} ${d.flag || ""}</div>
            <div style="margin-top:6px;"><strong>${formatFull(d.value?.[2] || 0)}</strong> t CO2e</div>
            <div>Share ${formatFull(d.sharePct || 0)}%</div>
          </div>
        `;
      }
    },
    geo: {
      map: "world",
      roam: true,
      zoom: 1.12,
      itemStyle: {
        areaColor: "rgba(148,163,184,0.10)",
        borderColor: "rgba(148,163,184,0.28)"
      },
      emphasis: {
        itemStyle: {
          areaColor: "rgba(47,95,179,0.18)"
        },
        label: { show: false }
      }
    },
    series: [
      {
        name: "Company emissions",
        type: "scatter",
        coordinateSystem: "geo",
        data,
        symbolSize: (value) => sizeForEmission(value[2], maxEmission),
        itemStyle: {
          color: "#2f5fb3",
          borderColor: "#ffffff",
          borderWidth: 1.5,
          shadowBlur: 20,
          shadowColor: "rgba(47,95,179,0.25)"
        },
        emphasis: {
          scale: true,
          itemStyle: {
            color: "#1f9d55"
          }
        }
      }
    ],
    graphic: data.length
      ? []
      : [
          {
            type: "text",
            left: "center",
            top: "middle",
            style: {
              text: "No country coordinates available for the latest output.",
              fill: muted,
              fontSize: 14,
              fontWeight: 500
            }
          }
        ]
  });
}

async function init() {
  const rows = readPayload();
  if (!document.getElementById("emissionsMapChart") || typeof window.echarts === "undefined") return;
  try {
    const { flags, coords } = await loadMapConfig();
    renderMap(rows, flags, coords);
    window.addEventListener("themechange", () => renderMap(rows, flags, coords));
  } catch (_err) {
    renderMap(rows, {}, {});
  }
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init, { once: true });
} else {
  init();
}
