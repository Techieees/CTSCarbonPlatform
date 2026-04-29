if (!window.MAPBOX_TOKEN) {
  console.error("Mapbox token missing");
}
mapboxgl.accessToken = window.MAPBOX_TOKEN;

(function () {
  const root = document.getElementById("locationsMap");
  const notice = document.getElementById("locationsMapNotice");
  const config = window.__locationsMapConfig || {};
  if (!root) return;
  const WATER_RISK_SOURCE_ID = "water-risk-source";
  const WATER_RISK_LAYER_ID = "water-risk-layer";
  const WATER_RISK_TOGGLE_ID = "water-risk-toggle";
  const WATER_RISK_LEGEND_ID = "water-risk-legend";
  const WATER_RISK_QUERY_BASE = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/aqueduct_water_risk/FeatureServer/0/query?";

  const categoryMeta = {
    HQ: { label: "Headquarters", color: "#0b3b8f", radius: 9, layerId: "hq-layer" },
    Office: { label: "Corporate Offices", color: "#1f77ff", radius: 8, layerId: "office-layer" },
    Production: { label: "Production Sites", color: "#18a957", radius: 8, layerId: "production-layer" }
  };

  function getOfficeImage(name) {
    const city = String(name || "").trim();
    const officeImageMap = {
      Copenhagen: "/static/images/Offices/Copenhagen.jpg",
      Dublin: "/static/images/Offices/Dublin.jpg",
      Frankfurt: "/static/images/Offices/Frankfurt.jpg",
      Helsinki: "/static/images/Offices/Helsinki.png",
      Lisbon: "/static/images/Offices/Lisbon.png",
      London: "/static/images/Offices/London.png",
      Madrid: "/static/images/Offices/Madrid.jpg",
      Milan: "/static/images/Offices/Milan.png",
      Nyon: "/static/images/Offices/Nyon.jpeg",
      Oslo: "/static/images/Offices/Oslo.jpg",
      Paris: "/static/images/Offices/Paris.jpg",
      Porto: "/static/images/Offices/Porto.jpg",
      Stockholm: "/static/images/Offices/Stockholm.png",
      Vienna: "/static/images/Offices/Vienna.png"
    };
    return officeImageMap[city] || null;
  }

  function getProjectImage(country, name) {
    const cleanName = String(name || "").trim();

    const imageMap = {
      "Norway-Kvandal": "/static/images/Projects/Norway - Kvandal-main.webp",
      "Norway-Pegasus": "/static/images/Projects/Norway- Pegasus-01.webp",
      "Norway-Heli": "/static/images/Projects/Norway-Heli-01.webp",
      "Norway-Rock": "/static/images/Projects/Norway-Rock-01_L.webp",
      "Norway-Beluga": "/static/images/Projects/Norway- Beluga-02.webp",
      "Norway-Oyster": "/static/images/Projects/Norway-Oyster-01.webp",
      "Norway-Colo": "/static/images/Projects/Norway-Colo-01.webp",
      "Sweden-SIF": "/static/images/Projects/Sweden- SIF-01.webp",
      "Finland-Espoo": "/static/images/Projects/Finland-Espoo-01_L.webp"
    };

    return imageMap[`${country}-${cleanName}`] || null;
  }

  function setNotice(message) {
    if (!notice) return;
    notice.textContent = message;
    notice.hidden = false;
  }

  async function fetchLocations() {
    const response = await fetch(String(config.dataUrl || ""), {
      headers: { Accept: "application/json" }
    });
    if (!response.ok) {
      throw new Error("Location data could not be loaded.");
    }
    return response.json();
  }

  function getSearchZoom(result) {
    const placeTypes = Array.isArray(result && result.place_type) ? result.place_type : [];
    if (placeTypes.indexOf("country") !== -1) return 4;
    if (placeTypes.indexOf("region") !== -1) return 6;
    if (placeTypes.indexOf("place") !== -1 || placeTypes.indexOf("locality") !== -1) return 10;
    return 8;
  }

  function addLocationSearchControl(mapInstance) {
    if (!window.MapboxGeocoder || !mapInstance) {
      return;
    }

    const geocoder = new MapboxGeocoder({
      accessToken: mapboxgl.accessToken,
      mapboxgl: mapboxgl,
      marker: false,
      placeholder: "Search city or country",
      types: "country,region,place,locality,district",
      language: "en",
      flyTo: false
    });

    geocoder.on("result", function (event) {
      const result = event && event.result;
      if (!result) {
        return;
      }

      const zoom = getSearchZoom(result);
      if (Array.isArray(result.bbox) && result.bbox.length === 4) {
        mapInstance.fitBounds(
          [
            [result.bbox[0], result.bbox[1]],
            [result.bbox[2], result.bbox[3]]
          ],
          {
            padding: 80,
            maxZoom: zoom,
            duration: 1200,
            essential: true
          }
        );
        return;
      }

      if (Array.isArray(result.center) && result.center.length >= 2) {
        mapInstance.flyTo({
          center: result.center,
          zoom: zoom,
          duration: 1200,
          essential: true
        });
      }
    });

    mapInstance.addControl(geocoder, "top-left");
  }

  function popupHtml(item) {
    const officeImage = item.type === "Office" || item.type === "HQ" ? getOfficeImage(item.name) : null;
    const projectImage = item.type === "Production" ? getProjectImage(item.country, item.name) : null;
    const imagePath = officeImage || projectImage || "/static/images/placeholder.webp";
    return [
      '<div class="location-popup">',
      imagePath ? '<img src="' + imagePath + '" class="popup-image" alt="' + item.name + '" onerror="this.onerror=null;this.src=\'/static/images/placeholder.webp\';">' : "",
      '<div class="popup-title">' + item.name + "</div>",
      '<div class="popup-subtitle">' + item.country + "</div>",
      "</div>"
    ].join("");
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function titleCase(value) {
    return String(value || "")
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/\b\w/g, function (char) {
        return char.toUpperCase();
      });
  }

  function numericValue(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function addWaterRiskLayer(mapInstance) {
    if (!mapInstance || typeof mapInstance.addSource !== "function") {
      return Promise.resolve(null);
    }

    if (!mapInstance.__waterRiskLayerState) {
      mapInstance.__waterRiskLayerState = {
        initialized: false,
        loading: null,
        visible: true,
        popup: new mapboxgl.Popup({
          closeButton: false,
          closeOnClick: false,
          offset: 12,
          maxWidth: "320px"
        }),
        riskField: "bau30_ws_x_r",
        riskLabelField: "bau30_ws_x_l",
        minRisk: 0,
        maxRisk: 1,
        boundInteractions: false,
        maxRecordCount: 750
      };
    }

    const state = mapInstance.__waterRiskLayerState;
    const layerPanel = document.getElementById("locationsLayerPanel");

    function firstDefined(properties, keys) {
      for (let i = 0; i < keys.length; i += 1) {
        const value = properties && properties[keys[i]];
        if (value != null && String(value).trim() !== "") {
          return String(value);
        }
      }
      return "";
    }

    function setLayerVisibility(visible) {
      state.visible = Boolean(visible);
      if (mapInstance.getLayer(WATER_RISK_LAYER_ID)) {
        mapInstance.setLayoutProperty(
          WATER_RISK_LAYER_ID,
          "visibility",
          state.visible ? "visible" : "none"
        );
      }
      const toggle = document.getElementById(WATER_RISK_TOGGLE_ID);
      if (toggle) {
        toggle.classList.toggle("is-active", state.visible);
        toggle.setAttribute("aria-pressed", state.visible ? "true" : "false");
        const stateNode = toggle.querySelector(".locations-layer-toggle__state");
        if (stateNode) {
          stateNode.textContent = state.visible ? "Visible on map" : "Hidden from map";
        }
      }
      const legend = document.getElementById(WATER_RISK_LEGEND_ID);
      if (legend) {
        legend.hidden = !state.visible;
      }
      if (!state.visible) {
        state.popup.remove();
        mapInstance.getCanvas().style.cursor = "";
      }
    }

    function ensureUi() {
      if (!layerPanel) {
        return;
      }
      if (!layerPanel.dataset.initialized) {
        layerPanel.dataset.initialized = "true";
        layerPanel.innerHTML = [
          '<div class="locations-layer-panel__title">Layers</div>',
          '<div class="locations-layer-panel__text">Toggle overlays without reloading the map. More layers can be added here later.</div>',
          '<button type="button" id="' + WATER_RISK_TOGGLE_ID + '" class="locations-layer-toggle is-active" aria-pressed="true">',
          '<span>Water Risk Layer</span>',
          '<span class="locations-layer-toggle__state">Visible on map</span>',
          "</button>",
          '<div id="' + WATER_RISK_LEGEND_ID + '" class="locations-map__overlay-legend">',
          '<div class="locations-map__overlay-legend-title">Baseline Annual</div>',
          '<div class="locations-map__overlay-legend-list">',
          '<div class="locations-map__overlay-legend-item"><span class="locations-map__overlay-legend-swatch is-extremely-high"></span><span>Extremely High (&gt;80%)</span></div>',
          '<div class="locations-map__overlay-legend-item"><span class="locations-map__overlay-legend-swatch is-high"></span><span>High (40-80%)</span></div>',
          '<div class="locations-map__overlay-legend-item"><span class="locations-map__overlay-legend-swatch is-medium-high"></span><span>Medium - High (20-40%)</span></div>',
          '<div class="locations-map__overlay-legend-item"><span class="locations-map__overlay-legend-swatch is-low-medium"></span><span>Low - Medium (10-20%)</span></div>',
          '<div class="locations-map__overlay-legend-item"><span class="locations-map__overlay-legend-swatch is-low"></span><span>Low (&lt;10%)</span></div>',
          '<div class="locations-map__overlay-legend-item"><span class="locations-map__overlay-legend-swatch is-arid"></span><span>Arid and Low Water Use</span></div>',
          '<div class="locations-map__overlay-legend-item"><span class="locations-map__overlay-legend-swatch is-no-data"></span><span>No Data</span></div>',
          "</div>",
          '<div class="locations-layer-panel__text" style="margin-top:8px">Styled to match the ArcGIS source symbology more closely.</div>',
          "</div>",
          '<div id="waterRiskLayerStatus" class="locations-layer-panel__text">Loading global water risk data...</div>'
        ].join("");

        const toggle = document.getElementById(WATER_RISK_TOGGLE_ID);
        if (toggle) {
          toggle.addEventListener("click", function () {
            setLayerVisibility(!state.visible);
          });
        }
      }
      setLayerVisibility(state.visible);
    }

    function setStatus(message, isError) {
      if (!layerPanel) {
        return;
      }
      const statusNode = document.getElementById("waterRiskLayerStatus");
      if (!statusNode) {
        return;
      }
      statusNode.textContent = message;
      statusNode.classList.toggle("is-error", Boolean(isError));
    }

    function chunkArray(values, size) {
      const chunks = [];
      for (let index = 0; index < values.length; index += size) {
        chunks.push(values.slice(index, index + size));
      }
      return chunks;
    }

    function detectGeometryType(featureCollection) {
      const feature = Array.isArray(featureCollection && featureCollection.features)
        ? featureCollection.features.find(function (item) {
            return item && item.geometry && item.geometry.type;
          })
        : null;
      return feature && feature.geometry ? String(feature.geometry.type || "") : "";
    }

    function detectRiskField(features) {
      const preferredFields = [
        "bau30_ws_x_r",
        "bau50_ws_x_r",
        "bau80_ws_x_r"
      ];
      for (let i = 0; i < preferredFields.length; i += 1) {
        const field = preferredFields[i];
        const found = features.some(function (feature) {
          return numericValue(feature && feature.properties ? feature.properties[field] : null) !== null;
        });
        if (found) {
          return field;
        }
      }
      return "bau30_ws_x_r";
    }

    function detectRiskLabelField(fieldName) {
      return fieldName && /_r$/.test(fieldName) ? fieldName.replace(/_r$/, "_l") : "";
    }

    function riskRange(features, field) {
      const values = features
        .map(function (feature) {
          return numericValue(feature && feature.properties ? feature.properties[field] : null);
        })
        .filter(function (value) {
          return value !== null;
        });

      if (!values.length) {
        return { min: 0, max: 1 };
      }

      const min = Math.min.apply(null, values);
      const max = Math.max.apply(null, values);
      return {
        min: Number.isFinite(min) ? min : 0,
        max: Number.isFinite(max) && max > min ? max : min + 1
      };
    }

    function colorExpression(field, labelField) {
      const safeLabelField = labelField || "bau30_ws_x_l";
      const safeField = field || "bau30_ws_x_r";
      return [
        "case",
        ["==", ["coalesce", ["get", safeLabelField], ""], "Extremely High (>80%)"], "#f07c3e",
        ["==", ["coalesce", ["get", safeLabelField], ""], "High (40-80%)"], "#7366c6",
        ["==", ["coalesce", ["get", safeLabelField], ""], "Medium - High (20-40%)"], "#4f8fd6",
        ["==", ["coalesce", ["get", safeLabelField], ""], "Low - Medium (10-20%)"], "#77d0ea",
        ["==", ["coalesce", ["get", safeLabelField], ""], "Low (<10%)"], "#d8f2f6",
        ["==", ["coalesce", ["get", safeLabelField], ""], "Arid and Low Water Use"], "#ead9c8",
        ["==", ["coalesce", ["get", safeLabelField], ""], "No Data"], "#d7d7d7",
        ["<", ["to-number", ["coalesce", ["get", safeField], 0]], 0.1], "#d8f2f6",
        ["<", ["to-number", ["coalesce", ["get", safeField], 0]], 0.2], "#77d0ea",
        ["<", ["to-number", ["coalesce", ["get", safeField], 0]], 0.4], "#4f8fd6",
        ["<", ["to-number", ["coalesce", ["get", safeField], 0]], 0.8], "#7366c6",
        "#f07c3e"
      ];
    }

    function layerDefinition(geometryType, field, minRisk, maxRisk) {
      const color = colorExpression(field, state.riskLabelField);

      if (geometryType === "Point" || geometryType === "MultiPoint") {
        return {
          id: WATER_RISK_LAYER_ID,
          type: "circle",
          source: WATER_RISK_SOURCE_ID,
          layout: {
            visibility: state.visible ? "visible" : "none"
          },
          paint: {
            "circle-radius": 5.5,
            "circle-color": color,
            "circle-opacity": 0.6,
            "circle-stroke-color": "#ffffff",
            "circle-stroke-width": 1
          }
        };
      }

      if (geometryType === "LineString" || geometryType === "MultiLineString") {
        return {
          id: WATER_RISK_LAYER_ID,
          type: "line",
          source: WATER_RISK_SOURCE_ID,
          layout: {
            visibility: state.visible ? "visible" : "none"
          },
          paint: {
            "line-color": color,
            "line-opacity": 0.58,
            "line-width": 1.8
          }
        };
      }

      return {
        id: WATER_RISK_LAYER_ID,
        type: "fill",
        source: WATER_RISK_SOURCE_ID,
        layout: {
          visibility: state.visible ? "visible" : "none"
        },
        paint: {
          "fill-color": color,
          "fill-opacity": 0.52,
          "fill-outline-color": "rgba(61, 92, 122, 0.25)"
        }
      };
    }

    function overlayPopupHtml(properties) {
      const title = firstDefined(properties, ["name", "Name", "NAME", "pfaf_id"]) || "Water Risk Area";
      const riskValue = properties && state.riskField ? properties[state.riskField] : "";
      const riskBand = properties && state.riskLabelField ? properties[state.riskLabelField] : "";
      const rows = [];

      if (riskBand) {
        rows.push(["Risk band", String(riskBand)]);
      }
      if (riskValue != null && String(riskValue).trim() !== "") {
        rows.push(["Risk score", String(Math.round(Number(riskValue) * 100) / 100)]);
      }
      if (properties && properties.pfaf_id != null) {
        rows.push(["Basin ID", String(properties.pfaf_id)]);
      }

      return [
        '<div class="locations-map__overlay-popup">',
        '<div class="locations-map__overlay-popup-title">' + escapeHtml(title) + "</div>",
        rows.map(function (row) {
          return '<div class="locations-map__overlay-popup-row"><span>' + escapeHtml(row[0]) + '</span><strong>' + escapeHtml(row[1]) + "</strong></div>";
        }).join(""),
        "</div>"
      ].join("");
    }

    function bindOverlayInteractions() {
      if (state.boundInteractions) {
        return;
      }
      state.boundInteractions = true;

      mapInstance.on("mousemove", WATER_RISK_LAYER_ID, function (event) {
        if (!state.visible) {
          return;
        }
        const feature = event.features && event.features[0];
        if (!feature) {
          return;
        }
        mapInstance.getCanvas().style.cursor = "pointer";
        state.popup
          .setLngLat(event.lngLat)
          .setHTML(overlayPopupHtml(feature.properties || {}))
          .addTo(mapInstance);
      });

      mapInstance.on("mouseleave", WATER_RISK_LAYER_ID, function () {
        mapInstance.getCanvas().style.cursor = "";
        state.popup.remove();
      });
    }

    function fetchJson(url) {
      return fetch(url, {
        headers: {
          Accept: "application/geo+json, application/json"
        }
      }).then(function (response) {
        if (!response.ok) {
          throw new Error("Water risk layer could not be loaded.");
        }
        return response.json();
      });
    }

    function fetchAllFeatures() {
      const fields = [
        "OBJECTID",
        "pfaf_id",
        "bau30_ws_x_r",
        "bau30_ws_x_l",
        "bau50_ws_x_r",
        "bau50_ws_x_l",
        "bau80_ws_x_r",
        "bau80_ws_x_l"
      ].join(",");
      const countUrl = WATER_RISK_QUERY_BASE + "where=1%3D1&returnCountOnly=true&f=json";

      return fetchJson(countUrl).then(function (countPayload) {
        const totalCount = Number(countPayload && countPayload.count || 0);
        const allFeatures = [];
        let offset = 0;

        function fetchNextBatch() {
          const queryUrl = WATER_RISK_QUERY_BASE +
            "where=1%3D1" +
            "&outFields=" + encodeURIComponent(fields) +
            "&returnGeometry=true" +
            "&outSR=4326" +
            "&resultOffset=" + String(offset) +
            "&resultRecordCount=" + String(state.maxRecordCount) +
            "&f=geojson";

          return fetchJson(queryUrl).then(function (payload) {
            const features = Array.isArray(payload && payload.features) ? payload.features : [];
            Array.prototype.push.apply(allFeatures, features);
            offset += features.length;
            if (totalCount > 0) {
              setStatus("Loading global water risk data... " + Math.min(offset, totalCount) + " / " + totalCount, false);
            }
            if (features.length === 0 || offset >= totalCount) {
              return {
                type: "FeatureCollection",
                features: allFeatures
              };
            }
            return fetchNextBatch();
          });
        }

        return fetchNextBatch();
      });
    }

    if (state.initialized) {
      ensureUi();
      return Promise.resolve(state);
    }

    if (state.loading) {
      return state.loading;
    }

    state.loading = fetchAllFeatures()
      .then(function (geojson) {
        const features = Array.isArray(geojson && geojson.features) ? geojson.features : [];
        if (!features.length) {
          throw new Error("Water risk layer returned no features.");
        }

        const geometryType = detectGeometryType(geojson);
        state.riskField = detectRiskField(features);
        state.riskLabelField = detectRiskLabelField(state.riskField);
        const range = riskRange(features, state.riskField);
        state.minRisk = range.min;
        state.maxRisk = range.max;

        if (!mapInstance.getSource(WATER_RISK_SOURCE_ID)) {
          mapInstance.addSource(WATER_RISK_SOURCE_ID, {
            type: "geojson",
            data: geojson
          });
        }

        if (!mapInstance.getLayer(WATER_RISK_LAYER_ID)) {
          mapInstance.addLayer(
            layerDefinition(
              geometryType,
              state.riskField,
              state.minRisk,
              state.maxRisk
            )
          );
        }

        bindOverlayInteractions();
        ensureUi();
        setStatus("Global water risk layer loaded.", false);
        state.initialized = true;
        return state;
      })
      .catch(function (error) {
        console.warn(error && error.message ? error.message : "Water risk layer failed to load.");
        ensureUi();
        setStatus("Water risk layer could not be loaded right now.", true);
        return null;
      });

    return state.loading;
  }

  const CLIMATE_LAYER_PREFIX = "climate";
  const CLIMATE_PANEL_ID = "climateLayerPanelSection";
  const CLIMATE_STATUS_ID = "climateLayerStatus";
  const CLIMATE_VISIBILITY_TOGGLE_ID = "climateVisibilityToggle";
  const CLIMATE_YEAR_SELECT_ID = "climateYearRangeSelect";
  const CLIMATE_MONTH_OVERLAY_ID = "climateMonthOverlay";
  const CLIMATE_MONTH_SLIDER_ID = "climateMonthSlider";
  const CLIMATE_MONTH_VALUE_ID = "climateMonthValue";
  const CLIMATE_MONTH_HELPER_ID = "climateMonthHelper";
  const CLIMATE_SCENARIO_FIELD_ID = "climateScenarioField";
  const CLIMATE_MODEL_FIELD_ID = "climateModelField";
  const CLIMATE_SCENARIO_SELECT_ID = "climateScenarioSelect";
  const CLIMATE_MODEL_SELECT_ID = "climateModelSelect";
  const CLIMATE_LEGEND_ID = "climateLegend";
  const CLIMATE_PANEL_OVERLAY_ID = "climateDataPanel";
  const CLIMATE_PANEL_CLOSE_ID = "climateDataPanelClose";
  const CLIMATE_PANEL_COORDS_ID = "climateDataPanelCoords";
  const CLIMATE_PANEL_META_ID = "climateDataPanelMeta";
  const CLIMATE_TEMP_CHART_ID = "climateTempChart";
  const CLIMATE_PRECIP_CHART_ID = "climatePrecipChart";
  const CLIMATE_CHART_JS_URL = "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js";
  const CLIMATE_YEAR_RANGES = ["1970-2000", "2021-2040", "2041-2060", "2061-2080", "2081-2100"];
  const CLIMATE_MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const CLIMATE_SCENARIOS = [
    { value: "SSP1-2.6", label: "Strong climate action" },
    { value: "SSP2-4.5", label: "Moderate" },
    { value: "SSP3-7.0", label: "Limited" },
    { value: "SSP5-8.5", label: "High emissions" }
  ];
  const CLIMATE_MODELS = [
    { value: "ensemble", label: "Ensemble Mean" },
    { value: "ACCESS-CM2", label: "ACCESS-CM2" },
    { value: "EC-Earth3", label: "EC-Earth3" },
    { value: "GFDL-ESM4", label: "GFDL-ESM4" }
  ];
  const OWM_API_KEY = String(window.OPENWEATHER_API_KEY || "").trim();

  function normalizeMonth(value) {
    const month = Number(value);
    if (!Number.isFinite(month)) {
      return 1;
    }
    return Math.min(12, Math.max(1, Math.round(month)));
  }

  function sanitizeYearRange(value) {
    return CLIMATE_YEAR_RANGES.indexOf(value) >= 0 ? value : CLIMATE_YEAR_RANGES[0];
  }

  function sanitizeScenario(value) {
    return CLIMATE_SCENARIOS.some(function (scenario) {
      return scenario.value === value;
    }) ? value : CLIMATE_SCENARIOS[1].value;
  }

  function sanitizeModel(value) {
    return CLIMATE_MODELS.some(function (model) {
      return model.value === value;
    }) ? value : CLIMATE_MODELS[0].value;
  }

  function isHistoricalRange(yearRange) {
    return sanitizeYearRange(yearRange) === "1970-2000";
  }

  function scenarioIntensity(scenario) {
    const values = {
      "SSP1-2.6": 0.55,
      "SSP2-4.5": 0.78,
      "SSP3-7.0": 1,
      "SSP5-8.5": 1.24
    };
    return values[sanitizeScenario(scenario)] || values["SSP2-4.5"];
  }

  function modelOffset(model) {
    const values = {
      ensemble: 0,
      "ACCESS-CM2": 0.35,
      "EC-Earth3": -0.22,
      "GFDL-ESM4": 0.18
    };
    return values[sanitizeModel(model)] || 0;
  }

  function buildFutureClimateTileUrl(layerKey, month, yearRange, scenario, model) {
    const template = String(
      config.climateFutureTileTemplate ||
      "https://tile-server/{layer}/{scenario}/{model}/{yearRange}/{month}/{z}/{x}/{y}.png"
    );

    return template
      .replace(/\{layer\}/g, encodeURIComponent(layerKey))
      .replace(/\{scenario\}/g, encodeURIComponent(sanitizeScenario(scenario)))
      .replace(/\{model\}/g, encodeURIComponent(sanitizeModel(model)))
      .replace(/\{yearRange\}/g, encodeURIComponent(sanitizeYearRange(yearRange)))
      .replace(/\{month\}/g, encodeURIComponent(String(normalizeMonth(month))));
  }

  function buildOpenWeatherTileUrl(mapName) {
    if (!OWM_API_KEY) {
      return "";
    }
    return "https://tile.openweathermap.org/map/" + mapName + "/{z}/{x}/{y}.png?appid=" + OWM_API_KEY;
  }

  const CLIMATE_LAYERS = {
    temperature_day: {
      label: "Temperature (Day)",
      tileLayer: "temperature_day",
      liveTileName: "temp_new",
      unit: "°C",
      legend: {
        min: -20,
        mid: 10,
        max: 40,
        unit: "°C",
        gradient: "linear-gradient(180deg, #b91c1c 0%, #f97316 20%, #facc15 45%, #67e8f9 72%, #2563eb 100%)"
      },
      getTileUrl: function (month, yearRange, scenario, model) {
        if (!isHistoricalRange(yearRange) && config.climateFutureTileTemplate) {
          return buildFutureClimateTileUrl(this.tileLayer, month, yearRange, scenario, model);
        }
        return buildOpenWeatherTileUrl(this.liveTileName);
      }
    },
    temperature_night: {
      label: "Temperature (Night)",
      tileLayer: "temperature_night",
      liveTileName: "temp_new",
      unit: "°C",
      legend: {
        min: -25,
        mid: 2,
        max: 30,
        unit: "°C",
        gradient: "linear-gradient(180deg, #fb7185 0%, #c084fc 26%, #818cf8 54%, #38bdf8 78%, #0f172a 100%)"
      },
      getTileUrl: function (month, yearRange, scenario, model) {
        if (!isHistoricalRange(yearRange) && config.climateFutureTileTemplate) {
          return buildFutureClimateTileUrl(this.tileLayer, month, yearRange, scenario, model);
        }
        return buildOpenWeatherTileUrl(this.liveTileName);
      }
    },
    precipitation: {
      label: "Precipitation",
      tileLayer: "precipitation",
      liveTileName: "precipitation_new",
      unit: "mm",
      legend: {
        min: 0,
        mid: 70,
        max: 140,
        unit: "mm",
        gradient: "linear-gradient(180deg, #1d4ed8 0%, #38bdf8 35%, #7dd3fc 62%, #e0f2fe 100%)"
      },
      getTileUrl: function (month, yearRange, scenario, model) {
        if (!isHistoricalRange(yearRange) && config.climateFutureTileTemplate) {
          return buildFutureClimateTileUrl(this.tileLayer, month, yearRange, scenario, model);
        }
        return buildOpenWeatherTileUrl(this.liveTileName);
      }
    },
    wet_days: {
      label: "Wet Days",
      tileLayer: "wet_days",
      liveTileName: "clouds_new",
      unit: "days",
      legend: {
        min: 0,
        mid: 15,
        max: 30,
        unit: "days",
        gradient: "linear-gradient(180deg, #475569 0%, #64748b 28%, #94a3b8 58%, #e2e8f0 100%)"
      },
      getTileUrl: function (month, yearRange, scenario, model) {
        if (!isHistoricalRange(yearRange) && config.climateFutureTileTemplate) {
          return buildFutureClimateTileUrl(this.tileLayer, month, yearRange, scenario, model);
        }
        return buildOpenWeatherTileUrl(this.liveTileName);
      }
    }
  };

  function climateSourceId(layerKey) {
    return CLIMATE_LAYER_PREFIX + "-source-" + layerKey;
  }

  function climateLayerId(layerKey) {
    return CLIMATE_LAYER_PREFIX + "-layer-" + layerKey;
  }

  function addClimateLayerSystem(mapInstance) {
    if (!mapInstance || typeof mapInstance.addSource !== "function") {
      return null;
    }

    if (!mapInstance.__climateLayerState) {
      mapInstance.__climateLayerState = {
        activeLayer: null,
        month: 1,
        yearRange: "1970-2000",
        scenario: "SSP2-4.5",
        model: "ensemble",
        visible: true,
        initialized: false,
        boundErrorHandler: false,
        boundMapClick: false,
        panelOpen: false,
        tempChart: null,
        precipChart: null
      };
    }

    const state = mapInstance.__climateLayerState;
    const layerPanel = document.getElementById("locationsLayerPanel");
    const mapWrap = root.parentElement;

    function setStatus(message, isError) {
      const statusNode = document.getElementById(CLIMATE_STATUS_ID);
      if (!statusNode) {
        return;
      }
      statusNode.textContent = message;
      statusNode.classList.toggle("is-error", Boolean(isError));
    }

    function formatLegendValue(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "";
      }
      return Math.abs(numeric - Math.round(numeric)) < 0.05
        ? String(Math.round(numeric))
        : String(Math.round(numeric * 10) / 10);
    }

    function formatCoordinate(value, suffixPositive, suffixNegative) {
      const numeric = Number(value) || 0;
      const suffix = numeric >= 0 ? suffixPositive : suffixNegative;
      return String(Math.round(Math.abs(numeric) * 100) / 100) + "° " + suffix;
    }

    function clamp(value, min, max) {
      return Math.min(max, Math.max(min, value));
    }

    function climateNoise(seed) {
      const raw = Math.sin(seed * 12.9898) * 43758.5453;
      return raw - Math.floor(raw);
    }

    function ensureChartJsLoaded() {
      if (window.Chart) {
        return Promise.resolve(window.Chart);
      }
      if (mapInstance.__climateChartJsPromise) {
        return mapInstance.__climateChartJsPromise;
      }

      mapInstance.__climateChartJsPromise = new Promise(function (resolve, reject) {
        const existingScript = document.querySelector('script[data-climate-chartjs="true"]');
        if (existingScript) {
          existingScript.addEventListener("load", function () {
            resolve(window.Chart);
          }, { once: true });
          existingScript.addEventListener("error", function () {
            reject(new Error("Chart.js could not be loaded."));
          }, { once: true });
          return;
        }

        const script = document.createElement("script");
        script.src = CLIMATE_CHART_JS_URL;
        script.async = true;
        script.dataset.climateChartjs = "true";
        script.onload = function () {
          resolve(window.Chart);
        };
        script.onerror = function () {
          reject(new Error("Chart.js could not be loaded."));
        };
        document.head.appendChild(script);
      });

      return mapInstance.__climateChartJsPromise;
    }

    function generateClimateData(lat, lng) {
      const latitude = Number(lat) || 0;
      const longitude = Number(lng) || 0;
      const latitudeAbs = Math.abs(latitude);
      const phaseShift = latitude >= 0 ? -Math.PI / 2 : Math.PI / 2;
      const baseDay = 27 - latitudeAbs * 0.36 + Math.cos(longitude * Math.PI / 180) * 1.8;
      const seasonalAmplitude = Math.max(4, 12 - latitudeAbs * 0.08);
      const nightOffset = 5 + latitudeAbs * 0.06;
      const tropicalMoisture = clamp(1 - latitudeAbs / 55, 0.15, 1);
      const futureRangeIndex = Math.max(0, CLIMATE_YEAR_RANGES.indexOf(state.yearRange));
      const projectionStep = isHistoricalRange(state.yearRange) ? 0 : futureRangeIndex;
      const scenarioFactor = scenarioIntensity(state.scenario);
      const modelAdjustment = modelOffset(state.model);
      const warming = projectionStep * scenarioFactor * 0.85 + modelAdjustment;
      const precipFactor = isHistoricalRange(state.yearRange)
        ? 1
        : clamp(0.88 + projectionStep * 0.06 * scenarioFactor + modelAdjustment * 0.035, 0.72, 1.42);

      const temperatureDay = CLIMATE_MONTH_LABELS.map(function (_, index) {
        const angle = (index / 12) * Math.PI * 2 + phaseShift;
        const noise = (climateNoise(latitude * 3.1 + longitude * 1.7 + index) - 0.5) * 1.8;
        return Math.round((baseDay + warming + Math.sin(angle) * seasonalAmplitude + noise) * 10) / 10;
      });

      const temperatureNight = temperatureDay.map(function (value, index) {
        const coolingNoise = (climateNoise(latitude * 5.4 + longitude * 2.3 + index * 0.8) - 0.5) * 1.2;
        return Math.round((value - nightOffset + coolingNoise) * 10) / 10;
      });

      const precipitation = CLIMATE_MONTH_LABELS.map(function (_, index) {
        const angle = (index / 12) * Math.PI * 2 + phaseShift + Math.PI / 3;
        const seasonality = (Math.cos(angle) + 1) / 2;
        const basePrecip = 18 + tropicalMoisture * 82 + seasonality * (12 + tropicalMoisture * 48);
        const noise = climateNoise(latitude * 1.9 + longitude * 4.7 + index * 2.1) * 18;
        return Math.round(clamp((basePrecip + noise - latitudeAbs * 0.12) * precipFactor, 4, 260));
      });

      return {
        months: CLIMATE_MONTH_LABELS.slice(),
        temperatureDay: temperatureDay,
        temperatureNight: temperatureNight,
        precipitation: precipitation
      };
    }

    function ensureLegendUi() {
      if (!mapWrap) {
        return null;
      }

      let legend = document.getElementById(CLIMATE_LEGEND_ID);
      if (!legend) {
        legend = document.createElement("section");
        legend.id = CLIMATE_LEGEND_ID;
        legend.className = "locations-climate-legend";
        legend.hidden = true;
        legend.innerHTML = [
          '<div class="locations-climate-legend__header">',
          '<div class="locations-climate-legend__title"></div>',
          '<div class="locations-climate-legend__subtitle"></div>',
          "</div>",
          '<div class="locations-climate-legend__scale">',
          '<div class="locations-climate-legend__gradient"></div>',
          '<div class="locations-climate-legend__labels">',
          '<span class="locations-climate-legend__label is-top"></span>',
          '<span class="locations-climate-legend__label is-middle"></span>',
          '<span class="locations-climate-legend__label is-bottom"></span>',
          "</div>",
          "</div>",
          '<div class="locations-climate-legend__unit"></div>'
        ].join("");
        mapWrap.appendChild(legend);
      }
      return legend;
    }

    function updateLegend() {
      const legend = ensureLegendUi();
      const layerConfig = state.activeLayer ? CLIMATE_LAYERS[state.activeLayer] : null;
      if (!legend || !layerConfig || !state.visible) {
        if (legend) {
          legend.hidden = true;
        }
        return;
      }

      const titleNode = legend.querySelector(".locations-climate-legend__title");
      const subtitleNode = legend.querySelector(".locations-climate-legend__subtitle");
      const gradientNode = legend.querySelector(".locations-climate-legend__gradient");
      const maxNode = legend.querySelector(".locations-climate-legend__label.is-top");
      const midNode = legend.querySelector(".locations-climate-legend__label.is-middle");
      const minNode = legend.querySelector(".locations-climate-legend__label.is-bottom");
      const unitNode = legend.querySelector(".locations-climate-legend__unit");
      const legendConfig = layerConfig.legend || {};

      if (titleNode) {
        titleNode.textContent = layerConfig.label;
      }
      if (subtitleNode) {
        subtitleNode.textContent = CLIMATE_MONTH_LABELS[state.month - 1] + " " + state.yearRange;
      }
      if (gradientNode && legendConfig.gradient) {
        gradientNode.style.background = legendConfig.gradient;
      }
      if (maxNode) {
        maxNode.textContent = formatLegendValue(legendConfig.max);
      }
      if (midNode) {
        midNode.textContent = formatLegendValue(
          legendConfig.mid != null ? legendConfig.mid : ((Number(legendConfig.min) + Number(legendConfig.max)) / 2)
        );
      }
      if (minNode) {
        minNode.textContent = formatLegendValue(legendConfig.min);
      }
      if (unitNode) {
        unitNode.textContent = legendConfig.unit || layerConfig.unit || "";
      }

      legend.hidden = false;
    }

    function ensureDataPanel() {
      if (!mapWrap) {
        return null;
      }

      let panel = document.getElementById(CLIMATE_PANEL_OVERLAY_ID);
      if (!panel) {
        panel = document.createElement("aside");
        panel.id = CLIMATE_PANEL_OVERLAY_ID;
        panel.className = "locations-climate-data-panel";
        panel.setAttribute("aria-live", "polite");
        panel.innerHTML = [
          '<button type="button" id="' + CLIMATE_PANEL_CLOSE_ID + '" class="locations-climate-data-panel__close" aria-label="Close climate panel">&times;</button>',
          '<div class="locations-climate-data-panel__eyebrow">Location Info</div>',
          '<div id="' + CLIMATE_PANEL_COORDS_ID + '" class="locations-climate-data-panel__title">Select a point on the map</div>',
          '<div id="' + CLIMATE_PANEL_META_ID + '" class="locations-climate-data-panel__meta">Monthly mock climate data updates instantly without reloading the map.</div>',
          '<div class="locations-climate-data-panel__chart-block">',
          '<div class="locations-climate-data-panel__chart-title">Temperature</div>',
          '<div class="locations-climate-data-panel__chart-wrap"><canvas id="' + CLIMATE_TEMP_CHART_ID + '"></canvas></div>',
          "</div>",
          '<div class="locations-climate-data-panel__chart-block">',
          '<div class="locations-climate-data-panel__chart-title">Precipitation</div>',
          '<div class="locations-climate-data-panel__chart-wrap"><canvas id="' + CLIMATE_PRECIP_CHART_ID + '"></canvas></div>',
          "</div>"
        ].join("");
        mapWrap.appendChild(panel);

        const closeButton = document.getElementById(CLIMATE_PANEL_CLOSE_ID);
        if (closeButton) {
          closeButton.addEventListener("click", function () {
            panel.classList.remove("is-open");
            state.panelOpen = false;
          });
        }
      }

      return panel;
    }

    function renderCharts(data) {
      return ensureChartJsLoaded().then(function () {
        const tempCanvas = document.getElementById(CLIMATE_TEMP_CHART_ID);
        const precipCanvas = document.getElementById(CLIMATE_PRECIP_CHART_ID);
        if (!tempCanvas || !precipCanvas || !window.Chart) {
          return;
        }

        const isDarkMode = document.body.classList.contains("dark-mode");
        const textColor = isDarkMode ? "#f8fafc" : "#0f172a";
        const gridColor = isDarkMode ? "rgba(148, 163, 184, 0.18)" : "rgba(148, 163, 184, 0.22)";
        const tempContext = tempCanvas.getContext("2d");
        const precipContext = precipCanvas.getContext("2d");
        const dayGradient = tempContext ? tempContext.createLinearGradient(0, 0, 0, tempCanvas.height || 260) : null;
        const nightGradient = tempContext ? tempContext.createLinearGradient(0, 0, 0, tempCanvas.height || 260) : null;
        const precipGradient = precipContext ? precipContext.createLinearGradient(0, 0, 0, precipCanvas.height || 260) : null;

        if (dayGradient) {
          dayGradient.addColorStop(0, "rgba(249, 115, 22, 0.38)");
          dayGradient.addColorStop(1, "rgba(249, 115, 22, 0.02)");
        }
        if (nightGradient) {
          nightGradient.addColorStop(0, "rgba(37, 99, 235, 0.28)");
          nightGradient.addColorStop(1, "rgba(37, 99, 235, 0.02)");
        }
        if (precipGradient) {
          precipGradient.addColorStop(0, "rgba(37, 99, 235, 0.88)");
          precipGradient.addColorStop(1, "rgba(125, 211, 252, 0.66)");
        }

        if (state.tempChart) {
          state.tempChart.destroy();
        }
        if (state.precipChart) {
          state.precipChart.destroy();
        }

        state.tempChart = new window.Chart(tempCanvas, {
          type: "line",
          data: {
            labels: data.months,
            datasets: [
              {
                label: "Temperature (Day)",
                data: data.temperatureDay,
                borderColor: "#f97316",
                backgroundColor: dayGradient || "rgba(249, 115, 22, 0.16)",
                borderWidth: 2.5,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 4,
                pointHitRadius: 14,
                cubicInterpolationMode: "monotone",
                tension: 0.4
              },
              {
                label: "Temperature (Night)",
                data: data.temperatureNight,
                borderColor: "#2563eb",
                backgroundColor: nightGradient || "rgba(37, 99, 235, 0.12)",
                borderWidth: 2.5,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 4,
                pointHitRadius: 14,
                cubicInterpolationMode: "monotone",
                tension: 0.4
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            elements: {
              line: {
                tension: 0.4
              }
            },
            interaction: {
              mode: "index",
              intersect: false
            },
            plugins: {
              legend: {
                position: "top",
                align: "start",
                labels: {
                  boxWidth: 10,
                  boxHeight: 10,
                  usePointStyle: true,
                  pointStyle: "circle",
                  color: textColor,
                  font: {
                    weight: "700"
                  }
                }
              }
            },
            scales: {
              x: {
                ticks: {
                  color: textColor,
                  font: {
                    weight: "700"
                  }
                },
                grid: {
                  display: false
                }
              },
              y: {
                ticks: {
                  color: textColor,
                  font: {
                    weight: "700"
                  }
                },
                grid: {
                  color: gridColor
                },
                title: {
                  display: true,
                  text: "Temperature (°C)",
                  color: textColor,
                  font: {
                    weight: "700"
                  }
                }
              }
            }
          }
        });

        state.precipChart = new window.Chart(precipCanvas, {
          type: "bar",
          data: {
            labels: data.months,
            datasets: [
              {
                label: "Precipitation",
                data: data.precipitation,
                backgroundColor: precipGradient || "rgba(37, 99, 235, 0.72)",
                borderColor: "#1d4ed8",
                borderWidth: 1,
                borderRadius: 6,
                maxBarThickness: 22
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: {
                display: false
              }
            },
            scales: {
              x: {
                ticks: {
                  color: textColor,
                  font: {
                    weight: "700"
                  }
                },
                grid: {
                  display: false
                }
              },
              y: {
                beginAtZero: true,
                ticks: {
                  color: textColor,
                  font: {
                    weight: "700"
                  }
                },
                grid: {
                  color: gridColor
                },
                title: {
                  display: true,
                  text: "mm",
                  color: textColor,
                  font: {
                    weight: "700"
                  }
                }
              }
            }
          }
        });
      });
    }

    function openDataPanel(lat, lng) {
      const panel = ensureDataPanel();
      if (!panel) {
        return;
      }

      const coordsNode = document.getElementById(CLIMATE_PANEL_COORDS_ID);
      const metaNode = document.getElementById(CLIMATE_PANEL_META_ID);
      const data = generateClimateData(lat, lng);
      state.lastPanelLat = lat;
      state.lastPanelLng = lng;

      if (coordsNode) {
        coordsNode.textContent = formatCoordinate(lat, "N", "S") + " , " + formatCoordinate(lng, "E", "W");
      }
      if (metaNode) {
        metaNode.textContent = isHistoricalRange(state.yearRange)
          ? "Monthly climate profile for " + state.yearRange + ". Selected month: " + CLIMATE_MONTH_LABELS[state.month - 1] + "."
          : "Monthly climate profile for " + state.yearRange + " using " + state.scenario + " and " + state.model + ".";
      }

      panel.classList.add("is-open");
      state.panelOpen = true;

      renderCharts(data).catch(function (error) {
        console.warn("Climate charts could not be rendered.", error);
        if (metaNode) {
          metaNode.textContent = "Climate charts are unavailable right now.";
        }
      });
    }

    function refreshOpenDataPanel() {
      if (!state.panelOpen || state.lastPanelLat == null || state.lastPanelLng == null) {
        return;
      }
      openDataPanel(state.lastPanelLat, state.lastPanelLng);
    }

    function getInsertBeforeLayerId() {
      const orderedLayerIds = [
        categoryMeta.HQ.layerId,
        categoryMeta.Office.layerId,
        categoryMeta.Production.layerId,
        WATER_RISK_LAYER_ID,
        "waterway-label"
      ];
      return orderedLayerIds.find(function (layerId) {
        return mapInstance.getLayer(layerId);
      });
    }

    function syncLayerButtons() {
      if (!layerPanel) {
        return;
      }
      Object.keys(CLIMATE_LAYERS).forEach(function (layerKey) {
        const button = layerPanel.querySelector('[data-climate-layer="' + layerKey + '"]');
        const isActive = state.activeLayer === layerKey;
        if (!button) {
          return;
        }
        button.classList.toggle("is-active", isActive);
        button.setAttribute("aria-pressed", isActive ? "true" : "false");
        const stateNode = button.querySelector(".locations-layer-toggle__state");
        if (stateNode) {
          stateNode.textContent = isActive
            ? (state.visible ? "Visible on map" : "Loaded but hidden")
            : "Click to activate";
        }
      });
    }

    function syncProjectionControls() {
      const scenarioField = document.getElementById(CLIMATE_SCENARIO_FIELD_ID);
      const modelField = document.getElementById(CLIMATE_MODEL_FIELD_ID);
      const scenarioSelect = document.getElementById(CLIMATE_SCENARIO_SELECT_ID);
      const modelSelect = document.getElementById(CLIMATE_MODEL_SELECT_ID);
      const showProjectionControls = !isHistoricalRange(state.yearRange);

      if (scenarioField) {
        scenarioField.classList.toggle("is-hidden", !showProjectionControls);
      }
      if (modelField) {
        modelField.classList.toggle("is-hidden", !showProjectionControls);
      }
      if (scenarioSelect) {
        scenarioSelect.disabled = !showProjectionControls;
      }
      if (modelSelect) {
        modelSelect.disabled = !showProjectionControls;
      }
    }

    function syncFormControls() {
      const monthSlider = document.getElementById(CLIMATE_MONTH_SLIDER_ID);
      const monthValue = document.getElementById(CLIMATE_MONTH_VALUE_ID);
      const monthHelper = document.getElementById(CLIMATE_MONTH_HELPER_ID);
      const yearSelect = document.getElementById(CLIMATE_YEAR_SELECT_ID);
      const scenarioSelect = document.getElementById(CLIMATE_SCENARIO_SELECT_ID);
      const modelSelect = document.getElementById(CLIMATE_MODEL_SELECT_ID);
      const visibilityToggle = document.getElementById(CLIMATE_VISIBILITY_TOGGLE_ID);
      const activeConfig = state.activeLayer ? CLIMATE_LAYERS[state.activeLayer] : null;

      if (monthSlider) {
        monthSlider.value = String(state.month);
      }
      if (monthValue) {
        monthValue.textContent = CLIMATE_MONTH_LABELS[state.month - 1];
      }
      if (monthHelper) {
        monthHelper.textContent = activeConfig
          ? activeConfig.label + " for " + CLIMATE_MONTH_LABELS[state.month - 1] + " " + state.yearRange
          : "Pick a climate layer to explore conditions for " + CLIMATE_MONTH_LABELS[state.month - 1] + ".";
      }
      if (yearSelect) {
        yearSelect.value = state.yearRange;
      }
      if (scenarioSelect) {
        scenarioSelect.value = state.scenario;
      }
      if (modelSelect) {
        modelSelect.value = state.model;
      }
      if (visibilityToggle) {
        visibilityToggle.classList.toggle("is-active", state.visible);
        visibilityToggle.setAttribute("aria-pressed", state.visible ? "true" : "false");
        const stateNode = visibilityToggle.querySelector(".locations-layer-toggle__state");
        if (stateNode) {
          stateNode.textContent = state.visible ? "Climate overlays visible" : "Climate overlays hidden";
        }
      }
      syncProjectionControls();
    }

    function syncUi() {
      syncLayerButtons();
      syncFormControls();
      updateLegend();
    }

    function removeClimateLayer(layerKey) {
      const sourceId = climateSourceId(layerKey);
      const layerId = climateLayerId(layerKey);
      if (mapInstance.getLayer(layerId)) {
        mapInstance.removeLayer(layerId);
      }
      if (mapInstance.getSource(sourceId)) {
        mapInstance.removeSource(sourceId);
      }
    }

    function setClimateVisibility(visible) {
      state.visible = Boolean(visible);
      if (state.activeLayer) {
        const activeLayerId = climateLayerId(state.activeLayer);
        if (mapInstance.getLayer(activeLayerId)) {
          mapInstance.setLayoutProperty(
            activeLayerId,
            "visibility",
            state.visible ? "visible" : "none"
          );
        }
      }
      syncUi();
      if (!state.activeLayer) {
        setStatus("Climate controls are ready. Select a layer to visualize live weather tiles.", false);
        return;
      }
      const activeConfig = CLIMATE_LAYERS[state.activeLayer];
      setStatus(
        state.visible
          ? activeConfig.label + " is visible for " + CLIMATE_MONTH_LABELS[state.month - 1] + " " + state.yearRange + "."
          : activeConfig.label + " remains loaded but hidden from the map.",
        false
      );
    }

    function loadClimateLayer(layerKey) {
      const layerConfig = CLIMATE_LAYERS[layerKey];
      if (!layerConfig) {
        return;
      }

      const sourceId = climateSourceId(layerKey);
      const layerId = climateLayerId(layerKey);
      const tileUrl = layerConfig.getTileUrl(state.month, state.yearRange, state.scenario, state.model);
      const beforeLayerId = getInsertBeforeLayerId();
      const climatePaint = {
        "raster-opacity": 0.95,
        "raster-contrast": isHistoricalRange(state.yearRange) ? 0.6 : 0.35 + scenarioIntensity(state.scenario) * 0.42,
        "raster-saturation": isHistoricalRange(state.yearRange) ? 0.4 : 0.15 + scenarioIntensity(state.scenario) * 0.45,
        "raster-hue-rotate": isHistoricalRange(state.yearRange) ? 0 : Math.round(modelOffset(state.model) * 90),
        "raster-brightness-min": 0.1,
        "raster-brightness-max": isHistoricalRange(state.yearRange) ? 1 : 0.92 + Math.min(0.18, scenarioIntensity(state.scenario) * 0.08),
        "raster-fade-duration": 150
      };

      if (!tileUrl) {
        setStatus("Climate tiles are unavailable because the weather API key is missing.", true);
        return;
      }

      try {
        if (state.activeLayer) {
          removeClimateLayer(state.activeLayer);
        }
        removeClimateLayer(layerKey);

        if (!mapInstance.getSource(sourceId)) {
          mapInstance.addSource(sourceId, {
            type: "raster",
            tiles: [tileUrl],
            tileSize: 256
          });
        }

        if (!mapInstance.getLayer(layerId)) {
          mapInstance.addLayer({
            id: layerId,
            type: "raster",
            source: sourceId,
            layout: {
              visibility: state.visible ? "visible" : "none"
            },
            paint: climatePaint
          }, beforeLayerId);
        }

        state.activeLayer = layerKey;
        syncUi();
        setStatus(
          layerConfig.label + " overlay updated for " + CLIMATE_MONTH_LABELS[state.month - 1] + " " + state.yearRange +
          (isHistoricalRange(state.yearRange) ? "." : " using " + state.scenario + " / " + state.model + "."),
          false
        );
      } catch (error) {
        try {
          removeClimateLayer(layerKey);
        } catch (cleanupError) {
          console.warn("Climate layer cleanup failed.", cleanupError);
        }
        console.warn("Climate layer could not be loaded.", error);
        setStatus(layerConfig.label + " could not be loaded right now.", true);
      }
    }

    function reloadActiveLayer() {
      if (state.activeLayer) {
        loadClimateLayer(state.activeLayer);
      }
    }

    function ensurePanelUi() {
      if (!layerPanel) {
        return;
      }

      let section = document.getElementById(CLIMATE_PANEL_ID);
      if (!section) {
        section = document.createElement("section");
        section.id = CLIMATE_PANEL_ID;
        section.className = "locations-climate-panel";
        section.innerHTML = [
          '<div class="locations-climate-panel__header">',
          '<div class="locations-layer-panel__title">Climate Layers</div>',
          '<div class="locations-layer-panel__text">Explore live raster overlays with one active climate layer at a time.</div>',
          "</div>",
          '<div class="locations-climate-panel__list">',
          Object.keys(CLIMATE_LAYERS).map(function (layerKey) {
            return [
              '<button type="button" class="locations-layer-toggle locations-climate-panel__button" data-climate-layer="' + layerKey + '" aria-pressed="false">',
              '<span>' + escapeHtml(CLIMATE_LAYERS[layerKey].label) + "</span>",
              '<span class="locations-layer-toggle__state">Click to activate</span>',
              "</button>"
            ].join("");
          }).join(""),
          "</div>",
          '<label class="locations-climate-panel__field" for="' + CLIMATE_YEAR_SELECT_ID + '">',
          '<span class="locations-climate-panel__label">Year range</span>',
          '<select id="' + CLIMATE_YEAR_SELECT_ID + '" class="locations-climate-panel__select">',
          CLIMATE_YEAR_RANGES.map(function (yearRange) {
            return '<option value="' + yearRange + '">' + yearRange + "</option>";
          }).join(""),
          "</select>",
          "</label>",
          '<label id="' + CLIMATE_SCENARIO_FIELD_ID + '" class="locations-climate-panel__field is-hidden" for="' + CLIMATE_SCENARIO_SELECT_ID + '">',
          '<span class="locations-climate-panel__label">Climate scenario</span>',
          '<select id="' + CLIMATE_SCENARIO_SELECT_ID + '" class="locations-climate-panel__select">',
          CLIMATE_SCENARIOS.map(function (scenario) {
            return '<option value="' + scenario.value + '">' + escapeHtml(scenario.label) + "</option>";
          }).join(""),
          "</select>",
          "</label>",
          '<label id="' + CLIMATE_MODEL_FIELD_ID + '" class="locations-climate-panel__field is-hidden" for="' + CLIMATE_MODEL_SELECT_ID + '">',
          '<span class="locations-climate-panel__label">Climate model</span>',
          '<select id="' + CLIMATE_MODEL_SELECT_ID + '" class="locations-climate-panel__select">',
          CLIMATE_MODELS.map(function (model) {
            return '<option value="' + model.value + '">' + escapeHtml(model.label) + "</option>";
          }).join(""),
          "</select>",
          "</label>",
          '<button type="button" id="' + CLIMATE_VISIBILITY_TOGGLE_ID + '" class="locations-layer-toggle is-active" aria-pressed="true">',
          '<span>Climate visibility</span>',
          '<span class="locations-layer-toggle__state">Climate overlays visible</span>',
          "</button>",
          '<div id="' + CLIMATE_STATUS_ID + '" class="locations-layer-panel__text">Climate controls are ready. Select a layer to visualize live weather tiles.</div>'
        ].join("");
        layerPanel.appendChild(section);

        Object.keys(CLIMATE_LAYERS).forEach(function (layerKey) {
          const button = section.querySelector('[data-climate-layer="' + layerKey + '"]');
          if (button) {
            button.addEventListener("click", function () {
              state.visible = true;
              loadClimateLayer(layerKey);
              setClimateVisibility(true);
            });
          }
        });

        const yearSelect = document.getElementById(CLIMATE_YEAR_SELECT_ID);
        if (yearSelect) {
          yearSelect.addEventListener("change", function (event) {
            state.yearRange = sanitizeYearRange(event.target.value);
            syncFormControls();
            reloadActiveLayer();
            refreshOpenDataPanel();
          });
        }

        const scenarioSelect = document.getElementById(CLIMATE_SCENARIO_SELECT_ID);
        if (scenarioSelect) {
          scenarioSelect.addEventListener("change", function (event) {
            state.scenario = sanitizeScenario(event.target.value);
            syncFormControls();
            reloadActiveLayer();
            refreshOpenDataPanel();
          });
        }

        const modelSelect = document.getElementById(CLIMATE_MODEL_SELECT_ID);
        if (modelSelect) {
          modelSelect.addEventListener("change", function (event) {
            state.model = sanitizeModel(event.target.value);
            syncFormControls();
            reloadActiveLayer();
            refreshOpenDataPanel();
          });
        }

        const visibilityToggle = document.getElementById(CLIMATE_VISIBILITY_TOGGLE_ID);
        if (visibilityToggle) {
          visibilityToggle.addEventListener("click", function () {
            setClimateVisibility(!state.visible);
          });
        }
      }

      syncUi();
    }

    function ensureMonthOverlay() {
      if (!mapWrap) {
        return;
      }

      let overlay = document.getElementById(CLIMATE_MONTH_OVERLAY_ID);
      if (!overlay) {
        overlay = document.createElement("section");
        overlay.id = CLIMATE_MONTH_OVERLAY_ID;
        overlay.className = "locations-climate-slider";
        overlay.innerHTML = [
          '<div class="locations-climate-slider__top">',
          '<div>',
          '<div class="locations-climate-slider__label">Month</div>',
          '<div id="' + CLIMATE_MONTH_HELPER_ID + '" class="locations-climate-slider__helper">Pick a climate layer to explore conditions for Jan.</div>',
          "</div>",
          '<div id="' + CLIMATE_MONTH_VALUE_ID + '" class="locations-climate-slider__value">Jan</div>',
          "</div>",
          '<input id="' + CLIMATE_MONTH_SLIDER_ID + '" class="locations-climate-slider__input" type="range" min="1" max="12" step="1" value="1" aria-label="Climate month selector">',
          '<div class="locations-climate-slider__ticks">',
          CLIMATE_MONTH_LABELS.map(function (label) {
            return "<span>" + label + "</span>";
          }).join(""),
          "</div>"
        ].join("");
        mapWrap.appendChild(overlay);

        const monthSlider = document.getElementById(CLIMATE_MONTH_SLIDER_ID);
        if (monthSlider) {
          monthSlider.addEventListener("input", function (event) {
            state.month = normalizeMonth(event.target.value);
            syncFormControls();
            reloadActiveLayer();
          refreshOpenDataPanel();
          });
        }
      }

      syncFormControls();
    }

    function bindErrorHandler() {
      if (state.boundErrorHandler) {
        return;
      }
      state.boundErrorHandler = true;

      mapInstance.on("error", function (event) {
        const sourceId = event && event.sourceId ? String(event.sourceId) : "";
        if (sourceId.indexOf(CLIMATE_LAYER_PREFIX + "-source-") !== 0) {
          return;
        }
        setStatus("Climate tiles are temporarily unavailable for this layer.", true);
      });
    }

    function bindMapClickHandler() {
      if (state.boundMapClick) {
        return;
      }
      state.boundMapClick = true;

      mapInstance.on("click", function (event) {
        if (!event || !event.lngLat) {
          return;
        }
        openDataPanel(event.lngLat.lat, event.lngLat.lng);
      });
    }

    ensurePanelUi();
    ensureMonthOverlay();
    ensureLegendUi();
    ensureDataPanel();
    bindErrorHandler();
    bindMapClickHandler();
    state.initialized = true;
    return state;
  }

  window.addWaterRiskLayer = addWaterRiskLayer;

  if (!window.mapboxgl) {
    setNotice("Map library could not be loaded.");
    return;
  }

  if (!mapboxgl.accessToken) {
    setNotice("Mapbox access token is not configured for this environment.");
    return;
  }

  const map = new mapboxgl.Map({
    container: root,
    style: config.style || "mapbox://styles/mapbox/streets-v12",
    center: Array.isArray(config.center) ? config.center : [15, 35],
    zoom: typeof config.zoom === "number" ? config.zoom : 1.6,
    projection: "globe",
    attributionControl: false
  });

  map.scrollZoom.disable();
  addLocationSearchControl(map);
  map.addControl(new mapboxgl.NavigationControl({ showCompass: false }), "top-right");

  function bindLayerInteractions(layerId, popupLabel) {
    map.on("click", layerId, function (event) {
      const feature = event.features && event.features[0];
      if (!feature) return;
      const coordinates = feature.geometry.coordinates.slice();
      const name = feature.properties && feature.properties.name ? feature.properties.name : "";
      const country = feature.properties && feature.properties.country ? feature.properties.country : "";
      new mapboxgl.Popup({ offset: 16 })
        .setLngLat(coordinates)
        .setHTML(popupHtml({ name: name, country: country, type: popupLabel }))
        .addTo(map);
    });

    map.on("mouseenter", layerId, function () {
      map.getCanvas().style.cursor = "pointer";
    });

    map.on("mouseleave", layerId, function () {
      map.getCanvas().style.cursor = "";
    });
  }

  map.on("load", function () {
    fetchLocations()
      .then(function (locations) {
        const geojson = {
          type: "FeatureCollection",
          features: locations.map(function (loc) {
            return {
              type: "Feature",
              properties: {
                name: loc.name,
                country: loc.country,
                type: loc.type
              },
              geometry: {
                type: "Point",
                coordinates: [loc.lng, loc.lat]
              }
            };
          })
        };

        map.addSource("locations", {
          type: "geojson",
          data: geojson
        });

        Object.keys(categoryMeta).forEach(function (key) {
          const meta = categoryMeta[key];
          map.addLayer({
            id: meta.layerId,
            type: "circle",
            source: "locations",
            filter: ["==", ["get", "type"], key],
            paint: {
              "circle-radius": meta.radius,
              "circle-color": meta.color,
              "circle-opacity": key === "Production" ? 0.95 : 0.9
            }
          });

          bindLayerInteractions(meta.layerId, key);
        });

        addWaterRiskLayer(map).then(function () {
          addClimateLayerSystem(map);
        });
      })
      .catch(function (error) {
        setNotice(error && error.message ? error.message : "Map data could not be rendered.");
      });
  });
})();
