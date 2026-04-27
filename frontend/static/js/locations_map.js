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

        addWaterRiskLayer(map);
      })
      .catch(function (error) {
        setNotice(error && error.message ? error.message : "Map data could not be rendered.");
      });
  });
})();
