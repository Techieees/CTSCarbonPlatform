mapboxgl.accessToken = MAPBOX_TOKEN;

(function () {
  const root = document.getElementById("locationsMap");
  const notice = document.getElementById("locationsMapNotice");
  const config = window.__locationsMapConfig || {};
  if (!root) return;

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
      })
      .catch(function (error) {
        setNotice(error && error.message ? error.message : "Map data could not be rendered.");
      });
  });
})();
