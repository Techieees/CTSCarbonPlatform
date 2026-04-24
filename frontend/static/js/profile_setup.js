(function () {
  const form = document.querySelector("[data-profile-form]");
  if (!form) return;

  const businessTypeSelect = form.querySelector("[data-business-type-select]");
  const productTypeWrapper = form.querySelector("[data-product-type-wrapper]");
  const productTypeSelect = form.querySelector("[data-product-type-select]");
  const heatingSourceSelect = form.querySelector("[data-heating-source-select]");
  const heatingNote = form.querySelector("[data-heating-note]");
  const travelProviderSelect = form.querySelector("[data-travel-provider-select]");
  const travelProviderNote = form.querySelector("[data-travel-provider-note]");
  const locationsWrap = form.querySelector("[data-operating-locations]");
  const addLocationBtn = form.querySelector("[data-add-operating-location]");
  const hiddenLocationsInput = form.querySelector("#operating_locations_json");

  const countryOptions = Array.isArray(window.__operatingLocationCountryOptions)
    ? window.__operatingLocationCountryOptions
    : [];
  const initialLocations = Array.isArray(window.__profileSetupInitialLocations)
    ? window.__profileSetupInitialLocations
    : [];

  function setHidden(el, hidden) {
    if (!el) return;
    el.hidden = hidden;
  }

  function syncConditionalUi() {
    const businessType = businessTypeSelect ? String(businessTypeSelect.value || "").trim() : "";
    const shouldShowProductType = businessType === "Manufacturer";
    setHidden(productTypeWrapper, !shouldShowProductType);
    if (!shouldShowProductType && productTypeSelect) {
      productTypeSelect.value = "";
    }

    const heatingSource = heatingSourceSelect ? String(heatingSourceSelect.value || "").trim() : "";
    setHidden(heatingNote, !heatingSource);

    const travelProvider = travelProviderSelect ? String(travelProviderSelect.value || "").trim() : "";
    setHidden(travelProviderNote, travelProvider !== "yes");
  }

  function countryOptionsHtml(selected) {
    const current = String(selected || "").trim().toUpperCase();
    const options = ['<option value="">Select country</option>'];
    countryOptions.forEach((row) => {
      const code = Array.isArray(row) ? String(row[0] || "") : "";
      const label = Array.isArray(row) ? String(row[1] || "") : "";
      if (!code || !label) return;
      const isSelected = code === current ? " selected" : "";
      options.push(`<option value="${code}"${isSelected}>${label} (${code})</option>`);
    });
    return options.join("");
  }

  function siteTypeOptionsHtml(selected) {
    const current = String(selected || "").trim().toLowerCase();
    const options = ['<option value="">Select site type</option>'];
    ["office", "factory", "warehouse", "other"].forEach((value) => {
      const label = value.charAt(0).toUpperCase() + value.slice(1);
      const isSelected = value === current ? " selected" : "";
      options.push(`<option value="${value}"${isSelected}>${label}</option>`);
    });
    return options.join("");
  }

  function readLocations() {
    if (!locationsWrap) return [];
    return Array.from(locationsWrap.querySelectorAll("[data-operating-location-row]")).map((row) => ({
      country: String(row.querySelector("[data-location-country]")?.value || "").trim().toUpperCase(),
      site_type: String(row.querySelector("[data-location-site-type]")?.value || "").trim().toLowerCase(),
    })).filter((row) => row.country || row.site_type);
  }

  function syncLocationsField() {
    if (!hiddenLocationsInput) return;
    hiddenLocationsInput.value = JSON.stringify(readLocations());
  }

  function bindRow(row) {
    row.querySelector("[data-remove-operating-location]")?.addEventListener("click", function () {
      row.remove();
      syncLocationsField();
    });
    row.querySelector("[data-location-country]")?.addEventListener("change", syncLocationsField);
    row.querySelector("[data-location-site-type]")?.addEventListener("change", syncLocationsField);
  }

  function addLocationRow(data) {
    if (!locationsWrap) return;
    const row = document.createElement("div");
    row.className = locationsWrap.classList.contains("profile-page-locations")
      ? "profile-page-location"
      : "profile-setup-location";
    row.setAttribute("data-operating-location-row", "1");
    row.innerHTML = `
      <div class="row g-3 align-items-end">
        <div class="col-md-5">
          <label class="form-label">Country</label>
          <select class="form-select" data-location-country>${countryOptionsHtml(data?.country)}</select>
        </div>
        <div class="col-md-5">
          <label class="form-label">Site Type</label>
          <select class="form-select" data-location-site-type>${siteTypeOptionsHtml(data?.site_type)}</select>
        </div>
        <div class="col-md-2 d-grid">
          <button type="button" class="btn btn-outline-danger btn-sm" data-remove-operating-location>Remove</button>
        </div>
      </div>
    `;
    locationsWrap.appendChild(row);
    bindRow(row);
    syncLocationsField();
  }

  if (locationsWrap) {
    const rows = initialLocations.length ? initialLocations : [{}];
    rows.forEach((row) => addLocationRow(row));
  }

  addLocationBtn?.addEventListener("click", function () {
    addLocationRow({});
  });

  businessTypeSelect?.addEventListener("change", syncConditionalUi);
  heatingSourceSelect?.addEventListener("change", syncConditionalUi);
  travelProviderSelect?.addEventListener("change", syncConditionalUi);
  syncConditionalUi();
  syncLocationsField();
})();
