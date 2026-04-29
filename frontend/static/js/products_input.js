(function () {
  const config = window.__productsInputConfig || {};
  const period = window.__productsInputPeriod || {};
  const countries = Array.isArray(window.__productsInputCountries) ? window.__productsInputCountries : [];
  const siteTypes = Array.isArray(window.__productsInputSiteTypes) ? window.__productsInputSiteTypes : [];
  const saveUrl = window.__productsInputSaveUrl || "/api/products-input/save";
  const dropdowns = config.dropdowns || {};

  const tbody = document.querySelector("[data-products-table-body]");
  const addRowBtn = document.querySelector("[data-add-product-row]");
  const submitBtn = document.querySelector("[data-submit-products]");
  const statusEl = document.querySelector("[data-products-status]");
  const locationList = document.querySelector("[data-location-list]");
  const addLocationBtn = document.querySelector("[data-add-location]");
  const businessTypeSelect = document.querySelector('[data-products-profile="business_type"]');
  const idleLabel = document.querySelector("[data-submit-idle]");
  const busyLabel = document.querySelector("[data-submit-busy]");
  const manufacturerFields = Array.from(document.querySelectorAll("[data-manufacturer-field]"));

  if (!tbody) return;

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function optionsHtml(options, selected, placeholder) {
    const out = [`<option value="">${esc(placeholder || "Select")}</option>`];
    (options || []).forEach((option) => {
      const value = Array.isArray(option) ? option[0] : option;
      const label = Array.isArray(option) ? option[1] : option;
      out.push(`<option value="${esc(value)}" ${String(value) === String(selected || "") ? "selected" : ""}>${esc(label)}</option>`);
    });
    return out.join("");
  }

  function countryOptionsHtml(selected) {
    const out = ['<option value="">Select country</option>'];
    countries.forEach((item) => {
      const code = Array.isArray(item) ? item[0] : "";
      const name = Array.isArray(item) ? item[1] : code;
      const label = `${name} (${code})`;
      out.push(`<option value="${esc(code)}" ${code === selected ? "selected" : ""}>${esc(label)}</option>`);
    });
    return out.join("");
  }

  function rowTemplate(row) {
    const productType = row["Product Type"] || row.product_type || "";
    const quantity = row["Quantity"] || row.quantity || "";
    const quantityUnit = row["Quantity Unit"] || row.quantity_unit || "";
    const endUseLocation = row["End Use Location"] || row.end_use_location || "";
    const productWeight = row["Product Weight"] || row.product_weight || "";
    const productUnit = row["Product Unit"] || row.product_unit || "";

    return `
      <tr data-product-row>
        <td><input class="form-control" data-field="Reporting period (month, year)" value="${esc(period.label || "")}" readonly></td>
        <td><select class="form-select" data-field="Product Type">${optionsHtml(dropdowns.product_types || [], productType, "Product type")}</select></td>
        <td><input class="form-control" type="number" min="0" step="any" data-field="Quantity" value="${esc(quantity)}"></td>
        <td><select class="form-select" data-field="Quantity Unit">${optionsHtml(dropdowns.quantity_units || [], quantityUnit, "Unit")}</select></td>
        <td><input class="form-control" data-field="End Use Location" value="${esc(endUseLocation)}" placeholder="Country or destination"></td>
        <td><input class="form-control" type="number" min="0" step="any" data-field="Product Weight" value="${esc(productWeight)}"></td>
        <td><select class="form-select" data-field="Product Unit">${optionsHtml(dropdowns.product_units || [], productUnit, "Unit")}</select></td>
        <td><button class="btn btn-outline-danger btn-sm" type="button" data-delete-product-row>Delete</button></td>
      </tr>
    `;
  }

  function addProductRow(row) {
    tbody.insertAdjacentHTML("beforeend", rowTemplate(row || {}));
  }

  function setStatus(message, type) {
    if (!statusEl) return;
    statusEl.textContent = message || "";
    statusEl.classList.toggle("text-danger", type === "error");
    statusEl.classList.toggle("text-success", type === "success");
  }

  function collectRows() {
    return Array.from(tbody.querySelectorAll("[data-product-row]")).map((tr) => {
      const row = {};
      tr.querySelectorAll("[data-field]").forEach((input) => {
        row[input.getAttribute("data-field")] = input.value.trim();
      });
      return row;
    });
  }

  function validateRows(rows) {
    const errors = [];
    if (!rows.length) {
      errors.push("Add at least one product row.");
    }
    rows.forEach((row, index) => {
      const label = `Row ${index + 1}`;
      ["Product Type", "Quantity Unit", "End Use Location", "Product Unit"].forEach((field) => {
        if (!row[field]) errors.push(`${label}: ${field} is required.`);
      });
      ["Quantity", "Product Weight"].forEach((field) => {
        const value = Number(row[field]);
        if (!Number.isFinite(value) || value <= 0) errors.push(`${label}: ${field} must be greater than 0.`);
      });
    });
    return errors;
  }

  function locationTemplate(row) {
    const country = row && row.country ? row.country : "";
    const siteType = row && row.site_type ? row.site_type : "";
    return `
      <div class="row g-2 align-items-center" data-location-row>
        <div class="col-7">
          <select class="form-select form-select-sm" data-location-country>${countryOptionsHtml(country)}</select>
        </div>
        <div class="col-4">
          <select class="form-select form-select-sm" data-location-site-type>${optionsHtml(siteTypes, siteType, "Site type")}</select>
        </div>
        <div class="col-1 text-end">
          <button class="btn btn-outline-danger btn-sm" type="button" data-delete-location aria-label="Delete location">x</button>
        </div>
      </div>
    `;
  }

  function addLocation(row) {
    if (!locationList) return;
    locationList.insertAdjacentHTML("beforeend", locationTemplate(row || {}));
  }

  function collectProfile() {
    const profile = {};
    document.querySelectorAll("[data-products-profile]").forEach((input) => {
      profile[input.getAttribute("data-products-profile")] = input.value.trim();
    });
    profile.operating_locations = Array.from(document.querySelectorAll("[data-location-row]"))
      .map((row) => ({
        country: (row.querySelector("[data-location-country]") || {}).value || "",
        site_type: (row.querySelector("[data-location-site-type]") || {}).value || ""
      }))
      .filter((row) => row.country || row.site_type);
    return profile;
  }

  function validateProfile(profile) {
    const errors = [];
    if (!profile.business_type) errors.push("Business Type is required.");
    if (profile.business_type === "Manufacturer" && !profile.number_of_products_in_use) {
      errors.push("Number of products in use is required for manufacturers.");
    }
    if (!profile.operating_locations.length) errors.push("Add at least one operating location.");
    return errors;
  }

  function syncManufacturerFields() {
    const show = businessTypeSelect && businessTypeSelect.value === "Manufacturer";
    manufacturerFields.forEach((field) => {
      field.hidden = !show;
    });
  }

  function setSaving(saving) {
    if (submitBtn) submitBtn.disabled = saving;
    if (idleLabel) idleLabel.classList.toggle("d-none", saving);
    if (busyLabel) {
      busyLabel.classList.toggle("d-none", !saving);
      busyLabel.classList.toggle("d-inline-flex", saving);
    }
  }

  addRowBtn?.addEventListener("click", () => addProductRow({}));

  tbody.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-delete-product-row]");
    if (!btn) return;
    btn.closest("[data-product-row]")?.remove();
    if (!tbody.querySelector("[data-product-row]")) addProductRow({});
  });

  addLocationBtn?.addEventListener("click", () => addLocation({}));
  locationList?.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-delete-location]");
    if (!btn) return;
    btn.closest("[data-location-row]")?.remove();
  });

  businessTypeSelect?.addEventListener("change", syncManufacturerFields);

  submitBtn?.addEventListener("click", async () => {
    const rows = collectRows();
    const profile = collectProfile();
    const errors = validateProfile(profile).concat(validateRows(rows));
    if (errors.length) {
      setStatus(errors[0], "error");
      return;
    }

    setSaving(true);
    setStatus("Saving monthly product data...", "");
    try {
      const res = await fetch(saveUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json"
        },
        body: JSON.stringify({ profile, rows })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data.error || "Save failed.");
      }
      setStatus(data.message || "Products log saved.", "success");
    } catch (error) {
      setStatus(error.message || "Save failed.", "error");
    } finally {
      setSaving(false);
    }
  });

  const initialRows = Array.isArray(window.__productsInputRows) ? window.__productsInputRows : [];
  if (initialRows.length) {
    initialRows.forEach(addProductRow);
  } else {
    addProductRow({});
  }

  const profile = window.__productsInputProfile || {};
  document.querySelectorAll("[data-products-profile]").forEach((input) => {
    const key = input.getAttribute("data-products-profile");
    if (Object.prototype.hasOwnProperty.call(profile, key)) {
      input.value = profile[key] || "";
    }
  });
  const initialLocations = Array.isArray(profile.operating_locations) ? profile.operating_locations : [];
  if (initialLocations.length) {
    initialLocations.forEach(addLocation);
  } else {
    addLocation({});
  }
  syncManufacturerFields();
})();
