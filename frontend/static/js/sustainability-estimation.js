(function () {
  const root = document.getElementById("sust-estimation-root");
  if (!root) return;

  const cfg = window.SUST_ESTIMATION_CONFIG || {};
  const feedback = document.getElementById("sustFeedback");
  const productType = document.getElementById("sustProductType");
  const scenarioPreview = document.getElementById("sustScenarioPreview");
  const calcBtn = document.getElementById("sustCalculateBtn");
  const saveBtn = document.getElementById("sustSaveQuestionnaireBtn");
  const resultPanel = document.getElementById("sustResultPanel");

  function showFeedback(msg, ok) {
    if (!feedback) return;
    feedback.textContent = msg;
    feedback.className = "sust-feedback mt-3 " + (ok ? "is-ok" : "is-err");
    feedback.hidden = false;
  }

  function formPayload() {
    const shared = document.getElementById("sustSharedOffice");
    const bills = document.getElementById("sustUtilityBills");
    return {
      company_name: document.getElementById("sustCompany")?.value || cfg.company_name,
      business_function: document.getElementById("sustBusinessFunction")?.value || cfg.workflow?.business_function,
      reporting_period_key: document.getElementById("sustPeriod")?.value || cfg.period_key,
      office_site_key: document.getElementById("sustOfficeSite")?.value || "",
      country: document.getElementById("sustCountry")?.value || "",
      employee_count: Number(document.getElementById("sustEmployees")?.value || 0),
      office_size_m2: Number(document.getElementById("sustOfficeM2")?.value || 0),
      is_shared_office: !!(shared && shared.checked),
      has_utility_bills: !!(bills && bills.checked),
      electricity_type: document.getElementById("sustElectricityType")?.value || "",
      heating_type: document.getElementById("sustHeatingType")?.value || "",
      product_type: productType?.value || "",
      product_weight_kg: Number(document.getElementById("sustProductWeight")?.value || 0),
      eol_scenario_id: Number(document.getElementById("sustEolScenario")?.value || 0) || null,
      actual_utilities: {
        electricity_kwh: Number(document.getElementById("sustActualElectricity")?.value || 0) || null,
        heating_kwh: Number(document.getElementById("sustActualHeating")?.value || 0) || null,
        water_m3: Number(document.getElementById("sustActualWater")?.value || 0) || null,
        waste_kg: Number(document.getElementById("sustActualWaste")?.value || 0) || null,
      },
    };
  }

  function renderScenarioPreview() {
    if (!scenarioPreview || !productType) return;
    const pt = productType.value;
    const meth = (cfg.methodologies || []).find((m) => m.product_type === pt);
    const scenario = meth?.eol_scenarios?.find((s) => s.is_default) || meth?.eol_scenarios?.[0];
    if (!scenario) {
      scenarioPreview.innerHTML = "<p class=\"text-muted small mb-0\">Select a product type to load the embedded disposal profile.</p>";
      return;
    }
    const hidden = document.getElementById("sustEolScenario");
    if (hidden) hidden.value = String(scenario.id);

    const streams = { recycling: 0, energy_recovery: 0, landfill: 0 };
    (scenario.components || []).forEach((c) => {
      const share = (c.weight_fraction || 0) * (c.ratio_pct || 0) / 100;
      streams[c.disposal_stream] = (streams[c.disposal_stream] || 0) + share;
    });
    const total = streams.recycling + streams.energy_recovery + streams.landfill || 1;
    const pct = (v) => ((v / total) * 100).toFixed(1);

    scenarioPreview.innerHTML = `
      <div class="sust-method-card mb-2">
        <strong>${scenario.label}</strong>
        <p class="small text-muted mb-2">${scenario.product_type} · ratios from methodology (not manual entry)</p>
        <div class="sust-stream-bar" aria-hidden="true">
          <span class="recycling" style="width:${pct(streams.recycling)}%"></span>
          <span class="energy_recovery" style="width:${pct(streams.energy_recovery)}%"></span>
          <span class="landfill" style="width:${pct(streams.landfill)}%"></span>
        </div>
        <div class="d-flex flex-wrap gap-2 small mt-2">
          <span>Recycling ${pct(streams.recycling)}%</span>
          <span>Energy recovery ${pct(streams.energy_recovery)}%</span>
          <span>Landfill ${pct(streams.landfill)}%</span>
        </div>
      </div>
      <ul class="small mb-0 ps-3">
        ${(scenario.components || []).slice(0, 6).map((c) =>
          `<li>${c.component_label}: ${c.ratio_pct}% ${c.disposal_stream.replace("_", " ")}</li>`
        ).join("")}
      </ul>`;
  }

  function renderResult(result) {
    if (!resultPanel || !result) return;
    const facility = result.facility || {};
    const eol = result.eol || null;
    const lines = (facility.lines || [])
      .map(
        (l) =>
          `<tr><td>${l.scope_category}</td><td>${l.activity?.toFixed?.(2) ?? l.activity} ${l.activity_unit}</td><td>${l.emissions_t?.toFixed?.(4) ?? l.emissions_t} t</td><td>${l.data_source}</td></tr>`
      )
      .join("");

    const eolRows = eol
      ? (eol.streams || [])
          .map(
            (s) =>
              `<tr><td>${s.disposal_stream}</td><td>${s.waste_kg?.toFixed?.(2)} kg</td><td>${s.emissions_t?.toFixed?.(4)} t</td></tr>`
          )
          .join("")
      : "";

    resultPanel.innerHTML = `
      <div class="sust-card mt-3">
        <h3 class="h6 mb-3">Calculation transparency</h3>
        <p class="mb-2"><strong>Total:</strong> ${result.total_co2e_t?.toFixed?.(4) ?? result.total_co2e_t} tCO₂e</p>
        <p class="small text-muted">Path: ${result.methodology_path?.facility_path} · shared office: ${result.methodology_path?.apply_shared_office ? "yes" : "no"}</p>
        <h4 class="h6 mt-3">Facility</h4>
        <table class="table table-sm sust-result-table"><thead><tr><th>Scope</th><th>Activity</th><th>Emissions</th><th>Source</th></tr></thead><tbody>${lines}</tbody></table>
        ${eol ? `<h4 class="h6 mt-3">Scope 3 Cat 12 — ${eol.scenario_label}</h4>
        <table class="table table-sm sust-result-table"><thead><tr><th>Stream</th><th>Waste</th><th>Emissions</th></tr></thead><tbody>${eolRows}</tbody></table>` : ""}
      </div>`;
  }

  async function postJson(url, body) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || "Request failed");
    return data;
  }

  if (productType) {
    productType.addEventListener("change", renderScenarioPreview);
    renderScenarioPreview();
  }

  if (saveBtn) {
    saveBtn.addEventListener("click", async () => {
      try {
        await postJson(cfg.questionnaire_url, formPayload());
        showFeedback("Questionnaire saved.", true);
      } catch (e) {
        showFeedback(e.message, false);
      }
    });
  }

  if (calcBtn) {
    calcBtn.addEventListener("click", async () => {
      try {
        calcBtn.disabled = true;
        const data = await postJson(cfg.calculate_url, formPayload());
        renderResult(data.result);
        showFeedback(`Calculation complete: ${data.total_co2e_t?.toFixed?.(4)} tCO₂e`, true);
        if (data.detail_url) {
          window.setTimeout(() => {
            window.location.href = data.detail_url;
          }, 1200);
        }
      } catch (e) {
        showFeedback(e.message, false);
      } finally {
        calcBtn.disabled = false;
      }
    });
  }
})();
