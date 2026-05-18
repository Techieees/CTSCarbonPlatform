(function () {
  const cfg = window.SUST_PRODUCTS_CONFIG || {};
  const body = document.getElementById("sustProductsBody");
  const feedback = document.getElementById("sustProductsFeedback");

  if (!body) return;

  const proofByRow = {};

  function showFeedback(msg, ok) {
    if (!feedback) return;
    feedback.textContent = msg;
    feedback.className = "sust-feedback mt-3 " + (ok ? "is-ok" : "is-err");
    feedback.hidden = false;
  }

  function rowHtml(data, idx) {
    const pt = (cfg.product_types || []).map((t) => `<option value="${t}" ${data.product_type === t ? "selected" : ""}>${t}</option>`).join("");
    const dest = (cfg.destinations || []).map((d) => `<option value="${d}" ${data.end_use_location === d ? "selected" : ""}>${d}</option>`).join("");
    const qu = (cfg.quantity_units || []).map((u) => `<option value="${u}" ${data.quantity_unit === u ? "selected" : ""}>${u}</option>`).join("");
    const pu = (cfg.product_units || []).map((u) => `<option value="${u}" ${data.product_unit === u ? "selected" : ""}>${u}</option>`).join("");
    return `<tr data-row="${idx}">
      <td><select class="form-select form-select-sm p-type">${pt}</select></td>
      <td><input type="number" class="form-control form-control-sm p-qty" min="0" step="any" value="${data.quantity || ""}"></td>
      <td><select class="form-select form-select-sm p-qunit">${qu}</select></td>
      <td><select class="form-select form-select-sm p-dest">${dest}</select></td>
      <td><input type="number" class="form-control form-control-sm p-weight" min="0" step="any" value="${data.product_weight || ""}"></td>
      <td><select class="form-select form-select-sm p-wunit">${pu}</select></td>
      <td><input type="file" class="form-control form-control-sm p-proof" accept=".pdf,.png,.jpg,.jpeg,.xlsx"></td>
      <td><button type="button" class="btn btn-sm btn-outline-danger p-remove">×</button></td>
    </tr>`;
  }

  function collectRows() {
    return [...body.querySelectorAll("tr")].map((tr, i) => ({
      product_type: tr.querySelector(".p-type")?.value || "",
      quantity: Number(tr.querySelector(".p-qty")?.value || 0),
      quantity_unit: tr.querySelector(".p-qunit")?.value || "",
      end_use_location: tr.querySelector(".p-dest")?.value || "",
      product_weight: Number(tr.querySelector(".p-weight")?.value || 0),
      product_unit: tr.querySelector(".p-wunit")?.value || "",
      proof_attachment_path: proofByRow[i]?.path || null,
      proof_attachment_name: proofByRow[i]?.name || null,
    }));
  }

  function addRow(data) {
    const idx = body.querySelectorAll("tr").length;
    body.insertAdjacentHTML("beforeend", rowHtml(data || {}, idx));
    const tr = body.lastElementChild;
    tr.querySelector(".p-remove")?.addEventListener("click", () => tr.remove());
    tr.querySelector(".p-proof")?.addEventListener("change", async (e) => {
      const file = e.target.files?.[0];
      if (!file) return;
      const fd = new FormData();
      fd.append("proof", file);
      const res = await fetch(cfg.proof_url, { method: "POST", body: fd });
      const data = await res.json();
      if (res.ok) proofByRow[idx] = { path: data.proof_attachment_path, name: data.proof_attachment_name };
    });
  }

  (cfg.initial_rows || []).forEach((r) => addRow(r));
  if (!cfg.initial_rows?.length) addRow({ product_type: "EPOD", quantity_unit: "pieces", product_unit: "kg", end_use_location: "Norway" });

  document.getElementById("sustAddProductRow")?.addEventListener("click", () => addRow({}));

  document.getElementById("sustSaveProducts")?.addEventListener("click", async () => {
    const rows = collectRows();
    const res = await fetch(cfg.save_url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ company_name: cfg.company_name, reporting_period_key: cfg.period_key, rows }),
    });
    const data = await res.json();
    showFeedback(res.ok ? data.message || "Saved." : data.error || "Save failed", res.ok);
  });

  document.getElementById("sustCalcProducts")?.addEventListener("click", async () => {
    const rows = collectRows();
    await fetch(cfg.save_url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ company_name: cfg.company_name, reporting_period_key: cfg.period_key, rows }),
    });
    const res = await fetch(cfg.calc_url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ company_name: cfg.company_name, reporting_period_key: cfg.period_key, business_function: "Manufacturer" }),
    });
    const data = await res.json();
    if (res.ok && data.detail_url) window.location.href = data.detail_url;
    else showFeedback(data.error || "Calculation failed", false);
  });
})();
