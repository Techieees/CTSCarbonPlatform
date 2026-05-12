(function () {
  'use strict';

  const root = document.getElementById('supplierAllRoot');
  if (!root) return;

  const registryUrl = root.dataset.registryUrl || '';
  const syncUrl = root.dataset.syncUrl || '';
  const runsUrl = root.dataset.runsUrl || '';
  const countriesUrl = root.dataset.countriesUrl || '';
  const jobTpl = root.dataset.jobTpl || '';

  const elStatus = root.querySelector('[data-field="sync-status"]');
  const elMsg = root.querySelector('[data-field="sync-message"]');
  const elRuns = root.querySelector('[data-field="sync-runs-body"]');
  const filterForm = root.querySelector('#supplierFilterForm');
  const elCountry = filterForm ? filterForm.querySelector('[name="filter_country"]') : null;
  const elActive = filterForm ? filterForm.querySelector('[name="filter_active"]') : null;
  const elQ = filterForm ? filterForm.querySelector('[name="filter_q"]') : null;
  const elSort = filterForm ? filterForm.querySelector('[name="sort_col"]') : null;
  const elOrder = filterForm ? filterForm.querySelector('[name="sort_order"]') : null;
  const elPage = filterForm ? filterForm.querySelector('input[name="page"]') : null;
  const elPageInfo = root.querySelector('[data-field="page-info"]');
  const elTableBody = root.querySelector('[data-field="registry-body"]');
  const btnFull = root.querySelector('[data-action="sync-full"]');
  const btnInc = root.querySelector('[data-action="sync-incremental"]');
  const btnRefresh = root.querySelector('[data-action="refresh-table"]');

  function jobStatusUrl(jobId) {
    if (!jobTpl || !jobId) return '';
    return jobTpl.replace('__JOB_ID__', encodeURIComponent(jobId));
  }

  function setBusy(busy, label) {
    [btnFull, btnInc].forEach((b) => {
      if (!b) return;
      b.disabled = busy;
      b.setAttribute('aria-busy', busy ? 'true' : 'false');
    });
    if (elMsg && label) elMsg.textContent = label;
  }

  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  async function fetchJson(url, options) {
    const res = await fetch(url, options);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const err = data && data.error ? data.error : `Request failed (${res.status})`;
      throw new Error(err);
    }
    return data;
  }

  async function pollJob(jobId) {
    const u = jobStatusUrl(jobId);
    if (!u) return;
    let delay = 600;
    for (let i = 0; i < 240; i += 1) {
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(3200, Math.floor(delay * 1.15));
      try {
        const st = await fetchJson(u);
        const j = st && st.job ? st.job : st;
        const p = Number(j.progress || 0);
        const msg = j.message || j.status || '';
        if (elMsg) elMsg.textContent = `${p}% — ${msg}`;
        if (String(j.status || '') === 'completed' || String(j.status || '') === 'failed' || String(j.status || '') === 'cancelled') {
          return;
        }
      } catch (e) {
        if (elMsg) elMsg.textContent = String(e.message || e);
        return;
      }
    }
  }

  async function startSync(mode) {
    if (!syncUrl) return;
    setBusy(true, 'Queueing supplier sync…');
    try {
      const data = await fetchJson(syncUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode }),
      });
      const jobId = data.job_id;
      if (elStatus) elStatus.textContent = 'Running';
      if (elMsg) elMsg.textContent = 'Job started — tracking progress…';
      if (jobId) await pollJob(jobId);
      await loadRuns();
      await loadRegistry();
      if (elStatus) elStatus.textContent = 'Idle';
    } catch (e) {
      if (elMsg) elMsg.textContent = String(e.message || e);
      if (elStatus) elStatus.textContent = 'Error';
    } finally {
      setBusy(false, '');
    }
  }

  async function loadRuns() {
    if (!runsUrl || !elRuns) return;
    try {
      const data = await fetchJson(`${runsUrl}?limit=20`);
      const rows = data.runs || [];
      elRuns.innerHTML = rows
        .map(
          (r) => `<tr>
          <td>${esc(r.started_at)}</td>
          <td>${esc(r.sync_mode)}</td>
          <td><span class="badge bg-secondary supplier-mgmt-badge">${esc(r.status)}</span></td>
          <td class="text-end">${esc(r.rows_fetched)}</td>
          <td class="text-end">${esc(r.rows_upserted)}</td>
          <td class="text-end">${esc(r.rows_skipped)}</td>
        </tr>`
        )
        .join('');
      if (!rows.length) elRuns.innerHTML = '<tr><td colspan="6" class="text-muted">No sync runs yet.</td></tr>';
    } catch (e) {
      elRuns.innerHTML = `<tr><td colspan="6" class="text-danger">${esc(e.message || e)}</td></tr>`;
    }
  }

  async function loadCountries() {
    if (!countriesUrl || !elCountry) return;
    try {
      const data = await fetchJson(countriesUrl);
      const list = data.countries || [];
      const cur = elCountry.value;
      elCountry.innerHTML =
        '<option value="">All countries</option>' +
        list.map((c) => `<option value="${esc(c)}">${esc(c)}</option>`).join('');
      elCountry.value = cur;
    } catch {
      /* optional */
    }
  }

  function queryParams() {
    const p = new URLSearchParams();
    p.set('page', elPage && elPage.value ? elPage.value : '1');
    p.set('page_size', '25');
    if (elQ && elQ.value) p.set('q', elQ.value);
    if (elCountry && elCountry.value) p.set('country', elCountry.value);
    if (elActive && elActive.value) p.set('active', elActive.value);
    if (elSort && elSort.value) p.set('sort', elSort.value);
    if (elOrder && elOrder.value) p.set('order', elOrder.value);
    return p.toString();
  }

  async function loadRegistry() {
    if (!registryUrl || !elTableBody) return;
    elTableBody.innerHTML = '<tr><td colspan="8" class="text-muted">Loading…</td></tr>';
    try {
      const data = await fetchJson(`${registryUrl}?${queryParams()}`);
      const rows = data.rows || [];
      const total = Number(data.total || 0);
      const page = Number(data.page || 1);
      const per = Number(data.page_size || 25);
      if (elPageInfo) elPageInfo.textContent = `Page ${page} — ${total} supplier(s)`;
      if (elPage) elPage.value = String(page);
      elTableBody.innerHTML = rows
        .map((r) => {
          const act = r.active ? '<span class="badge bg-success supplier-mgmt-badge">Active</span>' : '<span class="badge bg-secondary supplier-mgmt-badge">Inactive</span>';
          const src = `<span class="badge text-bg-secondary supplier-mgmt-badge">${esc(r.source_system || '')}</span>`;
          const noteShort = esc((r.notes || '').slice(0, 80));
          return `<tr data-id="${esc(r.id)}">
            <td><strong>${esc(r.supplier_name)}</strong><div class="supplier-mgmt-muted text-truncate" style="max-width:280px">${noteShort}</div></td>
            <td>${src}</td>
            <td>${esc(r.country || '—')}</td>
            <td>${esc(r.last_synced_at || '—')}</td>
            <td class="text-end">${esc(r.usage_count)}</td>
            <td>${esc(r.supplier_type || '—')}</td>
            <td>${act}</td>
            <td class="supplier-mgmt-muted small">${esc(r.external_supplier_id || '').slice(0, 48)}${String(r.external_supplier_id || '').length > 48 ? '…' : ''}</td>
          </tr>`;
        })
        .join('');
      if (!rows.length) elTableBody.innerHTML = '<tr><td colspan="8" class="text-muted">No rows match filters.</td></tr>';
      const pg = Number((elPage && elPage.value) || page || 1) || 1;
      const maxPg = Math.max(1, Math.ceil(total / per));
      root.querySelectorAll('[data-action="page-prev"]').forEach((btn) => {
        btn.disabled = pg <= 1;
      });
      root.querySelectorAll('[data-action="page-next"]').forEach((btn) => {
        btn.disabled = pg >= maxPg || total === 0;
      });
    } catch (e) {
      elTableBody.innerHTML = `<tr><td colspan="8" class="text-danger">${esc(e.message || e)}</td></tr>`;
    }
  }

  if (btnFull) btnFull.addEventListener('click', () => startSync('full'));
  if (btnInc) btnInc.addEventListener('click', () => startSync('incremental'));
  if (btnRefresh) btnRefresh.addEventListener('click', () => loadRegistry());

  root.querySelectorAll('[data-action="page-prev"]').forEach((btn) =>
    btn.addEventListener('click', () => {
      if (!elPage) return;
      const pg = Number(elPage.value || '1');
      elPage.value = String(Math.max(1, pg - 1));
      loadRegistry();
    })
  );
  root.querySelectorAll('[data-action="page-next"]').forEach((btn) =>
    btn.addEventListener('click', () => {
      if (!elPage) return;
      const pg = Number(elPage.value || '1');
      elPage.value = String(pg + 1);
      loadRegistry();
    })
  );

  ['change', 'submit'].forEach((ev) =>
    root.addEventListener(
      ev,
      (e) => {
        if (e.target && e.target.closest && e.target.closest('#supplierFilterForm')) {
          if (ev === 'submit') e.preventDefault();
          elPage.value = '1';
          loadRegistry();
        }
      },
      true
    )
  );

  loadCountries();
  loadRuns();
  loadRegistry();
})();
