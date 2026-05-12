(function () {
  var bootEl = document.getElementById('engage-waste-boot');
  if (!bootEl) return;

  var CFG = {};
  try {
    CFG = JSON.parse(bootEl.textContent || '{}');
  } catch (e) {
    CFG = {};
  }

  function qs(id) {
    return document.getElementById(id);
  }

  function showErr(msg) {
    var el = qs('engageWasteFlash');
    if (!el) return;
    if (!msg) {
      el.classList.add('d-none');
      el.textContent = '';
      return;
    }
    el.textContent = msg;
    el.classList.remove('d-none');
  }

  function fmt(n) {
    if (n === undefined || n === null || n === '') return '—';
    return String(n);
  }

  async function fetchJson(url, opts) {
    var res = await fetch(
      url,
      Object.assign({ credentials: 'same-origin', headers: { Accept: 'application/json' } }, opts || {})
    );
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok) {
      throw new Error(data.detail || data.error || 'HTTP ' + res.status);
    }
    return data;
  }

  async function pollJob(jobId) {
    while (true) {
      var job = await fetchJson('/job-status/' + encodeURIComponent(jobId));
      var st = String(job.status || '');
      var prog = Math.max(0, Math.min(100, Number(job.progress || 0)));
      var bar = qs('engageWasteImportProgressBar');
      var msg = qs('engageWasteImportProgressMessage');
      if (bar) bar.style.width = prog + '%';
      if (msg) msg.textContent = job.message || job.status || '';
      var r = job.result || {};
      if (typeof r.rows_inserted === 'number' && qs('ewImpInserted')) {
        qs('ewImpInserted').textContent = fmt(r.rows_inserted);
      }
      if (typeof r.duplicates_skipped === 'number' && qs('ewImpDup')) {
        qs('ewImpDup').textContent = fmt(r.duplicates_skipped);
      }
      if (typeof r.fingerprint_duplicates_skipped === 'number' && qs('ewImpFpDup')) {
        qs('ewImpFpDup').textContent = fmt(r.fingerprint_duplicates_skipped);
      }
      if (typeof r.validation_skipped === 'number' && qs('ewImpValSkip')) {
        qs('ewImpValSkip').textContent = fmt(r.validation_skipped);
      }
      var ec = r.errors_count;
      if (ec === undefined || ec === null) {
        ec = Array.isArray(r.errors) ? r.errors.length : ec;
      }
      if (qs('ewImpErr')) qs('ewImpErr').textContent = fmt(ec);
      if (['completed', 'failed', 'cancelled'].indexOf(st) >= 0) {
        return job;
      }
      await new Promise(function (r2) {
        setTimeout(r2, 900);
      });
    }
  }

  async function findBlockingEngageJob() {
    try {
      var data = await fetchJson('/job-history');
      var jobs = data.jobs || [];
      return jobs.find(function (j) {
        return (
          String(j.type || '') === 'engage_waste_import' &&
          ['pending', 'running'].indexOf(String(j.status || '')) >= 0
        );
      });
    } catch (e) {
      return null;
    }
  }

  var lastPreviewId = null;
  var lastPreviewRows = [];
  var lastPreviewTruncated = false;

  function headerIndex(headers, name) {
    var h = headers || [];
    for (var i = 0; i < h.length; i++) {
      if (String(h[i]) === name) return i;
    }
    return -1;
  }

  function renderPreview(payload) {
    var tbody = qs('engageWastePreviewBody');
    var trunc = qs('engagePreviewTrunc');
    var pid = qs('engagePreviewId');
    var truncNotice = qs('engageTruncNotice');

    lastPreviewTruncated = !!payload.preview_truncated;
    if (truncNotice) truncNotice.classList.toggle('d-none', !lastPreviewTruncated);

    if (pid) pid.textContent = payload.preview_id || '—';
    if (trunc) trunc.classList.toggle('d-none', !payload.preview_truncated);

    var headers = payload.headers || [];
    var ixRp = headerIndex(headers, 'Reporting period (month, year)');
    var ixWs = headerIndex(headers, 'Waste Stream');
    var ixW = headerIndex(headers, 'Weight');
    var ixTag = headerIndex(headers, 'Site Tag');

    lastPreviewRows = Array.isArray(payload.preview_rows) ? payload.preview_rows : [];
    if (!tbody) return;

    tbody.innerHTML = '';
    if (!lastPreviewRows.length) {
      var empty = document.createElement('tr');
      empty.innerHTML =
        '<td colspan="6" class="text-muted small p-3">No preview rows (API returned nothing or filters excluded weight).</td>';
      tbody.appendChild(empty);
      var selAll0 = qs('engageSelectAll');
      if (selAll0) {
        selAll0.disabled = false;
        selAll0.checked = false;
      }
      syncImportBtn();
      return;
    }

    lastPreviewRows.forEach(function (row, idx) {
      var cells = row.cells || [];
      var pv = row.preview || {};
      var tr = document.createElement('tr');
      var notes = pv.unmapped_site_tag ? 'Unmapped site tag' : '';
      var rp = ixRp >= 0 ? cells[ixRp] || '' : pv.reporting_period || '';
      var ws = ixWs >= 0 ? cells[ixWs] || '' : pv.waste_stream_en || '';
      var w = ixW >= 0 ? cells[ixW] || '' : '';
      var tag = ixTag >= 0 ? cells[ixTag] || '' : '';
      var bundleIdx = typeof row.bundle_row_index === 'number' ? row.bundle_row_index : idx;
      var cbDisabled = lastPreviewTruncated ? ' disabled' : '';
      var checkedAttr = lastPreviewTruncated ? '' : ' checked';
      tr.innerHTML =
        '<td class="text-center"><input type="checkbox" class="engage-row-cb" data-bundle-idx="' +
        bundleIdx +
        '"' +
        checkedAttr +
        cbDisabled +
        ' aria-label="Select row"></td>' +
        '<td>' +
        escapeHtml(rp) +
        '</td>' +
        '<td>' +
        escapeHtml(ws) +
        '</td>' +
        '<td class="text-end">' +
        escapeHtml(w) +
        '</td>' +
        '<td>' +
        escapeHtml(tag) +
        '</td>' +
        '<td class="small text-muted">' +
        escapeHtml(notes) +
        '</td>';
      tbody.appendChild(tr);
    });

    wireSelectAll();
    var selAll = qs('engageSelectAll');
    if (selAll) {
      selAll.disabled = lastPreviewTruncated;
      selAll.checked = !lastPreviewTruncated;
    }
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function wireSelectAll() {
    var all = qs('engageSelectAll');
    if (!all || lastPreviewTruncated) return;
    all.onchange = function () {
      document.querySelectorAll('.engage-row-cb').forEach(function (cb) {
        cb.checked = all.checked;
      });
      syncImportBtn();
    };
    document.querySelectorAll('.engage-row-cb').forEach(function (cb) {
      cb.addEventListener('change', syncImportBtn);
    });
    syncImportBtn();
  }

  function syncImportBtn() {
    var btn = qs('engageWasteImportBtn');
    if (!btn) return;
    if (lastPreviewTruncated) {
      btn.disabled = !lastPreviewId || btn.dataset.blocked === '1';
      return;
    }
    var any =
      !!lastPreviewId &&
      Array.prototype.some.call(document.querySelectorAll('.engage-row-cb'), function (c) {
        return c.checked;
      });
    btn.disabled = !any || btn.dataset.blocked === '1';
  }

  function selectedIndices() {
    var out = [];
    document.querySelectorAll('.engage-row-cb').forEach(function (cb) {
      if (!cb.checked) return;
      var i = parseInt(cb.getAttribute('data-bundle-idx'), 10);
      if (!Number.isNaN(i)) out.push(i);
    });
    return out;
  }

  async function refreshBlockedImportBtn() {
    var btn = qs('engageWasteImportBtn');
    var badge = qs('engageWasteImportRunningBadge');
    if (!btn || !CFG.canImport) return;
    var blocking = await findBlockingEngageJob();
    if (blocking) {
      btn.dataset.blocked = '1';
      btn.title = 'An Engage Waste import is already running (' + (blocking.job_id || '') + ').';
      if (badge) badge.classList.remove('d-none');
    } else {
      btn.dataset.blocked = '0';
      btn.title = '';
      if (badge) badge.classList.add('d-none');
    }
    syncImportBtn();
  }

  var fetchBtn = qs('engageWasteFetchBtn');
  if (fetchBtn && CFG.fetchUrl && CFG.canImport) {
    fetchBtn.addEventListener('click', async function () {
      showErr('');
      var company = (qs('engageWasteCompany') && qs('engageWasteCompany').value) || '';
      if (!company) {
        showErr('Select a target company.');
        return;
      }
      var fb = (qs('engagePeriodFallback') && qs('engagePeriodFallback').value.trim()) || '';
      fetchBtn.disabled = true;
      var load = qs('engageWasteFetchLoading');
      if (load) load.classList.remove('d-none');
      try {
        var data = await fetchJson(CFG.fetchUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            company_name: company,
            reporting_period_fallback: fb,
          }),
        });
        lastPreviewId = data.preview_id || null;
        var cts = data.counts || {};
        if (qs('ewStatRaw')) qs('ewStatRaw').textContent = fmt(cts.raw_rows);
        if (qs('ewStatReady')) qs('ewStatReady').textContent = fmt(cts.ready_rows);
        if (qs('ewStatUnmapped')) qs('ewStatUnmapped').textContent = fmt(cts.unmapped_site_tags);
        renderPreview(data);
      } catch (e) {
        showErr(e.message || String(e));
      } finally {
        fetchBtn.disabled = false;
        if (load) load.classList.add('d-none');
      }
      refreshBlockedImportBtn();
    });
  }

  var impBtn = qs('engageWasteImportBtn');
  if (impBtn && CFG.importUrl && CFG.canImport) {
    impBtn.addEventListener('click', async function () {
      await refreshBlockedImportBtn();
      if (impBtn.dataset.blocked === '1') {
        window.alert('An Engage Waste import is already running. Wait for it to finish.');
        return;
      }
      if (!lastPreviewId) {
        showErr('Fetch data before importing.');
        return;
      }

      var body = { preview_id: lastPreviewId };
      if (!lastPreviewTruncated) {
        var picked = selectedIndices();
        if (!picked.length) {
          showErr('Select at least one preview row.');
          return;
        }
        body.row_indices = picked;
      }

      showErr('');
      impBtn.disabled = true;
      var prog = qs('engageWasteImportProgress');
      var badge = qs('engageWasteImportRunningBadge');
      if (prog) prog.classList.remove('d-none');
      if (badge) badge.classList.remove('d-none');
      impBtn.dataset.blocked = '1';
      try {
        var started = await fetchJson(CFG.importUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        var jobId = started.job_id;
        if (!jobId) throw new Error('No job_id returned');
        var done = await pollJob(jobId);
        var st = String(done.status || '');
        if (st === 'failed') {
          showErr(done.error || 'Import failed.');
        }
      } catch (e) {
        showErr(e.message || String(e));
      } finally {
        impBtn.disabled = false;
        impBtn.dataset.blocked = '0';
        if (badge) badge.classList.add('d-none');
        refreshBlockedImportBtn();
      }
    });
  }

  var stBtn = qs('engageWasteRefreshStatusBtn');
  if (stBtn && CFG.statusUrl) {
    stBtn.addEventListener('click', async function () {
      try {
        await fetchJson(CFG.statusUrl);
      } catch (e) {}
      window.location.reload();
    });
  }

  refreshBlockedImportBtn();
  if (window.CtsPerf) {
    window.CtsPerf.managePoll('engage-waste-blocked', refreshBlockedImportBtn, 26000, {
      pauseWhenHidden: true,
      runImmediate: false,
    });
  } else {
    setInterval(refreshBlockedImportBtn, 8000);
  }
})();
