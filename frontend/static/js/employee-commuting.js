/**
 * Employee Commuting hub — headcount CRUD via JSON APIs; national averages + importer (privileged).
 */
(function () {
  var bootEl = document.getElementById("ec-page-boot");
  if (!bootEl || !document.getElementById("ec-page-root")) {
    return;
  }

  var CFG = {};
  try {
    CFG = JSON.parse(bootEl.textContent || "{}");
  } catch (e) {
    CFG = {};
  }

  var privileged = !!CFG.privileged;
  var scopedCompany = String(CFG.scoped_company_name || "");
  var displayCategory = String(
    CFG.page_title || CFG.display_category || "Employee Commuting"
  );
  var months = Array.isArray(CFG.reporting_months) ? CFG.reporting_months : [];
  var companies = Array.isArray(CFG.company_options) ? CFG.company_options : [];
  var apiNational = String(CFG.api_national || "").trim();
  var apiGenerate = String(CFG.api_generate || "").trim();
  var apiRuns = String(CFG.api_runs || "").trim();

  function qs(id) {
    return document.getElementById(id);
  }

  function flashErr(msg) {
    var el = qs("ecFlash");
    var ok = qs("ecNotice");
    if (ok) {
      ok.classList.add("d-none");
      ok.textContent = "";
    }
    if (!el) {
      return;
    }
    el.textContent = msg || "";
    el.classList.toggle("d-none", !msg);
  }

  function flashOk(msg) {
    var el = qs("ecNotice");
    var bad = qs("ecFlash");
    if (bad) {
      bad.classList.add("d-none");
      bad.textContent = "";
    }
    if (!el) {
      return;
    }
    el.textContent = msg || "";
    el.classList.toggle("d-none", !msg);
  }

  async function fetchJson(url, opts) {
    var res = await fetch(
      url,
      Object.assign({ credentials: "same-origin", headers: { Accept: "application/json" } }, opts || {})
    );
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok) {
      throw new Error(data.detail || data.error || "HTTP " + res.status);
    }
    return data;
  }

  function activateTabFromHash() {
    var h = (location.hash || "").replace(/^#/, "").toLowerCase();
    var map = {
      headcount: "ec-tab-headcount",
      national: "ec-tab-national",
      generate: "ec-tab-generate",
      dataset: "ec-tab-generate",
      history: "ec-tab-history",
    };
    var btnId = map[h];
    if (!btnId) {
      return;
    }
    var btn = document.getElementById(btnId);
    if (!btn || typeof bootstrap === "undefined" || !bootstrap.Tab) {
      return;
    }
    try {
      bootstrap.Tab.getOrCreateInstance(btn).show();
    } catch (e) {
      /* ignore */
    }
  }

  window.addEventListener("hashchange", activateTabFromHash);

  function fillMonthSelect(selectEl, current) {
    if (!selectEl) {
      return;
    }
    selectEl.innerHTML = "";
    var cur = String(current || "");
    months.forEach(function (m) {
      var ms = String(m);
      var o = document.createElement("option");
      o.value = ms;
      o.textContent = ms;
      selectEl.appendChild(o);
    });
    if (!months.length && cur) {
      var o = document.createElement("option");
      o.value = cur;
      o.textContent = cur;
      selectEl.appendChild(o);
      selectEl.value = cur;
      return;
    }
    selectEl.value = cur && months.indexOf(cur) !== -1 ? cur : months[0] || "";
    if (!selectEl.value && cur) {
      var o2 = document.createElement("option");
      o2.value = cur;
      o2.textContent = cur;
      selectEl.appendChild(o2);
      selectEl.value = cur;
    }
  }

  function fillCompanySelect(selectEl, current) {
    if (!selectEl) {
      return;
    }
    selectEl.innerHTML = "";
    var cur = String(current || "").trim();
    companies.forEach(function (c) {
      var lbl = typeof c.label === "string" ? c.label : c.key;
      var ky = typeof c.key === "string" ? c.key : lbl;
      var o = document.createElement("option");
      o.value = ky || lbl || "";
      o.textContent = lbl || ky || "";
      selectEl.appendChild(o);
    });
    selectEl.value = cur;
    if (!selectEl.value && companies[0]) {
      selectEl.selectedIndex = 0;
    }
  }

  function headcountStateFromSeed() {
    var rows = Array.isArray(CFG.headcount_seed) ? CFG.headcount_seed : [];
    return rows.map(function (r) {
      return {
        company_name: String(r.company_name || (privileged ? "" : scopedCompany)),
        month: String(r.month || ""),
        headcount: Math.max(0, parseInt(String(r.headcount || 0), 10) || 0),
      };
    });
  }

  var headRows = headcountStateFromSeed();
  var natRows = Array.isArray(CFG.national_seed) ? CFG.national_seed.slice() : [];

  function rerenderHeadcount() {
    var body = qs("ecHeadcountBody");
    var tplPrivileged = qs("ecHeadcountPrivilegedRowTpl");
    var tplUser = qs("ecHeadcountUserRowTpl");
    if (!body || (!tplPrivileged && !tplUser)) {
      return;
    }
    body.innerHTML = "";
    headRows.forEach(function (row, idx) {
      var tmpl = privileged ? tplPrivileged.content : tplUser.content;
      var clone = tmpl.cloneNode(true);
      body.appendChild(clone);
      var tr = body.lastElementChild;

      var del = tr.querySelector(".ec-row-del");
      if (del) {
        del.addEventListener("click", function () {
          headRows.splice(idx, 1);
          rerenderHeadcount();
        });
      }

      if (privileged) {
        fillMonthSelect(tr.querySelector(".ec-month-select"), row.month || months[0] || "");
        fillCompanySelect(tr.querySelector(".ec-company-select"), row.company_name);
        var numEl = tr.querySelector(".ec-hc-num");
        if (numEl) {
          numEl.value = String(row.headcount);
        }
        var msPriv = tr.querySelector(".ec-month-select");
        if (msPriv) {
          msPriv.addEventListener("change", function () {
            row.month = String(this.value);
          });
        }
        var coSel = tr.querySelector(".ec-company-select");
        if (coSel) {
          coSel.addEventListener("change", function () {
            row.company_name = String(this.value);
          });
        }
        if (numEl) {
          numEl.addEventListener("change", function () {
            row.headcount = Math.max(0, parseInt(String(this.value || 0), 10) || 0);
          });
        }
      } else {
        var rd = tr.querySelector(".ec-comp-readonly");
        if (rd) {
          rd.textContent = scopedCompany || "—";
        }
        fillMonthSelect(tr.querySelector(".ec-month-select"), row.month || months[0] || "");
        var numNu = tr.querySelector(".ec-hc-num");
        if (numNu) {
          numNu.value = String(row.headcount);
        }
        var msEl = tr.querySelector(".ec-month-select");
        if (msEl) {
          msEl.addEventListener("change", function () {
            row.month = String(this.value);
          });
        }
        if (numNu) {
          numNu.addEventListener("change", function () {
            row.headcount = Math.max(0, parseInt(String(this.value || 0), 10) || 0);
          });
        }
        row.company_name = scopedCompany;
      }
    });
  }

  function collectHeadcountPayload() {
    var out = [];
    headRows.forEach(function (r) {
      var co = privileged ? String(r.company_name || "").trim() : String(scopedCompany || "").trim();
      var mo = String(r.month || "").trim();
      var hc = Math.max(0, parseInt(String(r.headcount || 0), 10) || 0);
      if (!mo && !hc && !co) {
        return;
      }
      if (!co || !mo) {
        throw new Error("Each headcount row needs company and month.");
      }
      out.push({
        company_name: co,
        month: mo,
        headcount: hc,
      });
    });
    return out;
  }

  var addBtn = qs("ecHeadcountAddBtn");
  if (addBtn) {
    addBtn.addEventListener("click", function () {
      headRows.push({
        company_name:
          privileged && companies[0] ? String(companies[0].key || companies[0].label || "") : scopedCompany,
        month: months[0] || "",
        headcount: 0,
      });
      rerenderHeadcount();
    });
  }

  var saveHc = qs("ecHeadcountSaveBtn");
  if (saveHc) {
    saveHc.addEventListener("click", async function () {
      flashErr("");
      flashOk("");
      var busy = qs("ecHeadcountBusy");
      if (busy) {
        busy.classList.remove("d-none");
      }
      try {
        var rowsPayload = collectHeadcountPayload();
        var out = await fetchJson(CFG.api_headcount, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rows: rowsPayload }),
        });
        flashOk(
          "Saved ".concat(typeof out.saved_rows === "number" ? out.saved_rows : rowsPayload.length, " row(s).")
        );
        if (Array.isArray(out.rows)) {
          CFG.headcount_seed = out.rows;
          headRows = headcountStateFromSeed();
          rerenderHeadcount();
        }
      } catch (e) {
        flashErr(String((e && e.message) || e || "Save failed."));
      }
      if (busy) {
        busy.classList.add("d-none");
      }
    });
  }

  function natKeys() {
    return ["country", "average_one_day", "car_pct", "bus_pct", "walking_and_cycling_pct", "mixed_pct"];
  }

  function rerenderNational() {
    var tpl = qs("ecNationalRowTpl");
    var body = qs("ecNationalBody");
    if (!privileged || !tpl || !body || !apiNational) {
      return;
    }
    body.innerHTML = "";
    natRows.forEach(function (row, idx) {
      var clone = tpl.content.cloneNode(true);
      body.appendChild(clone);
      var tr = body.lastElementChild;
      var del = tr.querySelector(".ec-row-del");
      if (del) {
        del.addEventListener("click", function () {
          natRows.splice(idx, 1);
          rerenderNational();
        });
      }
      natKeys().forEach(function (k) {
        var inp = tr.querySelector(".ec-na-" + k);
        if (!inp) {
          return;
        }
        inp.value = row[k] === undefined ? "" : String(row[k]);
        inp.addEventListener("change", function () {
          row[k] = inp.value;
        });
      });
    });
  }

  if (privileged && apiNational) {
    var nab = qs("ecNationalAddBtn");
    if (nab) {
      nab.addEventListener("click", function () {
        natRows.push({
          country: "",
          average_one_day: "",
          car_pct: "",
          bus_pct: "",
          walking_and_cycling_pct: "",
          mixed_pct: "",
        });
        rerenderNational();
      });
    }
    var nsb = qs("ecNationalSaveBtn");
    if (nsb) {
      nsb.addEventListener("click", async function () {
        flashErr("");
        flashOk("");
        var busy = qs("ecNationalBusy");
        if (busy) {
          busy.classList.remove("d-none");
        }
        try {
          var normalized = natRows.slice();
          await fetchJson(apiNational, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ rows: normalized }),
          });
          flashOk("National averages saved.");
          var fresh = await fetchJson(apiNational).catch(function () {
            return null;
          });
          if (fresh && fresh.rows) {
            natRows = fresh.rows.slice();
            rerenderNational();
          }
        } catch (e) {
          flashErr(String((e && e.message) || e || "Save failed."));
        }
        if (busy) {
          busy.classList.add("d-none");
        }
      });
    }
  }

  function fmtStatNum(st, k) {
    if (!st || typeof st !== "object") {
      return "—";
    }
    var v = st[k];
    if (v === undefined || v === null) {
      return "—";
    }
    return String(v);
  }

  function rerenderRuns(list) {
    var body = qs("ecRunsBody");
    if (!body) {
      return;
    }
    body.innerHTML = "";
    (list || []).forEach(function (r) {
      var tr = document.createElement("tr");
      var st = r.stats || {};
      tr.innerHTML =
        '<td class="ec-mono text-body-secondary">' +
        String(r.id || "") +
        "</td>" +
        '<td><span class="ec-mono ec-mono--job">' +
        String(r.job_id || "") +
        "</span></td>" +
        '<td><span class="ec-status">' +
        String(r.status || "") +
        "</span></td>" +
        '<td class="text-end fw-medium">' +
        fmtStatNum(st, "saved_rows_count") +
        '</td><td class="text-end">' +
        fmtStatNum(st, "duplicates_skipped") +
        '</td><td class="text-end">' +
        fmtStatNum(st, "mapping_jobs_queued") +
        '</td><td class="text-muted small">' +
        String(r.created_at || "") +
        '</td><td class="text-muted small">' +
        String(r.completed_at || "") +
        "</td>";
      body.appendChild(tr);
    });
  }

  async function pollJob(jobId) {
    function statusUrl(id) {
      return String(CFG.job_status_tpl || "").replace("__JOB_ID__", String(id));
    }

    while (true) {
      var job = await fetchJson(statusUrl(jobId));
      var prog = qs("ecJobProgressBar");
      var msg = qs("ecJobProgressMessage");
      var wrap = qs("ecJobProgressWrap");
      if (prog) {
        prog.style.width = Math.max(0, Math.min(100, Number(job.progress || 0))) + "%";
      }
      if (wrap) {
        wrap.classList.remove("d-none");
      }
      if (msg) {
        msg.textContent = String(job.message || job.status || "");
      }
      var st = String(job.status || "");
      if (["completed", "failed", "cancelled"].indexOf(st) >= 0) {
        var lab = qs("ecGenerateJobLabel");
        if (lab) {
          lab.textContent = st === "completed" ? "Generation finished." : "Job " + st + ".";
        }
        if (st !== "completed" && msg) {
          msg.textContent += job.error ? " — ".concat(job.error) : "";
        }
        return job;
      }
      await new Promise(function (r) {
        setTimeout(r, 900);
      });
    }
  }

  if (privileged && apiGenerate) {
    var genBtn = qs("ecGenerateBtn");
    if (genBtn) {
      genBtn.addEventListener("click", async function () {
        flashErr("");
        flashOk("");
        var btn = genBtn;
        var tplSel = qs("ecTemplateModeSelect");
        try {
          btn.disabled = true;
          var res = await fetchJson(apiGenerate, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              template_mode: tplSel && tplSel.value ? tplSel.value : CFG.template_mode,
            }),
          });
          var jobId = String(res.job_id || "");
          var gl = qs("ecGenerateJobLabel");
          if (gl) {
            gl.textContent = "Job ".concat(jobId, " …");
          }
          var job = await pollJob(jobId);
          if (String(job.status) === "completed") {
            var rslt = job.result || {};
            var saved =
              rslt.saved_rows_count != null ? String(rslt.saved_rows_count) : "0";
            var dups =
              typeof rslt.duplicates_skipped === "number" ? String(rslt.duplicates_skipped) : "—";
            var valSkip =
              typeof rslt.validation_skipped === "number" ? String(rslt.validation_skipped) : "—";
            var sched =
              rslt.mapping_targets_scheduled != null
                ? String(rslt.mapping_targets_scheduled)
                : typeof rslt.mapping_jobs_queued === "number"
                  ? String(rslt.mapping_jobs_queued)
                  : "—";
            var parts = [
              displayCategory + ": created " + saved + " new Data Entry row(s).",
              typeof rslt.duplicates_skipped === "number"
                ? "Duplicates skipped (existing dedup keys): " + dups + "."
                : "",
              typeof rslt.validation_skipped === "number"
                ? "Rows skipped during validation/persist: " + valSkip + "."
                : "",
              (typeof rslt.mapping_targets_scheduled === "number" ||
                typeof rslt.mapping_jobs_queued === "number")
                ? "Sequential mapping batch queued for " + sched + " company/sheet target(s)."
                : "",
            ];
            if (rslt.user_mapping_notice) {
              parts.push(String(rslt.user_mapping_notice));
            } else if (rslt.mapping_queue_partial) {
              parts.push(
                "Dataset generated successfully. Some mapping jobs were skipped or delayed."
              );
            }
            flashOk(parts.filter(Boolean).join(" "));
          } else if (job.error) {
            flashErr(String(job.error || "Generation failed."));
          }
          if (apiRuns) {
            try {
              var runs = await fetchJson(apiRuns);
              rerenderRuns(runs.runs || []);
            } catch (eRun) {}
          }
        } catch (e) {
          flashErr(String((e && e.message) || e || "Could not enqueue job."));
        }
        var wrap = qs("ecJobProgressWrap");
        if (wrap) {
          wrap.classList.add("d-none");
        }
        btn.disabled = false;
      });
    }
  }

  rerenderHeadcount();
  if (privileged && apiNational) {
    rerenderNational();
  }
  if (privileged && apiRuns) {
    rerenderRuns(CFG.runs_seed || []);
  }
  activateTabFromHash();
})();
