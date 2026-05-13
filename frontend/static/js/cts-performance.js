/**
 * Lightweight polling + visibility helpers (Flask SPA-style pages).
 * Not a framework — keeps intervals single-shot per logical name and skips work when hidden.
 */
(function () {
  if (typeof window === "undefined" || typeof document === "undefined") return;

  var pollers = {};
  var scriptPromises = {};
  var timings = window.__CTS_INIT_TIMINGS__ = window.__CTS_INIT_TIMINGS__ || [];

  function now() {
    return window.performance && typeof window.performance.now === "function"
      ? window.performance.now()
      : Date.now();
  }

  function recordInit(name, durationMs, detail) {
    var entry = {
      name: String(name || "init"),
      duration: Math.round(Number(durationMs || 0) * 10) / 10,
      detail: detail ? String(detail) : "",
      path: window.location.pathname,
      timestamp: new Date().toISOString(),
    };
    timings.push(entry);
    if (timings.length > 40) timings.splice(0, timings.length - 40);
    if (window.__CTS_FRONTEND_DEBUG__ && window.console && typeof window.console.debug === "function") {
      window.console.debug("[CTS perf]", entry.name, entry.duration + "ms", entry.detail || "");
    }
    return entry;
  }

  function measureInit(name, callback, detail) {
    var started = now();
    try {
      return callback();
    } finally {
      recordInit(name, now() - started, detail);
    }
  }

  function loadScriptOnce(key, src) {
    var normalizedKey = String(key || src || "").trim();
    var normalizedSrc = String(src || "").trim();
    if (!normalizedKey || !normalizedSrc) {
      return Promise.reject(new Error("Script source is required."));
    }
    if (scriptPromises[normalizedKey]) return scriptPromises[normalizedKey];
    scriptPromises[normalizedKey] = new Promise(function (resolve, reject) {
      var existing = document.querySelector('script[data-cts-script-key="' + normalizedKey.replace(/"/g, '\\"') + '"]');
      if (existing && existing.dataset.loaded === "true") {
        resolve(existing);
        return;
      }
      var script = existing || document.createElement("script");
      script.src = normalizedSrc;
      script.async = true;
      script.defer = true;
      script.dataset.ctsScriptKey = normalizedKey;
      script.addEventListener("load", function () {
        script.dataset.loaded = "true";
        resolve(script);
      }, { once: true });
      script.addEventListener("error", function () {
        delete scriptPromises[normalizedKey];
        reject(new Error("Failed to load " + normalizedSrc));
      }, { once: true });
      if (!existing) {
        document.head.appendChild(script);
      }
    });
    return scriptPromises[normalizedKey];
  }

  function stopPoll(key) {
    var p = pollers[key];
    if (!p) return;
    if (p.id != null) window.clearInterval(p.id);
    if (p.onVisibility) document.removeEventListener("visibilitychange", p.onVisibility);
    delete pollers[key];
  }

  /**
   * @param {string} key
   * @param {() => void} callback
   * @param {number} intervalMs
   * @param {{ pauseWhenHidden?: boolean; runImmediate?: boolean }} options
   */
  function managePoll(key, callback, intervalMs, options) {
    options = options || {};
    var pauseWhenHidden = options.pauseWhenHidden !== false;
    stopPoll(key);

    function tick() {
      try {
        if (pauseWhenHidden && document.visibilityState === "hidden") return;
        callback();
      } catch (_e) {
        /* keep interval alive */
      }
    }

    var id = window.setInterval(tick, intervalMs);

    function onVisibility() {
      if (!pauseWhenHidden) return;
      if (document.visibilityState === "visible") tick();
    }

    document.addEventListener("visibilitychange", onVisibility);
    pollers[key] = { id: id, onVisibility: onVisibility };

    if (options.runImmediate) tick();

    return { cancel: function () { stopPoll(key); } };
  }

  window.CtsPerf = {
    getInitTimings: function () {
      return timings.slice();
    },
    isDocumentVisible: function () {
      return document.visibilityState !== "hidden";
    },
    loadScriptOnce: loadScriptOnce,
    managePoll: managePoll,
    measureInit: measureInit,
    recordInit: recordInit,
    stopPoll: stopPoll,
  };
})();
