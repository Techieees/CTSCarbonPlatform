/**
 * Lightweight polling + visibility helpers (Flask SPA-style pages).
 * Not a framework — keeps intervals single-shot per logical name and skips work when hidden.
 */
(function () {
  if (typeof window === "undefined" || typeof document === "undefined") return;

  var pollers = {};

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
    isDocumentVisible: function () {
      return document.visibilityState !== "hidden";
    },
    managePoll: managePoll,
    stopPoll: stopPoll,
  };
})();
