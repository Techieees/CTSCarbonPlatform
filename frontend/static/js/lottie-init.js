/**
 * Centralized Lottie initialization for .lottie-icon[data-animation].
 * Instances are created lazily, play only while visible, and are destroyed when
 * their DOM node is removed.
 */
(function () {
  'use strict';

  var SELECTOR = '.lottie-icon[data-animation]';
  var LOTTIE_SRC = window.__CTS_LOTTIE_WEB_URL__ || 'https://cdn.jsdelivr.net/npm/lottie-web@5.12.2/build/player/lottie.min.js';
  var registry = new WeakMap();
  var active = [];
  var reducedMotionQuery = window.matchMedia
    ? window.matchMedia('(prefers-reduced-motion: reduce)')
    : null;
  var scanTimer = 0;
  var lottieLoadPromise = null;
  var booted = false;

  function getLottie() {
    return window.lottie && typeof window.lottie.loadAnimation === 'function'
      ? window.lottie
      : null;
  }

  function recordInit(name, started, detail) {
    var duration = 0;
    if (window.performance && typeof window.performance.now === 'function') {
      duration = window.performance.now() - started;
    }
    if (window.CtsPerf && typeof window.CtsPerf.recordInit === 'function') {
      window.CtsPerf.recordInit(name, duration, detail);
    } else {
      window.__CTS_INIT_TIMINGS__ = window.__CTS_INIT_TIMINGS__ || [];
      window.__CTS_INIT_TIMINGS__.push({
        name: name,
        duration: Math.round(duration * 10) / 10,
        detail: detail || '',
        path: window.location.pathname,
        timestamp: new Date().toISOString(),
      });
    }
  }

  function loadLottie() {
    var existing = getLottie();
    if (existing) return Promise.resolve(existing);
    if (lottieLoadPromise) return lottieLoadPromise;
    var started = window.performance && typeof window.performance.now === 'function'
      ? window.performance.now()
      : 0;
    if (window.CtsPerf && typeof window.CtsPerf.loadScriptOnce === 'function') {
      lottieLoadPromise = window.CtsPerf.loadScriptOnce('lottie-web', LOTTIE_SRC)
        .then(function () {
          recordInit('lottie library load', started, 'lazy');
          return getLottie();
        });
      return lottieLoadPromise;
    }
    lottieLoadPromise = new Promise(function (resolve, reject) {
      var script = document.createElement('script');
      script.src = LOTTIE_SRC;
      script.async = true;
      script.defer = true;
      script.addEventListener('load', function () {
        recordInit('lottie library load', started, 'lazy');
        resolve(getLottie());
      }, { once: true });
      script.addEventListener('error', reject, { once: true });
      document.head.appendChild(script);
    });
    return lottieLoadPromise;
  }

  function prefersReducedMotion() {
    return Boolean(reducedMotionQuery && reducedMotionQuery.matches);
  }

  function isSidebarIcon(el) {
    return Boolean(el && el.closest && el.closest('.app-sidebar'));
  }

  function shouldAutoPlay(el) {
    if (!el || document.hidden || prefersReducedMotion()) return false;
    if (String(el.getAttribute('data-lottie-autoplay') || '').trim() === 'false') return false;
    return !isSidebarIcon(el);
  }

  function getEntry(el) {
    var entry = registry.get(el);
    if (entry) return entry;
    entry = {
      anim: null,
      isVisible: false,
      isLoaded: false,
    };
    registry.set(el, entry);
    active.push(el);
    return entry;
  }

  function cleanupActiveList() {
    var next = [];
    for (var i = 0; i < active.length; i++) {
      var el = active[i];
      if (el && document.documentElement.contains(el) && registry.has(el)) {
        next.push(el);
      }
    }
    active = next;
  }

  function pauseEntry(entry) {
    if (!entry || !entry.anim) return;
    try {
      entry.anim.pause();
    } catch (e) {}
  }

  function stopEntry(entry) {
    if (!entry || !entry.anim) return;
    try {
      entry.anim.goToAndStop(0, true);
    } catch (e) {
      pauseEntry(entry);
    }
  }

  function ensureAnimation(el) {
    var entry = getEntry(el);
    if (entry.isLoaded) return entry;

    var L = getLottie();
    var path = el.getAttribute('data-animation');
    if (!path) return entry;
    if (!L) {
      loadLottie().then(function () {
        syncPlayback(el);
      }).catch(function () {});
      return entry;
    }

    var loopAttr = el.getAttribute('data-loop');
    var loop = loopAttr !== 'false';

    try {
      entry.anim = L.loadAnimation({
        container: el,
        renderer: 'svg',
        loop: loop,
        autoplay: false,
        path: path,
        rendererSettings: {
          preserveAspectRatio: 'xMidYMid meet',
          clearCanvas: true,
          progressiveLoad: true,
        },
      });
      entry.isLoaded = true;
      el.setAttribute('data-lottie-ready', '1');
    } catch (e) {
      return entry;
    }

    var spdRaw = el.getAttribute('data-lottie-speed');
    if (spdRaw) {
      var spd = parseFloat(spdRaw, 10);
      if (!isNaN(spd) && spd > 0) {
        try {
          entry.anim.setSpeed(spd);
        } catch (e2) {}
      }
    }

    if (prefersReducedMotion() || isSidebarIcon(el)) {
      stopEntry(entry);
    }

    return entry;
  }

  function syncPlayback(el) {
    var entry = getEntry(el);
    if (!entry.isVisible) {
      pauseEntry(entry);
      return;
    }
    if (isSidebarIcon(el) && !entry.isLoaded) {
      return;
    }
    entry = ensureAnimation(el);
    if (!entry.anim) return;
    if (shouldAutoPlay(el)) {
      try {
        entry.anim.play();
      } catch (e) {}
    } else {
      stopEntry(entry);
    }
  }

  function destroyEntry(el) {
    if (!el || el.nodeType !== 1) return;
    var entry = registry.get(el);
    if (!entry) return;
    try {
      if (entry.anim) entry.anim.destroy();
    } catch (e) {}
    registry.delete(el);
    el.removeAttribute('data-lottie-ready');
  }

  var io = window.IntersectionObserver
    ? new IntersectionObserver(
        function (entries) {
          for (var i = 0; i < entries.length; i++) {
            var en = entries[i];
            var el = en.target;
            var entry = getEntry(el);
            entry.isVisible = Boolean(en.isIntersecting && en.intersectionRatio > 0);
            syncPlayback(el);
          }
          cleanupActiveList();
        },
        { root: null, rootMargin: '96px 0px', threshold: [0, 0.01] }
      )
    : null;

  function observe(el) {
    if (!el || el.nodeType !== 1 || registry.has(el)) return;
    getEntry(el);
    if (io) {
      io.observe(el);
    } else {
      var entry = getEntry(el);
      entry.isVisible = true;
      syncPlayback(el);
    }
  }

  function scan(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var list = scope.querySelectorAll(SELECTOR);
    for (var i = 0; i < list.length; i++) {
      observe(list[i]);
    }
  }

  function queueScan(root) {
    if (scanTimer) return;
    var run = function () {
      scanTimer = 0;
      scan(root || document);
    };
    if (window.requestIdleCallback) {
      scanTimer = window.requestIdleCallback(run, { timeout: 700 });
    } else {
      scanTimer = window.setTimeout(run, 50);
    }
  }

  function collectLottieRoots(node, out) {
    if (!node || node.nodeType !== 1) return;
    if (node.matches && node.matches(SELECTOR)) {
      out.push(node);
    }
    if (!node.querySelectorAll) return;
    var nested = node.querySelectorAll(SELECTOR);
    for (var i = 0; i < nested.length; i++) out.push(nested[i]);
  }

  var mo = window.MutationObserver
    ? new MutationObserver(function (mutations) {
        var added = [];
        var removed = [];
        for (var i = 0; i < mutations.length; i++) {
          var m = mutations[i];
          var j;
          for (j = 0; j < m.addedNodes.length; j++) {
            collectLottieRoots(m.addedNodes[j], added);
          }
          for (j = 0; j < m.removedNodes.length; j++) {
            collectLottieRoots(m.removedNodes[j], removed);
          }
        }
        for (var r = 0; r < removed.length; r++) {
          if (io) {
            try {
              io.unobserve(removed[r]);
            } catch (e) {}
          }
          destroyEntry(removed[r]);
        }
        for (var a = 0; a < added.length; a++) {
          observe(added[a]);
        }
        cleanupActiveList();
      })
    : null;

  function syncAllPlayback() {
    cleanupActiveList();
    for (var i = 0; i < active.length; i++) {
      syncPlayback(active[i]);
    }
  }

  function playSidebarIcon(el) {
    if (!el || !isSidebarIcon(el) || document.hidden || prefersReducedMotion()) return;
    var entry = ensureAnimation(el);
    if (!entry.anim) {
      loadLottie().then(function () {
        playSidebarIcon(el);
      }).catch(function () {});
      return;
    }
    if (!entry.isVisible) return;
    try {
      entry.anim.play();
    } catch (e) {}
  }

  function stopSidebarIcon(el) {
    if (!el || !isSidebarIcon(el)) return;
    stopEntry(registry.get(el));
  }

  function sidebarIconFromEvent(event) {
    return event.target && event.target.closest ? event.target.closest('.app-sidebar ' + SELECTOR) : null;
  }

  function boot() {
    if (booted) return;
    booted = true;
    var started = window.performance && typeof window.performance.now === 'function'
      ? window.performance.now()
      : 0;
    queueScan(document);
    if (mo) {
      mo.observe(document.documentElement, { childList: true, subtree: true });
    }
    document.addEventListener('visibilitychange', syncAllPlayback);
    document.addEventListener('pointerenter', function (event) {
      playSidebarIcon(sidebarIconFromEvent(event));
    }, true);
    document.addEventListener('pointerleave', function (event) {
      stopSidebarIcon(sidebarIconFromEvent(event));
    }, true);
    document.addEventListener('focusin', function (event) {
      playSidebarIcon(sidebarIconFromEvent(event));
    });
    document.addEventListener('focusout', function (event) {
      stopSidebarIcon(sidebarIconFromEvent(event));
    });
    if (reducedMotionQuery && typeof reducedMotionQuery.addEventListener === 'function') {
      reducedMotionQuery.addEventListener('change', syncAllPlayback);
    }
    window.CtsLottie = {
      scan: function (root) {
        queueScan(root || document);
      },
      sync: syncAllPlayback,
    };
    recordInit('lottie init', started, document.querySelector(SELECTOR) ? 'icons present' : 'no initial icons');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
})();
