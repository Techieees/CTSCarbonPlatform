/**
 * Centralized Lottie (SVG) initialization for .lottie-icon[data-animation].
 * Plays when intersecting the viewport; pauses when off-screen to limit CPU use.
 */
(function () {
  'use strict';

  var wm = new WeakMap();

  function getLottie() {
    return window.lottie && typeof window.lottie.loadAnimation === 'function'
      ? window.lottie
      : null;
  }

  function destroyEntry(el) {
    if (!el || el.nodeType !== 1) return;
    var entry = wm.get(el);
    if (!entry) return;
    try {
      if (entry.io) entry.io.disconnect();
    } catch (e) {}
    try {
      if (entry.anim) entry.anim.destroy();
    } catch (e) {}
    wm.delete(el);
    el.removeAttribute('data-lottie-ready');
  }

  function mount(el) {
    if (!el || el.nodeType !== 1) return;
    if (wm.has(el)) return;

    var L = getLottie();
    if (!L) return;

    var path = el.getAttribute('data-animation');
    if (!path) return;

    var loopAttr = el.getAttribute('data-loop');
    var loop = loopAttr !== 'false';

    var anim;
    try {
      anim = L.loadAnimation({
        container: el,
        renderer: 'svg',
        loop: loop,
        autoplay: false,
        path: path,
        rendererSettings: {
          preserveAspectRatio: 'xMidYMid meet',
          clearCanvas: true,
          progressiveLoad: false,
        },
      });
    } catch (e) {
      return;
    }

    if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      try {
        anim.goToAndStop(0, true);
      } catch (e) {}
      el.setAttribute('data-lottie-ready', '1');
      wm.set(el, { anim: anim, io: null });
      return;
    }

    function setPlaying(on) {
      var entry = wm.get(el);
      if (!entry || !entry.anim) return;
      try {
        if (on) entry.anim.play();
        else entry.anim.pause();
      } catch (e) {}
    }

    var io = new IntersectionObserver(
      function (entries) {
        for (var i = 0; i < entries.length; i++) {
          var en = entries[i];
          setPlaying(en.isIntersecting && en.intersectionRatio > 0);
        }
      },
      { root: null, rootMargin: '48px 0px', threshold: [0, 0.01, 1] }
    );

    el.setAttribute('data-lottie-ready', '1');
    wm.set(el, { anim: anim, io: io });
    io.observe(el);

    requestAnimationFrame(function () {
      try {
        var rect = el.getBoundingClientRect();
        var vh = window.innerHeight || document.documentElement.clientHeight || 0;
        var vw = window.innerWidth || document.documentElement.clientWidth || 0;
        var visible =
          rect.width > 0 &&
          rect.height > 0 &&
          rect.bottom > 0 &&
          rect.right > 0 &&
          rect.top < vh &&
          rect.left < vw;
        setPlaying(visible);
      } catch (e) {}
    });
  }

  function scan(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var list = scope.querySelectorAll('.lottie-icon[data-animation]');
    for (var i = 0; i < list.length; i++) {
      var node = list[i];
      if (node.getAttribute('data-lottie-ready') === '1') continue;
      mount(node);
    }
  }

  function collectLottieRoots(node, out) {
    if (!node || node.nodeType !== 1) return;
    if (node.matches && node.matches('.lottie-icon[data-animation]')) {
      out.push(node);
    }
    if (!node.querySelectorAll) return;
    var nested = node.querySelectorAll('.lottie-icon[data-animation]');
    for (var i = 0; i < nested.length; i++) out.push(nested[i]);
  }

  var mo = new MutationObserver(function (mutations) {
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
      destroyEntry(removed[r]);
    }
    for (var a = 0; a < added.length; a++) {
      mount(added[a]);
    }
  });

  function boot() {
    if (!getLottie()) return;
    scan(document);
    mo.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
