(() => {
  const root = document.documentElement;
  const BUTTON_SELECTOR = "button, .btn, a.button";
  const BUTTON_EXCLUDE_SELECTOR = ".btn-close, .navbar-toggler";
  const INTERACTIVE_SELECTOR = ".card, .glass-card, .lp-feature, .mock, .lp-cta, .logo-marquee, .list-group-item, .lp-kicker, .lp-stat";
  const PARALLAX_SELECTOR = "[data-parallax-scope]";

  const reducedMotionQuery = window.matchMedia("(prefers-reduced-motion: reduce)");
  const heavyEffectsQuery = window.matchMedia("(max-width: 767.98px), (pointer: coarse)");

  let pointerX = window.innerWidth / 2;
  let pointerY = window.innerHeight / 2;
  let pointerRaf = 0;

  const scopes = [];

  const prefersReducedMotion = () => reducedMotionQuery.matches;
  const disableHeavyEffects = () => heavyEffectsQuery.matches || prefersReducedMotion();

  const ensureCursorLight = () => {
    if (document.querySelector(".cursor-light")) {
      return;
    }

    const light = document.createElement("div");
    light.className = "cursor-light";
    light.setAttribute("aria-hidden", "true");
    document.body.prepend(light);
  };

  const queueGlobalPointerUpdate = (x, y) => {
    pointerX = x;
    pointerY = y;

    if (pointerRaf) {
      return;
    }

    pointerRaf = window.requestAnimationFrame(() => {
      pointerRaf = 0;
      root.style.setProperty("--mouse-x", `${pointerX}px`);
      root.style.setProperty("--mouse-y", `${pointerY}px`);
    });
  };

  const centerGlobalPointer = () => {
    queueGlobalPointerUpdate(window.innerWidth / 2, window.innerHeight / 2);
  };

  const isExcludedButton = (element) => element.matches(BUTTON_EXCLUDE_SELECTOR);

  const getTrackedButton = (target) => {
    if (!(target instanceof Element)) {
      return null;
    }

    const button = target.closest(BUTTON_SELECTOR);
    if (!button || isExcludedButton(button)) {
      return null;
    }

    return button;
  };

  const getTrackedSurface = (target) => {
    if (!(target instanceof Element)) {
      return null;
    }

    return target.closest(INTERACTIVE_SELECTOR);
  };

  const updateLocalPointerVars = (element, event, xVar, yVar) => {
    const rect = element.getBoundingClientRect();
    if (!rect.width || !rect.height) {
      return;
    }

    element.style.setProperty(xVar, `${event.clientX - rect.left}px`);
    element.style.setProperty(yVar, `${event.clientY - rect.top}px`);
  };

  const clearLocalPointerVars = (element, xVar, yVar) => {
    element.style.removeProperty(xVar);
    element.style.removeProperty(yVar);
  };

  const clearTrackedElement = (element, xVar, yVar) => {
    if (!element) {
      return;
    }

    element.classList.remove("is-premium-lit");
    clearLocalPointerVars(element, xVar, yVar);
  };

  const initInteractiveTracking = () => {
    document.addEventListener(
      "pointermove",
      (event) => {
        queueGlobalPointerUpdate(event.clientX, event.clientY);

        if (disableHeavyEffects()) {
          return;
        }

        const button = getTrackedButton(event.target);
        if (button) {
          updateLocalPointerVars(button, event, "--button-mouse-x", "--button-mouse-y");
        }

        const surface = getTrackedSurface(event.target);
        if (surface) {
          updateLocalPointerVars(surface, event, "--fx-local-x", "--fx-local-y");
        }
      },
      { passive: true }
    );

    document.addEventListener(
      "pointerover",
      (event) => {
        const button = getTrackedButton(event.target);
        if (button) {
          button.classList.add("is-premium-lit");
        }

        const surface = getTrackedSurface(event.target);
        if (surface) {
          surface.classList.add("is-premium-lit");
        }
      },
      { passive: true }
    );

    document.addEventListener(
      "pointerout",
      (event) => {
        const relatedTarget = event.relatedTarget;

        const button = getTrackedButton(event.target);
        if (button && (!(relatedTarget instanceof Node) || !button.contains(relatedTarget))) {
          clearTrackedElement(button, "--button-mouse-x", "--button-mouse-y");
        }

        const surface = getTrackedSurface(event.target);
        if (surface && (!(relatedTarget instanceof Node) || !surface.contains(relatedTarget))) {
          clearTrackedElement(surface, "--fx-local-x", "--fx-local-y");
        }
      },
      { passive: true }
    );
  };

  const updateHeroLogoOpacity = (scope, lastX, lastY) => {
    const logoImages = Array.from(scope.querySelectorAll(".hero-logo-grid img"));
    if (!logoImages.length) {
      return;
    }

    const radius = 240;
    const peakOpacity = 0.28;
    let closestImage = null;
    let closestDistance = Number.POSITIVE_INFINITY;

    for (const image of logoImages) {
      const rect = image.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const centerY = rect.top + rect.height / 2;
      const deltaX = centerX - lastX;
      const deltaY = centerY - lastY;
      const distance = Math.hypot(deltaX, deltaY);

      if (distance < closestDistance) {
        closestDistance = distance;
        closestImage = image;
      }
    }

    for (const image of logoImages) {
      image.style.setProperty("--heroLogoOpacity", "0");
    }

    if (closestImage && closestDistance <= radius) {
      closestImage.style.setProperty("--heroLogoOpacity", peakOpacity.toFixed(3));
    }
  };

  const resetHeroLogoOpacity = (scope) => {
    const logoImages = scope.querySelectorAll(".hero-logo-grid img");
    for (const image of logoImages) {
      image.style.setProperty("--heroLogoOpacity", "0");
    }
  };

  const initParallaxScope = (scope) => {
    const hero = scope.matches(".lp-hero") ? scope : scope.querySelector(".lp-hero");
    const state = {
      scope,
      hero,
      mx: 0,
      my: 0,
      heroX: 0,
      heroY: 0,
      lastX: 0,
      lastY: 0,
      raf: 0
    };

    const apply = () => {
      state.raf = 0;
      scope.style.setProperty("--mx", String(state.mx));
      scope.style.setProperty("--my", String(state.my));

      if (hero) {
        hero.style.setProperty("--hero-parallax-x", `${state.heroX}px`);
        hero.style.setProperty("--hero-parallax-y", `${state.heroY}px`);
      }

      if (!disableHeavyEffects()) {
        updateHeroLogoOpacity(scope, state.lastX, state.lastY);
      }
    };

    const queue = (clientX, clientY) => {
      if (disableHeavyEffects()) {
        return;
      }

      const rect = scope.getBoundingClientRect();
      if (!rect.width || !rect.height) {
        return;
      }

      const normalizedX = (clientX - rect.left) / rect.width - 0.5;
      const normalizedY = (clientY - rect.top) / rect.height - 0.5;

      state.mx = normalizedX * 2;
      state.my = normalizedY * 2;
      state.heroX = normalizedX * 48;
      state.heroY = normalizedY * 36;
      state.lastX = clientX;
      state.lastY = clientY;

      if (state.raf) {
        return;
      }

      state.raf = window.requestAnimationFrame(apply);
    };

    const reset = () => {
      state.mx = 0;
      state.my = 0;
      state.heroX = 0;
      state.heroY = 0;
      resetHeroLogoOpacity(scope);

      if (state.raf) {
        return;
      }

      state.raf = window.requestAnimationFrame(apply);
    };

    scope.addEventListener(
      "pointermove",
      (event) => {
        queue(event.clientX, event.clientY);
      },
      { passive: true }
    );

    scope.addEventListener(
      "pointerenter",
      () => {
        resetHeroLogoOpacity(scope);
      },
      { passive: true }
    );

    scope.addEventListener(
      "pointerleave",
      () => {
        reset();
      },
      { passive: true }
    );

    scopes.push(state);
  };

  const resetAllEffects = () => {
    centerGlobalPointer();

    document.querySelectorAll(".is-premium-lit").forEach((element) => {
      element.classList.remove("is-premium-lit");
      clearLocalPointerVars(element, "--button-mouse-x", "--button-mouse-y");
      clearLocalPointerVars(element, "--fx-local-x", "--fx-local-y");
    });

    for (const state of scopes) {
      state.mx = 0;
      state.my = 0;
      state.heroX = 0;
      state.heroY = 0;
      state.scope.style.setProperty("--mx", "0");
      state.scope.style.setProperty("--my", "0");
      if (state.hero) {
        state.hero.style.setProperty("--hero-parallax-x", "0px");
        state.hero.style.setProperty("--hero-parallax-y", "0px");
      }
      resetHeroLogoOpacity(state.scope);
    }
  };

  const init = () => {
    ensureCursorLight();
    centerGlobalPointer();
    initInteractiveTracking();
    document.querySelectorAll(PARALLAX_SELECTOR).forEach(initParallaxScope);

    window.addEventListener(
      "mouseleave",
      () => {
        resetAllEffects();
      },
      { passive: true }
    );

    window.addEventListener(
      "blur",
      () => {
        resetAllEffects();
      },
      { passive: true }
    );

    if (typeof heavyEffectsQuery.addEventListener === "function") {
      heavyEffectsQuery.addEventListener("change", resetAllEffects);
      reducedMotionQuery.addEventListener("change", resetAllEffects);
    }
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
