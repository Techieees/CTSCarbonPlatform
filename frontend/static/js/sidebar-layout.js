(function () {
  const desktopMq = window.matchMedia("(min-width: 992px)");
  const mobileMq = window.matchMedia("(max-width: 768px)");

  function init() {
    const sidebarInitStarted = window.performance && typeof window.performance.now === "function" ? window.performance.now() : Date.now();
    const layout = document.getElementById("appLayout");
    const sidebar = document.querySelector("[data-sidebar-root]");
    if (!layout || !sidebar) return;

    const railButtons = Array.from(sidebar.querySelectorAll("[data-sidebar-section-trigger]"));
    const sectionPanels = Array.from(sidebar.querySelectorAll("[data-sidebar-section-panel]"));
    const flyoutPanels = Array.from(sidebar.querySelectorAll("[data-sidebar-flyout-panel]"));
    const flyoutTriggers = Array.from(sidebar.querySelectorAll("[data-sidebar-flyout-trigger]"));
    const legacySecondaryTriggers = Array.from(sidebar.querySelectorAll("[data-sidebar-secondary-trigger]"));
    const branchButtons = Array.from(new Set([...flyoutTriggers, ...legacySecondaryTriggers]));
    const toggleButton = sidebar.querySelector("[data-sidebar-toggle]");
    const mobileToggle = document.querySelector("[data-sidebar-mobile-toggle]");
    const mobileBackdrop = sidebar.querySelector("[data-sidebar-mobile-backdrop]");
    let pinnedExpanded = false;

    function isExpanded() {
      return sidebar.classList.contains("is-expanded") || sidebar.classList.contains("is-hover-expanded");
    }

    function syncExpandedState() {
      const expanded = isExpanded();
      sidebar.classList.toggle("is-collapsed", !expanded);
      if (!expanded) {
        closeSecondary();
        sidebar.classList.remove("is-primary-open");
      }
      if (toggleButton) {
        toggleButton.setAttribute("aria-expanded", expanded ? "true" : "false");
      }
    }

    function setMobileOpen(open) {
      if (!mobileMq.matches) {
        layout.classList.remove("is-sidebar-mobile-open");
        document.body.classList.remove("app-mobile-sidebar-open");
        if (mobileToggle) {
          mobileToggle.setAttribute("aria-expanded", "false");
        }
        return;
      }
      layout.classList.toggle("is-sidebar-mobile-open", Boolean(open));
      if (mobileToggle) {
        mobileToggle.setAttribute("aria-expanded", open ? "true" : "false");
      }
      document.body.classList.toggle("app-mobile-sidebar-open", Boolean(open));
    }

    function setSection(sectionId) {
      const normalized = String(sectionId || "").trim();
      railButtons.forEach((button) => {
        const isMatch = button.getAttribute("data-section-id") === normalized;
        button.classList.toggle("is-active", isMatch);
        button.classList.toggle("active", isMatch);
      });
      sectionPanels.forEach((panel) => {
        panel.classList.toggle("is-active", panel.getAttribute("data-sidebar-section-panel") === normalized);
      });
      sidebar.setAttribute("data-current-section", normalized);
    }

    function panelLevel(panel) {
      const raw = Number.parseInt(panel.getAttribute("data-sidebar-flyout-level") || "2", 10);
      return Number.isFinite(raw) ? raw : 2;
    }

    function triggerTarget(button) {
      return button.getAttribute("data-flyout-id") || button.getAttribute("data-secondary-id") || "";
    }

    function closeFlyouts(fromLevel = 2) {
      flyoutPanels.forEach((panel) => {
        if (panelLevel(panel) >= fromLevel) {
          panel.classList.remove("is-active");
        }
      });
      branchButtons.forEach((button) => {
        const targetId = triggerTarget(button);
        const targetPanel = flyoutPanels.find((panel) => panel.getAttribute("data-sidebar-flyout-panel") === targetId);
        if (!targetPanel || panelLevel(targetPanel) >= fromLevel) {
          button.classList.remove("is-open");
          button.setAttribute("aria-expanded", "false");
        }
      });
      const hasOpenFlyouts = flyoutPanels.some((panel) => panel.classList.contains("is-active"));
      sidebar.classList.toggle("is-secondary-open", hasOpenFlyouts);
      if (!hasOpenFlyouts) {
        sidebar.setAttribute("data-current-secondary", "");
      }
    }

    function closeSecondary() {
      closeFlyouts(2);
    }

    function ensureExpandedForNestedNavigation() {
      if (isExpanded()) {
        return true;
      }
      if (desktopMq.matches && !pinnedExpanded) {
        sidebar.classList.add("is-hover-expanded");
        sidebar.classList.add("is-primary-open");
        syncExpandedState();
        return true;
      }
      return false;
    }

    function openFlyout(panelId) {
      if (!ensureExpandedForNestedNavigation()) {
        return;
      }
      const normalized = String(panelId || "").trim();
      if (!normalized) {
        closeFlyouts(2);
        return;
      }
      const targetPanel = flyoutPanels.find((panel) => panel.getAttribute("data-sidebar-flyout-panel") === normalized);
      if (!targetPanel) {
        closeFlyouts(2);
        return;
      }
      const level = panelLevel(targetPanel);
      closeFlyouts(level);
      targetPanel.classList.add("is-active");
      sidebar.classList.add("is-secondary-open");
      branchButtons.forEach((button) => {
        const targetId = triggerTarget(button);
        const isMatch = targetId === normalized;
        if (isMatch) {
          button.classList.add("is-open");
          button.setAttribute("aria-expanded", "true");
        }
      });
      if (level === 2) {
        sidebar.setAttribute("data-current-secondary", normalized);
      }
    }

    function openSecondary(panelId) {
      openFlyout(panelId);
    }

    railButtons.forEach((button) => {
      button.addEventListener("click", () => {
        setSection(button.getAttribute("data-section-id"));
        sidebar.classList.add("is-primary-open");
        if (desktopMq.matches && !pinnedExpanded) {
          sidebar.classList.add("is-hover-expanded");
        }
        syncExpandedState();
      });
    });

    Array.from(sidebar.querySelectorAll("a.app-sidebar__menu-item")).forEach((link) => {
      link.addEventListener("click", () => {
        if (mobileMq.matches) {
          setMobileOpen(false);
        }
      });
    });

    branchButtons.forEach((button) => {
      button.setAttribute("aria-expanded", "false");
    });

    sidebar.addEventListener("click", (event) => {
      const closeButton = event.target.closest("[data-sidebar-flyout-close], [data-sidebar-secondary-close]");
      if (closeButton && sidebar.contains(closeButton)) {
        const panel = closeButton.closest("[data-sidebar-flyout-panel]");
        closeFlyouts(panel ? panelLevel(panel) : 2);
        return;
      }

      const trigger = event.target.closest("[data-sidebar-flyout-trigger], [data-sidebar-secondary-trigger]");
      if (!trigger || !sidebar.contains(trigger)) {
        return;
      }
      event.preventDefault();
      if (!ensureExpandedForNestedNavigation()) {
        return;
      }
      const targetId = triggerTarget(trigger);
      const targetPanel = flyoutPanels.find((panel) => panel.getAttribute("data-sidebar-flyout-panel") === targetId);
      if (!targetPanel) {
        return;
      }
      const isAlreadyOpen = targetPanel.classList.contains("is-active");
      if (isAlreadyOpen) {
        closeFlyouts(panelLevel(targetPanel));
        return;
      }
      openFlyout(targetId);
    });

    if (toggleButton) {
      toggleButton.addEventListener("click", () => {
        pinnedExpanded = !pinnedExpanded;
        sidebar.classList.toggle("is-expanded", pinnedExpanded);
        if (!pinnedExpanded) {
          sidebar.classList.remove("is-hover-expanded");
        }
        syncExpandedState();
      });
    }

    if (mobileToggle) {
      mobileToggle.addEventListener("click", () => {
        setMobileOpen(!layout.classList.contains("is-sidebar-mobile-open"));
      });
    }

    if (mobileBackdrop) {
      mobileBackdrop.addEventListener("click", () => {
        setMobileOpen(false);
      });
    }

    sidebar.addEventListener("mouseenter", () => {
      if (!desktopMq.matches || pinnedExpanded) return;
      sidebar.classList.add("is-hover-expanded");
      syncExpandedState();
    });

    sidebar.addEventListener("mouseleave", () => {
      if (!desktopMq.matches || pinnedExpanded) return;
      sidebar.classList.remove("is-hover-expanded");
      syncExpandedState();
    });

    function applyResponsiveState() {
      if (desktopMq.matches) {
        pinnedExpanded = false;
        sidebar.classList.toggle("is-expanded", pinnedExpanded);
        sidebar.classList.remove("is-hover-expanded");
        layout.classList.remove("app-layout--mobile-drawer");
      } else {
        pinnedExpanded = true;
        sidebar.classList.add("is-expanded");
        sidebar.classList.remove("is-hover-expanded");
        layout.classList.toggle("app-layout--mobile-drawer", mobileMq.matches);
      }
      if (!mobileMq.matches) {
        setMobileOpen(false);
      }
      syncExpandedState();
    }

    window.addEventListener("load", () => {
      setMobileOpen(false);
    }, { once: true });

    if (desktopMq.addEventListener) {
      desktopMq.addEventListener("change", applyResponsiveState);
    } else if (desktopMq.addListener) {
      desktopMq.addListener(applyResponsiveState);
    }

    if (mobileMq.addEventListener) {
      mobileMq.addEventListener("change", applyResponsiveState);
    } else if (mobileMq.addListener) {
      mobileMq.addListener(applyResponsiveState);
    }

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && layout.classList.contains("is-sidebar-mobile-open")) {
        setMobileOpen(false);
      }
    });

    const initialSection = sidebar.getAttribute("data-initial-section") || "dashboard";
    const initialSecondary = sidebar.getAttribute("data-initial-secondary") || "";
    setSection(initialSection);
    applyResponsiveState();
    if (initialSecondary) {
      openSecondary(initialSecondary);
    } else {
      closeSecondary();
    }
    if (window.CtsPerf && typeof window.CtsPerf.recordInit === "function") {
      const sidebarInitEnded = window.performance && typeof window.performance.now === "function" ? window.performance.now() : Date.now();
      window.CtsPerf.recordInit("sidebar init", sidebarInitEnded - sidebarInitStarted, String(railButtons.length + branchButtons.length));
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
