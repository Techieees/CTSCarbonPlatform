(function () {
  const STORAGE_KEY = "sidebarCollapsed";
  const desktopMq = window.matchMedia("(min-width: 992px)");

  function init() {
    const layout = document.getElementById("appLayout");
    const sidebar = document.querySelector("[data-sidebar-root]");
    if (!layout || !sidebar) return;

    const railButtons = Array.from(sidebar.querySelectorAll("[data-sidebar-section-trigger]"));
    const sectionPanels = Array.from(sidebar.querySelectorAll("[data-sidebar-section-panel]"));
    const secondaryShell = sidebar.querySelector("[data-sidebar-secondary-shell]");
    const secondaryPanels = Array.from(sidebar.querySelectorAll("[data-sidebar-secondary-panel]"));
    const secondaryTitle = sidebar.querySelector("[data-sidebar-secondary-title]");
    const secondaryClose = sidebar.querySelector("[data-sidebar-secondary-close]");
    const branchButtons = Array.from(sidebar.querySelectorAll("[data-sidebar-secondary-trigger]"));
    const toggleButton = sidebar.querySelector("[data-sidebar-toggle]");
    let pinnedExpanded = false;

    function readStoredCollapsed() {
      try {
        return window.localStorage.getItem(STORAGE_KEY);
      } catch (e) {
        return null;
      }
    }

    function storeCollapsed(isCollapsed) {
      try {
        window.localStorage.setItem(STORAGE_KEY, isCollapsed ? "true" : "false");
      } catch (e) {}
    }

    function isExpanded() {
      return sidebar.classList.contains("is-expanded") || sidebar.classList.contains("is-hover-expanded");
    }

    function syncExpandedState() {
      const expanded = isExpanded();
      sidebar.classList.toggle("is-collapsed", !expanded);
      if (!expanded) {
        closeSecondary();
      }
      if (toggleButton) {
        toggleButton.setAttribute("aria-expanded", expanded ? "true" : "false");
      }
    }

    function setSection(sectionId) {
      const normalized = String(sectionId || "").trim();
      railButtons.forEach((button) => {
        button.classList.toggle("is-active", button.getAttribute("data-section-id") === normalized);
      });
      sectionPanels.forEach((panel) => {
        panel.classList.toggle("is-active", panel.getAttribute("data-sidebar-section-panel") === normalized);
      });
      sidebar.setAttribute("data-current-section", normalized);
    }

    function closeSecondary() {
      sidebar.classList.remove("is-secondary-open");
      if (secondaryShell) {
        secondaryShell.classList.remove("is-active");
      }
      secondaryPanels.forEach((panel) => {
        panel.classList.remove("is-active");
      });
      branchButtons.forEach((button) => {
        button.classList.remove("is-open");
        button.setAttribute("aria-expanded", "false");
      });
      sidebar.setAttribute("data-current-secondary", "");
    }

    function openSecondary(panelId, titleText) {
      if (!isExpanded()) {
        return;
      }
      const normalized = String(panelId || "").trim();
      if (!normalized) {
        closeSecondary();
        return;
      }
      sidebar.classList.add("is-secondary-open");
      if (secondaryShell) {
        secondaryShell.classList.add("is-active");
      }
      secondaryPanels.forEach((panel) => {
        panel.classList.toggle("is-active", panel.getAttribute("data-sidebar-secondary-panel") === normalized);
      });
      branchButtons.forEach((button) => {
        const isMatch = button.getAttribute("data-secondary-id") === normalized;
        button.classList.toggle("is-open", isMatch);
        button.setAttribute("aria-expanded", isMatch ? "true" : "false");
      });
      if (secondaryTitle) {
        secondaryTitle.textContent = titleText || "Details";
      }
      sidebar.setAttribute("data-current-secondary", normalized);
    }

    railButtons.forEach((button) => {
      button.addEventListener("click", () => {
        setSection(button.getAttribute("data-section-id"));
        if (desktopMq.matches && !pinnedExpanded) {
          sidebar.classList.add("is-hover-expanded");
        }
        syncExpandedState();
      });
    });

    branchButtons.forEach((button) => {
      button.addEventListener("click", () => {
        if (!isExpanded()) {
          if (desktopMq.matches && !pinnedExpanded) {
            sidebar.classList.add("is-hover-expanded");
            syncExpandedState();
          } else {
            return;
          }
        }
        const targetId = button.getAttribute("data-secondary-id");
        const isAlreadyOpen = sidebar.getAttribute("data-current-secondary") === targetId && sidebar.classList.contains("is-secondary-open");
        if (isAlreadyOpen) {
          closeSecondary();
          return;
        }
        openSecondary(targetId, button.getAttribute("data-secondary-title"));
      });
    });

    if (secondaryClose) {
      secondaryClose.addEventListener("click", closeSecondary);
    }

    if (toggleButton) {
      toggleButton.addEventListener("click", () => {
        pinnedExpanded = !pinnedExpanded;
        sidebar.classList.toggle("is-expanded", pinnedExpanded);
        if (!pinnedExpanded) {
          sidebar.classList.remove("is-hover-expanded");
        }
        storeCollapsed(!pinnedExpanded);
        syncExpandedState();
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
        const stored = readStoredCollapsed();
        pinnedExpanded = stored === "false" ? true : false;
        sidebar.classList.toggle("is-expanded", pinnedExpanded);
        sidebar.classList.remove("is-hover-expanded");
      } else {
        pinnedExpanded = true;
        sidebar.classList.add("is-expanded");
        sidebar.classList.remove("is-hover-expanded");
      }
      syncExpandedState();
    }

    if (desktopMq.addEventListener) {
      desktopMq.addEventListener("change", applyResponsiveState);
    } else if (desktopMq.addListener) {
      desktopMq.addListener(applyResponsiveState);
    }

    const initialSection = sidebar.getAttribute("data-initial-section") || "dashboard";
    const initialSecondary = sidebar.getAttribute("data-initial-secondary") || "";
    setSection(initialSection);
    applyResponsiveState();
    if (initialSecondary && isExpanded()) {
      const matchingTrigger = branchButtons.find((button) => button.getAttribute("data-secondary-id") === initialSecondary);
      openSecondary(initialSecondary, matchingTrigger ? matchingTrigger.getAttribute("data-secondary-title") : "Details");
    } else {
      closeSecondary();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
