(function () {
  const STORAGE_KEY = "cts-sidebar-collapsed";
  const mq = window.matchMedia("(max-width: 991.98px)");

  function getLayout() {
    return document.getElementById("appLayout");
  }

  function syncSidebarState() {
    const layout = getLayout();
    if (!layout) return;
    let labelsHidden;
    if (mq.matches) {
      layout.classList.add("app-layout--mobile-narrow");
      labelsHidden = !layout.classList.contains("app-layout--mobile-expanded");
    } else {
      layout.classList.remove("app-layout--mobile-narrow", "app-layout--mobile-expanded");
      labelsHidden = layout.classList.contains("app-layout--collapsed");
    }
    layout.classList.toggle("app-sidebar-labels-hidden", labelsHidden);

    const btn = document.getElementById("appSidebarToggle");
    if (btn) {
      btn.setAttribute("aria-expanded", labelsHidden ? "false" : "true");
    }
  }

  function onToggle() {
    const layout = getLayout();
    if (!layout) return;
    if (mq.matches) {
      layout.classList.toggle("app-layout--mobile-expanded");
    } else {
      layout.classList.toggle("app-layout--collapsed");
      try {
        window.localStorage.setItem(STORAGE_KEY, layout.classList.contains("app-layout--collapsed") ? "1" : "0");
      } catch (e) {}
    }
    syncSidebarState();
  }

  function init() {
    const layout = getLayout();
    if (!layout) return;
    syncSidebarState();

    const btn = document.getElementById("appSidebarToggle");
    if (btn) {
      btn.addEventListener("click", onToggle);
    }

    if (mq.addEventListener) {
      mq.addEventListener("change", syncSidebarState);
    } else if (mq.addListener) {
      mq.addListener(syncSidebarState);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
