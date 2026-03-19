(function () {
  const STORAGE_KEY = "cts-theme";
  const mediaQuery = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  function getStoredTheme() {
    try {
      return window.localStorage.getItem(STORAGE_KEY);
    } catch (error) {
      return null;
    }
  }

  function getPreferredTheme() {
    const stored = getStoredTheme();
    if (stored === "dark" || stored === "light") {
      return stored;
    }
    return mediaQuery?.matches ? "dark" : "light";
  }

  function applyTheme(theme, options = {}) {
    const { persist = true, dispatch = true } = options;
    const isDark = theme === "dark";
    const body = document.body;
    if (!body) {
      return;
    }

    body.classList.toggle("dark-mode", isDark);
    document.documentElement.classList.toggle("theme-preload-dark", isDark);
    document.documentElement.style.colorScheme = isDark ? "dark" : "light";

    const toggles = document.querySelectorAll("#darkModeToggle");
    toggles.forEach((toggle) => {
      toggle.checked = isDark;
      toggle.setAttribute("aria-checked", String(isDark));
    });

    if (persist) {
      try {
        window.localStorage.setItem(STORAGE_KEY, theme);
      } catch (error) {}
    }

    if (dispatch) {
      window.dispatchEvent(new CustomEvent("themechange", { detail: { theme, isDark } }));
    }
  }

  function handleToggleChange(event) {
    applyTheme(event.target.checked ? "dark" : "light");
  }

  function bindToggles() {
    document.querySelectorAll("#darkModeToggle").forEach((toggle) => {
      toggle.removeEventListener("change", handleToggleChange);
      toggle.addEventListener("change", handleToggleChange);
    });
  }

  function initTheme() {
    applyTheme(getPreferredTheme(), { persist: false, dispatch: false });
    bindToggles();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initTheme, { once: true });
  } else {
    initTheme();
  }

  if (mediaQuery?.addEventListener) {
    mediaQuery.addEventListener("change", (event) => {
      if (getStoredTheme()) {
        return;
      }
      applyTheme(event.matches ? "dark" : "light", { persist: false });
    });
  }
})();
