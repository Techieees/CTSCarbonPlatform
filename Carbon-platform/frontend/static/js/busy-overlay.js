(() => {
  const root = document.getElementById("ctsBusyOverlay");
  const textEl = document.getElementById("ctsBusyOverlayText");

  window.showCtsBusyOverlay = (message) => {
    if (!root) return;
    if (textEl) textEl.textContent = message ? String(message) : "";
    root.hidden = false;
    root.setAttribute("aria-busy", "true");
    root.setAttribute("aria-hidden", "false");
    document.documentElement.classList.add("cts-busy-overlay-open");
  };

  window.hideCtsBusyOverlay = () => {
    if (!root) return;
    root.hidden = true;
    root.setAttribute("aria-busy", "false");
    root.setAttribute("aria-hidden", "true");
    document.documentElement.classList.remove("cts-busy-overlay-open");
  };
})();
