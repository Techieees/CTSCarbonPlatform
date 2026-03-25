(() => {
  const root = document.querySelector(".prod-page[data-spotlight-root]");
  if (!root) return;

  const off = -4000;
  const hosts = () => root.querySelectorAll("[data-spotlight-host]");

  function setOff() {
    hosts().forEach((el) => {
      el.style.setProperty("--sl-x", `${off}px`);
      el.style.setProperty("--sl-y", `${off}px`);
    });
  }

  function update(clientX, clientY) {
    const pr = root.getBoundingClientRect();
    if (
      clientX < pr.left ||
      clientX > pr.right ||
      clientY < pr.top ||
      clientY > pr.bottom
    ) {
      setOff();
      return;
    }
    hosts().forEach((el) => {
      const r = el.getBoundingClientRect();
      el.style.setProperty("--sl-x", `${clientX - r.left}px`);
      el.style.setProperty("--sl-y", `${clientY - r.top}px`);
    });
  }

  if (
    window.matchMedia("(hover: none)").matches ||
    window.matchMedia("(prefers-reduced-motion: reduce)").matches
  ) {
    root.classList.add("landing-spotlight-fallback");
    return;
  }

  setOff();

  document.addEventListener(
    "pointermove",
    (e) => update(e.clientX, e.clientY),
    { capture: true, passive: true }
  );

  document.addEventListener(
    "pointerdown",
    (e) => update(e.clientX, e.clientY),
    { capture: true, passive: true }
  );

  window.addEventListener("blur", setOff);
  document.addEventListener(
    "visibilitychange",
    () => {
      if (document.visibilityState === "hidden") setOff();
    },
    { passive: true }
  );
})();
