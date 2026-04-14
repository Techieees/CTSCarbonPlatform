(function () {
  const source = window.__countryFlagsUrl;
  if (!source) return;

  let flagsPromise = null;

  function fallbackFlag(code) {
    const key = String(code || "").trim().toUpperCase();
    if (!/^[A-Z]{2}$/.test(key)) return "";
    try {
      return String.fromCodePoint(...Array.from(key).map((char) => 127397 + char.charCodeAt(0)));
    } catch (_err) {
      return "";
    }
  }

  function loadFlags() {
    if (!flagsPromise) {
      flagsPromise = fetch(source, { headers: { Accept: "application/json" } })
        .then((res) => (res.ok ? res.json() : {}))
        .catch(() => ({}));
    }
    return flagsPromise;
  }

  function updateSelect(select, flagsMap) {
    const targetId = select.getAttribute("data-country-flag-target");
    const target = targetId ? document.getElementById(targetId) : null;
    if (!target) return;
    const code = String(select.value || "").trim().toUpperCase();
    if (!code) {
      target.textContent = "No country selected";
      return;
    }
    const flag = flagsMap[code] || fallbackFlag(code);
    const label = select.options[select.selectedIndex]?.textContent || code;
    target.textContent = flag ? `${flag} ${label}` : label;
  }

  function decorateOptions(select, flagsMap) {
    Array.from(select.options || []).forEach((option) => {
      const code = String(option.value || "").trim().toUpperCase();
      if (!code || option.dataset.flagDecorated === "1") return;
      const flag = flagsMap[code] || fallbackFlag(code);
      if (!flag) return;
      option.textContent = `${flag} ${option.textContent.replace(/^[^\w(]+\s*/, "")}`;
      option.dataset.flagDecorated = "1";
    });
  }

  function initFlags(flagsMap) {
    document.querySelectorAll("select[data-country-flag-target]").forEach((select) => {
      decorateOptions(select, flagsMap);
      updateSelect(select, flagsMap);
      select.addEventListener("change", () => updateSelect(select, flagsMap));
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      loadFlags().then(initFlags);
    }, { once: true });
  } else {
    loadFlags().then(initFlags);
  }
})();
