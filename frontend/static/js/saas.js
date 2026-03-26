(() => {
  const prefersReduced = window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;

  // Marquee (coverage logos)
  const initMarquees = () => {
    const marquees = Array.from(document.querySelectorAll("[data-marquee]"));
    if (!marquees.length) return;

    for (const marquee of marquees) {
      const track = marquee.querySelector(".logo-marquee__track");
      const group = marquee.querySelector(".logo-marquee__group");
      if (!track || !group) continue;

      // Ensure exactly one clone
      if (!track.querySelector(".logo-marquee__group.is-clone")) {
        const clone = group.cloneNode(true);
        clone.classList.add("is-clone");
        clone.setAttribute("aria-hidden", "true");
        track.appendChild(clone);
      }

      const measureAndSet = () => {
        const g = marquee.querySelector(".logo-marquee__group");
        if (!g) return;
        const distance = g.scrollWidth;
        // Speed: px per second
        const speed = 70;
        const duration = Math.max(18, Math.min(60, distance / speed));
        marquee.style.setProperty("--marquee-distance", `${distance}px`);
        marquee.style.setProperty("--marquee-duration", `${duration.toFixed(2)}s`);
      };

      // Wait for images, then measure
      const imgs = Array.from(marquee.querySelectorAll("img"));
      let pending = 0;
      for (const img of imgs) {
        if (img.complete) continue;
        pending++;
        img.addEventListener(
          "load",
          () => {
            pending--;
            if (pending <= 0) measureAndSet();
          },
          { once: true }
        );
        img.addEventListener(
          "error",
          () => {
            pending--;
            if (pending <= 0) measureAndSet();
          },
          { once: true }
        );
      }

      // Initial measure
      measureAndSet();

      if ("ResizeObserver" in window) {
        const ro = new ResizeObserver(() => measureAndSet());
        ro.observe(marquee);
      } else {
        window.addEventListener("resize", () => measureAndSet(), { passive: true });
      }
    }
  };

  // Scroll reveal
  const revealEls = Array.from(document.querySelectorAll("[data-reveal]"));
  if (revealEls.length) {
    if (prefersReduced || !("IntersectionObserver" in window)) {
      revealEls.forEach((el) => el.classList.add("is-visible"));
    } else {
      const io = new IntersectionObserver(
        (entries) => {
          for (const e of entries) {
            if (e.isIntersecting) {
              e.target.classList.add("is-visible");
              io.unobserve(e.target);
            }
          }
        },
        { threshold: 0.12, rootMargin: "0px 0px -10% 0px" }
      );
      revealEls.forEach((el) => io.observe(el));
    }
  }

  // Kick off marquee after DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initMarquees, { once: true });
  } else {
    initMarquees();
  }
})();

