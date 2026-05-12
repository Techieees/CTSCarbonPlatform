/**
 * Shared markup for admin mapping popup + notification bell (compact).
 * Assets: window.__CTS_MAPPING_CARD_ASSETS__ or window.__CTS_ADMIN_UPLOAD_MODAL
 */
(function () {
  "use strict";

  var AVATAR_PAIRS = [
    ["#dbeafe", "#1d4ed8"],
    ["#dcfce7", "#166534"],
    ["#fae8ff", "#9333ea"],
    ["#fee2e2", "#b91c1c"],
    ["#fef3c7", "#b45309"],
    ["#e0f2fe", "#0369a1"],
  ];

  function getAssets() {
    return window.__CTS_MAPPING_CARD_ASSETS__ || window.__CTS_ADMIN_UPLOAD_MODAL || {};
  }

  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function escAttr(s) {
    return esc(s).replace(/`/g, "&#96;");
  }

  function companySlugFromName(name) {
    var raw = String(name || "").trim();
    return raw.replace(/[^0-9a-zA-Z]+/g, "_").replace(/^_+|_+$/g, "") || "company";
  }

  function companyLogoUrlFromName(name) {
    var A = getAssets();
    var DEFAULT_CO_LOGO = A.defaultCompanyLogo || "";
    var CO_LOGOS_DIR = String(A.companyLogosDir || "").replace(/\/?$/, "/");
    if (!CO_LOGOS_DIR) return DEFAULT_CO_LOGO || "";
    return CO_LOGOS_DIR + encodeURIComponent(companySlugFromName(name) + ".png");
  }

  function initialsFromName(name) {
    var parts = String(name || "")
      .trim()
      .split(/\s+/)
      .filter(Boolean);
    if (!parts.length) return "CP";
    var ch = parts.length >= 2 ? parts[0][0] + parts[1][0] : parts[0].slice(0, 2);
    return String(ch || "CP").toUpperCase();
  }

  function avatarDataUrl(seedName) {
    var seed = String(seedName || "").trim() || "Carbon Platform";
    var h = 0;
    for (var i = 0; i < seed.length; i++) h += seed.charCodeAt(i);
    var pair = AVATAR_PAIRS[h % AVATAR_PAIRS.length];
    var ini = initialsFromName(seed);
    var svg =
      "<svg xmlns='http://www.w3.org/2000/svg' width='160' height='160' viewBox='0 0 160 160'>" +
      "<rect width='160' height='160' rx='80' fill='" +
      pair[0] +
      "'/>" +
      "<text x='50%' y='54%' text-anchor='middle' font-family='Overused Grotesk, system-ui, sans-serif' font-size='56' font-weight='700' fill='" +
      pair[1] +
      "'>" +
      ini +
      "</text></svg>";
    return "data:image/svg+xml;utf8," + encodeURIComponent(svg);
  }

  function pickCategoryLottieJson(category) {
    var c = String(category || "").toLowerCase();
    var rules = [
      [/waste|toxic|recycl|garbage|landfill|scrap/, "recycling-truck.json"],
      [
        /purchased goods|goods & services|scope 3 category 1|scope 3 cat 1|procurement|supplier|\bcategory\s*1\b|\bcat\s*1\b|vendor|purchase order/,
        "box.json",
      ],
      [/\bfera\b|fuel energy related|fuel and energy|cat 3|category 3/, "carbon-footprint.json"],
      [
        /business travel|employee commute|upstream transportation|downstream transportation|flight|air travel|rail|train|luggage|(\bcat\b|\bcategory\b)\s*6|(\bcat\b|\bcategory\b)\s*4|transport(?!\s*factor)|mileage|hotel/,
        "vacation.json",
      ],
      [/water|wastewater|aqua|effluent|rain|hydro/, "water.json"],
      [/\bfuel\b|combust|diesel|petrol|gasoline|scope 1/, "eco-fuel.json"],
      [/electric|energy|renewable|scope 2|steam|heating/, "renewable-energy.json"],
      [/factory|manufactur|production site|process/, "eco-factory.json"],
      [/end of life|eol|cat 12|category 12/, "zero-waste.json"],
      [/leased asset|franchise|investment|cat 15|category 15/, "business.json"],
      [/capital goods|capex|cat 2|category 2/, "manufacture.json"],
      [/processing of sold|cat 10|category 10/, "eco-process.json"],
      [/use of sold|cat 11|category 11/, "natural-product.json"],
      [/emission factor|carbon|co2|ghg|inventory/, "carbon-footprint.json"],
    ];
    for (var i = 0; i < rules.length; i++) {
      if (rules[i][0].test(c)) return rules[i][1];
    }
    return "timeline-chart.json";
  }

  function lottieUrlForCategory(category) {
    var A = getAssets();
    var dir = String(A.lottieDir || "/static/lottie/").replace(/\/?$/, "/");
    var file = pickCategoryLottieJson(category);
    return dir + encodeURIComponent(file);
  }

  function statusPillLabel(statusRaw, mappingState) {
    var state = String(mappingState || "")
      .trim()
      .toLowerCase();
    if (state === "fully_mapped") return "Fully mapped (EF)";
    if (state === "partially_mapped") return "Partially mapped";
    if (state === "unmapped") return "Unmapped / no EF";
    if (state === "pending") return "Not mapped yet";
    if (state === "failed") return "Mapping failed";
    if (state === "pipeline_ready") return "Pipeline refreshed";
    var raw = String(statusRaw || "").trim();
    var t = raw.toLowerCase();
    if (t.indexOf("not mapped yet") !== -1) return "Not mapped yet";
    if (t.indexOf("no ef match") !== -1 || t.indexOf("unmapped") !== -1) return "Unmapped / no EF";
    if (t.indexOf("partially mapped") !== -1 || t.indexOf("partial") !== -1) return "Partially mapped";
    if (t.indexOf("fully mapped") !== -1) return "Fully mapped (EF)";
    if (t.indexOf("not mapped") !== -1) return "Not mapped yet";
    if (t.indexOf("failed") !== -1) return "Failed";
    if (t.indexOf("processing") !== -1) return "Processing";
    if (t.indexOf("progress") !== -1) return "Processing";
    if (t.indexOf("waiting") !== -1) return "Waiting review";
    if (t.indexOf("pending review") !== -1) return "Waiting review";
    if (t.indexOf("review") !== -1) return "Waiting review";
    if (t.indexOf("pipeline") !== -1) return "Pipeline refreshed";
    if (t.indexOf("completed") !== -1) return "Completed";
    if (t.indexOf("mapped") !== -1 && t.indexOf("not mapped") === -1 && t.indexOf("unmapped") === -1) return "Mapped";
    return raw || "Unknown";
  }

  function statusPillKind(statusRaw, mappingState) {
    var state = String(mappingState || "")
      .trim()
      .toLowerCase();
    if (state === "fully_mapped") return "done";
    if (state === "partially_mapped") return "partial";
    if (state === "unmapped") return "pending";
    if (state === "pending") return "pending";
    if (state === "failed") return "failed";
    if (state === "pipeline_ready") return "done";
    var t = String(statusRaw || "").toLowerCase();
    if (t.indexOf("no ef match") !== -1 || (t.indexOf("unmapped") !== -1 && t.indexOf("fully") === -1)) return "pending";
    if (t.indexOf("partially mapped") !== -1 || t.indexOf("partial") !== -1) return "partial";
    if (t.indexOf("fully mapped") !== -1) return "done";
    if (t.indexOf("not mapped") !== -1) return "pending";
    if (t.indexOf("failed") !== -1) return "failed";
    if (t.indexOf("processing") !== -1) return "processing";
    if (t.indexOf("progress") !== -1) return "processing";
    if (t.indexOf("completed") !== -1) return "done";
    if (t.indexOf("mapped") !== -1 && t.indexOf("not mapped") === -1 && t.indexOf("unmapped") === -1) return "done";
    if (t.indexOf("waiting") !== -1) return "review";
    if (t.indexOf("pending review") !== -1) return "review";
    if (t.indexOf("review") !== -1) return "review";
    return "neutral";
  }

  /**
   * @param {object} item API mapping card fields
   * @param {{ compact?: boolean }} opts
   */
  function renderCardHtml(item, opts) {
    var options = opts || {};
    var compact = Boolean(options.compact);
    var A = getAssets();
    var DEFAULT_CO_LOGO = A.defaultCompanyLogo || "";
    var PROFILE_PHOTO_PREFIX = String(A.profilePhotoUrlPrefix || "/api/profile-photo/").replace(/\/?$/, "/");

    var companyName = item.company_name || "";
    var logoSrc = companyLogoUrlFromName(companyName) || DEFAULT_CO_LOGO;
    var uploaderName = item.uploaded_by_user || "";
    var uidRaw = item.uploaded_by_user_id;
    var uid =
      uidRaw === null || uidRaw === undefined || uidRaw === ""
        ? null
        : String(uidRaw).trim();
    var hasProfPhoto = item.uploaded_by_has_profile_photo === true || item.uploaded_by_has_profile_photo === "true";
    var externalAvatar = String(item.uploaded_by_avatar_url || "").trim();
    var avSrc = "";
    if (uid && hasProfPhoto) {
      avSrc = PROFILE_PHOTO_PREFIX + encodeURIComponent(uid);
    } else if (externalAvatar) {
      avSrc = externalAvatar;
    } else {
      avSrc = avatarDataUrl(uploaderName);
    }
    var avatarFallback = avatarDataUrl(uploaderName);
    var role = String(item.uploaded_by_job_title || "").trim();
    var roleBlock = role
      ? '<div class="admin-upload-notification-card__role text-truncate" title="' +
        escAttr(role) +
        '">' +
        esc(role) +
        "</div>"
      : "";
    var categoryLabel = String(item.category || item.sheet_name || "").trim();
    var lottieSrc = lottieUrlForCategory(categoryLabel);
    var pillKind = statusPillKind(item.mapping_status, item.mapping_state);
    var pillText = statusPillLabel(item.mapping_status, item.mapping_state);
    var footState = String(item.mapping_state || "").toLowerCase();
    var mappedLine =
      item.mapped_by_admin && item.mapping_timestamp && footState && footState !== "pending"
        ? '<div class="admin-upload-notification-card__mapped-foot text-truncate">' +
          "Last mapping run by " +
          esc(item.mapped_by_admin) +
          " at " +
          esc(item.mapping_timestamp) +
          "</div>"
        : "";

    var compactClass = compact ? " admin-upload-notification-card--compact" : "";
    var lottieClass = compact
      ? "lottie-icon animated-icon admin-upload-notification-category-lottie admin-upload-notification-category-lottie--compact"
      : "lottie-icon animated-icon admin-upload-notification-category-lottie";

    return (
      '<div class="admin-upload-notification-card' +
      compactClass +
      '">' +
      '<div class="admin-upload-notification-card__company">' +
      '<span class="admin-upload-notification-card__co-name text-truncate">' +
      esc(companyName) +
      "</span>" +
      '<img class="admin-upload-notification-card__co-logo" src="' +
      escAttr(logoSrc) +
      '" alt="" loading="lazy" decoding="async" data-fallback-logo="' +
      escAttr(DEFAULT_CO_LOGO) +
      '"/>' +
      "</div>" +
      '<div class="admin-upload-notification-card__uploader">' +
      '<img class="admin-upload-notification-card__avatar" src="' +
      escAttr(avSrc) +
      '" alt="" width="30" height="30" loading="lazy" decoding="async" data-fallback-avatar="' +
      escAttr(avatarFallback) +
      '"/>' +
      '<div class="admin-upload-notification-card__uploader-meta">' +
      '<div class="admin-upload-notification-card__uploader-name text-truncate" title="' +
      escAttr(uploaderName) +
      '">' +
      esc(uploaderName) +
      "</div>" +
      roleBlock +
      "</div>" +
      "</div>" +
      '<div class="admin-upload-notification-card__category">' +
      '<div class="' +
      lottieClass +
      '" data-animation="' +
      escAttr(lottieSrc) +
      '" data-lottie-speed="0.88" aria-hidden="true"></div>' +
      '<span class="admin-upload-notification-card__category-text">' +
      esc(categoryLabel) +
      "</span>" +
      "</div>" +
      '<div class="admin-upload-notification-card__stats">' +
      '<span class="admin-upload-notification-card__stat"><span class="admin-upload-notification-card__stat-label">Rows</span> <span class="admin-upload-notification-card__stat-value">' +
      esc(String(item.row_count ?? "")) +
      "</span></span>" +
      '<span class="admin-upload-notification-card__stat"><span class="admin-upload-notification-card__stat-label">Uploaded</span> <span class="admin-upload-notification-card__stat-value">' +
      esc(item.upload_timestamp || "") +
      "</span></span>" +
      "</div>" +
      '<div class="admin-upload-notification-card__status">' +
      '<span class="admin-upload-notification-status-pill admin-upload-notification-status-pill--' +
      pillKind +
      '">' +
      '<span class="admin-upload-notification-status-pill__dot" aria-hidden="true"></span>' +
      '<span class="admin-upload-notification-status-pill__label">' +
      esc(pillText) +
      "</span>" +
      "</span>" +
      "</div>" +
      mappedLine +
      "</div>"
    );
  }

  function wireLogoAvatarFallbacks(root) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll(".admin-upload-notification-card__co-logo").forEach(function (img) {
      function onErr() {
        var fb = img.getAttribute("data-fallback-logo");
        if (fb && img.src !== fb) img.src = fb;
        img.removeEventListener("error", onErr);
      }
      img.addEventListener("error", onErr);
    });
    root.querySelectorAll(".admin-upload-notification-card__avatar").forEach(function (img) {
      function onErr() {
        var fb = img.getAttribute("data-fallback-avatar");
        if (fb && img.src !== fb) img.src = fb;
        img.removeEventListener("error", onErr);
      }
      img.addEventListener("error", onErr);
    });
  }

  window.CtsMappingCards = {
    renderCardHtml: renderCardHtml,
    wireLogoAvatarFallbacks: wireLogoAvatarFallbacks,
    pickCategoryLottieJson: pickCategoryLottieJson,
    lottieUrlForCategory: lottieUrlForCategory,
    statusPillLabel: statusPillLabel,
    statusPillKind: statusPillKind,
    getAssets: getAssets,
  };
})();
