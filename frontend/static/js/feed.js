(function () {
  const modalEl = document.getElementById("feedPostModal");
  const form = document.getElementById("feedComposerForm");
  const contentInput = document.getElementById("feedPostContent");
  const imageInput = document.getElementById("feedImageInput");
  const videoInput = document.getElementById("feedVideoInput");
  const fileInput = document.getElementById("feedFileInput");
  const preview = document.getElementById("feedMediaPreview");
  const fileName = document.getElementById("feedFileName");
  const reactionMeta = {
    like: { label: "Like", icon: "👍" },
    celebrate: { label: "Celebrate", icon: "👏" },
    support: { label: "Support", icon: "❤️" },
    insightful: { label: "Insightful", icon: "💡" },
    funny: { label: "Funny", icon: "😂" }
  };

  function clearPreview() {
    if (!preview || !fileName) {
      return;
    }
    preview.innerHTML = "";
    preview.setAttribute("hidden", "");
    fileName.textContent = "";
    fileName.setAttribute("hidden", "");
  }

  function clearOtherInputs(activeInput) {
    if (!imageInput || !videoInput || !fileInput) {
      return;
    }
    [imageInput, videoInput, fileInput].forEach(function (input) {
      if (input !== activeInput) {
        input.value = "";
      }
    });
  }

  function renderPreview(file, kind) {
    if (!preview || !fileName) {
      return;
    }
    clearPreview();
    if (!file) {
      return;
    }

    if (kind === "image" || kind === "video") {
      const objectUrl = URL.createObjectURL(file);
      const mediaEl = document.createElement(kind === "image" ? "img" : "video");
      mediaEl.src = objectUrl;
      if (kind === "video") {
        mediaEl.controls = true;
        mediaEl.preload = "metadata";
      } else {
        mediaEl.alt = file.name || "Preview";
      }
      preview.appendChild(mediaEl);
      preview.removeAttribute("hidden");
    }

    fileName.textContent = file.name || "";
    fileName.removeAttribute("hidden");
  }

  function handleFileSelection(input, kind) {
    const file = input.files && input.files[0] ? input.files[0] : null;
    if (!file) {
      clearPreview();
      return;
    }
    clearOtherInputs(input);
    renderPreview(file, kind);
  }

  function reactionClassList(button) {
    Object.keys(reactionMeta).forEach(function (type) {
      button.classList.remove("feed-action--" + type);
    });
  }

  function applyReactionState(picker, reactionType) {
    if (!picker) {
      return;
    }
    const trigger = picker.querySelector("[data-feed-reaction-trigger]");
    const iconEl = picker.querySelector("[data-feed-reaction-icon]");
    const labelEl = picker.querySelector("[data-feed-reaction-label]");
    if (!trigger || !iconEl || !labelEl) {
      return;
    }
    const normalized = Object.prototype.hasOwnProperty.call(reactionMeta, reactionType) ? reactionType : "";
    const meta = normalized ? reactionMeta[normalized] : reactionMeta.like;
    reactionClassList(trigger);
    if (normalized) {
      trigger.classList.add("feed-action--" + normalized);
    }
    trigger.classList.toggle("is-active", Boolean(normalized));
    trigger.classList.toggle("is-reacted", Boolean(normalized));
    trigger.dataset.currentReaction = normalized;
    trigger.setAttribute("aria-pressed", normalized ? "true" : "false");
    iconEl.textContent = meta.icon;
    labelEl.textContent = normalized ? meta.label : "Like";
  }

  function renderReactionSummary(postEl, summary) {
    if (!postEl) {
      return;
    }
    const summaryEl = postEl.querySelector("[data-feed-reaction-summary]");
    if (!summaryEl) {
      return;
    }
    summaryEl.innerHTML = "";
    if (!Array.isArray(summary) || !summary.length) {
      summaryEl.setAttribute("hidden", "");
      return;
    }
    summary.forEach(function (item) {
      const stat = document.createElement("span");
      stat.className = "feed-reaction-stat";
      stat.dataset.reactionType = String(item.type || "");

      const icon = document.createElement("span");
      icon.className = "feed-reaction-stat__icon";
      icon.setAttribute("aria-hidden", "true");
      icon.textContent = String(item.icon || "");

      const count = document.createElement("span");
      count.className = "feed-reaction-stat__count";
      count.textContent = String(item.count || 0);

      stat.appendChild(icon);
      stat.appendChild(count);
      summaryEl.appendChild(stat);
    });
    summaryEl.removeAttribute("hidden");
  }

  function setReactionBusy(picker, busy) {
    if (!picker) {
      return;
    }
    Array.from(picker.querySelectorAll("button")).forEach(function (button) {
      button.disabled = Boolean(busy);
    });
  }

  function postForReactionPicker(picker) {
    return picker ? picker.closest("[data-feed-post-id]") : null;
  }

  function saveReaction(picker, reactionType) {
    const endpoint = picker ? String(picker.dataset.reactionEndpoint || "").trim() : "";
    if (!endpoint || !reactionType) {
      return;
    }
    setReactionBusy(picker, true);
    fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest"
      },
      body: JSON.stringify({ reaction_type: reactionType })
    })
      .then(function (response) {
        return response.json().catch(function () {
          return {};
        }).then(function (payload) {
          if (!response.ok || !payload.ok) {
            throw new Error(String(payload.error || "Could not save reaction."));
          }
          return payload;
        });
      })
      .then(function (payload) {
        applyReactionState(picker, payload.current_reaction || "");
        renderReactionSummary(postForReactionPicker(picker), payload.reaction_summary || []);
      })
      .catch(function () {})
      .finally(function () {
        setReactionBusy(picker, false);
      });
  }

  function copyText(text) {
    const value = String(text || "").trim();
    if (!value) {
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value).catch(function () {});
      return;
    }
    const temp = document.createElement("textarea");
    temp.value = value;
    temp.setAttribute("readonly", "");
    temp.style.position = "absolute";
    temp.style.left = "-9999px";
    document.body.appendChild(temp);
    temp.select();
    document.execCommand("copy");
    document.body.removeChild(temp);
  }

  if (imageInput) {
    imageInput.addEventListener("change", function () {
      handleFileSelection(imageInput, "image");
    });
  }
  if (videoInput) {
    videoInput.addEventListener("change", function () {
      handleFileSelection(videoInput, "video");
    });
  }
  if (fileInput) {
    fileInput.addEventListener("change", function () {
      handleFileSelection(fileInput, "file");
    });
  }
  if (modalEl && form) {
    modalEl.addEventListener("hidden.bs.modal", function () {
      form.reset();
      clearPreview();
    });
  }

  Array.from(document.querySelectorAll("[data-feed-reaction-picker]")).forEach(function (picker) {
    const initialTrigger = picker.querySelector("[data-feed-reaction-trigger]");
    const initialReaction = initialTrigger ? String(initialTrigger.dataset.currentReaction || "").trim() : "";
    applyReactionState(picker, initialReaction);

    const trigger = picker.querySelector("[data-feed-reaction-trigger]");
    if (trigger) {
      trigger.addEventListener("click", function () {
        saveReaction(picker, "like");
      });
    }

    Array.from(picker.querySelectorAll("[data-feed-reaction-option]")).forEach(function (button) {
      button.addEventListener("click", function () {
        saveReaction(picker, String(button.dataset.reactionType || "").trim());
      });
    });
  });

  Array.from(document.querySelectorAll('[data-feed-ui="share"]')).forEach(function (button) {
    button.addEventListener("click", function () {
      copyText(window.location.href);
      button.classList.add("is-active");
      window.setTimeout(function () {
        button.classList.remove("is-active");
      }, 1200);
    });
  });
})();
