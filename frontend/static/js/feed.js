(function () {
  const modalEl = document.getElementById("feedPostModal");
  const form = document.getElementById("feedComposerForm");
  const contentInput = document.getElementById("feedPostContent");
  const imageInput = document.getElementById("feedImageInput");
  const videoInput = document.getElementById("feedVideoInput");
  const fileInput = document.getElementById("feedFileInput");
  const challengeResponseModalEl = document.getElementById("feedChallengeResponseModal");
  const challengeResponseForm = document.getElementById("feedChallengeResponseForm");
  const challengeResponseTitle = document.getElementById("feedChallengeResponseTitle");
  const preview = document.getElementById("feedMediaPreview");
  const fileName = document.getElementById("feedFileName");
  const reactionMeta = {
    like: { label: "Like", icon: "L" },
    celebrate: { label: "Celebrate", icon: "C" },
    support: { label: "Support", icon: "S" },
    insightful: { label: "Insightful", icon: "I" },
    funny: { label: "Funny", icon: "F" }
  };
  let mentionContactsPromise = null;

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
      return Promise.resolve(false);
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(value).then(function () {
        return true;
      }).catch(function () {
        return false;
      });
    }
    const temp = document.createElement("textarea");
    temp.value = value;
    temp.setAttribute("readonly", "");
    temp.style.position = "absolute";
    temp.style.left = "-9999px";
    document.body.appendChild(temp);
    temp.select();
    const copied = document.execCommand("copy");
    document.body.removeChild(temp);
    return Promise.resolve(Boolean(copied));
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function nl2br(value) {
    return String(value || "").replace(/\n/g, "<br>");
  }

  function commentHtml(comment) {
    const likeCount = Number(comment && comment.like_count || 0);
    const liked = Boolean(comment && comment.liked_by_viewer);
    return '' +
      '<article class="feed-comment" data-feed-comment-id="' + Number(comment.id || 0) + '">' +
        '<a href="' + escapeHtml(comment.author_profile_url || "#") + '" class="feed-comment__avatar-link" aria-label="' + escapeHtml(comment.author_name || "User") + ' profile">' +
          '<img class="feed-avatar-image feed-avatar-image--sm" src="' + escapeHtml(comment.author_avatar_url || "") + '" alt="' + escapeHtml(comment.author_name || "User") + '">' +
        '</a>' +
        '<div class="feed-comment__body">' +
          '<div class="feed-comment__bubble">' +
            '<div class="feed-comment__meta">' +
              '<a href="' + escapeHtml(comment.author_profile_url || "#") + '" class="feed-comment__author">' + escapeHtml(comment.author_name || "User") + '</a>' +
              '<span class="feed-role-badge">' + escapeHtml(comment.author_role_label || "User") + '</span>' +
              '<time class="feed-comment__time" datetime="' + escapeHtml(comment.created_at_iso || "") + '">' + escapeHtml(comment.created_at_label || "") + '</time>' +
            '</div>' +
            '<div class="feed-comment__content">' + nl2br(String(comment.content_html || "")) + '</div>' +
          '</div>' +
          '<button class="feed-comment__like' + (liked ? ' is-active' : '') + '" type="button" data-feed-comment-like data-like-endpoint="' + escapeHtml(comment.like_endpoint || "") + '" aria-pressed="' + (liked ? "true" : "false") + '">' +
            'Like <span data-feed-comment-like-count>' + likeCount + '</span>' +
          '</button>' +
        '</div>' +
      '</article>';
  }

  function loadMentionContacts(endpoint) {
    const resolved = String(endpoint || "").trim();
    if (!resolved) {
      return Promise.resolve([]);
    }
    if (!mentionContactsPromise) {
      mentionContactsPromise = fetch(resolved, {
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest"
        }
      })
        .then(function (response) {
          return response.json().catch(function () {
            return {};
          }).then(function (payload) {
            if (!response.ok) {
              throw new Error("Could not load mention contacts.");
            }
            return Array.isArray(payload.contacts) ? payload.contacts : [];
          });
        })
        .catch(function () {
          mentionContactsPromise = null;
          return [];
        });
    }
    return mentionContactsPromise;
  }

  function activeMentionQuery(input) {
    const value = String(input && input.value || "");
    const caret = Number(input && input.selectionStart || value.length);
    const beforeCaret = value.slice(0, caret);
    const match = beforeCaret.match(/(^|\s)@([a-zA-Z0-9._-]{1,30})$/);
    if (!match) {
      return null;
    }
    return {
      query: String(match[2] || "").toLowerCase(),
      replaceStart: caret - match[2].length - 1,
      replaceEnd: caret
    };
  }

  function ensureMentionMap(form) {
    if (!form._mentionMap) {
      form._mentionMap = {};
    }
    return form._mentionMap;
  }

  function mentionedIdsForForm(form, content) {
    const map = ensureMentionMap(form);
    return Object.keys(map).filter(function (token) {
      return String(content || "").indexOf(token) !== -1;
    }).map(function (token) {
      return Number(map[token] || 0);
    }).filter(function (id, index, arr) {
      return id > 0 && arr.indexOf(id) === index;
    });
  }

  function hideMentionList(list) {
    if (!list) {
      return;
    }
    list.innerHTML = "";
    list.hidden = true;
  }

  function renderMentionList(form, input, list, contacts, queryState) {
    if (!list || !queryState || !queryState.query) {
      hideMentionList(list);
      return;
    }
    const query = queryState.query;
    const matches = contacts.filter(function (contact) {
      const name = String(contact && contact.name || "").toLowerCase();
      return name.indexOf(query) !== -1;
    }).slice(0, 6);
    if (!matches.length) {
      hideMentionList(list);
      return;
    }
    list.innerHTML = matches.map(function (contact) {
      return '' +
        '<button type="button" class="feed-comment-mentions__item" data-feed-mention-option data-user-id="' + Number(contact.id || 0) + '" data-user-name="' + escapeHtml(contact.name || "") + '">' +
          '<img class="feed-avatar-image feed-avatar-image--xs" src="' + escapeHtml(contact.profile_photo_url || "") + '" alt="' + escapeHtml(contact.name || "User") + '">' +
          '<span>' + escapeHtml(contact.name || "User") + '</span>' +
        '</button>';
    }).join("");
    list.hidden = false;
    Array.from(list.querySelectorAll("[data-feed-mention-option]")).forEach(function (button) {
      button.addEventListener("click", function () {
        const userName = String(button.dataset.userName || "").trim();
        const userId = Number(button.dataset.userId || 0);
        const value = String(input.value || "");
        const replacement = "@" + userName + " ";
        input.value = value.slice(0, queryState.replaceStart) + replacement + value.slice(queryState.replaceEnd);
        input.focus();
        input.selectionStart = input.selectionEnd = queryState.replaceStart + replacement.length;
        ensureMentionMap(form)["@" + userName] = userId;
        hideMentionList(list);
      });
    });
  }

  function updateCommentLikeButton(button, liked, count) {
    if (!button) {
      return;
    }
    button.classList.toggle("is-active", Boolean(liked));
    button.setAttribute("aria-pressed", liked ? "true" : "false");
    const countNode = button.querySelector("[data-feed-comment-like-count]");
    if (countNode) {
      countNode.textContent = String(count || 0);
    }
  }

  function initCommentForm(form) {
    if (!form || form.dataset.commentInit === "true") {
      return;
    }
    form.dataset.commentInit = "true";
    const input = form.querySelector("[data-feed-comment-input]");
    const mentionList = form.querySelector("[data-feed-mention-list]");
    const endpoint = String(form.dataset.commentEndpoint || "").trim();
    const mentionEndpoint = String(form.dataset.mentionEndpoint || "").trim();
    if (!input || !endpoint) {
      return;
    }
    input.addEventListener("input", function () {
      const queryState = activeMentionQuery(input);
      if (!queryState) {
        hideMentionList(mentionList);
        return;
      }
      loadMentionContacts(mentionEndpoint).then(function (contacts) {
        renderMentionList(form, input, mentionList, contacts, queryState);
      });
    });
    input.addEventListener("keydown", function (event) {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        form.requestSubmit();
      }
      if (event.key === "Escape") {
        hideMentionList(mentionList);
      }
    });
    form.addEventListener("submit", function (event) {
      event.preventDefault();
      const content = String(input.value || "").trim();
      if (!content) {
        return;
      }
      const commentList = form.closest("[data-feed-comments]")?.querySelector("[data-feed-comment-list]");
      const mentionedUserIds = mentionedIdsForForm(form, content);
      input.disabled = true;
      fetch(endpoint, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Requested-With": "XMLHttpRequest"
        },
        body: JSON.stringify({ content: content, mentioned_user_ids: mentionedUserIds })
      })
        .then(function (response) {
          return response.json().catch(function () {
            return {};
          }).then(function (payload) {
            if (!response.ok || !payload.ok || !payload.comment) {
              throw new Error(String(payload.error || "Could not add comment."));
            }
            return payload.comment;
          });
        })
        .then(function (comment) {
          comment.like_endpoint = "/api/feed/comments/" + encodeURIComponent(comment.id) + "/like";
          if (commentList) {
            commentList.insertAdjacentHTML("beforeend", commentHtml(comment));
          }
          input.value = "";
          form._mentionMap = {};
          hideMentionList(mentionList);
        })
        .catch(function () {})
        .finally(function () {
          input.disabled = false;
          input.focus();
        });
    });
  }

  function applyFollowButtonState(button, following, busy) {
    if (!button) {
      return;
    }
    button.dataset.following = following ? "true" : "false";
    button.disabled = Boolean(busy);
    button.textContent = following ? "Following" : "Follow";
    button.classList.toggle("btn-primary", !following);
    button.classList.toggle("btn-ghost", following);
  }

  function setProfileCoverBusy(button, busy) {
    if (!button) {
      return;
    }
    button.disabled = Boolean(busy);
    button.textContent = busy ? "Uploading..." : "Edit cover";
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
      const rawUrl = String(button.dataset.shareUrl || "").trim() || window.location.href;
      const shareUrl = new URL(rawUrl, window.location.origin).href;
      const labelNode = button.querySelector("span:last-child");
      const originalLabel = labelNode ? labelNode.textContent : "Share";
      copyText(shareUrl).then(function (copied) {
        button.classList.add("is-active");
        if (labelNode) {
          labelNode.textContent = copied ? "Copied" : "Share";
        }
        window.setTimeout(function () {
          button.classList.remove("is-active");
          if (labelNode) {
            labelNode.textContent = originalLabel;
          }
        }, 1200);
      });
    });
  });

  if (challengeResponseModalEl && challengeResponseForm) {
    Array.from(document.querySelectorAll("[data-feed-open-challenge-response]")).forEach(function (button) {
      button.addEventListener("click", function () {
        const challengeId = String(button.getAttribute("data-challenge-id") || "").trim();
        const challengeTitleText = String(button.getAttribute("data-challenge-title") || "").trim();
        if (!challengeId) {
          return;
        }
        challengeResponseForm.action = "/feed/challenges/" + encodeURIComponent(challengeId) + "/responses";
        if (challengeResponseTitle) {
          challengeResponseTitle.textContent = challengeTitleText || "Respond to challenge";
        }
        if (typeof bootstrap !== "undefined") {
          bootstrap.Modal.getOrCreateInstance(challengeResponseModalEl).show();
        }
      });
    });

    challengeResponseModalEl.addEventListener("hidden.bs.modal", function () {
      challengeResponseForm.reset();
    });
  }

  Array.from(document.querySelectorAll("[data-profile-follow-toggle]")).forEach(function (button) {
    button.addEventListener("click", function () {
      const following = String(button.dataset.following || "").trim() === "true";
      const endpoint = String(following ? button.dataset.unfollowUrl : button.dataset.followUrl || "").trim();
      if (!endpoint) {
        return;
      }
      applyFollowButtonState(button, following, true);
      fetch(endpoint, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest"
        }
      })
        .then(function (response) {
          return response.json().catch(function () {
            return {};
          }).then(function (payload) {
            if (!response.ok || !payload.ok) {
              throw new Error(String(payload.error || "Could not update follow status."));
            }
            return payload;
          });
        })
        .then(function (payload) {
          const nextFollowing = Boolean(payload.following);
          applyFollowButtonState(button, nextFollowing, false);
          Array.from(document.querySelectorAll("[data-profile-follower-count]")).forEach(function (node) {
            node.textContent = String(payload.follower_count || 0);
          });
        })
        .catch(function () {
          applyFollowButtonState(button, following, false);
        });
    });
  });

  Array.from(document.querySelectorAll("[data-profile-cover-trigger]")).forEach(function (button) {
    const banner = document.querySelector("[data-profile-cover-banner]");
    const input = document.querySelector("[data-profile-cover-input]");
    const endpoint = String(button.dataset.uploadUrl || "").trim();
    if (!banner || !input || !endpoint) {
      return;
    }

    button.addEventListener("click", function () {
      input.click();
    });

    input.addEventListener("change", function () {
      const file = input.files && input.files[0] ? input.files[0] : null;
      if (!file) {
        return;
      }
      const formData = new FormData();
      formData.append("cover_image", file);
      setProfileCoverBusy(button, true);
      fetch(endpoint, {
        method: "POST",
        body: formData,
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest"
        }
      })
        .then(function (response) {
          return response.json().catch(function () {
            return {};
          }).then(function (payload) {
            if (!response.ok || !payload.ok || !payload.cover_url) {
              throw new Error(String(payload.error || "Could not upload cover image."));
            }
            return payload;
          });
        })
        .then(function (payload) {
          banner.style.backgroundImage = 'url("' + String(payload.cover_url) + '")';
          banner.classList.add("has-custom-cover");
        })
        .catch(function () {})
        .finally(function () {
          input.value = "";
          setProfileCoverBusy(button, false);
        });
    });
  });

  Array.from(document.querySelectorAll(".feed-comment-form")).forEach(initCommentForm);

  document.addEventListener("click", function (event) {
    const toggleButton = event.target.closest("[data-feed-comment-toggle]");
    if (toggleButton) {
      const postEl = toggleButton.closest("[data-feed-post-id]");
      const composer = postEl ? postEl.querySelector("[data-feed-comment-composer]") : null;
      const input = composer ? composer.querySelector("[data-feed-comment-input]") : null;
      if (composer) {
        const shouldOpen = composer.hidden;
        composer.hidden = !shouldOpen ? true : false;
        toggleButton.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
        if (shouldOpen && input) {
          input.focus();
        }
      }
      return;
    }

    const likeButton = event.target.closest("[data-feed-comment-like]");
    if (likeButton) {
      const endpoint = String(likeButton.dataset.likeEndpoint || "").trim();
      if (!endpoint) {
        return;
      }
      likeButton.disabled = true;
      fetch(endpoint, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest"
        }
      })
        .then(function (response) {
          return response.json().catch(function () {
            return {};
          }).then(function (payload) {
            if (!response.ok || !payload.ok) {
              throw new Error(String(payload.error || "Could not update comment like."));
            }
            return payload;
          });
        })
        .then(function (payload) {
          updateCommentLikeButton(likeButton, Boolean(payload.liked), Number(payload.like_count || 0));
        })
        .catch(function () {})
        .finally(function () {
          likeButton.disabled = false;
        });
      return;
    }

    if (!event.target.closest("[data-feed-mention-list]")) {
      Array.from(document.querySelectorAll("[data-feed-mention-list]")).forEach(hideMentionList);
    }
  });
})();
