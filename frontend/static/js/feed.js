(function () {
  const feedInitStarted = performance && typeof performance.now === "function" ? performance.now() : Date.now();
  const modalEl = document.getElementById("feedPostModal");
  const form = document.getElementById("feedComposerForm");
  const contentInput = document.getElementById("feedPostContent");
  const imageInput = document.getElementById("feedImageInput");
  const videoInput = document.getElementById("feedVideoInput");
  const fileInput = document.getElementById("feedFileInput");
  const challengeResponseModalEl = document.getElementById("feedChallengeResponseModal");
  const challengeResponseForm = document.getElementById("feedChallengeResponseForm");
  const challengeResponseTitle = document.getElementById("feedChallengeResponseTitle");
  const deletePostModalEl = document.getElementById("feedDeletePostModal");
  const deletePostConfirmButton = deletePostModalEl ? deletePostModalEl.querySelector("[data-feed-post-delete-confirm]") : null;
  const preview = document.getElementById("feedMediaPreview");
  const fileName = document.getElementById("feedFileName");
  const reactionMeta = {
    like: { label: "Like", icon: "👍" },
    celebrate: { label: "Celebrate", icon: "👏" },
    support: { label: "Support", icon: "❤️" },
    insightful: { label: "Insightful", icon: "💡" },
    funny: { label: "Funny", icon: "😂" }
  };
  let mentionContactsPromise = null;
  let previewObjectUrl = "";
  let deletePostState = {
    endpoint: "",
    postId: "",
    postEl: null
  };

  function clearPreview() {
    if (!preview || !fileName) {
      return;
    }
    if (previewObjectUrl) {
      URL.revokeObjectURL(previewObjectUrl);
      previewObjectUrl = "";
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
      previewObjectUrl = objectUrl;
      const mediaEl = document.createElement(kind === "image" ? "img" : "video");
      mediaEl.src = objectUrl;
      if (kind === "video") {
        mediaEl.controls = true;
        mediaEl.preload = "metadata";
      } else {
        mediaEl.alt = file.name || "Preview";
        mediaEl.loading = "lazy";
        mediaEl.decoding = "async";
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
          '<img class="feed-avatar-image feed-avatar-image--sm" src="' + escapeHtml(comment.author_avatar_url || "") + '" alt="' + escapeHtml(comment.author_name || "User") + '" loading="lazy" decoding="async">' +
        '</a>' +
        '<div class="feed-comment__body">' +
          '<div class="feed-comment__bubble">' +
            '<div class="feed-comment__meta">' +
              '<a href="' + escapeHtml(comment.author_profile_url || "#") + '" class="feed-comment__author">' + escapeHtml(comment.author_name || "User") + '</a>' +
              (comment.author_role_label ? '<span class="feed-role-badge">' + escapeHtml(comment.author_role_label) + '</span>' : '') +
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

  function setCommentsBusy(root, busy) {
    if (!root) {
      return;
    }
    root.classList.toggle("is-loading", Boolean(busy));
  }

  function renderComments(root, comments) {
    if (!root) {
      return;
    }
    const list = root.querySelector("[data-feed-comment-list]");
    if (!list) {
      return;
    }
    const normalized = Array.isArray(comments) ? comments : [];
    const placeholder = list.querySelector("[data-feed-comments-placeholder]");
    if (placeholder) {
      placeholder.remove();
    }
    list.innerHTML = normalized.map(commentHtml).join("");
    root.dataset.commentsLoaded = "true";
  }

  function loadComments(root) {
    if (!root || root.dataset.commentsLoaded === "true" || root.dataset.commentsLoading === "true") {
      return Promise.resolve();
    }
    const endpoint = String(root.dataset.commentsEndpoint || "").trim();
    if (!endpoint) {
      root.dataset.commentsLoaded = "true";
      return Promise.resolve();
    }
    root.dataset.commentsLoading = "true";
    setCommentsBusy(root, true);
    return fetch(endpoint, {
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
            throw new Error(String(payload.error || "Could not load comments."));
          }
          return payload;
        });
      })
      .then(function (payload) {
        renderComments(root, payload.comments || []);
      })
      .catch(function () {})
      .finally(function () {
        root.dataset.commentsLoading = "false";
        setCommentsBusy(root, false);
      });
  }

  function ensureCommentComposer(root) {
    if (!root) {
      return null;
    }
    var existing = root.querySelector("[data-feed-comment-composer]");
    if (existing) {
      return existing;
    }
    var template = root.querySelector("[data-feed-comment-composer-template]");
    if (!template || !template.content || !template.content.firstElementChild) {
      return null;
    }
    var composer = template.content.firstElementChild.cloneNode(true);
    var list = root.querySelector("[data-feed-comment-list]");
    root.insertBefore(composer, list || null);
    return composer;
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
            const placeholder = commentList.querySelector("[data-feed-comments-placeholder]");
            if (placeholder) {
              placeholder.remove();
            }
            commentList.insertAdjacentHTML("beforeend", commentHtml(comment));
          }
          const root = form.closest("[data-feed-comments]");
          if (root) {
            root.dataset.commentsLoaded = "true";
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

  function showFeedToast(message, variant) {
    const text = String(message || "").trim();
    if (!text) {
      return;
    }
    let stack = document.querySelector("[data-feed-toast-stack]");
    if (!stack) {
      stack = document.createElement("div");
      stack.className = "feed-toast-stack";
      stack.setAttribute("data-feed-toast-stack", "");
      stack.setAttribute("aria-live", "polite");
      stack.setAttribute("aria-atomic", "true");
      document.body.appendChild(stack);
    }
    const toast = document.createElement("div");
    toast.className = "feed-toast" + (variant ? " feed-toast--" + String(variant) : "");
    toast.setAttribute("role", "status");
    toast.textContent = text;
    stack.appendChild(toast);
    window.setTimeout(function () {
      toast.classList.add("is-visible");
    }, 10);
    window.setTimeout(function () {
      toast.classList.remove("is-visible");
      window.setTimeout(function () {
        toast.remove();
      }, 180);
    }, 2600);
  }

  function closeAllPostMenus(exceptMenu) {
    Array.from(document.querySelectorAll("[data-feed-post-menu]")).forEach(function (menu) {
      if (exceptMenu && menu === exceptMenu) {
        return;
      }
      const toggle = menu.querySelector("[data-feed-post-menu-toggle]");
      const panel = menu.querySelector("[data-feed-post-menu-panel]");
      if (toggle) {
        toggle.setAttribute("aria-expanded", "false");
      }
      if (panel) {
        panel.hidden = true;
      }
    });
  }

  function setPostMenuOpen(menu, open) {
    if (!menu) {
      return;
    }
    const toggle = menu.querySelector("[data-feed-post-menu-toggle]");
    const panel = menu.querySelector("[data-feed-post-menu-panel]");
    if (toggle) {
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
    }
    if (panel) {
      panel.hidden = !open;
    }
  }

  function setDeletePostBusy(busy) {
    if (!deletePostConfirmButton) {
      return;
    }
    const idle = deletePostConfirmButton.querySelector(".feed-delete-modal__confirm-idle");
    const busyLabel = deletePostConfirmButton.querySelector(".feed-delete-modal__confirm-busy");
    deletePostConfirmButton.disabled = Boolean(busy);
    if (idle) {
      idle.hidden = Boolean(busy);
    }
    if (busyLabel) {
      busyLabel.hidden = !busy;
    }
  }

  function hideDeletePostModal() {
    if (!deletePostModalEl) {
      return;
    }
    if (typeof bootstrap !== "undefined" && bootstrap.Modal) {
      bootstrap.Modal.getOrCreateInstance(deletePostModalEl).hide();
      return;
    }
    deletePostModalEl.classList.remove("show");
    deletePostModalEl.style.display = "none";
  }

  function showDeletePostModal() {
    if (!deletePostModalEl) {
      return;
    }
    if (typeof bootstrap !== "undefined" && bootstrap.Modal) {
      bootstrap.Modal.getOrCreateInstance(deletePostModalEl).show();
      return;
    }
    deletePostModalEl.style.display = "block";
    deletePostModalEl.classList.add("show");
  }

  function removeDeletedPost(postEl) {
    if (!postEl) {
      return;
    }
    postEl.classList.add("is-deleting");
    window.setTimeout(function () {
      const parentList = postEl.closest(".feed-stream__list");
      postEl.remove();
      if (parentList && !parentList.querySelector("[data-feed-post-id]")) {
        const empty = document.createElement("div");
        empty.className = "feed-empty-state feed-empty-state--inline";
        empty.innerHTML = "<h3>No posts yet</h3><p>No visible posts remain.</p>";
        parentList.replaceWith(empty);
      }
    }, 180);
  }

  function updateProfilePostCounters(delta) {
    Array.from(document.querySelectorAll("[data-profile-post-count]")).forEach(function (node) {
      const current = Number(node.textContent || 0);
      if (!Number.isFinite(current)) {
        return;
      }
      node.textContent = String(Math.max(0, current + delta));
    });
  }

  function openDeletePostDialog(button) {
    const endpoint = String(button && button.dataset.deleteEndpoint || "").trim();
    const postEl = button ? button.closest("[data-feed-post-id]") : null;
    if (!endpoint || !postEl) {
      return;
    }
    deletePostState = {
      endpoint: endpoint,
      postId: String(button.dataset.postId || postEl.dataset.feedPostId || ""),
      postEl: postEl
    };
    closeAllPostMenus();
    setDeletePostBusy(false);
    showDeletePostModal();
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

  if (deletePostModalEl) {
    deletePostModalEl.addEventListener("hidden.bs.modal", function () {
      setDeletePostBusy(false);
      deletePostState = { endpoint: "", postId: "", postEl: null };
    });
  }

  if (deletePostConfirmButton) {
    deletePostConfirmButton.addEventListener("click", function () {
      const endpoint = String(deletePostState.endpoint || "").trim();
      const postEl = deletePostState.postEl;
      if (!endpoint || !postEl) {
        hideDeletePostModal();
        return;
      }
      setDeletePostBusy(true);
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
              throw new Error(String(payload.error || "Could not delete post."));
            }
            return payload;
          });
        })
        .then(function () {
          hideDeletePostModal();
          removeDeletedPost(postEl);
          updateProfilePostCounters(-1);
          showFeedToast("Post deleted.", "success");
        })
        .catch(function (error) {
          showFeedToast(error && error.message ? error.message : "Could not delete post.", "danger");
        })
        .finally(function () {
          setDeletePostBusy(false);
        });
    });
  }

  Array.from(document.querySelectorAll("[data-feed-reaction-picker]")).forEach(function (picker) {
    const initialTrigger = picker.querySelector("[data-feed-reaction-trigger]");
    const initialReaction = initialTrigger ? String(initialTrigger.dataset.currentReaction || "").trim() : "";
    applyReactionState(picker, initialReaction);
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
    const editor = document.querySelector("[data-profile-cover-editor]");
    const viewport = editor ? editor.querySelector("[data-profile-cover-viewport]") : null;
    const preview = editor ? editor.querySelector("[data-profile-cover-preview]") : null;
    const zoomInput = editor ? editor.querySelector("[data-profile-cover-zoom]") : null;
    const saveButton = editor ? editor.querySelector("[data-profile-cover-save]") : null;
    const cancelButtons = editor ? Array.from(editor.querySelectorAll("[data-profile-cover-cancel]")) : [];
    const endpoint = String(button.dataset.uploadUrl || "").trim();
    if (!banner || !input || !endpoint) {
      return;
    }
    const coverState = {
      file: null,
      imageUrl: "",
      imageNaturalWidth: 0,
      imageNaturalHeight: 0,
      baseScale: 1,
      zoom: 1,
      offsetX: 0,
      offsetY: 0,
      dragging: false,
      dragStartX: 0,
      dragStartY: 0,
      startOffsetX: 0,
      startOffsetY: 0,
      renderRaf: 0
    };

    function closeCoverEditor() {
      if (editor) {
        editor.hidden = true;
      }
      document.body.classList.remove("modal-open", "overflow-hidden");
      if (coverState.imageUrl) {
        URL.revokeObjectURL(coverState.imageUrl);
      }
      coverState.file = null;
      coverState.imageUrl = "";
      input.value = "";
    }

    function clampCoverOffsets() {
      if (!viewport) {
        return;
      }
      const rect = viewport.getBoundingClientRect();
      const renderedWidth = coverState.imageNaturalWidth * coverState.baseScale * coverState.zoom;
      const renderedHeight = coverState.imageNaturalHeight * coverState.baseScale * coverState.zoom;
      const maxX = Math.max(0, (renderedWidth - rect.width) / 2);
      const maxY = Math.max(0, (renderedHeight - rect.height) / 2);
      coverState.offsetX = Math.min(maxX, Math.max(-maxX, coverState.offsetX));
      coverState.offsetY = Math.min(maxY, Math.max(-maxY, coverState.offsetY));
    }

    function renderCoverPreview() {
      if (!preview || !viewport) {
        return;
      }
      const rect = viewport.getBoundingClientRect();
      coverState.baseScale = Math.max(
        rect.width / Math.max(1, coverState.imageNaturalWidth),
        rect.height / Math.max(1, coverState.imageNaturalHeight)
      );
      clampCoverOffsets();
      preview.style.width = (coverState.imageNaturalWidth * coverState.baseScale * coverState.zoom) + "px";
      preview.style.height = (coverState.imageNaturalHeight * coverState.baseScale * coverState.zoom) + "px";
      preview.style.transform = "translate(calc(-50% + " + coverState.offsetX + "px), calc(-50% + " + coverState.offsetY + "px))";
    }

    function scheduleCoverPreview() {
      if (coverState.renderRaf) {
        return;
      }
      coverState.renderRaf = window.requestAnimationFrame(function () {
        coverState.renderRaf = 0;
        renderCoverPreview();
      });
    }

    function cropCoverToBlob() {
      return new Promise(function (resolve, reject) {
        if (!preview || !viewport || !coverState.file) {
          reject(new Error("Cover image is not ready."));
          return;
        }
        renderCoverPreview();
        const rect = viewport.getBoundingClientRect();
        const outputWidth = 1600;
        const outputHeight = Math.round(outputWidth * rect.height / Math.max(1, rect.width));
        const canvas = document.createElement("canvas");
        canvas.width = outputWidth;
        canvas.height = outputHeight;
        const context = canvas.getContext("2d");
        if (!context) {
          reject(new Error("Cover image could not be cropped."));
          return;
        }
        const scale = coverState.baseScale * coverState.zoom;
        const sourceX = ((coverState.imageNaturalWidth * scale - rect.width) / 2 - coverState.offsetX) / scale;
        const sourceY = ((coverState.imageNaturalHeight * scale - rect.height) / 2 - coverState.offsetY) / scale;
        const sourceWidth = rect.width / scale;
        const sourceHeight = rect.height / scale;
        context.drawImage(
          preview,
          Math.max(0, sourceX),
          Math.max(0, sourceY),
          Math.min(coverState.imageNaturalWidth, sourceWidth),
          Math.min(coverState.imageNaturalHeight, sourceHeight),
          0,
          0,
          outputWidth,
          outputHeight
        );
        canvas.toBlob(function (blob) {
          if (!blob) {
            reject(new Error("Cover image could not be cropped."));
            return;
          }
          resolve(blob);
        }, "image/jpeg", 0.92);
      });
    }

    button.addEventListener("click", function () {
      input.click();
    });

    input.addEventListener("change", function () {
      const file = input.files && input.files[0] ? input.files[0] : null;
      if (!file) {
        return;
      }
      if (!editor || !preview || !viewport || !zoomInput || !saveButton) {
        return;
      }
      if (coverState.imageUrl) {
        URL.revokeObjectURL(coverState.imageUrl);
      }
      coverState.file = file;
      coverState.imageUrl = URL.createObjectURL(file);
      coverState.zoom = 1;
      coverState.offsetX = 0;
      coverState.offsetY = 0;
      zoomInput.value = "1";
      preview.onload = function () {
        coverState.imageNaturalWidth = preview.naturalWidth || 1;
        coverState.imageNaturalHeight = preview.naturalHeight || 1;
        editor.hidden = false;
        document.body.classList.add("modal-open", "overflow-hidden");
        scheduleCoverPreview();
      };
      preview.src = coverState.imageUrl;
    });

    if (zoomInput) {
      zoomInput.addEventListener("input", function () {
        coverState.zoom = Number(zoomInput.value) || 1;
        scheduleCoverPreview();
      });
    }

    if (viewport) {
      viewport.addEventListener("pointerdown", function (event) {
        coverState.dragging = true;
        coverState.dragStartX = event.clientX;
        coverState.dragStartY = event.clientY;
        coverState.startOffsetX = coverState.offsetX;
        coverState.startOffsetY = coverState.offsetY;
        viewport.classList.add("is-dragging");
        viewport.setPointerCapture(event.pointerId);
      });
      viewport.addEventListener("pointermove", function (event) {
        if (!coverState.dragging) {
          return;
        }
        coverState.offsetX = coverState.startOffsetX + event.clientX - coverState.dragStartX;
        coverState.offsetY = coverState.startOffsetY + event.clientY - coverState.dragStartY;
        scheduleCoverPreview();
      });
      viewport.addEventListener("pointerup", function (event) {
        coverState.dragging = false;
        viewport.classList.remove("is-dragging");
        viewport.releasePointerCapture(event.pointerId);
      });
    }

    cancelButtons.forEach(function (cancelButton) {
      cancelButton.addEventListener("click", closeCoverEditor);
    });

    if (saveButton) {
      saveButton.addEventListener("click", function () {
        setProfileCoverBusy(button, true);
        saveButton.disabled = true;
        cropCoverToBlob()
          .then(function (blob) {
            const formData = new FormData();
            formData.append("cover_image", blob, "cover.jpg");
            return fetch(endpoint, {
              method: "POST",
              body: formData,
              headers: {
                Accept: "application/json",
                "X-Requested-With": "XMLHttpRequest"
              }
            });
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
            closeCoverEditor();
          })
          .catch(function () {})
          .finally(function () {
            saveButton.disabled = false;
            setProfileCoverBusy(button, false);
          });
      });
    }

    window.addEventListener("resize", scheduleCoverPreview, { passive: true });
  });

  document.addEventListener("click", function (event) {
    const reactionOption = event.target.closest("[data-feed-reaction-option]");
    if (reactionOption) {
      const picker = reactionOption.closest("[data-feed-reaction-picker]");
      saveReaction(picker, String(reactionOption.dataset.reactionType || "").trim());
      return;
    }

    const reactionTrigger = event.target.closest("[data-feed-reaction-trigger]");
    if (reactionTrigger) {
      const picker = reactionTrigger.closest("[data-feed-reaction-picker]");
      saveReaction(picker, "like");
      return;
    }

    const shareButton = event.target.closest('[data-feed-ui="share"]');
    if (shareButton) {
      const rawUrl = String(shareButton.dataset.shareUrl || "").trim() || window.location.href;
      const shareUrl = new URL(rawUrl, window.location.origin).href;
      const labelNode = shareButton.querySelector("span:last-child");
      const originalLabel = labelNode ? labelNode.textContent : "Share";
      copyText(shareUrl).then(function (copied) {
        shareButton.classList.add("is-active");
        if (labelNode) {
          labelNode.textContent = copied ? "Copied" : "Share";
        }
        window.setTimeout(function () {
          shareButton.classList.remove("is-active");
          if (labelNode) {
            labelNode.textContent = originalLabel;
          }
        }, 1200);
      });
      return;
    }

    const menuToggle = event.target.closest("[data-feed-post-menu-toggle]");
    if (menuToggle) {
      const menu = menuToggle.closest("[data-feed-post-menu]");
      const expanded = menuToggle.getAttribute("aria-expanded") === "true";
      closeAllPostMenus(menu);
      setPostMenuOpen(menu, !expanded);
      return;
    }

    const deleteOpenButton = event.target.closest("[data-feed-post-delete-open]");
    if (deleteOpenButton) {
      openDeletePostDialog(deleteOpenButton);
      return;
    }

    if (!event.target.closest("[data-feed-post-menu]")) {
      closeAllPostMenus();
    }

    const toggleButton = event.target.closest("[data-feed-comment-toggle]");
    if (toggleButton) {
      const postEl = toggleButton.closest("[data-feed-post-id]");
      const commentsRoot = postEl ? postEl.querySelector("[data-feed-comments]") : null;
      const composer = ensureCommentComposer(commentsRoot);
      const input = composer ? composer.querySelector("[data-feed-comment-input]") : null;
      const form = composer ? composer.querySelector(".feed-comment-form") : null;
      if (commentsRoot) {
        const shouldOpen = commentsRoot.hidden;
        commentsRoot.hidden = !shouldOpen;
        if (composer) {
          composer.hidden = !shouldOpen ? true : false;
        }
        toggleButton.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
        if (shouldOpen && form) {
          initCommentForm(form);
        }
        if (shouldOpen) {
          loadComments(commentsRoot);
        }
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

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      closeAllPostMenus();
    }
  });
  if (window.CtsPerf && typeof window.CtsPerf.recordInit === "function") {
    const feedInitEnded = performance && typeof performance.now === "function" ? performance.now() : Date.now();
    window.CtsPerf.recordInit("feed init", feedInitEnded - feedInitStarted, String(document.querySelectorAll("[data-feed-post-id]").length || 0));
  }
})();
