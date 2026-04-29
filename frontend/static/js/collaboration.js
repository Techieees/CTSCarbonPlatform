(function () {
  const notificationRoot = document.querySelector("[data-collab-notifications]");
  const chatRoot = document.querySelector("[data-chat-widget]");

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, function (char) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char];
    });
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, Object.assign({ headers: { Accept: "application/json" } }, options || {}));
    const data = await response.json().catch(function () { return {}; });
    if (!response.ok) {
      throw new Error(data.error || "Request failed");
    }
    return data;
  }

  function setBadge(element, count) {
    if (!element) return;
    const value = Number(count || 0);
    element.hidden = value < 1;
    element.textContent = value > 99 ? "99+" : String(value);
  }

  function initialsFromName(name) {
    const parts = String(name || "").trim().split(/\s+/).filter(Boolean).slice(0, 2);
    if (!parts.length) return "?";
    return parts.map(function (part) { return part.charAt(0).toUpperCase(); }).join("");
  }

  function renderAvatar(person) {
    const photoUrl = person && person.profile_photo_url ? String(person.profile_photo_url) : "";
    const name = person && person.name ? person.name : "";
    if (photoUrl) {
      return '<img class="chat-widget__avatar" src="' + escapeHtml(photoUrl) + '" alt="' + escapeHtml(name) + '">';
    }
    return '<span class="chat-widget__avatar chat-widget__avatar--placeholder" aria-hidden="true">' + escapeHtml(initialsFromName(name)) + '</span>';
  }

  function renderPersonHeading(person) {
    return '' +
      '<span class="chat-widget__person">' +
        renderAvatar(person) +
        '<span class="chat-widget__person-copy">' +
          '<span class="collab-notification-item__title">' + escapeHtml(person.name || "") + '</span>' +
        '</span>' +
      '</span>';
  }

  if (notificationRoot) {
    const list = notificationRoot.querySelector("[data-collab-notification-list]");
    const badge = notificationRoot.querySelector("[data-collab-notification-badge]");
    const markAllBtn = notificationRoot.querySelector("[data-mark-all-notifications]");

    async function loadNotifications() {
      const data = await fetchJson("/api/notifications/recent");
      setBadge(badge, data.unread_count);
      const rows = Array.isArray(data.notifications) ? data.notifications : [];
      if (!rows.length) {
        list.innerHTML = '<div class="collab-empty-state">No notifications yet.</div>';
        return;
      }
      list.innerHTML = rows.map(function (item) {
        const titleHtml = item.link
          ? '<a href="' + escapeHtml(item.link) + '">' + escapeHtml(item.title) + "</a>"
          : escapeHtml(item.title);
        return '' +
          '<article class="collab-notification-item" data-notification-id="' + Number(item.id) + '">' +
            '<div>' +
              '<div class="collab-notification-item__title">' + titleHtml + '</div>' +
              '<div>' + escapeHtml(item.message) + '</div>' +
              '<div class="collab-notification-item__meta">' + escapeHtml(item.created_at) + '</div>' +
            '</div>' +
            (item.is_read ? '' : '<span class="search-result-item__kind">New</span>') +
          '</article>';
      }).join("");
    }

    notificationRoot.addEventListener("show.bs.dropdown", function () {
      loadNotifications().catch(function () {});
    });

    list.addEventListener("click", function (event) {
      const item = event.target.closest("[data-notification-id]");
      if (!item) return;
      fetchJson("/api/notifications/mark-read", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ notification_id: Number(item.getAttribute("data-notification-id")) })
      }).then(function (data) {
        setBadge(badge, data.unread_count);
      }).catch(function () {});
    });

    if (markAllBtn) {
      markAllBtn.addEventListener("click", function () {
        fetchJson("/api/notifications/mark-read", {
          method: "POST",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ notification_id: "all" })
        }).then(function (data) {
          setBadge(badge, data.unread_count);
          loadNotifications().catch(function () {});
        }).catch(function () {});
      });
    }

    document.querySelectorAll("[data-open-notifications]").forEach(function (button) {
      button.addEventListener("click", function (event) {
        event.preventDefault();
        const toggle = notificationRoot.querySelector("[data-bs-toggle='dropdown']");
        if (!toggle || typeof bootstrap === "undefined") return;
        const dropdown = bootstrap.Dropdown.getOrCreateInstance(toggle);
        dropdown.show();
      });
    });

    loadNotifications().catch(function () {});
    window.setInterval(function () {
      fetchJson("/api/notifications/unread_count")
        .then(function (data) { setBadge(badge, data.unread_count); })
        .catch(function () {});
    }, 10000);
  }

  if (chatRoot) {
    document.body.classList.add("has-chat-widget");
    const launcherButtons = Array.from(chatRoot.querySelectorAll("[data-chat-toggle]"));
    const panel = chatRoot.querySelector("[data-chat-panel]");
    const closeBtn = chatRoot.querySelector("[data-chat-close]");
    const conversationsList = chatRoot.querySelector("[data-chat-conversations]");
    const contactsList = chatRoot.querySelector("[data-chat-contacts]");
    const messagesWrap = chatRoot.querySelector("[data-chat-messages]");
    const form = chatRoot.querySelector("[data-chat-form]");
    const input = chatRoot.querySelector("[data-chat-input]");
    const title = chatRoot.querySelector("[data-chat-thread-title]");
    const unreadBadge = chatRoot.querySelector("[data-chat-unread-badge]");
    const threadWindow = chatRoot.querySelector("[data-chat-thread-window]");
    const threadCloseBtn = chatRoot.querySelector("[data-chat-thread-close]");
    const threadMinimizeBtn = chatRoot.querySelector("[data-chat-thread-minimize]");
    const composeButtons = Array.from(chatRoot.querySelectorAll("[data-chat-compose]"));
    const searchInput = chatRoot.querySelector("[data-chat-search]");
    const sendButton = chatRoot.querySelector("[data-chat-send]");
    const imageTrigger = chatRoot.querySelector("[data-chat-image-trigger]");
    const fileTrigger = chatRoot.querySelector("[data-chat-file-trigger]");
    const imageInput = chatRoot.querySelector("[data-chat-image-input]");
    const fileInput = chatRoot.querySelector("[data-chat-file-input]");
    const attachmentPreview = chatRoot.querySelector("[data-chat-attachment-preview]");
    const emojiToggle = chatRoot.querySelector("[data-chat-emoji-toggle]");
    const emojiPicker = chatRoot.querySelector("[data-chat-emoji-picker]");
    const emptyHint = chatRoot.querySelector("[data-chat-empty-hint]");
    const typingIndicator = chatRoot.querySelector("[data-chat-typing-indicator]");
    let activeUserId = null;
    let conversations = [];
    let contacts = [];
    let activeContact = null;
    let selectedAttachment = null;
    let attachmentObjectUrl = "";
    let currentThreadMessageIds = [];
    let typingIdleTimer = null;
    let typingPollTimer = null;
    let threadPollTimer = null;
    let typingSentActive = false;

    function personMeta(person) {
      const parts = [
        person && person.job_title,
        person && person.company_name
      ].filter(Boolean);
      if (parts.length) {
        return parts.join(" · ");
      }
      return String((person && (person.email || person.company_name)) || "").trim();
    }

    function setPanelOpen(open) {
      chatRoot.classList.toggle("is-panel-open", Boolean(open));
      if (panel) {
        panel.setAttribute("aria-hidden", open ? "false" : "true");
      }
      launcherButtons.forEach(function (button) {
        button.setAttribute("aria-expanded", open ? "true" : "false");
      });
    }

    function setThreadOpen(open) {
      chatRoot.classList.toggle("is-thread-open", Boolean(open));
      if (threadWindow) {
        threadWindow.setAttribute("aria-hidden", open ? "false" : "true");
      }
    }

    function isPanelOpen() {
      return chatRoot.classList.contains("is-panel-open");
    }

    function attachmentLabel(file) {
      if (!file) return "";
      const sizeKb = Math.max(1, Math.round((Number(file.size || 0) || 0) / 1024));
      return String(file.name || "Attachment") + " (" + sizeKb + " KB)";
    }

    function clearAttachmentPreview() {
      selectedAttachment = null;
      if (imageInput) imageInput.value = "";
      if (fileInput) fileInput.value = "";
      if (attachmentObjectUrl) {
        try { URL.revokeObjectURL(attachmentObjectUrl); } catch (e) {}
        attachmentObjectUrl = "";
      }
      if (attachmentPreview) {
        attachmentPreview.innerHTML = "";
        attachmentPreview.hidden = true;
      }
    }

    function renderAttachmentPreview(file) {
      if (!attachmentPreview) return;
      attachmentPreview.innerHTML = "";
      if (!file) {
        attachmentPreview.hidden = true;
        return;
      }
      const isImage = String(file.type || "").indexOf("image/") === 0;
      const wrapper = document.createElement("div");
      wrapper.className = "chat-thread-window__attachment-card";

      if (isImage) {
        attachmentObjectUrl = URL.createObjectURL(file);
        const image = document.createElement("img");
        image.className = "chat-thread-window__attachment-image";
        image.src = attachmentObjectUrl;
        image.alt = String(file.name || "Selected image");
        wrapper.appendChild(image);
      } else {
        const icon = document.createElement("span");
        icon.className = "chat-thread-window__attachment-icon";
        icon.textContent = "📎";
        wrapper.appendChild(icon);
      }

      const meta = document.createElement("div");
      meta.className = "chat-thread-window__attachment-meta";
      meta.innerHTML =
        '<strong>' + escapeHtml(String(file.name || "Attachment")) + '</strong>' +
        '<span>' + escapeHtml(attachmentLabel(file)) + '</span>';
      wrapper.appendChild(meta);

      const removeButton = document.createElement("button");
      removeButton.type = "button";
      removeButton.className = "chat-thread-window__attachment-remove";
      removeButton.textContent = "Remove";
      removeButton.addEventListener("click", clearAttachmentPreview);
      wrapper.appendChild(removeButton);

      attachmentPreview.appendChild(wrapper);
      attachmentPreview.hidden = false;
    }

    function setAttachment(file) {
      if (attachmentObjectUrl) {
        try { URL.revokeObjectURL(attachmentObjectUrl); } catch (e) {}
        attachmentObjectUrl = "";
      }
      selectedAttachment = file || null;
      renderAttachmentPreview(selectedAttachment);
      updateComposerState();
    }

    function syncPanelDensityState() {
      const hasConversations = Array.isArray(conversations) && conversations.length > 0;
      chatRoot.classList.toggle("is-empty-conversations", !hasConversations);
      if (emptyHint) {
        emptyHint.hidden = hasConversations;
      }
    }

    function autoResizeComposer() {
      if (!input) return;
      input.style.height = "auto";
      const computed = window.getComputedStyle(input);
      const lineHeight = parseFloat(computed.lineHeight || "20") || 20;
      const maxHeight = lineHeight * 4 + 20;
      input.style.height = Math.min(input.scrollHeight, maxHeight) + "px";
      input.style.overflowY = input.scrollHeight > maxHeight ? "auto" : "hidden";
    }

    function updateComposerState() {
      const hasText = Boolean(String(input && input.value || "").trim());
      const canSend = Boolean(activeUserId) && (hasText || Boolean(selectedAttachment)) && !(input && input.disabled);
      if (sendButton) {
        sendButton.disabled = !canSend;
      }
    }

    function renderThreadTitle(person) {
      if (!title) return;
      if (!person) {
        title.innerHTML = "Select a conversation";
        return;
      }
      const meta = personMeta(person);
      title.innerHTML = '' +
        '<span class="chat-widget__person">' +
          renderAvatar(person) +
          '<span class="chat-widget__person-copy">' +
            '<span class="chat-widget__thread-name">' + escapeHtml(person.name || "") + '</span>' +
            (meta ? '<span class="chat-widget__thread-meta">' + escapeHtml(meta) + '</span>' : '') +
          '</span>' +
        '</span>';
    }

    function renderTypingIndicator(label, visible) {
      if (!typingIndicator) return;
      typingIndicator.textContent = label || "";
      typingIndicator.hidden = !visible;
    }

    function messageReceipt(item) {
      if (!item || !item.is_mine) return "";
      return item.is_read
        ? '<span class="chat-thread-window__receipt is-read" aria-label="Read">✓✓</span>'
        : '<span class="chat-thread-window__receipt" aria-label="Sent">✓</span>';
    }

    function messageMetaHtml(item) {
      return '' +
        '<span>' + escapeHtml(item.created_at || "") + '</span>' +
        messageReceipt(item);
    }

    function messageHtml(item, isAppearing) {
      return '' +
        '<article class="chat-thread-window__message' + (item.is_mine ? ' is-mine' : '') + (isAppearing ? ' is-appearing' : '') + '" data-message-id="' + Number(item.id) + '">' +
          '<div class="chat-thread-window__bubble">' + escapeHtml(item.message) + '</div>' +
          '<div class="chat-thread-window__message-meta">' + messageMetaHtml(item) + '</div>' +
        '</article>';
    }

    function scrollMessagesToBottom() {
      if (!messagesWrap) return;
      messagesWrap.scrollTo({ top: messagesWrap.scrollHeight, behavior: "smooth" });
    }

    function updateMessageElement(element, item) {
      if (!element || !item) return;
      const bubble = element.querySelector(".chat-thread-window__bubble");
      const meta = element.querySelector(".chat-thread-window__message-meta");
      if (bubble) {
        bubble.textContent = String(item.message || "");
      }
      if (meta) {
        meta.innerHTML = messageMetaHtml(item);
      }
      element.classList.toggle("is-mine", Boolean(item.is_mine));
    }

    function renderThread(messages) {
      currentThreadMessageIds = Array.isArray(messages) ? messages.map(function (item) { return Number(item.id); }) : [];
      if (!messages.length) {
        messagesWrap.innerHTML = '<div class="collab-empty-state collab-empty-state--compact">No messages yet.</div>';
        renderTypingIndicator("", false);
        return;
      }
      messagesWrap.innerHTML = messages.map(function (item) {
        return messageHtml(item, false);
      }).join("");
      scrollMessagesToBottom();
    }

    function syncThread(messages, options) {
      const rows = Array.isArray(messages) ? messages : [];
      const config = options || {};
      if (config.replace || !messagesWrap.querySelector("[data-message-id]")) {
        renderThread(rows);
        return;
      }
      let appended = false;
      rows.forEach(function (item) {
        const selector = '[data-message-id="' + Number(item.id) + '"]';
        const existing = messagesWrap.querySelector(selector);
        if (existing) {
          updateMessageElement(existing, item);
          return;
        }
        messagesWrap.insertAdjacentHTML("beforeend", messageHtml(item, true));
        appended = true;
      });
      currentThreadMessageIds = rows.map(function (item) { return Number(item.id); });
      if (appended) {
        scrollMessagesToBottom();
      }
    }

    function lastMessageId() {
      if (!currentThreadMessageIds.length) return 0;
      return Math.max.apply(null, currentThreadMessageIds);
    }

    function stopTypingTimers() {
      if (typingIdleTimer) {
        window.clearTimeout(typingIdleTimer);
        typingIdleTimer = null;
      }
      typingSentActive = false;
    }

    function stopTypingPolling() {
      if (typingPollTimer) {
        window.clearInterval(typingPollTimer);
        typingPollTimer = null;
      }
    }

    function stopThreadPolling() {
      if (threadPollTimer) {
        window.clearInterval(threadPollTimer);
        threadPollTimer = null;
      }
    }

    function sendTypingState(isTyping) {
      if (!activeUserId) return Promise.resolve();
      if (typingSentActive === Boolean(isTyping) && Boolean(isTyping)) {
        return Promise.resolve();
      }
      typingSentActive = Boolean(isTyping);
      return fetchJson("/api/messages/typing", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ receiver_id: activeUserId, is_typing: Boolean(isTyping) })
      }).catch(function () {});
    }

    function queueTypingSignal() {
      if (!activeUserId) return;
      sendTypingState(true);
      if (typingIdleTimer) {
        window.clearTimeout(typingIdleTimer);
      }
      typingIdleTimer = window.setTimeout(function () {
        sendTypingState(false);
      }, 1500);
    }

    function pollTypingStatus() {
      if (!activeUserId) {
        renderTypingIndicator("", false);
        return;
      }
      fetchJson("/api/messages/typing_status?user_id=" + encodeURIComponent(activeUserId))
        .then(function (data) {
          const isTyping = Boolean(data && data.is_typing);
          renderTypingIndicator(
            isTyping ? String((data && data.user_name) || "User") + " is typing..." : "",
            isTyping
          );
        })
        .catch(function () {});
    }

    function startTypingPolling() {
      if (typingPollTimer) return;
      typingPollTimer = window.setInterval(pollTypingStatus, 2000);
    }

    function pollThreadUpdates() {
      if (!activeUserId) return;
      fetchJson("/api/messages/thread?user_id=" + encodeURIComponent(activeUserId))
        .then(function (data) {
          const rows = Array.isArray(data.messages) ? data.messages : [];
          syncThread(rows, { replace: false });
          if (rows.some(function (item) { return !item.is_mine && !item.is_read; })) {
            fetchJson("/api/messages/mark-read", {
              method: "POST",
              headers: { "Content-Type": "application/json", Accept: "application/json" },
              body: JSON.stringify({ user_id: Number(activeUserId) })
            }).catch(function () {});
          }
        })
        .catch(function () {});
    }

    function startThreadPolling() {
      if (threadPollTimer) return;
      threadPollTimer = window.setInterval(pollThreadUpdates, 2000);
    }

    function filterRows(rows, kind) {
      const term = String(searchInput && searchInput.value || "").trim().toLowerCase();
      if (!term) return rows;
      return rows.filter(function (row) {
        const person = row.other_user || row;
        const haystack = [
          person && person.name,
          person && person.company_name,
          person && person.email,
          kind === "conversation" && row.last_message ? row.last_message.message : ""
        ].join(" ").toLowerCase();
        return haystack.indexOf(term) !== -1;
      });
    }

    function renderPeople(target, rows, emptyLabel, activeId, kind) {
      if (!rows.length) {
        target.innerHTML = '<div class="collab-empty-state collab-empty-state--compact">' + escapeHtml(emptyLabel) + '</div>';
        return;
      }
      target.innerHTML = rows.map(function (row) {
        const person = row.other_user || row;
        const preview = kind === "conversation"
          ? String(row.last_message && row.last_message.message || "No messages yet.")
          : (personMeta(person) || "Start a new conversation");
        const timeLabel = kind === "conversation" && row.last_message ? String(row.last_message.created_at || "") : "";
        const unread = kind === "conversation" && row.unread_count
          ? '<span class="chat-widget__conversation-badge">' + Number(row.unread_count) + '</span>'
          : '';
        return '' +
          '<button type="button" class="chat-widget__conversation-row' + (Number(person.id) === Number(activeId) ? ' is-active' : '') + '" data-user-id="' + Number(person.id) + '">' +
            '<div class="chat-widget__person-row">' +
              renderAvatar(person) +
              '<div class="chat-widget__conversation-copy">' +
                '<div class="chat-widget__conversation-topline">' +
                  '<span class="chat-widget__conversation-name-wrap"><span class="chat-widget__conversation-name">' + escapeHtml(person.name) + '</span><span class="chat-widget__online-dot" aria-hidden="true"></span></span>' +
                  (timeLabel ? '<span class="chat-widget__conversation-time">' + escapeHtml(timeLabel) + '</span>' : '') +
                '</div>' +
                '<div class="chat-widget__conversation-preview">' + escapeHtml(preview) + '</div>' +
              '</div>' +
            '</div>' +
            unread +
          '</button>';
      }).join("");
    }

    function renderLists() {
      syncPanelDensityState();
      renderPeople(
        conversationsList,
        filterRows(conversations, "conversation"),
        "No conversations yet.",
        activeUserId,
        "conversation"
      );
      renderPeople(
        contactsList,
        filterRows(contacts, "contact"),
        "No available contacts.",
        activeUserId,
        "contact"
      );
    }

    async function loadContacts() {
      const data = await fetchJson("/api/messages/contacts");
      contacts = Array.isArray(data.contacts) ? data.contacts : [];
      renderLists();
    }

    async function loadConversations() {
      const data = await fetchJson("/api/messages/conversations");
      setBadge(unreadBadge, data.unread_count);
      conversations = Array.isArray(data.conversations) ? data.conversations : [];
      renderLists();
    }

    async function openThread(userId) {
      const data = await fetchJson("/api/messages/thread?user_id=" + encodeURIComponent(userId));
      activeUserId = Number(userId);
      activeContact = data.contact || null;
      renderThreadTitle(activeContact);
      renderThread(Array.isArray(data.messages) ? data.messages : []);
      setThreadOpen(true);
      setPanelOpen(true);
      await fetchJson("/api/messages/mark-read", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ user_id: Number(userId) })
      }).catch(function () {});
      sendTypingState(false);
      stopThreadPolling();
      startThreadPolling();
      startTypingPolling();
      pollTypingStatus();
      loadConversations().catch(function () {});
      loadContacts().catch(function () {});
      updateComposerState();
    }

    function openChatForUser(userId) {
      const normalizedUserId = Number(userId);
      if (!normalizedUserId) {
        return Promise.resolve();
      }
      return openThread(normalizedUserId).catch(function () {
        setPanelOpen(true);
        loadConversations().catch(function () {});
        loadContacts().catch(function () {});
      });
    }

    launcherButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        const nextState = !isPanelOpen();
        setPanelOpen(nextState);
        if (nextState) {
          loadConversations().catch(function () {});
          loadContacts().catch(function () {});
        }
      });
    });

    if (closeBtn) {
      closeBtn.addEventListener("click", function () {
        setPanelOpen(false);
      });
    }

    if (threadMinimizeBtn) {
      threadMinimizeBtn.addEventListener("click", function () {
        sendTypingState(false);
        setThreadOpen(false);
      });
    }

    if (threadCloseBtn) {
      threadCloseBtn.addEventListener("click", function () {
        activeUserId = null;
        activeContact = null;
        renderThreadTitle(null);
        renderThread([]);
        if (input) {
          input.value = "";
        }
        clearAttachmentPreview();
        autoResizeComposer();
        sendTypingState(false);
        stopTypingTimers();
        stopTypingPolling();
        stopThreadPolling();
        renderTypingIndicator("", false);
        setThreadOpen(false);
        renderLists();
        updateComposerState();
      });
    }

    composeButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        setPanelOpen(true);
        window.setTimeout(function () {
          if (searchInput) {
            searchInput.focus();
          }
        }, 120);
        loadConversations().catch(function () {});
        loadContacts().catch(function () {});
      });
    });

    document.querySelectorAll("[data-open-chat]").forEach(function (button) {
      button.addEventListener("click", function (event) {
        event.preventDefault();
        setPanelOpen(true);
        loadConversations().catch(function () {});
        loadContacts().catch(function () {});
        window.setTimeout(function () {
          if (searchInput) {
            searchInput.focus();
          }
        }, 120);
      });
    });

    document.querySelectorAll("[data-open-chat-user]").forEach(function (button) {
      button.addEventListener("click", function (event) {
        event.preventDefault();
        openChatForUser(button.getAttribute("data-open-chat-user"));
      });
    });

    document.addEventListener("open-chat-user", function (event) {
      const detail = event && event.detail ? event.detail : {};
      openChatForUser(detail.userId);
    });

    window.openChatWithUser = openChatForUser;

    [conversationsList, contactsList].forEach(function (container) {
      container.addEventListener("click", function (event) {
        const button = event.target.closest("[data-user-id]");
        if (!button) return;
        openThread(Number(button.getAttribute("data-user-id"))).catch(function () {});
      });
    });

    if (searchInput) {
      searchInput.addEventListener("input", function () {
        renderLists();
      });
    }

    if (input) {
      input.addEventListener("input", function () {
        autoResizeComposer();
        updateComposerState();
        queueTypingSignal();
      });
      input.addEventListener("keydown", function (event) {
        if (event.key === "Enter" && !event.shiftKey) {
          event.preventDefault();
          if (form && !sendButton.disabled) {
            form.requestSubmit();
          }
        }
      });
    }

    if (imageTrigger && imageInput) {
      imageTrigger.addEventListener("click", function () {
        imageInput.click();
      });
      imageInput.addEventListener("change", function () {
        const file = imageInput.files && imageInput.files[0] ? imageInput.files[0] : null;
        if (!file) return;
        if (fileInput) fileInput.value = "";
        setAttachment(file);
      });
    }

    if (fileTrigger && fileInput) {
      fileTrigger.addEventListener("click", function () {
        fileInput.click();
      });
      fileInput.addEventListener("change", function () {
        const file = fileInput.files && fileInput.files[0] ? fileInput.files[0] : null;
        if (!file) return;
        if (imageInput) imageInput.value = "";
        setAttachment(file);
      });
    }

    if (emojiToggle && emojiPicker) {
      emojiToggle.addEventListener("click", function () {
        emojiPicker.hidden = !emojiPicker.hidden;
      });
      emojiPicker.addEventListener("click", function (event) {
        const button = event.target.closest("[data-chat-emoji]");
        if (!button || !input) return;
        const emoji = String(button.textContent || "");
        const start = Number(input.selectionStart || input.value.length);
        const end = Number(input.selectionEnd || input.value.length);
        const currentValue = String(input.value || "");
        input.value = currentValue.slice(0, start) + emoji + currentValue.slice(end);
        input.focus();
        input.selectionStart = input.selectionEnd = start + emoji.length;
        emojiPicker.hidden = true;
        autoResizeComposer();
        updateComposerState();
      });
      document.addEventListener("click", function (event) {
        if (!emojiPicker.hidden && !emojiPicker.contains(event.target) && !emojiToggle.contains(event.target)) {
          emojiPicker.hidden = true;
        }
      });
    }

    form.addEventListener("submit", function (event) {
      event.preventDefault();
      if (!activeUserId) return;
      const message = String(input.value || "").trim();
      const attachmentNote = selectedAttachment
        ? ("\n\n" + (String(selectedAttachment.type || "").indexOf("image/") === 0 ? "[Image attached: " : "[Attachment: ") + String(selectedAttachment.name || "file") + "]")
        : "";
      const bodyToSend = (message + attachmentNote).trim();
      if (!bodyToSend) return;
      input.disabled = true;
      if (sendButton) sendButton.disabled = true;
      sendTypingState(false);
      stopTypingTimers();
      updateComposerState();
      fetchJson("/api/messages/send", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ receiver_id: activeUserId, message: bodyToSend })
      }).then(function (data) {
        input.value = "";
        clearAttachmentPreview();
        autoResizeComposer();
        if (data && data.message) {
          const existing = messagesWrap.querySelector('[data-message-id="' + Number(data.message.id) + '"]');
          if (!existing) {
            messagesWrap.insertAdjacentHTML("beforeend", messageHtml(data.message, true));
          } else {
            updateMessageElement(existing, data.message);
          }
          currentThreadMessageIds.push(Number(data.message.id));
          currentThreadMessageIds = currentThreadMessageIds.filter(function (value, index, arr) {
            return arr.indexOf(value) === index;
          });
          scrollMessagesToBottom();
        }
        loadConversations().catch(function () {});
      }).catch(function () {
      }).finally(function () {
        input.disabled = false;
        input.focus();
        updateComposerState();
      });
    });

    loadConversations().catch(function () {});
    loadContacts().catch(function () {});
    renderThreadTitle(null);
    renderTypingIndicator("", false);
    autoResizeComposer();
    updateComposerState();
    window.setInterval(function () {
      loadConversations().catch(function () {});
      if (activeUserId) {
        pollThreadUpdates();
        pollTypingStatus();
      }
    }, 10000);
  }

  document.querySelectorAll("[data-collab-search]").forEach(function (searchRoot) {
    const toggle = searchRoot.querySelector(".collab-search__toggle");
    const input = searchRoot.querySelector(".collab-search__input");
    const submit = searchRoot.querySelector(".collab-search__submit");
    if (!toggle || !input) return;

    function setOpen(open) {
      searchRoot.classList.toggle("is-open", open);
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
      if (!open) {
        input.blur();
      }
    }

    if (searchRoot.classList.contains("is-persistent")) {
      setOpen(true);
    }

    toggle.addEventListener("click", function () {
      const shouldOpen = !searchRoot.classList.contains("is-open");
      setOpen(shouldOpen);
      if (shouldOpen) {
        window.setTimeout(function () { input.focus(); }, 140);
      } else if (String(input.value || "").trim()) {
        submit?.click();
      }
    });

    input.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && !searchRoot.classList.contains("is-persistent")) {
        input.value = "";
        setOpen(false);
      }
    });

    document.addEventListener("click", function (event) {
      if (searchRoot.classList.contains("is-persistent")) return;
      if (!searchRoot.contains(event.target)) {
        if (!String(input.value || "").trim()) {
          setOpen(false);
        }
      }
    });
  });
})();
