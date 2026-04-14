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
    const launcher = chatRoot.querySelector("[data-chat-toggle]");
    const panel = chatRoot.querySelector(".chat-widget__panel");
    const closeBtn = chatRoot.querySelector("[data-chat-close]");
    const conversationsList = chatRoot.querySelector("[data-chat-conversations]");
    const contactsList = chatRoot.querySelector("[data-chat-contacts]");
    const messagesWrap = chatRoot.querySelector("[data-chat-messages]");
    const form = chatRoot.querySelector("[data-chat-form]");
    const input = chatRoot.querySelector("[data-chat-input]");
    const title = chatRoot.querySelector("[data-chat-thread-title]");
    const unreadBadge = chatRoot.querySelector("[data-chat-unread-badge]");
    let activeUserId = null;

    function setOpen(open) {
      panel.hidden = !open;
      launcher.setAttribute("aria-expanded", open ? "true" : "false");
    }

    function renderThread(messages) {
      if (!messages.length) {
        messagesWrap.innerHTML = '<div class="collab-empty-state">No messages yet.</div>';
        return;
      }
      messagesWrap.innerHTML = messages.map(function (item) {
        return '' +
          '<article class="chat-widget__message' + (item.is_mine ? ' is-mine' : '') + '">' +
            '<div>' + escapeHtml(item.message) + '</div>' +
            '<div class="chat-widget__message-meta">' + escapeHtml(item.sender_name) + ' · ' + escapeHtml(item.created_at) + '</div>' +
          '</article>';
      }).join("");
      messagesWrap.scrollTop = messagesWrap.scrollHeight;
    }

    function renderPeople(target, rows, emptyLabel, activeId) {
      if (!rows.length) {
        target.innerHTML = '<div class="collab-empty-state">' + escapeHtml(emptyLabel) + '</div>';
        return;
      }
      target.innerHTML = rows.map(function (row) {
        const person = row.other_user || row;
        const lastMessage = row.last_message ? '<div class="chat-widget__meta">' + escapeHtml(row.last_message.message) + '</div>' : '';
        const unread = row.unread_count ? '<span class="search-result-item__kind">' + Number(row.unread_count) + '</span>' : '';
        return '' +
          '<button type="button" data-user-id="' + Number(person.id) + '"' + (Number(person.id) === Number(activeId) ? ' class="is-active"' : '') + '>' +
            '<div class="chat-widget__person-row">' +
              renderAvatar(person) +
              '<div class="chat-widget__person-copy">' +
                '<div class="collab-notification-item__title">' + escapeHtml(person.name) + '</div>' +
                '<div class="chat-widget__meta">' + escapeHtml(person.company_name || person.email || '') + '</div>' +
                lastMessage +
              '</div>' +
            '</div>' +
            unread +
          '</button>';
      }).join("");
    }

    async function loadContacts() {
      const data = await fetchJson("/api/messages/contacts");
      renderPeople(contactsList, Array.isArray(data.contacts) ? data.contacts : [], "No available contacts.", activeUserId);
    }

    async function loadConversations() {
      const data = await fetchJson("/api/messages/conversations");
      setBadge(unreadBadge, data.unread_count);
      renderPeople(conversationsList, Array.isArray(data.conversations) ? data.conversations : [], "No conversations yet.", activeUserId);
    }

    async function openThread(userId) {
      const data = await fetchJson("/api/messages/thread?user_id=" + encodeURIComponent(userId));
      activeUserId = Number(userId);
      title.innerHTML = data.contact ? renderPersonHeading(data.contact) : "Conversation";
      renderThread(Array.isArray(data.messages) ? data.messages : []);
      await fetchJson("/api/messages/mark-read", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ user_id: Number(userId) })
      }).catch(function () {});
      loadConversations().catch(function () {});
      loadContacts().catch(function () {});
    }

    launcher.addEventListener("click", function () {
      const nextState = panel.hidden;
      setOpen(nextState);
      if (nextState) {
        loadConversations().catch(function () {});
        loadContacts().catch(function () {});
      }
    });

    closeBtn.addEventListener("click", function () {
      setOpen(false);
    });

    document.querySelectorAll("[data-open-chat]").forEach(function (button) {
      button.addEventListener("click", function (event) {
        event.preventDefault();
        setOpen(true);
        loadConversations().catch(function () {});
        loadContacts().catch(function () {});
      });
    });

    [conversationsList, contactsList].forEach(function (container) {
      container.addEventListener("click", function (event) {
        const button = event.target.closest("[data-user-id]");
        if (!button) return;
        openThread(Number(button.getAttribute("data-user-id"))).catch(function () {});
      });
    });

    form.addEventListener("submit", function (event) {
      event.preventDefault();
      if (!activeUserId) return;
      const message = String(input.value || "").trim();
      if (!message) return;
      input.disabled = true;
      fetchJson("/api/messages/send", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ receiver_id: activeUserId, message: message })
      }).then(function () {
        input.value = "";
        openThread(activeUserId).catch(function () {});
      }).catch(function () {
      }).finally(function () {
        input.disabled = false;
        input.focus();
      });
    });

    loadConversations().catch(function () {});
    loadContacts().catch(function () {});
    window.setInterval(function () {
      loadConversations().catch(function () {});
      if (activeUserId) {
        openThread(activeUserId).catch(function () {});
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
