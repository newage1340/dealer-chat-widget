(function () {
  // --- dealer slug (set by the template via <body data-dealer-slug="...">) ---
  // Identifies which dealer's bot/inventory to talk to. Empty = let the
  // server fall back to the legacy WIDGET_DEALER_TWILIO_NUM env var.
  const dealerSlug = document.body.dataset.dealerSlug || "";

  // --- session id (persists in localStorage, scoped per dealer so two
  //     widgets on the same browser don't collide) ---
  const sessionStorageKey = "dealer_chat_session_id" + (dealerSlug ? ":" + dealerSlug : "");
  let sessionId = localStorage.getItem(sessionStorageKey);
  if (!sessionId) {
    sessionId = "sess_" + Math.random().toString(36).slice(2) + Date.now().toString(36);
    localStorage.setItem(sessionStorageKey, sessionId);
  }

  // --- terms gate ---
  // Customer must accept terms before entering the chat. Phone number is
  // collected later in-chat by a proactive welcome message from the bot.
  const termsStorageKey = "dealer_chat_terms_accepted" + (dealerSlug ? ":" + dealerSlug : "");
  // Legacy key kept only to read previously saved phones for the chat post body
  // (so existing customers don't lose the SMS bridge after we drop the phone gate).
  const phoneStorageKey = "dealer_chat_phone" + (dealerSlug ? ":" + dealerSlug : "");
  const phoneGate       = document.getElementById("phoneGate");
  const phoneForm       = document.getElementById("phoneGateForm");
  const phoneSubmit     = document.getElementById("phoneGateSubmit");
  const phoneError      = document.getElementById("phoneGateError");
  const termsCheckbox   = document.getElementById("phoneGateTerms");
  const privacyCheckbox = document.getElementById("phoneGatePrivacy");

  function showPhoneGate() {
    if (phoneGate) phoneGate.hidden = false;
    // Reset both checkboxes to unchecked every time the gate is shown —
    // defense against any browser-restore scenario that might leave them
    // checked.
    if (termsCheckbox)   termsCheckbox.checked   = false;
    if (privacyCheckbox) privacyCheckbox.checked = false;
  }
  function hidePhoneGate() {
    if (phoneGate) phoneGate.hidden = true;
  }

  // --- profile form (name + phone) ---
  // Rendered INLINE inside the chat window as a regular bot bubble. Replaces
  // the old modal so it feels like part of the conversation. There's only
  // ever one of these on screen at a time.
  let activeProfileBubble = null;
  // The "Sure, I can help with that. Before I do, could I get..." gate message
  // is shown right before the profile form. Tracked so we can remove it from
  // the chat once the customer submits — the prompt is no longer relevant.
  let activeProfileGateMsg = null;

  function formatPhoneInput(input) {
    const d = input.value.replace(/\D/g, "").slice(0, 10);
    if (d.length <= 3) {
      input.value = d;
    } else if (d.length <= 6) {
      input.value = "(" + d.slice(0, 3) + ") " + d.slice(3);
    } else {
      input.value = "(" + d.slice(0, 3) + ") " + d.slice(3, 6) + "-" + d.slice(6);
    }
  }

  function appendProfileFormBubble() {
    // Treat the bubble as missing if its DOM node is no longer attached (e.g.
    // after a Clear Chat that wiped chatWindow.innerHTML but left the JS
    // reference dangling).
    if (activeProfileBubble && !activeProfileBubble.isConnected) {
      activeProfileBubble = null;
    }
    if (activeProfileBubble) return; // don't double-append
    const wrap = document.createElement("div");
    wrap.className = "message assistant";
    const avatar = document.createElement("div");
    avatar.className = "avatar";
    avatar.textContent = "D";
    const bubble = document.createElement("div");
    bubble.className = "bubble profile-form-bubble";

    const nameInput = document.createElement("input");
    nameInput.type = "text";
    nameInput.placeholder = "First name";
    nameInput.autocomplete = "given-name";
    nameInput.className = "profile-input";

    const phoneInput = document.createElement("input");
    phoneInput.type = "tel";
    phoneInput.placeholder = "10-digit phone";
    phoneInput.autocomplete = "tel";
    phoneInput.inputMode = "numeric";
    phoneInput.maxLength = 14;
    phoneInput.className = "profile-input";
    phoneInput.addEventListener("input", function () { formatPhoneInput(this); });

    const submitBtn = document.createElement("button");
    submitBtn.type = "button";
    submitBtn.textContent = "Continue";
    submitBtn.className = "profile-submit";

    const errorEl = document.createElement("div");
    errorEl.className = "profile-error";

    bubble.appendChild(nameInput);
    bubble.appendChild(phoneInput);
    bubble.appendChild(submitBtn);
    bubble.appendChild(errorEl);

    wrap.appendChild(avatar);
    wrap.appendChild(bubble);
    chatWindow.appendChild(wrap);
    chatWindow.scrollTop = chatWindow.scrollHeight;
    nameInput.focus();

    activeProfileBubble = wrap;

    submitBtn.addEventListener("click", async function (e) {
      e.preventDefault();
      const name = nameInput.value.trim();
      const phoneRaw = phoneInput.value.trim();
      const digits = phoneRaw.replace(/\D/g, "");
      if (!name) { errorEl.textContent = "Please enter your first name."; return; }
      if (digits.length !== 10) { errorEl.textContent = "Please enter a 10-digit phone number."; return; }
      errorEl.textContent = "";
      submitBtn.disabled = true;
      nameInput.disabled = true;
      phoneInput.disabled = true;
      try {
        const r = await fetch("/widget/profile", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            session_id: sessionId,
            slug: dealerSlug,
            name: name,
            phone: digits,
          }),
        });
        const data = await r.json();
        if (!r.ok || !data.ok) {
          errorEl.textContent = data.error || "Could not save - please try again.";
          submitBtn.disabled = false;
          nameInput.disabled = false;
          phoneInput.disabled = false;
          return;
        }
        localStorage.setItem(phoneStorageKey, "+1" + digits);
        // Remove the form bubble from the chat now that it's done.
        if (activeProfileBubble && activeProfileBubble.parentNode) {
          activeProfileBubble.parentNode.removeChild(activeProfileBubble);
        }
        activeProfileBubble = null;
        // Also remove the "Sure, I can help — first I need your name/phone"
        // prompt that preceded the form. Once submitted, the prompt is noise.
        if (activeProfileGateMsg && activeProfileGateMsg.parentNode) {
          activeProfileGateMsg.parentNode.removeChild(activeProfileGateMsg);
        }
        activeProfileGateMsg = null;
        // Auto-resend the original question.
        if (pendingMessage) {
          const msg = pendingMessage;
          pendingMessage = "";
          appendTyping();
          try {
            const data2 = await postChat(msg);
            removeTyping();
            handleChatResponse(data2, msg);
          } catch (_) {
            removeTyping();
            appendMessage("assistant", "Network error - please check your connection and try again.");
          }
        }
      } catch (err) {
        errorEl.textContent = "Network error - please try again.";
        submitBtn.disabled = false;
        nameInput.disabled = false;
        phoneInput.disabled = false;
      }
    });
  }

  function appendPrimerBubble(text) {
    const wrap = document.createElement("div");
    wrap.className = "message assistant primer";
    const avatar = document.createElement("div");
    avatar.className = "avatar";
    avatar.textContent = "i";
    const bubble = document.createElement("div");
    bubble.className = "bubble primer-bubble";
    bubble.textContent = text;
    bubble.innerHTML = bubble.innerHTML.replace(
      /(https?:\/\/[^\s)]+)/g,
      '<a href="$1" target="_blank" rel="noopener">$1</a>'
    );
    wrap.appendChild(avatar);
    wrap.appendChild(bubble);
    chatWindow.appendChild(wrap);
    chatWindow.scrollTop = chatWindow.scrollHeight;
  }

  async function fetchWelcome() {
    try {
      const r = await fetch("/widget/welcome", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: sessionId, slug: dealerSlug }),
      });
      const data = await r.json();
      if (r.ok && data.welcome) {
        appendMessage("assistant", data.welcome);
        if (data.welcome_followup) {
          appendMessage("assistant", data.welcome_followup);
        }
      }
    } catch (err) {
      // Non-fatal: customer can still chat normally.
    }
  }

  async function fetchHistory() {
    try {
      const r = await fetch("/widget/history", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: sessionId, slug: dealerSlug }),
      });
      const data = await r.json();
      if (r.ok && Array.isArray(data.messages) && data.messages.length) {
        for (const m of data.messages) {
          appendMessage(m.role === "user" ? "user" : "assistant", m.content || "");
        }
      }
    } catch (err) {
      // Non-fatal: customer just sees an empty window.
    }
  }

  function syncGateFromStorage() {
    if (localStorage.getItem(termsStorageKey)) {
      hidePhoneGate();
    } else {
      showPhoneGate();
    }
  }
  syncGateFromStorage();

  // If terms are already accepted (returning visitor), pull saved history so
  // the chat doesn't appear empty after a refresh.
  if (localStorage.getItem(termsStorageKey)) {
    fetchHistory();
  }

  // Re-sync the gate whenever the page is restored from BFCache (mobile
  // Safari resuming a backgrounded tab) — without this the gate's visible
  // state can desync from localStorage and appear skipped on return.
  window.addEventListener("pageshow", syncGateFromStorage);

  // Click-only acceptance: the gate is no longer a <form>, so Enter key
  // presses, browser autofill, and other "implicit submit" vectors can't
  // trigger acceptance. Acceptance ONLY happens on a real click of the
  // Enter chat button while the checkbox is also checked. Also requires
  // termsCheckbox to be a real element — a defensive null-check that
  // refuses to auto-accept if the checkbox is missing.
  if (phoneSubmit) {
    phoneSubmit.addEventListener("click", function (e) {
      e.preventDefault();
      if (!termsCheckbox || !termsCheckbox.checked) {
        if (phoneError) phoneError.textContent = "Please accept the Terms of Service to continue.";
        return;
      }
      if (!privacyCheckbox || !privacyCheckbox.checked) {
        if (phoneError) phoneError.textContent = "Please accept the Privacy Policy to continue.";
        return;
      }
      if (phoneError) phoneError.textContent = "";
      localStorage.setItem(termsStorageKey, "1");
      hidePhoneGate();
      fetchWelcome();
    });
  }


  const chatWindow = document.getElementById("chatWindow");
  const userInput  = document.getElementById("userInput");
  const sendBtn    = document.getElementById("sendBtn");

  // --- mobile menu toggle ---
  const sidebar      = document.getElementById("sidebar");
  const menuToggle   = document.getElementById("menuToggle");
  const sidebarClose = document.getElementById("sidebarClose");
  if (menuToggle)   menuToggle.addEventListener("click",  () => sidebar.classList.add("open"));
  if (sidebarClose) sidebarClose.addEventListener("click", () => sidebar.classList.remove("open"));

  // --- DOM helpers ---
  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function autoLinkEscaped(escapedText) {
    return escapedText.replace(
      /(https?:\/\/[^\s)<]+)/g,
      function (url) {
        return '<a href="' + url + '" target="_blank" rel="noopener">' + url + "</a>";
      }
    );
  }

  function renderRichText(text) {
    // Markdown-style links: [label](https://url) → <a>label</a>.
    // Bare URLs in surrounding text get auto-linked. Crucially, auto-linking
    // only runs on plain-text segments (NEVER on text that's already been
    // converted to HTML) so the URL inside a markdown link's href doesn't
    // get re-linked into a second, broken anchor.
    const parts = [];
    let i = 0;
    const re = /\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g;
    let m;
    while ((m = re.exec(text)) !== null) {
      if (m.index > i) {
        parts.push(autoLinkEscaped(escapeHtml(text.slice(i, m.index))));
      }
      parts.push(
        '<a href="' + escapeHtml(m[2]) + '" target="_blank" rel="noopener">' +
        escapeHtml(m[1]) + "</a>"
      );
      i = m.index + m[0].length;
    }
    if (i < text.length) {
      parts.push(autoLinkEscaped(escapeHtml(text.slice(i))));
    }
    return parts.join("");
  }

  function appendMessage(role, text) {
    const wrap = document.createElement("div");
    wrap.className = "message " + (role === "user" ? "user" : "assistant");
    const avatar = document.createElement("div");
    avatar.className = "avatar";
    avatar.textContent = role === "user" ? "You" : "D";
    const bubble = document.createElement("div");
    bubble.className = "bubble";
    bubble.innerHTML = renderRichText(text);
    wrap.appendChild(avatar);
    wrap.appendChild(bubble);
    chatWindow.appendChild(wrap);
    chatWindow.scrollTop = chatWindow.scrollHeight;
    return wrap;
  }

  function appendTyping() {
    const wrap = document.createElement("div");
    wrap.className = "message assistant";
    wrap.id = "typingIndicator";
    const avatar = document.createElement("div");
    avatar.className = "avatar";
    avatar.textContent = "D";
    const bubble = document.createElement("div");
    bubble.className = "bubble";
    bubble.innerHTML = '<span class="typing-bubble"><span></span><span></span><span></span></span>';
    wrap.appendChild(avatar);
    wrap.appendChild(bubble);
    chatWindow.appendChild(wrap);
    chatWindow.scrollTop = chatWindow.scrollHeight;
  }

  function removeTyping() {
    const t = document.getElementById("typingIndicator");
    if (t) t.remove();
  }

  // Holds the customer's question if the server replies with needs_profile;
  // resent automatically once the profile form is submitted.
  let pendingMessage = "";

  async function postChat(message) {
    const r = await fetch("/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: sessionId,
        message: message,
        slug: dealerSlug,
        real_phone: localStorage.getItem(phoneStorageKey) || "",
      }),
    });
    return r.json();
  }

  // Shared response handler so the initial send AND the post-form auto-resend
  // both handle needs_profile / silent / primer the same way.
  function handleChatResponse(data, originalMessage) {
    if (data.needs_profile) {
      if (data.reply) {
        activeProfileGateMsg = appendMessage("assistant", data.reply);
      }
      pendingMessage = data.pending_message || originalMessage;
      appendProfileFormBubble();
      return;
    }
    if (data.silent) {
      // intentional no-op
    } else if (data.reply) {
      appendMessage("assistant", data.reply);
    } else {
      appendMessage("assistant", "Sorry, something went wrong. Please try again.");
    }
    if (data.primer) {
      const wrap = document.createElement("div");
      wrap.className = "message assistant primer";
      const avatar = document.createElement("div");
      avatar.className = "avatar";
      avatar.textContent = "i";
      const bubble = document.createElement("div");
      bubble.className = "bubble primer-bubble";
      bubble.innerHTML = renderRichText(data.primer);
      wrap.appendChild(avatar);
      wrap.appendChild(bubble);
      chatWindow.appendChild(wrap);
      chatWindow.scrollTop = chatWindow.scrollHeight;
    }
  }

  // --- send a message ---
  async function sendMessage() {
    const message = userInput.value.trim();
    if (!message) return;

    appendMessage("user", message);
    userInput.value = "";
    userInput.style.height = "auto";
    sendBtn.disabled = true;
    appendTyping();

    try {
      const data = await postChat(message);
      removeTyping();
      handleChatResponse(data, message);
    } catch (err) {
      removeTyping();
      appendMessage("assistant", "Network error - please check your connection and try again.");
    } finally {
      sendBtn.disabled = false;
      userInput.focus();
    }
  }

  // --- shortcuts exposed to inline onclick handlers in HTML ---
  window.sendMessage = sendMessage;

  window.askTopic = function (text) {
    userInput.value = text;
    sendMessage();
    sidebar.classList.remove("open");
  };

  window.clearChat = async function () {
    if (!confirm("Clear this conversation?")) return;

    // Tell the server to wipe everything tied to this session BEFORE we reset
    // local state, so follow-ups / lead reminders stop firing against the old
    // conversation. Failure is non-fatal — the local reset still proceeds so
    // the customer isn't stuck.
    try {
      await fetch("/widget/clear-session", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: sessionId,
          slug: dealerSlug,
          // Pass the cached real phone so the server can wipe ALL sibling
          // sessions tied to this human - not just the current one. Without
          // this, old appointments / dedup flags from previous sessions
          // linger and the next test goes silent.
          real_phone: localStorage.getItem(phoneStorageKey) || "",
        }),
      });
    } catch (err) { /* non-fatal */ }

    chatWindow.innerHTML = "";
    localStorage.removeItem(sessionStorageKey);
    localStorage.removeItem(phoneStorageKey);
    localStorage.removeItem(termsStorageKey);
    sessionId = "sess_" + Math.random().toString(36).slice(2) + Date.now().toString(36);
    localStorage.setItem(sessionStorageKey, sessionId);
    showPhoneGate();
  };

  window.handleKey = function (e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  // --- auto-resize textarea ---
  userInput.addEventListener("input", function () {
    this.style.height = "auto";
    this.style.height = Math.min(this.scrollHeight, 120) + "px";
  });

  userInput.focus();
})();
