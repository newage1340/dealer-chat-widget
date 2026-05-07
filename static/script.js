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
  const phoneGate     = document.getElementById("phoneGate");
  const phoneForm     = document.getElementById("phoneGateForm");
  const phoneSubmit   = document.getElementById("phoneGateSubmit");
  const phoneError    = document.getElementById("phoneGateError");
  const termsCheckbox = document.getElementById("phoneGateTerms");

  function showPhoneGate() {
    if (phoneGate) phoneGate.hidden = false;
    // Reset checkbox to unchecked every time the gate is shown — defense
    // against any browser-restore scenario that might leave it checked.
    if (termsCheckbox) termsCheckbox.checked = false;
  }
  function hidePhoneGate() {
    if (phoneGate) phoneGate.hidden = true;
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
        if (data.terms_note) {
          appendPrimerBubble(data.terms_note);
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
        if (phoneError) phoneError.textContent = "Please accept the terms to continue.";
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
  function appendMessage(role, text) {
    const wrap = document.createElement("div");
    wrap.className = "message " + (role === "user" ? "user" : "assistant");
    const avatar = document.createElement("div");
    avatar.className = "avatar";
    avatar.textContent = role === "user" ? "You" : "AI";
    const bubble = document.createElement("div");
    bubble.className = "bubble";
    bubble.textContent = text;
    bubble.innerHTML = bubble.innerHTML.replace(
      /(https?:\/\/[^\s)]+)/g,
      '<a href="$1" target="_blank" rel="noopener">$1</a>'
    );
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
    avatar.textContent = "AI";
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
      const data = await r.json();
      removeTyping();
      if (data.silent) {
        // Server intentionally chose not to respond (e.g. terse ack after
        // a confirmed appointment) - just remove typing, no bubble.
      } else if (data.reply) {
        appendMessage("assistant", data.reply);
      } else {
        appendMessage("assistant", "Sorry, something went wrong. Please try again.");
      }
      // First-time customers also get a primer (terms or capability) — render
      // it as a smaller follow-up bubble so it doesn't compete with the reply.
      if (data.primer) {
        const wrap = document.createElement("div");
        wrap.className = "message assistant primer";
        const avatar = document.createElement("div");
        avatar.className = "avatar";
        avatar.textContent = "i";
        const bubble = document.createElement("div");
        bubble.className = "bubble primer-bubble";
        bubble.textContent = data.primer;
        bubble.innerHTML = bubble.innerHTML.replace(
          /(https?:\/\/[^\s)]+)/g,
          '<a href="$1" target="_blank" rel="noopener">$1</a>'
        );
        wrap.appendChild(avatar);
        wrap.appendChild(bubble);
        chatWindow.appendChild(wrap);
        chatWindow.scrollTop = chatWindow.scrollHeight;
      }
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
        body: JSON.stringify({ session_id: sessionId, slug: dealerSlug }),
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
