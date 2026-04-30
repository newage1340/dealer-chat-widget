(function () {
  // --- session id (persists in localStorage so chat survives page refresh) ---
  let sessionId = localStorage.getItem("dealer_chat_session_id");
  if (!sessionId) {
    sessionId = "sess_" + Math.random().toString(36).slice(2) + Date.now().toString(36);
    localStorage.setItem("dealer_chat_session_id", sessionId);
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
        body: JSON.stringify({ session_id: sessionId, message: message }),
      });
      const data = await r.json();
      removeTyping();
      if (data.reply) {
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

  window.clearChat = function () {
    if (!confirm("Clear this conversation?")) return;
    chatWindow.innerHTML = "";
    localStorage.removeItem("dealer_chat_session_id");
    sessionId = "sess_" + Math.random().toString(36).slice(2) + Date.now().toString(36);
    localStorage.setItem("dealer_chat_session_id", sessionId);
    appendMessage("assistant",
      "Chat cleared. How can I help you find a vehicle today?");
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
