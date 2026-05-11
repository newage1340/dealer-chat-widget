/* twilio-bot3 widget embed loader.
 *
 * Dealer pastes this on their site:
 *   <script src="https://YOUR-RENDER-URL/embed.js?dealer=<slug>"></script>
 *
 * Loader creates a floating chat bubble in the bottom-right corner. Clicking
 * the bubble opens a popup iframe pointing at /widget/<slug>, which loads the
 * full chat UI scoped to that dealer. The popup re-uses the dealer's brand
 * color and logo (from the Google Sheet) so it feels native to their site.
 */
(function () {
  // Find the script tag that loaded this file so we can pull the ?dealer= slug
  // and figure out the bot's base URL (so iframe / config calls hit the right host).
  function getOwnScript() {
    if (document.currentScript) return document.currentScript;
    var scripts = document.getElementsByTagName("script");
    for (var i = scripts.length - 1; i >= 0; i--) {
      if ((scripts[i].src || "").indexOf("/embed.js") !== -1) return scripts[i];
    }
    return null;
  }

  var ownScript = getOwnScript();
  if (!ownScript) {
    console.warn("[dealer-chat] could not locate own <script> tag");
    return;
  }

  var srcUrl;
  try { srcUrl = new URL(ownScript.src); }
  catch (e) {
    console.warn("[dealer-chat] invalid script src:", ownScript.src);
    return;
  }

  var dealerSlug = srcUrl.searchParams.get("dealer") || "";
  var botOrigin  = srcUrl.origin; // e.g. "https://yourapp.onrender.com"

  if (!dealerSlug) {
    console.warn("[dealer-chat] missing ?dealer=<slug> in embed snippet");
    return;
  }

  // Don't double-inject if the dealer pasted the snippet twice.
  if (window.__dealerChatLoaded) return;
  window.__dealerChatLoaded = true;

  // ── Inject CSS ────────────────────────────────────────────────────────
  var css = ""
    + ".dealerchat-bubble{position:fixed;bottom:24px;right:24px;height:54px;padding:0 22px 0 18px;border-radius:30px;background:#1f2937;color:#fff;border:none;box-shadow:0 6px 24px rgba(0,0,0,0.25);cursor:pointer;font-size:15px;font-weight:600;display:inline-flex;align-items:center;gap:10px;z-index:2147483646;transition:transform 0.15s,box-shadow 0.15s;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1;}"
    + ".dealerchat-bubble:hover{transform:translateY(-1px);box-shadow:0 8px 28px rgba(0,0,0,0.32);}"
    + ".dealerchat-bubble[aria-expanded=true]{transform:scale(0.96);}"
    + ".dealerchat-bubble .dealerchat-icon{font-size:20px;line-height:1;}"
    + ".dealerchat-panel{position:fixed;bottom:92px;right:24px;width:400px;height:620px;max-height:calc(100vh - 120px);max-width:calc(100vw - 32px);background:#0f1216;border-radius:14px;box-shadow:0 12px 48px rgba(0,0,0,0.45);overflow:hidden;z-index:2147483647;display:none;}"
    + ".dealerchat-panel.dealerchat-open{display:block;}"
    + ".dealerchat-panel iframe{width:100%;height:100%;border:none;display:block;}"
    + ".dealerchat-close{position:absolute;top:8px;right:12px;background:rgba(255,255,255,0.08);border:none;color:#fff;width:30px;height:30px;border-radius:50%;cursor:pointer;font-size:18px;line-height:1;z-index:2;}"
    + ".dealerchat-close:hover{background:rgba(255,255,255,0.18);}"
    + "@media (max-width:480px){.dealerchat-panel{bottom:80px;right:8px;left:8px;width:auto;max-width:none;height:min(620px,calc(100dvh - 100px));max-height:calc(100dvh - 100px);}.dealerchat-bubble{bottom:16px;right:16px;}}";
  var style = document.createElement("style");
  style.textContent = css;
  document.head.appendChild(style);

  // ── Build elements ────────────────────────────────────────────────────
  var bubble = document.createElement("button");
  bubble.className = "dealerchat-bubble";
  bubble.setAttribute("aria-label", "Open chat");
  bubble.setAttribute("aria-expanded", "false");
  bubble.innerHTML =
    '<span class="dealerchat-icon" aria-hidden="true">&#128172;</span>' +
    '<span class="dealerchat-label">Chat now</span>';

  var panel = document.createElement("div");
  panel.className = "dealerchat-panel";

  var closeBtn = document.createElement("button");
  closeBtn.className = "dealerchat-close";
  closeBtn.setAttribute("aria-label", "Close chat");
  closeBtn.innerHTML = "&times;";

  // iframe is created lazily on first open so we don't hammer the bot until
  // someone actually clicks.
  var iframe = null;

  function ensureIframe() {
    if (iframe) return;
    iframe = document.createElement("iframe");
    iframe.src = botOrigin + "/widget/" + encodeURIComponent(dealerSlug);
    iframe.setAttribute("title", "Dealer chat");
    iframe.setAttribute("allow", "clipboard-write");
    panel.appendChild(iframe);
  }

  function open() {
    ensureIframe();
    panel.classList.add("dealerchat-open");
    bubble.setAttribute("aria-expanded", "true");
  }
  function close() {
    panel.classList.remove("dealerchat-open");
    bubble.setAttribute("aria-expanded", "false");
  }
  function toggle() {
    if (panel.classList.contains("dealerchat-open")) close(); else open();
  }

  bubble.addEventListener("click", toggle);
  closeBtn.addEventListener("click", close);

  panel.appendChild(closeBtn);
  document.body.appendChild(panel);
  document.body.appendChild(bubble);

  // ── Color the bubble to match the dealer's brand ──────────────────────
  // Fetch the dealer's branding from the bot so the bubble matches their
  // brand color. Falls back to the dark default if the call fails.
  fetch(botOrigin + "/widget-config?dealer=" + encodeURIComponent(dealerSlug))
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (cfg) {
      if (!cfg) return;
      if (cfg.brand_color) {
        bubble.style.background = cfg.brand_color;
      }
      if (cfg.dealer_name) {
        bubble.setAttribute("aria-label", "Open chat with " + cfg.dealer_name);
      }
    })
    .catch(function () { /* keep defaults on failure */ });
})();
