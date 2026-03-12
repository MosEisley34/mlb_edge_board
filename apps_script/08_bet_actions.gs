/* ===================== ACTION ROUTING + OPTIONAL LEGACY BET TRACKING ===================== */

function doGet(e) {
  if (!isBetTrackingEnabled_()) return renderBetTrackingDisabledPage_();
  return handleBetActionGet_(e && e.parameter ? e.parameter : {});
}

function doPost(e) {
  if (!isBetTrackingEnabled_()) return renderBetTrackingDisabledPage_();
  return handleBetActionPost_(e && e.parameter ? e.parameter : {});
}

function handleBetActionGet_(p) {
  var action = String(p.action || "").toLowerCase();
  if (action === "test") return renderActionTestPage_(p);
  if (action === "pending") return renderPendingHelp_();
  if (action === "confirm" || action === "mark") return renderLegacyActionRetiredPage_();
  return renderHtmlPage_("Lucky Luciano MLB — Actions", "<p>Use Discord action links to continue.</p>");
}

function renderActionTestPage_(p) {
  var token = String(p.token || "");
  var localNow = localPretty_(new Date());
  return renderHtmlPage_(
    "Lucky Luciano MLB — Action Test",
    "<p>✅ Discord button + web app routing works.</p>" +
    "<p><b>Local:</b> " + htmlEscape_(localNow) + "</p>" +
    "<p><b>Token:</b> <code>" + htmlEscape_(token || "(none)") + "</code></p>" +
    "<p>This endpoint is test-only and does not write to legacy tracking sheets.</p>"
  );
}

function handleBetActionPost_(p) {
  var action = String(p.action || "").toLowerCase();
  if (action === "confirm_submit") return renderLegacyActionRetiredPage_();
  return renderHtmlPage_("Error", "<p>Unknown action.</p>");
}

function createPendingBet_(cfg, args) {
  log_("INFO", "createPendingBet_ is retired; legacy bet tracking is disabled by default.", {});
  return "";
}

function appendBetEvent_(betId, eventName, fromStatus, toStatus, detailObj) {
  log_("INFO", "appendBetEvent_ is retired; legacy bet tracking is disabled by default.", {
    bet_id: String(betId || ""),
    event: String(eventName || "")
  });
}

function buildActionLinks_(cfg, betId) {
  var baseUrl = String(cfg.WEB_APP_URL || "").trim();
  var secret = String(cfg.ACTION_TOKEN_SECRET || "").trim();
  if (!baseUrl || !secret || !betId) return null;

  var ttlMin = Math.max(5, toInt_(cfg.ACTION_TOKEN_TTL_MIN, 60));
  var confirmToken = makeActionToken_(secret, { action: "confirm", bet_id: betId }, ttlMin);
  var markWinToken = makeActionToken_(secret, { action: "mark", bet_id: betId, result: "WIN" }, ttlMin);
  var markLossToken = makeActionToken_(secret, { action: "mark", bet_id: betId, result: "LOSS" }, ttlMin);

  return {
    confirm: baseUrl + "?action=confirm&bet_id=" + encodeURIComponent(betId) + "&token=" + encodeURIComponent(confirmToken),
    markWin: baseUrl + "?action=mark&bet_id=" + encodeURIComponent(betId) + "&result=WIN&token=" + encodeURIComponent(markWinToken),
    markLoss: baseUrl + "?action=mark&bet_id=" + encodeURIComponent(betId) + "&result=LOSS&token=" + encodeURIComponent(markLossToken),
    pending: baseUrl + "?action=pending"
  };
}

function makeActionToken_(secret, claimsObj, ttlMin) {
  var expMs = new Date().getTime() + Math.max(1, ttlMin) * 60000;
  var payload = {
    action: String(claimsObj.action || ""),
    bet_id: String(claimsObj.bet_id || ""),
    result: String(claimsObj.result || ""),
    exp: expMs,
    nonce: Utilities.getUuid()
  };
  var payloadB64 = Utilities.base64EncodeWebSafe(JSON.stringify(payload));
  var sigBytes = Utilities.computeHmacSha256Signature(payloadB64, secret);
  var sigB64 = Utilities.base64EncodeWebSafe(sigBytes);
  return payloadB64 + "." + sigB64;
}

function verifyActionTokenFromParam_(token, expectedAction, expectedBetId) {
  var cfg = getConfig_();
  var secret = String(cfg.ACTION_TOKEN_SECRET || "").trim();
  if (!secret) return { ok: false, error: "ACTION_TOKEN_SECRET is missing." };

  var tok = String(token || "");
  if (!tok || tok.indexOf(".") < 0) return { ok: false, error: "Invalid token." };
  var parts = tok.split(".");
  if (parts.length !== 2) return { ok: false, error: "Invalid token format." };

  var payloadB64 = parts[0], sigB64 = parts[1];
  var expectedSig = Utilities.base64EncodeWebSafe(Utilities.computeHmacSha256Signature(payloadB64, secret));
  if (expectedSig !== sigB64) return { ok: false, error: "Token signature mismatch." };

  var payload;
  try {
    payload = JSON.parse(Utilities.newBlob(Utilities.base64DecodeWebSafe(payloadB64)).getDataAsString());
  } catch (e) {
    return { ok: false, error: "Token payload decode failed." };
  }

  if (String(payload.action || "") !== String(expectedAction || "")) return { ok: false, error: "Token action mismatch." };
  if (String(payload.bet_id || "") !== String(expectedBetId || "")) return { ok: false, error: "Token bet mismatch." };
  if (!payload.exp || Number(payload.exp) < new Date().getTime()) return { ok: false, error: "Token expired." };
  if (!payload.nonce) return { ok: false, error: "Token nonce missing." };

  return { ok: true, payload: payload };
}

function consumeNonce_(nonce) {
  var key = "ACTION_NONCE_" + String(nonce || "");
  if (!nonce) return { ok: false, error: "Invalid nonce." };
  var props = PropertiesService.getScriptProperties();
  var existing = props.getProperty(key);
  if (existing) return { ok: false, error: "Token already used." };
  props.setProperty(key, String(new Date().getTime()));
  return { ok: true };
}

function renderPendingHelp_() {
  var body = '<p>Legacy bet actions are retired.</p>' +
    '<p>No new records are written to <b>' + htmlEscape_(BET_TRACKING_SHEETS.BET_LOG) + '</b> or <b>' + htmlEscape_(BET_TRACKING_SHEETS.BET_EVENTS) + '</b>.</p>' +
    '<p>Existing tabs are left intact for historical reference when present.</p>';
  return renderHtmlPage_("Pending Actions", body);
}

function renderBetTrackingDisabledPage_() {
  return renderHtmlPage_(
    "Legacy Bet Tracking Disabled",
    "<p>This web app endpoint is intentionally disabled because <code>ENABLE_BET_TRACKING</code> is <b>FALSE</b>.</p>" +
    "<p>Legacy action links are no longer processed by default.</p>"
  );
}

function renderLegacyActionRetiredPage_() {
  return renderHtmlPage_(
    "Legacy Bet Action Retired",
    "<p>This legacy action was retired as part of bet-tracking deprecation.</p>" +
    "<p>If you need temporary legacy behavior, explicitly enable <code>ENABLE_BET_TRACKING</code> and restore archived handlers.</p>"
  );
}

function renderHtmlPage_(title, bodyHtml) {
  var html = '<!doctype html><html><head><meta charset="utf-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1" />' +
    '<style>body{font-family:Arial,sans-serif;padding:18px;max-width:720px;margin:auto;line-height:1.4;}input{padding:8px;width:260px;}button{padding:10px 14px;}code{background:#f2f2f2;padding:2px 6px;border-radius:4px;}</style>' +
    '<title>' + htmlEscape_(title) + '</title></head><body><h2>' + htmlEscape_(title) + '</h2>' + bodyHtml + '</body></html>';
  return HtmlService.createHtmlOutput(html).setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function htmlEscape_(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
