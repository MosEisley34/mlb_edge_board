/* ===================== ACTION ROUTING + OPTIONAL LEGACY BET TRACKING ===================== */

function doGet(e) {
  return handleBetActionGet_(e && e.parameter ? e.parameter : {});
}

function doPost(e) {
  return handleBetActionPost_(e && e.parameter ? e.parameter : {});
}

function handleBetActionGet_(p) {
  var action = String(p.action || "").toLowerCase();
  if (action === "test") return renderActionTestPage_(p);
  if (action === "confirm") return renderConfirmPlacedForm_(p);
  if (action === "mark") return handleMarkResultFromLink_(p);
  if (action === "pending") return renderPendingHelp_();
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
  if (action !== "confirm_submit") return renderHtmlPage_("Error", "<p>Unknown action.</p>");

  var v = verifyActionTokenFromParam_(p.token, "confirm", String(p.bet_id || ""));
  if (!v.ok) return renderHtmlPage_("Token Error", "<p>" + htmlEscape_(v.error) + "</p>");

  var consume = consumeNonce_(v.payload.nonce);
  if (!consume.ok) return renderHtmlPage_("Token Error", "<p>" + htmlEscape_(consume.error) + "</p>");

  try {
    var out = confirmBetPlaced_(String(p.bet_id || ""), p.placed_american_odds, p.units_placed);
    return renderHtmlPage_("Bet marked PLACED", "<p>✅ Bet marked as <b>PLACED</b>.</p>" +
      "<p><b>American odds:</b> " + htmlEscape_(String(out.american)) + "</p>" +
      "<p><b>Decimal odds:</b> " + htmlEscape_(String(out.decimal)) + "</p>" +
      "<p><b>Units:</b> " + htmlEscape_(String(out.units)) + "</p>");
  } catch (err) {
    return renderHtmlPage_("Error", "<p>❌ " + htmlEscape_(String(err)) + "</p>");
  }
}

function createPendingBet_(cfg, args) {
  var sh = SpreadsheetApp.getActive().getSheetByName(BET_TRACKING_SHEETS.BET_LOG);
  if (!sh) return "";

  var betId = Utilities.getUuid();
  var nowLocal = isoLocalWithOffset_(new Date());

  var row = [
    betId,
    nowLocal,
    "PENDING",
    String(args.oddsGameId || ""),
    String(args.mlbGamePk || ""),
    String(args.awayTeam || ""),
    String(args.homeTeam || ""),
    String(args.pickSide || ""),
    String(args.pickTeam || ""),
    "h2h",
    String(args.commenceLocal || ""),
    args.pickPrice,
    args.modelProb,
    args.implied,
    args.noVigImplied,
    args.edge,
    args.confidence,
    args.unitsSuggested,
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    ""
  ];

  sh.appendRow(row);
  appendBetEvent_(betId, "PENDING_CREATED", "", "PENDING", {
    odds_game_id: String(args.oddsGameId || ""),
    mode: String(args.mode || "")
  });
  return betId;
}

function appendBetEvent_(betId, eventName, fromStatus, toStatus, detailObj) {
  var sh = SpreadsheetApp.getActive().getSheetByName(BET_TRACKING_SHEETS.BET_EVENTS);
  if (!sh) return;
  var detail = detailObj ? JSON.stringify(detailObj) : "";
  if (detail.length > 1800) detail = detail.slice(0, 1800) + "…(trimmed)";
  sh.appendRow([isoLocalWithOffset_(new Date()), String(betId || ""), String(eventName || ""), String(fromStatus || ""), String(toStatus || ""), detail]);
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

function findBetRowById_(betId) {
  var sh = SpreadsheetApp.getActive().getSheetByName(BET_TRACKING_SHEETS.BET_LOG);
  if (!sh) return null;
  var v = sh.getDataRange().getValues();
  if (!v || v.length < 2) return null;
  var h = mapToString_(v[0]);
  var iBet = indexOf_(h, "bet_id");
  if (iBet < 0) return null;
  for (var i = 1; i < v.length; i++) {
    if (String(v[i][iBet] || "") === String(betId || "")) return { sh: sh, values: v, header: h, row: i + 1 };
  }
  return null;
}

function confirmBetPlaced_(betId, americanOdds, unitsPlaced) {
  var found = findBetRowById_(betId);
  if (!found) throw new Error("bet_id not found");

  var h = found.header;
  var rowVals = found.values[found.row - 1];

  var iStatus = indexOf_(h, "status");
  var iPlacedAt = indexOf_(h, "placed_at_local");
  var iAm = indexOf_(h, "placed_american_odds");
  var iDec = indexOf_(h, "placed_decimal_odds");
  var iUnits = indexOf_(h, "units_placed");

  var fromStatus = String(rowVals[iStatus] || "");
  if (fromStatus !== "PENDING") throw new Error("Only PENDING bets can be marked PLACED.");

  var am = normalizeAmericanOdds_(americanOdds);
  var dec = americanToDecimal_(am);
  var u = toFloat_(unitsPlaced, NaN);
  if (!isFinite(u) || u <= 0) throw new Error("Units placed must be > 0.");

  found.sh.getRange(found.row, iStatus + 1).setValue("PLACED");
  found.sh.getRange(found.row, iPlacedAt + 1).setValue(isoLocalWithOffset_(new Date()));
  found.sh.getRange(found.row, iAm + 1).setValue(am);
  found.sh.getRange(found.row, iDec + 1).setValue(dec);
  found.sh.getRange(found.row, iUnits + 1).setValue(Math.round(u * 100) / 100);

  appendBetEvent_(betId, "PLACED_CONFIRMED", fromStatus, "PLACED", { american: am, decimal: dec, units: u });
  return { american: am, decimal: dec, units: Math.round(u * 100) / 100 };
}

function markBetResult_(betId, result) {
  var found = findBetRowById_(betId);
  if (!found) throw new Error("bet_id not found");

  var h = found.header;
  var r = found.values[found.row - 1];
  var iStatus = indexOf_(h, "status");
  var iResult = indexOf_(h, "result");
  var iResultAt = indexOf_(h, "result_at_local");
  var iPnL = indexOf_(h, "pnl_units");
  var iUnits = indexOf_(h, "units_placed");
  var iDec = indexOf_(h, "placed_decimal_odds");

  var fromStatus = String(r[iStatus] || "");
  if (fromStatus !== "PLACED") throw new Error("Only PLACED bets can be settled.");

  var rs = String(result || "").toUpperCase();
  if (rs !== "WIN" && rs !== "LOSS" && rs !== "PUSH" && rs !== "VOID") throw new Error("Invalid result.");

  var units = toFloat_(r[iUnits], NaN);
  var dec = toFloat_(r[iDec], NaN);
  if (!isFinite(units) || units <= 0) throw new Error("units_placed missing.");

  var pnl = 0;
  if (rs === "WIN") {
    if (!isFinite(dec) || dec <= 1) throw new Error("placed_decimal_odds missing.");
    pnl = units * (dec - 1);
  } else if (rs === "LOSS") {
    pnl = -units;
  }

  found.sh.getRange(found.row, iStatus + 1).setValue(rs);
  found.sh.getRange(found.row, iResult + 1).setValue(rs);
  found.sh.getRange(found.row, iResultAt + 1).setValue(isoLocalWithOffset_(new Date()));
  found.sh.getRange(found.row, iPnL + 1).setValue(Math.round(pnl * 100) / 100);

  appendBetEvent_(betId, "RESULT_MARKED", fromStatus, rs, { result: rs, pnl_units: pnl });
  return { result: rs, pnl: Math.round(pnl * 100) / 100 };
}

function handleMarkResultFromLink_(p) {
  var betId = String(p.bet_id || "");
  var result = String(p.result || "").toUpperCase();
  var v = verifyActionTokenFromParam_(p.token, "mark", betId);
  if (!v.ok) return renderHtmlPage_("Token Error", "<p>" + htmlEscape_(v.error) + "</p>");
  if (String(v.payload.result || "").toUpperCase() !== result) return renderHtmlPage_("Token Error", "<p>Token result mismatch.</p>");

  var consume = consumeNonce_(v.payload.nonce);
  if (!consume.ok) return renderHtmlPage_("Token Error", "<p>" + htmlEscape_(consume.error) + "</p>");

  try {
    var out = markBetResult_(betId, result);
    return renderHtmlPage_("Bet settled", "<p>✅ Bet marked as <b>" + htmlEscape_(out.result) + "</b>.</p>" +
      "<p><b>PnL units:</b> " + htmlEscape_(String(out.pnl)) + "</p>");
  } catch (err) {
    return renderHtmlPage_("Error", "<p>❌ " + htmlEscape_(String(err)) + "</p>");
  }
}

function renderConfirmPlacedForm_(p) {
  var betId = String(p.bet_id || "");
  var v = verifyActionTokenFromParam_(p.token, "confirm", betId);
  if (!v.ok) return renderHtmlPage_("Token Error", "<p>" + htmlEscape_(v.error) + "</p>");

  var body = '' +
    '<p><b>Bet ID:</b> <code>' + htmlEscape_(betId) + '</code></p>' +
    '<p>Confirm placement by providing actual American odds and units.</p>' +
    '<form method="post">' +
    '<input type="hidden" name="action" value="confirm_submit" />' +
    '<input type="hidden" name="bet_id" value="' + htmlEscape_(betId) + '" />' +
    '<input type="hidden" name="token" value="' + htmlEscape_(String(p.token || "")) + '" />' +
    '<p><label>American odds</label><br/><input name="placed_american_odds" placeholder="-118 or +145" required /></p>' +
    '<p><label>Units placed</label><br/><input name="units_placed" placeholder="0.33" required /></p>' +
    '<p><button type="submit">Confirm Placed</button></p>' +
    '</form>';

  return renderHtmlPage_("Confirm Placed", body);
}

function renderPendingHelp_() {
  var body = '<p>This endpoint supports optional legacy workflow actions.</p>' +
    '<p>If legacy tracking is enabled, open <b>' + htmlEscape_(BET_TRACKING_SHEETS.BET_LOG) + '</b> and filter <code>status = PENDING</code>.</p>' +
    '<p>Use Discord links to confirm placement and mark outcomes.</p>';
  return renderHtmlPage_("Pending Actions", body);
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

function normalizeAmericanOdds_(v) {
  var s = String(v || "").trim();
  if (!s) throw new Error("American odds are required.");
  var n = Number(s);
  if (!isFinite(n) || n === 0 || Math.abs(n) < 100) throw new Error("American odds must be like -110 or +145.");
  return (n > 0) ? Math.round(n) : -Math.round(Math.abs(n));
}

function americanToDecimal_(am) {
  var n = Number(am);
  if (!isFinite(n) || n === 0) throw new Error("Invalid American odds.");
  if (n > 0) return 1 + (n / 100);
  return 1 + (100 / Math.abs(n));
}
