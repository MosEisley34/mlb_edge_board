/* ===================== PROJECTIONS (Razzball HTML scrape) ===================== */

function refreshProjectionsScheduled() {
  var cfg = getConfig_();
  refreshProjectionsIfStale_(cfg, false);
}

function refreshProjectionsIfStale_(cfg, force) {
  var props = PropertiesService.getScriptProperties();
  var lastHit = props.getProperty(PROP.LAST_PROJ_HIT);
  var lastPit = props.getProperty(PROP.LAST_PROJ_PIT);

  var nowMs = new Date().getTime();
  var maxAgeMs = toFloat_(cfg.PROJ_CACHE_HOURS, 12) * 3600 * 1000;

  var hitAge = lastHit ? (nowMs - Date.parse(lastHit)) : 999999999;
  var pitAge = lastPit ? (nowMs - Date.parse(lastPit)) : 999999999;

  if (!force && hitAge <= maxAgeMs && pitAge <= maxAgeMs) {
    log_("INFO", "Projections cache hit (skipping fetch)", { lastHit: lastHit, lastPit: lastPit, cacheHours: toFloat_(cfg.PROJ_CACHE_HOURS, 12) });
    return { hittersUpdated: false, pitchersUpdated: false, lastHit: lastHit, lastPit: lastPit };
  }

  var hitUrl = String(cfg.RAZZ_HIT_URL || "").trim();
  var pitUrl = String(cfg.RAZZ_PIT_URL || "").trim();
  if (!isValidUrl_(hitUrl) || !isValidUrl_(pitUrl)) {
    log_("ERROR", "Razzball URLs invalid/blank. Update SETTINGS: RAZZ_HIT_URL / RAZZ_PIT_URL", { hitUrl: hitUrl, pitUrl: pitUrl });
    return { hittersUpdated: false, pitchersUpdated: false, lastHit: lastHit, lastPit: lastPit };
  }

  var ss = SpreadsheetApp.getActive();
  var shHit = ss.getSheetByName(SH.BATTER_PROJ);
  var shPit = ss.getSheetByName(SH.PITCHER_PROJ);

  var hitHtml = fetchHtml_(hitUrl);
  var hitTable = parseRazzballHtmlTableTo2D_(hitHtml, ["Name", "Team", "OPS"]);
  if (!hitTable || hitTable.length < 2) {
    log_("ERROR", "Failed to parse hitters table from Razzball", { url: hitUrl });
  } else {
    shHit.clearContents();
    shHit.getRange(1, 1, hitTable.length, hitTable[0].length).setValues(hitTable);
    props.setProperty(PROP.LAST_PROJ_HIT, new Date().toISOString());
    log_("INFO", "Razzball hitters projections loaded", { rows: hitTable.length - 1, cols: hitTable[0] });
  }

  var pitHtml = fetchHtml_(pitUrl);
  var pitTable = parseRazzballHtmlTableTo2D_(pitHtml, ["Name", "Team", "SIERA"]);
  if (!pitTable || pitTable.length < 2) {
    log_("ERROR", "Failed to parse pitchers table from Razzball", { url: pitUrl });
  } else {
    shPit.clearContents();
    shPit.getRange(1, 1, pitTable.length, pitTable[0].length).setValues(pitTable);
    props.setProperty(PROP.LAST_PROJ_PIT, new Date().toISOString());
    log_("INFO", "Razzball pitchers projections loaded", { rows: pitTable.length - 1, cols: pitTable[0] });
  }

  return {
    hittersUpdated: true,
    pitchersUpdated: true,
    lastHit: props.getProperty(PROP.LAST_PROJ_HIT),
    lastPit: props.getProperty(PROP.LAST_PROJ_PIT)
  };
}

function fetchHtml_(url) {
  var resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    headers: { "User-Agent": "Mozilla/5.0 (compatible; SheetsBot/1.0)" }
  });
  var code = resp.getResponseCode();
  if (code < 200 || code >= 300) {
    log_("ERROR", "Razzball fetch failed", { http: code, url: url, body: resp.getContentText().slice(0, 300) });
    return "";
  }
  return resp.getContentText();
}

function parseRazzballHtmlTableTo2D_(html, requiredCols) {
  if (!html) return null;
  var tables = extractTables_(html);
  if (!tables || tables.length === 0) return null;

  var best = null, bestScore = -1;
  for (var i = 0; i < tables.length; i++) {
    var parsed = parseHtmlTable_(tables[i]);
    if (!parsed || !parsed.header || parsed.header.length === 0) continue;
    var score = scoreTable_(parsed, requiredCols);
    if (score > bestScore) { bestScore = score; best = parsed; }
  }
  if (!best || bestScore <= 0) return null;

  var out = [best.header];
  for (var r = 0; r < best.rows.length; r++) out.push(best.rows[r]);
  return out;
}

function extractTables_(html) {
  var re = /<table[\s\S]*?<\/table>/gi;
  var out = [];
  var m;
  while ((m = re.exec(html)) !== null) {
    out.push(m[0]);
    if (out.length > 25) break;
  }
  return out;
}

function parseHtmlTable_(tableHtml) {
  var rows = tableHtml.match(/<tr[\s\S]*?<\/tr>/gi) || [];
  if (rows.length === 0) return null;

  var header = null;
  var dataStart = 0;
  for (var i = 0; i < rows.length; i++) {
    if (/<th/i.test(rows[i])) {
      header = extractCells_(rows[i], "th");
      dataStart = i + 1;
      break;
    }
  }
  if (!header || header.length === 0) return null;

  for (var h = 0; h < header.length; h++) header[h] = cleanCellText_(header[h]);

  var data = [];
  for (var r = dataStart; r < rows.length; r++) {
    var cells = extractCells_(rows[r], "td");
    if (!cells || cells.length === 0) continue;
    var cleaned = [];
    for (var c = 0; c < cells.length; c++) cleaned.push(cleanCellText_(cells[c]));
    data.push(cleaned);
  }

  return { header: header, rows: data };
}

function extractCells_(rowHtml, tag) {
  var re = new RegExp("<" + tag + "[^>]*>([\\s\\S]*?)<\\/" + tag + ">", "gi");
  var out = [];
  var m;
  while ((m = re.exec(rowHtml)) !== null) out.push(m[1]);
  return out;
}

function cleanCellText_(htmlFrag) { return decodeHtmlEntities_(stripHtml_(String(htmlFrag || ""))).replace(/\s+/g, " ").trim(); }

function stripHtml_(s) {
  return String(s || "")
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function decodeHtmlEntities_(s) {
  return String(s || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .trim();
}

function scoreTable_(parsed, requiredCols) {
  var header = parsed.header || [];
  var rowCount = parsed.rows ? parsed.rows.length : 0;

  var matched = 0;
  for (var i = 0; i < requiredCols.length; i++) if (headerHasCol_(header, requiredCols[i])) matched++;
  if (matched < Math.min(2, requiredCols.length)) return 0;
  return matched * 100000 + rowCount;
}

function headerHasCol_(header, colName) {
  var target = String(colName || "").toLowerCase();
  for (var i = 0; i < header.length; i++) if (String(header[i] || "").toLowerCase() === target) return true;
  if (target === "siera") {
    for (var j = 0; j < header.length; j++) {
      var h = String(header[j] || "").toLowerCase();
      if (h.indexOf("siera") >= 0) return true;
    }
  }
  return false;
}

function isValidUrl_(u) {
  if (!u) return false;
  var s = String(u);
  if (s.indexOf("http://") !== 0 && s.indexOf("https://") !== 0) return false;
  if (s.indexOf("example.com") !== -1) return false;
  return true;
}

/* ===================== MODEL + EDGE_BOARD ===================== */

// NOTE: This function is intentionally kept in its current form for compatibility.
function refreshModelAndEdge_(cfg, mlbRes) {
  // Function body retained in dedicated file to keep behavior stable.
  return refreshModelAndEdge_core_(cfg, mlbRes);
}
