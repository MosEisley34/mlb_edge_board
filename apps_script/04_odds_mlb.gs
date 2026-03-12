/* ===================== ODDS (TheOddsAPI v4) ===================== */

function refreshOdds_(cfg) {
  var apiKey = cfg.ODDS_API_KEY || PropertiesService.getScriptProperties().getProperty(PROP.ODDS_API_KEY);
  if (!apiKey) {
    log_("ERROR", "Missing ODDS_API_KEY", {});
    return { sportKeyUsed: chooseSportKey_(cfg), games: 0, updatedAt: isoLocalWithOffset_(new Date()) };
  }

  var lookaheadH = toFloat_(cfg.ODDS_LOOKAHEAD_HOURS, 36);
  var fromIso = isoUtcNoMs_(new Date());
  var toIso = isoUtcNoMs_(new Date(new Date().getTime() + lookaheadH * 3600 * 1000));

  var sportKeyUsed = chooseSportKey_(cfg);
  var data = fetchOddsData_(apiKey, cfg, sportKeyUsed, fromIso, toIso);

  if (cfg.ODDS_FALLBACK_ON_EMPTY === true && data.length === 0 && sportKeyUsed === cfg.ODDS_SPORT_KEY_PRESEASON) {
    log_("INFO", "Odds preseason empty; trying fallback sport key", { from: sportKeyUsed, to: cfg.ODDS_SPORT_KEY_REGULAR });
    var data2 = fetchOddsData_(apiKey, cfg, cfg.ODDS_SPORT_KEY_REGULAR, fromIso, toIso);
    if (data2.length > 0) { data = data2; sportKeyUsed = cfg.ODDS_SPORT_KEY_REGULAR; }
  }

  var ss = SpreadsheetApp.getActive();
  var shOdds = ss.getSheetByName(SH.ODDS_RAW);
  var shOddsHistory = ss.getSheetByName(SH.ODDS_HISTORY) || getOrCreateSheet_(ss, SH.ODDS_HISTORY);
  ensureOddsHistoryHeader_(shOddsHistory);
  var nowLocal = isoLocalWithOffset_(new Date());

  var rows = [];
  var historyRows = [];
  for (var i = 0; i < data.length; i++) {
    var g = data[i];
    var best = pickBestH2H_(g.bookmakers || [], g.away_team, g.home_team);

    var gameId = String(g.id || "");
    var commenceUtc = String(g.commence_time || "");
    var awayTeam = String(g.away_team || "");
    var homeTeam = String(g.home_team || "");

    rows.push([
      gameId,
      commenceUtc,
      awayTeam,
      homeTeam,
      best.awayDecimal,
      best.homeDecimal,
      best.awayImplied,
      best.homeImplied,
      best.bestBookAway,
      best.bestBookHome,
      nowLocal,
      sportKeyUsed
    ]);

    historyRows.push([
      nowLocal,
      gameId,
      commenceUtc,
      awayTeam,
      homeTeam,
      best.awayDecimal,
      best.homeDecimal,
      best.awayImplied,
      best.homeImplied,
      best.bestBookAway,
      best.bestBookHome,
      sportKeyUsed
    ]);
  }

  replaceSheetBody_(shOdds, rows);
  if (historyRows.length > 0) {
    shOddsHistory.getRange(shOddsHistory.getLastRow() + 1, 1, historyRows.length, historyRows[0].length).setValues(historyRows);
  }
  log_("INFO", "refreshOdds completed", { sportKeyUsed: sportKeyUsed, games: rows.length, historyRowsAppended: historyRows.length, windowHours: lookaheadH, from: fromIso, to: toIso });
  return { sportKeyUsed: sportKeyUsed, games: rows.length, updatedAt: nowLocal };
}

function fetchOddsData_(apiKey, cfg, sportKey, fromIso, toIso) {
  log_("INFO", "Odds sport key selected", {
    sportKeyUsed: sportKey,
    regions: cfg.ODDS_REGIONS,
    markets: cfg.ODDS_MARKETS,
    oddsFormat: cfg.ODDS_FORMAT,
    dateFormat: cfg.ODDS_DATE_FORMAT,
    strategy: "best",
    refBookKey: (cfg.ODDS_REF_BOOK || ""),
    commenceTimeFrom: fromIso,
    commenceTimeTo: toIso
  });

  var url =
    "https://api.the-odds-api.com/v4/sports/" + encodeURIComponent(sportKey) + "/odds/" +
    "?apiKey=" + encodeURIComponent(apiKey) +
    "&regions=" + encodeURIComponent(cfg.ODDS_REGIONS) +
    "&markets=" + encodeURIComponent(cfg.ODDS_MARKETS) +
    "&oddsFormat=" + encodeURIComponent(cfg.ODDS_FORMAT) +
    "&dateFormat=" + encodeURIComponent(cfg.ODDS_DATE_FORMAT) +
    "&commenceTimeFrom=" + encodeURIComponent(fromIso) +
    "&commenceTimeTo=" + encodeURIComponent(toIso);

  var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  var http = resp.getResponseCode();
  var headers = resp.getAllHeaders() || {};
  var used = parseOddsApiHeaderInt_(headers, "x-requests-used");
  var remaining = parseOddsApiHeaderInt_(headers, "x-requests-remaining");
  log_("INFO", "Odds API credits", {
    used: used,
    remaining: remaining,
    http: http
  });
  maybeSendOddsCreditsLowAlert_(cfg, remaining, used, http, sportKey);

  if (http !== 200) {
    log_("ERROR", "Odds API fetch failed", { http: http, body: resp.getContentText().slice(0, 500) });
    return [];
  }

  try { return JSON.parse(resp.getContentText()) || []; }
  catch (e) { log_("ERROR", "Odds API JSON parse failed", { message: String(e) }); return []; }
}


function parseOddsApiHeaderInt_(headers, keyLower) {
  var normalizedKey = String(keyLower || "").toLowerCase();
  var raw = "";
  var keys = Object.keys(headers || {});
  for (var i = 0; i < keys.length; i++) {
    var k = String(keys[i] || "");
    if (k.toLowerCase() === normalizedKey) { raw = headers[k]; break; }
  }
  var n = Number(raw);
  return isFinite(n) ? n : "";
}

function maybeSendOddsCreditsLowAlert_(cfg, remaining, used, http, sportKey) {
  if (!isFinite(Number(remaining))) {
    log_("INFO", "Odds credits alert suppressed", { reasonCode: "remaining_missing_or_non_numeric", remaining: remaining, used: used, http: http, sportKeyUsed: sportKey });
    return;
  }

  var threshold = Math.max(0, toInt_(cfg.ODDS_ALERT_REMAINING_THRESHOLD, 75));
  if (Number(remaining) > threshold) {
    log_("INFO", "Odds credits alert suppressed", { reasonCode: "remaining_above_threshold", remaining: remaining, threshold: threshold, used: used, http: http, sportKeyUsed: sportKey });
    return;
  }

  var forceEveryCall = cfg.ODDS_ALERT_ON_EVERY_CALL_UNDER_THRESHOLD === true;
  var cooldownMin = Math.max(0, toInt_(cfg.ODDS_ALERT_COOLDOWN_MIN, 180));
  var nowMs = Date.now();
  var props = PropertiesService.getScriptProperties();
  var lastSentAtMs = Math.max(0, toInt_(props.getProperty(PROP.ODDS_ALERT_LAST_SENT_AT_MS), 0));
  var elapsedMin = lastSentAtMs > 0 ? ((nowMs - lastSentAtMs) / 60000) : null;

  if (!forceEveryCall && lastSentAtMs > 0 && elapsedMin < cooldownMin) {
    log_("INFO", "Odds credits alert suppressed", {
      reasonCode: "cooldown_active",
      remaining: remaining,
      used: used,
      threshold: threshold,
      cooldownMin: cooldownMin,
      elapsedMin: round_(elapsedMin, 2),
      http: http,
      sportKeyUsed: sportKey
    });
    return;
  }

  var cfgLive = getConfig_();
  var deliveryMode = discordDeliveryMode_(cfgLive, { allowWebhook: true });
  if (deliveryMode.mode === "missing") {
    log_("WARN", "Odds credits alert suppressed", {
      reasonCode: "discord_delivery_missing",
      remaining: remaining,
      used: used,
      threshold: threshold,
      cooldownMin: cooldownMin,
      forceEveryCall: forceEveryCall,
      http: http,
      sportKeyUsed: sportKey
    });
    return;
  }

  var tsLocal = isoLocalWithOffset_(new Date());
  var payload = {
    content:
      "⚠️ **Odds API credits running low**\n" +
      "Remaining: **" + String(remaining) + "** (threshold: " + String(threshold) + ")\n" +
      "Used: **" + String(used) + "**\n" +
      "Timestamp: " + tsLocal + "\n" +
      "Sport key: `" + String(sportKey || "") + "`\n" +
      "Action required: rotate the Odds API key manually before credits are exhausted."
  };


  var res = sendDiscordByMode_(deliveryMode, payload);
  var ok = (res.http >= 200 && res.http < 300);
  var detail = {
    remaining: remaining,
    used: used,
    threshold: threshold,
    cooldownMin: cooldownMin,
    forceEveryCall: forceEveryCall,
    http: http,
    sportKeyUsed: sportKey,
    discordHttp: res.http,
    discordDeliveryMode: res.deliveryMode,
    discordBody: String(res.body || "").slice(0, 300)
  };
  if (ok) {
    props.setProperty(PROP.ODDS_ALERT_LAST_SENT_AT_MS, String(nowMs));
    detail.alertTimestampLocal = tsLocal;
    detail.lastSentAtMs = nowMs;
    log_("WARN", "Odds credits alert emitted", detail);
    return;
  }

  detail.reasonCode = "discord_send_failed";
  log_("WARN", "Odds credits alert failed", detail);
}

function pickBestH2H_(bookmakers, awayTeam, homeTeam) {
  var bestAway = 0, bestHome = 0;
  var bestBookAway = "", bestBookHome = "";

  for (var i = 0; i < bookmakers.length; i++) {
    var bm = bookmakers[i];
    var bk = bm && bm.key ? String(bm.key) : "";
    var markets = bm && bm.markets ? bm.markets : [];

    for (var j = 0; j < markets.length; j++) {
      var m = markets[j];
      if (!m || m.key !== "h2h") continue;

      var outs = m.outcomes || [];
      for (var k = 0; k < outs.length; k++) {
        var o = outs[k];
        if (!o) continue;

        var nm = String(o.name || "");
        var price = Number(o.price);
        if (!isFinite(price) || price <= 1) continue;

        var outNorm = normalizeTeam_(nm);
        var awayNorm = normalizeTeam_(awayTeam);
        var homeNorm = normalizeTeam_(homeTeam);
        if (outNorm === awayNorm) {
          if (price > bestAway) { bestAway = price; bestBookAway = bk; }
        } else if (outNorm === homeNorm) {
          if (price > bestHome) { bestHome = price; bestBookHome = bk; }
        }
      }
    }
  }

  return {
    awayDecimal: bestAway ? bestAway : "",
    homeDecimal: bestHome ? bestHome : "",
    awayImplied: bestAway ? (1 / bestAway) : "",
    homeImplied: bestHome ? (1 / bestHome) : "",
    bestBookAway: bestBookAway,
    bestBookHome: bestBookHome
  };
}


function getMLBOddsRefreshWindow_(cfg) {
  var now = new Date();
  var todayLocal = Utilities.formatDate(now, TZ, "yyyy-MM-dd");
  var preMin = Math.max(0, toInt_(cfg.ODDS_WINDOW_PRE_FIRST_MIN, 60));
  var postMin = Math.max(0, toInt_(cfg.ODDS_WINDOW_POST_LAST_MIN, 0));

  var schedUrl =
    "https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=" + encodeURIComponent(todayLocal) +
    "&endDate=" + encodeURIComponent(todayLocal);

  var resp = UrlFetchApp.fetch(schedUrl, { muteHttpExceptions: true });
  var http = resp.getResponseCode();
  if (http !== 200) throw new Error("MLB schedule fetch failed: http=" + http);

  var payload;
  try {
    payload = JSON.parse(resp.getContentText());
  } catch (e) {
    throw new Error("MLB schedule JSON parse failed: " + String(e));
  }

  var games = [];
  var dates = (payload && payload.dates) ? payload.dates : [];
  for (var i = 0; i < dates.length; i++) {
    var dg = dates[i] && dates[i].games ? dates[i].games : [];
    for (var j = 0; j < dg.length; j++) games.push(dg[j]);
  }

  if (games.length === 0) {
    return {
      hasGames: false,
      gameCount: 0,
      firstGameLocal: null,
      lastGameLocal: null,
      windowStart: null,
      windowEnd: null,
      preFirstMin: preMin,
      postLastMin: postMin,
      scheduleDateLocal: todayLocal
    };
  }

  var minStart = Number.POSITIVE_INFINITY;
  var maxStart = Number.NEGATIVE_INFINITY;
  for (var g = 0; g < games.length; g++) {
    var t = Date.parse(String(games[g] && games[g].gameDate ? games[g].gameDate : ""));
    if (!isFinite(t)) continue;
    if (t < minStart) minStart = t;
    if (t > maxStart) maxStart = t;
  }

  if (!isFinite(minStart) || !isFinite(maxStart)) {
    return {
      hasGames: false,
      gameCount: games.length,
      firstGameLocal: null,
      lastGameLocal: null,
      windowStart: null,
      windowEnd: null,
      preFirstMin: preMin,
      postLastMin: postMin,
      scheduleDateLocal: todayLocal
    };
  }

  var firstGameLocal = new Date(minStart);
  var lastGameLocal = new Date(maxStart);
  var windowStart = new Date(minStart - preMin * 60 * 1000);
  var windowEnd = new Date(maxStart + postMin * 60 * 1000);

  return {
    hasGames: true,
    gameCount: games.length,
    firstGameLocal: firstGameLocal,
    lastGameLocal: lastGameLocal,
    windowStart: windowStart,
    windowEnd: windowEnd,
    preFirstMin: preMin,
    postLastMin: postMin,
    scheduleDateLocal: todayLocal,
    firstGameLocalIso: isoLocalWithOffset_(firstGameLocal),
    lastGameLocalIso: isoLocalWithOffset_(lastGameLocal),
    windowStartIso: isoLocalWithOffset_(windowStart),
    windowEndIso: isoLocalWithOffset_(windowEnd)
  };
}

/* ===================== MLB SCHEDULE + LINEUPS ===================== */

function refreshMLBScheduleAndLineups_(cfg, opts) {
  opts = opts || {};
  var ss = SpreadsheetApp.getActive();
  var shOdds = ss.getSheetByName(SH.ODDS_RAW);
  var shSchedule = ss.getSheetByName(SH.MLB_SCHEDULE);
  var shLineups = ss.getSheetByName(SH.MLB_LINEUPS);

  var oddsRows = readSheetAsObjects_(shOdds);
  var commenceTimes = [];
  for (var i = 0; i < oddsRows.length; i++) {
    var t = Date.parse(String(oddsRows[i].commence_time_utc || ""));
    if (isFinite(t) && t > 0) commenceTimes.push(t);
  }

  var queryWindow = deriveScheduleQueryWindowFromOdds_(commenceTimes, cfg);
  var startDate = queryWindow.startDate;
  var endDate = queryWindow.endDate;

  log_("INFO", "MLB schedule query window", {
    startDate: startDate,
    endDate: endDate,
    startDateSource: queryWindow.source,
    timezone: TZ,
    minOddsCommenceUtc: queryWindow.minOddsCommenceUtc,
    maxOddsCommenceUtc: queryWindow.maxOddsCommenceUtc,
    minOddsCommenceLocal: queryWindow.minOddsCommenceLocal,
    maxOddsCommenceLocal: queryWindow.maxOddsCommenceLocal,
    bufferBeforeHours: queryWindow.bufferBeforeHours,
    bufferAfterHours: queryWindow.bufferAfterHours,
    bufferedStartLocal: queryWindow.bufferedStartLocal,
    bufferedEndLocal: queryWindow.bufferedEndLocal,
    oddsUpcoming: oddsRows.length,
    matchTolMin: cfg.MATCH_TOL_MIN,
    lineupMin: cfg.LINEUP_MIN
  });

  var shouldUseExpandedFallback = oddsRows.length <= 3;
  var expandedFallbackUsed = false;
  var expandedWindowFallbackSkipReasonCode = "";

  var oddsSportKey = resolveOddsSportKey_(oddsRows, opts.sportKeyUsed || "");
  var schedMain = fetchScheduleGames_(startDate, endDate, "");
  if (!schedMain.ok) {
    log_("ERROR", "MLB schedule fetch failed", { http: schedMain.http, body: schedMain.body });
    replaceSheetBody_(shSchedule, []);
    replaceSheetBody_(shLineups, []);
    return { scheduleGames: 0, matchedCount: 0, lineupRows: 0, matched: [], rejectionSummary: {} };
  }

  var games = schedMain.games;
  var usedGameType = "default";
  if (oddsSportKey === cfg.ODDS_SPORT_KEY_PRESEASON && oddsRows.length > 0) {
    var minNeeded = Math.max(1, Math.floor(oddsRows.length * 0.5));
    if (games.length < minNeeded) {
      var schedPreseason = fetchScheduleGames_(startDate, endDate, "S");
      if (schedPreseason.ok && schedPreseason.games.length >= games.length) {
        games = schedPreseason.games;
        usedGameType = "S";
      }
      log_("INFO", "Preseason schedule check", {
        oddsSportKeyUsed: oddsSportKey,
        oddsEvents: oddsRows.length,
        defaultScheduleGames: schedMain.games.length,
        preseasonScheduleGames: schedPreseason.ok ? schedPreseason.games.length : 0,
        selectedGameType: usedGameType,
        preseasonHttp: schedPreseason.http
      });
    }
  }

  var schedRows = [];
  var nowLocal = isoLocalWithOffset_(new Date());
  for (var x = 0; x < games.length; x++) {
    var gg = games[x];
    schedRows.push([
      String(gg.gamePk || ""),
      String(gg.gameGuid || ""),
      String(gg.gameDate || ""),
      getTeamNameSafe_(gg, "away"),
      getTeamNameSafe_(gg, "home"),
      getTeamIdSafe_(gg, "away"),
      getTeamIdSafe_(gg, "home"),
      getProbablePitcherNameSafe_(gg, "away"),
      getProbablePitcherNameSafe_(gg, "home"),
      String(gg.status && gg.status.detailedState ? gg.status.detailedState : ""),
      String(gg.venue && gg.venue.name ? gg.venue.name : ""),
      nowLocal
    ]);
  }
  replaceSheetBody_(shSchedule, schedRows);

  var matchTolMin = toInt_(cfg.MATCH_TOL_MIN, 360);
  var matchRes = matchOddsToSchedule_(shOdds, shSchedule, matchTolMin, { enableTeamFallback: cfg.ODDS_TEAM_MATCH_FALLBACK_ENABLE });
  var matched = matchRes.matched;
  var firstPassAllEventsNoTeamTokenMatch = isAllEventsNoTeamTokenMatch_(matched.length, matchRes.rejectionSummary, oddsRows.length);
  var scheduleWindowMatchPath = matched.length > 0 ? "primary-window matched" : "fallback required";
  var firstPassFullMatchNoHardRejections = isPrimaryWindowFullMatchWithoutHardRejections_(matched.length, matchRes.rejectionSummary, oddsRows.length);

  if (firstPassFullMatchNoHardRejections) {
    shouldUseExpandedFallback = false;
    expandedWindowFallbackSkipReasonCode = "PRIMARY_PASS_MATCH_SHORT_CIRCUIT";
    log_("INFO", "MLB schedule expanded-window fallback skipped", {
      reasonCode: expandedWindowFallbackSkipReasonCode,
      oddsUpcoming: oddsRows.length,
      firstPassMatched: matched.length,
      scheduleWindowMatchPath: scheduleWindowMatchPath,
      firstPassRejectionSummary: matchRes.rejectionSummary
    });
  }

  if (!shouldUseExpandedFallback && matched.length === 0 && (matchRes.rejectionSummary.no_team_token_match || 0) > 0) {
    shouldUseExpandedFallback = true;
  }

  if (shouldUseExpandedFallback) {
    expandedFallbackUsed = true;
    var expandedStartDate = shiftDateYmd_(startDate, -1);
    var expandedEndDate = shiftDateYmd_(endDate, 1);
    log_("INFO", "MLB schedule expanded-window fallback check", {
      originalStartDate: startDate,
      originalEndDate: endDate,
      expandedStartDate: expandedStartDate,
      expandedEndDate: expandedEndDate,
      oddsUpcoming: oddsRows.length,
      firstPassMatched: matched.length,
      firstPassAllEventsNoTeamTokenMatch: firstPassAllEventsNoTeamTokenMatch,
      scheduleWindowMatchPath: scheduleWindowMatchPath,
      firstPassRejectionSummary: matchRes.rejectionSummary
    });
    var schedExpanded = fetchScheduleGames_(expandedStartDate, expandedEndDate, "");
    if (schedExpanded.ok) {
      var expandedRows = [];
      for (var ex = 0; ex < schedExpanded.games.length; ex++) {
        var eg = schedExpanded.games[ex];
        expandedRows.push([
          String(eg.gamePk || ""),
          String(eg.gameGuid || ""),
          String(eg.gameDate || ""),
          getTeamNameSafe_(eg, "away"),
          getTeamNameSafe_(eg, "home"),
          getTeamIdSafe_(eg, "away"),
          getTeamIdSafe_(eg, "home"),
          getProbablePitcherNameSafe_(eg, "away"),
          getProbablePitcherNameSafe_(eg, "home"),
          String(eg.status && eg.status.detailedState ? eg.status.detailedState : ""),
          String(eg.venue && eg.venue.name ? eg.venue.name : ""),
          nowLocal
        ]);
      }
      var shSchedExpanded = ss.insertSheet("__mlb_sched_expanded_tmp__" + String(new Date().getTime()));
      try {
        ensureScheduleHeader_(shSchedExpanded);
        replaceSheetBody_(shSchedExpanded, expandedRows);
        var expandedMatchRes = matchOddsToSchedule_(shOdds, shSchedExpanded, matchTolMin, { enableTeamFallback: cfg.ODDS_TEAM_MATCH_FALLBACK_ENABLE });
        if (expandedMatchRes.matched.length > 0 || matched.length === 0) {
          matchRes = expandedMatchRes;
          matched = expandedMatchRes.matched;
          games = schedExpanded.games;
          schedRows = expandedRows;
          replaceSheetBody_(shSchedule, schedRows);
        }
      } finally {
        ss.deleteSheet(shSchedExpanded);
      }
    }
  }

  var lineupRows = [];
  for (var m = 0; m < matched.length; m++) {
    var mm = matched[m];
    var gamePk = mm.mlb_gamePk;

    var boxUrl = "https://statsapi.mlb.com/api/v1/game/" + encodeURIComponent(gamePk) + "/boxscore";
    var boxResp = UrlFetchApp.fetch(boxUrl, { muteHttpExceptions: true });
    if (boxResp.getResponseCode() !== 200) continue;

    var box = JSON.parse(boxResp.getContentText());
    var awayRows = extractLineupRows_(box, gamePk, mm.odds_game_id, "away");
    var homeRows = extractLineupRows_(box, gamePk, mm.odds_game_id, "home");

    for (var a = 0; a < awayRows.length; a++) lineupRows.push(awayRows[a]);
    for (var h = 0; h < homeRows.length; h++) lineupRows.push(homeRows[h]);
  }

  replaceSheetBody_(shLineups, lineupRows);

  log_("INFO", "refreshMLBScheduleAndLineups completed", {
    scheduleGames: games.length,
    matchedCount: matched.length,
    lineupRows: lineupRows.length,
    scheduleGameType: usedGameType,
    scheduleWindowMatchPath: scheduleWindowMatchPath,
    firstPassAllEventsNoTeamTokenMatch: firstPassAllEventsNoTeamTokenMatch,
    expandedWindowFallbackUsed: expandedFallbackUsed,
    expandedWindowFallbackSkipReasonCode: expandedWindowFallbackSkipReasonCode,
    rejectionSummary: matchRes.rejectionSummary
  });
  return {
    scheduleGames: games.length,
    matchedCount: matched.length,
    lineupRows: lineupRows.length,
    matched: matched,
    rejectionSummary: matchRes.rejectionSummary,
    expandedWindowFallbackUsed: expandedFallbackUsed,
    expandedWindowFallbackSkipReasonCode: expandedWindowFallbackSkipReasonCode,
    scheduleWindowMatchPath: scheduleWindowMatchPath,
    firstPassAllEventsNoTeamTokenMatch: firstPassAllEventsNoTeamTokenMatch,
    queryWindow: queryWindow
  };
}

function isPrimaryWindowFullMatchWithoutHardRejections_(matchedCount, rejectionSummary, oddsCount) {
  var totalOdds = Math.max(0, Number(oddsCount || 0));
  var matched = Math.max(0, Number(matchedCount || 0));
  if (totalOdds === 0) return false;
  if (matched < totalOdds) return false;

  var summary = rejectionSummary || {};
  return Number(summary.invalid_odds_time || 0) === 0 &&
    Number(summary.outside_time_tolerance || 0) === 0 &&
    Number(summary.no_team_token_match || 0) === 0;
}

function deriveScheduleQueryWindowFromOdds_(commenceTimes, cfg, nowOverride) {
  var now = nowOverride instanceof Date ? nowOverride : new Date();
  var bufferBeforeHours = Math.max(0, toFloat_(cfg.ODDS_SCHEDULE_QUERY_BUFFER_BEFORE_H, 24));
  var bufferAfterHours = Math.max(0, toFloat_(cfg.ODDS_SCHEDULE_QUERY_BUFFER_AFTER_H, 24));

  if (!commenceTimes || commenceTimes.length === 0) {
    var fallbackStart = new Date(now.getTime() - bufferBeforeHours * 3600 * 1000);
    var fallbackEnd = new Date(now.getTime() + (24 + bufferAfterHours) * 3600 * 1000);
    return {
      source: "fallback_now",
      startDate: Utilities.formatDate(fallbackStart, TZ, "yyyy-MM-dd"),
      endDate: Utilities.formatDate(fallbackEnd, TZ, "yyyy-MM-dd"),
      minOddsCommenceUtc: null,
      maxOddsCommenceUtc: null,
      minOddsCommenceLocal: null,
      maxOddsCommenceLocal: null,
      bufferBeforeHours: bufferBeforeHours,
      bufferAfterHours: bufferAfterHours,
      bufferedStartLocal: isoLocalWithOffset_(fallbackStart),
      bufferedEndLocal: isoLocalWithOffset_(fallbackEnd)
    };
  }

  var minT = Math.min.apply(null, commenceTimes);
  var maxT = Math.max.apply(null, commenceTimes);
  var bufferedStart = new Date(minT - bufferBeforeHours * 3600 * 1000);
  var bufferedEnd = new Date(maxT + bufferAfterHours * 3600 * 1000);

  return {
    source: "odds_commence_bounds",
    startDate: Utilities.formatDate(bufferedStart, TZ, "yyyy-MM-dd"),
    endDate: Utilities.formatDate(bufferedEnd, TZ, "yyyy-MM-dd"),
    minOddsCommenceUtc: new Date(minT).toISOString(),
    maxOddsCommenceUtc: new Date(maxT).toISOString(),
    minOddsCommenceLocal: isoLocalWithOffset_(new Date(minT)),
    maxOddsCommenceLocal: isoLocalWithOffset_(new Date(maxT)),
    bufferBeforeHours: bufferBeforeHours,
    bufferAfterHours: bufferAfterHours,
    bufferedStartLocal: isoLocalWithOffset_(bufferedStart),
    bufferedEndLocal: isoLocalWithOffset_(bufferedEnd)
  };
}

function shiftDateYmd_(dateYmd, deltaDays) {
  var t = Date.parse(String(dateYmd || "") + "T00:00:00Z");
  if (!isFinite(t)) return String(dateYmd || "");
  return Utilities.formatDate(new Date(t + (toInt_(deltaDays, 0) * 24 * 3600 * 1000)), "UTC", "yyyy-MM-dd");
}

function fetchScheduleGames_(startDate, endDate, gameType) {
  var url =
    "https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=" + encodeURIComponent(startDate) +
    "&endDate=" + encodeURIComponent(endDate) + "&hydrate=team,probablePitcher,venue";
  if (gameType) url += "&gameType=" + encodeURIComponent(gameType);

  var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  var http = resp.getResponseCode();
  if (http !== 200) {
    return { ok: false, http: http, games: [], body: resp.getContentText().slice(0, 500) };
  }

  var payload = JSON.parse(resp.getContentText());
  var games = [];
  var dates = payload.dates || [];
  for (var d = 0; d < dates.length; d++) {
    var dg = dates[d].games || [];
    for (var g = 0; g < dg.length; g++) games.push(dg[g]);
  }
  return { ok: true, http: http, games: games, body: "" };
}

function resolveOddsSportKey_(oddsRows, fallbackSportKey) {
  fallbackSportKey = String(fallbackSportKey || "");
  if (!oddsRows || oddsRows.length === 0) return fallbackSportKey;

  var counts = {};
  for (var i = 0; i < oddsRows.length; i++) {
    var k = String(oddsRows[i].sport_key_used || "").trim();
    if (!k) continue;
    counts[k] = (counts[k] || 0) + 1;
  }

  var bestKey = fallbackSportKey;
  var bestN = 0;
  for (var key in counts) {
    if ((counts[key] || 0) > bestN) {
      bestN = counts[key];
      bestKey = key;
    }
  }
  return bestKey;
}

function extractLineupRows_(box, gamePk, oddsGameId, side) {
  var team = box && box.teams && box.teams[side] ? box.teams[side] : null;
  if (!team) return [];

  var players = team.players || {};
  var orderList = (team.battingOrder && team.battingOrder.length) ? team.battingOrder : (team.batters || []);
  var posted = (orderList && orderList.length >= 9);

  var rows = [];
  var used = {};
  var order = 1;

  for (var i = 0; i < orderList.length; i++) {
    var pid = String(orderList[i] || "");
    if (!pid || used[pid]) continue;
    used[pid] = true;

    var p = players["ID" + pid];
    if (!p) continue;

    rows.push([
      String(gamePk), String(oddsGameId || ""), side, order, pid,
      String(p.person && p.person.fullName ? p.person.fullName : ""),
      String(p.position && p.position.abbreviation ? p.position.abbreviation : ""),
      String(p.batSide && p.batSide.code ? p.batSide.code : ""),
      posted ? "Y" : "N",
      isoLocalWithOffset_(new Date())
    ]);

    order++;
    if (order > 9) break;
  }

  return rows;
}

function getTeamNameSafe_(g, side) {
  try {
    var t = g.teams && g.teams[side] && g.teams[side].team ? g.teams[side].team : null;
    return t && t.name ? String(t.name) : "";
  } catch (e) { return ""; }
}

function getTeamIdSafe_(g, side) {
  try {
    var t = g.teams && g.teams[side] && g.teams[side].team ? g.teams[side].team : null;
    return t && t.id ? String(t.id) : "";
  } catch (e) { return ""; }
}

function getProbablePitcherNameSafe_(g, side) {
  try {
    var p = g.teams && g.teams[side] && g.teams[side].probablePitcher ? g.teams[side].probablePitcher : null;
    return p && (p.fullName || p.name) ? String(p.fullName || p.name) : "";
  } catch (e) { return ""; }
}

function matchOddsToSchedule_(shOdds, shSchedule, matchTolMin, opts) {
  opts = opts || {};
  var enableTeamFallback = (opts.enableTeamFallback !== false);

  var odds = shOdds.getDataRange().getValues();
  var sched = shSchedule.getDataRange().getValues();
  if (odds.length < 2 || sched.length < 2) return { matched: [], rejectionSummary: { insufficient_input_rows: 0 } };

  var oh = mapToString_(odds[0]);
  var sh = mapToString_(sched[0]);

  var oIdxId = indexOf_(oh, "odds_game_id");
  var oIdxComm = indexOf_(oh, "commence_time_utc");
  var oIdxAway = indexOf_(oh, "away_team");
  var oIdxHome = indexOf_(oh, "home_team");

  var sIdxPk = indexOf_(sh, "mlb_gamePk");
  var sIdxDate = indexOf_(sh, "gameDate_utc");
  var sIdxAway = indexOf_(sh, "away_team");
  var sIdxHome = indexOf_(sh, "home_team");
  var sIdxAwayId = indexOf_(sh, "away_team_id");
  var sIdxHomeId = indexOf_(sh, "home_team_id");

  var schedRows = [];
  for (var i = 1; i < sched.length; i++) {
    var r = sched[i];
    var gamePk = String(r[sIdxPk] || "");
    if (!gamePk) continue;
    schedRows.push({
      mlb_gamePk: gamePk,
      gameDate: String(r[sIdxDate] || ""),
      away_team: String(r[sIdxAway] || ""),
      home_team: String(r[sIdxHome] || ""),
      away_team_id: String(r[sIdxAwayId] || ""),
      home_team_id: String(r[sIdxHomeId] || "")
    });
  }

  log_("INFO", "matchOddsToSchedule schedule inventory", buildScheduleInventory_(schedRows));

  var out = [];
  var rejectionSummary = {
    invalid_odds_time: 0,
    no_team_token_match: 0,
    outside_time_tolerance: 0
  };
  var unmatchedNoTeamEvents = [];

  for (var j = 1; j < odds.length; j++) {
    var or = odds[j];
    var oddsId = String(or[oIdxId] || "");
    if (!oddsId) continue;

    var oAway = String(or[oIdxAway] || "");
    var oHome = String(or[oIdxHome] || "");
    var oTimeRaw = String(or[oIdxComm] || "");
    var oTime = Date.parse(oTimeRaw);

    var oAwayNorm = normalizeTeam_(oAway);
    var oHomeNorm = normalizeTeam_(oHome);

    if (!isFinite(oTime) || oTime <= 0) {
      rejectionSummary.invalid_odds_time++;
      log_("DEBUG", "matchOddsToSchedule unmatched", {
        reason: "invalid_odds_time",
        odds_event_id: oddsId,
        away_team: oAway,
        home_team: oHome,
        commence_time_utc: oTimeRaw,
        team_tokens: {
          raw: { away: oAway, home: oHome },
          canonical: { away: oAwayNorm, home: oHomeNorm }
        },
        candidate_schedule_rows: []
      });
      continue;
    }

    var firstPass = findBestScheduleMatch_(schedRows, {
      oddsAwayRaw: oAway,
      oddsHomeRaw: oHome,
      oddsAwayNorm: oAwayNorm,
      oddsHomeNorm: oHomeNorm,
      oddsTime: oTime,
      matchTolMin: matchTolMin,
      mode: "canonical_exact"
    });

    if (firstPass.found) {
      out.push({
        odds_game_id: oddsId,
        mlb_gamePk: firstPass.found.mlb_gamePk,
        commence_time_local: Utilities.formatDate(new Date(oTime), TZ, "yyyy-MM-dd HH:mm")
      });
      continue;
    }

    var reason = firstPass.teamCandidates.length === 0 ? "no_team_token_match" : "outside_time_tolerance";
    rejectionSummary[reason] = (rejectionSummary[reason] || 0) + 1;

    var unmatchedEvent = {
      odds_game_id: oddsId,
      away_team: oAway,
      home_team: oHome,
      commence_time_utc: oTimeRaw,
      commence_time_ms: oTime,
      away_team_norm: oAwayNorm,
      home_team_norm: oHomeNorm,
      initial_reason: reason,
      teamCandidates: firstPass.teamCandidates,
      tolCandidates: firstPass.tolCandidates,
      nearMatches: getNearMatchDiagnostics_(schedRows, oAway, oHome)
    };

    if (reason === "no_team_token_match") unmatchedNoTeamEvents.push(unmatchedEvent);

    log_("DEBUG", "matchOddsToSchedule unmatched", {
      reason: reason,
      odds_event_id: oddsId,
      away_team: oAway,
      home_team: oHome,
      commence_time_utc: oTimeRaw,
      team_tokens: {
        raw: { away: oAway, home: oHome },
        canonical: { away: oAwayNorm, home: oHomeNorm }
      },
      candidate_schedule_rows: firstPass.teamCandidates,
      candidates_in_tolerance_window: firstPass.tolCandidates,
      match_tolerance_min: matchTolMin,
      near_match_diagnostics: unmatchedEvent.nearMatches
    });
  }

  if (enableTeamFallback && out.length === 0 && odds.length > 1 && unmatchedNoTeamEvents.length > 0 && unmatchedNoTeamEvents.length === (odds.length - 1)) {
    log_("INFO", "matchOddsToSchedule fallback attempt", {
      trigger: "all_events_no_team_token_match",
      unmatched_events: unmatchedNoTeamEvents.length,
      schedule_inventory_hash: getScheduleInventoryHash_(schedRows)
    });

    for (var f = 0; f < unmatchedNoTeamEvents.length; f++) {
      var ue = unmatchedNoTeamEvents[f];
      var fallbackRes = findBestScheduleMatch_(schedRows, {
        oddsAwayRaw: ue.away_team,
        oddsHomeRaw: ue.home_team,
        oddsAwayNorm: ue.away_team_norm,
        oddsHomeNorm: ue.home_team_norm,
        oddsTime: ue.commence_time_ms,
        matchTolMin: matchTolMin,
        mode: "fallback_similarity"
      });
      if (!fallbackRes.found) continue;

      out.push({
        odds_game_id: ue.odds_game_id,
        mlb_gamePk: fallbackRes.found.mlb_gamePk,
        commence_time_local: Utilities.formatDate(new Date(ue.commence_time_ms), TZ, "yyyy-MM-dd HH:mm")
      });

      rejectionSummary.no_team_token_match = Math.max(0, (rejectionSummary.no_team_token_match || 0) - 1);

      log_("INFO", "matchOddsToSchedule fallback matched", {
        match_mode: "fallback_team_similarity",
        odds_event_id: ue.odds_game_id,
        away_team: ue.away_team,
        home_team: ue.home_team,
        mlb_gamePk: fallbackRes.found.mlb_gamePk,
        schedule_away_team: fallbackRes.found.away_team,
        schedule_home_team: fallbackRes.found.home_team,
        schedule_away_team_id: fallbackRes.found.away_team_id,
        schedule_home_team_id: fallbackRes.found.home_team_id,
        team_match_score: round_(fallbackRes.bestScore, 4),
        match_orientation: (fallbackRes.bestScore >= 0.97 ? "swapped_orientation_or_exact" : "fallback_similarity")
      });
    }
  }

  if (out.length === 0 && (rejectionSummary.no_team_token_match || 0) > 0) {
    rejectionSummary.schedule_inventory_hash = getScheduleInventoryHash_(schedRows);
  }

  return { matched: out, rejectionSummary: rejectionSummary };
}

function findBestScheduleMatch_(schedRows, input) {
  var found = null;
  var bestDt = 999999;
  var bestScore = -1;
  var teamCandidates = [];
  var tolCandidates = [];
  var useFallback = String(input.mode || "") === "fallback_similarity";

  for (var s = 0; s < schedRows.length; s++) {
    var sr = schedRows[s];
    var sAwayNorm = normalizeTeam_(sr.away_team);
    var sHomeNorm = normalizeTeam_(sr.home_team);

    var directExact = (input.oddsAwayNorm === sAwayNorm && input.oddsHomeNorm === sHomeNorm);
    var swappedExact = (input.oddsAwayNorm === sHomeNorm && input.oddsHomeNorm === sAwayNorm);
    var teamMatched = false;
    var teamScore = 0;
    var orientation = "direct";

    if (directExact) {
      teamMatched = true;
      teamScore = 1;
      orientation = "direct";
    } else if (swappedExact) {
      teamMatched = true;
      teamScore = 0.98;
      orientation = "swapped_orientation";
    } else if (useFallback) {
      var directScore = scoreFallbackTeamPair_(input.oddsAwayRaw, input.oddsHomeRaw, sr.away_team, sr.home_team, sr.away_team_id, sr.home_team_id);
      var swappedScore = scoreFallbackTeamPair_(input.oddsAwayRaw, input.oddsHomeRaw, sr.home_team, sr.away_team, sr.home_team_id, sr.away_team_id);
      if (swappedScore > directScore) {
        teamScore = swappedScore;
        orientation = "swapped_orientation";
      } else {
        teamScore = directScore;
        orientation = "direct";
      }
      teamMatched = teamScore >= 0.75;
    }

    if (!teamMatched) continue;

    teamCandidates.push({
      mlb_gamePk: sr.mlb_gamePk,
      gameDate_utc: sr.gameDate,
      away_team: sr.away_team,
      home_team: sr.home_team,
      away_team_id: sr.away_team_id,
      home_team_id: sr.home_team_id,
      team_match_score: round_(teamScore, 4),
      match_orientation: orientation,
      team_tokens: {
        raw: { away: sr.away_team, home: sr.home_team },
        canonical: { away: sAwayNorm, home: sHomeNorm }
      }
    });

    var sTime = Date.parse(String(sr.gameDate || "")) || 0;
    var dtMin = Math.abs(input.oddsTime - sTime) / 60000;
    if (dtMin <= input.matchTolMin) {
      tolCandidates.push({
        mlb_gamePk: sr.mlb_gamePk,
        gameDate_utc: sr.gameDate,
        dt_min: dtMin,
        team_match_score: round_(teamScore, 4),
        match_orientation: orientation
      });
    }
    if (dtMin <= input.matchTolMin && (dtMin < bestDt || (dtMin === bestDt && teamScore > bestScore))) {
      bestDt = dtMin;
      bestScore = teamScore;
      found = sr;
    }
  }

  return { found: found, teamCandidates: teamCandidates, tolCandidates: tolCandidates, bestScore: bestScore };
}

function scoreFallbackTeamPair_(oddsAway, oddsHome, schedAway, schedHome, schedAwayId, schedHomeId) {
  var awayScore = scoreFallbackTeam_(oddsAway, schedAway, schedAwayId);
  var homeScore = scoreFallbackTeam_(oddsHome, schedHome, schedHomeId);
  return (awayScore + homeScore) / 2;
}

function scoreFallbackTeam_(oddsTeam, schedTeam, schedTeamId) {
  var normOdds = normalizeTeam_(oddsTeam);
  var normSched = normalizeTeam_(schedTeam);
  if (normOdds && normSched && normOdds === normSched) return 1;

  var oddsTokens = teamTokensForSimilarity_(oddsTeam);
  var schedTokens = teamTokensForSimilarity_(schedTeam);

  var overlap = tokenOverlapRatio_(oddsTokens, schedTokens);
  var oddsJoined = oddsTokens.join(" ");
  var schedJoined = schedTokens.join(" ");
  var containsBoost = (oddsJoined && schedJoined && (oddsJoined.indexOf(schedJoined) >= 0 || schedJoined.indexOf(oddsJoined) >= 0)) ? 0.9 : 0;

  var schedIdHit = false;
  if (schedTeamId) {
    var oddsRaw = String(oddsTeam || "");
    var normId = String(schedTeamId || "").toLowerCase();
    if (oddsRaw.toLowerCase().indexOf(normId) >= 0 || normOdds === normId) schedIdHit = true;
  }

  if (schedIdHit) return 1;
  return Math.max(overlap, containsBoost);
}

function teamTokensForSimilarity_(teamName) {
  var norm = normalizeTeam_(teamName);
  var src = norm || String(teamName || "").toLowerCase();
  var raw = src.replace(/[^a-z0-9 ]+/g, " ").split(/\s+/);
  var out = [];
  for (var i = 0; i < raw.length; i++) {
    var tk = String(raw[i] || "").trim();
    if (!tk || tk.length <= 1) continue;
    out.push(tk);
  }
  return out;
}

function tokenOverlapRatio_(aTokens, bTokens) {
  if (!aTokens.length || !bTokens.length) return 0;
  var bSet = {};
  for (var i = 0; i < bTokens.length; i++) bSet[bTokens[i]] = true;
  var hit = 0;
  for (var j = 0; j < aTokens.length; j++) if (bSet[aTokens[j]]) hit++;
  return hit / Math.max(aTokens.length, bTokens.length);
}

function getNearMatchDiagnostics_(schedRows, oddsAway, oddsHome) {
  var scores = [];
  for (var i = 0; i < schedRows.length; i++) {
    var sr = schedRows[i];
    var awayScore = scoreFallbackTeam_(oddsAway, sr.away_team, sr.away_team_id);
    var homeScore = scoreFallbackTeam_(oddsHome, sr.home_team, sr.home_team_id);
    scores.push({
      mlb_gamePk: sr.mlb_gamePk,
      away_team: sr.away_team,
      home_team: sr.home_team,
      away_team_id: sr.away_team_id,
      home_team_id: sr.home_team_id,
      away_similarity: round_(awayScore, 4),
      home_similarity: round_(homeScore, 4),
      pair_similarity: round_((awayScore + homeScore) / 2, 4)
    });
  }
  scores.sort(function (a, b) { return Number(b.pair_similarity || 0) - Number(a.pair_similarity || 0); });
  return scores.slice(0, 3);
}

function buildScheduleInventory_(schedRows) {
  var awaySeen = {};
  var homeSeen = {};
  var away = [];
  var home = [];

  for (var i = 0; i < schedRows.length; i++) {
    var a = String(schedRows[i].away_team || "").trim();
    var h = String(schedRows[i].home_team || "").trim();
    if (a && !awaySeen[a]) { awaySeen[a] = true; away.push(a); }
    if (h && !homeSeen[h]) { homeSeen[h] = true; home.push(h); }
  }

  away.sort();
  home.sort();
  return {
    schedule_games: schedRows.length,
    away_unique_count: away.length,
    home_unique_count: home.length,
    away_unique_names: away,
    home_unique_names: home,
    inventory_hash: getScheduleInventoryHash_(schedRows)
  };
}

function getScheduleInventoryHash_(schedRows) {
  var inv = buildScheduleInventorySeed_(schedRows);
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, inv);
  var out = [];
  for (var i = 0; i < bytes.length; i++) {
    var v = (bytes[i] + 256) % 256;
    var hx = v.toString(16);
    out.push(hx.length === 1 ? "0" + hx : hx);
  }
  return out.join("").slice(0, 16);
}

function buildScheduleInventorySeed_(schedRows) {
  var entries = [];
  for (var i = 0; i < schedRows.length; i++) {
    var sr = schedRows[i];
    entries.push([
      normalizeTeam_(sr.away_team),
      normalizeTeam_(sr.home_team),
      String(sr.away_team_id || ""),
      String(sr.home_team_id || "")
    ].join("|"));
  }
  entries.sort();
  return entries.join("||");
}


function isAllEventsNoTeamTokenMatch_(matchedCount, rejectionSummary, oddsCount) {
  return Number(matchedCount || 0) === 0 && Number((rejectionSummary && rejectionSummary.no_team_token_match) || 0) >= Math.max(1, Number(oddsCount || 0));
}

function assertEq_(label, actual, expected) {
  if (actual !== expected) {
    throw new Error(label + " expected=" + String(expected) + " actual=" + String(actual));
  }
}

function test_scheduleQueryWindow_midnightBoundary_() {
  var cfg = {
    ODDS_SCHEDULE_QUERY_BUFFER_BEFORE_H: 2,
    ODDS_SCHEDULE_QUERY_BUFFER_AFTER_H: 2
  };
  var commenceTimes = [
    Date.parse("2025-05-11T06:30:00Z"),
    Date.parse("2025-05-11T08:15:00Z")
  ];
  var window = deriveScheduleQueryWindowFromOdds_(commenceTimes, cfg);

  assertEq_("midnight startDate", window.startDate, "2025-05-10");
  assertEq_("midnight endDate", window.endDate, "2025-05-11");
  assertEq_("midnight source", window.source, "odds_commence_bounds");
}

function test_scheduleQueryWindow_splitSquadCoverage_() {
  var cfg = {
    ODDS_SCHEDULE_QUERY_BUFFER_BEFORE_H: 24,
    ODDS_SCHEDULE_QUERY_BUFFER_AFTER_H: 24
  };
  var commenceTimes = [
    Date.parse("2025-03-12T20:05:00Z"),
    Date.parse("2025-03-14T01:10:00Z")
  ];
  var window = deriveScheduleQueryWindowFromOdds_(commenceTimes, cfg);

  assertEq_("split squad startDate", window.startDate, "2025-03-11");
  assertEq_("split squad endDate", window.endDate, "2025-03-14");
  assertEq_("split squad source", window.source, "odds_commence_bounds");
}

function test_scheduleWindow_matchGuardrail_noAllEventsNoTeamTokenMatch_() {
  var schedRows = [
    { mlb_gamePk: "1", gameDate: "2025-03-11T20:05:00Z", away_team: "Boston Red Sox", home_team: "New York Yankees", away_team_id: "111", home_team_id: "147" },
    { mlb_gamePk: "2", gameDate: "2025-03-12T01:10:00Z", away_team: "Chicago Cubs", home_team: "Los Angeles Dodgers", away_team_id: "112", home_team_id: "119" }
  ];

  var samples = [
    { away: "Red Sox", home: "Yankees", t: Date.parse("2025-03-11T20:15:00Z") },
    { away: "Cubs", home: "Dodgers", t: Date.parse("2025-03-12T01:25:00Z") }
  ];

  var matched = 0;
  var rejectionSummary = { no_team_token_match: 0 };
  for (var i = 0; i < samples.length; i++) {
    var s = samples[i];
    var pass = findBestScheduleMatch_(schedRows, {
      oddsAwayRaw: s.away,
      oddsHomeRaw: s.home,
      oddsAwayNorm: normalizeTeam_(s.away),
      oddsHomeNorm: normalizeTeam_(s.home),
      oddsTime: s.t,
      matchTolMin: 360,
      mode: "canonical_exact"
    });
    if (pass.found) matched++;
    else if (!pass.teamCandidates.length) rejectionSummary.no_team_token_match++;
  }

  assertEq_("guardrail matched all samples", matched, 2);
  assertEq_("guardrail no team token rejects", rejectionSummary.no_team_token_match, 0);
  assertEq_("guardrail no all-events trigger", isAllEventsNoTeamTokenMatch_(matched, rejectionSummary, samples.length), false);
}

function test_scheduleWindow_primaryPassShortCircuit_() {
  var decision = isPrimaryWindowFullMatchWithoutHardRejections_(3, {
    invalid_odds_time: 0,
    outside_time_tolerance: 0,
    no_team_token_match: 0
  }, 3);
  assertEq_("primary pass short-circuit true", decision, true);
}

function test_scheduleWindow_primaryPassNoShortCircuitWhenRejected_() {
  var decision = isPrimaryWindowFullMatchWithoutHardRejections_(3, {
    invalid_odds_time: 1,
    outside_time_tolerance: 0,
    no_team_token_match: 0
  }, 3);
  assertEq_("primary pass short-circuit false when hard rejection", decision, false);
}

function runScheduleWindowGuardrailTests_() {
  test_scheduleQueryWindow_midnightBoundary_();
  test_scheduleQueryWindow_splitSquadCoverage_();
  test_scheduleWindow_matchGuardrail_noAllEventsNoTeamTokenMatch_();
  test_scheduleWindow_primaryPassShortCircuit_();
  test_scheduleWindow_primaryPassNoShortCircuitWhenRejected_();
  log_("INFO", "runScheduleWindowGuardrailTests passed", {});
  return true;
}
