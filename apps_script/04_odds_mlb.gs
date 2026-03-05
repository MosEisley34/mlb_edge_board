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
  var nowLocal = isoLocalWithOffset_(new Date());

  var rows = [];
  for (var i = 0; i < data.length; i++) {
    var g = data[i];
    var best = pickBestH2H_(g.bookmakers || [], g.away_team, g.home_team);

    rows.push([
      String(g.id || ""),
      String(g.commence_time || ""),
      String(g.away_team || ""),
      String(g.home_team || ""),
      best.awayDecimal,
      best.homeDecimal,
      best.awayImplied,
      best.homeImplied,
      best.bestBookAway,
      best.bestBookHome,
      nowLocal,
      sportKeyUsed
    ]);
  }

  replaceSheetBody_(shOdds, rows);
  log_("INFO", "refreshOdds completed", { sportKeyUsed: sportKeyUsed, games: rows.length, windowHours: lookaheadH, from: fromIso, to: toIso });
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
  log_("INFO", "Odds API credits", {
    used: String(headers["x-requests-used"] || headers["X-Requests-Used"] || ""),
    remaining: String(headers["x-requests-remaining"] || headers["X-Requests-Remaining"] || ""),
    http: http
  });

  if (http !== 200) {
    log_("ERROR", "Odds API fetch failed", { http: http, body: resp.getContentText().slice(0, 500) });
    return [];
  }

  try { return JSON.parse(resp.getContentText()) || []; }
  catch (e) { log_("ERROR", "Odds API JSON parse failed", { message: String(e) }); return []; }
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

  var startDate, endDate;
  if (commenceTimes.length === 0) {
    var now = new Date();
    startDate = Utilities.formatDate(now, TZ, "yyyy-MM-dd");
    endDate = Utilities.formatDate(new Date(now.getTime() + 24 * 3600 * 1000), TZ, "yyyy-MM-dd");
  } else {
    var minT = Math.min.apply(null, commenceTimes);
    var maxT = Math.max.apply(null, commenceTimes);
    startDate = Utilities.formatDate(new Date(minT), "UTC", "yyyy-MM-dd");
    endDate = Utilities.formatDate(new Date(maxT + 24 * 3600 * 1000), "UTC", "yyyy-MM-dd");
  }

  log_("INFO", "MLB schedule query window", {
    startDate: startDate,
    endDate: endDate,
    oddsUpcoming: oddsRows.length,
    matchTolMin: cfg.MATCH_TOL_MIN,
    lineupMin: cfg.LINEUP_MIN
  });

  var shouldUseExpandedFallback = oddsRows.length <= 3;
  var expandedFallbackUsed = false;

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
  var matchRes = matchOddsToSchedule_(shOdds, shSchedule, matchTolMin);
  var matched = matchRes.matched;

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
        replaceSheetBody_(shSchedExpanded, expandedRows);
        var expandedMatchRes = matchOddsToSchedule_(shOdds, shSchedExpanded, matchTolMin);
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
    expandedWindowFallbackUsed: expandedFallbackUsed,
    rejectionSummary: matchRes.rejectionSummary
  });
  return {
    scheduleGames: games.length,
    matchedCount: matched.length,
    lineupRows: lineupRows.length,
    matched: matched,
    rejectionSummary: matchRes.rejectionSummary,
    expandedWindowFallbackUsed: expandedFallbackUsed
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

function matchOddsToSchedule_(shOdds, shSchedule, matchTolMin) {
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

  var schedRows = [];
  for (var i = 1; i < sched.length; i++) {
    var r = sched[i];
    var gamePk = String(r[sIdxPk] || "");
    if (!gamePk) continue;
    schedRows.push({ mlb_gamePk: gamePk, gameDate: String(r[sIdxDate] || ""), away_team: String(r[sIdxAway] || ""), home_team: String(r[sIdxHome] || "") });
  }

  var out = [];
  var rejectionSummary = {
    invalid_odds_time: 0,
    no_team_token_match: 0,
    outside_time_tolerance: 0
  };
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

    var found = null;
    var bestDt = 999999;
    var teamCandidates = [];
    var tolCandidates = [];

    for (var s = 0; s < schedRows.length; s++) {
      var sr = schedRows[s];
      var sAwayNorm = normalizeTeam_(sr.away_team);
      var sHomeNorm = normalizeTeam_(sr.home_team);
      if (oAwayNorm !== sAwayNorm) continue;
      if (oHomeNorm !== sHomeNorm) continue;

      teamCandidates.push({
        mlb_gamePk: sr.mlb_gamePk,
        gameDate_utc: sr.gameDate,
        away_team: sr.away_team,
        home_team: sr.home_team,
        team_tokens: {
          raw: { away: sr.away_team, home: sr.home_team },
          canonical: { away: sAwayNorm, home: sHomeNorm }
        }
      });

      var sTime = Date.parse(String(sr.gameDate || "")) || 0;
      var dtMin = Math.abs(oTime - sTime) / 60000;
      if (dtMin <= matchTolMin) {
        tolCandidates.push({
          mlb_gamePk: sr.mlb_gamePk,
          gameDate_utc: sr.gameDate,
          dt_min: dtMin
        });
      }
      if (dtMin <= matchTolMin && dtMin < bestDt) { bestDt = dtMin; found = sr; }
    }

    if (found) {
      out.push({
        odds_game_id: oddsId,
        mlb_gamePk: found.mlb_gamePk,
        commence_time_local: Utilities.formatDate(new Date(oTime), TZ, "yyyy-MM-dd HH:mm")
      });
    } else {
      var reason = teamCandidates.length === 0 ? "no_team_token_match" : "outside_time_tolerance";
      rejectionSummary[reason] = (rejectionSummary[reason] || 0) + 1;
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
        candidate_schedule_rows: teamCandidates,
        candidates_in_tolerance_window: tolCandidates,
        match_tolerance_min: matchTolMin
      });
    }
  }

  return { matched: out, rejectionSummary: rejectionSummary };
}
