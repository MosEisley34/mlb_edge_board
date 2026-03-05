function refreshModelAndEdge_core_(cfg, mlbRes) {
  var ss = SpreadsheetApp.getActive();
  var shOdds = ss.getSheetByName(SH.ODDS_RAW);
  var shSched = ss.getSheetByName(SH.MLB_SCHEDULE);
  var shLine = ss.getSheetByName(SH.MLB_LINEUPS);
  var shHit = ss.getSheetByName(SH.BATTER_PROJ);
  var shPit = ss.getSheetByName(SH.PITCHER_PROJ);
  var shEdge = ss.getSheetByName(SH.EDGE_BOARD);
  var shNotify = ss.getSheetByName(SH.NOTIFY_STATE);

  var mode = String(cfg.MODE || "PRESEASON").toUpperCase();
  var lineupMin = toInt_(cfg.LINEUP_MIN, 9);
  var kOps = toFloat_(cfg.MODEL_K_OPS, 6.0);
  var kPit = toFloat_(cfg.MODEL_K_PIT, 3.0);
  var defaultOPS = toFloat_(cfg.LEAGUE_AVG_OPS, 0.675);
  var defaultSIERA = toFloat_(cfg.DEFAULT_PITCHER_SIERA, 4.20);
  var requirePitcherMatch = String(cfg.REQUIRE_PITCHER_MATCH || "false").toLowerCase() === "true";

  var oddsArr = readSheetAsObjects_(shOdds), oddsById = {};
  for (var i = 0; i < oddsArr.length; i++) {
    var id = String(oddsArr[i].odds_game_id || "").trim();
    if (id) oddsById[id] = oddsArr[i];
  }

  var schedArr = readSheetAsObjects_(shSched), schedByPk = {};
  for (var s = 0; s < schedArr.length; s++) {
    var pk = String(schedArr[s].mlb_gamePk || "").trim();
    if (pk) schedByPk[pk] = schedArr[s];
  }

  var lineupsByOdds = groupLineupsByOdds_(readSheetAsObjects_(shLine));
  var opsMapObj = buildOPSMap_(shHit);
  var sieraMapObj = buildSIERAMap_(shPit);
  var opsLeagueAvg = opsMapObj.leagueAvgOps || defaultOPS;

  var matched = (mlbRes && mlbRes.matched && mlbRes.matched.length)
    ? mlbRes.matched
    : matchOddsToSchedule_(shOdds, shSched, toInt_(cfg.MATCH_TOL_MIN, 360));

  var todayKey = localDateKey_();
  var exposure = getExposureState_(shNotify, todayKey);
  var caps = getCaps_(cfg, mode);
  var th = getThresholds_(cfg, mode);
  var unitsCfg = getUnitsCfg_(cfg, mode);

  var edgeRows = [];
  var computed = 0;
  var betSignalsFound = 0;
  var skippedNoMatch = 0, skippedLineups = 0, skippedPitchers = 0;

  for (var m = 0; m < matched.length; m++) {
    var oddsId = String(matched[m].odds_game_id || "").trim();
    var mlbPk = String(matched[m].mlb_gamePk || "").trim();
    if (!oddsId || !mlbPk) continue;

    var o = oddsById[oddsId];
    if (!o) { skippedNoMatch++; continue; }

    var awayTeam = String(o.away_team || "");
    var homeTeam = String(o.home_team || "");

    var lu = lineupsByOdds[oddsId] || { away: [], home: [] };
    var awayLu = lu.away.slice(0).sort(sortBatOrder_).slice(0, lineupMin);
    var homeLu = lu.home.slice(0).sort(sortBatOrder_).slice(0, lineupMin);
    var ready = (awayLu.length >= lineupMin && homeLu.length >= lineupMin);

    if (!ready) {
      skippedLineups++;
      edgeRows.push(makeEdgeRow_({
        odds_game_id: oddsId, mlb_gamePk: mlbPk,
        commence_time_local: String(matched[m].commence_time_local || ""),
        away_team: awayTeam, home_team: homeTeam,
        away_odds_decimal: o.away_odds_decimal, home_odds_decimal: o.home_odds_decimal,
        away_implied: implied_(o.away_odds_decimal, o.away_implied), home_implied: implied_(o.home_odds_decimal, o.home_implied),
        model_p_away: "", model_p_home: "", edge_away: "", edge_home: "",
        away_hitters_matched: awayLu.length, home_hitters_matched: homeLu.length,
        min_hitters_matched: Math.min(awayLu.length, homeLu.length),
        away_pitcher_name: "", home_pitcher_name: "", away_pitcher_matched: "", home_pitcher_matched: "",
        confidence: "", bet_side: "", bet_tier: "", bet_edge: "", units: "",
        notes: "WAIT_LINEUPS", updated_at_local: isoLocalWithOffset_(new Date())
      }));
      continue;
    }

    var awayOPS = lineupOPS_(awayLu, opsMapObj.map, opsLeagueAvg);
    var homeOPS = lineupOPS_(homeLu, opsMapObj.map, opsLeagueAvg);

    var sched = schedByPk[mlbPk] || null;
    var awayP = sched ? String(sched.away_probable_pitcher || "") : "";
    var homeP = sched ? String(sched.home_probable_pitcher || "") : "";

    var awaySI = pitcherSIERA_(awayP, sieraMapObj.map, defaultSIERA);
    var homeSI = pitcherSIERA_(homeP, sieraMapObj.map, defaultSIERA);

    if (requirePitcherMatch && (!awaySI.matched || !homeSI.matched)) {
      skippedPitchers++;
      edgeRows.push(makeEdgeRow_({
        odds_game_id: oddsId, mlb_gamePk: mlbPk, commence_time_local: String(matched[m].commence_time_local || ""),
        away_team: awayTeam, home_team: homeTeam,
        away_odds_decimal: o.away_odds_decimal, home_odds_decimal: o.home_odds_decimal,
        away_implied: implied_(o.away_odds_decimal, o.away_implied), home_implied: implied_(o.home_odds_decimal, o.home_implied),
        model_p_away: "", model_p_home: "", edge_away: "", edge_home: "",
        away_hitters_matched: awayOPS.matched, home_hitters_matched: homeOPS.matched,
        min_hitters_matched: Math.min(awayOPS.matched, homeOPS.matched),
        away_pitcher_name: awayP, home_pitcher_name: homeP,
        away_pitcher_matched: awaySI.matched ? "Y" : "N", home_pitcher_matched: homeSI.matched ? "Y" : "N",
        confidence: "", bet_side: "", bet_tier: "", bet_edge: "", units: "",
        notes: "WAIT_PITCHERS", updated_at_local: isoLocalWithOffset_(new Date())
      }));
      continue;
    }

    var awayPitFactor = clamp_(0.10, 0.60, 1 / awaySI.siera);
    var homePitFactor = clamp_(0.10, 0.60, 1 / homeSI.siera);
    var x = kOps * (awayOPS.ops - homeOPS.ops) + kPit * (awayPitFactor - homePitFactor);
    var pAway = clamp_(0.05, 0.95, 1 / (1 + Math.exp(-x)));
    var pHome = 1 - pAway;

    var awayImp = implied_(o.away_odds_decimal, o.away_implied);
    var homeImp = implied_(o.home_odds_decimal, o.home_implied);
    var vigSum = (isFinite(awayImp) && isFinite(homeImp) && (awayImp + homeImp) > 0) ? (awayImp + homeImp) : NaN;
    var awayNoVig = isFinite(vigSum) ? (awayImp / vigSum) : NaN;
    var homeNoVig = isFinite(vigSum) ? (homeImp / vigSum) : NaN;
    var edgeAway = (isFinite(awayImp) ? (pAway - awayImp) : "");
    var edgeHome = (isFinite(homeImp) ? (pHome - homeImp) : "");
    var conf = confidence_(mode, awayOPS.matched, homeOPS.matched, awaySI.matched, homeSI.matched);

    var bet = pickBetSignal_(edgeAway, edgeHome, conf, th);
    var units = "";
    var notes = "";

    if (bet.side) {
      if (exposure.plays >= caps.maxPlays) {
        notes = "cap_plays";
        bet.side = "";
      } else {
        var u = computeUnits_(unitsCfg, bet.tier, conf);
        if ((exposure.units + u) > caps.maxUnits) {
          notes = "cap_units";
          bet.side = "";
        } else {
          units = u;
          betSignalsFound++;

          var sent = maybeNotifyDiscord_(cfg, oddsId, todayKey, {
            mode: mode,
            awayTeam: awayTeam,
            homeTeam: homeTeam,
            commenceLocal: String(matched[m].commence_time_local || ""),
            bet: bet,
            units: units,
            conf: conf,
            pAway: pAway,
            pHome: pHome,
            awayOdds: o.away_odds_decimal,
            homeOdds: o.home_odds_decimal,
            awayImp: awayImp,
            homeImp: homeImp,
            awayNoVig: awayNoVig,
            homeNoVig: homeNoVig,
            coverageAway: awayOPS.matched + "/" + lineupMin,
            coverageHome: homeOPS.matched + "/" + lineupMin,
            pitchers: (awaySI.matched ? "Y" : "N") + "/" + (homeSI.matched ? "Y" : "N"),
            mlbGamePk: mlbPk,
            updatedAt: (o.updated_at_local || o.updated_at_utc || "")
          });

          if (sent) {
            exposure.plays += 1;
            exposure.units += Number(units || 0);
            saveExposureState_(shNotify, todayKey, exposure);
          }
        }
      }
    }

    edgeRows.push(makeEdgeRow_({
      odds_game_id: oddsId, mlb_gamePk: mlbPk, commence_time_local: String(matched[m].commence_time_local || ""),
      away_team: awayTeam, home_team: homeTeam,
      away_odds_decimal: o.away_odds_decimal, home_odds_decimal: o.home_odds_decimal,
      away_implied: isFinite(awayImp) ? awayImp : "", home_implied: isFinite(homeImp) ? homeImp : "",
      model_p_away: pAway, model_p_home: pHome, edge_away: edgeAway, edge_home: edgeHome,
      away_hitters_matched: awayOPS.matched, home_hitters_matched: homeOPS.matched,
      min_hitters_matched: Math.min(awayOPS.matched, homeOPS.matched),
      away_pitcher_name: awayP, home_pitcher_name: homeP,
      away_pitcher_matched: awaySI.matched ? "Y" : "N", home_pitcher_matched: homeSI.matched ? "Y" : "N",
      confidence: conf, bet_side: bet.side || "", bet_tier: bet.tier || "", bet_edge: bet.side ? bet.edge : "",
      units: units, notes: notes || bet.notes || "", updated_at_local: isoLocalWithOffset_(new Date())
    }));

    computed++;
  }

  writeRowsByHeader_(shEdge, edgeRows);

  log_("INFO", "refreshModelAndEdge completed", {
    opsLeagueAvg: opsLeagueAvg,
    matched: matched.length,
    computed: computed,
    skippedNoMatch: skippedNoMatch,
    skippedLineups: skippedLineups,
    skippedPitchers: skippedPitchers,
    betSignalsFound: betSignalsFound
  });

  return { computed: computed, betSignalsFound: betSignalsFound };
}

function makeEdgeRow_(obj) { return obj; }
function writeRowsByHeader_(sheet, rowObjects) {
  var values = sheet.getDataRange().getValues();
  if (!values || values.length < 1) return;
  var header = values[0].map(function (x) { return String(x || "").trim(); });
  var colCount = header.length;
  var lastRow = sheet.getLastRow();
  if (lastRow > 1) sheet.getRange(2, 1, lastRow - 1, colCount).clearContent();
  if (!rowObjects || !rowObjects.length) return;

  var rows = [];
  for (var i = 0; i < rowObjects.length; i++) {
    var o = rowObjects[i] || {};
    var row = new Array(colCount);
    for (var c = 0; c < colCount; c++) row[c] = (o[header[c]] === undefined) ? "" : o[header[c]];
    rows.push(row);
  }
  sheet.getRange(2, 1, rows.length, colCount).setValues(rows);
}

function groupLineupsByOdds_(lineArr) {
  var out = {};
  for (var i = 0; i < lineArr.length; i++) {
    var r = lineArr[i];
    var oid = String(r.odds_game_id || "").trim();
    if (!oid) continue;

    var side = String(r.side || "").toLowerCase();
    if (side !== "away" && side !== "home") continue;

    var posted = String(r.is_confirmed || "").toUpperCase();
    if (posted && posted !== "Y" && posted !== "TRUE" && posted !== "YES" && posted !== "1") continue;

    if (!out[oid]) out[oid] = { away: [], home: [] };
    out[oid][side].push({ bat_order: Number(r.bat_order || 999), player_name: String(r.player_name || "") });
  }
  return out;
}

function sortBatOrder_(a, b) { return Number(a.bat_order || 999) - Number(b.bat_order || 999); }
function normName_(s) { return String(s || "").toLowerCase().replace(/\./g, "").replace(/'/g, "").replace(/-/g, " ").replace(/\s+/g, " ").trim(); }

function buildOPSMap_(shHit) {
  var v = shHit.getDataRange().getValues();
  if (!v || v.length < 2) return { map: {}, leagueAvgOps: 0 };

  var header = v[0].map(function (x) { return String(x || "").trim(); });
  var idxName = header.indexOf("Name");
  if (idxName < 0) idxName = header.indexOf("Player");
  var idxOPS = header.indexOf("OPS");

  var map = {}, sum = 0, cnt = 0;
  for (var i = 1; i < v.length; i++) {
    var name = idxName >= 0 ? String(v[i][idxName] || "").trim() : "";
    if (!name) continue;
    var ops = idxOPS >= 0 ? Number(v[i][idxOPS]) : NaN;
    if (!isFinite(ops) || ops <= 0) continue;
    map[normName_(name)] = ops;
    sum += ops;
    cnt++;
  }
  return { map: map, leagueAvgOps: (cnt > 0 ? (sum / cnt) : 0) };
}

function buildSIERAMap_(shPit) {
  var v = shPit.getDataRange().getValues();
  if (!v || v.length < 2) return { map: {} };

  var header = v[0].map(function (x) { return String(x || "").trim(); });
  var idxName = header.indexOf("Name");
  if (idxName < 0) idxName = header.indexOf("Player");

  var idxSIERA = header.indexOf("SIERA");
  if (idxSIERA < 0) {
    for (var i = 0; i < header.length; i++) {
      if (String(header[i]).toLowerCase().indexOf("siera") >= 0) { idxSIERA = i; break; }
    }
  }

  var map = {};
  for (var r = 1; r < v.length; r++) {
    var name = idxName >= 0 ? String(v[r][idxName] || "").trim() : "";
    if (!name) continue;
    var siera = idxSIERA >= 0 ? Number(v[r][idxSIERA]) : NaN;
    if (!isFinite(siera) || siera <= 0) continue;
    map[normName_(name)] = siera;
  }
  return { map: map };
}

function lineupOPS_(lineupArr, opsMap, fallbackOPS) {
  var matched = 0, sum = 0;
  for (var i = 0; i < lineupArr.length; i++) {
    var key = normName_(lineupArr[i].player_name || "");
    var ops = opsMap[key];
    if (ops !== undefined && isFinite(ops)) { matched++; sum += Number(ops); }
    else sum += fallbackOPS;
  }
  var opsAvg = (lineupArr.length > 0) ? (sum / lineupArr.length) : fallbackOPS;
  return { ops: opsAvg, matched: matched };
}

function pitcherSIERA_(pitcherName, sieraMap, fallbackSIERA) {
  var nm = String(pitcherName || "").trim();
  if (!nm) return { siera: fallbackSIERA, matched: false };
  var v = sieraMap[normName_(nm)];
  if (v !== undefined && isFinite(v) && v > 0) return { siera: Number(v), matched: true };
  return { siera: fallbackSIERA, matched: false };
}

function implied_(decOdds, fallbackImp) {
  var o = Number(decOdds);
  if (isFinite(o) && o > 1.0001) return 1 / o;
  var f = Number(fallbackImp);
  return isFinite(f) ? f : NaN;
}

function clamp_(lo, hi, x) { return Math.max(lo, Math.min(hi, x)); }

function confidence_(mode, awayHitMatched, homeHitMatched, awayPitMatched, homePitMatched) {
  var c = 50;
  c += (awayHitMatched / 9) * 20;
  c += (homeHitMatched / 9) * 20;
  c += (awayPitMatched ? 5 : -5);
  c += (homePitMatched ? 5 : -5);

  var miss = (9 - awayHitMatched) + (9 - homeHitMatched);
  var missPenalty = (String(mode).toUpperCase() === "PRESEASON") ? 2.0 : 3.5;
  c -= miss * missPenalty;
  return clamp_(0, 100, c);
}

function getThresholds_(cfg, mode) {
  mode = String(mode || "PRESEASON").toUpperCase();
  if (mode === "PRESEASON") {
    return {
      EDGE_MICRO: toFloat_(cfg.PS_EDGE_MICRO, 0.018),
      EDGE_SMALL: toFloat_(cfg.PS_EDGE_SMALL, 0.028),
      EDGE_MED: toFloat_(cfg.PS_EDGE_MED, 0.040),
      EDGE_STRONG: toFloat_(cfg.PS_EDGE_STRONG, 0.050),
      CONF_MIN: toFloat_(cfg.PS_CONF_MIN, 55)
    };
  }
  return {
    EDGE_MICRO: toFloat_(cfg.RS_EDGE_MICRO, 0.020),
    EDGE_SMALL: toFloat_(cfg.RS_EDGE_SMALL, 0.040),
    EDGE_MED: toFloat_(cfg.RS_EDGE_MED, 0.055),
    EDGE_STRONG: toFloat_(cfg.RS_EDGE_STRONG, 0.065),
    CONF_MIN: toFloat_(cfg.RS_CONF_MIN, 62)
  };
}

function getCaps_(cfg, mode) {
  mode = String(mode || "PRESEASON").toUpperCase();
  if (mode === "PRESEASON") return { maxPlays: toInt_(cfg.PS_MAX_PLAYS_DAY, 6), maxUnits: toFloat_(cfg.PS_MAX_UNITS_DAY, 2.0) };
  return { maxPlays: toInt_(cfg.RS_MAX_PLAYS_DAY, 5), maxUnits: toFloat_(cfg.RS_MAX_UNITS_DAY, 2.5) };
}

function getUnitsCfg_(cfg, mode) {
  mode = String(mode || "PRESEASON").toUpperCase();
  if (mode === "PRESEASON") {
    return {
      base: toFloat_(cfg.PS_UNIT_BASE, 0.20),
      mult: { MICRO: toFloat_(cfg.PS_UNIT_MICRO_MULT, 0.6), SMALL: toFloat_(cfg.PS_UNIT_SMALL_MULT, 1.0), MED: toFloat_(cfg.PS_UNIT_MED_MULT, 1.25), STRONG: toFloat_(cfg.PS_UNIT_STRONG_MULT, 1.5) }
    };
  }
  return {
    base: toFloat_(cfg.RS_UNIT_BASE, 0.25),
    mult: { MICRO: toFloat_(cfg.RS_UNIT_MICRO_MULT, 0.6), SMALL: toFloat_(cfg.RS_UNIT_SMALL_MULT, 1.0), MED: toFloat_(cfg.RS_UNIT_MED_MULT, 1.3), STRONG: toFloat_(cfg.RS_UNIT_STRONG_MULT, 1.6) }
  };
}

function pickBetSignal_(edgeAway, edgeHome, confidence, th) {
  var a = (edgeAway === "" ? -999 : Number(edgeAway));
  var h = (edgeHome === "" ? -999 : Number(edgeHome));
  if (!isFinite(a)) a = -999;
  if (!isFinite(h)) h = -999;

  var side = (a > h) ? "AWAY" : "HOME";
  var edge = (a > h) ? a : h;

  if (confidence < th.CONF_MIN) return { side: "", tier: "", edge: "", notes: "conf<" + th.CONF_MIN };
  if (!isFinite(edge) || edge <= -999) return { side: "", tier: "", edge: "", notes: "no_edge" };

  var tier = "";
  if (edge >= th.EDGE_STRONG) tier = "STRONG";
  else if (edge >= th.EDGE_MED) tier = "MED";
  else if (edge >= th.EDGE_SMALL) tier = "SMALL";
  else if (edge >= th.EDGE_MICRO) tier = "MICRO";
  if (!tier) return { side: "", tier: "", edge: "", notes: "edge<threshold" };

  return { side: side, tier: tier, edge: edge, notes: "" };
}

function computeUnits_(unitsCfg, tier, confidence) {
  var t = String(tier || "").toUpperCase();
  var mult = unitsCfg.mult[t] || 1.0;
  var confScale = clamp_(0.75, 1.15, 0.75 + (confidence / 100) * 0.40);
  var u = unitsCfg.base * mult * confScale;
  return Math.round(u * 100) / 100;
}

function maybeNotifyDiscord_(cfg, oddsId, dateKey, payload) {
  var maxAgeMin = toFloat_(cfg.NOTIFY_MAX_ODDS_AGE_MIN, 45);
  var cooldownMin = toFloat_(cfg.NOTIFY_COOLDOWN_MIN, 60);
  var minOddsMove = toFloat_(cfg.NOTIFY_MIN_ODDS_MOVE, 0.03);
  var minEdgeMovePct = toFloat_(cfg.NOTIFY_MIN_EDGE_MOVE_PCT, 0.75);
  var t = Date.parse(String(payload.updatedAt || ""));
  if (t) {
    var ageMin = (new Date().getTime() - t) / 60000;
    if (ageMin > maxAgeMin) {
      log_("INFO", "Discord notify skipped: stale odds", { oddsId: oddsId, ageMin: round_(ageMin, 2), maxAgeMin: maxAgeMin });
      return false;
    }
  }

  var props = PropertiesService.getScriptProperties();
  var key = "NOTIFY_" + String(oddsId);
  var prevRaw = props.getProperty(key) || "";
  var prevState = parseNotifyState_(prevRaw);

  var sig = [dateKey, payload.bet.side, payload.bet.tier, round_(payload.bet.edge, 4), round_(payload.pAway, 4), round_(payload.pHome, 4)].join("|");
  if (prevState.sig === sig) {
    log_("INFO", "Discord notify skipped: duplicate signal", { oddsId: oddsId, sig: sig });
    return false;
  }

  var pickTeam = (payload.bet.side === "AWAY") ? payload.awayTeam : payload.homeTeam;
  var price = Number((payload.bet.side === "AWAY") ? payload.awayOdds : payload.homeOdds);
  var implied = (payload.bet.side === "AWAY") ? payload.awayImp : payload.homeImp;
  var noVig = (payload.bet.side === "AWAY") ? payload.awayNoVig : payload.homeNoVig;
  var modelP = (payload.bet.side === "AWAY") ? payload.pAway : payload.pHome;
  var edge = Number(payload.bet.edge);
  var nowMs = new Date().getTime();
  var isSameDaySignal = (String(prevState.dateKey || "") === String(dateKey || ""));

  var reason = "";
  if (isFinite(prevState.lastSentMs) && cooldownMin > 0) {
    var minsSinceLast = (nowMs - prevState.lastSentMs) / 60000;
    if (minsSinceLast < cooldownMin) {
      log_("INFO", "Discord notify skipped: cooldown", {
        oddsId: oddsId,
        minsSinceLast: round_(minsSinceLast, 2),
        cooldownMin: cooldownMin,
        lastTier: prevState.lastTier || "",
        tier: payload.bet.tier
      });
      return false;
    }
    reason = "cooldown_expired";
  }

  var hasPriorMetrics = isFinite(prevState.lastPrice) && isFinite(prevState.lastEdge);
  var oddsMove = hasPriorMetrics ? Math.abs(price - prevState.lastPrice) : NaN;
  var edgeMovePct = hasPriorMetrics ? Math.abs(edge - prevState.lastEdge) * 100 : NaN;
  var tierChanged = String(prevState.lastTier || "") !== String(payload.bet.tier || "");
  if (hasPriorMetrics && !tierChanged && oddsMove < minOddsMove && edgeMovePct < minEdgeMovePct) {
    log_("INFO", "Discord notify skipped: insufficient movement", {
      oddsId: oddsId,
      oddsMove: round_(oddsMove, 4),
      minOddsMove: minOddsMove,
      edgeMovePct: round_(edgeMovePct, 3),
      minEdgeMovePct: minEdgeMovePct,
      tier: payload.bet.tier
    });
    return false;
  }

  if (!reason) {
    if (tierChanged) reason = "tier_upgrade";
    else if (hasPriorMetrics && edgeMovePct >= minEdgeMovePct) reason = "edge_delta";
    else reason = "cooldown_expired";
  }

  var betId = createPendingBet_(cfg, {
    oddsGameId: oddsId,
    mlbGamePk: payload.mlbGamePk || "",
    awayTeam: payload.awayTeam,
    homeTeam: payload.homeTeam,
    pickSide: payload.bet.side,
    pickTeam: pickTeam,
    commenceLocal: payload.commenceLocal,
    pickPrice: price,
    modelProb: modelP,
    implied: implied,
    noVigImplied: isFinite(noVig) ? noVig : "",
    edge: payload.bet.edge,
    confidence: payload.conf,
    unitsSuggested: payload.units,
    mode: payload.mode
  });

  var msg =
    "📈 **" + payload.mode + " MODEL SIGNAL — " + payload.bet.tier + "**\n" +
    "**" + payload.awayTeam + " @ " + payload.homeTeam + "**\n" +
    "🕒 " + payload.commenceLocal + "\n\n" +
    "🎯 **Bet:** " + payload.bet.side + " (" + pickTeam + ") @ **" + price + "**\n" +
    "💰 **Units:** " + Number(payload.units).toFixed(2) + "\n\n" +
    "📊 **Model:** " + (modelP * 100).toFixed(1) + "% | **Implied:** " + (implied * 100).toFixed(1) + "%" +
    (isFinite(noVig) ? " | **No-vig:** " + (noVig * 100).toFixed(1) + "%" : "") + "\n" +
    "📈 **Edge:** " + (payload.bet.edge * 100).toFixed(2) + "% | **Confidence:** " + Math.round(payload.conf) + "/100\n" +
    "📚 Coverage: " + payload.coverageAway + " vs " + payload.coverageHome + " | ⚾ Pitchers: " + payload.pitchers + "\n" +
    "🆔 BetId: `" + betId + "` | OddsId: `" + oddsId + "`";

  var isUpdate = isSameDaySignal && !!prevState.lastBetId;
  if (isUpdate) {
    msg =
      "🔁 SIGNAL UPDATE\n" +
      "Prior BetId: `" + prevState.lastBetId + "` → New BetId: `" + betId + "`\n" +
      "Reason: `" + reason + "`\n" +
      "Tier: **" + (prevState.lastTier || "?") + "** → **" + String(payload.bet.tier || "") + "**\n" +
      "Odds: **" + (isFinite(prevState.lastPrice) ? prevState.lastPrice : "?") + "** → **" + (isFinite(price) ? price : "?") + "**\n" +
      "Edge: **" + (isFinite(prevState.lastEdge) ? (prevState.lastEdge * 100).toFixed(2) + "%" : "?") + "** → **" + (isFinite(edge) ? (edge * 100).toFixed(2) + "%" : "?") + "**\n\n" +
      msg;
  }

  var payloadObj = { content: msg };
  var includesComponents = false;
  var deliveryMode = discordDeliveryMode_(cfg, { allowWebhook: true });
  if (deliveryMode.mode === "missing") {
    appendBetEvent_(betId, "DISCORD_FAILED", "PENDING", "PENDING", {
      http: 0,
      body: "missing Discord delivery config",
      includesComponents: includesComponents,
      deliveryMode: deliveryMode.mode,
      update_reason: reason || ""
    });
    log_("WARN", "Discord notify skipped: missing delivery config", { includesComponents: includesComponents, deliveryMode: deliveryMode.mode });
    return false;
  }

  var res = sendDiscordByMode_(deliveryMode, payloadObj);
  if (res.http >= 200 && res.http < 300) {
    var messageId = discordMessageIdFromBody_(res.body);
    props.setProperty(key, JSON.stringify({
      sig: sig,
      dateKey: dateKey,
      lastSentAt: isoLocalWithOffset_(new Date()),
      lastSentMs: nowMs,
      lastPrice: isFinite(price) ? round_(price, 4) : "",
      lastEdge: isFinite(edge) ? round_(edge, 6) : "",
      lastTier: String(payload.bet.tier || ""),
      lastBetId: String(betId || ""),
      lastDiscordMessageId: String(messageId || "")
    }));
    appendBetEvent_(betId, isUpdate ? "DISCORD_UPDATE_SENT" : "DISCORD_SENT", "PENDING", "PENDING", {
      http: res.http,
      body: String(res.body || "").slice(0, 200),
      includesComponents: includesComponents,
      deliveryMode: res.deliveryMode,
      discord_message_id: String(messageId || ""),
      update_reason: reason || ""
    });
    return true;
  }

  appendBetEvent_(betId, "DISCORD_FAILED", "PENDING", "PENDING", {
    http: res.http,
    body: String(res.body || "").slice(0, 200),
    includesComponents: includesComponents,
    deliveryMode: res.deliveryMode,
    update_reason: reason || ""
  });
  log_("WARN", "Discord notify failed", {
    http: res.http,
    body: String(res.body || "").slice(0, 250),
    includesComponents: includesComponents,
    deliveryMode: res.deliveryMode
  });
  return false;
}

function parseNotifyState_(raw) {
  var out = {
    sig: "",
    dateKey: "",
    lastSentMs: NaN,
    lastPrice: NaN,
    lastEdge: NaN,
    lastTier: "",
    lastBetId: "",
    lastDiscordMessageId: ""
  };
  var s = String(raw || "").trim();
  if (!s) return out;

  if (s.charAt(0) !== "{") {
    out.sig = s;
    return out;
  }

  try {
    var obj = JSON.parse(s);
    out.sig = String(obj.sig || "");
    out.dateKey = String(obj.dateKey || "");
    out.lastSentMs = toFloat_(obj.lastSentMs, NaN);
    if (!isFinite(out.lastSentMs)) {
      var parsed = Date.parse(String(obj.lastSentAt || ""));
      out.lastSentMs = isFinite(parsed) ? parsed : NaN;
    }
    out.lastPrice = toFloat_(obj.lastPrice, NaN);
    out.lastEdge = toFloat_(obj.lastEdge, NaN);
    out.lastTier = String(obj.lastTier || "");
    out.lastBetId = String(obj.lastBetId || "");
    out.lastDiscordMessageId = String(obj.lastDiscordMessageId || "");
  } catch (e) {
    out.sig = s;
  }
  return out;
}

function discordMessageIdFromBody_(bodyText) {
  var parsed = null;
  try { parsed = JSON.parse(String(bodyText || "")); } catch (e) { parsed = null; }
  if (parsed && parsed.id !== undefined && parsed.id !== null) return String(parsed.id);
  return "";
}

function getExposureState_(shNotify, dateKey) {
  if (!shNotify) return { plays: 0, units: 0 };
  var v = shNotify.getDataRange().getValues();
  if (!v || v.length < 2) return { plays: 0, units: 0 };

  var header = mapToString_(v[0]);
  var iDate = header.indexOf("date_key");
  var iPlays = header.indexOf("plays");
  var iUnits = header.indexOf("units");

  for (var i = 1; i < v.length; i++) {
    if (String(v[i][iDate] || "") === dateKey) {
      return { plays: toInt_(v[i][iPlays], 0), units: toFloat_(v[i][iUnits], 0) };
    }
  }
  return { plays: 0, units: 0 };
}

function saveExposureState_(shNotify, dateKey, exposure) {
  if (!shNotify) return;
  var v = shNotify.getDataRange().getValues();
  var header = mapToString_(v[0]);
  var iDate = header.indexOf("date_key");
  var iPlays = header.indexOf("plays");
  var iUnits = header.indexOf("units");
  var iUpd = header.indexOf("last_updated_local");

  for (var i = 1; i < v.length; i++) {
    if (String(v[i][iDate] || "") === dateKey) {
      shNotify.getRange(i + 1, iPlays + 1).setValue(exposure.plays);
      shNotify.getRange(i + 1, iUnits + 1).setValue(exposure.units);
      if (iUpd >= 0) shNotify.getRange(i + 1, iUpd + 1).setValue(isoLocalWithOffset_(new Date()));
      return;
    }
  }
  shNotify.appendRow([dateKey, exposure.plays, exposure.units, isoLocalWithOffset_(new Date())]);
}
