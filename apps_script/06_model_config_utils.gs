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
  var lineupFallbackMode = String(cfg.LINEUP_FALLBACK_MODE || "STRICT").toUpperCase();
  var allowLineupFallback = (lineupFallbackMode === "FALLBACK");
  var lineupPaWeights = getLineupPaWeights_(cfg, lineupMin);

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
  var bullpenCtx = buildBullpenUsageContext_(cfg, mlbRes);

  var fallbackMatchRes = null;
  var skipFallbackRematch = !!(mlbRes && toInt_(mlbRes.matchedCount, -1) === 0 && mlbRes.rejectionSummary);
  var matched = [];
  if (mlbRes && mlbRes.matched && mlbRes.matched.length) {
    matched = mlbRes.matched;
  } else if (skipFallbackRematch) {
    matched = [];
    log_("INFO", "Model stage skipped fallback odds/schedule rematch", {
      reason: "schedule_stage_reported_zero_with_rejections",
      rejectionSummary: mlbRes.rejectionSummary || {}
    });
  } else {
    fallbackMatchRes = matchOddsToSchedule_(shOdds, shSched, toInt_(cfg.MATCH_TOL_MIN, 360), { enableTeamFallback: cfg.ODDS_TEAM_MATCH_FALLBACK_ENABLE });
    matched = (fallbackMatchRes.matched || []);
  }

  var externalFeatureCtx = loadExternalFeatureContext_(cfg, matched, oddsById);

  var todayKey = localDateKey_();
  var exposure = getExposureState_(shNotify, todayKey);
  var caps = getCaps_(cfg, mode);
  var th = getThresholds_(cfg, mode);
  var unitsCfg = getUnitsCfg_(cfg, mode);

  var edgeRows = [];
  var computed = 0;
  var betSignalsFound = 0;
  var notificationsDurationMs = 0;
  var calibrationSnapshotWriteDurationMs = 0;
  var skippedNoMatch = 0, skippedLineups = 0, skippedPitchers = 0;
  var lineupFallbackGames = 0;
  var totalLineupSlots = 0, totalMatchedSlots = 0;
  var totalLineupWeight = 0, totalMatchedWeight = 0;
  var weatherAppliedGames = 0, bullpenAppliedGames = 0, experimentalAppliedGames = 0;

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
    var lineupFallbackUsed = false;

    if (!ready) {
      if (!allowLineupFallback) {
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
          bullpenAvailAway: "", bullpenAvailHome: "", bullpenAdjDelta: "",
          confidence: "", bet_side: "", bet_tier: "", bet_edge: "", units: "",
          notes: "WAIT_LINEUPS", updated_at_local: isoLocalWithOffset_(new Date())
        }));
        continue;
      }

      lineupFallbackUsed = true;
      lineupFallbackGames++;
      awayLu = buildFallbackLineup_(awayLu, lineupMin, awayTeam);
      homeLu = buildFallbackLineup_(homeLu, lineupMin, homeTeam);
    }

    var awayOPS = lineupOPS_(awayLu, opsMapObj.map, opsLeagueAvg, lineupPaWeights);
    var homeOPS = lineupOPS_(homeLu, opsMapObj.map, opsLeagueAvg, lineupPaWeights);

    totalLineupSlots += awayOPS.slots + homeOPS.slots;
    totalMatchedSlots += awayOPS.matched + homeOPS.matched;
    totalLineupWeight += awayOPS.totalWeight + homeOPS.totalWeight;
    totalMatchedWeight += awayOPS.matchedWeight + homeOPS.matchedWeight;

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
        bullpenAvailAway: "", bullpenAvailHome: "", bullpenAdjDelta: "",
        confidence: "", bet_side: "", bet_tier: "", bet_edge: "", units: "",
        notes: "WAIT_PITCHERS", updated_at_local: isoLocalWithOffset_(new Date())
      }));
      continue;
    }

    var awayPitFactor = clamp_(0.10, 0.60, 1 / awaySI.siera);
    var homePitFactor = clamp_(0.10, 0.60, 1 / homeSI.siera);
    var awayBp = teamBullpenFactor_(awayTeam, sieraMapObj.map, defaultSIERA, bullpenCtx);
    var homeBp = teamBullpenFactor_(homeTeam, sieraMapObj.map, defaultSIERA, bullpenCtx);
    var bullpenShare = clamp_(0.15, 0.70, toFloat_(cfg.MODEL_BULLPEN_SHARE, 0.42));
    var awayRunPrev = ((1 - bullpenShare) * awayPitFactor) + (bullpenShare * awayBp.factorAdj);
    var homeRunPrev = ((1 - bullpenShare) * homePitFactor) + (bullpenShare * homeBp.factorAdj);
    var bullpenAdjDelta = awayBp.factorAdj - homeBp.factorAdj;

    var extByGame = externalFeatureCtx.byGame[mlbPk] || {};
    var featureAdj = buildFeatureAdjustmentsForGame_(cfg, extByGame);
    if (featureAdj.weatherApplied) weatherAppliedGames++;
    if (featureAdj.bullpenApplied) bullpenAppliedGames++;
    if (featureAdj.experimentalApplied) experimentalAppliedGames++;

    var awayRunPrevAdj = clamp_(0.08, 0.75, awayRunPrev + featureAdj.totalAwayRunPrevDelta + featureAdj.totalRunEnvDelta);
    var homeRunPrevAdj = clamp_(0.08, 0.75, homeRunPrev + featureAdj.totalHomeRunPrevDelta - featureAdj.totalRunEnvDelta);
    var x = kOps * (awayOPS.ops - homeOPS.ops) + kPit * (awayRunPrevAdj - homeRunPrevAdj);
    var pAway = clamp_(0.05, 0.95, 1 / (1 + Math.exp(-x)));
    var pHome = 1 - pAway;

    var awayImp = implied_(o.away_odds_decimal, o.away_implied);
    var homeImp = implied_(o.home_odds_decimal, o.home_implied);
    var vigSum = (isFinite(awayImp) && isFinite(homeImp) && (awayImp + homeImp) > 0) ? (awayImp + homeImp) : NaN;
    var awayNoVig = isFinite(vigSum) ? (awayImp / vigSum) : NaN;
    var homeNoVig = isFinite(vigSum) ? (homeImp / vigSum) : NaN;
    var edgeAway = (isFinite(awayImp) ? (pAway - awayImp) : "");
    var edgeHome = (isFinite(homeImp) ? (pHome - homeImp) : "");
    var conf = confidence_(mode, awayOPS.matched, homeOPS.matched, awaySI.matched, homeSI.matched, lineupFallbackUsed);

    var bet = pickBetSignal_(edgeAway, edgeHome, conf, th);
    var units = "";
    var notes = lineupFallbackUsed ? "LINEUP_FALLBACK" : "";

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

          var notifyStartMs = Date.now();
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

          notificationsDurationMs += Math.max(0, Date.now() - notifyStartMs);

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
      bullpenAvailAway: awayBp.availability,
      bullpenAvailHome: homeBp.availability,
      bullpenAdjDelta: bullpenAdjDelta,
      weatherApplied: featureAdj.weatherApplied ? "Y" : "N",
      bullpenFeatureApplied: featureAdj.bullpenApplied ? "Y" : "N",
      experimentalApplied: featureAdj.experimentalApplied ? "Y" : "N",
      weatherRunEnvDelta: featureAdj.weatherRunEnvDelta,
      bullpenRunPrevDeltaAway: featureAdj.bullpenAwayRunPrevDelta,
      bullpenRunPrevDeltaHome: featureAdj.bullpenHomeRunPrevDelta,
      experimentalRunEnvDelta: featureAdj.marketDelta + featureAdj.statcastDelta,
      featureSet: (featureAdj.weatherApplied || featureAdj.bullpenApplied || featureAdj.experimentalApplied) ? "ENHANCED" : "BASELINE",
      confidence: conf, bet_side: bet.side || "", bet_tier: bet.tier || "", bet_edge: bet.side ? bet.edge : "",
      units: units, notes: notes || bet.notes || "", updated_at_local: isoLocalWithOffset_(new Date())
    }));

    computed++;
  }

  writeRowsByHeader_(shEdge, edgeRows);

  var calibrationSnapshotWriteStartedAtMs = Date.now();
  var snapshotRes = persistCalibrationSnapshots_(cfg, edgeRows, schedByPk, externalFeatureCtx);
  calibrationSnapshotWriteDurationMs = Math.max(0, Date.now() - calibrationSnapshotWriteStartedAtMs);

  log_("INFO", "refreshModelAndEdge completed", {
    opsLeagueAvg: opsLeagueAvg,
    matched: matched.length,
    computed: computed,
    skippedNoMatch: skippedNoMatch,
    skippedLineups: skippedLineups,
    skippedPitchers: skippedPitchers,
    betSignalsFound: betSignalsFound,
    lineupFallbackMode: lineupFallbackMode,
    lineupFallbackUsed: lineupFallbackGames > 0,
    lineupFallbackGames: lineupFallbackGames,
    lineupCoverageUnweighted: totalLineupSlots > 0 ? (totalMatchedSlots / totalLineupSlots) : 0,
    lineupCoverageWeighted: totalLineupWeight > 0 ? (totalMatchedWeight / totalLineupWeight) : 0,
    lineupCoverageSlots: totalMatchedSlots + "/" + totalLineupSlots,
    lineupCoverageWeight: totalMatchedWeight + "/" + totalLineupWeight,
    bullpenWindowDays: bullpenCtx.windowDays,
    bullpenTeamsTracked: bullpenCtx.teamCount,
    weatherAppliedGames: weatherAppliedGames,
    bullpenFeatureAppliedGames: bullpenAppliedGames,
    experimentalAppliedGames: experimentalAppliedGames,
    externalFeatureHealth: externalFeatureCtx.health.bySource,
    calibrationSnapshotsUpserted: snapshotRes.upserted,
    notificationsDurationMs: notificationsDurationMs,
    calibrationSnapshotWriteDurationMs: calibrationSnapshotWriteDurationMs
  });

  return {
    computed: computed,
    betSignalsFound: betSignalsFound,
    lineupFallbackMode: lineupFallbackMode,
    lineupFallbackUsed: lineupFallbackGames > 0,
    lineupFallbackGames: lineupFallbackGames,
    lineupCoverageUnweighted: totalLineupSlots > 0 ? (totalMatchedSlots / totalLineupSlots) : 0,
    lineupCoverageWeighted: totalLineupWeight > 0 ? (totalMatchedWeight / totalLineupWeight) : 0,
    weatherAppliedGames: weatherAppliedGames,
    bullpenFeatureAppliedGames: bullpenAppliedGames,
    experimentalAppliedGames: experimentalAppliedGames,
    externalFeatureHealth: externalFeatureCtx.health.bySource,
    externalFeatureFetchLogs: externalFeatureCtx.diagnostics.fetchLogs,
    calibrationSnapshotsUpserted: snapshotRes.upserted,
    stageTimings: {
      notifications: { durationMs: notificationsDurationMs },
      calibration_snapshot_write: { durationMs: calibrationSnapshotWriteDurationMs }
    }
  };
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

function getLineupPaWeights_(cfg, lineupMin) {
  var raw = cfg.LINEUP_PA_WEIGHTS;
  var defaults = [1.12, 1.08, 1.05, 1.03, 1.00, 0.97, 0.93, 0.91, 0.91];
  var out = [];
  for (var i = 0; i < lineupMin; i++) {
    var w = (raw && raw[i] !== undefined) ? Number(raw[i]) : defaults[Math.min(i, defaults.length - 1)];
    if (!isFinite(w) || w <= 0) w = defaults[Math.min(i, defaults.length - 1)];
    out.push(w);
  }
  return out;
}

function lineupOPS_(lineupArr, opsMap, fallbackOPS, paWeights) {
  var matched = 0, weightedSum = 0, totalWeight = 0, matchedWeight = 0;
  for (var i = 0; i < lineupArr.length; i++) {
    var key = normName_(lineupArr[i].player_name || "");
    var ops = opsMap[key];
    var w = (paWeights && paWeights[i] !== undefined) ? Number(paWeights[i]) : 1;
    if (!isFinite(w) || w <= 0) w = 1;

    totalWeight += w;
    if (ops !== undefined && isFinite(ops)) {
      matched++;
      matchedWeight += w;
      weightedSum += Number(ops) * w;
    } else {
      weightedSum += fallbackOPS * w;
    }
  }

  var opsAvg = (totalWeight > 0) ? (weightedSum / totalWeight) : fallbackOPS;
  return {
    ops: opsAvg,
    matched: matched,
    slots: lineupArr.length,
    totalWeight: totalWeight,
    matchedWeight: matchedWeight
  };
}

function pitcherSIERA_(pitcherName, sieraMap, fallbackSIERA) {
  var nm = String(pitcherName || "").trim();
  if (!nm) return { siera: fallbackSIERA, matched: false };
  var v = sieraMap[normName_(nm)];
  if (v !== undefined && isFinite(v) && v > 0) return { siera: Number(v), matched: true };
  return { siera: fallbackSIERA, matched: false };
}

function buildBullpenUsageContext_(cfg, mlbRes) {
  var windowDays = clamp_(3, 5, toInt_(cfg.BULLPEN_USAGE_DAYS, 4));
  var teams = [];
  var seen = {};
  var matched = (mlbRes && mlbRes.matched && mlbRes.matched.length) ? mlbRes.matched : [];

  for (var i = 0; i < matched.length; i++) {
    var away = String(matched[i].away_team || "").trim();
    var home = String(matched[i].home_team || "").trim();
    if (away && !seen[normalizeTeam_(away)]) { seen[normalizeTeam_(away)] = true; teams.push(away); }
    if (home && !seen[normalizeTeam_(home)]) { seen[normalizeTeam_(home)] = true; teams.push(home); }
  }

  if (!teams.length) {
    return { windowDays: windowDays, byTeam: {}, byPitcherTeam: {}, teamCount: 0 };
  }

  var cache = CacheService.getScriptCache();
  var keySeed = teams.map(function (t) { return normalizeTeam_(t); }).sort().join("|");
  var cacheKey = "BULLPEN_USAGE_V1|" + windowDays + "|" + keySeed.slice(0, 220);
  var cached = cache.get(cacheKey);
  if (cached) {
    try {
      var parsed = JSON.parse(cached);
      parsed.windowDays = windowDays;
      parsed.teamCount = Object.keys(parsed.byTeam || {}).length;
      return parsed;
    } catch (e) { }
  }

  var usage = fetchRecentBullpenUsage_(windowDays, teams);
  usage.windowDays = windowDays;
  usage.teamCount = Object.keys(usage.byTeam || {}).length;
  cache.put(cacheKey, JSON.stringify(usage), 60 * 20);
  return usage;
}

function fetchRecentBullpenUsage_(windowDays, targetTeams) {
  var now = new Date();
  var endDate = Utilities.formatDate(now, "UTC", "yyyy-MM-dd");
  var startMs = now.getTime() - (windowDays * 24 * 3600 * 1000);
  var startDate = Utilities.formatDate(new Date(startMs), "UTC", "yyyy-MM-dd");
  var url =
    "https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=" + encodeURIComponent(startDate) +
    "&endDate=" + encodeURIComponent(endDate);

  var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  if (resp.getResponseCode() !== 200) {
    log_("WARN", "Bullpen usage schedule fetch failed", { http: resp.getResponseCode(), startDate: startDate, endDate: endDate });
    return { byTeam: {}, byPitcherTeam: {} };
  }

  var payload = JSON.parse(resp.getContentText() || "{}");
  var dates = payload.dates || [];
  var games = [];
  for (var d = 0; d < dates.length; d++) {
    var dg = dates[d] && dates[d].games ? dates[d].games : [];
    for (var g = 0; g < dg.length; g++) games.push(dg[g]);
  }

  var target = {};
  for (var t = 0; t < targetTeams.length; t++) target[normalizeTeam_(targetTeams[t])] = true;

  var byTeamPitcher = {};
  for (var i = 0; i < games.length; i++) {
    var game = games[i] || {};
    var awayTeam = getTeamNameSafe_(game, "away");
    var homeTeam = getTeamNameSafe_(game, "home");
    if (!target[normalizeTeam_(awayTeam)] && !target[normalizeTeam_(homeTeam)]) continue;

    var gamePk = String(game.gamePk || "");
    if (!gamePk) continue;
    var gameDate = String(game.gameDate || "");
    var gameTs = Date.parse(gameDate);
    var daysAgo = isFinite(gameTs) ? Math.max(0, Math.floor((now.getTime() - gameTs) / (24 * 3600 * 1000))) : windowDays;

    var boxUrl = "https://statsapi.mlb.com/api/v1/game/" + encodeURIComponent(gamePk) + "/boxscore";
    var boxResp = UrlFetchApp.fetch(boxUrl, { muteHttpExceptions: true });
    if (boxResp.getResponseCode() !== 200) continue;

    var box = JSON.parse(boxResp.getContentText() || "{}");
    accumulateRelieverUsage_(box, "away", daysAgo, byTeamPitcher);
    accumulateRelieverUsage_(box, "home", daysAgo, byTeamPitcher);
  }

  var byTeam = {};
  var byPitcherTeam = {};
  for (var teamKey in byTeamPitcher) {
    var pMap = byTeamPitcher[teamKey] || {};
    var relievers = [];
    var sumAvail = 0;
    var cntAvail = 0;
    for (var pid in pMap) {
      var usage = pMap[pid];
      usage.availability = calcRelieverAvailability_(usage);
      relievers.push(usage);
      sumAvail += usage.availability;
      cntAvail++;
      byPitcherTeam[teamKey + "|" + pid] = usage;
    }
    byTeam[teamKey] = {
      relievers: relievers,
      availability: cntAvail > 0 ? (sumAvail / cntAvail) : 1.0
    };
  }

  return { byTeam: byTeam, byPitcherTeam: byPitcherTeam };
}

function accumulateRelieverUsage_(box, side, daysAgo, byTeamPitcher) {
  var team = box && box.teams && box.teams[side] ? box.teams[side] : null;
  if (!team) return;

  var teamName = String(team.team && team.team.name ? team.team.name : "");
  var teamKey = normalizeTeam_(teamName);
  if (!teamKey) return;
  if (!byTeamPitcher[teamKey]) byTeamPitcher[teamKey] = {};

  var pitchers = team.pitchers || [];
  if (!pitchers.length) return;
  var starterId = String(pitchers[0] || "");
  var players = team.players || {};

  for (var i = 0; i < pitchers.length; i++) {
    var pid = String(pitchers[i] || "");
    if (!pid || pid === starterId) continue;

    var pObj = players["ID" + pid] || {};
    var fullName = String(pObj.person && pObj.person.fullName ? pObj.person.fullName : "");
    var pitches = Number(pObj.stats && pObj.stats.pitching ? pObj.stats.pitching.numberOfPitches : 0);
    if (!isFinite(pitches) || pitches < 0) pitches = 0;

    var key = pid || normName_(fullName);
    if (!byTeamPitcher[teamKey][key]) {
      byTeamPitcher[teamKey][key] = {
        team: teamName,
        pitcherId: pid,
        pitcherName: fullName,
        appearances: 0,
        pitches: 0,
        weightedLoad: 0,
        minDaysAgo: 99
      };
    }

    var u = byTeamPitcher[teamKey][key];
    u.appearances += 1;
    u.pitches += pitches;
    u.minDaysAgo = Math.min(u.minDaysAgo, daysAgo);
    u.weightedLoad += bullpenAppearanceLoad_(daysAgo, pitches);
  }
}

function bullpenAppearanceLoad_(daysAgo, pitches) {
  var restW = (daysAgo <= 0) ? 1.0 : ((daysAgo === 1) ? 0.75 : ((daysAgo === 2) ? 0.45 : 0.25));
  var pitchComponent = clamp_(0.08, 0.55, (Math.min(Math.max(0, pitches), 45) / 90));
  return restW * (0.20 + pitchComponent);
}

function calcRelieverAvailability_(usage) {
  var restBonus = (usage.minDaysAgo >= 2) ? 0.18 : ((usage.minDaysAgo === 1) ? 0.08 : -0.18);
  var appearancePenalty = Math.max(0, usage.appearances - 2) * 0.08;
  var score = 1.0 + restBonus - usage.weightedLoad - appearancePenalty;
  return clamp_(0.05, 1.15, score);
}

function teamBullpenFactor_(teamName, sieraMap, fallbackSIERA, bullpenCtx) {
  var teamKey = normalizeTeam_(teamName);
  var teamUsage = bullpenCtx && bullpenCtx.byTeam ? bullpenCtx.byTeam[teamKey] : null;
  if (!teamUsage || !teamUsage.relievers || !teamUsage.relievers.length) {
    return {
      availability: 1.0,
      factorAdj: clamp_(0.10, 0.60, 1 / fallbackSIERA)
    };
  }

  var relievers = teamUsage.relievers;
  var weighted = 0;
  var wsum = 0;
  for (var i = 0; i < relievers.length; i++) {
    var rel = relievers[i];
    var siera = sieraMap[normName_(rel.pitcherName || "")];
    if (!isFinite(siera) || siera <= 0) siera = fallbackSIERA;
    var rpFactor = clamp_(0.10, 0.60, 1 / siera);
    var w = clamp_(0.05, 1.25, Number(rel.availability || 1));
    weighted += rpFactor * w;
    wsum += w;
  }

  return {
    availability: teamUsage.availability,
    factorAdj: (wsum > 0) ? (weighted / wsum) : clamp_(0.10, 0.60, 1 / fallbackSIERA)
  };
}

function implied_(decOdds, fallbackImp) {
  var o = Number(decOdds);
  if (isFinite(o) && o > 1.0001) return 1 / o;
  var f = Number(fallbackImp);
  return isFinite(f) ? f : NaN;
}

function clamp_(lo, hi, x) { return Math.max(lo, Math.min(hi, x)); }

function confidence_(mode, awayHitMatched, homeHitMatched, awayPitMatched, homePitMatched, lineupFallbackUsed) {
  var c = 50;
  c += (awayHitMatched / 9) * 20;
  c += (homeHitMatched / 9) * 20;
  c += (awayPitMatched ? 5 : -5);
  c += (homePitMatched ? 5 : -5);

  var miss = (9 - awayHitMatched) + (9 - homeHitMatched);
  var missPenalty = (String(mode).toUpperCase() === "PRESEASON") ? 2.0 : 3.5;
  c -= miss * missPenalty;
  if (lineupFallbackUsed) c -= 8;
  return clamp_(0, 100, c);
}

function buildFallbackLineup_(lineupArr, lineupMin, teamName) {
  var out = lineupArr ? lineupArr.slice(0, lineupMin) : [];
  var next = out.length + 1;
  while (out.length < lineupMin) {
    out.push({
      bat_order: next,
      player_name: String(teamName || "Team") + " Default Batter " + next
    });
    next++;
  }
  return out;
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

function buildBetSizingPlan_(units, decimalOdds, cfg) {
  var unitMxn = Math.max(0, toFloat_(cfg.BANKROLL_UNIT_MXN, 100));
  var mode = String(cfg.BET_SIZING_MODE || "RISK").toUpperCase();
  if (mode !== "RISK" && mode !== "TO_WIN") mode = "RISK";

  var minMxn = Math.max(0, toFloat_(cfg.BET_MIN_MXN, 20));
  var minAppliesTo = String(cfg.BET_MIN_APPLIES_TO || "STAKE_OR_TO_WIN").toUpperCase();
  if (minAppliesTo === "EITHER") minAppliesTo = "STAKE_OR_TO_WIN";
  if (minAppliesTo !== "STAKE" && minAppliesTo !== "TO_WIN" && minAppliesTo !== "STAKE_OR_TO_WIN") minAppliesTo = "STAKE_OR_TO_WIN";

  var roundTo = Math.max(1, Math.floor(toFloat_(cfg.BET_ROUND_TO_MXN, 1)));
  var stakeToWinMult = Number(decimalOdds) - 1;
  var validOdds = isFinite(stakeToWinMult) && stakeToWinMult > 0;

  var modelRisk = Math.max(0, Number(units) * unitMxn);
  var modelToWin = validOdds ? (modelRisk * stakeToWinMult) : 0;
  var placedRisk = modelRisk;
  var placedToWin = modelToWin;
  var noteParts = [];
  var minApplied = false;

  if (mode === "TO_WIN") {
    placedToWin = Math.max(0, Number(units) * unitMxn);
    if (validOdds) {
      placedRisk = placedToWin / stakeToWinMult;
    } else {
      placedRisk = placedToWin;
      noteParts.push("invalid_odds_to_win_fallback_risk_equals_to_win");
    }
  } else {
    placedRisk = modelRisk;
    if (validOdds) {
      placedToWin = placedRisk * stakeToWinMult;
    } else {
      placedToWin = 0;
      noteParts.push("invalid_odds_risk_fallback_zero_to_win");
    }
  }

  var needsStakeMin = (minAppliesTo === "STAKE" || minAppliesTo === "STAKE_OR_TO_WIN");
  var needsToWinMin = (minAppliesTo === "TO_WIN" || minAppliesTo === "STAKE_OR_TO_WIN");
  var appliedOnStake = false;
  var appliedOnToWin = false;

  if (needsStakeMin && placedRisk < minMxn) {
    placedRisk = minMxn;
    minApplied = true;
    appliedOnStake = true;
  }
  if (needsToWinMin && placedToWin < minMxn) {
    placedToWin = minMxn;
    minApplied = true;
    appliedOnToWin = true;
  }

  if (validOdds) {
    if (appliedOnStake && !appliedOnToWin) placedToWin = placedRisk * stakeToWinMult;
    if (appliedOnToWin && !appliedOnStake) placedRisk = placedToWin / stakeToWinMult;
    if (appliedOnStake && appliedOnToWin) {
      var riskFromToWinMin = placedToWin / stakeToWinMult;
      if (riskFromToWinMin > placedRisk) placedRisk = riskFromToWinMin;
      placedToWin = placedRisk * stakeToWinMult;
    }
  }

  placedRisk = roundBetAmountToIncrement_(placedRisk, roundTo);
  placedToWin = roundBetAmountToIncrement_(placedToWin, roundTo);

  if (needsStakeMin && placedRisk < minMxn) {
    placedRisk = roundBetAmountUpToMin_(minMxn, roundTo);
    if (validOdds) placedToWin = roundBetAmountToIncrement_(placedRisk * stakeToWinMult, roundTo);
  }
  if (needsToWinMin && placedToWin < minMxn) {
    placedToWin = roundBetAmountUpToMin_(minMxn, roundTo);
    if (validOdds) placedRisk = roundBetAmountToIncrement_(placedToWin / stakeToWinMult, roundTo);
  }

  if (minApplied) {
    if (appliedOnStake && !appliedOnToWin) noteParts.push("min_bet_applied_risk_upscaled");
    else if (appliedOnToWin && !appliedOnStake) noteParts.push("min_bet_applied_to_win_upscaled");
    else noteParts.push("min_bet_applied_stake_or_to_win_upscaled");
  }
  if (roundTo > 1) noteParts.push("rounded_to_increment_" + roundTo);

  var effectiveUnits = (unitMxn > 0) ? (placedRisk / unitMxn) : 0;
  return {
    unit_mxn: round_(unitMxn, 2),
    model_risk_mxn: round_(modelRisk, 2),
    model_to_win_mxn: round_(modelToWin, 2),
    placed_risk_mxn: round_(placedRisk, 2),
    placed_to_win_mxn: round_(placedToWin, 2),
    min_applied: !!minApplied,
    sizing_mode: mode,
    min_applies_to: minAppliesTo,
    effective_units: round_(effectiveUnits, 4),
    sizing_note: noteParts.join("|")
  };
}

function roundBetAmountToIncrement_(amountMxn, roundToMxn) {
  var amt = Math.max(0, Number(amountMxn) || 0);
  var inc = Math.max(1, Math.floor(Number(roundToMxn) || 1));
  return Math.round(amt / inc) * inc;
}

function roundBetAmountUpToMin_(minMxn, roundToMxn) {
  var minAmt = Math.max(0, Number(minMxn) || 0);
  var inc = Math.max(1, Math.floor(Number(roundToMxn) || 1));
  return Math.ceil(minAmt / inc) * inc;
}



function normalizeOddsHistorySnapshot_(input, rowIndex) {
  var snap = input || {};
  var capturedAtLocal = String(snap.captured_at_local || "");
  var capturedAtUtc = String(snap.captured_at_utc || "");
  var capturedAtMs = Date.parse(capturedAtUtc || capturedAtLocal);
  var commenceUtc = String(snap.commence_time_utc || "");
  return {
    away_price: toFloat_(snap.away_price !== undefined ? snap.away_price : snap.away_odds_decimal, NaN),
    home_price: toFloat_(snap.home_price !== undefined ? snap.home_price : snap.home_odds_decimal, NaN),
    away_implied: toFloat_(snap.away_implied, NaN),
    home_implied: toFloat_(snap.home_implied, NaN),
    commence_time_utc: commenceUtc,
    captured_at_local: capturedAtLocal,
    captured_at_utc: capturedAtUtc,
    captured_at_ms: isFinite(capturedAtMs) ? capturedAtMs : NaN,
    row_index: toInt_(rowIndex, 0)
  };
}

function compareOddsSnapshots_(a, b) {
  var aMs = isFinite(a.captured_at_ms) ? a.captured_at_ms : -Infinity;
  var bMs = isFinite(b.captured_at_ms) ? b.captured_at_ms : -Infinity;
  if (aMs !== bMs) return aMs - bMs;
  return Number(a.row_index || 0) - Number(b.row_index || 0);
}

var ODDS_HISTORY_INDEX_PROP_MAX_BYTES_ = 8500;
var ODDS_HISTORY_INDEX_PROP_SHARD_BYTES_ = 8000;
var ODDS_HISTORY_INDEX_PROP_MAX_SHARDS_ = 55;
var ODDS_HISTORY_INDEX_CACHE_TTL_SEC_ = 21600;

function cleanupOddsHistoryIndexShards_(props, keepShardCount) {
  var start = Math.max(0, toInt_(keepShardCount, 0));
  for (var i = start; i < ODDS_HISTORY_INDEX_PROP_MAX_SHARDS_; i++) {
    props.deleteProperty(PROP.ODDS_HISTORY_INDEX_SHARD_PREFIX + i);
  }
}

function getOddsHistoryIndexCache_() {
  var cache = CacheService.getScriptCache();
  var props = PropertiesService.getScriptProperties();
  var cacheKey = PROP.ODDS_HISTORY_INDEX_CACHE;

  try {
    var cacheRaw = cache.get(cacheKey) || "";
    if (cacheRaw) {
      var cacheParsed = JSON.parse(cacheRaw);
      if (cacheParsed && typeof cacheParsed === "object" && cacheParsed.by_id) {
        return {
          ok: true,
          reason: "hit_cache_service",
          index: cacheParsed,
          cacheBackend: "cache_service",
          cacheSizeBytes: cacheRaw.length
        };
      }
    }
  } catch (cacheErr) {
    log_("WARN", "Odds history index cache read failed", {
      reasonCode: "cache_service_read_failed",
      cacheBackend: "cache_service",
      error: String(cacheErr)
    });
  }

  var metaRaw = props.getProperty(PROP.ODDS_HISTORY_INDEX_META) || "";
  if (!metaRaw) {
    var legacyRaw = props.getProperty(PROP.ODDS_HISTORY_INDEX) || "";
    if (!legacyRaw) return { ok: false, reason: "missing", index: null, cacheBackend: "none", cacheSizeBytes: 0 };
    try {
      var legacyParsed = JSON.parse(legacyRaw);
      if (!legacyParsed || typeof legacyParsed !== "object" || !legacyParsed.by_id) return { ok: false, reason: "invalid_shape", index: null, cacheBackend: "legacy_script_property", cacheSizeBytes: legacyRaw.length };
      return { ok: true, reason: "hit_legacy_script_property", index: legacyParsed, cacheBackend: "legacy_script_property", cacheSizeBytes: legacyRaw.length };
    } catch (legacyErr) {
      return { ok: false, reason: "parse_error", index: null, error: String(legacyErr), cacheBackend: "legacy_script_property", cacheSizeBytes: legacyRaw.length };
    }
  }

  try {
    var meta = JSON.parse(metaRaw);
    var shardCount = Math.max(0, toInt_(meta.shardCount, 0));
    if (!shardCount) return { ok: false, reason: "meta_missing_shards", index: null, cacheBackend: "script_properties", cacheSizeBytes: 0 };

    var shards = [];
    var totalSize = 0;
    for (var i = 0; i < shardCount; i++) {
      var part = props.getProperty(PROP.ODDS_HISTORY_INDEX_SHARD_PREFIX + i) || "";
      if (!part) return { ok: false, reason: "shard_missing_" + i, index: null, cacheBackend: "script_properties", cacheSizeBytes: totalSize };
      shards.push(part);
      totalSize += part.length;
    }

    var raw = shards.join("");
    var parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !parsed.by_id) return { ok: false, reason: "invalid_shape", index: null, cacheBackend: "script_properties", cacheSizeBytes: raw.length };

    try {
      cache.put(cacheKey, raw, ODDS_HISTORY_INDEX_CACHE_TTL_SEC_);
    } catch (cacheWriteErr) {
      log_("WARN", "Odds history index cache write failed", {
        reasonCode: "cache_service_backfill_failed",
        cacheBackend: "cache_service",
        error: String(cacheWriteErr)
      });
    }

    return {
      ok: true,
      reason: "hit_script_properties",
      index: parsed,
      cacheBackend: "script_properties",
      cacheSizeBytes: raw.length
    };
  } catch (e) {
    return { ok: false, reason: "parse_error", index: null, error: String(e), cacheBackend: "script_properties", cacheSizeBytes: 0 };
  }
}

function persistOddsHistoryIndexCache_(indexObj) {
  if (!indexObj) return { ok: false, reasonCode: "empty_index", cacheBackend: "none", cacheSizeBytes: 0 };

  var payload = JSON.stringify(indexObj);
  var payloadSize = payload.length;
  var cache = CacheService.getScriptCache();
  var props = PropertiesService.getScriptProperties();
  var cacheBackend = "cache_service+script_properties";

  if (payloadSize > (ODDS_HISTORY_INDEX_PROP_SHARD_BYTES_ * ODDS_HISTORY_INDEX_PROP_MAX_SHARDS_)) {
    log_("WARN", "Odds history index cache persist skipped", {
      reasonCode: "payload_exceeds_max_supported",
      cacheBackend: "none",
      cacheSizeBytes: payloadSize
    });
    return { ok: false, reasonCode: "payload_exceeds_max_supported", cacheBackend: "none", cacheSizeBytes: payloadSize };
  }

  try {
    cache.put(PROP.ODDS_HISTORY_INDEX_CACHE, payload, ODDS_HISTORY_INDEX_CACHE_TTL_SEC_);
  } catch (cacheErr) {
    cacheBackend = "script_properties";
    log_("WARN", "Odds history index cache write failed", {
      reasonCode: "cache_service_write_failed",
      cacheBackend: "cache_service",
      cacheSizeBytes: payloadSize,
      error: String(cacheErr)
    });
  }

  if (payloadSize > ODDS_HISTORY_INDEX_PROP_MAX_BYTES_) {
    var shardCount = Math.ceil(payloadSize / ODDS_HISTORY_INDEX_PROP_SHARD_BYTES_);
    if (shardCount > ODDS_HISTORY_INDEX_PROP_MAX_SHARDS_) {
      log_("WARN", "Odds history index cache persist skipped", {
        reasonCode: "shard_limit_exceeded",
        cacheBackend: "none",
        cacheSizeBytes: payloadSize,
        shardCount: shardCount
      });
      return { ok: false, reasonCode: "shard_limit_exceeded", cacheBackend: "none", cacheSizeBytes: payloadSize };
    }

    try {
      props.setProperty(PROP.ODDS_HISTORY_INDEX_META, JSON.stringify({
        version: 1,
        shardCount: shardCount,
        cacheSizeBytes: payloadSize,
        updatedAtMs: Date.now()
      }));
      for (var i = 0; i < shardCount; i++) {
        props.setProperty(
          PROP.ODDS_HISTORY_INDEX_SHARD_PREFIX + i,
          payload.substring(i * ODDS_HISTORY_INDEX_PROP_SHARD_BYTES_, (i + 1) * ODDS_HISTORY_INDEX_PROP_SHARD_BYTES_)
        );
      }
      cleanupOddsHistoryIndexShards_(props, shardCount);
      props.deleteProperty(PROP.ODDS_HISTORY_INDEX);
      return { ok: true, reasonCode: "persisted_sharded", cacheBackend: cacheBackend, cacheSizeBytes: payloadSize, shardCount: shardCount };
    } catch (propErr) {
      log_("WARN", "Odds history index cache write failed", {
        reasonCode: "script_properties_sharded_write_failed",
        cacheBackend: "script_properties",
        cacheSizeBytes: payloadSize,
        error: String(propErr)
      });
      return { ok: false, reasonCode: "script_properties_sharded_write_failed", cacheBackend: cacheBackend, cacheSizeBytes: payloadSize };
    }
  }

  try {
    props.setProperty(PROP.ODDS_HISTORY_INDEX, payload);
    props.setProperty(PROP.ODDS_HISTORY_INDEX_META, JSON.stringify({
      version: 1,
      shardCount: 1,
      cacheSizeBytes: payloadSize,
      storage: "single_property",
      updatedAtMs: Date.now()
    }));
    props.setProperty(PROP.ODDS_HISTORY_INDEX_SHARD_PREFIX + 0, payload);
    cleanupOddsHistoryIndexShards_(props, 1);
    return { ok: true, reasonCode: "persisted_single_property", cacheBackend: cacheBackend, cacheSizeBytes: payloadSize, shardCount: 1 };
  } catch (singleErr) {
    log_("WARN", "Odds history index cache write failed", {
      reasonCode: "script_properties_single_write_failed",
      cacheBackend: "script_properties",
      cacheSizeBytes: payloadSize,
      error: String(singleErr)
    });
    return { ok: false, reasonCode: "script_properties_single_write_failed", cacheBackend: cacheBackend, cacheSizeBytes: payloadSize };
  }
}

function buildOddsHistoryIndexFromRows_(rows, preStartMin) {
  var targetPreStartMin = Math.max(0, toInt_(preStartMin, 15));
  var cutoffOffsetMs = targetPreStartMin * 60000;
  var out = {
    version: 1,
    pre_start_min: targetPreStartMin,
    built_at_ms: Date.now(),
    by_id: {}
  };

  for (var i = 0; i < rows.length; i++) {
    var r = rows[i] || {};
    var oddsId = String(r.odds_game_id || "").trim();
    if (!oddsId) continue;

    var snap = normalizeOddsHistorySnapshot_(r, i);
    if (!out.by_id[oddsId]) out.by_id[oddsId] = { open: null, latest: null, close_cutoff: null, close_reason: "", commence_time_utc: "" };
    var entry = out.by_id[oddsId];

    if (!entry.open) entry.open = snap;
    if (!entry.latest || compareOddsSnapshots_(snap, entry.latest) >= 0) entry.latest = snap;
    if (snap.commence_time_utc) entry.commence_time_utc = snap.commence_time_utc;

    var commenceMs = Date.parse(String(snap.commence_time_utc || entry.commence_time_utc || ""));
    var cutoffMs = isFinite(commenceMs) ? (commenceMs - cutoffOffsetMs) : NaN;
    if (!isFinite(cutoffMs)) {
      if (!entry.close_cutoff || compareOddsSnapshots_(snap, entry.close_cutoff) >= 0) entry.close_cutoff = snap;
    } else if (isFinite(snap.captured_at_ms) && snap.captured_at_ms <= cutoffMs) {
      if (!entry.close_cutoff || compareOddsSnapshots_(snap, entry.close_cutoff) >= 0) entry.close_cutoff = snap;
    }
  }

  var ids = Object.keys(out.by_id);
  for (var j = 0; j < ids.length; j++) {
    var id = ids[j];
    var e = out.by_id[id];
    e.close_reason = e.close_cutoff ? "" : "close_no_snapshot_before_cutoff";
  }

  return out;
}

function rebuildOddsHistoryIndexCache_(preStartMin, reason) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.ODDS_HISTORY);
  if (!sh) return { version: 1, pre_start_min: Math.max(0, toInt_(preStartMin, 15)), built_at_ms: Date.now(), by_id: {} };
  var rows = readSheetAsObjects_(sh);
  var indexObj = buildOddsHistoryIndexFromRows_(rows, preStartMin);
  var persistRes = persistOddsHistoryIndexCache_(indexObj);
  log_("INFO", "Odds history index rebuilt", {
    reasonCode: String(reason || "rebuild_requested"),
    gamesIndexed: Object.keys(indexObj.by_id || {}).length,
    rowsScanned: rows.length,
    preStartMin: indexObj.pre_start_min,
    cacheBackend: String((persistRes && persistRes.cacheBackend) || "none"),
    cacheSizeBytes: toInt_(persistRes && persistRes.cacheSizeBytes, 0),
    cachePersistReasonCode: String((persistRes && persistRes.reasonCode) || "none")
  });
  return indexObj;
}

function getOddsHistoryIndexOrRebuild_(preStartMin) {
  var targetPreStartMin = Math.max(0, toInt_(preStartMin, 15));
  var cacheRes = getOddsHistoryIndexCache_();
  if (!cacheRes.ok) {
    log_("INFO", "Odds history index cache miss", {
      reasonCode: cacheRes.reason || "missing",
      cacheBackend: String(cacheRes.cacheBackend || "none"),
      cacheSizeBytes: toInt_(cacheRes.cacheSizeBytes, 0)
    });
    return rebuildOddsHistoryIndexCache_(targetPreStartMin, "cache_" + String(cacheRes.reason || "miss"));
  }

  var indexObj = cacheRes.index;
  if (toInt_(indexObj.pre_start_min, targetPreStartMin) !== targetPreStartMin) {
    log_("INFO", "Odds history index cache miss", {
      reasonCode: "prestart_mismatch",
      cachedPreStartMin: toInt_(indexObj.pre_start_min, targetPreStartMin),
      requestedPreStartMin: targetPreStartMin
    });
    return rebuildOddsHistoryIndexCache_(targetPreStartMin, "prestart_mismatch");
  }

  log_("INFO", "Odds history index cache hit", {
    gamesIndexed: Object.keys(indexObj.by_id || {}).length,
    preStartMin: targetPreStartMin,
    cacheBackend: String(cacheRes.cacheBackend || "unknown"),
    cacheSizeBytes: toInt_(cacheRes.cacheSizeBytes, 0)
  });
  return indexObj;
}

function updateOddsHistoryIndexIncremental_(historyRows, preStartMin) {
  if (!historyRows || !historyRows.length) return;
  var targetPreStartMin = Math.max(0, toInt_(preStartMin, 15));
  var cacheRes = getOddsHistoryIndexCache_();
  var indexObj = null;

  if (!cacheRes.ok) {
    log_("INFO", "Odds history index incremental update cache miss", {
      reasonCode: cacheRes.reason || "missing",
      cacheBackend: String(cacheRes.cacheBackend || "none"),
      cacheSizeBytes: toInt_(cacheRes.cacheSizeBytes, 0)
    });
    indexObj = rebuildOddsHistoryIndexCache_(targetPreStartMin, "incremental_cache_miss");
    return indexObj;
  }

  if (toInt_(cacheRes.index.pre_start_min, targetPreStartMin) !== targetPreStartMin) {
    log_("INFO", "Odds history index incremental update prestart mismatch", {
      cachedPreStartMin: toInt_(cacheRes.index.pre_start_min, targetPreStartMin),
      requestedPreStartMin: targetPreStartMin
    });
    indexObj = rebuildOddsHistoryIndexCache_(targetPreStartMin, "incremental_prestart_mismatch");
    return indexObj;
  }

  indexObj = cacheRes.index;

  for (var i = 0; i < historyRows.length; i++) {
    var hr = historyRows[i] || [];
    var oddsId = String(hr[1] || "").trim();
    if (!oddsId) continue;

    var snap = normalizeOddsHistorySnapshot_({
      captured_at_local: String(hr[0] || ""),
      odds_game_id: oddsId,
      commence_time_utc: String(hr[2] || ""),
      away_odds_decimal: hr[5],
      home_odds_decimal: hr[6],
      away_implied: hr[7],
      home_implied: hr[8]
    }, Date.now() + i);

    if (!indexObj.by_id[oddsId]) indexObj.by_id[oddsId] = { open: null, latest: null, close_cutoff: null, close_reason: "", commence_time_utc: "" };
    var entry = indexObj.by_id[oddsId];
    if (!entry.open) entry.open = snap;
    if (!entry.latest || compareOddsSnapshots_(snap, entry.latest) >= 0) entry.latest = snap;
    if (snap.commence_time_utc) entry.commence_time_utc = snap.commence_time_utc;

    var commenceMs = Date.parse(String(snap.commence_time_utc || entry.commence_time_utc || ""));
    var cutoffMs = isFinite(commenceMs) ? (commenceMs - (targetPreStartMin * 60000)) : NaN;
    if (!isFinite(cutoffMs)) {
      if (!entry.close_cutoff || compareOddsSnapshots_(snap, entry.close_cutoff) >= 0) entry.close_cutoff = snap;
    } else if (isFinite(snap.captured_at_ms) && snap.captured_at_ms <= cutoffMs) {
      if (!entry.close_cutoff || compareOddsSnapshots_(snap, entry.close_cutoff) >= 0) entry.close_cutoff = snap;
    }
    entry.close_reason = entry.close_cutoff ? "" : "close_no_snapshot_before_cutoff";
  }

  indexObj.pre_start_min = targetPreStartMin;
  indexObj.built_at_ms = Date.now();
  var persistRes = persistOddsHistoryIndexCache_(indexObj);
  log_("INFO", "Odds history index incremental update completed", {
    appendedRows: historyRows.length,
    gamesIndexed: Object.keys(indexObj.by_id || {}).length,
    preStartMin: targetPreStartMin,
    cacheBackend: String((persistRes && persistRes.cacheBackend) || "none"),
    cacheSizeBytes: toInt_(persistRes && persistRes.cacheSizeBytes, 0),
    cachePersistReasonCode: String((persistRes && persistRes.reasonCode) || "none")
  });
  return indexObj;
}

function getSignalLogOpenSnapshotByOddsId_() {
  var cfg = getConfig_();
  var preStartMin = Math.max(0, toInt_(cfg.SIGNAL_CLOSE_PRESTART_MIN, 15));
  var indexObj = getOddsHistoryIndexOrRebuild_(preStartMin);
  var out = {};
  var byId = indexObj.by_id || {};
  var ids = Object.keys(byId);
  for (var i = 0; i < ids.length; i++) {
    var oddsId = ids[i];
    if (byId[oddsId] && byId[oddsId].open) out[oddsId] = byId[oddsId].open;
  }
  return out;
}

function getPickSideOdds_(snapshot, pickSide) {
  var side = String(pickSide || "").toUpperCase();
  if (side !== "AWAY" && side !== "HOME") {
    return { price: NaN, implied: NaN, reason: "missing_pick_side" };
  }
  if (!snapshot) return { price: NaN, implied: NaN, reason: "open_snapshot_missing" };

  var price = side === "AWAY" ? Number(snapshot.away_price) : Number(snapshot.home_price);
  var implied = side === "AWAY" ? Number(snapshot.away_implied) : Number(snapshot.home_implied);
  if (!isFinite(price) || !isFinite(implied)) return { price: NaN, implied: NaN, reason: "open_pick_price_missing" };

  return { price: price, implied: implied, reason: "" };
}

function buildSignalOpenMetrics_(oddsId, pickSide, signalPrice, signalImplied, openByOddsId) {
  var source = openByOddsId || getSignalLogOpenSnapshotByOddsId_();
  var entry = getPickSideOdds_(source[String(oddsId || "")], pickSide);
  if (!isFinite(entry.price) || !isFinite(entry.implied)) {
    return {
      open_price_pick: "",
      open_implied_pick: "",
      delta_open_to_signal_price: "",
      delta_open_to_signal_implied: "",
      open_reason_code: String(entry.reason || "open_pick_price_missing")
    };
  }

  var signalP = Number(signalPrice);
  var signalI = Number(signalImplied);
  return {
    open_price_pick: round_(entry.price, 4),
    open_implied_pick: round_(entry.implied, 6),
    delta_open_to_signal_price: isFinite(signalP) ? round_(signalP - entry.price, 4) : "",
    delta_open_to_signal_implied: isFinite(signalI) ? round_(signalI - entry.implied, 6) : "",
    open_reason_code: ""
  };
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
    if (tierChanged) reason = "tier_change";
    else if (hasPriorMetrics && edgeMovePct >= minEdgeMovePct) reason = "edge_delta";
    else reason = "cooldown_expired";
  }

  var signalId = Utilities.getUuid();
  var openMetrics = buildSignalOpenMetrics_(oddsId, payload.bet.side, price, implied);

  var signalLogPayload = {
    signal_id: signalId,
    sent_at_local: isoLocalWithOffset_(new Date()),
    odds_game_id: String(oddsId || ""),
    mlb_gamePk: String(payload.mlbGamePk || ""),
    pick_side: String(payload.bet.side || ""),
    pick_team: String(pickTeam || ""),
    open_price_pick: openMetrics.open_price_pick,
    open_implied_pick: openMetrics.open_implied_pick,
    delta_open_to_signal_price: openMetrics.delta_open_to_signal_price,
    delta_open_to_signal_implied: openMetrics.delta_open_to_signal_implied,
    open_reason_code: openMetrics.open_reason_code,
    price_at_signal: isFinite(price) ? round_(price, 4) : "",
    implied_at_signal: isFinite(implied) ? round_(implied, 6) : "",
    model_prob_at_signal: isFinite(modelP) ? round_(modelP, 6) : "",
    edge_at_signal: isFinite(edge) ? round_(edge, 6) : "",
    close_price_pick: "",
    close_implied_pick: "",
    delta_signal_to_close_price: "",
    delta_signal_to_close_implied: "",
    close_reason_code: "close_not_stamped_yet",
    tier: String(payload.bet.tier || ""),
    confidence: isFinite(payload.conf) ? round_(payload.conf, 2) : "",
    units_suggested: isFinite(payload.units) ? round_(payload.units, 2) : "",
    source_reason: String(reason || ""),
    delivery_status: "",
    delivery_reason_code: "",
    delivery_http: "",
    delivery_mode: "",
    delivery_error_preview: "",
    discord_message_id: ""
  };

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
    "🆔 SignalId: `" + signalId + "` | OddsId: `" + oddsId + "`";

  var isUpdate = isSameDaySignal && !!prevState.lastSignalId;
  if (isUpdate) {
    msg =
      "🔁 SIGNAL UPDATE\n" +
      "Prior SignalId: `" + prevState.lastSignalId + "` → New SignalId: `" + signalId + "`\n" +
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
    signalLogPayload.delivery_status = "failed";
    signalLogPayload.delivery_reason_code = "missing_delivery_config";
    signalLogPayload.delivery_mode = String(deliveryMode.mode || "");
    signalLogPayload.delivery_error_preview = "missing delivery config";
    appendSignalLogRow_(signalLogPayload);
    log_("WARN", "Discord notify skipped: missing delivery config", { includesComponents: includesComponents, deliveryMode: deliveryMode.mode, signalId: signalId });
    return false;
  }

  var res = sendDiscordByMode_(deliveryMode, payloadObj);
  signalLogPayload.delivery_http = isFinite(res.http) ? Number(res.http) : "";
  signalLogPayload.delivery_mode = String(res.deliveryMode || deliveryMode.mode || "");
  if (res.http >= 200 && res.http < 300) {
    var messageId = discordMessageIdFromBody_(res.body);
    signalLogPayload.delivery_status = "sent";
    signalLogPayload.discord_message_id = String(messageId || "");
    appendSignalLogRow_(signalLogPayload);
    props.setProperty(key, JSON.stringify({
      sig: sig,
      dateKey: dateKey,
      lastSentAt: isoLocalWithOffset_(new Date()),
      lastSentMs: nowMs,
      lastPrice: isFinite(price) ? round_(price, 4) : "",
      lastEdge: isFinite(edge) ? round_(edge, 6) : "",
      lastTier: String(payload.bet.tier || ""),
      lastSignalId: String(signalId || ""),
      lastDiscordMessageId: String(messageId || "")
    }));
    return true;
  }

  signalLogPayload.delivery_status = "failed";
  signalLogPayload.delivery_reason_code = "delivery_http_error";
  signalLogPayload.delivery_error_preview = String(res.body || "").slice(0, 250);
  appendSignalLogRow_(signalLogPayload);
  log_("WARN", "Discord notify failed", {
    http: res.http,
    body: String(res.body || "").slice(0, 250),
    includesComponents: includesComponents,
    deliveryMode: res.deliveryMode,
    signalId: signalId
  });
  return false;
}


function getCloseSourceSnapshotsByGameId_(preStartMin) {
  return getOddsHistoryIndexOrRebuild_(preStartMin);
}

function selectCloseSnapshotsByGameId_(historyIndex, preStartMin, nowMs) {
  var out = {};
  var byId = (historyIndex && historyIndex.by_id) ? historyIndex.by_id : {};
  var ids = Object.keys(byId);
  var cutoffOffsetMs = Math.max(0, Number(preStartMin) || 0) * 60000;

  for (var i = 0; i < ids.length; i++) {
    var oddsId = ids[i];
    var entry = byId[oddsId] || {};
    var latest = entry.latest || null;
    var cutoffSnap = entry.close_cutoff || null;
    var commenceMs = Date.parse(String((latest && latest.commence_time_utc) || entry.commence_time_utc || ""));
    var cutoffMs = isFinite(commenceMs) ? (commenceMs - cutoffOffsetMs) : NaN;

    if (isFinite(cutoffMs) && isFinite(nowMs) && nowMs < cutoffMs) {
      out[oddsId] = { snapshot: null, reason: "close_still_too_early", cutoff_ms: cutoffMs };
      continue;
    }

    if (!cutoffSnap) {
      out[oddsId] = { snapshot: null, reason: String(entry.close_reason || "close_no_snapshot_before_cutoff"), cutoff_ms: cutoffMs };
      continue;
    }

    out[oddsId] = { snapshot: cutoffSnap, reason: "", cutoff_ms: cutoffMs };
  }

  return out;
}

function updateSignalLogCloseMetrics() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.SIGNAL_LOG);
  if (!sh || sh.getLastRow() < 2) {
    log_("INFO", "Signal close updater skipped", { reasonCode: "signal_log_empty" });
    return { updatedRows: 0, skippedRows: 0, reasonCode: "signal_log_empty" };
  }
  ensureSignalLogHeader_(sh);

  var range = sh.getDataRange();
  var values = range.getValues();
  var header = values[0].map(function (x) { return String(x || ""); });
  var idx = {};
  for (var h = 0; h < header.length; h++) idx[header[h]] = h;

  var required = ["odds_game_id", "pick_side", "price_at_signal", "implied_at_signal", "close_price_pick", "close_implied_pick", "delta_signal_to_close_price", "delta_signal_to_close_implied", "close_reason_code"];
  for (var r = 0; r < required.length; r++) {
    if (idx[required[r]] === undefined) {
      log_("WARN", "Signal close updater skipped", { reasonCode: "signal_log_header_missing_col", col: required[r] });
      return { updatedRows: 0, skippedRows: 0, reasonCode: "signal_log_header_missing_col" };
    }
  }

  var cfg = getConfig_();
  var preStartMin = Math.max(0, toInt_(cfg.SIGNAL_CLOSE_PRESTART_MIN, 15));
  var nowMs = Date.now();
  var closeSourceByOddsId = getCloseSourceSnapshotsByGameId_(preStartMin);
  var selectedCloseByOddsId = selectCloseSnapshotsByGameId_(closeSourceByOddsId, preStartMin, nowMs);
  var updatedRows = 0;
  var skippedRows = 0;

  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    if (String(row[idx.close_price_pick] || "") !== "" || String(row[idx.close_implied_pick] || "") !== "") continue;

    var oddsId = String(row[idx.odds_game_id] || "");
    var pickSide = String(row[idx.pick_side] || "");
    var signalPrice = Number(row[idx.price_at_signal]);
    var signalImplied = Number(row[idx.implied_at_signal]);

    if (!oddsId) {
      row[idx.close_reason_code] = "close_odds_game_id_missing";
      skippedRows++;
      continue;
    }

    var selection = selectedCloseByOddsId[oddsId] || null;
    if (!selection || !selection.snapshot) {
      row[idx.close_reason_code] = String((selection && selection.reason) || "close_no_snapshot_before_cutoff");
      skippedRows++;
      continue;
    }

    var closeEntry = getPickSideOdds_(selection.snapshot, pickSide);
    if (!isFinite(closeEntry.price) || !isFinite(closeEntry.implied)) {
      row[idx.close_price_pick] = "";
      row[idx.close_implied_pick] = "";
      row[idx.delta_signal_to_close_price] = "";
      row[idx.delta_signal_to_close_implied] = "";
      row[idx.close_reason_code] = String(closeEntry.reason || "close_pick_price_missing").replace(/^open_/, "close_");
      skippedRows++;
      continue;
    }

    row[idx.close_price_pick] = round_(closeEntry.price, 4);
    row[idx.close_implied_pick] = round_(closeEntry.implied, 6);
    row[idx.delta_signal_to_close_price] = isFinite(signalPrice) ? round_(closeEntry.price - signalPrice, 4) : "";
    row[idx.delta_signal_to_close_implied] = isFinite(signalImplied) ? round_(closeEntry.implied - signalImplied, 6) : "";
    row[idx.close_reason_code] = "";
    updatedRows++;
  }

  range.setValues(values);
  log_("INFO", "Signal close updater completed", { updatedRows: updatedRows, skippedRows: skippedRows, preStartMin: preStartMin });
  return { updatedRows: updatedRows, skippedRows: skippedRows, reasonCode: "ok" };
}

function appendSignalLogRow_(rowObj) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.SIGNAL_LOG) || getOrCreateSheet_(ss, SH.SIGNAL_LOG);
  ensureSignalLogHeader_(sh);
  var header = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  var row = rowObjectToHeader_(rowObj || {}, header);
  sh.appendRow(row);
}

function parseNotifyState_(raw) {
  var out = {
    sig: "",
    dateKey: "",
    lastSentMs: NaN,
    lastPrice: NaN,
    lastEdge: NaN,
    lastTier: "",
    lastSignalId: "",
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
    out.lastSignalId = String(obj.lastSignalId || obj.lastBetId || "");
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
