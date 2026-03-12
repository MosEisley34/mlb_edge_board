/* ===================== CALIBRATION SNAPSHOTS + REPORT ===================== */

function persistCalibrationSnapshots_(cfg, edgeRows, schedByPk, externalFeatureCtx) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.CALIBRATION_SNAPSHOTS);
  if (!sh) return { upserted: 0 };
  if (!edgeRows || !edgeRows.length) return { upserted: 0 };

  var header = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(function (x) { return String(x || "").trim(); });
  var idIdx = indexOf_(header, "snapshot_id");
  var existingById = {};
  if (idIdx >= 0 && sh.getLastRow() > 1) {
    var ids = sh.getRange(2, idIdx + 1, sh.getLastRow() - 1, 1).getValues();
    for (var i = 0; i < ids.length; i++) {
      var sid = String(ids[i][0] || "").trim();
      if (sid) existingById[sid] = i + 2;
    }
  }

  var nowIso = isoLocalWithOffset_(new Date());
  var dateKey = localDateKey_();
  var appendRows = [];
  var overwriteRows = [];

  for (var r = 0; r < edgeRows.length; r++) {
    var row = edgeRows[r] || {};
    var oddsId = String(row.odds_game_id || "").trim();
    var mlbPk = String(row.mlb_gamePk || "").trim();
    if (!oddsId || !mlbPk) continue;

    var edgeAway = Number(row.edge_away);
    var edgeHome = Number(row.edge_home);
    var pickSide = "";
    if (String(row.bet_side || "").trim()) pickSide = String(row.bet_side || "").trim().toUpperCase();
    else if (isFinite(edgeAway) || isFinite(edgeHome)) pickSide = (Math.abs(edgeAway) >= Math.abs(edgeHome)) ? "AWAY" : "HOME";
    if (pickSide !== "AWAY" && pickSide !== "HOME") continue;

    var modelPick = pickSide === "AWAY" ? Number(row.model_p_away) : Number(row.model_p_home);
    var marketPick = pickSide === "AWAY" ? Number(row.away_implied) : Number(row.home_implied);
    if (!isFinite(modelPick) || !isFinite(marketPick)) continue;

    var sched = schedByPk[mlbPk] || {};
    var awayTeamId = String(sched.away_team_id || "");
    var homeTeamId = String(sched.home_team_id || "");
    var pickTeam = pickSide === "AWAY" ? String(row.away_team || "") : String(row.home_team || "");
    var pickTeamId = pickSide === "AWAY" ? awayTeamId : homeTeamId;
    var pickHomeAway = pickSide === "HOME" ? "HOME" : "AWAY";

    var snapshotId = [dateKey, oddsId, pickSide].join("|");
    var obj = {
      snapshot_id: snapshotId,
      snapshot_date_local: dateKey,
      snapshot_at_local: nowIso,
      odds_game_id: oddsId,
      mlb_gamePk: mlbPk,
      away_team: String(row.away_team || ""),
      home_team: String(row.home_team || ""),
      away_team_id: awayTeamId,
      home_team_id: homeTeamId,
      bet_side: pickSide,
      pick_team: pickTeam,
      pick_team_id: pickTeamId,
      pick_home_away: pickHomeAway,
      confidence: Number(row.confidence),
      bet_tier: String(row.bet_tier || ""),
      bet_edge: pickSide === "AWAY" ? edgeAway : edgeHome,
      model_prob_pick: modelPick,
      market_implied_pick: marketPick,
      model_prob_away: Number(row.model_p_away),
      model_prob_home: Number(row.model_p_home),
      market_implied_away: Number(row.away_implied),
      market_implied_home: Number(row.home_implied),
      away_odds_decimal: Number(row.away_odds_decimal),
      home_odds_decimal: Number(row.home_odds_decimal),
      units_suggested: Number(row.units),
      notes: String(row.notes || ""),
      feature_set: String(row.featureSet || "BASELINE"),
      weather_applied: String(row.weatherApplied || "N"),
      bullpen_applied: String(row.bullpenFeatureApplied || "N"),
      experimental_applied: String(row.experimentalApplied || "N"),
      result: "",
      pnl_units: "",
      resolved_at_local: "",
      updated_at_local: nowIso
    };

    applyResultToSnapshot_(obj);

    var vals = rowObjectToHeader_(obj, header);
    if (existingById[snapshotId]) overwriteRows.push({ row: existingById[snapshotId], values: vals });
    else appendRows.push(vals);
  }

  for (var w = 0; w < overwriteRows.length; w++) {
    var ow = overwriteRows[w];
    sh.getRange(ow.row, 1, 1, header.length).setValues([ow.values]);
  }
  if (appendRows.length) sh.getRange(sh.getLastRow() + 1, 1, appendRows.length, header.length).setValues(appendRows);

  return { upserted: overwriteRows.length + appendRows.length };
}

function applyResultToSnapshot_(snapshotObj) {
  var ss = SpreadsheetApp.getActive();
  var shSignals = ss.getSheetByName(SH.SIGNAL_LOG);
  if (!shSignals || shSignals.getLastRow() < 2) return;

  var rows = readSheetAsObjects_(shSignals);
  var best = null;
  for (var i = 0; i < rows.length; i++) {
    var sig = rows[i] || {};
    if (String(sig.odds_game_id || "") !== String(snapshotObj.odds_game_id || "")) continue;
    if (String(sig.pick_side || "").toUpperCase() !== String(snapshotObj.bet_side || "").toUpperCase()) continue;
    best = sig;
  }
  if (!best) return;

  // SIGNAL_LOG is immutable and does not track bet settlement; snapshot result fields remain empty.
}

function runDailyCalibration() {
  var settlement = settleCalibrationSnapshots_();
  log_("INFO", "Calibration settlement complete", {
    settledCount: settlement.settledCount,
    unresolvedCount: settlement.unresolvedCount,
    inspectedUnresolved: settlement.inspectedUnresolved,
    skippedMissingOutcome: settlement.skippedMissingOutcome,
    skippedInvalidRows: settlement.skippedInvalidRows
  });

  var cfg = getConfig_();
  var report = computeCalibrationReport_(cfg);
  log_("INFO", "Calibration report generated", {
    windowDays: report.windowDays,
    sampleSize: report.sampleSize,
    resolvedCount: report.resolvedCount,
    summary: report.summary,
    suggestions: report.suggestions
  });
  return report;
}

function settleCalibrationSnapshots_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.CALIBRATION_SNAPSHOTS);
  if (!sh || sh.getLastRow() < 2) {
    return { settledCount: 0, unresolvedCount: 0, inspectedUnresolved: 0, skippedMissingOutcome: 0, skippedInvalidRows: 0 };
  }

  var header = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(function (x) { return String(x || "").trim(); });
  var idxPk = indexOf_(header, "mlb_gamePk");
  var idxSide = indexOf_(header, "bet_side");
  var idxResult = indexOf_(header, "result");
  var idxPnl = indexOf_(header, "pnl_units");
  var idxResolvedAt = indexOf_(header, "resolved_at_local");
  var idxUpdatedAt = indexOf_(header, "updated_at_local");
  var idxAwayOdds = indexOf_(header, "away_odds_decimal");
  var idxHomeOdds = indexOf_(header, "home_odds_decimal");
  var idxUnits = indexOf_(header, "units_suggested");

  if (idxPk < 0 || idxSide < 0 || idxResult < 0 || idxPnl < 0 || idxResolvedAt < 0 || idxAwayOdds < 0 || idxHomeOdds < 0 || idxUnits < 0) {
    log_("WARN", "Calibration settlement skipped due to missing required columns", {
      idxPk: idxPk,
      idxSide: idxSide,
      idxResult: idxResult,
      idxPnl: idxPnl,
      idxResolvedAt: idxResolvedAt,
      idxAwayOdds: idxAwayOdds,
      idxHomeOdds: idxHomeOdds,
      idxUnits: idxUnits
    });
    return { settledCount: 0, unresolvedCount: 0, inspectedUnresolved: 0, skippedMissingOutcome: 0, skippedInvalidRows: 0 };
  }

  var rowCount = sh.getLastRow() - 1;
  var data = sh.getRange(2, 1, rowCount, header.length).getValues();
  var outcomeByPk = {};
  var settledCount = 0;
  var unresolvedCount = 0;
  var inspectedUnresolved = 0;
  var skippedMissingOutcome = 0;
  var skippedInvalidRows = 0;
  var nowIso = isoLocalWithOffset_(new Date());

  for (var i = 0; i < data.length; i++) {
    var row = data[i];
    var alreadyResolved = String(row[idxResolvedAt] || "").trim();
    var existingResult = String(row[idxResult] || "").toUpperCase();
    if (alreadyResolved || existingResult === "WIN" || existingResult === "LOSS") continue;

    unresolvedCount++;
    inspectedUnresolved++;

    var gamePk = String(row[idxPk] || "").trim();
    var side = String(row[idxSide] || "").trim().toUpperCase();
    if (!gamePk || (side !== "AWAY" && side !== "HOME")) {
      skippedInvalidRows++;
      continue;
    }

    if (!outcomeByPk[gamePk]) outcomeByPk[gamePk] = fetchMlbGameOutcomeByPk_(gamePk);
    var outcome = outcomeByPk[gamePk];
    if (!outcome || !outcome.isFinal || !outcome.winnerSide) {
      skippedMissingOutcome++;
      continue;
    }

    var odds = side === "AWAY" ? Number(row[idxAwayOdds]) : Number(row[idxHomeOdds]);
    var units = Number(row[idxUnits]);
    if (!isFinite(odds) || odds <= 1 || !isFinite(units)) {
      skippedInvalidRows++;
      continue;
    }

    var isWin = side === outcome.winnerSide;
    var pnlUnits = isWin ? (units * (odds - 1)) : (-1 * units);
    row[idxResult] = isWin ? "WIN" : "LOSS";
    row[idxPnl] = round_(pnlUnits, 4);
    row[idxResolvedAt] = nowIso;
    if (idxUpdatedAt >= 0) row[idxUpdatedAt] = nowIso;
    settledCount++;
  }

  if (settledCount > 0) sh.getRange(2, 1, rowCount, header.length).setValues(data);

  var unresolvedRemainder = unresolvedCount - settledCount;
  return {
    settledCount: settledCount,
    unresolvedCount: Math.max(0, unresolvedRemainder),
    inspectedUnresolved: inspectedUnresolved,
    skippedMissingOutcome: skippedMissingOutcome,
    skippedInvalidRows: skippedInvalidRows
  };
}

function fetchMlbGameOutcomeByPk_(gamePk) {
  var url = "https://statsapi.mlb.com/api/v1.1/game/" + encodeURIComponent(String(gamePk || "")) + "/feed/live";
  try {
    var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (resp.getResponseCode() !== 200) return { isFinal: false, winnerSide: "" };

    var payload = JSON.parse(resp.getContentText() || "{}");
    var abs = payload && payload.liveData && payload.liveData.linescore && payload.liveData.linescore.teams
      ? payload.liveData.linescore.teams
      : null;
    if (!abs) return { isFinal: false, winnerSide: "" };

    var awayRuns = Number(abs.away && abs.away.runs);
    var homeRuns = Number(abs.home && abs.home.runs);
    if (!isFinite(awayRuns) || !isFinite(homeRuns)) return { isFinal: false, winnerSide: "" };

    var detailed = String(payload && payload.gameData && payload.gameData.status && payload.gameData.status.detailedState || "").toLowerCase();
    var abstract = String(payload && payload.gameData && payload.gameData.status && payload.gameData.status.abstractGameState || "").toLowerCase();
    var isFinal = abstract === "final" || detailed.indexOf("final") >= 0 || detailed.indexOf("game over") >= 0;
    if (!isFinal || awayRuns === homeRuns) return { isFinal: false, winnerSide: "" };

    return { isFinal: true, winnerSide: awayRuns > homeRuns ? "AWAY" : "HOME" };
  } catch (e) {
    return { isFinal: false, winnerSide: "" };
  }
}

function computeCalibrationReport_(cfg) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.CALIBRATION_SNAPSHOTS);
  if (!sh || sh.getLastRow() < 2) return { windowDays: cfg.CALIBRATION_WINDOW_DAYS, sampleSize: 0, resolvedCount: 0, pendingCount: 0, summary: "Calibration: no snapshots yet.", suggestions: [], rolling: {}, alerts: [] };

  var rows = readSheetAsObjects_(sh);
  var windowDays = Math.max(7, toInt_(cfg.CALIBRATION_WINDOW_DAYS, 30));
  var edgeCuts = (cfg.CALIBRATION_EDGE_BUCKETS && cfg.CALIBRATION_EDGE_BUCKETS.length) ? cfg.CALIBRATION_EDGE_BUCKETS : [0, 0.02, 0.04, 0.06, 0.10];
  var nowMs = new Date().getTime();

  var scoped = [];
  var resolvedScoped = [];
  var pendingCount = 0;
  var cutoffMs = nowMs - windowDays * 24 * 3600 * 1000;

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i] || {};
    var snapMs = Date.parse(String(row.snapshot_at_local || ""));
    if (!isFinite(snapMs) || snapMs < cutoffMs) continue;
    scoped.push(row);

    var result = String(row.result || "").toUpperCase();
    if (result === "WIN" || result === "LOSS") resolvedScoped.push(row);
    else pendingCount++;
  }

  var metrics = {
    byTier: bucketMetrics_(resolvedScoped, function (r) { return String(r.bet_tier || "UNTIERED") || "UNTIERED"; }),
    byEdgeBucket: bucketMetrics_(resolvedScoped, function (r) { return edgeBucketLabel_(Math.abs(Number(r.bet_edge || 0)), edgeCuts); }),
    byConfidenceBucket: bucketMetrics_(resolvedScoped, function (r) { return confidenceBucketLabel_(Number(r.confidence || 0)); }),
    byFeatureSet: bucketMetrics_(resolvedScoped, function (r) { return String(r.feature_set || "BASELINE").toUpperCase(); }),
    byTeam: bucketMetrics_(resolvedScoped, function (r) { return String(r.pick_team_id || r.pick_team || "UNKNOWN"); }),
    byHomeAway: bucketMetrics_(resolvedScoped, function (r) { return String(r.pick_home_away || "UNKNOWN"); })
  };

  var overall = aggregateMetrics_(resolvedScoped);
  var suggestions = calibrationSuggestions_(overall, metrics.byEdgeBucket, cfg);

  var resolvedAll = [];
  for (var j = 0; j < rows.length; j++) {
    var rr = rows[j] || {};
    var res = String(rr.result || "").toUpperCase();
    if (res !== "WIN" && res !== "LOSS") continue;
    var resolvedMs = calibrationResolvedAtMs_(rr);
    if (!isFinite(resolvedMs)) continue;
    resolvedAll.push(rr);
  }

  var rollingWindows = [7, 14, 30];
  var rolling = {};
  var alerts = [];
  for (var w = 0; w < rollingWindows.length; w++) {
    var days = rollingWindows[w];
    rolling[String(days)] = computeCalibrationRollingWindow_(resolvedAll, days, nowMs, cfg);
    alerts = alerts.concat(buildCalibrationAlertsForWindow_(rolling[String(days)], cfg));
  }

  appendCalibrationTrendRows_(ss, rolling, alerts);

  var fsBase = metrics.byFeatureSet.BASELINE || { n: 0, roi: "" };
  var fsEnh = metrics.byFeatureSet.ENHANCED || { n: 0, roi: "" };
  var summary = "Calibration " + windowDays + "d: resolved=" + resolvedScoped.length + " pending=" + pendingCount +
    " brier(model=" + round_(overall.brierModel, 4) + ", market=" + round_(overall.brierMarket, 4) +
    ") roi=" + round_(overall.roi, 3) + " bias=" + round_(overall.biasModel, 3) +
    " featureSet(base_n=" + fsBase.n + ", base_roi=" + round_(fsBase.roi, 3) + ", enh_n=" + fsEnh.n + ", enh_roi=" + round_(fsEnh.roi, 3) + ")" +
    " rolling7(n=" + ((rolling["7"] && rolling["7"].overall && rolling["7"].overall.n) || 0) + ", roi=" + round_(((rolling["7"] && rolling["7"].overall && rolling["7"].overall.roi) || 0), 3) + ")";

  var shReport = ss.getSheetByName(SH.CALIBRATION_REPORT);
  if (shReport) {
    shReport.appendRow([
      isoLocalWithOffset_(new Date()),
      windowDays,
      scoped.length,
      resolvedScoped.length,
      pendingCount,
      JSON.stringify({ overall: overall, metrics: metrics, rolling: rolling, alerts: alerts, suggestions: suggestions }),
      summary
    ]);
  }

  var alertSummary = alerts.map(function (a) { return a.windowDays + "d:" + a.code + "(" + a.scope + ")"; }).join(", ");
  PropertiesService.getScriptProperties().setProperty(PROP.LAST_CALIBRATION_SUMMARY, summary + " | " + suggestions.join("; ") + (alertSummary ? (" | alerts=" + alertSummary) : ""));

  return {
    windowDays: windowDays,
    sampleSize: scoped.length,
    resolvedCount: resolvedScoped.length,
    pendingCount: pendingCount,
    overall: overall,
    metrics: metrics,
    rolling: rolling,
    alerts: alerts,
    suggestions: suggestions,
    summary: summary
  };
}

function calibrationResolvedAtMs_(row) {
  var resolvedMs = Date.parse(String(row.resolved_at_local || ""));
  if (isFinite(resolvedMs)) return resolvedMs;
  var snapshotMs = Date.parse(String(row.snapshot_at_local || ""));
  return isFinite(snapshotMs) ? snapshotMs : NaN;
}

function computeCalibrationRollingWindow_(resolvedRows, windowDays, nowMs, cfg) {
  var cutoffMs = nowMs - (windowDays * 24 * 3600 * 1000);
  var scoped = [];
  for (var i = 0; i < resolvedRows.length; i++) {
    var row = resolvedRows[i] || {};
    var rowMs = calibrationResolvedAtMs_(row);
    if (!isFinite(rowMs) || rowMs < cutoffMs) continue;
    scoped.push(row);
  }

  var byFeatureSet = bucketMetrics_(scoped, function (r) { return String(r.feature_set || "BASELINE").toUpperCase(); });
  var baseline = byFeatureSet.BASELINE || aggregateMetrics_([]);
  var enhanced = byFeatureSet.ENHANCED || aggregateMetrics_([]);
  var overall = aggregateMetrics_(scoped);

  var deltas = {
    winRate: calibrationDelta_(enhanced.hitRate, baseline.hitRate),
    roi: calibrationDelta_(enhanced.roi, baseline.roi),
    brierModel: calibrationDelta_(enhanced.brierModel, baseline.brierModel),
    brierDeltaVsMarket: calibrationDelta_(
      calibrationDelta_(enhanced.brierModel, enhanced.brierMarket),
      calibrationDelta_(baseline.brierModel, baseline.brierMarket)
    )
  };

  return {
    windowDays: windowDays,
    overall: overall,
    byFeatureSet: { BASELINE: baseline, ENHANCED: enhanced },
    deltas: deltas
  };
}

function calibrationDelta_(a, b) {
  if (!isFinite(a) || !isFinite(b)) return "";
  return a - b;
}

function buildCalibrationAlertsForWindow_(windowMetrics, cfg) {
  var out = [];
  if (!windowMetrics || !windowMetrics.overall) return out;

  var minSample = Math.max(1, toInt_(cfg.CALIBRATION_ALERT_MIN_SAMPLE, 15));
  var maxBrierGap = Math.max(0, toFloat_(cfg.CALIBRATION_ALERT_BRIER_GAP, 0.010));
  var windowDays = windowMetrics.windowDays;

  var scopes = ["OVERALL", "BASELINE", "ENHANCED"];
  for (var i = 0; i < scopes.length; i++) {
    var scope = scopes[i];
    var m = scope === "OVERALL" ? windowMetrics.overall : windowMetrics.byFeatureSet[scope];
    if (!m) continue;

    if (m.n < minSample) {
      out.push({
        code: "LOW_SAMPLE",
        windowDays: windowDays,
        scope: scope,
        detail: "n=" + m.n + " < " + minSample
      });
    }

    if (isFinite(m.brierModel) && isFinite(m.brierMarket) && (m.brierModel - m.brierMarket) > maxBrierGap) {
      out.push({
        code: "MODEL_UNDER_MARKET",
        windowDays: windowDays,
        scope: scope,
        detail: "brier_gap=" + round_(m.brierModel - m.brierMarket, 4) + " > " + round_(maxBrierGap, 4)
      });
    }
  }

  return out;
}

function appendCalibrationTrendRows_(ss, rolling, alerts) {
  var shTrend = ss.getSheetByName(SH.CALIBRATION_TRENDS);
  if (!shTrend) return;

  var nowIso = isoLocalWithOffset_(new Date());
  var alertMap = {};
  for (var i = 0; i < alerts.length; i++) {
    var a = alerts[i] || {};
    var key = String(a.windowDays) + "|" + String(a.scope || "OVERALL");
    if (!alertMap[key]) alertMap[key] = [];
    alertMap[key].push(a);
  }

  var rows = [];
  var windows = [7, 14, 30];
  var scopes = ["BASELINE", "ENHANCED"];
  for (var w = 0; w < windows.length; w++) {
    var win = rolling[String(windows[w])];
    if (!win) continue;

    for (var sIdx = 0; sIdx < scopes.length; sIdx++) {
      var scope = scopes[sIdx];
      var m = win.byFeatureSet[scope] || aggregateMetrics_([]);
      var key = String(windows[w]) + "|" + scope;
      var aList = alertMap[key] || [];
      rows.push([
        nowIso,
        windows[w],
        scope,
        m.n,
        isFinite(m.hitRate) ? round_(m.hitRate, 4) : "",
        isFinite(m.roi) ? round_(m.roi, 4) : "",
        isFinite(m.brierModel) ? round_(m.brierModel, 4) : "",
        isFinite(m.brierMarket) ? round_(m.brierMarket, 4) : "",
        (isFinite(m.brierModel) && isFinite(m.brierMarket)) ? round_(m.brierModel - m.brierMarket, 4) : "",
        isFinite(win.deltas.winRate) ? round_(win.deltas.winRate, 4) : "",
        isFinite(win.deltas.roi) ? round_(win.deltas.roi, 4) : "",
        isFinite(win.deltas.brierModel) ? round_(win.deltas.brierModel, 4) : "",
        aList.map(function (a) { return a.code; }).join("|"),
        aList.map(function (a) { return a.detail; }).join("; ")
      ]);
    }
  }

  if (rows.length) shTrend.getRange(shTrend.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
}

function bucketMetrics_(rows, bucketFn) {
  var out = {};
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    var k = String(bucketFn(r) || "UNKNOWN");
    if (!out[k]) out[k] = [];
    out[k].push(r);
  }

  var finalOut = {};
  for (var key in out) finalOut[key] = aggregateMetrics_(out[key]);
  return finalOut;
}

function aggregateMetrics_(rows) {
  var n = rows.length;
  if (!n) return { n: 0, brierModel: "", brierMarket: "", logLossModel: "", logLossMarket: "", hitRate: "", roi: "", biasModel: "", biasMarket: "" };

  var sumBrierM = 0, sumBrierMk = 0, sumLogM = 0, sumLogMk = 0, sumY = 0, sumPM = 0, sumPMk = 0;
  var wins = 0, totalUnits = 0, totalPnl = 0;

  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    var y = String(r.result || "").toUpperCase() === "WIN" ? 1 : 0;
    var pm = clamp_(0.001, 0.999, Number(r.model_prob_pick || 0.5));
    var pk = clamp_(0.001, 0.999, Number(r.market_implied_pick || 0.5));

    sumBrierM += Math.pow(pm - y, 2);
    sumBrierMk += Math.pow(pk - y, 2);
    sumLogM += -((y * Math.log(pm)) + ((1 - y) * Math.log(1 - pm)));
    sumLogMk += -((y * Math.log(pk)) + ((1 - y) * Math.log(1 - pk)));
    sumY += y;
    sumPM += pm;
    sumPMk += pk;
    if (y === 1) wins++;

    var u = Number(r.units_suggested || 0);
    var pnl = Number(r.pnl_units);
    if (isFinite(u) && u > 0) totalUnits += u;
    if (isFinite(pnl)) totalPnl += pnl;
  }

  return {
    n: n,
    brierModel: sumBrierM / n,
    brierMarket: sumBrierMk / n,
    logLossModel: sumLogM / n,
    logLossMarket: sumLogMk / n,
    hitRate: wins / n,
    roi: totalUnits > 0 ? (totalPnl / totalUnits) : 0,
    biasModel: (sumY / n) - (sumPM / n),
    biasMarket: (sumY / n) - (sumPMk / n)
  };
}

function calibrationSuggestions_(overall, byEdgeBucket, cfg) {
  var out = [];
  if (!overall || !overall.n) return ["Need more resolved bets before coefficient/threshold changes."];

  if (isFinite(overall.biasModel) && Math.abs(overall.biasModel) >= 0.015) {
    var dir = overall.biasModel > 0 ? "increase" : "decrease";
    out.push("Consider " + dir + " MODEL_K_OPS / MODEL_K_PIT by ~5% (bias=" + round_(overall.biasModel, 3) + ").");
  }

  if (isFinite(overall.brierModel) && isFinite(overall.brierMarket) && overall.brierModel > overall.brierMarket) {
    out.push("Model trailing market on Brier; tighten edge cutoffs by +0.005 before scaling volume.");
  }

  var weakBuckets = [];
  for (var k in byEdgeBucket) {
    var m = byEdgeBucket[k];
    if (m && m.n >= 8 && isFinite(m.roi) && m.roi < 0) weakBuckets.push(k + " ROI=" + round_(m.roi, 3));
  }
  if (weakBuckets.length) out.push("Negative ROI edge buckets: " + weakBuckets.join(", ") + ". Consider raising RS/PS edge minimums.");

  if (!out.length) out.push("Calibration stable. Keep thresholds unchanged; monitor next sample window.");
  return out;
}

function edgeBucketLabel_(edgeAbs, cuts) {
  var c = (cuts || []).slice(0).sort(function (a, b) { return a - b; });
  if (!c.length) c = [0, 0.02, 0.04, 0.06, 0.10];
  if (edgeAbs < c[0]) return "<" + c[0];
  for (var i = 0; i < c.length - 1; i++) {
    if (edgeAbs >= c[i] && edgeAbs < c[i + 1]) return "[" + c[i] + "," + c[i + 1] + ")";
  }
  return ">=" + c[c.length - 1];
}


function confidenceBucketLabel_(confidence) {
  var c = Number(confidence || 0);
  if (!isFinite(c)) return "unknown";
  if (c < 55) return "<55";
  if (c < 60) return "55-59";
  if (c < 65) return "60-64";
  if (c < 70) return "65-69";
  return "70+";
}

function rowObjectToHeader_(obj, header) {
  var out = [];
  for (var i = 0; i < header.length; i++) out.push(obj[header[i]] === undefined ? "" : obj[header[i]]);
  return out;
}
