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

function computeCalibrationReport_(cfg) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.CALIBRATION_SNAPSHOTS);
  if (!sh || sh.getLastRow() < 2) return { windowDays: cfg.CALIBRATION_WINDOW_DAYS, sampleSize: 0, resolvedCount: 0, pendingCount: 0, summary: "Calibration: no snapshots yet.", suggestions: [] };

  var rows = readSheetAsObjects_(sh);
  var windowDays = Math.max(7, toInt_(cfg.CALIBRATION_WINDOW_DAYS, 30));
  var edgeCuts = (cfg.CALIBRATION_EDGE_BUCKETS && cfg.CALIBRATION_EDGE_BUCKETS.length) ? cfg.CALIBRATION_EDGE_BUCKETS : [0, 0.02, 0.04, 0.06, 0.10];

  var cutoffMs = new Date().getTime() - windowDays * 24 * 3600 * 1000;
  var scoped = [];
  for (var i = 0; i < rows.length; i++) {
    var dt = Date.parse(String(rows[i].snapshot_at_local || ""));
    if (!isFinite(dt) || dt < cutoffMs) continue;
    scoped.push(rows[i]);
  }

  var resolved = [];
  var pendingCount = 0;
  for (var j = 0; j < scoped.length; j++) {
    var rr = scoped[j];
    var result = String(rr.result || "").toUpperCase();
    if (result === "WIN" || result === "LOSS") resolved.push(rr);
    else pendingCount++;
  }

  var metrics = {
    byTier: bucketMetrics_(resolved, function (r) { return String(r.bet_tier || "UNTIERED") || "UNTIERED"; }),
    byEdgeBucket: bucketMetrics_(resolved, function (r) { return edgeBucketLabel_(Math.abs(Number(r.bet_edge || 0)), edgeCuts); }),
    byConfidenceBucket: bucketMetrics_(resolved, function (r) { return confidenceBucketLabel_(Number(r.confidence || 0)); }),
    byFeatureSet: bucketMetrics_(resolved, function (r) { return String(r.feature_set || "BASELINE").toUpperCase(); }),
    byTeam: bucketMetrics_(resolved, function (r) { return String(r.pick_team_id || r.pick_team || "UNKNOWN"); }),
    byHomeAway: bucketMetrics_(resolved, function (r) { return String(r.pick_home_away || "UNKNOWN"); })
  };

  var overall = aggregateMetrics_(resolved);
  var suggestions = calibrationSuggestions_(overall, metrics.byEdgeBucket, cfg);
  var fsBase = metrics.byFeatureSet.BASELINE || { n: 0, roi: "" };
  var fsEnh = metrics.byFeatureSet.ENHANCED || { n: 0, roi: "" };
  var summary = "Calibration " + windowDays + "d: resolved=" + resolved.length + " pending=" + pendingCount +
    " brier(model=" + round_(overall.brierModel, 4) + ", market=" + round_(overall.brierMarket, 4) +
    ") roi=" + round_(overall.roi, 3) + " bias=" + round_(overall.biasModel, 3) +
    " featureSet(base_n=" + fsBase.n + ", base_roi=" + round_(fsBase.roi, 3) + ", enh_n=" + fsEnh.n + ", enh_roi=" + round_(fsEnh.roi, 3) + ")";

  var shReport = ss.getSheetByName(SH.CALIBRATION_REPORT);
  if (shReport) {
    shReport.appendRow([
      isoLocalWithOffset_(new Date()),
      windowDays,
      scoped.length,
      resolved.length,
      pendingCount,
      JSON.stringify({ overall: overall, metrics: metrics, suggestions: suggestions }),
      summary
    ]);
  }

  PropertiesService.getScriptProperties().setProperty(PROP.LAST_CALIBRATION_SUMMARY, summary + " | " + suggestions.join("; "));

  return { windowDays: windowDays, sampleSize: scoped.length, resolvedCount: resolved.length, pendingCount: pendingCount, overall: overall, metrics: metrics, suggestions: suggestions, summary: summary };
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
