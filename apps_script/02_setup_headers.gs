/* ===================== SETUP / RESET ===================== */

function setup() {
  var ss = SpreadsheetApp.getActive();

  getOrCreateSheet_(ss, SH.SETTINGS);
  ensureSettings_(ss.getSheetByName(SH.SETTINGS));

  getOrCreateSheet_(ss, SH.LOG);
  getOrCreateSheet_(ss, SH.RUN_SUMMARY_LOG);
  getOrCreateSheet_(ss, SH.ODDS_RAW);
  getOrCreateSheet_(ss, SH.ODDS_HISTORY);
  getOrCreateSheet_(ss, SH.MLB_SCHEDULE);
  getOrCreateSheet_(ss, SH.MLB_LINEUPS);
  getOrCreateSheet_(ss, SH.BATTER_PROJ);
  getOrCreateSheet_(ss, SH.PITCHER_PROJ);
  getOrCreateSheet_(ss, SH.EDGE_BOARD);
  getOrCreateSheet_(ss, SH.SIGNAL_LOG);
  getOrCreateSheet_(ss, SH.PLAYER_MAP);
  getOrCreateSheet_(ss, SH.NOTIFY_STATE);
  if (isBetTrackingEnabled_()) {
    getOrCreateSheet_(ss, BET_TRACKING_SHEETS.BET_LOG);
    getOrCreateSheet_(ss, BET_TRACKING_SHEETS.BET_EVENTS);
  }
  getOrCreateSheet_(ss, SH.CALIBRATION_SNAPSHOTS);
  getOrCreateSheet_(ss, SH.CALIBRATION_REPORT);
  getOrCreateSheet_(ss, SH.CALIBRATION_TRENDS);

  ensureLogHeader_(ss.getSheetByName(SH.LOG));
  ensureRunSummaryLogHeader_(ss.getSheetByName(SH.RUN_SUMMARY_LOG));
  ensureOddsHeader_(ss.getSheetByName(SH.ODDS_RAW));
  ensureOddsHistoryHeader_(ss.getSheetByName(SH.ODDS_HISTORY));
  ensureScheduleHeader_(ss.getSheetByName(SH.MLB_SCHEDULE));
  ensureLineupsHeader_(ss.getSheetByName(SH.MLB_LINEUPS));
  ensureBatterProjHeader_(ss.getSheetByName(SH.BATTER_PROJ));
  ensurePitcherProjHeader_(ss.getSheetByName(SH.PITCHER_PROJ));
  ensureEdgeHeader_(ss.getSheetByName(SH.EDGE_BOARD));
  ensureSignalLogHeader_(ss.getSheetByName(SH.SIGNAL_LOG));
  ensurePlayerMapHeader_(ss.getSheetByName(SH.PLAYER_MAP));
  ensureNotifyStateHeader_(ss.getSheetByName(SH.NOTIFY_STATE));
  if (isBetTrackingEnabled_()) {
    ensureBetLogHeader_(ss.getSheetByName(BET_TRACKING_SHEETS.BET_LOG));
    ensureBetEventsHeader_(ss.getSheetByName(BET_TRACKING_SHEETS.BET_EVENTS));
  }
  ensureCalibrationSnapshotsHeader_(ss.getSheetByName(SH.CALIBRATION_SNAPSHOTS));
  ensureCalibrationReportHeader_(ss.getSheetByName(SH.CALIBRATION_REPORT));
  ensureCalibrationTrendHeader_(ss.getSheetByName(SH.CALIBRATION_TRENDS));

  log_("INFO", "Sheets created/verified.", {
    script_tz: Session.getScriptTimeZone(),
    display_tz: TZ
  });
}

function resetWorkbook() {
  var ss = SpreadsheetApp.getActive();
  var names = [
    SH.LOG, SH.RUN_SUMMARY_LOG, SH.ODDS_RAW, SH.ODDS_HISTORY, SH.MLB_SCHEDULE, SH.MLB_LINEUPS,
    SH.BATTER_PROJ, SH.PITCHER_PROJ, SH.EDGE_BOARD, SH.SIGNAL_LOG, SH.PLAYER_MAP, SH.NOTIFY_STATE,
    SH.CALIBRATION_SNAPSHOTS, SH.CALIBRATION_REPORT, SH.CALIBRATION_TRENDS
  ];
  if (isBetTrackingEnabled_()) {
    names.push(BET_TRACKING_SHEETS.BET_LOG);
    names.push(BET_TRACKING_SHEETS.BET_EVENTS);
  }
  for (var i = 0; i < names.length; i++) {
    var sh = ss.getSheetByName(names[i]);
    if (sh) ss.deleteSheet(sh);
  }

  getOrCreateSheet_(ss, SH.LOG);
  getOrCreateSheet_(ss, SH.RUN_SUMMARY_LOG);
  getOrCreateSheet_(ss, SH.ODDS_RAW);
  getOrCreateSheet_(ss, SH.ODDS_HISTORY);
  getOrCreateSheet_(ss, SH.MLB_SCHEDULE);
  getOrCreateSheet_(ss, SH.MLB_LINEUPS);
  getOrCreateSheet_(ss, SH.BATTER_PROJ);
  getOrCreateSheet_(ss, SH.PITCHER_PROJ);
  getOrCreateSheet_(ss, SH.EDGE_BOARD);
  getOrCreateSheet_(ss, SH.SIGNAL_LOG);
  getOrCreateSheet_(ss, SH.PLAYER_MAP);
  getOrCreateSheet_(ss, SH.NOTIFY_STATE);
  if (isBetTrackingEnabled_()) {
    getOrCreateSheet_(ss, BET_TRACKING_SHEETS.BET_LOG);
    getOrCreateSheet_(ss, BET_TRACKING_SHEETS.BET_EVENTS);
  }
  getOrCreateSheet_(ss, SH.CALIBRATION_SNAPSHOTS);
  getOrCreateSheet_(ss, SH.CALIBRATION_REPORT);
  getOrCreateSheet_(ss, SH.CALIBRATION_TRENDS);

  ensureLogHeader_(ss.getSheetByName(SH.LOG));
  ensureRunSummaryLogHeader_(ss.getSheetByName(SH.RUN_SUMMARY_LOG));
  ensureOddsHeader_(ss.getSheetByName(SH.ODDS_RAW));
  ensureOddsHistoryHeader_(ss.getSheetByName(SH.ODDS_HISTORY));
  ensureScheduleHeader_(ss.getSheetByName(SH.MLB_SCHEDULE));
  ensureLineupsHeader_(ss.getSheetByName(SH.MLB_LINEUPS));
  ensureBatterProjHeader_(ss.getSheetByName(SH.BATTER_PROJ));
  ensurePitcherProjHeader_(ss.getSheetByName(SH.PITCHER_PROJ));
  ensureEdgeHeader_(ss.getSheetByName(SH.EDGE_BOARD));
  ensureSignalLogHeader_(ss.getSheetByName(SH.SIGNAL_LOG));
  ensurePlayerMapHeader_(ss.getSheetByName(SH.PLAYER_MAP));
  ensureNotifyStateHeader_(ss.getSheetByName(SH.NOTIFY_STATE));
  if (isBetTrackingEnabled_()) {
    ensureBetLogHeader_(ss.getSheetByName(BET_TRACKING_SHEETS.BET_LOG));
    ensureBetEventsHeader_(ss.getSheetByName(BET_TRACKING_SHEETS.BET_EVENTS));
  }
  ensureCalibrationSnapshotsHeader_(ss.getSheetByName(SH.CALIBRATION_SNAPSHOTS));
  ensureCalibrationReportHeader_(ss.getSheetByName(SH.CALIBRATION_REPORT));
  ensureCalibrationTrendHeader_(ss.getSheetByName(SH.CALIBRATION_TRENDS));

  var props = PropertiesService.getScriptProperties();
  var all = props.getProperties();
  for (var k in all) {
    if (!all.hasOwnProperty(k)) continue;
    if (k.indexOf("NOTIFY_") === 0) props.deleteProperty(k);
  }
  props.deleteProperty(PROP.LAST_PROJ_HIT);
  props.deleteProperty(PROP.LAST_PROJ_PIT);
  props.deleteProperty(PROP.LAST_PIPELINE_AT);
  props.deleteProperty(PROP.LAST_PIPELINE_STATUS);
  props.deleteProperty(PROP.LAST_PIPELINE_SUMMARY);
  props.deleteProperty(PROP.LAST_CALIBRATION_SUMMARY);
  props.deleteProperty(PROP.PIPELINE_TRIGGER_SIGNATURE);
  props.deleteProperty(PROP.PIPELINE_RUN_DEBOUNCE_UNTIL_MS);
  props.deleteProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED);
  props.deleteProperty(PROP.SIGNAL_CLOSE_UPDATER_CADENCE_MINUTES);
  props.deleteProperty(PROP.SIGNAL_CLOSE_UPDATER_TRIGGER_SIGNATURE);
  props.deleteProperty(PROP.LAST_HEARTBEAT_KEY);
  props.deleteProperty(PROP.ODDS_WINDOW_CACHE);
  props.deleteProperty(PROP.ODDS_ALERT_LAST_SENT_AT_MS);
  props.deleteProperty(PROP.DISCORD_WEBHOOK);
  props.deleteProperty(PROP.DISCORD_BOT_TOKEN);
  props.deleteProperty(PROP.DISCORD_CHANNEL_ID);

  log_("INFO", "Workbook reset completed (tabs deleted + recreated)", {});
}

function isBetTrackingEnabled_() {
  var cfg = getConfig_();
  return !!cfg.ENABLE_BET_TRACKING && !!LEGACY_BET_TRACKING_ALLOW_REENABLE;
}

/* ===================== HEADERS ===================== */

function ensureLogHeader_(sh) { setHeader_(sh, ["ts_local", "level", "message", "detail"]); }

function ensureRunSummaryLogHeader_(sh) {
  setHeader_(sh, [
    "run_id","started_at_utc","finished_at_utc","outcome",
    "trigger_source","app_mode","active_window","duration_ms",
    "odds_outcome","odds_games","schedule_outcome","matched_count",
    "model_outcome","computed","signal_outcome","bet_signals_found",
    "cadence_mode","cadence_reason","cadence_minutes","zero_streak",
    "credit_pressure_level","remaining_credits",
    "reason_code","reason_detail","warnings_count",
    "log_row_start","log_row_end","summary_schema_version"
  ]);
}

function ensureOddsHeader_(sh) {
  setHeader_(sh, [
    "odds_game_id","commence_time_utc","away_team","home_team",
    "away_odds_decimal","home_odds_decimal","away_implied","home_implied",
    "best_book_away","best_book_home","updated_at_local","sport_key_used"
  ]);
}

function ensureOddsHistoryHeader_(sh) {
  setHeader_(sh, [
    "captured_at_local","odds_game_id","commence_time_utc","away_team","home_team",
    "away_odds_decimal","home_odds_decimal","away_implied","home_implied",
    "best_book_away","best_book_home","sport_key_used"
  ]);
}

function ensureScheduleHeader_(sh) {
  setHeader_(sh, [
    "mlb_gamePk","mlb_gameGuid","gameDate_utc","away_team","home_team","away_team_id","home_team_id",
    "away_probable_pitcher","home_probable_pitcher","status","venue","updated_at_local"
  ]);
}

function ensureLineupsHeader_(sh) {
  setHeader_(sh, [
    "mlb_gamePk","odds_game_id","side","bat_order","player_id_mlb",
    "player_name","pos","bats","is_confirmed","updated_at_local"
  ]);
}

function ensureBatterProjHeader_(sh) { if (sh.getLastRow() === 0) sh.getRange(1, 1).setValue("Run projections refresh to load"); }
function ensurePitcherProjHeader_(sh) { if (sh.getLastRow() === 0) sh.getRange(1, 1).setValue("Run projections refresh to load"); }

function ensureEdgeHeader_(sh) {
  setHeader_(sh, [
    "odds_game_id","mlb_gamePk","commence_time_local","away_team","home_team",
    "away_odds_decimal","home_odds_decimal","away_implied","home_implied",
    "model_p_away","model_p_home","edge_away","edge_home",
    "away_hitters_matched","home_hitters_matched","min_hitters_matched",
    "away_pitcher_name","home_pitcher_name","away_pitcher_matched","home_pitcher_matched",
    "bullpenAvailAway","bullpenAvailHome","bullpenAdjDelta",
    "weatherApplied","bullpenFeatureApplied","experimentalApplied","weatherRunEnvDelta",
    "bullpenRunPrevDeltaAway","bullpenRunPrevDeltaHome","experimentalRunEnvDelta","featureSet",
    "confidence","bet_side","bet_tier","bet_edge","units","notes","updated_at_local"
  ]);
}

function ensurePlayerMapHeader_(sh) { setHeader_(sh, ["name_variant","canonical_name","mlb_id","razz_id","notes","updated_at_local"]); }
function ensureNotifyStateHeader_(sh) { setHeader_(sh, ["date_key","plays","units","last_updated_local"]); }
function ensureSignalLogHeader_(sh) {
  setHeader_(sh, [
    "signal_id","sent_at_local","odds_game_id","mlb_gamePk",
    "pick_side","pick_team",
    "open_price_pick","open_implied_pick",
    "delta_open_to_signal_price","delta_open_to_signal_implied",
    "open_reason_code",
    "price_at_signal","implied_at_signal","model_prob_at_signal","edge_at_signal",
    "close_price_pick","close_implied_pick",
    "delta_signal_to_close_price","delta_signal_to_close_implied",
    "close_reason_code",
    "tier","confidence","units_suggested","source_reason",
    "unit_mxn","model_risk_mxn","model_to_win_mxn","placed_risk_mxn","placed_to_win_mxn",
    "sizing_mode","min_bet_mxn","min_applies_to","min_applied","sizing_note",
    "delivery_status","delivery_reason_code","delivery_http","delivery_mode","delivery_error_preview","discord_message_id"
  ]);
  applySignalLogColumnNotes_(sh);
}

function applySignalLogColumnNotes_(sh) {
  if (!sh || sh.getLastRow() < 1 || sh.getLastColumn() < 1) return;
  var header = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  var notesByCol = {
    "delta_open_to_signal_price": "Opening Drift (Open→Signal): price_at_signal - open_price_pick",
    "delta_open_to_signal_implied": "Opening Drift (Open→Signal): implied_at_signal - open_implied_pick",
    "delta_signal_to_close_price": "CLV (Signal→Close): close_price_pick - price_at_signal",
    "delta_signal_to_close_implied": "CLV (Signal→Close): close_implied_pick - implied_at_signal",
    "open_reason_code": "Populated only when opening metrics are unavailable.",
    "close_reason_code": "Populated only when close metrics are unavailable."
  };
  for (var i = 0; i < header.length; i++) {
    var key = String(header[i] || "");
    if (notesByCol[key]) sh.getRange(1, i + 1).setNote(notesByCol[key]);
  }
}
function ensureBetLogHeader_(sh) {
  setHeader_(sh, [
    "bet_id","created_at_local","status","odds_game_id","mlb_gamePk",
    "away_team","home_team","pick_side","pick_team","market",
    "commence_time_local","odds_decimal_alert","model_prob_pick","market_implied_pick",
    "no_vig_implied_pick","edge_pick","confidence","units_suggested",
    "placed_at_local","placed_american_odds","placed_decimal_odds","units_placed",
    "result","result_at_local","pnl_units","notes"
  ]);
}
function ensureBetEventsHeader_(sh) {
  setHeader_(sh, ["event_at_local","bet_id","event","from_status","to_status","detail"]);
}

function ensureCalibrationSnapshotsHeader_(sh) {
  setHeader_(sh, [
    "snapshot_id","snapshot_date_local","snapshot_at_local","odds_game_id","mlb_gamePk",
    "away_team","home_team","away_team_id","home_team_id","bet_side","pick_team","pick_team_id",
    "pick_home_away","confidence","bet_tier","bet_edge","model_prob_pick","market_implied_pick",
    "model_prob_away","model_prob_home","market_implied_away","market_implied_home",
    "away_odds_decimal","home_odds_decimal","units_suggested","notes","feature_set","weather_applied",
    "bullpen_applied","experimental_applied","result","pnl_units","resolved_at_local","updated_at_local"
  ]);
}

function ensureCalibrationReportHeader_(sh) {
  setHeader_(sh, [
    "run_at_local","window_days","sample_size","resolved_count","pending_count","report_json","summary"
  ]);
}

function ensureCalibrationTrendHeader_(sh) {
  setHeader_(sh, [
    "run_at_local","window_days","feature_set","resolved_count","win_rate","roi","brier_model","brier_market","brier_delta_vs_market","win_rate_delta_vs_baseline","roi_delta_vs_baseline","brier_delta_vs_baseline","alert_codes","alert_summary"
  ]);
}
