/* ===================== SETUP / RESET ===================== */

function setup() {
  var ss = SpreadsheetApp.getActive();

  getOrCreateSheet_(ss, SH.SETTINGS);
  ensureSettings_(ss.getSheetByName(SH.SETTINGS));

  getOrCreateSheet_(ss, SH.LOG);
  getOrCreateSheet_(ss, SH.ODDS_RAW);
  getOrCreateSheet_(ss, SH.MLB_SCHEDULE);
  getOrCreateSheet_(ss, SH.MLB_LINEUPS);
  getOrCreateSheet_(ss, SH.BATTER_PROJ);
  getOrCreateSheet_(ss, SH.PITCHER_PROJ);
  getOrCreateSheet_(ss, SH.EDGE_BOARD);
  getOrCreateSheet_(ss, SH.PLAYER_MAP);
  getOrCreateSheet_(ss, SH.NOTIFY_STATE);
  getOrCreateSheet_(ss, SH.BET_LOG);
  getOrCreateSheet_(ss, SH.BET_EVENTS);

  ensureLogHeader_(ss.getSheetByName(SH.LOG));
  ensureOddsHeader_(ss.getSheetByName(SH.ODDS_RAW));
  ensureScheduleHeader_(ss.getSheetByName(SH.MLB_SCHEDULE));
  ensureLineupsHeader_(ss.getSheetByName(SH.MLB_LINEUPS));
  ensureBatterProjHeader_(ss.getSheetByName(SH.BATTER_PROJ));
  ensurePitcherProjHeader_(ss.getSheetByName(SH.PITCHER_PROJ));
  ensureEdgeHeader_(ss.getSheetByName(SH.EDGE_BOARD));
  ensurePlayerMapHeader_(ss.getSheetByName(SH.PLAYER_MAP));
  ensureNotifyStateHeader_(ss.getSheetByName(SH.NOTIFY_STATE));
  ensureBetLogHeader_(ss.getSheetByName(SH.BET_LOG));
  ensureBetEventsHeader_(ss.getSheetByName(SH.BET_EVENTS));

  log_("INFO", "Sheets created/verified.", {
    script_tz: Session.getScriptTimeZone(),
    display_tz: TZ
  });
}

function resetWorkbook() {
  var ss = SpreadsheetApp.getActive();
  var names = [
    SH.LOG, SH.ODDS_RAW, SH.MLB_SCHEDULE, SH.MLB_LINEUPS,
    SH.BATTER_PROJ, SH.PITCHER_PROJ, SH.EDGE_BOARD, SH.PLAYER_MAP, SH.NOTIFY_STATE,
    SH.BET_LOG, SH.BET_EVENTS
  ];
  for (var i = 0; i < names.length; i++) {
    var sh = ss.getSheetByName(names[i]);
    if (sh) ss.deleteSheet(sh);
  }

  getOrCreateSheet_(ss, SH.LOG);
  getOrCreateSheet_(ss, SH.ODDS_RAW);
  getOrCreateSheet_(ss, SH.MLB_SCHEDULE);
  getOrCreateSheet_(ss, SH.MLB_LINEUPS);
  getOrCreateSheet_(ss, SH.BATTER_PROJ);
  getOrCreateSheet_(ss, SH.PITCHER_PROJ);
  getOrCreateSheet_(ss, SH.EDGE_BOARD);
  getOrCreateSheet_(ss, SH.PLAYER_MAP);
  getOrCreateSheet_(ss, SH.NOTIFY_STATE);
  getOrCreateSheet_(ss, SH.BET_LOG);
  getOrCreateSheet_(ss, SH.BET_EVENTS);

  ensureLogHeader_(ss.getSheetByName(SH.LOG));
  ensureOddsHeader_(ss.getSheetByName(SH.ODDS_RAW));
  ensureScheduleHeader_(ss.getSheetByName(SH.MLB_SCHEDULE));
  ensureLineupsHeader_(ss.getSheetByName(SH.MLB_LINEUPS));
  ensureBatterProjHeader_(ss.getSheetByName(SH.BATTER_PROJ));
  ensurePitcherProjHeader_(ss.getSheetByName(SH.PITCHER_PROJ));
  ensureEdgeHeader_(ss.getSheetByName(SH.EDGE_BOARD));
  ensurePlayerMapHeader_(ss.getSheetByName(SH.PLAYER_MAP));
  ensureNotifyStateHeader_(ss.getSheetByName(SH.NOTIFY_STATE));
  ensureBetLogHeader_(ss.getSheetByName(SH.BET_LOG));
  ensureBetEventsHeader_(ss.getSheetByName(SH.BET_EVENTS));

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
  props.deleteProperty(PROP.LAST_HEARTBEAT_KEY);
  props.deleteProperty(PROP.ODDS_WINDOW_CACHE);
  props.deleteProperty(PROP.DISCORD_WEBHOOK);
  props.deleteProperty(PROP.DISCORD_BOT_TOKEN);
  props.deleteProperty(PROP.DISCORD_CHANNEL_ID);

  log_("INFO", "Workbook reset completed (tabs deleted + recreated)", {});
}

/* ===================== HEADERS ===================== */

function ensureLogHeader_(sh) { setHeader_(sh, ["ts_local", "level", "message", "detail"]); }

function ensureOddsHeader_(sh) {
  setHeader_(sh, [
    "odds_game_id","commence_time_utc","away_team","home_team",
    "away_odds_decimal","home_odds_decimal","away_implied","home_implied",
    "best_book_away","best_book_home","updated_at_local","sport_key_used"
  ]);
}

function ensureScheduleHeader_(sh) {
  setHeader_(sh, [
    "mlb_gamePk","mlb_gameGuid","gameDate_utc","away_team","home_team",
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
    "confidence","bet_side","bet_tier","bet_edge","units","notes","updated_at_local"
  ]);
}

function ensurePlayerMapHeader_(sh) { setHeader_(sh, ["name_variant","canonical_name","mlb_id","razz_id","notes","updated_at_local"]); }
function ensureNotifyStateHeader_(sh) { setHeader_(sh, ["date_key","plays","units","last_updated_local"]); }
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
