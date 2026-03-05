/* ===================== SETTINGS ===================== */

function ensureSettings_(sh) {
  var defaults = [
    ["KEY", "VALUE", "NOTES"],
    ["MODE", "PRESEASON", "PRESEASON or REGULAR"],
    ["ACTIVE_START", "09:00", "Local time window start (HH:MM)"],
    ["ACTIVE_END", "23:30", "Local time window end (HH:MM)"],

    ["ODDS_API_KEY", "", "TheOddsAPI key"],
    ["DISCORD_WEBHOOK", "", "Discord webhook URL (plain text notices)"],
    ["WEB_APP_URL", "", "Apps Script Web App URL for secure Discord action links"],
    ["ACTION_TOKEN_SECRET", "", "Secret used to sign Discord action tokens"],
    ["ACTION_TOKEN_TTL_MIN", "60", "Token TTL in minutes for action links"],

    ["PIPELINE_MINUTES", "15", "Pipeline trigger frequency"],
    ["PIPELINE_DEGRADE_ZERO_STREAK_THRESHOLD", "3", "Consecutive runs with matched==0 OR computed==0 before degraded cadence starts"],
    ["PIPELINE_DEGRADE_LEVEL2_THRESHOLD", "6", "Consecutive zero-data runs before strongest degraded cadence"],
    ["PIPELINE_DEGRADE_MINUTES_L1", "30", "Pipeline cadence minutes while degraded level 1 is active"],
    ["PIPELINE_DEGRADE_MINUTES_L2", "60", "Pipeline cadence minutes while degraded level 2 is active"],

    ["HEARTBEAT_MODE", "DAILY", "OFF / DAILY / HOURLY"],
    ["HEARTBEAT_HOUR", "9", "Daily heartbeat hour (script timezone)"],
    ["HEARTBEAT_MINUTE", "5", "Daily heartbeat minute"],

    ["ODDS_SPORT_KEY_PRESEASON", "baseball_mlb_preseason", ""],
    ["ODDS_SPORT_KEY_REGULAR", "baseball_mlb", ""],
    ["ODDS_SPORT_KEY_OVERRIDE", "", "Optional override key"],

    ["ODDS_REGIONS", "us,us2", "Recommended: us,us2 for better coverage"],
    ["ODDS_MARKETS", "h2h", "Moneyline = h2h"],
    ["ODDS_FORMAT", "decimal", ""],
    ["ODDS_DATE_FORMAT", "iso", ""],
    ["ODDS_REF_BOOK", "", "Optional bookmaker key"],
    ["ODDS_LOOKAHEAD_HOURS", "36", "Odds window now→now+hours"],
    ["ODDS_FALLBACK_ON_EMPTY", "TRUE", "If preseason empty, try regular"],
    ["ODDS_WINDOW_PRE_FIRST_MIN", "60", "Minutes before first local game start to begin odds refresh"],
    ["ODDS_WINDOW_POST_LAST_MIN", "0", "Minutes after last local game start to continue odds refresh"],
    ["ODDS_WINDOW_REFRESH_MIN", "5", "Minimum minutes between odds-window schedule refresh fetch attempts"],
    ["ODDS_WINDOW_FORCE_REFRESH", "FALSE", "TRUE/FALSE to bypass cache freshness and min refresh interval"],
    ["ODDS_NO_GAMES_BEHAVIOR", "SKIP", "SKIP or FALLBACK_STATIC_WINDOW"],

    ["MATCH_TOL_MIN", "360", "Team+time match tolerance (minutes)"],
    ["LINEUP_MIN", "9", "Min hitters per lineup"],
    ["LINEUP_FALLBACK_MODE", "STRICT", "STRICT or FALLBACK"],

    ["PROJ_CACHE_HOURS", "12", "Min hours between projection refetch"],
    ["RAZZ_HIT_URL", "https://razzball.com/steamer-hitter-projections/", "Razzball hitters projections (HTML table)"],
    ["RAZZ_PIT_URL", "https://razzball.com/steamer-pitcher-projections/", "Razzball pitchers projections (HTML table)"],

    ["LEAGUE_AVG_OPS", "0.675", "Fallback OPS"],
    ["DEFAULT_PITCHER_SIERA", "4.20", "Fallback SIERA"],
    ["MODEL_K_OPS", "6.0", "Logit weight for OPS diff"],
    ["MODEL_K_PIT", "3.0", "Logit weight for pitcher factor diff"],
    ["REQUIRE_PITCHER_MATCH", "false", "true/false"],
    ["NOTIFY_MAX_ODDS_AGE_MIN", "45", "Only notify if odds updated within X minutes"],
    ["NOTIFY_COOLDOWN_MIN", "60", "Minimum minutes between Discord sends for the same odds_game_id"],
    ["NOTIFY_MIN_ODDS_MOVE", "0.03", "Minimum decimal odds change required to re-notify"],
    ["NOTIFY_MIN_EDGE_MOVE_PCT", "0.75", "Minimum edge change (percentage points) required to re-notify"],

    ["RS_EDGE_MICRO", "0.020", ""], ["RS_EDGE_SMALL", "0.040", ""], ["RS_EDGE_MED", "0.055", ""], ["RS_EDGE_STRONG", "0.065", ""], ["RS_CONF_MIN", "62", ""],
    ["PS_EDGE_MICRO", "0.018", ""], ["PS_EDGE_SMALL", "0.028", ""], ["PS_EDGE_MED", "0.040", ""], ["PS_EDGE_STRONG", "0.050", ""], ["PS_CONF_MIN", "55", ""],

    ["RS_MAX_PLAYS_DAY", "5", ""], ["RS_MAX_UNITS_DAY", "2.50", ""], ["RS_UNIT_BASE", "0.25", ""], ["RS_UNIT_MICRO_MULT", "0.6", ""], ["RS_UNIT_SMALL_MULT", "1.0", ""], ["RS_UNIT_MED_MULT", "1.3", ""], ["RS_UNIT_STRONG_MULT", "1.6", ""],
    ["PS_MAX_PLAYS_DAY", "6", ""], ["PS_MAX_UNITS_DAY", "2.00", ""], ["PS_UNIT_BASE", "0.20", ""], ["PS_UNIT_MICRO_MULT", "0.6", ""], ["PS_UNIT_SMALL_MULT", "1.0", ""], ["PS_UNIT_MED_MULT", "1.25", ""], ["PS_UNIT_STRONG_MULT", "1.5", ""]
  ];

  var existing = sh.getDataRange().getValues();
  if (existing.length < 1) {
    sh.getRange(1, 1, defaults.length, defaults[0].length).setValues(defaults);
    sh.setFrozenRows(1);
    return;
  }

  var h = existing[0];
  if (String(h[0]) !== "KEY" || String(h[1]) !== "VALUE") {
    sh.clearContents();
    sh.getRange(1, 1, defaults.length, defaults[0].length).setValues(defaults);
    sh.setFrozenRows(1);
    return;
  }

  var keyRow = {};
  for (var i = 1; i < existing.length; i++) {
    var k = String(existing[i][0] || "").trim();
    if (k) keyRow[k] = true;
  }

  for (var d = 1; d < defaults.length; d++) {
    var dk = String(defaults[d][0]);
    if (!keyRow[dk]) sh.appendRow([defaults[d][0], defaults[d][1], defaults[d][2]]);
  }

  sh.setFrozenRows(1);
}

function getConfig_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.SETTINGS);
  var values = sh.getDataRange().getValues();
  var cfg = {};

  for (var i = 1; i < values.length; i++) {
    var key = String(values[i][0] || "").trim();
    if (!key) continue;
    cfg[key] = values[i][1];
  }

  cfg.MODE = String(cfg.MODE || "PRESEASON").toUpperCase();
  cfg.ACTIVE_START = cfg.ACTIVE_START || "09:00";
  cfg.ACTIVE_END = cfg.ACTIVE_END || "23:30";
  cfg.ODDS_API_KEY = String(cfg.ODDS_API_KEY || "").trim();
  cfg.DISCORD_WEBHOOK = String(cfg.DISCORD_WEBHOOK || "").trim();
  cfg.DISCORD_BOT_TOKEN = String(cfg.DISCORD_BOT_TOKEN || "").trim();
  cfg.DISCORD_CHANNEL_ID = String(cfg.DISCORD_CHANNEL_ID || "").trim();
  cfg.WEB_APP_URL = String(cfg.WEB_APP_URL || "").trim();
  cfg.ACTION_TOKEN_SECRET = String(cfg.ACTION_TOKEN_SECRET || "").trim();
  cfg.ACTION_TOKEN_TTL_MIN = toInt_(cfg.ACTION_TOKEN_TTL_MIN, 60);
  cfg.PIPELINE_MINUTES = toInt_(cfg.PIPELINE_MINUTES, 15);
  cfg.PIPELINE_DEGRADE_ZERO_STREAK_THRESHOLD = toInt_(cfg.PIPELINE_DEGRADE_ZERO_STREAK_THRESHOLD, 3);
  cfg.PIPELINE_DEGRADE_LEVEL2_THRESHOLD = toInt_(cfg.PIPELINE_DEGRADE_LEVEL2_THRESHOLD, 6);
  cfg.PIPELINE_DEGRADE_MINUTES_L1 = toInt_(cfg.PIPELINE_DEGRADE_MINUTES_L1, 30);
  cfg.PIPELINE_DEGRADE_MINUTES_L2 = toInt_(cfg.PIPELINE_DEGRADE_MINUTES_L2, 60);
  cfg.HEARTBEAT_MODE = String(cfg.HEARTBEAT_MODE || "DAILY").toUpperCase();
  cfg.HEARTBEAT_HOUR = toInt_(cfg.HEARTBEAT_HOUR, 9);
  cfg.HEARTBEAT_MINUTE = toInt_(cfg.HEARTBEAT_MINUTE, 5);
  cfg.ODDS_SPORT_KEY_PRESEASON = String(cfg.ODDS_SPORT_KEY_PRESEASON || "baseball_mlb_preseason");
  cfg.ODDS_SPORT_KEY_REGULAR = String(cfg.ODDS_SPORT_KEY_REGULAR || "baseball_mlb");
  cfg.ODDS_SPORT_KEY_OVERRIDE = String(cfg.ODDS_SPORT_KEY_OVERRIDE || "").trim();
  cfg.ODDS_REGIONS = String(cfg.ODDS_REGIONS || "us,us2");
  cfg.ODDS_MARKETS = String(cfg.ODDS_MARKETS || "h2h");
  cfg.ODDS_FORMAT = String(cfg.ODDS_FORMAT || "decimal");
  cfg.ODDS_DATE_FORMAT = String(cfg.ODDS_DATE_FORMAT || "iso");
  cfg.ODDS_REF_BOOK = String(cfg.ODDS_REF_BOOK || "");
  cfg.ODDS_LOOKAHEAD_HOURS = toFloat_(cfg.ODDS_LOOKAHEAD_HOURS, 36);
  cfg.ODDS_FALLBACK_ON_EMPTY = String(cfg.ODDS_FALLBACK_ON_EMPTY || "TRUE").toUpperCase() === "TRUE";
  cfg.ODDS_WINDOW_PRE_FIRST_MIN = toInt_(cfg.ODDS_WINDOW_PRE_FIRST_MIN, 60);
  cfg.ODDS_WINDOW_POST_LAST_MIN = toInt_(cfg.ODDS_WINDOW_POST_LAST_MIN, 0);
  cfg.ODDS_WINDOW_REFRESH_MIN = toInt_(cfg.ODDS_WINDOW_REFRESH_MIN, 5);
  cfg.ODDS_WINDOW_FORCE_REFRESH = String(cfg.ODDS_WINDOW_FORCE_REFRESH || "FALSE").toUpperCase() === "TRUE";
  cfg.ODDS_NO_GAMES_BEHAVIOR = String(cfg.ODDS_NO_GAMES_BEHAVIOR || "SKIP").toUpperCase();
  cfg.MATCH_TOL_MIN = toInt_(cfg.MATCH_TOL_MIN, 360);
  cfg.LINEUP_MIN = toInt_(cfg.LINEUP_MIN, 9);
  cfg.LINEUP_FALLBACK_MODE = String(cfg.LINEUP_FALLBACK_MODE || "STRICT").toUpperCase();
  cfg.PROJ_CACHE_HOURS = toFloat_(cfg.PROJ_CACHE_HOURS, 12);
  cfg.RAZZ_HIT_URL = String(cfg.RAZZ_HIT_URL || "").trim();
  cfg.RAZZ_PIT_URL = String(cfg.RAZZ_PIT_URL || "").trim();
  cfg.LEAGUE_AVG_OPS = toFloat_(cfg.LEAGUE_AVG_OPS, 0.675);
  cfg.DEFAULT_PITCHER_SIERA = toFloat_(cfg.DEFAULT_PITCHER_SIERA, 4.20);
  cfg.MODEL_K_OPS = toFloat_(cfg.MODEL_K_OPS, 6.0);
  cfg.MODEL_K_PIT = toFloat_(cfg.MODEL_K_PIT, 3.0);
  cfg.REQUIRE_PITCHER_MATCH = String(cfg.REQUIRE_PITCHER_MATCH || "false").toLowerCase() === "true";
  cfg.NOTIFY_MAX_ODDS_AGE_MIN = toFloat_(cfg.NOTIFY_MAX_ODDS_AGE_MIN, 45);
  cfg.NOTIFY_COOLDOWN_MIN = toFloat_(cfg.NOTIFY_COOLDOWN_MIN, 60);
  cfg.NOTIFY_MIN_ODDS_MOVE = toFloat_(cfg.NOTIFY_MIN_ODDS_MOVE, 0.03);
  cfg.NOTIFY_MIN_EDGE_MOVE_PCT = toFloat_(cfg.NOTIFY_MIN_EDGE_MOVE_PCT, 0.75);

  return cfg;
}

function chooseSportKey_(cfg) {
  if (cfg.ODDS_SPORT_KEY_OVERRIDE) return cfg.ODDS_SPORT_KEY_OVERRIDE;
  return (cfg.MODE === "PRESEASON") ? cfg.ODDS_SPORT_KEY_PRESEASON : cfg.ODDS_SPORT_KEY_REGULAR;
}

function withinActiveHours_(cfg) {
  var now = new Date();
  var nowMin = minutesSinceMidnightLocal_(now);
  var startMin = minutesFromSetting_(cfg.ACTIVE_START);
  var endMin = minutesFromSetting_(cfg.ACTIVE_END);
  if (startMin < 0 || endMin < 0) return true;
  if (endMin >= startMin) return (nowMin >= startMin && nowMin <= endMin);
  return (nowMin >= startMin || nowMin <= endMin);
}

function minutesSinceMidnightLocal_(dt) {
  var hh = Number(Utilities.formatDate(dt, TZ, "H"));
  var mm = Number(Utilities.formatDate(dt, TZ, "m"));
  return hh * 60 + mm;
}

function minutesFromSetting_(v) {
  if (Object.prototype.toString.call(v) === "[object Date]") {
    var hh = Number(Utilities.formatDate(v, TZ, "H"));
    var mm = Number(Utilities.formatDate(v, TZ, "m"));
    return hh * 60 + mm;
  }
  if (typeof v === "number") {
    if (v >= 0 && v <= 1) return Math.round(v * 24 * 60);
  }
  var s = String(v || "").trim();
  if (!s) return -1;
  var m = s.match(/(\d{1,2}):(\d{2})/);
  if (m && m.length >= 3) return Number(m[1]) * 60 + Number(m[2]);
  return -1;
}

/* ===================== LOGGING + SHEET UTILS ===================== */

function log_(level, message, detailObj) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.LOG) || getOrCreateSheet_(ss, SH.LOG);
  if (sh.getLastRow() === 0) ensureLogHeader_(sh);

  var detail = detailObj ? JSON.stringify(detailObj) : "";
  if (detail.length > 1800) detail = detail.slice(0, 1800) + "…(trimmed)";
  sh.appendRow([isoLocalWithOffset_(new Date()), level, message, detail]);
}

function getOrCreateSheet_(ss, name) { var sh = ss.getSheetByName(name); if (!sh) sh = ss.insertSheet(name); return sh; }

function setHeader_(sh, headerArr) {
  var lastRow = sh.getLastRow();
  var lastCol = sh.getLastColumn();
  var existing = [];
  if (lastRow >= 1 && lastCol >= 1) existing = sh.getRange(1, 1, 1, lastCol).getValues()[0];

  var matches = (existing.length >= headerArr.length);
  if (matches) {
    for (var i = 0; i < headerArr.length; i++) {
      if (String(existing[i]) !== String(headerArr[i])) { matches = false; break; }
    }
  }

  if (!matches) {
    sh.clearContents();
    sh.getRange(1, 1, 1, headerArr.length).setValues([headerArr]);
    sh.setFrozenRows(1);
  }
}

function replaceSheetBody_(sh, rows) {
  var lastRow = sh.getLastRow();
  if (lastRow > 1) sh.getRange(2, 1, lastRow - 1, sh.getLastColumn()).clearContent();
  if (!rows || rows.length === 0) return;
  sh.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
}

function readSheetAsObjects_(sheet) {
  var v = sheet.getDataRange().getValues();
  if (!v || v.length < 2) return [];
  var header = v[0].map(function (x) { return String(x || "").trim(); });
  var out = [];
  for (var i = 1; i < v.length; i++) {
    var row = v[i];
    var blank = true;
    for (var c = 0; c < row.length; c++) {
      if (String(row[c] || "").trim() !== "") { blank = false; break; }
    }
    if (blank) continue;
    var obj = {};
    for (var j = 0; j < header.length; j++) obj[header[j]] = row[j];
    obj.odds_game_id = obj.odds_game_id || obj["odds_game_id"];
    obj.commence_time_utc = obj.commence_time_utc || obj["commence_time_utc"];
    obj.away_team = obj.away_team || obj["away_team"];
    obj.home_team = obj.home_team || obj["home_team"];
    obj.away_odds_decimal = obj.away_odds_decimal || obj["away_odds_decimal"];
    obj.home_odds_decimal = obj.home_odds_decimal || obj["home_odds_decimal"];
    obj.away_implied = obj.away_implied || obj["away_implied"];
    obj.home_implied = obj.home_implied || obj["home_implied"];
    obj.updated_at_local = obj.updated_at_local || obj["updated_at_local"];
    obj.mlb_gamePk = obj.mlb_gamePk || obj["mlb_gamePk"];
    obj.gameDate_utc = obj.gameDate_utc || obj["gameDate_utc"];
    obj.away_probable_pitcher = obj.away_probable_pitcher || obj["away_probable_pitcher"];
    obj.home_probable_pitcher = obj.home_probable_pitcher || obj["home_probable_pitcher"];
    obj.is_confirmed = obj.is_confirmed || obj["is_confirmed"];
    obj.player_name = obj.player_name || obj["player_name"];
    obj.bat_order = obj.bat_order || obj["bat_order"];
    obj.side = obj.side || obj["side"];
    out.push(obj);
  }
  return out;
}

/* ===================== SMALL UTILS ===================== */

function mapToString_(arr) { var out = []; for (var i = 0; i < arr.length; i++) out.push(String(arr[i])); return out; }
function indexOf_(arr, val) { for (var i = 0; i < arr.length; i++) if (String(arr[i]) === String(val)) return i; return -1; }
function toInt_(v, def) { var n = parseInt(v, 10); return isFinite(n) ? n : def; }
function toFloat_(v, def) { var n = parseFloat(v); return isFinite(n) ? n : def; }
function clampInt_(n, lo, hi) { n = parseInt(n, 10); if (!isFinite(n)) return lo; return Math.max(lo, Math.min(hi, n)); }
function pad2_(n) { n = Number(n); return (n < 10 ? "0" : "") + String(n); }
function round_(x, d) { var n = Number(x); if (!isFinite(n)) return ""; var p = Math.pow(10, d); return Math.round(n * p) / p; }
function normalizeTeam_(name) { return String(name || "").toLowerCase().replace(/\s+/g, " ").trim(); }
