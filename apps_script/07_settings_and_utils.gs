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
    ["PIPELINE_CREDIT_WARNING_THRESHOLD", "75", "Apply slower cadence when remaining credits fall below this threshold"],
    ["PIPELINE_CREDIT_CRITICAL_THRESHOLD", "25", "Apply very slow cadence when remaining credits are near depletion"],
    ["PIPELINE_DEGRADE_MINUTES_CREDIT_WARNING", "30", "Pipeline cadence minutes while credit pressure warning is active"],
    ["PIPELINE_DEGRADE_MINUTES_CREDIT_CRITICAL", "60", "Pipeline cadence minutes while credit pressure is critical"],
    ["PIPELINE_STAGE_WARN_SEC", "20", "Warn when a pipeline stage takes longer than this many seconds"],
    ["PIPELINE_STAGE_WARN_ODDS_FETCH_SEC", "30", "Warn when odds fetch stage exceeds this many seconds"],
    ["PIPELINE_STAGE_WARN_MODEL_SEC", "30", "Warn when model stage exceeds this many seconds"],
    ["PIPELINE_STAGE_DURATION_EMA_ALPHA", "0.2", "EMA alpha (0-1] for per-stage duration moving averages"],
    ["PIPELINE_STAGE_DRIFT_SPIKE_MULTIPLIER", "2.0", "Drift spike when duration exceeds moving average by this multiplier"],
    ["PIPELINE_STAGE_DRIFT_SPIKE_MIN_MS", "5000", "Minimum absolute ms over moving average to classify as drift spike"],

    ["HEARTBEAT_MODE", "DAILY", "OFF / DAILY / HOURLY"],
    ["HEARTBEAT_HOUR", "9", "Daily heartbeat hour (script timezone)"],
    ["HEARTBEAT_MINUTE", "5", "Daily heartbeat minute"],

    ["ODDS_SPORT_KEY_PRESEASON", "baseball_mlb_preseason", ""],
    ["ODDS_SPORT_KEY_REGULAR", "baseball_mlb", ""],
    ["ODDS_SPORT_KEY_OVERRIDE", "", "Optional override key"],

    ["ODDS_REGIONS", "us,us2", "Recommended: us,us2 for better coverage"],
    ["ODDS_USAGE_PROFILE", "NORMAL", "NORMAL or LOW_CREDIT"],
    ["ODDS_LOW_CREDIT_REGIONS", "us", "LOW_CREDIT override for ODDS_REGIONS"],
    ["ODDS_LOW_CREDIT_LOOKAHEAD_HOURS", "12", "LOW_CREDIT override for ODDS_LOOKAHEAD_HOURS (recommended 12-18)"],
    ["ODDS_LOW_CREDIT_BOOKMAKERS", "", "Optional LOW_CREDIT bookmakers CSV passed via bookmakers="],
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
    ["ODDS_SCHEDULE_QUERY_BUFFER_BEFORE_H", "24", "Hours before min odds commence_time_utc when querying MLB schedule"],
    ["ODDS_SCHEDULE_QUERY_BUFFER_AFTER_H", "24", "Hours after max odds commence_time_utc when querying MLB schedule"],
    ["ODDS_ALERT_REMAINING_THRESHOLD", "75", "Warn when Odds API remaining credits are <= this value"],
    ["ODDS_ALERT_COOLDOWN_MIN", "180", "Minimum minutes between low-credit alerts while remaining is low"],
    ["ODDS_ALERT_ON_EVERY_CALL_UNDER_THRESHOLD", "FALSE", "TRUE/FALSE: send warning on every low-credit call (ignores cooldown)"],
    ["ODDS_MIN_REMAINING_TO_FETCH", "20", "Skip odds fetch when remaining credits are below this threshold"],
    ["ODDS_FETCH_BLOCK_MIN", "120", "Cooldown minutes to skip odds fetch after low-credits block engages"],
    ["ODDS_CREDITS_SNAPSHOT_MAX_AGE_MIN", "180", "Max age in minutes for persisted credits snapshot before allowing a probe fetch"],

    ["MATCH_TOL_MIN", "360", "Team+time match tolerance (minutes)"],
    ["LINEUP_MIN", "9", "Min hitters per lineup"],
    ["LINEUP_FALLBACK_MODE", "STRICT", "STRICT or FALLBACK"],
    ["LINEUP_PA_W_1", "1.12", "Expected PA weight for lineup slot 1"],
    ["LINEUP_PA_W_2", "1.08", "Expected PA weight for lineup slot 2"],
    ["LINEUP_PA_W_3", "1.05", "Expected PA weight for lineup slot 3"],
    ["LINEUP_PA_W_4", "1.03", "Expected PA weight for lineup slot 4"],
    ["LINEUP_PA_W_5", "1.00", "Expected PA weight for lineup slot 5"],
    ["LINEUP_PA_W_6", "0.97", "Expected PA weight for lineup slot 6"],
    ["LINEUP_PA_W_7", "0.93", "Expected PA weight for lineup slot 7"],
    ["LINEUP_PA_W_8", "0.91", "Expected PA weight for lineup slot 8"],
    ["LINEUP_PA_W_9", "0.91", "Expected PA weight for lineup slot 9"],

    ["PROJ_CACHE_HOURS", "12", "Min hours between projection refetch"],
    ["RAZZ_HIT_URL", "https://razzball.com/steamer-hitter-projections/", "Razzball hitters projections (HTML table)"],
    ["RAZZ_PIT_URL", "https://razzball.com/steamer-pitcher-projections/", "Razzball pitchers projections (HTML table)"],

    ["LEAGUE_AVG_OPS", "0.675", "Fallback OPS"],
    ["DEFAULT_PITCHER_SIERA", "4.20", "Fallback SIERA"],
    ["MODEL_K_OPS", "6.0", "Logit weight for OPS diff"],
    ["MODEL_K_PIT", "3.0", "Logit weight for pitcher factor diff"],
    ["BULLPEN_USAGE_DAYS", "4", "Recent days of bullpen logs (recommended 3-5)"],
    ["MODEL_BULLPEN_SHARE", "0.42", "Share of run-prevention input allocated to bullpen"],
    ["REQUIRE_PITCHER_MATCH", "false", "true/false"],
    ["NOTIFY_MAX_ODDS_AGE_MIN", "45", "Only notify if odds updated within X minutes"],
    ["NOTIFY_COOLDOWN_MIN", "60", "Minimum minutes between Discord sends for the same odds_game_id"],
    ["NOTIFY_MIN_ODDS_MOVE", "0.03", "Minimum decimal odds change required to re-notify"],
    ["NOTIFY_MIN_EDGE_MOVE_PCT", "0.75", "Minimum edge change (percentage points) required to re-notify"],
    ["ENABLE_BET_TRACKING", "FALSE", "TRUE/FALSE: legacy toggle (also requires LEGACY_BET_TRACKING_ALLOW_REENABLE=true in constants)"],
    ["ENABLE_SIGNAL_CLOSE_UPDATER", "FALSE", "TRUE/FALSE: enable time-based close/CLV stamp updates for SIGNAL_LOG"],
    ["SIGNAL_CLOSE_UPDATER_MINUTES", "30", "Cadence in minutes for SIGNAL_LOG close/CLV updater trigger"],
    ["SIGNAL_CLOSE_PRESTART_MIN", "15", "Allow close stamping starting this many minutes before first pitch"],

    ["EXT_FEATURES_ENABLE_WEATHER", "FALSE", "Enable weather external feature ingestion"],
    ["EXT_FEATURES_ENABLE_BULLPEN", "FALSE", "Enable bullpen external feature ingestion"],
    ["EXT_FEATURES_ENABLE_EXPERIMENTAL", "FALSE", "Master switch for experimental external features"],
    ["EXT_FEATURES_ENABLE_MARKET", "FALSE", "Enable market-based experimental features"],
    ["EXT_FEATURES_ENABLE_STATCAST", "FALSE", "Enable statcast-like experimental features"],
    ["EXT_FEATURES_PROVIDER_WEATHER", "NOAA", "Weather provider selector"],
    ["EXT_FEATURES_PROVIDER_BULLPEN", "INTERNAL", "Bullpen provider selector"],
    ["EXT_FEATURES_PROVIDER_MARKET", "INTERNAL", "Market provider selector"],
    ["EXT_FEATURES_PROVIDER_STATCAST", "INTERNAL", "Statcast provider selector"],
    ["EXT_FEATURES_TTL_WEATHER_MIN", "30", "Cache freshness TTL in minutes for weather source"],
    ["EXT_FEATURES_TTL_BULLPEN_MIN", "20", "Cache freshness TTL in minutes for bullpen source"],
    ["EXT_FEATURES_TTL_MARKET_MIN", "15", "Cache freshness TTL in minutes for market source"],
    ["EXT_FEATURES_TTL_STATCAST_MIN", "60", "Cache freshness TTL in minutes for statcast source"],
    ["EXT_FEATURES_FORCE_REFRESH", "FALSE", "TRUE/FALSE to bypass external feature cache freshness"],
    ["EXT_FEATURES_DEBUG", "FALSE", "TRUE/FALSE to emit verbose external feature diagnostics"],
    ["EXT_FEATURE_WEIGHT_WEATHER_RUN_ENV", "0.18", "Weather adjustment weight on run-environment input"],
    ["EXT_FEATURE_WEIGHT_BULLPEN_RUN_PREV", "0.14", "Bullpen adjustment weight on run-prevention input"],
    ["EXT_FEATURE_WEIGHT_MARKET", "0.06", "Experimental market feature influence"],
    ["EXT_FEATURE_WEIGHT_STATCAST", "0.05", "Experimental statcast-like feature influence"],
    ["EXT_FEATURES_EXPERIMENTAL_FAIL_THRESHOLD", "3", "Consecutive source failures before temporary disable"],
    ["EXT_FEATURES_EXPERIMENTAL_DISABLE_MIN", "120", "Minutes to keep experimental source disabled after breaker trips"],
    ["CALIBRATION_WINDOW_DAYS", "30", "Days of snapshots to include in calibration report"],
    ["CALIBRATION_EDGE_BUCKETS", "0,0.02,0.04,0.06,0.10", "Absolute edge bucket boundaries for calibration summaries"],

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
  cfg.PIPELINE_CREDIT_WARNING_THRESHOLD = Math.max(0, toInt_(cfg.PIPELINE_CREDIT_WARNING_THRESHOLD, 75));
  cfg.PIPELINE_CREDIT_CRITICAL_THRESHOLD = Math.max(0, toInt_(cfg.PIPELINE_CREDIT_CRITICAL_THRESHOLD, 25));
  cfg.PIPELINE_DEGRADE_MINUTES_CREDIT_WARNING = toInt_(cfg.PIPELINE_DEGRADE_MINUTES_CREDIT_WARNING, 30);
  cfg.PIPELINE_DEGRADE_MINUTES_CREDIT_CRITICAL = toInt_(cfg.PIPELINE_DEGRADE_MINUTES_CREDIT_CRITICAL, 60);
  cfg.PIPELINE_STAGE_WARN_SEC = Math.max(1, toFloat_(cfg.PIPELINE_STAGE_WARN_SEC, 20));
  cfg.PIPELINE_STAGE_WARN_ODDS_FETCH_SEC = Math.max(1, toFloat_(cfg.PIPELINE_STAGE_WARN_ODDS_FETCH_SEC, 30));
  cfg.PIPELINE_STAGE_WARN_MODEL_SEC = Math.max(1, toFloat_(cfg.PIPELINE_STAGE_WARN_MODEL_SEC, 30));
  cfg.PIPELINE_STAGE_DURATION_EMA_ALPHA = clamp_(0.01, 1, toFloat_(cfg.PIPELINE_STAGE_DURATION_EMA_ALPHA, 0.2));
  cfg.PIPELINE_STAGE_DRIFT_SPIKE_MULTIPLIER = Math.max(1.1, toFloat_(cfg.PIPELINE_STAGE_DRIFT_SPIKE_MULTIPLIER, 2.0));
  cfg.PIPELINE_STAGE_DRIFT_SPIKE_MIN_MS = Math.max(500, toInt_(cfg.PIPELINE_STAGE_DRIFT_SPIKE_MIN_MS, 5000));
  cfg.HEARTBEAT_MODE = String(cfg.HEARTBEAT_MODE || "DAILY").toUpperCase();
  cfg.HEARTBEAT_HOUR = toInt_(cfg.HEARTBEAT_HOUR, 9);
  cfg.HEARTBEAT_MINUTE = toInt_(cfg.HEARTBEAT_MINUTE, 5);
  cfg.ODDS_SPORT_KEY_PRESEASON = String(cfg.ODDS_SPORT_KEY_PRESEASON || "baseball_mlb_preseason");
  cfg.ODDS_SPORT_KEY_REGULAR = String(cfg.ODDS_SPORT_KEY_REGULAR || "baseball_mlb");
  cfg.ODDS_SPORT_KEY_OVERRIDE = String(cfg.ODDS_SPORT_KEY_OVERRIDE || "").trim();
  cfg.ODDS_REGIONS = String(cfg.ODDS_REGIONS || "us,us2");
  cfg.ODDS_USAGE_PROFILE = String(cfg.ODDS_USAGE_PROFILE || "NORMAL").toUpperCase();
  if (cfg.ODDS_USAGE_PROFILE !== "LOW_CREDIT") cfg.ODDS_USAGE_PROFILE = "NORMAL";
  cfg.ODDS_LOW_CREDIT_REGIONS = String(cfg.ODDS_LOW_CREDIT_REGIONS || "us").trim();
  cfg.ODDS_LOW_CREDIT_LOOKAHEAD_HOURS = Math.max(1, toFloat_(cfg.ODDS_LOW_CREDIT_LOOKAHEAD_HOURS, 12));
  cfg.ODDS_LOW_CREDIT_BOOKMAKERS = String(cfg.ODDS_LOW_CREDIT_BOOKMAKERS || "").trim();
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
  cfg.ODDS_SCHEDULE_QUERY_BUFFER_BEFORE_H = Math.max(0, toFloat_(cfg.ODDS_SCHEDULE_QUERY_BUFFER_BEFORE_H, 24));
  cfg.ODDS_SCHEDULE_QUERY_BUFFER_AFTER_H = Math.max(0, toFloat_(cfg.ODDS_SCHEDULE_QUERY_BUFFER_AFTER_H, 24));
  cfg.ODDS_ALERT_REMAINING_THRESHOLD = Math.max(0, toInt_(cfg.ODDS_ALERT_REMAINING_THRESHOLD, 75));
  cfg.ODDS_ALERT_COOLDOWN_MIN = Math.max(0, toInt_(cfg.ODDS_ALERT_COOLDOWN_MIN, 180));
  cfg.ODDS_ALERT_ON_EVERY_CALL_UNDER_THRESHOLD = String(cfg.ODDS_ALERT_ON_EVERY_CALL_UNDER_THRESHOLD || "FALSE").toUpperCase() === "TRUE";
  cfg.ODDS_MIN_REMAINING_TO_FETCH = Math.max(0, toInt_(cfg.ODDS_MIN_REMAINING_TO_FETCH, 20));
  cfg.ODDS_FETCH_BLOCK_MIN = Math.max(1, toInt_(cfg.ODDS_FETCH_BLOCK_MIN, 120));
  cfg.ODDS_CREDITS_SNAPSHOT_MAX_AGE_MIN = Math.max(1, toInt_(cfg.ODDS_CREDITS_SNAPSHOT_MAX_AGE_MIN, 180));
  cfg.MATCH_TOL_MIN = toInt_(cfg.MATCH_TOL_MIN, 360);
  cfg.ODDS_TEAM_MATCH_FALLBACK_ENABLE = String(cfg.ODDS_TEAM_MATCH_FALLBACK_ENABLE || "TRUE").toUpperCase() === "TRUE";
  cfg.LINEUP_MIN = toInt_(cfg.LINEUP_MIN, 9);
  cfg.LINEUP_FALLBACK_MODE = String(cfg.LINEUP_FALLBACK_MODE || "STRICT").toUpperCase();
  cfg.LINEUP_PA_WEIGHTS = [
    toFloat_(cfg.LINEUP_PA_W_1, 1.12),
    toFloat_(cfg.LINEUP_PA_W_2, 1.08),
    toFloat_(cfg.LINEUP_PA_W_3, 1.05),
    toFloat_(cfg.LINEUP_PA_W_4, 1.03),
    toFloat_(cfg.LINEUP_PA_W_5, 1.00),
    toFloat_(cfg.LINEUP_PA_W_6, 0.97),
    toFloat_(cfg.LINEUP_PA_W_7, 0.93),
    toFloat_(cfg.LINEUP_PA_W_8, 0.91),
    toFloat_(cfg.LINEUP_PA_W_9, 0.91)
  ];
  cfg.PROJ_CACHE_HOURS = toFloat_(cfg.PROJ_CACHE_HOURS, 12);
  cfg.RAZZ_HIT_URL = String(cfg.RAZZ_HIT_URL || "").trim();
  cfg.RAZZ_PIT_URL = String(cfg.RAZZ_PIT_URL || "").trim();
  cfg.LEAGUE_AVG_OPS = toFloat_(cfg.LEAGUE_AVG_OPS, 0.675);
  cfg.DEFAULT_PITCHER_SIERA = toFloat_(cfg.DEFAULT_PITCHER_SIERA, 4.20);
  cfg.MODEL_K_OPS = toFloat_(cfg.MODEL_K_OPS, 6.0);
  cfg.MODEL_K_PIT = toFloat_(cfg.MODEL_K_PIT, 3.0);
  cfg.BULLPEN_USAGE_DAYS = toInt_(cfg.BULLPEN_USAGE_DAYS, 4);
  cfg.MODEL_BULLPEN_SHARE = toFloat_(cfg.MODEL_BULLPEN_SHARE, 0.42);
  cfg.REQUIRE_PITCHER_MATCH = String(cfg.REQUIRE_PITCHER_MATCH || "false").toLowerCase() === "true";
  cfg.NOTIFY_MAX_ODDS_AGE_MIN = toFloat_(cfg.NOTIFY_MAX_ODDS_AGE_MIN, 45);
  cfg.NOTIFY_COOLDOWN_MIN = toFloat_(cfg.NOTIFY_COOLDOWN_MIN, 60);
  cfg.NOTIFY_MIN_ODDS_MOVE = toFloat_(cfg.NOTIFY_MIN_ODDS_MOVE, 0.03);
  cfg.NOTIFY_MIN_EDGE_MOVE_PCT = toFloat_(cfg.NOTIFY_MIN_EDGE_MOVE_PCT, 0.75);
  cfg.ENABLE_BET_TRACKING = String(cfg.ENABLE_BET_TRACKING || "FALSE").toUpperCase() === "TRUE";
  cfg.ENABLE_SIGNAL_CLOSE_UPDATER = String(cfg.ENABLE_SIGNAL_CLOSE_UPDATER || "FALSE").toUpperCase() === "TRUE";
  cfg.SIGNAL_CLOSE_UPDATER_MINUTES_REQUESTED = parseCadenceMinutesSetting_(cfg.SIGNAL_CLOSE_UPDATER_MINUTES, 30, "SIGNAL_CLOSE_UPDATER_MINUTES");
  cfg.SIGNAL_CLOSE_UPDATER_MINUTES = normalizePipelineTriggerCadenceMinutes_(cfg.SIGNAL_CLOSE_UPDATER_MINUTES_REQUESTED);
  cfg.SIGNAL_CLOSE_PRESTART_MIN = Math.max(0, toInt_(cfg.SIGNAL_CLOSE_PRESTART_MIN, 15));
  cfg.CALIBRATION_WINDOW_DAYS = Math.max(7, toInt_(cfg.CALIBRATION_WINDOW_DAYS, 30));
  cfg.CALIBRATION_EDGE_BUCKETS = parseNumberList_(cfg.CALIBRATION_EDGE_BUCKETS, [0, 0.02, 0.04, 0.06, 0.10]);

  cfg.EXT_FEATURES_ENABLE_WEATHER = String(cfg.EXT_FEATURES_ENABLE_WEATHER || "FALSE").toUpperCase() === "TRUE";
  cfg.EXT_FEATURES_ENABLE_BULLPEN = String(cfg.EXT_FEATURES_ENABLE_BULLPEN || "FALSE").toUpperCase() === "TRUE";
  cfg.EXT_FEATURES_ENABLE_EXPERIMENTAL = String(cfg.EXT_FEATURES_ENABLE_EXPERIMENTAL || "FALSE").toUpperCase() === "TRUE";
  cfg.EXT_FEATURES_ENABLE_MARKET = String(cfg.EXT_FEATURES_ENABLE_MARKET || "FALSE").toUpperCase() === "TRUE";
  cfg.EXT_FEATURES_ENABLE_STATCAST = String(cfg.EXT_FEATURES_ENABLE_STATCAST || "FALSE").toUpperCase() === "TRUE";
  cfg.EXT_FEATURES_PROVIDER_WEATHER = String(cfg.EXT_FEATURES_PROVIDER_WEATHER || "NOAA").toUpperCase();
  cfg.EXT_FEATURES_PROVIDER_BULLPEN = String(cfg.EXT_FEATURES_PROVIDER_BULLPEN || "INTERNAL").toUpperCase();
  cfg.EXT_FEATURES_PROVIDER_MARKET = String(cfg.EXT_FEATURES_PROVIDER_MARKET || "INTERNAL").toUpperCase();
  cfg.EXT_FEATURES_PROVIDER_STATCAST = String(cfg.EXT_FEATURES_PROVIDER_STATCAST || "INTERNAL").toUpperCase();
  cfg.EXT_FEATURES_TTL_WEATHER_MIN = Math.max(5, toInt_(cfg.EXT_FEATURES_TTL_WEATHER_MIN, 30));
  cfg.EXT_FEATURES_TTL_BULLPEN_MIN = Math.max(5, toInt_(cfg.EXT_FEATURES_TTL_BULLPEN_MIN, 20));
  cfg.EXT_FEATURES_TTL_MARKET_MIN = Math.max(5, toInt_(cfg.EXT_FEATURES_TTL_MARKET_MIN, 15));
  cfg.EXT_FEATURES_TTL_STATCAST_MIN = Math.max(5, toInt_(cfg.EXT_FEATURES_TTL_STATCAST_MIN, 60));
  cfg.EXT_FEATURES_FORCE_REFRESH = String(cfg.EXT_FEATURES_FORCE_REFRESH || "FALSE").toUpperCase() === "TRUE";
  cfg.EXT_FEATURES_DEBUG = String(cfg.EXT_FEATURES_DEBUG || "FALSE").toUpperCase() === "TRUE";
  cfg.EXT_FEATURE_WEIGHT_WEATHER_RUN_ENV = Math.max(0, Math.min(0.35, toFloat_(cfg.EXT_FEATURE_WEIGHT_WEATHER_RUN_ENV, 0.18)));
  cfg.EXT_FEATURE_WEIGHT_BULLPEN_RUN_PREV = Math.max(0, Math.min(0.35, toFloat_(cfg.EXT_FEATURE_WEIGHT_BULLPEN_RUN_PREV, 0.14)));
  cfg.EXT_FEATURE_WEIGHT_MARKET = Math.max(0, Math.min(0.20, toFloat_(cfg.EXT_FEATURE_WEIGHT_MARKET, 0.06)));
  cfg.EXT_FEATURE_WEIGHT_STATCAST = Math.max(0, Math.min(0.20, toFloat_(cfg.EXT_FEATURE_WEIGHT_STATCAST, 0.05)));
  cfg.EXT_FEATURES_EXPERIMENTAL_FAIL_THRESHOLD = Math.max(1, toInt_(cfg.EXT_FEATURES_EXPERIMENTAL_FAIL_THRESHOLD, 3));
  cfg.EXT_FEATURES_EXPERIMENTAL_DISABLE_MIN = Math.max(15, toInt_(cfg.EXT_FEATURES_EXPERIMENTAL_DISABLE_MIN, 120));

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

function canonicalReasonCode_(providedCode, message, level) {
  var code = String(providedCode || "").trim();
  var known = {};
  known[REASON_CODE.ODDS_SKIP] = true;
  known[REASON_CODE.SCHEDULE_FALLBACK] = true;
  known[REASON_CODE.MODEL_SKIP] = true;
  known[REASON_CODE.NOTIFY_SKIP] = true;
  known[REASON_CODE.CADENCE_CHANGE] = true;
  known[REASON_CODE.BLOCKER_STATE] = true;
  if (known[code]) return code;

  var msg = String(message || "").toLowerCase();
  var lvl = String(level || "").toUpperCase();
  var token = (code || msg).toLowerCase();

  if (token.indexOf("cadence") >= 0) return REASON_CODE.CADENCE_CHANGE;
  if (token.indexOf("fallback") >= 0 || token.indexOf("schedule") >= 0) return REASON_CODE.SCHEDULE_FALLBACK;
  if (token.indexOf("notify") >= 0 || token.indexOf("delivery") >= 0 || token.indexOf("discord") >= 0) return REASON_CODE.NOTIFY_SKIP;
  if (token.indexOf("model") >= 0 || token.indexOf("projection") >= 0) return REASON_CODE.MODEL_SKIP;
  if (token.indexOf("odds") >= 0) return REASON_CODE.ODDS_SKIP;
  if (token.indexOf("block") >= 0 || token.indexOf("lock") >= 0 || token.indexOf("debounce") >= 0) return REASON_CODE.BLOCKER_STATE;
  if (lvl === "WARN" || lvl === "ERROR") return REASON_CODE.BLOCKER_STATE;
  return "";
}

function log_(level, message, detailObj) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.LOG) || getOrCreateSheet_(ss, SH.LOG);
  if (sh.getLastRow() === 0) ensureLogHeader_(sh);

  var detailData = detailObj || {};
  var nonHappy = /skipp|fail|fallback|block|degrad|error/i.test(String(message || "")) || String(level || "").toUpperCase() === "WARN" || String(level || "").toUpperCase() === "ERROR";
  if (nonHappy) {
    var providedCode = detailData.reason_code || detailData.reasonCode || detailData.reason;
    var canonicalCode = canonicalReasonCode_(providedCode, message, level);
    if (canonicalCode) detailData.reason_code = canonicalCode;

    var reasonDetail = detailData.reason_detail || detailData.reasonDetail || "";
    if (!reasonDetail && providedCode && String(providedCode) !== String(canonicalCode || "")) {
      detailData.reason_detail = String(providedCode);
    }
  }

  var detail = (detailObj || nonHappy) ? JSON.stringify(detailData) : "";
  if (detail.length > 1800) detail = detail.slice(0, 1800) + "…(trimmed)";
  sh.appendRow([isoLocalWithOffset_(new Date()), level, message, detail]);
}

function getLogDataRowStart_() { return 2; }

function getCurrentLogRowCount_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.LOG) || getOrCreateSheet_(ss, SH.LOG);
  if (sh.getLastRow() === 0) ensureLogHeader_(sh);
  return Math.max(0, sh.getLastRow() - 1);
}

function enforceRunSummaryRetention_(ss, archiveEnabled) {
  var summarySheet = ss.getSheetByName(SH.RUN_SUMMARY_LOG) || getOrCreateSheet_(ss, SH.RUN_SUMMARY_LOG);
  if (summarySheet.getLastRow() === 0) ensureRunSummaryLogHeader_(summarySheet);

  var maxRows = Math.max(1, toInt_(RUN_SUMMARY_RETENTION_MAX_ROWS, 2000));
  var dataRows = Math.max(0, summarySheet.getLastRow() - 1);
  var overflow = dataRows - maxRows;
  if (overflow <= 0) return;

  var rowsToArchive = summarySheet.getRange(2, 1, overflow, summarySheet.getLastColumn()).getValues();
  if (archiveEnabled) {
    var archiveSheet = ss.getSheetByName(SH.RUN_SUMMARY_ARCHIVE) || getOrCreateSheet_(ss, SH.RUN_SUMMARY_ARCHIVE);
    if (archiveSheet.getLastRow() === 0) ensureRunSummaryLogHeader_(archiveSheet);
    var archiveStart = archiveSheet.getLastRow() + 1;
    archiveSheet.getRange(archiveStart, 1, rowsToArchive.length, rowsToArchive[0].length).setValues(rowsToArchive);
  }

  summarySheet.deleteRows(2, overflow);
}

function appendRunSummaryLog_(runSummary) {
  var summary = runSummary || {};
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.RUN_SUMMARY_LOG) || getOrCreateSheet_(ss, SH.RUN_SUMMARY_LOG);
  if (sh.getLastRow() === 0) ensureRunSummaryLogHeader_(sh);

  var stage = summary.stages || {};
  var odds = stage.odds || {};
  var schedule = stage.schedule || {};
  var model = stage.model || {};
  var signal = stage.signal || {};
  var cadence = summary.cadence || {};
  var creditState = summary.credit_state || {};
  var reasonCodes = summary.reason_codes || {};
  var warnings = reasonCodes.warnings || [];

  sh.appendRow([
    String(summary.run_id || ""),
    String(summary.started_at || ""),
    String(summary.finished_at || ""),
    String(summary.outcome || ""),
    String((summary.mode && summary.mode.trigger_source) || ""),
    String((summary.mode && summary.mode.app_mode) || ""),
    String((summary.mode && summary.mode.active_start) || "") + "-" + String((summary.mode && summary.mode.active_end) || ""),
    toInt_(summary.duration_ms, 0),
    String(odds.outcome || ""),
    toInt_(odds.games, 0),
    String(schedule.outcome || ""),
    toInt_(schedule.matched_count, 0),
    String(model.outcome || ""),
    toInt_(model.computed, 0),
    String(signal.outcome || ""),
    toInt_(signal.bet_signals_found, 0),
    String(cadence.mode || ""),
    String(cadence.reason || ""),
    toInt_(cadence.cadence_minutes, 0),
    toInt_(cadence.zero_streak, 0),
    String(creditState.credit_pressure_level || ""),
    toInt_(creditState.remaining_credits, 0),
    String(summary.reason_code || ""),
    String(summary.reason_detail || ""),
    warnings.length,
    toInt_(summary.log_row_start, 0),
    toInt_(summary.log_row_end, 0),
    String(summary.summary_schema_version || "")
  ]);

  enforceRunSummaryRetention_(ss, false);
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
function getOpeningOddsByGameId_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SH.ODDS_HISTORY);
  if (!sh) return {};

  var rows = readSheetAsObjects_(sh);
  var out = {};
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i] || {};
    var gameId = String(r.odds_game_id || "").trim();
    if (!gameId || out[gameId]) continue;
    out[gameId] = r;
  }
  return out;
}


/* ===================== SMALL UTILS ===================== */

function mapToString_(arr) { var out = []; for (var i = 0; i < arr.length; i++) out.push(String(arr[i])); return out; }
function indexOf_(arr, val) { for (var i = 0; i < arr.length; i++) if (String(arr[i]) === String(val)) return i; return -1; }
function toInt_(v, def) { var n = parseInt(v, 10); return isFinite(n) ? n : def; }
function toFloat_(v, def) { var n = parseFloat(v); return isFinite(n) ? n : def; }
function parseCadenceMinutesSetting_(value, fallback, keyName) {
  var raw = String(value == null ? "" : value).trim();
  var parsed = toInt_(raw, fallback);
  var valid = [1, 5, 10, 15, 30];
  var sourceKey = String(keyName || "cadence_minutes");

  if (!raw) return parsed;

  if (!/^[-+]?\d+$/.test(raw)) {
    log_("WARN", "Cadence setting is not an integer; using fallback", {
      key: sourceKey,
      rawValue: raw,
      fallbackMinutes: fallback
    });
    return fallback;
  }

  if (indexOf_(valid, parsed) < 0) {
    var applied = normalizePipelineTriggerCadenceMinutes_(parsed);
    log_("WARN", "Cadence setting is outside allowed trigger minutes; normalization will apply", {
      key: sourceKey,
      requestedMinutes: parsed,
      appliedMinutes: applied,
      allowedMinutes: valid.join(",")
    });
  }

  return parsed;
}

function parseNumberList_(v, fallbackArr) {
  var raw = String(v || "").trim();
  var out = [];
  if (raw) {
    var parts = raw.split(",");
    for (var i = 0; i < parts.length; i++) {
      var n = Number(String(parts[i] || "").trim());
      if (isFinite(n)) out.push(n);
    }
  }
  if (!out.length) out = (fallbackArr || []).slice(0);
  out = out.filter(function (x) { return isFinite(x); }).sort(function (a, b) { return a - b; });
  if (!out.length || out[0] > 0) out.unshift(0);
  return out;
}
function clampInt_(n, lo, hi) { n = parseInt(n, 10); if (!isFinite(n)) return lo; return Math.max(lo, Math.min(hi, n)); }
function pad2_(n) { n = Number(n); return (n < 10 ? "0" : "") + String(n); }
function round_(x, d) { var n = Number(x); if (!isFinite(n)) return ""; var p = Math.pow(10, d); return Math.round(n * p) / p; }

var MLB_TEAM_CANONICAL_ALIASES_ = (function () {
  var map = {};
  function add_(canonical, variants) {
    var canon = String(canonical || "").toLowerCase().replace(/\s+/g, " ").trim();
    if (!canon) return;
    map[canon] = canon;
    for (var i = 0; i < variants.length; i++) {
      var v = String(variants[i] || "").toLowerCase().replace(/\s+/g, " ").trim();
      if (v) map[v] = canon;
    }
  }

  add_("arizona diamondbacks", ["arizona", "diamondbacks", "d backs", "dbacks", "ari", "az diamondbacks"]);
  add_("atlanta braves", ["atlanta", "braves", "atl", "a braves"]);
  add_("baltimore orioles", ["baltimore", "orioles", "bal", "o s", "os"]);
  add_("boston red sox", ["boston", "red sox", "bos", "boston redsox"]);
  add_("chicago cubs", ["chicago cubs", "chi cubs", "cubs", "chc"]);
  add_("chicago white sox", ["chicago white sox", "chi white sox", "white sox", "chi sox", "cws", "sox"]);
  add_("cincinnati reds", ["cincinnati", "reds", "cin"]);
  add_("cleveland guardians", ["cleveland", "guardians", "cle", "cleveland indians", "indians"]);
  add_("colorado rockies", ["colorado", "rockies", "col"]);
  add_("detroit tigers", ["detroit", "tigers", "det"]);
  add_("houston astros", ["houston", "astros", "hou"]);
  add_("kansas city royals", ["kansas city royals", "kansas city", "kc royals", "k c royals", "royals", "kc", "kcr"]);
  add_("los angeles angels", ["los angeles angels", "la angels", "angels", "anaheim angels", "laa"]);
  add_("los angeles dodgers", ["los angeles dodgers", "la dodgers", "dodgers", "lad"]);
  add_("miami marlins", ["miami", "marlins", "mia", "florida marlins"]);
  add_("milwaukee brewers", ["milwaukee", "brewers", "mil"]);
  add_("minnesota twins", ["minnesota", "twins", "min"]);
  add_("new york mets", ["new york mets", "ny mets", "mets", "nym"]);
  add_("new york yankees", ["new york yankees", "ny yankees", "yankees", "nyy"]);
  add_("oakland athletics", ["oakland athletics", "oakland", "athletics", "a s", "as", "oak", "sacramento athletics"]);
  add_("philadelphia phillies", ["philadelphia", "phillies", "phi"]);
  add_("pittsburgh pirates", ["pittsburgh", "pirates", "pit"]);
  add_("san diego padres", ["san diego", "padres", "sd", "sdp"]);
  add_("san francisco giants", ["san francisco", "giants", "sf", "sfg"]);
  add_("seattle mariners", ["seattle", "mariners", "sea"]);
  add_("st louis cardinals", ["st louis cardinals", "st. louis cardinals", "st louis", "st. louis", "cardinals", "stl"]);
  add_("tampa bay rays", ["tampa bay", "rays", "tb", "tbr"]);
  add_("texas rangers", ["texas", "rangers", "tex", "tx rangers", "tex rangers"]);
  add_("toronto blue jays", ["toronto", "blue jays", "jays", "tor"]);
  add_("washington nationals", ["washington", "nationals", "nats", "wsh"]);

  return map;
})();

function normalizeTeam_(name) {
  var normalized = String(name || "").toLowerCase().replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  return MLB_TEAM_CANONICAL_ALIASES_[normalized] || normalized;
}
