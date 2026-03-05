/* ===================== EXTERNAL FEATURE INGESTION ===================== */

function loadExternalFeatureContext_(cfg, matched) {
  var context = {
    enabled: {
      weather: !!cfg.EXT_FEATURES_ENABLE_WEATHER,
      bullpen: !!cfg.EXT_FEATURES_ENABLE_BULLPEN,
      experimental: !!cfg.EXT_FEATURES_ENABLE_EXPERIMENTAL,
      market: !!cfg.EXT_FEATURES_ENABLE_MARKET,
      statcast: !!cfg.EXT_FEATURES_ENABLE_STATCAST
    },
    flags: {
      forceRefresh: !!cfg.EXT_FEATURES_FORCE_REFRESH,
      debug: !!cfg.EXT_FEATURES_DEBUG
    },
    byGame: {},
    health: { bySource: {}, fallbackUsedSources: [] },
    diagnostics: { fetchLogs: [] }
  };

  if (!matched || !matched.length) return context;

  var gameCtx = buildExternalFeatureGameContext_(matched);
  var weatherRes = fetchExternalFeatureSource_(cfg, "weather", gameCtx);
  var bullpenRes = fetchExternalFeatureSource_(cfg, "bullpen", gameCtx);
  var marketRes = fetchExternalFeatureSource_(cfg, "market", gameCtx);
  var statcastRes = fetchExternalFeatureSource_(cfg, "statcast", gameCtx);

  mergeExternalFeatureRows_(context.byGame, weatherRes.rows);
  mergeExternalFeatureRows_(context.byGame, bullpenRes.rows);
  mergeExternalFeatureRows_(context.byGame, marketRes.rows);
  mergeExternalFeatureRows_(context.byGame, statcastRes.rows);

  context.health.bySource.weather = weatherRes.health;
  context.health.bySource.bullpen = bullpenRes.health;
  context.health.bySource.market = marketRes.health;
  context.health.bySource.statcast = statcastRes.health;
  context.health.fallbackUsedSources = ["weather", "bullpen", "market", "statcast"].filter(function (k) {
    return !!(context.health.bySource[k] && context.health.bySource[k].staleFallbackUsed);
  });

  context.diagnostics.fetchLogs = [weatherRes.logDetail, bullpenRes.logDetail, marketRes.logDetail, statcastRes.logDetail];
  for (var i = 0; i < context.diagnostics.fetchLogs.length; i++) {
    var detail = context.diagnostics.fetchLogs[i];
    if (!detail) continue;
    if (detail.enabled || context.flags.debug) log_("INFO", "externalFeature source status", detail);
  }
  return context;
}

function buildExternalFeatureGameContext_(matched) {
  var out = [];
  for (var i = 0; i < matched.length; i++) {
    var g = matched[i] || {};
    out.push({
      odds_game_id: String(g.odds_game_id || "").trim(),
      mlb_gamePk: String(g.mlb_gamePk || "").trim(),
      away_team: String(g.away_team || ""),
      home_team: String(g.home_team || ""),
      gameDate_local: String(g.gameDate_local || localDateKey_()),
      commence_time_local: String(g.commence_time_local || "")
    });
  }
  return out;
}

function fetchExternalFeatureSource_(cfg, source, games) {
  var sourceCfg = externalFeatureSourceCfg_(cfg, source);
  var health = readExternalFeatureHealth_(source);
  var nowMs = Date.now();
  var disableUntilMs = toInt_(health.disabledUntilMs, 0);
  var isDisabled = disableUntilMs > nowMs;
  var cache = readExternalFeatureCache_(sourceCfg.cacheProp);
  var cacheAgeMin = cache && isFinite(cache.cachedAtMs) ? ((nowMs - cache.cachedAtMs) / 60000) : -1;
  var cacheFresh = !!(cache && cacheAgeMin >= 0 && cacheAgeMin <= sourceCfg.ttlMin);
  var rows = [];
  var errorClass = "";
  var errorMessage = "";
  var staleFallbackUsed = false;
  var fetchAttempted = false;
  var rowsParsed = 0;

  if (!sourceCfg.enabled) {
    health.disabledReason = "feature_flag_off";
    writeExternalFeatureHealth_(source, health);
    return {
      rows: [],
      health: health,
      logDetail: makeExternalSourceLogDetail_(sourceCfg, health, fetchAttempted, cacheFresh, cacheAgeMin, rowsParsed, staleFallbackUsed, errorClass, errorMessage)
    };
  }

  if (sourceCfg.experimental && !cfg.EXT_FEATURES_ENABLE_EXPERIMENTAL) {
    health.disabledReason = "experimental_master_off";
    writeExternalFeatureHealth_(source, health);
    return {
      rows: [],
      health: health,
      logDetail: makeExternalSourceLogDetail_(sourceCfg, health, fetchAttempted, cacheFresh, cacheAgeMin, rowsParsed, staleFallbackUsed, errorClass, errorMessage)
    };
  }

  if (isDisabled) {
    health.disabledReason = "circuit_breaker_open";
    writeExternalFeatureHealth_(source, health);
    return {
      rows: cache && cache.rows ? cache.rows : [],
      health: health,
      logDetail: makeExternalSourceLogDetail_(sourceCfg, health, false, cacheFresh, cacheAgeMin, cache && cache.rows ? cache.rows.length : 0, !!cache, "", "disabled_until_" + new Date(disableUntilMs).toISOString())
    };
  }

  if (cacheFresh && !cfg.EXT_FEATURES_FORCE_REFRESH) {
    health.lastCacheHitAt = new Date().toISOString();
    writeExternalFeatureHealth_(source, health);
    return {
      rows: cache.rows || [],
      health: health,
      logDetail: makeExternalSourceLogDetail_(sourceCfg, health, false, true, cacheAgeMin, (cache.rows || []).length, false, "", "")
    };
  }

  try {
    fetchAttempted = true;
    rows = fetchExternalFeatureRowsBySource_(sourceCfg, games);
    rowsParsed = rows.length;
    health.successCount = toInt_(health.successCount, 0) + 1;
    health.failureCount = toInt_(health.failureCount, 0);
    health.consecutiveFailures = 0;
    health.lastSuccessAt = new Date().toISOString();
    health.disabledUntilMs = 0;
    health.disabledReason = "";
    writeExternalFeatureCache_(sourceCfg.cacheProp, { rows: rows, cachedAtMs: nowMs, source: sourceCfg.source });
  } catch (e) {
    errorClass = classifyExternalFeatureError_(e);
    errorMessage = String(e && e.message ? e.message : e || "unknown_error");
    health.failureCount = toInt_(health.failureCount, 0) + 1;
    health.consecutiveFailures = toInt_(health.consecutiveFailures, 0) + 1;
    health.lastFailureAt = new Date().toISOString();
    if (cache && cache.rows && cache.rows.length) {
      rows = cache.rows;
      rowsParsed = rows.length;
      staleFallbackUsed = true;
    }
    if (sourceCfg.experimental && health.consecutiveFailures >= cfg.EXT_FEATURES_EXPERIMENTAL_FAIL_THRESHOLD) {
      health.disabledUntilMs = nowMs + (toInt_(cfg.EXT_FEATURES_EXPERIMENTAL_DISABLE_MIN, 120) * 60000);
      health.disabledReason = "circuit_breaker_failures";
    }
  }

  writeExternalFeatureHealth_(source, health);
  return {
    rows: rows,
    health: health,
    logDetail: makeExternalSourceLogDetail_(sourceCfg, health, fetchAttempted, cacheFresh, cacheAgeMin, rowsParsed, staleFallbackUsed, errorClass, errorMessage)
  };
}

function externalFeatureSourceCfg_(cfg, source) {
  var m = {
    weather: { enabled: !!cfg.EXT_FEATURES_ENABLE_WEATHER, provider: cfg.EXT_FEATURES_PROVIDER_WEATHER, ttlMin: cfg.EXT_FEATURES_TTL_WEATHER_MIN, cacheProp: PROP.EXT_FEATURE_CACHE_WEATHER, experimental: false },
    bullpen: { enabled: !!cfg.EXT_FEATURES_ENABLE_BULLPEN, provider: cfg.EXT_FEATURES_PROVIDER_BULLPEN, ttlMin: cfg.EXT_FEATURES_TTL_BULLPEN_MIN, cacheProp: PROP.EXT_FEATURE_CACHE_BULLPEN, experimental: false },
    market: { enabled: !!cfg.EXT_FEATURES_ENABLE_MARKET, provider: cfg.EXT_FEATURES_PROVIDER_MARKET, ttlMin: cfg.EXT_FEATURES_TTL_MARKET_MIN, cacheProp: PROP.EXT_FEATURE_CACHE_MARKET, experimental: true },
    statcast: { enabled: !!cfg.EXT_FEATURES_ENABLE_STATCAST, provider: cfg.EXT_FEATURES_PROVIDER_STATCAST, ttlMin: cfg.EXT_FEATURES_TTL_STATCAST_MIN, cacheProp: PROP.EXT_FEATURE_CACHE_STATCAST, experimental: true }
  };
  var x = m[source] || { enabled: false, provider: "NONE", ttlMin: 30, cacheProp: "", experimental: false };
  x.source = source;
  return x;
}

function fetchExternalFeatureRowsBySource_(sourceCfg, games) {
  // Placeholder deterministic stubs with normalized schema.
  // External providers can replace these implementations without touching scorer internals.
  var out = [];
  for (var i = 0; i < games.length; i++) {
    var g = games[i] || {};
    if (!g.mlb_gamePk) continue;
    var item = {
      source: sourceCfg.source,
      provider: sourceCfg.provider,
      mlb_gamePk: g.mlb_gamePk,
      gameDate_local: g.gameDate_local,
      away_team: g.away_team,
      home_team: g.home_team,
      generated_at: isoLocalWithOffset_(new Date())
    };

    if (sourceCfg.source === "weather") {
      item.weatherSeverity = syntheticWeatherSeverity_(g.mlb_gamePk);
      item.runEnvDelta = (item.weatherSeverity - 0.5) * 0.08;
    } else if (sourceCfg.source === "bullpen") {
      item.awayBullpenFatigue = syntheticFatigue_(g.mlb_gamePk, "away");
      item.homeBullpenFatigue = syntheticFatigue_(g.mlb_gamePk, "home");
      item.runPrevDeltaAway = -0.06 * item.awayBullpenFatigue;
      item.runPrevDeltaHome = -0.06 * item.homeBullpenFatigue;
    } else if (sourceCfg.source === "market") {
      item.marketPressure = syntheticMarketPressure_(g.mlb_gamePk);
      item.marketDelta = (item.marketPressure - 0.5) * 0.04;
    } else if (sourceCfg.source === "statcast") {
      item.contactQualityDelta = syntheticContactDelta_(g.mlb_gamePk);
      item.statcastDelta = item.contactQualityDelta * 0.03;
    }

    out.push(item);
  }
  return out;
}

function mergeExternalFeatureRows_(byGame, rows) {
  for (var i = 0; i < (rows || []).length; i++) {
    var r = rows[i] || {};
    var gamePk = String(r.mlb_gamePk || "").trim();
    if (!gamePk) continue;
    if (!byGame[gamePk]) byGame[gamePk] = { mlb_gamePk: gamePk };
    var tgt = byGame[gamePk];
    for (var k in r) {
      if (k === "mlb_gamePk") continue;
      tgt[k] = r[k];
    }
  }
}

function buildFeatureAdjustmentsForGame_(cfg, gameFeatureObj) {
  var f = gameFeatureObj || {};
  var out = {
    weatherApplied: false,
    bullpenApplied: false,
    experimentalApplied: false,
    weatherRunEnvDelta: 0,
    bullpenAwayRunPrevDelta: 0,
    bullpenHomeRunPrevDelta: 0,
    marketDelta: 0,
    statcastDelta: 0,
    totalRunEnvDelta: 0,
    totalAwayRunPrevDelta: 0,
    totalHomeRunPrevDelta: 0
  };

  if (cfg.EXT_FEATURES_ENABLE_WEATHER && isFinite(Number(f.runEnvDelta))) {
    out.weatherApplied = true;
    out.weatherRunEnvDelta = Number(f.runEnvDelta) * toFloat_(cfg.EXT_FEATURE_WEIGHT_WEATHER_RUN_ENV, 0.18);
    out.totalRunEnvDelta += out.weatherRunEnvDelta;
  }

  if (cfg.EXT_FEATURES_ENABLE_BULLPEN) {
    if (isFinite(Number(f.runPrevDeltaAway))) {
      out.bullpenApplied = true;
      out.bullpenAwayRunPrevDelta = Number(f.runPrevDeltaAway) * toFloat_(cfg.EXT_FEATURE_WEIGHT_BULLPEN_RUN_PREV, 0.14);
      out.totalAwayRunPrevDelta += out.bullpenAwayRunPrevDelta;
    }
    if (isFinite(Number(f.runPrevDeltaHome))) {
      out.bullpenApplied = true;
      out.bullpenHomeRunPrevDelta = Number(f.runPrevDeltaHome) * toFloat_(cfg.EXT_FEATURE_WEIGHT_BULLPEN_RUN_PREV, 0.14);
      out.totalHomeRunPrevDelta += out.bullpenHomeRunPrevDelta;
    }
  }

  if (cfg.EXT_FEATURES_ENABLE_EXPERIMENTAL) {
    if (cfg.EXT_FEATURES_ENABLE_MARKET && isFinite(Number(f.marketDelta))) {
      out.experimentalApplied = true;
      out.marketDelta = Number(f.marketDelta) * toFloat_(cfg.EXT_FEATURE_WEIGHT_MARKET, 0.06);
      out.totalRunEnvDelta += out.marketDelta;
    }
    if (cfg.EXT_FEATURES_ENABLE_STATCAST && isFinite(Number(f.statcastDelta))) {
      out.experimentalApplied = true;
      out.statcastDelta = Number(f.statcastDelta) * toFloat_(cfg.EXT_FEATURE_WEIGHT_STATCAST, 0.05);
      out.totalRunEnvDelta += out.statcastDelta;
    }
  }

  return out;
}

function readExternalFeatureCache_(propKey) {
  var raw = PropertiesService.getScriptProperties().getProperty(propKey);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { return null; }
}

function writeExternalFeatureCache_(propKey, payload) {
  PropertiesService.getScriptProperties().setProperty(propKey, JSON.stringify(payload || {}));
}

function readExternalFeatureHealth_(source) {
  var key = PROP.EXT_FEATURE_HEALTH_PREFIX + source.toUpperCase();
  var raw = PropertiesService.getScriptProperties().getProperty(key);
  if (!raw) return {
    source: source,
    successCount: 0,
    failureCount: 0,
    consecutiveFailures: 0,
    disabledUntilMs: 0,
    disabledReason: ""
  };
  try {
    var x = JSON.parse(raw);
    x.source = source;
    return x;
  } catch (e) {
    return {
      source: source,
      successCount: 0,
      failureCount: 0,
      consecutiveFailures: 0,
      disabledUntilMs: 0,
      disabledReason: "parse_error"
    };
  }
}

function writeExternalFeatureHealth_(source, health) {
  var key = PROP.EXT_FEATURE_HEALTH_PREFIX + source.toUpperCase();
  PropertiesService.getScriptProperties().setProperty(key, JSON.stringify(health || {}));
}

function makeExternalSourceLogDetail_(sourceCfg, health, fetchAttempted, cacheFresh, cacheAgeMin, rowsParsed, staleFallbackUsed, errorClass, errorMessage) {
  return {
    source: sourceCfg.source,
    provider: sourceCfg.provider,
    enabled: sourceCfg.enabled,
    experimental: sourceCfg.experimental,
    fetchAttempted: fetchAttempted,
    cacheFresh: cacheFresh,
    cacheAgeMin: cacheAgeMin,
    rowsParsed: rowsParsed,
    staleFallbackUsed: staleFallbackUsed,
    errorClass: errorClass || "",
    errorMessage: errorMessage || "",
    successCount: toInt_(health.successCount, 0),
    failureCount: toInt_(health.failureCount, 0),
    consecutiveFailures: toInt_(health.consecutiveFailures, 0),
    disabledUntilMs: toInt_(health.disabledUntilMs, 0),
    disabledReason: String(health.disabledReason || "")
  };
}

function classifyExternalFeatureError_(e) {
  var msg = String(e && e.message ? e.message : e || "").toLowerCase();
  if (msg.indexOf("timed out") >= 0) return "timeout";
  if (msg.indexOf("parse") >= 0) return "parse";
  if (msg.indexOf("429") >= 0 || msg.indexOf("rate") >= 0) return "rate_limit";
  return "runtime";
}

function syntheticWeatherSeverity_(seed) { return seededRatio_(seed + "|w"); }
function syntheticFatigue_(seed, side) { return seededRatio_(seed + "|bp|" + side); }
function syntheticMarketPressure_(seed) { return seededRatio_(seed + "|m"); }
function syntheticContactDelta_(seed) { return seededRatio_(seed + "|s") - 0.5; }

function seededRatio_(seed) {
  var s = String(seed || "0");
  var h = 0;
  for (var i = 0; i < s.length; i++) h = ((h * 31) + s.charCodeAt(i)) % 100000;
  return (h % 1000) / 1000;
}
