/* ===================== TRIGGERS ===================== */

function installTriggers() {
  removeTriggers();
  var cfg = getConfig_();

  var pipeMins = Math.max(5, toInt_(cfg.PIPELINE_MINUTES, 15));
  ScriptApp.newTrigger("runPipeline").timeBased().everyMinutes(pipeMins).create();

  ScriptApp.newTrigger("refreshProjectionsScheduled").timeBased().everyDays(1).atHour(6).nearMinute(5).create();
  ScriptApp.newTrigger("refreshProjectionsScheduled").timeBased().everyDays(1).atHour(11).nearMinute(5).create();

  var mode = String(cfg.HEARTBEAT_MODE || "DAILY").toUpperCase();
  if (mode === "DAILY") {
    var hh = clampInt_(toInt_(cfg.HEARTBEAT_HOUR, 9), 0, 23);
    var mm = clampInt_(toInt_(cfg.HEARTBEAT_MINUTE, 5), 0, 59);
    ScriptApp.newTrigger("sendDiscordHeartbeat").timeBased().everyDays(1).atHour(hh).nearMinute(mm).create();
  } else if (mode === "HOURLY") {
    ScriptApp.newTrigger("sendDiscordHeartbeat").timeBased().everyHours(1).create();
  }

  log_("INFO", "Triggers installed", {
    pipeline: pipeMins + "m",
    projections: "06:05 + 11:05",
    heartbeat_mode: mode,
    heartbeat_time: (mode === "DAILY") ? (pad2_(toInt_(cfg.HEARTBEAT_HOUR, 9)) + ":" + pad2_(toInt_(cfg.HEARTBEAT_MINUTE, 5))) : (mode === "HOURLY" ? "hourly" : "off")
  });
}

function removeTriggers() {
  var all = ScriptApp.getProjectTriggers();
  var removed = 0;
  for (var i = 0; i < all.length; i++) {
    var fn = all[i].getHandlerFunction();
    if (fn === "runPipeline" || fn === "refreshProjectionsScheduled" || fn === "sendDiscordHeartbeat") {
      ScriptApp.deleteTrigger(all[i]);
      removed++;
    }
  }
  log_("INFO", "Triggers removed", { count: removed });
}

/* ===================== DISCORD ===================== */

function getDiscordWebhook_(cfg) {
  return (cfg && cfg.DISCORD_WEBHOOK ? cfg.DISCORD_WEBHOOK : "") ||
    PropertiesService.getScriptProperties().getProperty(PROP.DISCORD_WEBHOOK) || "";
}

function sendDiscord_(webhook, payloadObj) {
  var resp = UrlFetchApp.fetch(webhook, {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payloadObj),
    muteHttpExceptions: true
  });
  return { http: resp.getResponseCode(), body: resp.getContentText() };
}

function sendDiscordTestPing() {
  var cfg = getConfig_();
  var webhook = getDiscordWebhook_(cfg);
  var ui = SpreadsheetApp.getUi();

  if (!webhook) {
    log_("ERROR", "Discord test ping failed: missing DISCORD_WEBHOOK", {});
    ui.alert("Discord test ping failed.\n\nSet DISCORD_WEBHOOK in SETTINGS and try again.");
    return;
  }

  var ss = SpreadsheetApp.getActive();
  var payload = {
    content:
      "✅ **Lucky Luciano MLB — Discord Test Ping**\n" +
      "**Sheet:** " + ss.getName() + "\n" +
      "**Local:** " + localPretty_(new Date()) + "\n" +
      "**UTC:** " + new Date().toISOString() + "\n" +
      "_If you see this, the webhook is connected and posting._"
  };

  var res = sendDiscord_(webhook, payload);
  if (res.http >= 200 && res.http < 300) {
    log_("INFO", "Discord test ping sent", { http: res.http });
    ui.alert("Discord test ping sent ✅\n\nCheck your Discord channel.");
  } else {
    log_("WARN", "Discord test ping failed", { http: res.http, body: String(res.body || "").slice(0, 300) });
    ui.alert("Discord test ping failed ❌\n\nHTTP: " + res.http + "\nCheck LOG for details.");
  }
}


function sendDiscordActionButtonsTest() {
  var cfg = getConfig_();
  var webhook = getDiscordWebhook_(cfg);
  var ui = SpreadsheetApp.getUi();

  if (!webhook) {
    log_("ERROR", "Discord action test failed: missing DISCORD_WEBHOOK", {});
    ui.alert("Discord action test failed.\n\nSet DISCORD_WEBHOOK in SETTINGS and try again.");
    return;
  }

  var baseUrl = String(cfg.WEB_APP_URL || "").trim();
  if (!baseUrl) {
    log_("ERROR", "Discord action test failed: missing WEB_APP_URL", {});
    ui.alert("Discord action test failed.\n\nSet WEB_APP_URL in SETTINGS and try again.");
    return;
  }

  var token = Utilities.getUuid();
  var testUrl = baseUrl + "?action=test&token=" + encodeURIComponent(token);
  var payloadObj = {
    content:
      "🧪 **Lucky Luciano MLB — Action Buttons Test**\n" +
      "Use this test message to validate Discord buttons and Web App routing.\n" +
      "This does **not** create or update betting logs.",
    components: [{
      type: 1,
      components: [
        { type: 2, style: 5, label: "Open Action Test", url: testUrl }
      ]
    }]
  };

  var webhookMode = discordWebhookMode_(webhook);
  var includesComponents = !!(payloadObj && payloadObj.components && payloadObj.components.length);

  var res = sendDiscord_(webhook, payloadObj);
  var bodyPreview = String(res.body || "").slice(0, 300);
  if (res.http >= 200 && res.http < 300) {
    log_("INFO", "Discord action buttons test sent", {
      action: "test",
      webhookMode: webhookMode,
      http: res.http,
      body: bodyPreview,
      includesComponents: includesComponents
    });
    ui.alert("Discord action buttons test sent ✅\n\nClick the button in Discord to validate the web app route.");
  } else {
    log_("WARN", "Discord action buttons test failed", {
      action: "test",
      webhookMode: webhookMode,
      http: res.http,
      body: bodyPreview,
      includesComponents: includesComponents
    });
    ui.alert("Discord action buttons test failed ❌\n\nHTTP: " + res.http + "\nCheck LOG for details.");
  }
}

function sendDiscordActionPayloadDiagnostics() {
  var cfg = getConfig_();
  var webhook = getDiscordWebhook_(cfg);
  var ui = SpreadsheetApp.getUi();

  if (!webhook) {
    log_("ERROR", "Discord diagnostics failed: missing DISCORD_WEBHOOK", {});
    ui.alert("Discord diagnostics failed.\n\nSet DISCORD_WEBHOOK in SETTINGS and try again.");
    return;
  }

  var baseUrl = String(cfg.WEB_APP_URL || "").trim();
  if (!baseUrl) {
    log_("ERROR", "Discord diagnostics failed: missing WEB_APP_URL", {});
    ui.alert("Discord diagnostics failed.\n\nSet WEB_APP_URL in SETTINGS and try again.");
    return;
  }

  var webhookMode = discordWebhookMode_(webhook);
  var token = Utilities.getUuid();
  var testUrl = baseUrl + "?action=test&token=" + encodeURIComponent(token);

  var diagnosticsPayloads = [
    {
      name: "content_only",
      payload: {
        content:
          "🧪 **Discord Diagnostics — Content Only**\n" +
          "This probe includes only message content so operators can compare webhook behavior."
      }
    },
    {
      name: "content_with_components",
      payload: {
        content:
          "🧪 **Discord Diagnostics — Content + Components**\n" +
          "This probe includes a link button to detect component stripping.",
        components: [{
          type: 1,
          components: [
            { type: 2, style: 5, label: "Open Action Test", url: testUrl }
          ]
        }]
      }
    }
  ];

  var allOk = true;
  for (var i = 0; i < diagnosticsPayloads.length; i++) {
    var diag = diagnosticsPayloads[i];
    var includesComponents = !!(diag.payload && diag.payload.components && diag.payload.components.length);
    var res = sendDiscord_(webhook, diag.payload);
    var bodyPreview = String(res.body || "").slice(0, 300);
    var level = (res.http >= 200 && res.http < 300) ? "INFO" : "WARN";
    if (level !== "INFO") allOk = false;

    log_(level, "Discord diagnostics payload sent", {
      payloadType: diag.name,
      webhookMode: webhookMode,
      http: res.http,
      body: bodyPreview,
      includesComponents: includesComponents
    });
  }

  if (allOk) {
    ui.alert("Discord diagnostics sent ✅\n\nReview LOG entries for payloadType content_only vs content_with_components.");
  } else {
    ui.alert("Discord diagnostics completed with warnings ⚠️\n\nReview LOG entries for HTTP/body details by payloadType.");
  }
}

function discordWebhookMode_(webhook) {
  var hook = String(webhook || "");
  if (!hook) return "missing_webhook";

  var parts = hook.split("?");
  if (parts.length < 2) return "no_query_params";

  var query = parts.slice(1).join("?");
  var hasWaitTrue = /(?:^|&)wait=true(?:&|$)/i.test(query);
  if (hasWaitTrue) return "query_params_wait_true";
  return "query_params_no_wait_true";
}

function sendDiscordHeartbeat() {
  var cfg = getConfig_();
  var webhook = getDiscordWebhook_(cfg);
  if (!webhook) { log_("ERROR", "Heartbeat skipped: missing DISCORD_WEBHOOK", {}); return; }

  var mode = String(cfg.HEARTBEAT_MODE || "DAILY").toUpperCase();
  if (mode === "OFF") { log_("INFO", "Heartbeat skipped (mode=OFF)", {}); return; }

  var props = PropertiesService.getScriptProperties();
  var key = (mode === "HOURLY") ? localHourKey_() : localDateKey_();
  var prevKey = props.getProperty(PROP.LAST_HEARTBEAT_KEY) || "";
  if (prevKey === key) {
    log_("INFO", "Heartbeat deduped (already sent for period)", { mode: mode, key: key });
    return;
  }

  var lastAt = props.getProperty(PROP.LAST_PIPELINE_AT) || "";
  var lastStatus = props.getProperty(PROP.LAST_PIPELINE_STATUS) || "";
  var lastSummary = props.getProperty(PROP.LAST_PIPELINE_SUMMARY) || "";

  var msg =
    "🫀 **Lucky Luciano MLB — Heartbeat (" + mode + ")**\n" +
    "**Local:** " + localPretty_(new Date()) + "\n" +
    "**UTC:** " + new Date().toISOString() + "\n";

  if (lastAt) msg += "**Last pipeline (UTC):** " + lastAt + "\n";
  if (lastStatus) msg += "**Last status:** " + lastStatus + "\n";
  if (lastSummary) msg += "**Last summary:** " + lastSummary + "\n";
  msg += "_If you see this, triggers + webhook are working._";

  var res = sendDiscord_(webhook, { content: msg });
  if (res.http >= 200 && res.http < 300) {
    props.setProperty(PROP.LAST_HEARTBEAT_KEY, key);
    log_("INFO", "Heartbeat sent", { http: res.http, mode: mode, key: key });
  } else {
    log_("WARN", "Heartbeat failed", { http: res.http, body: String(res.body || "").slice(0, 200) });
  }
}

/* ===================== PIPELINE ===================== */

function runPipeline() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(25000)) { log_("WARN", "Pipeline skipped (lock busy)", {}); return; }

  var props = PropertiesService.getScriptProperties();
  var startedUtc = new Date().toISOString();

  try {
    var cfg = getConfig_();

    if (!withinActiveHours_(cfg)) {
      props.setProperty(PROP.LAST_PIPELINE_AT, startedUtc);
      props.setProperty(PROP.LAST_PIPELINE_STATUS, "SKIPPED_OUTSIDE_WINDOW");
      props.setProperty(PROP.LAST_PIPELINE_SUMMARY, "outside active window");
      log_("INFO", "Pipeline skipped (outside active window)", { activeStart: cfg.ACTIVE_START, activeEnd: cfg.ACTIVE_END });
      return;
    }

    var oddsRes = { sportKeyUsed: chooseSportKey_(cfg), games: 0, updatedAt: isoLocalWithOffset_(new Date()), skipped: true };
    var oddsWindowCtx = resolveOddsWindowForPipeline_(cfg, props);
    var oddsWindow = oddsWindowCtx.window;
    var oddsWindowError = oddsWindowCtx.error;

    var shouldRefreshOdds = false;
    var nowLocal = new Date();

    if (oddsWindow && oddsWindow.hasGames) {
      shouldRefreshOdds = nowLocal.getTime() >= oddsWindow.windowStart.getTime() && nowLocal.getTime() <= oddsWindow.windowEnd.getTime();
      if (!shouldRefreshOdds) {
        log_("INFO", "Odds refresh skipped: outside computed window", {
          nowLocal: isoLocalWithOffset_(nowLocal),
          firstGameLocal: oddsWindow.firstGameLocalIso,
          lastGameLocal: oddsWindow.lastGameLocalIso,
          windowStart: oddsWindow.windowStartIso,
          windowEnd: oddsWindow.windowEndIso,
          gameCount: oddsWindow.gameCount,
          windowSource: oddsWindowCtx.source
        });
      }
    } else if (oddsWindow && !oddsWindow.hasGames) {
      var noGamesBehavior = String(cfg.ODDS_NO_GAMES_BEHAVIOR || "SKIP").toUpperCase();
      if (noGamesBehavior === "FALLBACK_STATIC_WINDOW") {
        shouldRefreshOdds = withinActiveHours_(cfg);
        if (!shouldRefreshOdds) {
          log_("INFO", "Odds refresh skipped: no games today + outside static window", {
            behavior: noGamesBehavior,
            activeStart: cfg.ACTIVE_START,
            activeEnd: cfg.ACTIVE_END,
            scheduleDateLocal: oddsWindow.scheduleDateLocal,
            windowSource: oddsWindowCtx.source
          });
        }
      } else {
        log_("INFO", "Odds refresh skipped: no games today", {
          behavior: noGamesBehavior,
          scheduleDateLocal: oddsWindow.scheduleDateLocal,
          windowSource: oddsWindowCtx.source
        });
      }
    } else {
      shouldRefreshOdds = withinActiveHours_(cfg);
      if (!shouldRefreshOdds) {
        log_("INFO", "Odds refresh skipped: schedule fetch error + outside static window", {
          activeStart: cfg.ACTIVE_START,
          activeEnd: cfg.ACTIVE_END,
          error: oddsWindowError,
          windowSource: oddsWindowCtx.source
        });
      }
    }

    if (shouldRefreshOdds) oddsRes = refreshOdds_(cfg);

    var mlbRes = refreshMLBScheduleAndLineups_(cfg);
    refreshProjectionsIfStale_(cfg, false);
    var modelRes = refreshModelAndEdge_(cfg, mlbRes);

    var summary = "odds=" + oddsRes.games + " matched=" + mlbRes.matchedCount + " computed=" + modelRes.computed + " bets=" + modelRes.betSignalsFound;

    props.setProperty(PROP.LAST_PIPELINE_AT, startedUtc);
    props.setProperty(PROP.LAST_PIPELINE_STATUS, "OK");
    props.setProperty(PROP.LAST_PIPELINE_SUMMARY, summary);

    log_("INFO", "runPipeline completed", { odds: oddsRes.games, matched: mlbRes.matchedCount, computed: modelRes.computed, betSignalsFound: modelRes.betSignalsFound });
  } catch (e) {
    props.setProperty(PROP.LAST_PIPELINE_AT, startedUtc);
    props.setProperty(PROP.LAST_PIPELINE_STATUS, "ERROR");
    props.setProperty(PROP.LAST_PIPELINE_SUMMARY, String(e));
    log_("ERROR", "runPipeline error", { message: String(e), stack: (e && e.stack) ? String(e.stack) : "" });
    throw e;
  } finally {
    lock.releaseLock();
  }
}


function resolveOddsWindowForPipeline_(cfg, props) {
  var cacheTtlMin = Math.max(1, toInt_(cfg.ODDS_WINDOW_CACHE_TTL_MIN, 30));
  var nowMs = Date.now();

  try {
    var freshWindow = getMLBOddsRefreshWindow_(cfg);
    storeOddsWindowCache_(props, freshWindow, nowMs);
    log_("INFO", "Odds window source selected", {
      source: "fresh_schedule",
      hasGames: !!(freshWindow && freshWindow.hasGames),
      gameCount: freshWindow ? freshWindow.gameCount : 0,
      cacheTtlMin: cacheTtlMin
    });
    return { window: freshWindow, source: "fresh_schedule", error: "" };
  } catch (eWindow) {
    var errMsg = String(eWindow);
    var cachedWindow = readOddsWindowCache_(props, nowMs, cacheTtlMin);
    if (cachedWindow) {
      log_("WARN", "Odds window source selected", {
        source: "cached_schedule",
        hasGames: !!(cachedWindow && cachedWindow.hasGames),
        gameCount: cachedWindow ? cachedWindow.gameCount : 0,
        cacheTtlMin: cacheTtlMin,
        fetchError: errMsg
      });
      return { window: cachedWindow, source: "cached_schedule", error: errMsg };
    }

    log_("WARN", "Odds window source selected", {
      source: "fallback_static_window",
      cacheTtlMin: cacheTtlMin,
      fetchError: errMsg
    });
    return { window: null, source: "fallback_static_window", error: errMsg };
  }
}

function storeOddsWindowCache_(props, windowObj, cachedAtMs) {
  if (!props || !windowObj) return;
  var cachePayload = {
    cachedAtMs: cachedAtMs,
    hasGames: !!windowObj.hasGames,
    gameCount: toInt_(windowObj.gameCount, 0),
    preFirstMin: toInt_(windowObj.preFirstMin, 0),
    postLastMin: toInt_(windowObj.postLastMin, 0),
    scheduleDateLocal: String(windowObj.scheduleDateLocal || ""),
    firstGameLocalIso: String(windowObj.firstGameLocalIso || ""),
    lastGameLocalIso: String(windowObj.lastGameLocalIso || ""),
    windowStartIso: String(windowObj.windowStartIso || ""),
    windowEndIso: String(windowObj.windowEndIso || "")
  };
  props.setProperty(PROP.ODDS_WINDOW_CACHE, JSON.stringify(cachePayload));
}

function readOddsWindowCache_(props, nowMs, cacheTtlMin) {
  if (!props) return null;
  var raw = props.getProperty(PROP.ODDS_WINDOW_CACHE);
  if (!raw) return null;

  try {
    var cached = JSON.parse(raw);
    var cachedAtMs = toInt_(cached.cachedAtMs, 0);
    if (cachedAtMs <= 0) return null;

    var maxAgeMs = Math.max(1, cacheTtlMin) * 60 * 1000;
    if ((nowMs - cachedAtMs) > maxAgeMs) return null;

    var parsed = {
      hasGames: !!cached.hasGames,
      gameCount: toInt_(cached.gameCount, 0),
      preFirstMin: toInt_(cached.preFirstMin, 0),
      postLastMin: toInt_(cached.postLastMin, 0),
      scheduleDateLocal: String(cached.scheduleDateLocal || ""),
      firstGameLocalIso: String(cached.firstGameLocalIso || ""),
      lastGameLocalIso: String(cached.lastGameLocalIso || ""),
      windowStartIso: String(cached.windowStartIso || ""),
      windowEndIso: String(cached.windowEndIso || "")
    };

    if (parsed.hasGames) {
      parsed.firstGameLocal = parsed.firstGameLocalIso ? new Date(parsed.firstGameLocalIso) : null;
      parsed.lastGameLocal = parsed.lastGameLocalIso ? new Date(parsed.lastGameLocalIso) : null;
      parsed.windowStart = parsed.windowStartIso ? new Date(parsed.windowStartIso) : null;
      parsed.windowEnd = parsed.windowEndIso ? new Date(parsed.windowEndIso) : null;
      if (!parsed.windowStart || !parsed.windowEnd || isNaN(parsed.windowStart.getTime()) || isNaN(parsed.windowEnd.getTime())) {
        return null;
      }
    } else {
      parsed.firstGameLocal = null;
      parsed.lastGameLocal = null;
      parsed.windowStart = null;
      parsed.windowEnd = null;
    }

    return parsed;
  } catch (eCache) {
    return null;
  }
}

function refreshOddsOnly() { var cfg = getConfig_(); refreshOdds_(cfg); }
function refreshMLBScheduleAndLineupsOnly() { var cfg = getConfig_(); refreshMLBScheduleAndLineups_(cfg); }
function refreshProjectionsForce() { var cfg = getConfig_(); refreshProjectionsIfStale_(cfg, true); }
function refreshModelAndEdgeOnly() { var cfg = getConfig_(); var mlbRes = refreshMLBScheduleAndLineups_(cfg); refreshModelAndEdge_(cfg, mlbRes); }
