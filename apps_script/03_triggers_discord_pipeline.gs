/* ===================== TRIGGERS ===================== */

function installTriggers() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(25000)) { log_("WARN", "Trigger maintenance skipped (lock busy)", { reason_code: REASON_CODE.BLOCKER_STATE, reason_detail: "maintenance_lock_busy" }); return; }

  try {
    var cfg = getConfig_();
    var props = PropertiesService.getScriptProperties();
    var pipeMins = Math.max(5, toInt_(cfg.PIPELINE_MINUTES, 15));
    var desiredState = describeDesiredTriggerState_(cfg, pipeMins);
    var currentState = describeCurrentTriggerState_();
    var triggersAlreadyCorrect = isTriggerInstallStateMatch_(desiredState, currentState);

    var cadenceUpdate = null;
    var closeUpdaterAction = "noop";
    if (!triggersAlreadyCorrect) {
      if (currentState.baseInstallStateMatch && currentState.closeUpdaterNeedsRecreate) {
        recreateSignalCloseUpdaterTrigger_(desiredState.signalCloseUpdaterMinutes);
        closeUpdaterAction = "recreated";
      } else if (currentState.baseInstallStateMatch && !desiredState.signalCloseUpdaterEnabled && currentState.updateSignalLogCloseMetrics > 0) {
        recreateSignalCloseUpdaterTrigger_(0);
        closeUpdaterAction = "removed";
      } else {
        removeTriggers();
        closeUpdaterAction = desiredState.signalCloseUpdaterEnabled ? "recreated" : "removed";
        cadenceUpdate = ensurePipelineTriggerCadence_(pipeMins, "install", { installAction: "reinstall" });
        ScriptApp.newTrigger("refreshProjectionsScheduled").timeBased().everyDays(1).atHour(6).nearMinute(5).create();
        ScriptApp.newTrigger("refreshProjectionsScheduled").timeBased().everyDays(1).atHour(11).nearMinute(5).create();
        ScriptApp.newTrigger("runDailyCalibration").timeBased().everyDays(1).atHour(8).nearMinute(20).create();
        if (cfg.ENABLE_SIGNAL_CLOSE_UPDATER) {
          createSignalCloseUpdaterTrigger_(cfg.SIGNAL_CLOSE_UPDATER_MINUTES);
        } else {
          clearSignalCloseUpdaterTriggerMetadata_();
        }

        if (desiredState.heartbeatMode === "DAILY") {
          ScriptApp.newTrigger("sendDiscordHeartbeat").timeBased().everyDays(1).atHour(desiredState.heartbeatHour).nearMinute(desiredState.heartbeatMinute).create();
        } else if (desiredState.heartbeatMode === "HOURLY") {
          ScriptApp.newTrigger("sendDiscordHeartbeat").timeBased().everyHours(1).create();
        }
      }
    }

    if (!cadenceUpdate) cadenceUpdate = ensurePipelineTriggerCadence_(pipeMins, "install", { installAction: triggersAlreadyCorrect ? "noop" : "close_updater_recreate" });

    props.setProperty(PROP.PIPELINE_ZERO_STREAK, "0");
    props.setProperty(PROP.PIPELINE_CADENCE_MODE, "NORMAL");
    props.setProperty(PROP.PIPELINE_CADENCE_REASON, "healthy");

    var runDecision = maybeRunPipelineAfterInstall_(cfg, cadenceUpdate);

    log_("INFO", "Triggers installed", {
      pipeline: pipeMins + "m",
      projections: "06:05 + 11:05",
      calibration: "08:20 daily",
      signalCloseUpdaterEnabled: !!cfg.ENABLE_SIGNAL_CLOSE_UPDATER,
      signalCloseUpdaterRequestedMinutes: toInt_(cfg.SIGNAL_CLOSE_UPDATER_MINUTES_REQUESTED, 30),
      signalCloseUpdaterAppliedMinutes: desiredState.signalCloseUpdaterMinutes,
      signalCloseUpdaterPrevMinutes: currentState.signalCloseUpdaterCadenceMinutes,
      signalCloseUpdaterNextMinutes: desiredState.signalCloseUpdaterMinutes,
      signalCloseUpdaterAction: closeUpdaterAction,
      heartbeat_mode: desiredState.heartbeatMode,
      heartbeat_time: (desiredState.heartbeatMode === "DAILY") ? (pad2_(desiredState.heartbeatHour) + ":" + pad2_(desiredState.heartbeatMinute)) : (desiredState.heartbeatMode === "HOURLY" ? "hourly" : "off"),
      reinstallSkipped: triggersAlreadyCorrect,
      postInstallRunReasonCode: runDecision.reasonCode,
      postInstallRunExecuted: !!runDecision.executed
    });

    setPipelineDebounceForMs_(props, 15000, "trigger_maintenance_completed");
  } finally {
    lock.releaseLock();
  }
}



function removeTriggers() {
  var all = ScriptApp.getProjectTriggers();
  var removed = 0;
  for (var i = 0; i < all.length; i++) {
    var fn = all[i].getHandlerFunction();
    if (fn === "runPipeline" || fn === "refreshProjectionsScheduled" || fn === "sendDiscordHeartbeat" || fn === "runDailyCalibration" || fn === "updateSignalLogCloseMetrics") {
      ScriptApp.deleteTrigger(all[i]);
      removed++;
    }
  }
  clearSignalCloseUpdaterTriggerMetadata_();
  log_("INFO", "Triggers removed", { count: removed });
}



function describeDesiredTriggerState_(cfg, pipelineMinutes) {
  var signalCloseUpdaterRequestedMinutes = toInt_(cfg.SIGNAL_CLOSE_UPDATER_MINUTES_REQUESTED, cfg.SIGNAL_CLOSE_UPDATER_MINUTES);
  return {
    pipelineMinutes: normalizePipelineTriggerCadenceMinutes_(pipelineMinutes),
    heartbeatMode: String(cfg.HEARTBEAT_MODE || "DAILY").toUpperCase(),
    heartbeatHour: clampInt_(toInt_(cfg.HEARTBEAT_HOUR, 9), 0, 23),
    heartbeatMinute: clampInt_(toInt_(cfg.HEARTBEAT_MINUTE, 5), 0, 59),
    signalCloseUpdaterEnabled: !!cfg.ENABLE_SIGNAL_CLOSE_UPDATER,
    signalCloseUpdaterMinutes: normalizePipelineTriggerCadenceMinutes_(signalCloseUpdaterRequestedMinutes)
  };
}

function describeCurrentTriggerState_() {
  var all = ScriptApp.getProjectTriggers();
  var counts = {
    runPipeline: 0,
    refreshProjectionsScheduled: 0,
    sendDiscordHeartbeat: 0,
    runDailyCalibration: 0,
    updateSignalLogCloseMetrics: 0
  };
  var closeUpdaterTrigger = null;
  for (var i = 0; i < all.length; i++) {
    var fn = String(all[i].getHandlerFunction() || "");
    if (counts[fn] !== undefined) counts[fn]++;
    if (!closeUpdaterTrigger && fn === "updateSignalLogCloseMetrics") closeUpdaterTrigger = all[i];
  }
  var props = PropertiesService.getScriptProperties();
  var storedCloseUpdaterMinutesRaw = toInt_(props.getProperty(PROP.SIGNAL_CLOSE_UPDATER_CADENCE_MINUTES), 0);
  var storedCloseUpdaterMinutes = storedCloseUpdaterMinutesRaw > 0 ? normalizePipelineTriggerCadenceMinutes_(storedCloseUpdaterMinutesRaw) : 0;
  var storedCloseUpdaterSignature = String(props.getProperty(PROP.SIGNAL_CLOSE_UPDATER_TRIGGER_SIGNATURE) || "");
  counts.signalCloseUpdaterCadenceMinutes = storedCloseUpdaterMinutes;
  counts.signalCloseUpdaterSignature = storedCloseUpdaterSignature;
  counts.signalCloseUpdaterHandlerSignature = triggerSignature_(closeUpdaterTrigger);
  counts.closeUpdaterHasMetadata = storedCloseUpdaterMinutes > 0 && !!storedCloseUpdaterSignature;
  return counts;
}

function isTriggerInstallStateMatch_(desiredState, currentState) {
  var expectedHeartbeatCount = desiredState.heartbeatMode === "OFF" ? 0 : 1;
  var expectedCloseUpdaterCount = desiredState.signalCloseUpdaterEnabled ? 1 : 0;
  var baseInstallStateMatch = currentState.runPipeline === 1 &&
    currentState.refreshProjectionsScheduled === 2 &&
    currentState.runDailyCalibration === 1 &&
    currentState.sendDiscordHeartbeat === expectedHeartbeatCount;
  var closeUpdaterCountMatch = currentState.updateSignalLogCloseMetrics === expectedCloseUpdaterCount;
  var closeUpdaterCadenceMatch = !desiredState.signalCloseUpdaterEnabled ||
    (closeUpdaterCountMatch && currentState.signalCloseUpdaterCadenceMinutes === desiredState.signalCloseUpdaterMinutes && currentState.closeUpdaterHasMetadata);
  currentState.baseInstallStateMatch = baseInstallStateMatch;
  currentState.closeUpdaterNeedsRecreate = desiredState.signalCloseUpdaterEnabled && (!closeUpdaterCountMatch || !closeUpdaterCadenceMatch);
  return baseInstallStateMatch && closeUpdaterCountMatch && closeUpdaterCadenceMatch;
}

function createSignalCloseUpdaterTrigger_(minutes) {
  var cadenceMinutes = normalizePipelineTriggerCadenceMinutes_(minutes);
  ScriptApp.newTrigger("updateSignalLogCloseMetrics").timeBased().everyMinutes(cadenceMinutes).create();
  var props = PropertiesService.getScriptProperties();
  props.setProperty(PROP.SIGNAL_CLOSE_UPDATER_CADENCE_MINUTES, String(cadenceMinutes));
  props.setProperty(PROP.SIGNAL_CLOSE_UPDATER_TRIGGER_SIGNATURE, "updateSignalLogCloseMetrics|CLOCK|everyMinutes:" + cadenceMinutes);
}

function clearSignalCloseUpdaterTriggerMetadata_() {
  var props = PropertiesService.getScriptProperties();
  props.deleteProperty(PROP.SIGNAL_CLOSE_UPDATER_CADENCE_MINUTES);
  props.deleteProperty(PROP.SIGNAL_CLOSE_UPDATER_TRIGGER_SIGNATURE);
}

function recreateSignalCloseUpdaterTrigger_(minutes) {
  var cadenceMinutes = toInt_(minutes, 0);
  var all = ScriptApp.getProjectTriggers();
  for (var i = 0; i < all.length; i++) {
    if (all[i].getHandlerFunction() === "updateSignalLogCloseMetrics") ScriptApp.deleteTrigger(all[i]);
  }
  if (cadenceMinutes > 0) createSignalCloseUpdaterTrigger_(cadenceMinutes);
  else clearSignalCloseUpdaterTriggerMetadata_();
}

function registerDuplicateRunPrevented_(props, reasonCode, detailObj) {
  var next = Math.max(0, toInt_(props.getProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED), 0)) + 1;
  props.setProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED, String(next));
  var detail = detailObj || {};
  detail.reasonCode = String(reasonCode || "unspecified");
  detail.duplicateRunPreventedCount = next;
  log_("INFO", "Duplicate-run prevented", detail);
  return next;
}

function pipelineDebounceState_(props, nowMs) {
  var currentMs = Math.max(0, toInt_(nowMs, Date.now()));
  var untilMs = Math.max(0, toInt_(props.getProperty(PROP.PIPELINE_RUN_DEBOUNCE_UNTIL_MS), 0));
  return {
    nowMs: currentMs,
    nowIso: new Date(currentMs).toISOString(),
    untilMs: untilMs,
    untilIso: untilMs > 0 ? new Date(untilMs).toISOString() : "",
    active: untilMs > currentMs,
    remainingMs: Math.max(0, untilMs - currentMs)
  };
}

function setPipelineDebounceForMs_(props, durationMs, reasonCode) {
  var nowMs = Date.now();
  var targetUntilMs = nowMs + Math.max(0, toInt_(durationMs, 0));
  var existingUntilMs = Math.max(0, toInt_(props.getProperty(PROP.PIPELINE_RUN_DEBOUNCE_UNTIL_MS), 0));
  var finalUntilMs = Math.max(targetUntilMs, existingUntilMs);
  props.setProperty(PROP.PIPELINE_RUN_DEBOUNCE_UNTIL_MS, String(finalUntilMs));
  log_("INFO", "Pipeline debounce updated", {
    reasonCode: String(reasonCode || "unspecified"),
    debounceUntil: new Date(finalUntilMs).toISOString(),
    addedMs: Math.max(0, finalUntilMs - nowMs)
  });
  return finalUntilMs;
}

function maybeRunPipelineAfterInstall_(cfg, cadenceUpdate) {
  var props = PropertiesService.getScriptProperties();
  var nowMs = Date.now();
  var debounce = pipelineDebounceState_(props, nowMs);
  var decision = { executed: false, reasonCode: "SKIPPED_DEBOUNCE_ACTIVE" };
  var cadenceUnchangedNoop = shouldSkipPostInstallImmediateRun_(cadenceUpdate);

  if (debounce.active) {
    registerDuplicateRunPrevented_(props, decision.reasonCode, {
      triggerPath: "install",
      debounceUntil: debounce.untilIso,
      remainingMs: debounce.remainingMs
    });
    log_("INFO", "Post-install immediate run skipped", {
      reasonCode: decision.reasonCode,
      cadenceChanged: cadenceUpdate ? !!cadenceUpdate.changed : false
    });
    return decision;
  }

  if (cadenceUnchangedNoop) {
    decision.reasonCode = "SKIPPED_NOOP_INSTALL_RUN";
    registerDuplicateRunPrevented_(props, decision.reasonCode, {
      triggerPath: "install",
      cadenceChanged: false,
      cadenceMinutes: cadenceUpdate ? cadenceUpdate.appliedCadenceMinutes : "",
      triggerSignatureChanged: false,
      installAction: cadenceUpdate ? cadenceUpdate.installAction : ""
    });
    log_("INFO", "Post-install immediate run skipped", {
      reasonCode: decision.reasonCode,
      cadenceChanged: false,
      cadenceMinutes: cadenceUpdate ? cadenceUpdate.appliedCadenceMinutes : "",
      triggerSignatureChanged: false,
      installAction: cadenceUpdate ? cadenceUpdate.installAction : ""
    });
    return decision;
  }

  decision.reasonCode = (cadenceUpdate && cadenceUpdate.changed) ? "EXECUTED_INSTALL_CADENCE_CHANGED" : "EXECUTED_INSTALL_NOOP_CADENCE";
  log_("INFO", "Post-install immediate run executing", {
    reasonCode: decision.reasonCode,
    cadenceChanged: cadenceUpdate ? !!cadenceUpdate.changed : false,
    cadenceMinutes: cadenceUpdate ? cadenceUpdate.appliedCadenceMinutes : ""
  });
  runPipeline({ source: "install", reasonCode: decision.reasonCode, cfgOverride: cfg, skipLock: true });
  decision.executed = true;
  return decision;
}

function shouldSkipPostInstallImmediateRun_(cadenceUpdate) {
  return !!(cadenceUpdate && cadenceUpdate.installAction === "noop" && !cadenceUpdate.changed && cadenceUpdate.signatureUnchanged);
}

function triggerSignature_(trigger) {
  if (!trigger) return "";
  var fn = String(trigger.getHandlerFunction ? trigger.getHandlerFunction() : "");
  var source = String(trigger.getEventType ? trigger.getEventType() : "");
  var unique = String(trigger.getUniqueId ? trigger.getUniqueId() : "");
  return fn + "|" + source + "|" + unique;
}

function pipelineTriggerStateSnapshot_(allTriggers, targetMins) {
  var desiredMins = normalizePipelineTriggerCadenceMinutes_(targetMins);
  var list = allTriggers || ScriptApp.getProjectTriggers();
  var pipelineTriggers = [];
  for (var i = 0; i < list.length; i++) {
    if (list[i].getHandlerFunction() === "runPipeline") pipelineTriggers.push(list[i]);
  }

  var props = PropertiesService.getScriptProperties();
  var storedMins = normalizePipelineTriggerCadenceMinutes_(toInt_(props.getProperty(PROP.PIPELINE_CADENCE_MINUTES), desiredMins));
  var storedSignature = String(props.getProperty(PROP.PIPELINE_TRIGGER_SIGNATURE) || "");
  var expectedSignature = "runPipeline|CLOCK|everyMinutes:" + desiredMins;
  var existing = pipelineTriggers[0] || null;
  var actualHandlerSignature = triggerSignature_(existing);
  var existingSignature = storedSignature || ("runPipeline|CLOCK|everyMinutes:" + storedMins);
  var isMatch = pipelineTriggers.length === 1 && storedMins === desiredMins && (!storedSignature || storedSignature === expectedSignature);

  return {
    triggerCount: pipelineTriggers.length,
    existingSignature: existingSignature,
    expectedSignature: expectedSignature,
    isMatch: isMatch,
    candidateTriggerSignature: actualHandlerSignature,
    storedCadenceMinutes: storedMins,
    desiredCadenceMinutes: desiredMins
  };
}

/* ===================== DISCORD ===================== */

var DISCORD_MESSAGE_DIVIDER = "────────────────────────";

function getDiscordWebhook_(cfg) {
  return (cfg && cfg.DISCORD_WEBHOOK ? cfg.DISCORD_WEBHOOK : "") ||
    PropertiesService.getScriptProperties().getProperty(PROP.DISCORD_WEBHOOK) || "";
}

function getDiscordBotConfig_(cfg) {
  var props = PropertiesService.getScriptProperties();
  var tokenRaw = (cfg && cfg.DISCORD_BOT_TOKEN ? cfg.DISCORD_BOT_TOKEN : "") ||
    props.getProperty(PROP.DISCORD_BOT_TOKEN) || "";
  var channelId = (cfg && cfg.DISCORD_CHANNEL_ID ? cfg.DISCORD_CHANNEL_ID : "") ||
    props.getProperty(PROP.DISCORD_CHANNEL_ID) || "";
  var tokenInfo = normalizeDiscordBotToken_(tokenRaw);
  return {
    token: tokenInfo.token,
    channelId: String(channelId || "").trim(),
    tokenHadPrefix: tokenInfo.hadPrefix,
    tokenLength: tokenInfo.token.length
  };
}

function normalizeDiscordBotToken_(rawToken) {
  var s = String(rawToken || "").trim();
  var hadPrefix = /^bot\s+/i.test(s);
  if (hadPrefix) s = s.replace(/^bot\s+/i, "").trim();
  return { token: s, hadPrefix: hadPrefix };
}

function discordBotAuthHeader_(botToken) {
  var t = String(botToken || "").trim();
  return "Bot " + t;
}

function sendDiscordWebhook_(webhook, payloadObj) {
  var endpoint = String(webhook || "");
  endpoint += (endpoint.indexOf("?") >= 0 ? "&" : "?") + "wait=true";
  var resp = UrlFetchApp.fetch(endpoint, {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payloadObj),
    muteHttpExceptions: true
  });
  return { http: resp.getResponseCode(), body: resp.getContentText(), deliveryMode: "webhook" };
}

function sendDiscordBotMessage_(botCfg, payloadObj) {
  var endpoint = "https://discord.com/api/v10/channels/" + encodeURIComponent(botCfg.channelId) + "/messages";
  var operation = "post_message";
  var maxAttempts = 3;
  var attemptCount = 0;
  var cumulativeWaitMs = 0;
  var finalResp = null;
  var finalBody = "";
  var finalHttp = 0;
  for (var attempt = 1; attempt <= maxAttempts; attempt++) {
    attemptCount = attempt;
    var resp = UrlFetchApp.fetch(endpoint, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payloadObj),
      headers: { Authorization: discordBotAuthHeader_(botCfg.token) },
      muteHttpExceptions: true
    });
    var http = resp.getResponseCode();
    var body = resp.getContentText();
    finalResp = resp;
    finalBody = body;
    finalHttp = http;

    var retryMeta = discordRetryMeta_(http, body, {
      attempt: attempt,
      operation: operation,
      defaultMaxAttempts: 3
    });
    maxAttempts = Math.max(maxAttempts, retryMeta.maxAttempts);
    if (attempt >= maxAttempts || !retryMeta.retry) break;

    log_("WARN", "Discord bot send retry scheduled", {
      deliveryMode: "bot_channel",
      operation: operation,
      attempt: attempt,
      maxAttempts: maxAttempts,
      http: http,
      discordCode: retryMeta.discordCode,
      retryAfterMs: retryMeta.retryAfterMs,
      body: String(body || "").slice(0, 300)
    });
    cumulativeWaitMs += retryMeta.retryAfterMs;
    Utilities.sleep(retryMeta.retryAfterMs);
  }

  var finalMeta = discordRetryMeta_(finalHttp, finalBody, {
    attempt: attemptCount,
    operation: operation,
    defaultMaxAttempts: 3
  });
  if (finalMeta.retry && attemptCount >= maxAttempts) {
    log_("ERROR", "Discord bot send retries exhausted", {
      deliveryMode: "bot_channel",
      operation: operation,
      endpoint: endpoint,
      totalAttempts: attemptCount,
      maxAttempts: maxAttempts,
      cumulativeWaitMs: cumulativeWaitMs,
      http: finalHttp,
      discordCode: finalMeta.discordCode,
      body: String(finalBody || "").slice(0, 300)
    });
  }

  return {
    http: finalHttp,
    body: finalBody,
    deliveryMode: "bot_channel",
    attempts: (finalResp ? attemptCount : 0)
  };
}

function discordRetryMeta_(http, bodyText, ctx) {
  var context = ctx || {};
  var attempt = Math.max(1, toInt_(context.attempt, 1));
  var operation = String(context.operation || "unknown");
  var defaultMaxAttempts = Math.max(1, toInt_(context.defaultMaxAttempts, 3));
  var body = String(bodyText || "");
  var parsed = null;
  var discordCode = "";
  var retryAfterMs = 0;
  var maxAttempts = defaultMaxAttempts;

  try { parsed = JSON.parse(body); } catch (e) { parsed = null; }
  if (parsed && parsed.code !== undefined && parsed.code !== null) discordCode = String(parsed.code);

  if (http === 429 && parsed && parsed.retry_after !== undefined) {
    var ra = Number(parsed.retry_after);
    if (isFinite(ra) && ra >= 0) {
      retryAfterMs = (ra > 20) ? Math.round(ra) : Math.round(ra * 1000);
    }
  }

  var retry = false;
  if (http >= 500 && http <= 599) retry = true;
  if (http === 429) retry = true;
  if (http === 403 && discordCode === "40333") retry = true;
  if (http === 403 && discordCode === "40333") maxAttempts = Math.max(maxAttempts, 5);

  if (retryAfterMs <= 0) {
    if (http === 429) retryAfterMs = 1200;
    else if (http >= 500) retryAfterMs = 1000;
    else if (http === 403 && discordCode === "40333") {
      var baseDelayMs = Math.round(900 * Math.pow(1.6, attempt - 1));
      var jitterMs = Math.floor(Math.random() * 240) - 120;
      retryAfterMs = baseDelayMs + jitterMs;
    }
    else retryAfterMs = 800;
  }

  retryAfterMs = Math.max(200, Math.min(5000, retryAfterMs));
  return {
    retry: retry,
    retryAfterMs: retryAfterMs,
    discordCode: discordCode,
    maxAttempts: maxAttempts,
    operation: operation
  };
}

function discordBotRequest_(botCfg, method, endpoint, payloadObj) {
  var options = {
    method: method,
    headers: { Authorization: discordBotAuthHeader_(botCfg.token) },
    muteHttpExceptions: true
  };
  if (payloadObj) {
    options.contentType = "application/json";
    options.payload = JSON.stringify(payloadObj);
  }
  var resp = UrlFetchApp.fetch(endpoint, options);
  return { http: resp.getResponseCode(), body: resp.getContentText() };
}

function discordErrorCodeFromBody_(bodyText) {
  var parsed = null;
  try { parsed = JSON.parse(String(bodyText || "")); } catch (e) { parsed = null; }
  if (parsed && parsed.code !== undefined && parsed.code !== null) return String(parsed.code);
  return "";
}

function runDiscordBotPreflight_(botCfg) {
  var tokenLen = String(botCfg.token || "").length;
  var channelId = String(botCfg.channelId || "").trim();
  var channelLooksValid = /^\d{16,22}$/.test(channelId);

  log_("INFO", "Discord bot preflight config", {
    deliveryMode: "bot_channel",
    tokenLength: tokenLen,
    tokenHadPrefix: !!botCfg.tokenHadPrefix,
    channelIdLength: channelId.length,
    channelIdLooksNumeric: channelLooksValid
  });

  var meRes = discordBotRequest_(botCfg, "get", "https://discord.com/api/v10/users/@me", null);
  log_((meRes.http >= 200 && meRes.http < 300) ? "INFO" : "WARN", "Discord bot preflight /users/@me", {
    deliveryMode: "bot_channel",
    http: meRes.http,
    body: String(meRes.body || "").slice(0, 300)
  });

  var authOk = (meRes.http >= 200 && meRes.http < 300);
  var channelEndpoint = "https://discord.com/api/v10/channels/" + encodeURIComponent(channelId);
  var probeCount = 3;
  var channelReachable = false;
  var channelErrorCode = "";
  var isLikelyDiscordInternal = false;
  var finalChannelHttp = 0;

  for (var probe = 1; probe <= probeCount; probe++) {
    var chRes = discordBotRequest_(botCfg, "get", channelEndpoint, null);
    var probeCode = discordErrorCodeFromBody_(chRes.body);
    finalChannelHttp = chRes.http;
    if (probeCode) channelErrorCode = probeCode;
    if (chRes.http >= 200 && chRes.http < 300) channelReachable = true;
    if (chRes.http === 403 && probeCode === "40333") isLikelyDiscordInternal = true;

    log_((chRes.http >= 200 && chRes.http < 300) ? "INFO" : "WARN", "Discord bot preflight /channels/{id} probe", {
      deliveryMode: "bot_channel",
      probe: probe,
      probeCount: probeCount,
      http: chRes.http,
      discordCode: probeCode,
      body: String(chRes.body || "").slice(0, 300)
    });

    if (probe < probeCount) {
      var jitterMs = 120 + Math.floor(Math.random() * 220);
      Utilities.sleep(jitterMs);
    }
  }

  return {
    ok: authOk && channelReachable,
    authOk: authOk,
    channelReachable: channelReachable,
    channelErrorCode: channelErrorCode,
    isLikelyDiscordInternal: isLikelyDiscordInternal,
    meHttp: meRes.http,
    channelHttp: finalChannelHttp
  };
}

function discordPreflightAlertMessage_(preflight, heading) {
  if (preflight.ok) {
    return heading + " passed ✅\n\n/users/@me HTTP: " + preflight.meHttp + "\n/channels/{id} HTTP: " + preflight.channelHttp;
  }

  var base = heading + " failed ❌\n\n/users/@me HTTP: " + preflight.meHttp + "\n/channels/{id} HTTP: " + preflight.channelHttp;
  if (preflight.channelErrorCode) base += "\nDiscord code: " + preflight.channelErrorCode;

  if (!preflight.authOk) {
    return base + "\n\nToken/auth failure: verify DISCORD_BOT_TOKEN (no extra spaces/prefix issues), then retry.";
  }
  if (preflight.isLikelyDiscordInternal) {
    return base + "\n\nDiscord internal/network failure detected (40333). Retry later and check Discord status if it persists.";
  }
  if (!preflight.channelReachable) {
    return base + "\n\nChannel permission failure: verify DISCORD_CHANNEL_ID and bot channel access/permissions.";
  }
  return base + "\n\nCheck LOG for details.";
}

function discordDeliveryMode_(cfg, opts) {
  var options = opts || {};
  var allowWebhook = (options.allowWebhook !== false);
  var webhook = getDiscordWebhook_(cfg);

  if (allowWebhook && webhook) return { mode: "webhook", botCfg: null, webhook: webhook };
  if (allowWebhook) return { mode: "missing", botCfg: null, webhook: "" };
  return { mode: "missing", botCfg: null, webhook: "" };
}

function sendDiscordByMode_(deliveryMode, payloadObj) {
  if (deliveryMode.mode === "bot_channel") return sendDiscordBotMessage_(deliveryMode.botCfg, payloadObj);
  if (deliveryMode.mode === "webhook") return sendDiscordWebhook_(deliveryMode.webhook, payloadObj);
  return { http: 0, body: "", deliveryMode: deliveryMode.mode };
}

function sendDiscordTestPing() {
  var cfg = getConfig_();
  var deliveryMode = discordDeliveryMode_(cfg, { allowWebhook: true });
  var ui = SpreadsheetApp.getUi();

  if (deliveryMode.mode === "missing") {
    log_("ERROR", "Discord test ping failed: missing delivery config", {});
    ui.alert("Discord test ping failed.\n\nSet DISCORD_WEBHOOK in SETTINGS and try again.");
    return;
  }

  var ss = SpreadsheetApp.getActive();
  var payload = {
    content:
      "✅ **Lucky Luciano MLB — Discord Test Ping**\n" +
      DISCORD_MESSAGE_DIVIDER + "\n" +
      "**Sheet:** " + ss.getName() + "\n" +
      "**Local:** " + localPretty_(new Date()) + "\n" +
      "**UTC:** " + new Date().toISOString() + "\n" +
      DISCORD_MESSAGE_DIVIDER + "\n" +
      "_If you see this, Discord delivery is connected and posting._"
  };

  var res = sendDiscordByMode_(deliveryMode, payload);
  var bodyPreview = String(res.body || "").slice(0, 300);
  if (res.http >= 200 && res.http < 300) {
    log_("INFO", "Discord test ping sent", { http: res.http, body: bodyPreview, deliveryMode: res.deliveryMode });
    ui.alert("Discord test ping sent ✅\n\nCheck your Discord channel.");
  } else {
    log_("WARN", "Discord test ping failed", { http: res.http, body: bodyPreview, deliveryMode: res.deliveryMode });
    ui.alert("Discord test ping failed ❌\n\nHTTP: " + res.http + "\nCheck LOG for details.");
  }
}


function sendDiscordActionButtonsTest() {
  var cfg = getConfig_();
  var deliveryMode = discordDeliveryMode_(cfg, { allowWebhook: false });
  var ui = SpreadsheetApp.getUi();

  if (deliveryMode.mode !== "bot_channel") {
    log_("ERROR", "Discord action test failed: missing bot config", { deliveryMode: deliveryMode.mode });
    ui.alert("Discord action test failed.\n\nSet DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID in SETTINGS and try again.");
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
      DISCORD_MESSAGE_DIVIDER + "\n" +
      "Use this test message to validate Discord buttons and Web App routing.\n" +
      DISCORD_MESSAGE_DIVIDER + "\n" +
      "This does **not** create or update legacy tracking logs.",
    components: [{
      type: 1,
      components: [
        { type: 2, style: 5, label: "Open Action Test", url: testUrl }
      ]
    }]
  };

  var includesComponents = !!(payloadObj && payloadObj.components && payloadObj.components.length);

  var res = sendDiscordByMode_(deliveryMode, payloadObj);
  var bodyPreview = String(res.body || "").slice(0, 300);
  if (res.http >= 200 && res.http < 300) {
    log_("INFO", "Discord action buttons test sent", {
      action: "test",
      deliveryMode: res.deliveryMode,
      http: res.http,
      body: bodyPreview,
      includesComponents: includesComponents
    });
    ui.alert("Discord action buttons test sent ✅\n\nClick the button in Discord to validate the web app route.");
  } else {
    log_("WARN", "Discord action buttons test failed", {
      action: "test",
      deliveryMode: res.deliveryMode,
      http: res.http,
      body: bodyPreview,
      includesComponents: includesComponents
    });
    ui.alert("Discord action buttons test failed ❌\n\nHTTP: " + res.http + "\nCheck LOG for details.");
  }
}

function sendDiscordActionPayloadDiagnostics() {
  var cfg = getConfig_();
  var deliveryMode = discordDeliveryMode_(cfg, { allowWebhook: false });
  var ui = SpreadsheetApp.getUi();

  if (deliveryMode.mode !== "bot_channel") {
    log_("ERROR", "Discord diagnostics failed: missing bot config", { deliveryMode: deliveryMode.mode });
    ui.alert("Discord diagnostics failed.\n\nSet DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID in SETTINGS and try again.");
    return;
  }

  var preflight = runDiscordBotPreflight_(deliveryMode.botCfg);
  if (!preflight.ok) {
    ui.alert(discordPreflightAlertMessage_(preflight, "Discord diagnostics preflight"));
    return;
  }

  var baseUrl = String(cfg.WEB_APP_URL || "").trim();
  if (!baseUrl) {
    log_("ERROR", "Discord diagnostics failed: missing WEB_APP_URL", {});
    ui.alert("Discord diagnostics failed.\n\nSet WEB_APP_URL in SETTINGS and try again.");
    return;
  }

  var token = Utilities.getUuid();
  var testUrl = baseUrl + "?action=test&token=" + encodeURIComponent(token);

  var diagnosticsPayloads = [
    {
      name: "content_only",
      payload: {
        content:
          "🧪 **Discord Diagnostics — Content Only**\n" +
          DISCORD_MESSAGE_DIVIDER + "\n" +
          "This probe includes only message content so operators can compare bot rendering behavior."
      }
    },
    {
      name: "content_with_components",
      payload: {
        content:
          "🧪 **Discord Diagnostics — Content + Components**\n" +
          DISCORD_MESSAGE_DIVIDER + "\n" +
          "This probe includes a link button to verify component rendering via bot posts.",
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
    var res = sendDiscordByMode_(deliveryMode, diag.payload);
    var bodyPreview = String(res.body || "").slice(0, 300);
    var level = (res.http >= 200 && res.http < 300) ? "INFO" : "WARN";
    if (level !== "INFO") allOk = false;

    log_(level, "Discord diagnostics payload sent", {
      payloadType: diag.name,
      deliveryMode: res.deliveryMode,
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

function sendDiscordBotPreflightDiagnostics() {
  var cfg = getConfig_();
  var deliveryMode = discordDeliveryMode_(cfg, { allowWebhook: false });
  var ui = SpreadsheetApp.getUi();

  if (deliveryMode.mode !== "bot_channel") {
    log_("ERROR", "Discord bot preflight failed: missing bot config", { deliveryMode: deliveryMode.mode });
    ui.alert("Discord bot preflight failed.\n\nSet DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID in SETTINGS and try again.");
    return;
  }

  var preflight = runDiscordBotPreflight_(deliveryMode.botCfg);
  ui.alert(discordPreflightAlertMessage_(preflight, "Discord bot preflight"));
}

function sendDiscordHeartbeat() {
  var cfg = getConfig_();
  var deliveryMode = discordDeliveryMode_(cfg, { allowWebhook: true });
  if (deliveryMode.mode === "missing") { log_("ERROR", "Heartbeat skipped: missing Discord delivery config", {}); return; }

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
  var lastCalibrationSummary = props.getProperty(PROP.LAST_CALIBRATION_SUMMARY) || "";

  var msg =
    "🫀 **Lucky Luciano MLB — Heartbeat (" + mode + ")**\n" +
    DISCORD_MESSAGE_DIVIDER + "\n" +
    "**Local:** " + localPretty_(new Date()) + "\n" +
    "**UTC:** " + new Date().toISOString() + "\n";

  if (lastAt) msg += "**Last pipeline (UTC):** " + lastAt + "\n";
  if (lastStatus) msg += "**Last status:** " + lastStatus + "\n";
  if (lastSummary) msg += "**Last summary:** " + lastSummary + "\n";
  if (lastCalibrationSummary) msg += "**Calibration:** " + lastCalibrationSummary + "\n";
  msg += DISCORD_MESSAGE_DIVIDER + "\n";
  msg += "_If you see this, triggers + Discord delivery are working._";

  var res = sendDiscordByMode_(deliveryMode, { content: msg });
  if (res.http >= 200 && res.http < 300) {
    props.setProperty(PROP.LAST_HEARTBEAT_KEY, key);
    log_("INFO", "Heartbeat sent", { http: res.http, mode: mode, key: key, deliveryMode: res.deliveryMode, body: String(res.body || "").slice(0, 200) });
  } else {
    log_("WARN", "Heartbeat failed", { http: res.http, body: String(res.body || "").slice(0, 200), deliveryMode: res.deliveryMode });
  }
}

/* ===================== PIPELINE ===================== */

function ensurePipelineTriggerCadence_(minutes, reason, detailObj) {
  var requestedMins = toInt_(minutes, 15);
  var targetMins = normalizePipelineTriggerCadenceMinutes_(requestedMins);
  var all = ScriptApp.getProjectTriggers();
  var snapshot = pipelineTriggerStateSnapshot_(all, targetMins);

  var removed = 0;
  var changed = !snapshot.isMatch;
  if (changed) {
    for (var i = 0; i < all.length; i++) {
      if (all[i].getHandlerFunction() === "runPipeline") {
        ScriptApp.deleteTrigger(all[i]);
        removed++;
      }
    }
    ScriptApp.newTrigger("runPipeline").timeBased().everyMinutes(targetMins).create();
  }

  var props = PropertiesService.getScriptProperties();
  props.setProperty(PROP.PIPELINE_CADENCE_MINUTES, String(targetMins));
  props.setProperty(PROP.PIPELINE_TRIGGER_SIGNATURE, snapshot.expectedSignature);

  var detail = detailObj || {};
  detail.reason = String(reason || "unspecified");
  detail.reinstallSkipped = !changed;
  detail.triggerStateUnchanged = !changed;
  detail.installAction = String(detail.installAction || "unspecified");
  detail.skipReasonCode = !changed ? "NOOP_INSTALL_TRIGGER_STATE_UNCHANGED" : "";
  detail.signatureUnchanged = snapshot.existingSignature === snapshot.expectedSignature;
  detail.removedTriggers = removed;
  detail.existingTriggerCount = snapshot.triggerCount;
  detail.existingTriggerSignature = snapshot.existingSignature;
  detail.desiredTriggerSignature = snapshot.expectedSignature;
  detail.candidateTriggerSignature = snapshot.candidateTriggerSignature;
  detail.requestedCadenceMinutes = requestedMins;
  detail.appliedCadenceMinutes = targetMins;
  detail.pipelineCadenceMinutes = targetMins;
  log_("INFO", "Pipeline trigger cadence updated", detail);

  return {
    changed: changed,
    installAction: detail.installAction,
    signatureUnchanged: snapshot.existingSignature === snapshot.expectedSignature,
    skipReasonCode: detail.skipReasonCode,
    removedTriggers: removed,
    requestedCadenceMinutes: requestedMins,
    appliedCadenceMinutes: targetMins,
    expectedSignature: snapshot.expectedSignature
  };
}

function normalizePipelineTriggerCadenceMinutes_(minutes) {
  var valid = [1, 5, 10, 15, 30];
  var requested = Math.max(1, toInt_(minutes, 15));
  var applied = valid[0];
  for (var i = 0; i < valid.length; i++) {
    if (valid[i] <= requested) applied = valid[i];
    else break;
  }
  return applied;
}

function updatePipelineCadenceState_(cfg, props, matchedCount, computedCount) {
  var matched = Math.max(0, toInt_(matchedCount, 0));
  var computed = Math.max(0, toInt_(computedCount, 0));
  var zeroDataRun = (matched === 0 || computed === 0);

  var streak = Math.max(0, toInt_(props.getProperty(PROP.PIPELINE_ZERO_STREAK), 0));
  streak = zeroDataRun ? (streak + 1) : 0;
  props.setProperty(PROP.PIPELINE_ZERO_STREAK, String(streak));

  var baseMinsRequested = Math.max(1, toInt_(cfg.PIPELINE_MINUTES, 15));
  var baseMins = normalizePipelineTriggerCadenceMinutes_(baseMinsRequested);
  var level1Threshold = Math.max(1, toInt_(cfg.PIPELINE_DEGRADE_ZERO_STREAK_THRESHOLD, 3));
  var level2Threshold = Math.max(level1Threshold, toInt_(cfg.PIPELINE_DEGRADE_LEVEL2_THRESHOLD, 6));
  var level1Requested = Math.max(baseMinsRequested, toInt_(cfg.PIPELINE_DEGRADE_MINUTES_L1, 30));
  var level1Mins = normalizePipelineTriggerCadenceMinutes_(level1Requested);
  var level2Requested = Math.max(level1Requested, toInt_(cfg.PIPELINE_DEGRADE_MINUTES_L2, 60));
  var level2Mins = normalizePipelineTriggerCadenceMinutes_(level2Requested);

  var creditWarningThreshold = Math.max(0, toInt_(cfg.PIPELINE_CREDIT_WARNING_THRESHOLD, 75));
  var creditCriticalThreshold = Math.max(0, toInt_(cfg.PIPELINE_CREDIT_CRITICAL_THRESHOLD, 25));
  if (creditCriticalThreshold > creditWarningThreshold) creditCriticalThreshold = creditWarningThreshold;

  var creditWarningRequested = Math.max(baseMinsRequested, toInt_(cfg.PIPELINE_DEGRADE_MINUTES_CREDIT_WARNING, 30));
  var creditWarningMins = normalizePipelineTriggerCadenceMinutes_(creditWarningRequested);
  var creditCriticalRequested = Math.max(creditWarningRequested, toInt_(cfg.PIPELINE_DEGRADE_MINUTES_CREDIT_CRITICAL, 60));
  var creditCriticalMins = normalizePipelineTriggerCadenceMinutes_(creditCriticalRequested);

  var remainingRaw = props.getProperty(PROP.ODDS_LAST_REMAINING_CREDITS);
  var remainingCredits = isFinite(Number(remainingRaw)) ? Math.max(0, toInt_(remainingRaw, 0)) : null;
  var creditPressureLevel = "NONE";
  if (remainingCredits !== null && remainingCredits <= creditCriticalThreshold) {
    creditPressureLevel = "CRITICAL";
  } else if (remainingCredits !== null && remainingCredits <= creditWarningThreshold) {
    creditPressureLevel = "WARNING";
  }

  var desiredMode = "NORMAL";
  var desiredMinutes = baseMins;
  var desiredRequestedMinutes = baseMinsRequested;
  var cadenceReason = "healthy";

  if (streak >= level2Threshold) {
    desiredMode = "DEGRADED_L2";
    desiredMinutes = level2Mins;
    desiredRequestedMinutes = level2Requested;
    cadenceReason = "zero_data_streak";
  } else if (streak >= level1Threshold) {
    desiredMode = "DEGRADED_L1";
    desiredMinutes = level1Mins;
    desiredRequestedMinutes = level1Requested;
    cadenceReason = "zero_data_streak";
  }

  if (creditPressureLevel === "CRITICAL" && creditCriticalMins >= desiredMinutes) {
    desiredMode = "CREDIT_PROTECTION_CRITICAL";
    desiredMinutes = creditCriticalMins;
    desiredRequestedMinutes = creditCriticalRequested;
    cadenceReason = "credit_protection";
  } else if (creditPressureLevel === "WARNING" && creditWarningMins >= desiredMinutes) {
    desiredMode = "CREDIT_PROTECTION_WARNING";
    desiredMinutes = creditWarningMins;
    desiredRequestedMinutes = creditWarningRequested;
    cadenceReason = "credit_protection";
  }

  var prevMode = String(props.getProperty(PROP.PIPELINE_CADENCE_MODE) || "NORMAL");
  var prevReason = String(props.getProperty(PROP.PIPELINE_CADENCE_REASON) || "healthy");
  var prevMinutes = normalizePipelineTriggerCadenceMinutes_(toInt_(props.getProperty(PROP.PIPELINE_CADENCE_MINUTES), baseMins));

  if (prevMode !== desiredMode || prevMinutes !== desiredMinutes) {
    ensurePipelineTriggerCadence_(desiredRequestedMinutes, "auto_degrade", {
      previousMode: prevMode,
      previousReason: prevReason,
      newMode: desiredMode,
      newReason: cadenceReason,
      zeroDataRun: zeroDataRun,
      zeroStreak: streak,
      matched: matched,
      computed: computed,
      remainingCredits: remainingCredits,
      creditPressureLevel: creditPressureLevel
    });
  }

  props.setProperty(PROP.PIPELINE_CADENCE_MODE, desiredMode);
  props.setProperty(PROP.PIPELINE_CADENCE_REASON, cadenceReason);

  if (desiredMode !== "NORMAL") {
    log_("WARN", "Pipeline degraded cadence active", {
      mode: desiredMode,
      cadenceReason: cadenceReason,
      cadenceMinutes: desiredMinutes,
      zeroStreak: streak,
      thresholds: { level1: level1Threshold, level2: level2Threshold },
      remainingCredits: remainingCredits,
      creditThresholds: { warning: creditWarningThreshold, critical: creditCriticalThreshold },
      matched: matched,
      computed: computed
    });
  } else if (prevMode !== "NORMAL") {
    log_("INFO", "Pipeline cadence recovered to normal", {
      previousMode: prevMode,
      previousReason: prevReason,
      cadenceReason: cadenceReason,
      cadenceMinutes: desiredMinutes,
      zeroStreak: streak,
      remainingCredits: remainingCredits,
      matched: matched,
      computed: computed
    });
  }

  return {
    mode: desiredMode,
    reason: cadenceReason,
    cadenceMinutes: desiredMinutes,
    zeroStreak: streak,
    zeroDataRun: zeroDataRun,
    remainingCredits: remainingCredits,
    creditPressureLevel: creditPressureLevel
  };
}


function evaluateOddsFetchBlocker_(cfg, props, nowMs) {
  var threshold = Math.max(0, toInt_(cfg.ODDS_MIN_REMAINING_TO_FETCH, 20));
  var cooldownMin = Math.max(1, toInt_(cfg.ODDS_FETCH_BLOCK_MIN, 120));
  var creditsSnapshotMaxAgeMin = Math.max(1, toInt_(cfg.ODDS_CREDITS_SNAPSHOT_MAX_AGE_MIN, 180));
  var remainingRaw = props.getProperty(PROP.ODDS_LAST_REMAINING_CREDITS);
  var remaining = isFinite(Number(remainingRaw)) ? Math.max(0, toInt_(remainingRaw, 0)) : null;
  var creditsAtMs = Math.max(0, toInt_(props.getProperty(PROP.ODDS_LAST_CREDITS_AT_MS), 0));
  var existingUntilMs = Math.max(0, toInt_(props.getProperty(PROP.ODDS_FETCH_BLOCK_UNTIL_MS), 0));
  var snapshotAgeMs = creditsAtMs > 0 ? Math.max(0, nowMs - creditsAtMs) : -1;
  var snapshotFresh = creditsAtMs > 0 && snapshotAgeMs <= (creditsSnapshotMaxAgeMin * 60000);

  if (!snapshotFresh) {
    if (existingUntilMs > 0) {
      props.deleteProperty(PROP.ODDS_FETCH_BLOCK_UNTIL_MS);
      props.deleteProperty(PROP.ODDS_FETCH_BLOCK_ALERT_SENT_AT_MS);
    }

    return {
      blocked: false,
      engagedNow: false,
      reasonCode: "credits_snapshot_stale_probe_fetch",
      remaining: remaining,
      creditsAtMs: creditsAtMs,
      threshold: threshold,
      cooldownMin: cooldownMin,
      creditsSnapshotMaxAgeMin: creditsSnapshotMaxAgeMin,
      snapshotAgeMs: snapshotAgeMs,
      snapshotFresh: false
    };
  }

  if (existingUntilMs > nowMs) {
    return {
      blocked: true,
      engagedNow: false,
      reasonCode: "credits_snapshot_fresh_blocked",
      remaining: remaining,
      creditsAtMs: creditsAtMs,
      threshold: threshold,
      unblockAtMs: existingUntilMs,
      unblockAtIso: new Date(existingUntilMs).toISOString(),
      cooldownMin: cooldownMin,
      creditsSnapshotMaxAgeMin: creditsSnapshotMaxAgeMin,
      snapshotAgeMs: snapshotAgeMs,
      snapshotFresh: true
    };
  }

  if (existingUntilMs > 0) {
    props.deleteProperty(PROP.ODDS_FETCH_BLOCK_UNTIL_MS);
    props.deleteProperty(PROP.ODDS_FETCH_BLOCK_ALERT_SENT_AT_MS);
  }

  if (remaining === null || remaining >= threshold) {
    return {
      blocked: false,
      engagedNow: false,
      remaining: remaining,
      threshold: threshold,
      cooldownMin: cooldownMin,
      creditsAtMs: creditsAtMs,
      creditsSnapshotMaxAgeMin: creditsSnapshotMaxAgeMin,
      snapshotAgeMs: snapshotAgeMs,
      snapshotFresh: true
    };
  }

  var unblockAtMs = nowMs + (cooldownMin * 60000);
  props.setProperty(PROP.ODDS_FETCH_BLOCK_UNTIL_MS, String(unblockAtMs));
  return {
    blocked: true,
    engagedNow: true,
    reasonCode: "credits_snapshot_fresh_blocked",
    remaining: remaining,
    creditsAtMs: creditsAtMs,
    threshold: threshold,
    unblockAtMs: unblockAtMs,
    unblockAtIso: new Date(unblockAtMs).toISOString(),
    cooldownMin: cooldownMin,
    creditsSnapshotMaxAgeMin: creditsSnapshotMaxAgeMin,
    snapshotAgeMs: snapshotAgeMs,
    snapshotFresh: true
  };
}

function maybeSendOddsFetchBlockerAlert_(cfg, blockState, props) {
  if (!blockState || !blockState.engagedNow || !blockState.blocked) return;
  var alertSentAtMs = Math.max(0, toInt_(props.getProperty(PROP.ODDS_FETCH_BLOCK_ALERT_SENT_AT_MS), 0));
  if (alertSentAtMs > 0) return;

  var cfgLive = getConfig_();
  var deliveryMode = discordDeliveryMode_(cfgLive, { allowWebhook: true });
  if (deliveryMode.mode === "missing") {
    log_("WARN", "Odds fetch blocker alert suppressed", {
      reasonCode: "discord_delivery_missing",
      blockerReasonCode: blockState.reasonCode,
      remaining: blockState.remaining,
      threshold: blockState.threshold,
      unblockAt: blockState.unblockAtIso
    });
    return;
  }

  var payload = {
    content:
      "⚠️ **Odds fetch temporarily blocked (low credits)**\n" +
      "Remaining: **" + String(blockState.remaining) + "** (threshold: " + String(blockState.threshold) + ")\n" +
      "Unblocks at: " + String(blockState.unblockAtIso) + "\n" +
      "Cooldown: " + String(blockState.cooldownMin) + " minutes\n" +
      "Pipeline will continue using cached `ODDS_RAW`."
  };

  var res = sendDiscordByMode_(deliveryMode, payload);
  if (res.http >= 200 && res.http < 300) {
    props.setProperty(PROP.ODDS_FETCH_BLOCK_ALERT_SENT_AT_MS, String(Date.now()));
    log_("WARN", "Odds fetch blocker alert emitted", {
      blockerReasonCode: blockState.reasonCode,
      remaining: blockState.remaining,
      threshold: blockState.threshold,
      unblockAt: blockState.unblockAtIso,
      discordHttp: res.http,
      discordDeliveryMode: res.deliveryMode
    });
    return;
  }

  log_("WARN", "Odds fetch blocker alert failed", {
    blockerReasonCode: blockState.reasonCode,
    remaining: blockState.remaining,
    threshold: blockState.threshold,
    unblockAt: blockState.unblockAtIso,
    discordHttp: res.http,
    discordDeliveryMode: res.deliveryMode,
    discordBody: String(res.body || "").slice(0, 300)
  });
}

function runPipeline(opts) {
  var options = opts || {};
  var runId = [String(Date.now()), Math.floor(Math.random() * 1000000)].join("-");
  var lock = null;
  var hasLock = false;
  var runSummary = {
    summary_schema_version: "1.1.0",
    run_id: runId,
    started_at: new Date().toISOString(),
    finished_at: "",
    duration_ms: 0,
    log_row_start: 0,
    log_row_end: 0,
    outcome: "unknown",
    mode: {
      trigger_source: String(options.source || "scheduled")
    },
    stages: {
      odds: { outcome: "not_started" },
      schedule: { outcome: "not_started" },
      model: { outcome: "not_started" },
      signal: { outcome: "not_started" }
    },
    stage_durations_ms: {},
    cadence: null,
    credit_state: null,
    reason_codes: {
      skips: [],
      blockers: [],
      warnings: []
    }
  };
  runSummary.log_row_start = Math.max(getLogDataRowStart_(), getCurrentLogRowCount_() + 2);

  function addReasonCode_(bucket, code) {
    if (!code) return;
    var key = String(bucket || "");
    if (!runSummary.reason_codes[key]) runSummary.reason_codes[key] = [];
    var target = runSummary.reason_codes[key];
    var normalized = String(code);
    for (var i = 0; i < target.length; i++) if (String(target[i]) === normalized) return;
    target.push(normalized);
  }

  function stageWarnThresholdMs_(cfg, stageName) {
    var genericMs = Math.max(1000, toFloat_(cfg.PIPELINE_STAGE_WARN_SEC, 20) * 1000);
    if (stageName === "odds_fetch") return Math.max(1000, toFloat_(cfg.PIPELINE_STAGE_WARN_ODDS_FETCH_SEC, cfg.PIPELINE_STAGE_WARN_SEC || 20) * 1000);
    if (stageName === "model") return Math.max(1000, toFloat_(cfg.PIPELINE_STAGE_WARN_MODEL_SEC, cfg.PIPELINE_STAGE_WARN_SEC || 20) * 1000);
    return genericMs;
  }

  function finalizeStage_(stageName, startedAtMs, endedAtMs, extras) {
    var startMs = Math.max(0, toInt_(startedAtMs, 0));
    var endMs = Math.max(startMs, toInt_(endedAtMs, Date.now()));
    var durationMs = Math.max(0, endMs - startMs);
    var detail = {
      started_at: new Date(startMs).toISOString(),
      ended_at: new Date(endMs).toISOString(),
      duration_ms: durationMs
    };
    var extraObj = extras || {};
    var extraKeys = Object.keys(extraObj);
    for (var i = 0; i < extraKeys.length; i++) detail[extraKeys[i]] = extraObj[extraKeys[i]];
    runSummary.stage_durations_ms[stageName] = durationMs;
    runSummary.stages[stageName] = detail;
    return detail;
  }

  function applyStageDurationWarnings_(cfg, props) {
    var stages = runSummary.stages || {};
    var stageKeys = Object.keys(stages);
    var raw = String(props.getProperty(PROP.PIPELINE_STAGE_DURATION_EMA_MS) || "");
    var emaMap = {};
    try { emaMap = raw ? JSON.parse(raw) : {}; } catch (eEma) { emaMap = {}; }

    var alpha = clamp_(0.01, 1, toFloat_(cfg.PIPELINE_STAGE_DURATION_EMA_ALPHA, 0.2));
    var spikeMultiplier = Math.max(1.1, toFloat_(cfg.PIPELINE_STAGE_DRIFT_SPIKE_MULTIPLIER, 2.0));
    var spikeMinMs = Math.max(500, toInt_(cfg.PIPELINE_STAGE_DRIFT_SPIKE_MIN_MS, 5000));
    var spikes = [];

    for (var i = 0; i < stageKeys.length; i++) {
      var stageName = String(stageKeys[i] || "");
      var stage = stages[stageName] || {};
      var durationMsRaw = toInt_(stage.duration_ms, -1);
      if (durationMsRaw < 0) continue;
      var durationMs = Math.max(0, durationMsRaw);

      var thresholdMs = stageWarnThresholdMs_(cfg, stageName);
      if (durationMs > thresholdMs) addReasonCode_("warnings", "stage_" + stageName + "_duration_warn");

      var prevAvg = toFloat_(emaMap[stageName], NaN);
      var hasPrev = isFinite(prevAvg) && prevAvg > 0;
      var nextAvg = hasPrev ? ((alpha * durationMs) + ((1 - alpha) * prevAvg)) : durationMs;
      emaMap[stageName] = round_(nextAvg, 3);

      if (hasPrev && durationMs > (prevAvg * spikeMultiplier) && (durationMs - prevAvg) >= spikeMinMs) {
        var driftCode = "stage_" + stageName + "_drift_spike";
        addReasonCode_("warnings", driftCode);
        spikes.push({
          stage: stageName,
          reason_code: driftCode,
          duration_ms: durationMs,
          moving_avg_ms: round_(prevAvg, 2),
          delta_ms: round_(durationMs - prevAvg, 2),
          multiplier: round_(durationMs / Math.max(1, prevAvg), 3)
        });
      }
    }

    props.setProperty(PROP.PIPELINE_STAGE_DURATION_EMA_MS, JSON.stringify(emaMap));
    if (spikes.length > 0) {
      runSummary.performance = {
        stage_duration_drift_spikes: spikes,
        ema_alpha: alpha,
        drift_spike_multiplier: spikeMultiplier,
        drift_spike_min_ms: spikeMinMs
      };
      log_("WARN", "Pipeline stage duration drift spike", { spikes: spikes });
    }
  }

  function emitRunSummary_(status) {
    runSummary.outcome = String(status || runSummary.outcome || "unknown");
    runSummary.finished_at = new Date().toISOString();
    runSummary.duration_ms = Math.max(0, Date.parse(runSummary.finished_at) - Date.parse(runSummary.started_at));
    if (runSummary.outcome !== "ok") {
      var blockerDetail = (runSummary.reason_codes && runSummary.reason_codes.blockers || []).join(",");
      var skipDetail = (runSummary.reason_codes && runSummary.reason_codes.skips || []).join(",");
      runSummary.reason_code = blockerDetail ? REASON_CODE.BLOCKER_STATE : REASON_CODE.ODDS_SKIP;
      runSummary.reason_detail = blockerDetail || skipDetail || runSummary.outcome;
    }
    log_(runSummary.outcome === "ok" ? "INFO" : "WARN", "runPipeline summary", runSummary);
    runSummary.log_row_end = Math.max(getLogDataRowStart_(), getCurrentLogRowCount_() + 1);
    appendRunSummaryLog_(runSummary);
  }

  if (!options.skipLock) {
    lock = LockService.getScriptLock();
    if (!lock.tryLock(25000)) {
      runSummary.stages.odds.outcome = "skipped";
      runSummary.stages.schedule.outcome = "skipped";
      runSummary.stages.model.outcome = "skipped";
      runSummary.stages.signal.outcome = "skipped";
      addReasonCode_("skips", "lock_busy");
      log_("WARN", "Pipeline skipped (lock busy)", { reason_code: REASON_CODE.BLOCKER_STATE, reason_detail: "lock_busy" });
      emitRunSummary_("skipped");
      return;
    }
    hasLock = true;
  }

  var props = PropertiesService.getScriptProperties();
  var nowMs = Date.now();
  var debounce = pipelineDebounceState_(props, nowMs);
  var startedUtc = new Date(nowMs).toISOString();
  runSummary.started_at = startedUtc;

  if (debounce.active) {
    runSummary.stages.odds.outcome = "skipped";
    runSummary.stages.schedule.outcome = "skipped";
    runSummary.stages.model.outcome = "skipped";
    runSummary.stages.signal.outcome = "skipped";
    addReasonCode_("skips", "debounce_active");
    registerDuplicateRunPrevented_(props, "SKIPPED_DEBOUNCE_ACTIVE", {
      triggerPath: String(options.source || "scheduled"),
      debounceUntil: debounce.untilIso,
      remainingMs: debounce.remainingMs
    });
    emitRunSummary_("skipped");
    if (hasLock && lock) lock.releaseLock();
    return;
  }

  try {
    var cfg = options.cfgOverride || getConfig_();
    runSummary.mode.app_mode = String(cfg.MODE || "PRESEASON").toUpperCase();
    runSummary.mode.active_start = cfg.ACTIVE_START;
    runSummary.mode.active_end = cfg.ACTIVE_END;

    if (!withinActiveHours_(cfg)) {
      runSummary.stages.odds.outcome = "skipped";
      runSummary.stages.schedule.outcome = "skipped";
      runSummary.stages.model.outcome = "skipped";
      runSummary.stages.signal.outcome = "skipped";
      addReasonCode_("skips", "outside_active_window");
      props.setProperty(PROP.LAST_PIPELINE_AT, startedUtc);
      props.setProperty(PROP.LAST_PIPELINE_STATUS, "SKIPPED_OUTSIDE_WINDOW");
      props.setProperty(PROP.LAST_PIPELINE_SUMMARY, "outside active window");
      log_("INFO", "Pipeline skipped (outside active window)", { reason_code: REASON_CODE.ODDS_SKIP, reason_detail: "outside_active_window", activeStart: cfg.ACTIVE_START, activeEnd: cfg.ACTIVE_END });
      emitRunSummary_("skipped");
      return;
    }

    var oddsRes = { sportKeyUsed: chooseSportKey_(cfg), games: 0, updatedAt: isoLocalWithOffset_(new Date()), skipped: true };
    var oddsWindowResolveStartedAtMs = Date.now();
    var oddsWindowCtx = resolveOddsWindowForPipeline_(cfg, props);
    finalizeStage_("odds_window_resolve", oddsWindowResolveStartedAtMs, Date.now(), {
      source: oddsWindowCtx.source
    });
    var oddsWindow = oddsWindowCtx.window;
    var oddsWindowError = oddsWindowCtx.error;

    var shouldRefreshOdds = false;
    var nowLocal = new Date();

    if (oddsWindow && oddsWindow.hasGames) {
      runSummary.stages.schedule.outcome = "ok";
      shouldRefreshOdds = nowLocal.getTime() >= oddsWindow.windowStart.getTime() && nowLocal.getTime() <= oddsWindow.windowEnd.getTime();
      if (!shouldRefreshOdds) {
        addReasonCode_("skips", "odds_outside_computed_window");
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
      runSummary.stages.schedule.outcome = "ok";
      var noGamesBehavior = String(cfg.ODDS_NO_GAMES_BEHAVIOR || "SKIP").toUpperCase();
      if (noGamesBehavior === "FALLBACK_STATIC_WINDOW") {
        shouldRefreshOdds = withinActiveHours_(cfg);
        if (!shouldRefreshOdds) {
          addReasonCode_("skips", "no_games_outside_static_window");
          log_("INFO", "Odds refresh skipped: no games today + outside static window", {
            behavior: noGamesBehavior,
            activeStart: cfg.ACTIVE_START,
            activeEnd: cfg.ACTIVE_END,
            scheduleDateLocal: oddsWindow.scheduleDateLocal,
            windowSource: oddsWindowCtx.source
          });
        }
      } else {
        addReasonCode_("skips", "no_games_today");
        log_("INFO", "Odds refresh skipped: no games today", {
          behavior: noGamesBehavior,
          scheduleDateLocal: oddsWindow.scheduleDateLocal,
          windowSource: oddsWindowCtx.source
        });
      }
    } else {
      runSummary.stages.schedule.outcome = "error";
      addReasonCode_("blockers", "schedule_window_fetch_error");
      shouldRefreshOdds = withinActiveHours_(cfg);
      if (!shouldRefreshOdds) {
        addReasonCode_("skips", "schedule_fetch_error_outside_static_window");
        log_("INFO", "Odds refresh skipped: schedule fetch error + outside static window", {
          activeStart: cfg.ACTIVE_START,
          activeEnd: cfg.ACTIVE_END,
          error: oddsWindowError,
          windowSource: oddsWindowCtx.source
        });
      }
    }

    var oddsFetchStartedAtMs = Date.now();
    if (shouldRefreshOdds) {
      var oddsBlockState = evaluateOddsFetchBlocker_(cfg, props, nowMs);
      runSummary.credit_state = {
        remaining_credits: oddsBlockState.remaining,
        threshold: oddsBlockState.threshold,
        snapshot_fresh: oddsBlockState.snapshotFresh,
        snapshot_age_ms: oddsBlockState.snapshotAgeMs,
        credits_at_ms: oddsBlockState.creditsAtMs,
        cooldown_min: oddsBlockState.cooldownMin
      };
      if (oddsBlockState.blocked) {
        runSummary.stages.odds.outcome = "blocked";
        oddsRes.skipped = true;
        oddsRes.skipReasonCode = oddsBlockState.reasonCode || "credits_snapshot_fresh_blocked";
        oddsRes.blockUnblockAt = oddsBlockState.unblockAtIso;
        addReasonCode_("blockers", oddsRes.skipReasonCode);
        log_("WARN", "Odds refresh blocked by low credits", {
          reason_code: REASON_CODE.BLOCKER_STATE,
          reason_detail: oddsBlockState.reasonCode || "credits_snapshot_fresh_blocked",
          remaining: oddsBlockState.remaining,
          threshold: oddsBlockState.threshold,
          creditsAtMs: oddsBlockState.creditsAtMs,
          snapshotAgeMs: oddsBlockState.snapshotAgeMs,
          snapshotFresh: oddsBlockState.snapshotFresh,
          creditsSnapshotMaxAgeMin: oddsBlockState.creditsSnapshotMaxAgeMin,
          unblockAt: oddsBlockState.unblockAtIso,
          cooldownMin: oddsBlockState.cooldownMin
        });
        maybeSendOddsFetchBlockerAlert_(cfg, oddsBlockState, props);
      } else {
        runSummary.stages.odds.outcome = "ok";
        if (oddsBlockState.reasonCode === "credits_snapshot_stale_probe_fetch") {
          addReasonCode_("blockers", oddsBlockState.reasonCode);
          log_("INFO", "Odds refresh probe allowed due to stale credit snapshot", {
            reason_code: REASON_CODE.BLOCKER_STATE,
            reason_detail: oddsBlockState.reasonCode,
            remaining: oddsBlockState.remaining,
            threshold: oddsBlockState.threshold,
            creditsAtMs: oddsBlockState.creditsAtMs,
            snapshotAgeMs: oddsBlockState.snapshotAgeMs,
            snapshotFresh: oddsBlockState.snapshotFresh,
            creditsSnapshotMaxAgeMin: oddsBlockState.creditsSnapshotMaxAgeMin
          });
        }
        oddsRes = refreshOdds_(cfg);
      }
    } else {
      runSummary.stages.odds.outcome = "skipped";
    }
    finalizeStage_("odds_fetch", oddsFetchStartedAtMs, Date.now(), {
      outcome: runSummary.stages.odds.outcome,
      refreshed: !!shouldRefreshOdds,
      game_count: Math.max(0, toInt_(oddsRes.games, 0))
    });

    if (runSummary.stages.schedule.outcome === "not_started") runSummary.stages.schedule.outcome = "ok";

    var scheduleLineupsStartedAtMs = Date.now();
    var mlbRes = refreshMLBScheduleAndLineups_(cfg, { sportKeyUsed: oddsRes.sportKeyUsed });
    runSummary.stages.schedule.outcome = "ok";
    finalizeStage_("schedule_lineups", scheduleLineupsStartedAtMs, Date.now(), {
      matched_count: Math.max(0, toInt_(mlbRes.matchedCount, 0))
    });

    var projectionsStartedAtMs = Date.now();
    refreshProjectionsIfStale_(cfg, false);
    finalizeStage_("projections", projectionsStartedAtMs, Date.now());

    var modelStartedAtMs = Date.now();
    var modelRes = refreshModelAndEdge_(cfg, mlbRes);
    finalizeStage_("model", modelStartedAtMs, Date.now(), {
      computed: Math.max(0, toInt_(modelRes.computed, 0))
    });
    runSummary.stages.model.outcome = "ok";
    runSummary.stages.signal.outcome = "ok";

    var modelStageTimings = modelRes.stageTimings || {};
    var notificationsDurationMs = Math.max(0, toInt_(modelStageTimings.notifications && modelStageTimings.notifications.durationMs, 0));
    var calibrationSnapshotWriteDurationMs = Math.max(0, toInt_(modelStageTimings.calibration_snapshot_write && modelStageTimings.calibration_snapshot_write.durationMs, 0));
    var modelEndedAtMs = Date.now();
    finalizeStage_("notifications", modelEndedAtMs - notificationsDurationMs, modelEndedAtMs);
    finalizeStage_("calibration_snapshot_write", modelEndedAtMs - calibrationSnapshotWriteDurationMs, modelEndedAtMs);

    var cadenceState = updatePipelineCadenceState_(cfg, props, mlbRes.matchedCount, modelRes.computed);
    runSummary.cadence = {
      mode: cadenceState.mode,
      reason: cadenceState.reason,
      cadence_minutes: cadenceState.cadenceMinutes,
      zero_streak: cadenceState.zeroStreak,
      zero_data_run: cadenceState.zeroDataRun
    };
    runSummary.credit_state = runSummary.credit_state || {};
    runSummary.credit_state.remaining_credits = cadenceState.remainingCredits;
    runSummary.credit_state.credit_pressure_level = cadenceState.creditPressureLevel;
    runSummary.stages.odds.games = oddsRes.games;
    runSummary.stages.schedule.matched_count = mlbRes.matchedCount;
    runSummary.stages.schedule.expanded_window_fallback_used = !!mlbRes.expandedWindowFallbackUsed;
    runSummary.stages.schedule.rejection_summary = mlbRes.rejectionSummary || {};
    runSummary.stages.model.computed = modelRes.computed;
    runSummary.stages.model.lineup_fallback_used = !!modelRes.lineupFallbackUsed;
    runSummary.stages.model.lineup_fallback_games = modelRes.lineupFallbackGames || 0;
    runSummary.stages.signal.bet_signals_found = modelRes.betSignalsFound;

    var rejectionSummaryText = JSON.stringify(mlbRes.rejectionSummary || {});
    var fullSummary = "odds=" + oddsRes.games + " matched=" + mlbRes.matchedCount + " computed=" + modelRes.computed + " bets=" + modelRes.betSignalsFound + " cadenceMode=" + cadenceState.mode + " cadenceReason=" + cadenceState.reason + " cadenceMin=" + cadenceState.cadenceMinutes + " zeroStreak=" + cadenceState.zeroStreak + " lineupFallbackUsed=" + (modelRes.lineupFallbackUsed ? "Y" : "N") + " lineupFallbackGames=" + (modelRes.lineupFallbackGames || 0) + " weatherApplied=" + (modelRes.weatherAppliedGames || 0) + " bullpenApplied=" + (modelRes.bullpenFeatureAppliedGames || 0) + " experimentalApplied=" + (modelRes.experimentalAppliedGames || 0) + " expandedWindowFallback=" + (mlbRes.expandedWindowFallbackUsed ? "Y" : "N") + " rejects=" + rejectionSummaryText;
    var runState = updatePipelineRunStateTelemetry_(props, cfg, {
      windowSource: oddsWindowCtx.source,
      shouldRefreshOdds: shouldRefreshOdds,
      matched: mlbRes.matchedCount,
      computed: modelRes.computed,
      cadenceMode: cadenceState.mode,
      cadenceReason: cadenceState.reason,
      rejectionSummary: mlbRes.rejectionSummary || {}
    });
    var summary = runState.suppressVerbose
      ? ("state_unchanged repeat=" + runState.repeatCount + " sig=" + runState.signature.slice(0, 12))
      : fullSummary;

    props.setProperty(PROP.LAST_PIPELINE_AT, startedUtc);
    props.setProperty(PROP.LAST_PIPELINE_STATUS, "OK");
    props.setProperty(PROP.LAST_PIPELINE_SUMMARY, summary);

    applyStageDurationWarnings_(cfg, props);

    if (runState.suppressVerbose) {
      log_("INFO", "state_unchanged", {
        repeatCount: runState.repeatCount,
        heartbeatEvery: runState.heartbeatEvery,
        signature: runState.signature
      });
    } else {
      log_("INFO", "runPipeline completed", { odds: oddsRes.games, matched: mlbRes.matchedCount, computed: modelRes.computed, betSignalsFound: modelRes.betSignalsFound, cadenceMode: cadenceState.mode, cadenceReason: cadenceState.reason, cadenceMinutes: cadenceState.cadenceMinutes, zeroStreak: cadenceState.zeroStreak, lineupFallbackMode: modelRes.lineupFallbackMode, lineupFallbackUsed: modelRes.lineupFallbackUsed, lineupFallbackGames: modelRes.lineupFallbackGames, weatherAppliedGames: modelRes.weatherAppliedGames || 0, bullpenFeatureAppliedGames: modelRes.bullpenFeatureAppliedGames || 0, experimentalAppliedGames: modelRes.experimentalAppliedGames || 0, externalFeatureFetchLogs: modelRes.externalFeatureFetchLogs || [], expandedWindowFallbackUsed: !!mlbRes.expandedWindowFallbackUsed, rejectionSummary: mlbRes.rejectionSummary || {}, runStateTransitioned: runState.transitioned, runStateRepeatCount: runState.repeatCount, runStateHeartbeatDue: runState.heartbeatDue, runStateSignature: runState.signature });
    }
    emitRunSummary_("ok");
  } catch (e) {
    runSummary.stages.odds.outcome = (runSummary.stages.odds.outcome === "not_started") ? "error" : runSummary.stages.odds.outcome;
    runSummary.stages.schedule.outcome = (runSummary.stages.schedule.outcome === "not_started") ? "error" : runSummary.stages.schedule.outcome;
    runSummary.stages.model.outcome = (runSummary.stages.model.outcome === "not_started") ? "error" : runSummary.stages.model.outcome;
    runSummary.stages.signal.outcome = (runSummary.stages.signal.outcome === "not_started") ? "error" : runSummary.stages.signal.outcome;
    addReasonCode_("blockers", "pipeline_exception");
    runSummary.error_message = String(e);
    props.setProperty(PROP.LAST_PIPELINE_AT, startedUtc);
    props.setProperty(PROP.LAST_PIPELINE_STATUS, "ERROR");
    props.setProperty(PROP.LAST_PIPELINE_SUMMARY, String(e));
    log_("ERROR", "runPipeline error", { message: String(e), stack: (e && e.stack) ? String(e.stack) : "" });
    emitRunSummary_("error");
    throw e;
  } finally {
    setPipelineDebounceForMs_(props, 45000, "pipeline_run_completed");
    if (hasLock && lock) lock.releaseLock();
  }
}




function canonicalJsonForSignature_(value) {
  if (value === null || value === undefined) return "null";
  var t = typeof value;
  if (t === "number") return isFinite(value) ? String(value) : "null";
  if (t === "boolean") return value ? "true" : "false";
  if (t === "string") return JSON.stringify(value);
  if (Object.prototype.toString.call(value) === "[object Date]") return JSON.stringify(value.toISOString());
  if (Array.isArray(value)) {
    var arr = [];
    for (var i = 0; i < value.length; i++) arr.push(canonicalJsonForSignature_(value[i]));
    return "[" + arr.join(",") + "]";
  }
  if (t === "object") {
    var keys = Object.keys(value).sort();
    var parts = [];
    for (var k = 0; k < keys.length; k++) {
      var key = keys[k];
      parts.push(JSON.stringify(key) + ":" + canonicalJsonForSignature_(value[key]));
    }
    return "{" + parts.join(",") + "}";
  }
  return JSON.stringify(String(value));
}

function shortHash16_(raw) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, String(raw || ""));
  var out = [];
  for (var i = 0; i < bytes.length; i++) {
    var v = (bytes[i] + 256) % 256;
    var hx = v.toString(16);
    out.push(hx.length === 1 ? "0" + hx : hx);
  }
  return out.join("").slice(0, 16);
}

function computePipelineRunStateSignature_(state) {
  var rejectionHash = shortHash16_(canonicalJsonForSignature_(state.rejectionSummary || {}));
  var pieces = [
    "window=" + String(state.windowSource || ""),
    "refreshOdds=" + (state.shouldRefreshOdds ? "Y" : "N"),
    "matched=" + Math.max(0, toInt_(state.matched, 0)),
    "computed=" + Math.max(0, toInt_(state.computed, 0)),
    "cadenceMode=" + String(state.cadenceMode || ""),
    "cadenceReason=" + String(state.cadenceReason || ""),
    "rejectHash=" + rejectionHash
  ];
  return pieces.join("|");
}

function updatePipelineRunStateTelemetry_(props, cfg, state) {
  var signature = computePipelineRunStateSignature_(state || {});
  var prevSignature = String(props.getProperty(PROP.PIPELINE_LAST_STATE_SIGNATURE) || "");
  var heartbeatEvery = Math.max(1, toInt_(cfg.PIPELINE_STATE_HEARTBEAT_EVERY, 10));
  var transitioned = !prevSignature || prevSignature !== signature;
  var repeatCount = transitioned ? 0 : Math.max(0, toInt_(props.getProperty(PROP.PIPELINE_STATE_REPEAT_COUNT), 0)) + 1;
  var heartbeatDue = !transitioned && (repeatCount % heartbeatEvery === 0);

  props.setProperty(PROP.PIPELINE_LAST_STATE_SIGNATURE, signature);
  props.setProperty(PROP.PIPELINE_STATE_REPEAT_COUNT, String(repeatCount));

  return {
    signature: signature,
    previousSignature: prevSignature,
    repeatCount: repeatCount,
    transitioned: transitioned,
    heartbeatEvery: heartbeatEvery,
    heartbeatDue: heartbeatDue,
    suppressVerbose: !transitioned && !heartbeatDue
  };
}

function resolveOddsWindowForPipeline_(cfg, props) {
  var cacheTtlMin = Math.max(1, toInt_(cfg.ODDS_WINDOW_CACHE_TTL_MIN, 30));
  var refreshMin = Math.max(1, toInt_(cfg.ODDS_WINDOW_REFRESH_MIN, 5));
  var forceRefresh = !!cfg.ODDS_WINDOW_FORCE_REFRESH;
  var nowMs = Date.now();
  var windowCacheKeyName = String(PROP.ODDS_WINDOW_CACHE || "").trim();
  var cacheResult = readOddsWindowCache_(props, cfg, nowMs, cacheTtlMin, windowCacheKeyName);
  var cachedWindow = cacheResult.window;
  var hasCachedWindow = !!cachedWindow;
  var cacheAgeMin = hasCachedWindow ? toFloat_(cacheResult.cacheAgeMin, -1) : -1;
  var cacheFresh = hasCachedWindow && !!cacheResult.cacheFresh;

  if (cacheFresh && !forceRefresh) {
    log_("INFO", "Odds window source selected", {
      source: "cached_schedule_fresh",
      hasGames: !!(cachedWindow && cachedWindow.hasGames),
      gameCount: cachedWindow ? cachedWindow.gameCount : 0,
      cacheTtlMin: cacheTtlMin,
      cacheAgeMin: cacheAgeMin,
      cacheFresh: true,
      fetchAttempted: false,
      reason: "fresh_cache_hit"
    });
    return { window: cachedWindow, source: "cached_schedule_fresh", error: "" };
  }

  var shouldAttemptFetch = true;
  var reason = forceRefresh ? "force_refresh" : "cache_expired";
  if (!forceRefresh && hasCachedWindow && cacheAgeMin >= 0 && cacheAgeMin < refreshMin) {
    shouldAttemptFetch = false;
    reason = "fresh_cache_hit";
  }

  if (!shouldAttemptFetch) {
    log_("INFO", "Odds window source selected", {
      source: "cached_schedule_stale",
      hasGames: !!(cachedWindow && cachedWindow.hasGames),
      gameCount: cachedWindow ? cachedWindow.gameCount : 0,
      cacheTtlMin: cacheTtlMin,
      refreshMin: refreshMin,
      cacheAgeMin: cacheAgeMin,
      cacheFresh: cacheFresh,
      fetchAttempted: false,
      reason: reason
    });
    return { window: cachedWindow, source: "cached_schedule_stale", error: "" };
  }

  try {
    var freshWindow = getMLBOddsRefreshWindow_(cfg);
    storeOddsWindowCache_(props, freshWindow, nowMs);
    log_("INFO", "Odds window source selected", {
      source: "fresh_schedule",
      hasGames: !!(freshWindow && freshWindow.hasGames),
      gameCount: freshWindow ? freshWindow.gameCount : 0,
      cacheTtlMin: cacheTtlMin,
      cacheAgeMin: cacheAgeMin,
      cacheFresh: cacheFresh,
      fetchAttempted: true,
      reason: reason
    });
    return { window: freshWindow, source: "fresh_schedule", error: "" };
  } catch (eWindow) {
    var errClass = (eWindow && eWindow.name) ? String(eWindow.name) : "Error";
    var errMsg = (eWindow && eWindow.message) ? String(eWindow.message) : String(eWindow);

    if (cachedWindow) {
      log_("WARN", "Odds window source selected", {
        source: "cached_schedule_stale",
        hasGames: !!(cachedWindow && cachedWindow.hasGames),
        gameCount: cachedWindow ? cachedWindow.gameCount : 0,
        cacheTtlMin: cacheTtlMin,
        cacheAgeMin: cacheAgeMin,
        cacheFresh: cacheFresh,
        fetchAttempted: true,
        reason: "fetch_error_fallback",
        configuredKeyName: windowCacheKeyName,
        sourceAttempted: cacheResult.sourceAttempted,
        cacheStatus: cacheResult.status,
        fetchErrorClass: errClass,
        fetchErrorMessage: errMsg,
        cacheErrorClass: cacheResult.errorClass,
        cacheErrorMessage: cacheResult.errorMessage
      });
      return { window: cachedWindow, source: "cached_schedule_stale", error: errMsg };
    }

    log_("WARN", "Odds window source selected", {
      source: "fallback_static_window",
      cacheTtlMin: cacheTtlMin,
      cacheAgeMin: cacheAgeMin,
      cacheFresh: cacheFresh,
      fetchAttempted: true,
      reason: "fetch_error_fallback",
      configuredKeyName: windowCacheKeyName,
      sourceAttempted: cacheResult.sourceAttempted,
      cacheStatus: cacheResult.status,
      fetchErrorClass: errClass,
      fetchErrorMessage: errMsg,
      cacheErrorClass: cacheResult.errorClass,
      cacheErrorMessage: cacheResult.errorMessage
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

function readOddsWindowCache_(props, cfg, nowMs, cacheTtlMin, keyName) {
  var resolvedKeyName = String(keyName || "").trim();
  if (!resolvedKeyName) {
    return {
      window: null,
      status: "missing_key_name",
      sourceAttempted: "none",
      errorClass: "ConfigError",
      errorMessage: "ODDS window cache key name is blank"
    };
  }

  var sourceAttempted = [];
  var raw = "";

  try {
    sourceAttempted.push("properties");
    if (props && typeof props.getProperty === "function") raw = String(props.getProperty(resolvedKeyName) || "");
  } catch (eProps) {
    return {
      window: null,
      status: "runtime_exception",
      sourceAttempted: sourceAttempted.join("|"),
      errorClass: (eProps && eProps.name) ? String(eProps.name) : "Error",
      errorMessage: (eProps && eProps.message) ? String(eProps.message) : String(eProps)
    };
  }

  if (!raw) {
    try {
      sourceAttempted.push("cache");
      var scriptCache = CacheService.getScriptCache();
      raw = scriptCache ? String(scriptCache.get(resolvedKeyName) || "") : "";
    } catch (eCacheRead) {
      return {
        window: null,
        status: "runtime_exception",
        sourceAttempted: sourceAttempted.join("|"),
        errorClass: (eCacheRead && eCacheRead.name) ? String(eCacheRead.name) : "Error",
        errorMessage: (eCacheRead && eCacheRead.message) ? String(eCacheRead.message) : String(eCacheRead)
      };
    }
  }

  if (!raw) {
    try {
      sourceAttempted.push("sheet");
      var sheetKeyValue = cfg ? cfg[resolvedKeyName] : "";
      raw = String(sheetKeyValue || "");
    } catch (eSheetRead) {
      return {
        window: null,
        status: "runtime_exception",
        sourceAttempted: sourceAttempted.join("|"),
        errorClass: (eSheetRead && eSheetRead.name) ? String(eSheetRead.name) : "Error",
        errorMessage: (eSheetRead && eSheetRead.message) ? String(eSheetRead.message) : String(eSheetRead)
      };
    }
  }

  if (!raw) {
    return {
      window: null,
      status: "missing_key",
      sourceAttempted: sourceAttempted.join("|"),
      errorClass: "",
      errorMessage: ""
    };
  }

  try {
    var cached = JSON.parse(raw);
    var cachedAtMs = toInt_(cached.cachedAtMs, 0);
    if (cachedAtMs <= 0) {
      return {
        window: null,
        status: "invalid",
        sourceAttempted: sourceAttempted.join("|"),
        cacheAgeMin: -1,
        cacheFresh: false,
        errorClass: "",
        errorMessage: ""
      };
    }

    var maxAgeMs = Math.max(1, cacheTtlMin) * 60 * 1000;
    var ageMs = Math.max(0, nowMs - cachedAtMs);
    var ageMin = Math.round((ageMs / 60000) * 100) / 100;
    var isFresh = ageMs <= maxAgeMs;

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
        return {
          window: null,
          status: "runtime_exception",
          sourceAttempted: sourceAttempted.join("|"),
          errorClass: "ParseError",
          errorMessage: "Cached odds window has invalid windowStart/windowEnd"
        };
      }
    } else {
      parsed.firstGameLocal = null;
      parsed.lastGameLocal = null;
      parsed.windowStart = null;
      parsed.windowEnd = null;
    }

    return {
      window: parsed,
      status: isFresh ? "ok" : "expired",
      sourceAttempted: sourceAttempted.join("|"),
      cacheAgeMin: ageMin,
      cacheFresh: isFresh,
      errorClass: "",
      errorMessage: ""
    };
  } catch (eCache) {
    return {
      window: null,
      status: "runtime_exception",
      sourceAttempted: sourceAttempted.join("|"),
      errorClass: (eCache && eCache.name) ? String(eCache.name) : "Error",
      errorMessage: (eCache && eCache.message) ? String(eCache.message) : String(eCache)
    };
  }
}

function refreshOddsOnly() { var cfg = getConfig_(); refreshOdds_(cfg); }
function refreshMLBScheduleAndLineupsOnly() { var cfg = getConfig_(); refreshMLBScheduleAndLineups_(cfg); }
function refreshProjectionsForce() { var cfg = getConfig_(); refreshProjectionsIfStale_(cfg, true); }
function refreshModelAndEdgeOnly() { var cfg = getConfig_(); var mlbRes = refreshMLBScheduleAndLineups_(cfg); refreshModelAndEdge_(cfg, mlbRes); }


function dryRunValidateNoopCadenceUpdateSingleRun_() {
  var fakeProps = {
    _store: {},
    getProperty: function (k) { return this._store[k] || ""; },
    setProperty: function (k, v) { this._store[k] = String(v); }
  };

  var triggeredRuns = 0;
  var cadenceUpdate = { changed: false, appliedCadenceMinutes: 15, installAction: "noop", signatureUnchanged: true };
  var firstDecision = dryRunPostInstallRunDecision_(fakeProps, cadenceUpdate);
  if (firstDecision.executed) triggeredRuns++;
  if (firstDecision.executed) fakeProps.setProperty(PROP.PIPELINE_RUN_DEBOUNCE_UNTIL_MS, String(Date.now() + 45000));

  var secondDecision = dryRunPostInstallRunDecision_(fakeProps, cadenceUpdate);
  if (secondDecision.executed) triggeredRuns++;

  var passed = (triggeredRuns === 0);
  log_(passed ? "INFO" : "ERROR", "Dry-run validation: no-op cadence update skips immediate run", {
    passed: passed,
    triggeredRuns: triggeredRuns,
    firstReasonCode: firstDecision.reasonCode,
    secondReasonCode: secondDecision.reasonCode,
    duplicateRunPreventedCount: toInt_(fakeProps.getProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED), 0)
  });
  if (!passed) throw new Error("Expected no pipeline runs for no-op cadence update dry-run");
}

function dryRunPostInstallRunDecision_(propsLike, cadenceUpdate) {
  var nowMs = Date.now();
  var untilMs = Math.max(0, toInt_(propsLike.getProperty(PROP.PIPELINE_RUN_DEBOUNCE_UNTIL_MS), 0));
  if (untilMs > nowMs) {
    var next = Math.max(0, toInt_(propsLike.getProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED), 0)) + 1;
    propsLike.setProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED, String(next));
    return { executed: false, reasonCode: "SKIPPED_DEBOUNCE_ACTIVE" };
  }
  if (shouldSkipPostInstallImmediateRun_(cadenceUpdate)) {
    var prevented = Math.max(0, toInt_(propsLike.getProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED), 0)) + 1;
    propsLike.setProperty(PROP.PIPELINE_DUPLICATE_RUN_PREVENTED, String(prevented));
    return { executed: false, reasonCode: "SKIPPED_NOOP_INSTALL_RUN" };
  }
  return {
    executed: true,
    reasonCode: (cadenceUpdate && cadenceUpdate.changed) ? "EXECUTED_INSTALL_CADENCE_CHANGED" : "EXECUTED_INSTALL_NOOP_CADENCE"
  };
}

function dryRunTest_noopInstallDecisionSkip_() {
  var skip = shouldSkipPostInstallImmediateRun_({ changed: false, installAction: "noop", signatureUnchanged: true });
  if (!skip) throw new Error("Expected noop install with unchanged signature to skip immediate run");
  var noSkip = shouldSkipPostInstallImmediateRun_({ changed: false, installAction: "noop", signatureUnchanged: false });
  if (noSkip) throw new Error("Expected noop install with changed signature signal to allow immediate run");
  log_("INFO", "dryRunTest_noopInstallDecisionSkip passed", {});
}
