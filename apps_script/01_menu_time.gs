/* ===================== MENU ===================== */

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Lucky Luciano MLB")
    .addItem("Setup / Verify Sheets", "setup")
    .addSeparator()
    .addItem("Run Pipeline Now", "runPipeline")
    .addItem("Refresh Odds Now", "refreshOddsOnly")
    .addItem("Refresh MLB Now (Schedule + Lineups)", "refreshMLBScheduleAndLineupsOnly")
    .addItem("Refresh Projections Now (Force)", "refreshProjectionsForce")
    .addItem("Compute Model/Edge Now", "refreshModelAndEdgeOnly")
    .addItem("Run Calibration Report Now", "runDailyCalibration")
    .addSeparator()
    .addItem("Send Discord Test Ping", "sendDiscordTestPing")
    .addItem("Send Heartbeat Now", "sendDiscordHeartbeat")
    .addSeparator()
    .addItem("Install Triggers", "installTriggers")
    .addItem("Remove Triggers", "removeTriggers")
    .addSeparator()
    .addItem("Reset Workbook (Delete + Recreate Tabs)", "resetWorkbook")
    .addToUi();
}

/* ===================== TIME HELPERS ===================== */

function isoUtcNoMs_(d) {
  return Utilities.formatDate(d, "UTC", "yyyy-MM-dd'T'HH:mm:ss'Z'");
}

function isoLocalWithOffset_(d) {
  return Utilities.formatDate(d, TZ, "yyyy-MM-dd'T'HH:mm:ss") + TZ_OFFSET;
}

function localPretty_(d) {
  return Utilities.formatDate(d, TZ, "yyyy-MM-dd HH:mm:ss") + " (" + TZ + ")";
}

function localDateKey_() {
  return Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd");
}

function localHourKey_() {
  return Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd-HH");
}
