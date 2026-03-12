# Apps Script Files (Lucky Luciano MLB)

This folder contains a split version of your Google Apps Script code so you can copy each file into the Apps Script editor.

## Suggested file creation order in Apps Script
1. `00_constants.gs`
2. `01_menu_time.gs`
3. `02_setup_headers.gs`
4. `03_triggers_discord_pipeline.gs`
5. `04_odds_mlb.gs`
6. `05_projections_model.gs`
7. `06_model_config_utils.gs`
8. `07_settings_and_utils.gs`
9. `08_bet_actions.gs`
10. `09_calibration.gs`

Apps Script loads global functions/variables across files, so order is mostly for readability.

## Notes
- Function names were preserved to keep menu entries and triggers working.
- `refreshModelAndEdge_` now delegates to `refreshModelAndEdge_core_` in another file only for organization.
- After pasting files in Apps Script:
  - run `setup()` once,
  - verify settings values,
  - run `installTriggers()`.

## SIGNAL_LOG drift metrics
- **Opening Drift** means **Open→Signal** and is tracked by:
  - `open_price_pick`, `open_implied_pick`
  - `delta_open_to_signal_price`, `delta_open_to_signal_implied`
- **CLV** means **Signal→Close** and is tracked by:
  - `close_price_pick`, `close_implied_pick`
  - `delta_signal_to_close_price`, `delta_signal_to_close_implied`
- Guardrails:
  - Missing opening values keep open/drift fields blank and set `open_reason_code`.
  - Missing/too-early close values keep close/CLV fields blank and set `close_reason_code`.
  - Optional time trigger: enable `ENABLE_SIGNAL_CLOSE_UPDATER=TRUE` and tune `SIGNAL_CLOSE_UPDATER_MINUTES` / `SIGNAL_CLOSE_PRESTART_MIN`.

