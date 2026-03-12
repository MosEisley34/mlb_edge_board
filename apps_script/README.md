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
- `SIGNAL_LOG` rows represent **signal delivery attempts** (not only successful sends).
- Delivery fields indicate outcome per attempt:
  - `delivery_status`: `sent` or `failed`
  - `delivery_reason_code`: failure reason code (for example `missing_delivery_config`, `delivery_http_error`)
  - `delivery_http`, `delivery_mode`, `delivery_error_preview`, `discord_message_id`
- **Opening Drift** means **Open→Signal** and is tracked by:
  - `open_price_pick`, `open_implied_pick`
  - `delta_open_to_signal_price`, `delta_open_to_signal_implied`
- **CLV** means **Signal→Close** and is tracked by:
  - `close_price_pick`, `close_implied_pick`
  - `delta_signal_to_close_price`, `delta_signal_to_close_implied`
- Guardrails:
  - Missing opening values keep open/drift fields blank and set `open_reason_code`.
  - Close snapshot selection uses a cutoff at `commence_time_utc - SIGNAL_CLOSE_PRESTART_MIN`: choose the latest snapshot captured at or before cutoff (nearest earlier fallback when exact cutoff snapshot is unavailable).
  - If no snapshot exists at/before cutoff, close/CLV fields remain blank with `close_no_snapshot_before_cutoff`; if current time is still before cutoff, reason is `close_still_too_early`.
  - Optional time trigger: enable `ENABLE_SIGNAL_CLOSE_UPDATER=TRUE` and tune `SIGNAL_CLOSE_UPDATER_MINUTES` / `SIGNAL_CLOSE_PRESTART_MIN`.

## Odds API usage profiles (`ODDS_USAGE_PROFILE`)
Use `ODDS_USAGE_PROFILE` in `SETTINGS` to switch odds request footprint without editing code.

- `NORMAL` (default): keeps your standard coverage (`ODDS_REGIONS`, `ODDS_LOOKAHEAD_HOURS`, all bookmakers unless otherwise constrained).
- `LOW_CREDIT`: applies low-credit overrides for a smaller payload/cost profile.

Recommended low-credit settings:
- `ODDS_USAGE_PROFILE=LOW_CREDIT`
- `ODDS_LOW_CREDIT_REGIONS=us`
- `ODDS_LOW_CREDIT_LOOKAHEAD_HOURS=12` (or up to `18` if you need a wider window)
- `ODDS_LOW_CREDIT_BOOKMAKERS=` a curated CSV list (optional), for example `draftkings,fanduel`

Switch back to `ODDS_USAGE_PROFILE=NORMAL` when credits are healthy.

## Legacy bet tracking deprecation
- Legacy bet tracking flows are retired by default.
- `doGet` / `doPost` action routes in `08_bet_actions.gs` return informational pages unless bet tracking is explicitly re-enabled.
- Setup/reset only create `BET_LOG` and `BET_EVENTS` when both the settings toggle and constants kill-switch are enabled.
- Existing legacy tabs are preserved when disabled so historical data is migration-safe.
