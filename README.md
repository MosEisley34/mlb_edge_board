# mlb_edge_board

Repository workspace for the Lucky Luciano MLB Sheets model.

## Included now
- `apps_script/` split `.gs` files ready to copy into Google Apps Script editor.
- `apps_script/README.md` with import order and setup steps.

## Legacy bet tracking status
- Legacy bet tracking (`BET_LOG` / `BET_EVENTS`) is retired by default.
- Web-app legacy actions in `08_bet_actions.gs` are guarded and return informational pages when disabled.
- Re-enable is intentionally two-step for safety: set `LEGACY_BET_TRACKING_ALLOW_REENABLE = true` in `00_constants.gs` and set `ENABLE_BET_TRACKING=TRUE` in `SETTINGS`.
