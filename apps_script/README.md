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

Apps Script loads global functions/variables across files, so order is mostly for readability.

## Notes
- Function names were preserved to keep menu entries and triggers working.
- `refreshModelAndEdge_` now delegates to `refreshModelAndEdge_core_` in another file only for organization.
- After pasting files in Apps Script:
  - run `setup()` once,
  - verify settings values,
  - run `installTriggers()`.
