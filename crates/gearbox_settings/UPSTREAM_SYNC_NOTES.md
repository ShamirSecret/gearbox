# Gearbox Upstream Sync Notes

This file records Gearbox-specific changes made outside `crates/gearbox` and `crates/gearbox_settings`.
When syncing with upstream Zed, check these files first. The intended rule is:

- Keep upstream Zed behavior unchanged when `GEARBOX_GUI` is not set.
- Put Gearbox-only localized text behind `GEARBOX_GUI=1`.
- Keep large Gearbox resource copies under `crates/gearbox_settings`.

## Shared Source Changes

### `Cargo.toml`

- Adds `crates/gearbox` as the copied Gearbox GUI crate.
- Adds `crates/gearbox_settings` as the Gearbox-only settings/keymap asset crate.
- Adds `gearbox_settings` to workspace dependencies.

### `Cargo.lock`

- Updated by Cargo after adding the `gearbox` and `gearbox_settings` workspace crates.

### `crates/settings/src/settings.rs`

- Adds `set_settings_asset_loader`.
- Adds `settings_asset_str`.
- Default behavior still loads upstream `SettingsAssets`.
- Gearbox registers `gearbox_settings::load` before `settings::init`, so Gearbox reads localized settings/keymaps without changing upstream assets.

### `crates/settings/src/keymap_file.rs`

- Built-in keymap loading now goes through `settings_asset_str`.
- This keeps default Zed keymaps unchanged and allows Gearbox to load copied keymaps from `crates/gearbox_settings`.

### `crates/onboarding/src/onboarding.rs`

- Adds `gearbox_text`.
- Localizes the onboarding title, subtitle, and finish button only when `GEARBOX_GUI=1`.

### `crates/onboarding/src/basics_page.rs`

- Localizes visible onboarding setup labels and descriptions only when `GEARBOX_GUI=1`.
- Theme names and editor family names are mostly left as upstream values when they are identifiers or existing theme names.

### `crates/onboarding/src/base_keymap_picker.rs`

- Localizes the base keymap picker placeholder only when `GEARBOX_GUI=1`.

### `crates/workspace/src/welcome.rs`

- Adds `gearbox_text`, `gearbox_label`-style welcome helpers.
- Localizes the workspace welcome page sections, buttons, subtitle, agent card, recent projects header, and tab title only when `GEARBOX_GUI=1`.

### `crates/project_panel/src/project_panel.rs`

- Adds `gearbox_label`.
- Localizes the project panel context menu only when `GEARBOX_GUI=1`.
- File-manager labels from shared `ui::utils` are intentionally not changed yet.

### `crates/command_palette/src/command_palette.rs`

- Adds a small Gearbox label helper.
- Localizes the command palette placeholder only when `GEARBOX_GUI=1`.
- Command names are not fully localized yet because they are derived from action metadata and should be handled by a separate action-name translation layer.

### `crates/file_finder/src/file_finder.rs`

- Adds `gearbox_label`.
- Localizes the file finder placeholder, ignored-file filter tooltip, create-file preview prompt, and split/open actions only when `GEARBOX_GUI=1`.

### `crates/open_path_prompt/src/open_path_prompt.rs`

- Adds `gearbox_label`.
- Localizes the create/replace path confirmation body, confirmation buttons, and empty-state text only when `GEARBOX_GUI=1`.
- The prompt title still includes the target path and remains mostly upstream-formatted.

### `crates/recent_projects/src/recent_projects.rs`

- Adds `gearbox_label`.
- Localizes the recent-project picker placeholder, section headers, no-match text, and several high-frequency footer/action labels only when `GEARBOX_GUI=1`.

### `crates/recent_projects/src/sidebar_recent_projects.rs`

- Adds `gearbox_label`.
- Localizes the sidebar recent-project picker placeholder, no-match text, open-project tooltip, and failed-open prompt title only when `GEARBOX_GUI=1`.

### `crates/recent_projects/src/wsl_picker.rs`

- Adds `gearbox_label`.
- Localizes the WSL distro picker placeholder only when `GEARBOX_GUI=1`.

### `crates/settings_ui/src/components/font_picker.rs`

- Localizes the font picker placeholder only when `GEARBOX_GUI=1`.

### `crates/settings_ui/src/components/theme_picker.rs`

- Localizes the theme picker placeholder only when `GEARBOX_GUI=1`.

### `crates/settings_ui/src/components/icon_theme_picker.rs`

- Localizes the icon theme picker placeholder only when `GEARBOX_GUI=1`.

### `crates/settings_ui/src/components/ollama_model_picker.rs`

- Localizes the Ollama model picker placeholder only when `GEARBOX_GUI=1`.

## Follow-up Localization Targets

- Add an action-name translation layer for command palette entries.
- Localize settings page section titles from `crates/settings_ui/src/page_data.rs` behind `GEARBOX_GUI=1`.
- Localize Agent panel labels in `crates/agent_ui`.
- Continue localizing editor/project prompts and confirmation dialogs as they are encountered.
