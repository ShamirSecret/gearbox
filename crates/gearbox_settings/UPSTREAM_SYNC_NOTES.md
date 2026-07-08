# Gearbox Upstream Sync Notes

Shared-source files modified for the Gearbox fork.  When syncing with upstream:
- Keep upstream behavior unchanged when `GEARBOX_GUI` is not set.
- Gearbox text → `GEARBOX_GUI=1`.  Large resources → `crates/gearbox_settings`.
- **Never** rename upstream internal identifiers (Rust types, action/enum/context names, protocols, test fixtures).  Only rename with clear functional impact and upstream merge cost documented.
- User-visible Gearbox UI/text → Gearbox brand + Chinese.

---

## Translation Architecture

Two new entrypoints in `crates/ui/src/gearbox_text.rs`, exported via `crates/ui/src/ui.rs`:

| Export | Tiers (tried in order) | Used by |
|--------|----------------------|---------|
| `gearbox_translate_text()` | exact → multiline → visible-sentence → title-token → brand-replace | Labels, buttons, tooltips, menus, modals, list items, chips, tree items, loading labels, copy buttons, update buttons, headlines, agent-setup, API cards, thread items, AI-setting items |
| `gearbox_translate_setting_description()` | exact → multiline → sentence-fragment → brand-replace | Settings descriptions |

`translate()` guards on `GEARBOX_GUI` internally.  Each tier:
- **exact**: ~350 static exact-match pairs (English → Chinese)
- **multiline**: line-by-line exact-match for multi-line runtime text
- **visible-sentence**: full sentences ending in `.`/`!`/`?` → `settings_sentence_translation`
- **title-token**: tokenizes CamelCase/space-separated labels, translates via `title_token_translation` table, then fallback `sentence_token_translation`
- **sentence-fragment** (setting descriptions only): strips known prefixes, then token-by-token + phrase replacement + punctuation localization
- **brand-replace**: standalone `Zed` → `Gearbox` (boundary-aware, preserves `ZedGraph` etc.)

`HighlightedLabel` intentionally excluded (byte-offset dependency on original text).

---

## Merge Guide

- `[NEW]` = entire function/struct/file added by Gearbox → **keep during merge**
- `[MOD]` = upstream function modified → **careful diff needed**; keep Gearbox additions, merge upstream logic
- `[NO-OP]` = cosmetic/text-only inside `GEARBOX_GUI=1` guard → **accept Gearbox side**, text is intentional
- Files NOT listed here have no Gearbox changes and can be accepted from upstream.

---

## Modified Upstream Files

### Build & workspace

| File | Change |
|------|--------|
| `Cargo.toml` | `+workspace deps: gearbox, gearbox_settings, gearbox_agent` |
| `Cargo.lock` | Auto-updated |
| `[NEW]` `.github/workflows/gearbox_release.yml` | New workflow. Builds `--package gearbox` on GH runners (Linux/macOS/Windows). Produces `.deb`, `.dmg`, `.exe`. Publishes to GH Release (no Sentry/Slack/notarization/signing). Keep separate from upstream. |

### Settings infrastructure

| File | Change |
|------|--------|
| `[MOD]` `crates/settings/src/settings.rs` | `+set_settings_asset_loader`, `+settings_asset_str`. Upstream default still loads `SettingsAssets`. Gearbox calls `gearbox_settings::load` before `settings::init`. |
| `[MOD]` `crates/settings/src/keymap_file.rs` | Keymap loading → `settings_asset_str` (keeps default Zed keymaps unchanged) |

### UI components — routed through `gearbox_translate_text`

All user-visible text in these components goes through the shared translation layer:

| File | Text type | Marker |
|------|-----------|--------|
| `crates/ui/src/components/label/label.rs` | Label | `[MOD]` |
| `crates/ui/src/components/label/loading_label.rs` | Loading label | `[MOD]` |
| `crates/ui/src/components/button/button.rs` | Button | `[MOD]` |
| `crates/ui/src/components/button/button_link.rs` | Button link | `[MOD]` |
| `crates/ui/src/components/button/toggle_button.rs` | Toggle button | `[MOD]` |
| `crates/ui/src/components/button/copy_button.rs` | Copy button messages/tooltips | `[MOD]` |
| `crates/ui/src/components/button/icon_button.rs` | Icon button | `[MOD]` |
| `crates/ui/src/components/tooltip.rs` | Tooltip | `[MOD]` |
| `crates/ui/src/components/context_menu.rs` | Context menu | `[MOD]` |
| `crates/ui/src/components/modal.rs` | Modal | `[MOD]` |
| `crates/ui/src/components/chip.rs` | Chip | `[MOD]` |
| `crates/ui/src/components/tree_view_item.rs` | Tree item | `[MOD]` |
| `crates/ui/src/components/project_empty_state.rs` | Empty state | `[MOD]` |
| `crates/ui/src/components/collab/update_button.rs` | Update button | `[MOD]` |
| `crates/ui/src/components/ai/agent_setup_button.rs` | Agent setup | `[MOD]` |
| `crates/ui/src/components/ai/configured_api_card.rs` | API card | `[MOD]` |
| `crates/ui/src/components/ai/thread_item.rs` | Thread item | `[MOD]` |
| `crates/ui/src/components/ai/ai_setting_item.rs` | AI setting | `[MOD]` |
| `crates/ui/src/components/list/list_header.rs` | List header | `[MOD]` |
| `crates/ui/src/components/list/list_sub_header.rs` | List sub-header | `[MOD]` |
| `crates/ui/src/components/list/list_bullet_item.rs` | List bullet | `[MOD]` |
| `crates/ui/src/styles/typography.rs` | Headline | `[MOD]` |

### GUI crates — own `gearbox_label` / `gearbox_text` helpers

Each crate has a local helper that checks `GEARBOX_GUI` and returns Chinese or English.  These are `[MOD]` — upstream functions with Gearbox text added inside `GEARBOX_GUI=1` guards.  Merge tip: accept Gearbox side for the guarded text blocks; the upstream logic around them should be merged normally.

| File | What's localized | Notes |
|------|-----------------|-------|
| `crates/onboarding/src/onboarding.rs` | Title, subtitle, finish button | |
| `crates/onboarding/src/basics_page.rs` | Setup labels, descriptions | Theme/font IDs left as upstream values |
| `crates/onboarding/src/base_keymap_picker.rs` | Placeholder | |
| `crates/workspace/src/welcome.rs` | Welcome page sections, buttons, subtitle, agent card, recent header, tab title | |
| `crates/workspace/src/notifications.rs` | Notification titles, secondary content, primary action labels | Catches text not entering via `Label::new` / `Button::new` |
| `crates/workspace/src/pane_group.rs` | Dynamic collab location/share labels with usernames | Cannot exact-match |
| `crates/workspace/src/security_modal.rs` | Restricted-mode text and buttons | |
| `crates/project_panel/src/project_panel.rs` | Context menu (36 labels), discard-changes prompt, restore/cancel buttons | File-manager labels from `ui::utils` intentionally not changed |
| `crates/recent_projects/src/recent_projects.rs` | Picker placeholder, section headers, no-match text, footer/action labels | |
| `crates/recent_projects/src/sidebar_recent_projects.rs` | Picker placeholder, no-match, tooltip, error prompt | |
| `crates/recent_projects/src/wsl_picker.rs` | Distro picker placeholder | |
| `crates/recent_projects/src/remote_connections.rs` | Connection failure prompt titles, retry/cancel buttons | |
| `crates/recent_projects/src/remote_servers.rs` | Remote-server and Dev Container action labels | |
| `crates/command_palette/src/command_palette.rs` | Placeholder, run/add-keybinding buttons | Command names not localized (derived from action metadata; needs separate action-name translation layer) |
| `crates/file_finder/src/file_finder.rs` | Placeholder, filter tooltip, create-file prompt, split/open labels | |
| `crates/open_path_prompt/src/open_path_prompt.rs` | Create/replace confirmation body, buttons, empty-state text | Prompt title keeps target path, mostly upstream-formatted |
| `crates/collab_ui/src/collab_panel.rs` | CLA error path; participant labels, tooltips, context menu entries | Removes `zed.dev/cla` branding from Gearbox path |
| `crates/collab_ui/src/notifications/incoming_call_notification.rs` | Call notification text with username | Dynamic format |
| `crates/debugger_ui/src/session/running.rs` | Debugger scenario error | |
| `crates/debugger_ui/src/new_process_modal.rs` | Command placeholder (`ENV=Gearbox ~/bin/program`) | |
| `crates/debugger_ui/src/debugger_panel.rs` | Empty-state labels | |
| `crates/extensions_ui/src/extensions_ui.rs` | Version compatibility tooltips, doc/install labels | |
| `crates/extensions_ui/src/extension_version_selector.rs` | Compatibility labels | |
| `crates/oauth_callback_server/src/oauth_callback_server.rs` | OAuth success/failure browser pages | Zed wording kept when not `GEARBOX_GUI` |
|------|-----------------|-------|
| `crates/onboarding/src/onboarding.rs` | Title, subtitle, finish button | |
| `crates/onboarding/src/basics_page.rs` | Setup labels, descriptions | Theme/font IDs left as upstream values |
| `crates/onboarding/src/base_keymap_picker.rs` | Placeholder | |
| `crates/workspace/src/welcome.rs` | Welcome page sections, buttons, subtitle, agent card, recent header, tab title | |
| `crates/workspace/src/notifications.rs` | Notification titles, secondary content, primary action labels | Catches text not entering via `Label::new` / `Button::new` |
| `crates/workspace/src/pane_group.rs` | Dynamic collab location/share labels with usernames | Cannot exact-match |
| `crates/workspace/src/security_modal.rs` | Restricted-mode text and buttons | |
| `crates/project_panel/src/project_panel.rs` | Context menu (36 labels), discard-changes prompt, restore/cancel buttons | File-manager labels from `ui::utils` intentionally not changed |
| `crates/recent_projects/src/recent_projects.rs` | Picker placeholder, section headers, no-match text, footer/action labels | |
| `crates/recent_projects/src/sidebar_recent_projects.rs` | Picker placeholder, no-match, tooltip, error prompt | |
| `crates/recent_projects/src/wsl_picker.rs` | Distro picker placeholder | |
| `crates/recent_projects/src/remote_connections.rs` | Connection failure prompt titles, retry/cancel buttons | |
| `crates/recent_projects/src/remote_servers.rs` | Remote-server and Dev Container action labels | |
| `crates/command_palette/src/command_palette.rs` | Placeholder, run/add-keybinding buttons | Command names not localized (derived from action metadata; needs separate action-name translation layer) |
| `crates/file_finder/src/file_finder.rs` | Placeholder, filter tooltip, create-file prompt, split/open labels | |
| `crates/open_path_prompt/src/open_path_prompt.rs` | Create/replace confirmation body, buttons, empty-state text | Prompt title keeps target path, mostly upstream-formatted |
| `crates/collab_ui/src/collab_panel.rs` | CLA error path; participant labels, tooltips, context menu entries | Removes `zed.dev/cla` branding from Gearbox path |
| `crates/collab_ui/src/notifications/incoming_call_notification.rs` | Call notification text with username | Dynamic format |
| `crates/debugger_ui/src/session/running.rs` | Debugger scenario error | |
| `crates/debugger_ui/src/new_process_modal.rs` | Command placeholder (`ENV=Gearbox ~/bin/program`) | |
| `crates/debugger_ui/src/debugger_panel.rs` | Empty-state labels | |
| `crates/extensions_ui/src/extensions_ui.rs` | Version compatibility tooltips, doc/install labels | |
| `crates/extensions_ui/src/extension_version_selector.rs` | Compatibility labels | |
| `crates/oauth_callback_server/src/oauth_callback_server.rs` | OAuth success/failure browser pages | Zed wording kept when not `GEARBOX_GUI` |

### Settings UI

All files are `[MOD]` — Gearbox text layers added inside `GEARBOX_GUI=1` guards; upstream data model and JSON paths unchanged.

| File | What | Notes |
|------|------|-------|
| `crates/settings_ui/src/settings_ui.rs` | Page names, section headers, item titles, descriptions, subpage links, action links, navigation entries, breadcrumbs, window title, search placeholder, settings-file buttons, user/project/server scope labels, workspace-restoration text, telemetry labels, scoped settings | Fallback → `gearbox_translate_text` / `gearbox_translate_setting_description`. `Zed`→`Gearbox` in descriptions. |
| `crates/settings_ui/src/components/dropdown.rs` | Enum labels: `Last Session`→`上次会话`, etc. | Enum values/settings unchanged |
| `crates/settings_ui/src/components/font_picker.rs` | Placeholder | |
| `crates/settings_ui/src/components/theme_picker.rs` | Placeholder; `Zed` theme names→`Gearbox` display | Internal theme IDs unchanged |
| `crates/settings_ui/src/components/icon_theme_picker.rs` | Placeholder; `Zed` icon theme names→`Gearbox` display | Internal IDs unchanged |
| `crates/settings_ui/src/components/ollama_model_picker.rs` | Placeholder | |
| `crates/settings_ui/src/pages/edit_prediction_provider_setup.rs` | Restart instruction→Gearbox | |
| `crates/settings_ui/src/pages/llm_providers_page.rs` | Restart instruction→Gearbox | |
| `crates/settings_ui/src/pages/tool_permissions_setup.rs` | Native-agent disclaimer→Gearbox | |
| `crates/settings_ui/src/pages/sandbox_settings.rs` | Sandbox explanation→Gearbox | |
| `crates/settings_ui/src/pages/skill_creator.rs` | Private-file retry→Gearbox | |

### Language model & OAuth providers

All are `[MOD]` — visible text replace `Zed`→`Gearbox` only. Internal type/plan/enum identifiers unchanged.

| File(s) | What |
|---------|------|
| `crates/language_models/src/provider.rs` | Visible provider/help/error wording |
| `crates/language_models/src/provider/{api_compatible,bedrock,cloud,llama_cpp,lmstudio,mistral,ollama,openai_subscribed,opencode,vercel_ai_gateway}.rs` | Same pattern across all 10 providers |
| `crates/context_server/src/context_server.rs` | OAuth/client metadata→Gearbox; endpoint constants kept |
| `crates/context_server/src/oauth.rs` | Same |

---

## `[NEW]` Gear Native Agent (`crates/gearbox_agent/`)

New runtime crate.  Functions as the orchestration engine for the `Gear` agent.

### Key modules

| Module | Purpose |
|--------|---------|
| `runtime.rs` | `Orchestrator::run()` — goal-pursuit loop: spec→plan→worker→verify→repair→review. Sync, runs on `background_spawn`. `DEFAULT_MAX_ITERATIONS=2`. Accepts `coordinator_model`/`coordinator_brief`. |
| `workers.rs` | `WorkerRegistry`→`CommandWorker`→external commands. `WorkerKind`: opencode/codex/claude/zed_agent/custom. `WorkerPacket` JSON contract. `require_worker`/`skip_worker` flags. |
| `state.rs` | `Goal`, `Session`, `Task`, `Event`, `CoordinatorModel` data models. `StateStore` — JSON files under `.gearbox-agent/`. |
| `tools.rs` | `git_snapshot`, `check_scope`, `run_shell_command_with_env_and_cancellation`, `CancellationToken` (`Arc<AtomicBool>`) |
| `languages.rs` | `LanguageDetection` — TypeScript/Python/Rust detection. `detect_with_request()` falls back to request text for empty workspaces (web/app prompts→TypeScript scaffold). |
| `product.rs` | Markdown artifacts: spec, plan, verification, final-report. Includes `coordinator_model`/`coordinator_brief` summaries. Web App stack guidance. |
| `cli.rs` | `gear` binary (name may conflict with system `gear`). `gear run <prompt>` with worker/scope/verify args. |
| `Cargo.toml` | Deps: `smol`, `chrono`, `clap`, `serde`, `serde_json`, `anyhow`. Binary: `gear`. |

### Recent additions
- `CoordinatorModel` (provider_id/model_id/name) persisted in goals and worker packets
- `coordinator_brief` (optional LLM planning context, generated before run)
- Empty-workspace prompts→TypeScript Web App default stack + npm verify commands
- `TaskInputs` (spec/plan packet paths) in worker packets

---

## Agent Integration Changes

### `[NEW]` `crates/agent/Cargo.toml`
- `+dep: gearbox_agent`

### `[MOD]` `crates/agent/src/agent.rs`

| Marker | Function / Change | What |
|--------|------------------|------|
| `[NEW]` | `GEAR_AGENT_ID` | Static `LazyLock<AgentId>` for `"Gear"` |
| `[MOD]` | `struct Session` | `+gear_cancellation_token: Option<CancellationToken>`, `+work_dirs: Option<PathList>` |
| `[NEW]` | `NativeAgentConnection::gear()` | Constructor for Gear native connection |
| `[NEW]` | `send_gear_prompt()` | Routes prompts → `Orchestrator::run()` on `cx.background_spawn` |
| `[NEW]` | `gear_coordinator_from_thread()` | Reads thread's model → `CoordinatorModel` metadata |
| `[NEW]` | `generate_gear_coordinator_brief()` | Async LLM call for planning brief; skips `"fake"` provider |
| `[NEW]` | `is_gear_executable_goal()` | Filters greetings; trims ASCII+CJK punctuation; checks action words + length |
| `[NEW]` | `gear_workspace_for_session()` | Resolves workspace: `work_dirs` → `visible_worktree` fallback |
| `[NEW]` | `push_gear_assistant_markdown()` | Pushes markdown block into ACP thread + internal Thread |
| `[NEW]` | `gear_request_from_prompt()` | Extracts text content from ACP prompt blocks |
| `[NEW]` | `gear_worker_config_from_env()` | Reads `GEARBOX_GEAR_WORKER`, `GEARBOX_GEAR_WORKER_COMMAND`, fallback `GEARBOX_OPENCODE_COMMAND`. Warns on invalid kinds. `require_worker=true` when command set. |
| `[NEW]` | `gear_verification_commands_from_env()` | Reads `GEARBOX_GEAR_VERIFY_COMMANDS` |
| `[NEW]` | `gear_event_status_markdown()` | Event → markdown status line |
| `[NEW]` | `gear_response_markdown()` | Final report → markdown summary |
| `[NEW]` | `clear_gear_cancellation_token()` | Clears session token if same reference |
| `[MOD]` | `cancel()` | +Gear token cancel; calls `token.cancel()` then `thread.cancel()` |
| `[NEW]` | `test_gear_prompt_runs_gearbox_orchestrator` | GPUI integration test |
| `[NEW]` | `test_gear_prompt_greeting_does_not_start_orchestrator` | GPUI test for small-talk filtering |

**Imports added:** `gearbox_agent::runtime::*`, `gearbox_agent::tools::CancellationToken`, `gearbox_agent::workers::*`, `gearbox_agent::state::CoordinatorModel`, `language_model::{CompletionIntent, LanguageModelRequest, LanguageModelRequestMessage, Role}`

### `[NEW]` `crates/agent/src/native_agent_server.rs`
- `NativeAgentServer::gear()`: `agent_id: GEAR_AGENT_ID`, `telemetry_id: "gear"`, `logo: Sparkle`

### `[MOD]` `crates/agent/src/tests/mod.rs`
- Native agent tests updated for explicit identity metadata struct fields (was tuple access)

### `[MOD]` `crates/agent_ui/src/agent_ui.rs`
- `+Agent::GearAgent` variant, serde alias `"GearAgent"`
- `Agent::label()`: `"Agent"` under GEARBOX_GUI (from `"Zed Agent"`), `"Gear"` for GearAgent
- `Agent::server()`: returns `NativeAgentServer::gear()`
- `Agent::icon()`: `Sparkle` for GearAgent and Custom
- `Agent::is_native()`: includes GearAgent

### `[MOD]` `crates/agent_ui/src/agent_panel.rs`
- Gear in `list_agents_and_models`: only when `GEARBOX_GUI=1`; shares native model list
- Context menu entry: `"Gear"` with `Sparkle` icon, launches new thread
- Agent ID routing for sibling thread creation

### `[MOD]` `crates/agent_ui/src/agent_connection_store.rs`
- `Agent::GearAgent` entries always retained

### `[MOD]` `crates/agent_ui/src/conversation_view/thread_view.rs`, `agent_ui/src/mention_set.rs`
- Updated for native connection identity metadata (struct fields instead of tuple access)

---

## Gearbox Branding & Packaging

### `[NEW]` Binary entry point (`crates/gearbox/`)

| File | Change |
|------|--------|
| `src/main.rs` | Sets `GEARBOX_GUI=1` at startup (unsafe, before multi-threading). Data dir→`~/.local/share/gearbox`. User-Agent→`Gearbox/{version}`. Error messages→"Gearbox". Env aliases: `GEARBOX_EXPERIMENTAL_A11Y`, `GEARBOX_STATELESS`, `GEARBOX_GENERATE_MINIDUMPS`, `GEARBOX_WINDOW_DECORATIONS`, `GEARBOX_ALLOW_EMULATED_GPU` (with `ZED_*` fallbacks). Build-time vars (`ZED_BUNDLE`, `ZED_BUILD_ID`, `ZED_COMMIT_SHA`) unchanged. |
| `src/zed.rs` | `DOCS_URL`, `STATUS_URL`, `MERCH_URL`→`github.com/ShamirSecret/gearbox-zed`. Internal action names (`OpenZedUrl`, `RegisterZedScheme`) unchanged. |
| `src/zed/app_menus.rs` | Full Chinese menu items: 视图→放大/缩小/重置缩放, 编辑器布局→拆分, 面板→项目/大纲/协作/终端/调试, etc. |
| `src/zed/open_listener.rs` | (no Gearbox-specific changes) |
| `src/zed/quick_action_bar/repl_menu.rs` | `ZED_REPL_DOCUMENTATION` const→gearbox repo URL |
| `build.rs` | Diagnostic prefix→`"gearbox build.rs:"` |

### `[NEW]` Packaging resources

| File | Change |
|------|--------|
| `crates/gearbox/resources/app-icon.icns` | macOS icon from Gearbox PNG |
| `crates/gearbox/resources/flatpak/manifest-template.json` | Command/path→`gearbox`; `ZED_BUNDLE_TYPE` kept |
| `crates/gearbox/resources/snap/snapcraft.yaml.in` | Entry/command→`gearbox`; `ZED_BUNDLE_TYPE` kept |
| `crates/gearbox_settings/assets/settings/*` | Settings with Gearbox comments/docs/menu strings. Internal IDs (`.ZedMono`, `Zed (Default)`, `ZedPredictModal`) kept; Gearbox display layer renames at render. |
| `crates/gearbox_settings/assets/keymaps/*` | Keymaps with Gearbox strings. Internal context IDs kept. |
| `[NEW]` `docs/gearbox-gear-agent-plan.md` | Design doc tracking Gear runtime progress and milestones |
| `[NEW]` `docs/gearbox-gear-agent-plan.md` | Design doc tracking Gear runtime progress and milestones |

---

## Follow-up Targets

- Action-name translation layer for command palette entries
- Continue expanding settings item title/description mappings in `settings_ui.rs` and `gearbox_text.rs`
- Continue localizing Agent panel labels
- Continue localizing editor/project prompts and confirmation dialogs
- Per-iteration provider-backed review (Milestone 3)
- TypeScript Web App sample generation (Milestone 3)
