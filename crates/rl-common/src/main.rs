use anyhow::{Context, Result, anyhow};
use boxcars::{NetworkParse, ParserBuilder};
use chrono::{DateTime, NaiveDate, Utc};
use eframe::egui;
use reqwest::blocking::{Client, Response};
use reqwest::header::AUTHORIZATION;
use rfd::{FileDialog, MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};
use rl_coach::{
    AnalysisReport as CoachAnalysisReport, BatchSummary as CoachBatchSummary,
    DiagnosisLabel as CoachDiagnosisLabel, MetricValue as CoachMetricValue,
    TimelinePlayer as CoachTimelinePlayer, analyze_path as coach_analyze_path,
    build_gemini_match_payload_for_player, list_replay_players, load_reports as coach_load_reports,
    write_position_timeline_file,
};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

const API_BASE: &str = "https://ballchasing.com/api";
const MIN_REQUEST_INTERVAL_SECONDS: u64 = 2;
const MIN_WATCH_INTERVAL_SECONDS: u64 = 2;
const UPDATE_CHECK_ENV: &str = "RL_TOOLKIT_UPDATE_CHECK";
const RELEASE_API_URL_ENV: &str = "RL_TOOLKIT_RELEASE_API_URL";
const DEFAULT_RELEASE_API_URL: &str =
    "https://api.github.com/repos/k-zshiba/rl-toolkit/releases/latest";
const UPDATE_GITHUB_TOKEN_ENV: &str = "RL_TOOLKIT_GITHUB_TOKEN";
const GITHUB_TOKEN_ENV: &str = "GITHUB_TOKEN";
const UPDATE_TIMEOUT_SECONDS: u64 = 5;
const FONT_PATH_ENV: &str = "RL_TOOLKIT_FONT_PATH";
const GEMINI_MAX_OUTPUT_TOKENS: u32 = 8192;

fn main() -> eframe::Result<()> {
    configure_platform_env();

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "RL Toolkit GUI",
        options,
        Box::new(|cc| {
            configure_localized_fonts(&cc.egui_ctx);
            Ok(Box::new(RlGuiApp::default()))
        }),
    )
}

fn configure_platform_env() {
    #[cfg(target_os = "linux")]
    {
        const WINIT_BACKEND_ENV: &str = "WINIT_UNIX_BACKEND";
        if std::env::var_os(WINIT_BACKEND_ENV).is_none() {
            // Rust 2024 marks process-wide env mutation as unsafe.
            unsafe {
                std::env::set_var(WINIT_BACKEND_ENV, "x11");
            }
        }
    }
}

fn configure_localized_fonts(ctx: &egui::Context) {
    let mut candidates = Vec::new();

    if let Ok(path) = env::var(FONT_PATH_ENV) {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            candidates.push(PathBuf::from(trimmed));
        }
    }

    candidates.extend(default_font_candidates());

    for font_path in candidates {
        if !font_path.is_file() {
            continue;
        }

        let Ok(bytes) = fs::read(&font_path) else {
            continue;
        };

        let mut fonts = egui::FontDefinitions::default();
        let font_name = "rl_localized".to_string();
        fonts
            .font_data
            .insert(font_name.clone(), egui::FontData::from_owned(bytes).into());

        if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
            family.insert(0, font_name.clone());
        }
        if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
            family.push(font_name.clone());
        }

        ctx.set_fonts(fonts);
        eprintln!("[font] loaded localized font: {}", font_path.display());
        return;
    }

    eprintln!(
        "[font] no Japanese-capable font detected. set {FONT_PATH_ENV} to a .ttf/.otf/.ttc path"
    );
}

fn default_font_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    #[cfg(target_os = "windows")]
    {
        candidates.push(PathBuf::from(r"C:\Windows\Fonts\YuGothM.ttc"));
        candidates.push(PathBuf::from(r"C:\Windows\Fonts\YuGothR.ttc"));
        candidates.push(PathBuf::from(r"C:\Windows\Fonts\meiryo.ttc"));
        candidates.push(PathBuf::from(r"C:\Windows\Fonts\msgothic.ttc"));
    }

    #[cfg(target_os = "linux")]
    {
        candidates.push(PathBuf::from(
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        ));
        candidates.push(PathBuf::from(
            "/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf",
        ));
        candidates.push(PathBuf::from(
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        ));
        candidates.push(PathBuf::from(
            "/usr/share/fonts/truetype/noto/NotoSansJP-Regular.ttf",
        ));
        candidates.push(PathBuf::from(
            "/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc",
        ));
    }

    #[cfg(target_os = "macos")]
    {
        candidates.push(PathBuf::from(
            "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
        ));
        candidates.push(PathBuf::from(
            "/System/Library/Fonts/ヒラギノ丸ゴ ProN W4.ttc",
        ));
        candidates.push(PathBuf::from("/System/Library/Fonts/Hiragino Sans GB.ttc"));
    }

    candidates
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Harvester,
    Replay2Json,
    Coach,
    Gemini,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Language {
    English,
    Japanese,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HarvesterSettings {
    api_key: String,
    player: String,
    output_dir: String,
    max_pages: u32,
    request_interval_seconds: u64,
}

impl Default for HarvesterSettings {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            player: String::new(),
            output_dir: String::new(),
            max_pages: 5,
            request_interval_seconds: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Replay2JsonSettings {
    input_dir: String,
    output_dir: String,
    watch_mode: bool,
    watch_interval_seconds: u64,
    pretty_json: bool,
}

impl Default for Replay2JsonSettings {
    fn default() -> Self {
        Self {
            input_dir: String::new(),
            output_dir: String::new(),
            watch_mode: false,
            watch_interval_seconds: 10,
            pretty_json: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum CoachInputMode {
    File,
    #[default]
    Directory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoachSettings {
    input_mode: CoachInputMode,
    input_path: String,
    output_dir: String,
    pretty_json: bool,
}

impl Default for CoachSettings {
    fn default() -> Self {
        Self {
            input_mode: CoachInputMode::Directory,
            input_path: String::new(),
            output_dir: String::new(),
            pretty_json: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct GeminiSettings {
    api_key: String,
    input_path: String,
    output_dir: String,
    model: String,
    selected_player_name: String,
    selected_player_team: Option<u8>,
    selected_player_unique_id: Option<String>,
    culprit_investigation: bool,
}

impl Default for GeminiSettings {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            input_path: String::new(),
            output_dir: String::new(),
            model: "gemini-2.5-flash".to_string(),
            selected_player_name: String::new(),
            selected_player_team: None,
            selected_player_unique_id: None,
            culprit_investigation: false,
        }
    }
}

#[derive(Debug)]
enum TaskKind {
    Harvester(HarvesterSettings),
    Replay2Json(Replay2JsonSettings),
    Coach(CoachSettings),
    Gemini(GeminiSettings),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunningTask {
    Harvester,
    Replay2Json,
    Coach,
    Gemini,
}

impl RunningTask {
    fn from_task(task: &TaskKind) -> Self {
        match task {
            TaskKind::Harvester(_) => Self::Harvester,
            TaskKind::Replay2Json(_) => Self::Replay2Json,
            TaskKind::Coach(_) => Self::Coach,
            TaskKind::Gemini(_) => Self::Gemini,
        }
    }
}

#[derive(Debug)]
enum WorkerEvent {
    Log(String),
    GeminiResult {
        response_text: String,
        output_path: PathBuf,
    },
    Finished(Result<(), String>),
}

struct RlGuiApp {
    language: Language,
    tab: Tab,
    harvester: HarvesterSettings,
    replay2json: Replay2JsonSettings,
    coach: CoachSettings,
    gemini: GeminiSettings,
    coach_view: CoachViewState,
    gemini_view: GeminiViewState,
    logs: Vec<String>,
    running: bool,
    worker_rx: Option<mpsc::Receiver<WorkerEvent>>,
    worker_cancel: Option<Arc<AtomicBool>>,
    active_task: Option<RunningTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct SavedGuiSettings {
    harvester: HarvesterSettings,
    replay2json: Replay2JsonSettings,
    coach: CoachSettings,
    gemini: GeminiSettings,
}

#[derive(Debug, Default)]
struct CoachViewState {
    summary: Option<CoachBatchSummary>,
    selected_match_index: usize,
    load_error: Option<String>,
}

#[derive(Debug, Default)]
struct GeminiViewState {
    latest_response: Option<String>,
    latest_output_path: Option<PathBuf>,
    players: Vec<CoachTimelinePlayer>,
    players_input_path: String,
    player_load_error: Option<String>,
}

impl Default for RlGuiApp {
    fn default() -> Self {
        let language = detect_initial_language();
        let logs = run_gui_update_flow("rl-toolkit", language);
        let saved = load_saved_settings();

        Self {
            language,
            tab: Tab::Harvester,
            harvester: saved.harvester,
            replay2json: saved.replay2json,
            coach: saved.coach,
            gemini: saved.gemini,
            coach_view: CoachViewState::default(),
            gemini_view: GeminiViewState::default(),
            logs,
            running: false,
            worker_rx: None,
            worker_cancel: None,
            active_task: None,
        }
    }
}

fn detect_initial_language() -> Language {
    let locale = env::var("LC_ALL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| env::var("LANG").ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if locale.starts_with("ja") {
        Language::Japanese
    } else {
        Language::English
    }
}

impl RlGuiApp {
    fn tr<'a>(&self, en: &'a str, ja: &'a str) -> &'a str {
        match self.language {
            Language::English => en,
            Language::Japanese => ja,
        }
    }

    fn start_task(&mut self, task: TaskKind) {
        if self.running {
            return;
        }

        let (tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_for_worker = Arc::clone(&cancel);
        let running_task = RunningTask::from_task(&task);

        self.logs.push(
            self.tr("starting task...", "タスクを開始しました...")
                .to_string(),
        );
        self.running = true;
        self.worker_cancel = Some(cancel);
        self.worker_rx = Some(rx);
        self.active_task = Some(running_task);

        thread::spawn(move || {
            let result = match task {
                TaskKind::Harvester(settings) => {
                    run_harvester_task(settings, &tx, &cancel_for_worker)
                }
                TaskKind::Replay2Json(settings) => {
                    run_replay2json_task(settings, &tx, &cancel_for_worker)
                }
                TaskKind::Coach(settings) => run_coach_task(settings, &tx, &cancel_for_worker),
                TaskKind::Gemini(settings) => run_gemini_task(settings, &tx, &cancel_for_worker),
            };

            let _ = tx.send(WorkerEvent::Finished(result.map_err(|err| err.to_string())));
        });
    }

    fn stop_task(&mut self) {
        if let Some(cancel) = &self.worker_cancel {
            cancel.store(true, Ordering::Relaxed);
        }
    }

    fn poll_worker_events(&mut self) {
        let mut done = false;
        let mut refresh_coach = false;

        if let Some(rx) = &self.worker_rx {
            while let Ok(event) = rx.try_recv() {
                match event {
                    WorkerEvent::Log(line) => self.logs.push(line),
                    WorkerEvent::GeminiResult {
                        response_text,
                        output_path,
                    } => {
                        self.gemini_view.latest_response = Some(response_text);
                        self.gemini_view.latest_output_path = Some(output_path);
                    }
                    WorkerEvent::Finished(result) => {
                        let finished_task = self.active_task;
                        match result {
                            Ok(()) => {
                                self.logs.push(
                                    self.tr("task finished", "タスクが完了しました").to_string(),
                                );
                                if finished_task == Some(RunningTask::Coach)
                                    && !self.coach.output_dir.trim().is_empty()
                                {
                                    refresh_coach = true;
                                }
                            }
                            Err(err) => self
                                .logs
                                .push(format!("{}: {err}", self.tr("task failed", "タスク失敗"))),
                        }
                        done = true;
                    }
                }
            }
        }

        if done {
            self.running = false;
            self.worker_rx = None;
            self.worker_cancel = None;
            self.active_task = None;
        }

        if refresh_coach && let Err(err) = self.refresh_coach_view() {
            self.logs.push(format!(
                "{}: {err}",
                self.tr("viewer refresh failed", "ビューア更新失敗")
            ));
        }
    }

    fn persist_settings(&self) {
        let settings = SavedGuiSettings {
            harvester: self.harvester.clone(),
            replay2json: self.replay2json.clone(),
            coach: self.coach.clone(),
            gemini: self.gemini.clone(),
        };
        let _ = save_saved_settings(&settings);
    }

    fn refresh_coach_view(&mut self) -> Result<()> {
        let output_dir = self.coach.output_dir.trim();
        if output_dir.is_empty() {
            self.coach_view.summary = None;
            self.coach_view.selected_match_index = 0;
            self.coach_view.load_error = None;
            return Ok(());
        }

        let summary = load_coach_view_from_output_dir(Path::new(output_dir))?;
        self.coach_view.selected_match_index = self
            .coach_view
            .selected_match_index
            .min(summary.matches.len().saturating_sub(1));
        self.coach_view.summary = Some(summary);
        self.coach_view.load_error = None;
        Ok(())
    }

    fn selected_coach_report(&self) -> Option<&CoachAnalysisReport> {
        let summary = self.coach_view.summary.as_ref()?;
        summary
            .loaded_reports
            .get(self.coach_view.selected_match_index)
    }

    fn refresh_gemini_players(&mut self) -> Result<()> {
        let input_path = self.gemini.input_path.trim();
        if input_path.is_empty() {
            self.gemini_view.players.clear();
            self.gemini_view.players_input_path.clear();
            self.gemini_view.player_load_error = None;
            self.gemini.selected_player_name.clear();
            self.gemini.selected_player_team = None;
            self.gemini.selected_player_unique_id = None;
            return Ok(());
        }

        let input_path = to_absolute_path(Path::new(input_path))?;
        if !input_path.is_file() {
            return Err(anyhow!(
                "replay JSON path is not a file: {}",
                input_path.display()
            ));
        }

        let players = list_replay_players(&input_path)?;
        self.gemini_view.players = players;
        self.gemini_view.players_input_path = input_path.display().to_string();
        self.gemini_view.player_load_error = None;
        self.ensure_gemini_player_selection();
        Ok(())
    }

    fn ensure_gemini_player_selection(&mut self) {
        let selected = self.selected_gemini_player_from_loaded();
        if selected.is_some() {
            return;
        }

        if let Some(first) = self.gemini_view.players.first().cloned() {
            self.set_gemini_selected_player(&first);
        } else {
            self.gemini.selected_player_name.clear();
            self.gemini.selected_player_team = None;
            self.gemini.selected_player_unique_id = None;
        }
    }

    fn set_gemini_selected_player(&mut self, player: &CoachTimelinePlayer) {
        self.gemini.selected_player_name = player.player_name.clone();
        self.gemini.selected_player_team = Some(player.team);
        self.gemini.selected_player_unique_id = player.unique_id.clone();
    }

    fn selected_gemini_player_from_loaded(&self) -> Option<CoachTimelinePlayer> {
        let selected_name = self.gemini.selected_player_name.as_str();
        let selected_team = self.gemini.selected_player_team?;
        if selected_name.is_empty() {
            return None;
        }

        if let Some(unique_id) = self
            .gemini
            .selected_player_unique_id
            .as_deref()
            .filter(|value| !value.is_empty())
            && let Some(player) = self
                .gemini_view
                .players
                .iter()
                .find(|player| player.unique_id.as_deref() == Some(unique_id))
        {
            return Some(player.clone());
        }

        self.gemini_view
            .players
            .iter()
            .find(|player| player.team == selected_team && player.player_name == selected_name)
            .cloned()
    }

    fn ui_header(&mut self, ui: &mut egui::Ui) {
        let tab_harvester = self.tr("Replay Harvester", "リプレイ収集");
        let tab_replay2json = self.tr("Replay2JSON", "リプレイJSON変換");
        let tab_coach = self.tr("RL Coach", "RL Coach");
        let tab_gemini = self.tr("Gemini", "Gemini");
        let label_language = self.tr("Language", "言語");
        let button_stop = self.tr("Stop Task", "タスク停止");
        let label_idle = self.tr("Idle", "待機中");
        let previous_language = self.language;

        ui.horizontal(|ui| {
            ui.selectable_value(&mut self.tab, Tab::Harvester, tab_harvester);
            ui.selectable_value(&mut self.tab, Tab::Replay2Json, tab_replay2json);
            ui.selectable_value(&mut self.tab, Tab::Coach, tab_coach);
            ui.selectable_value(&mut self.tab, Tab::Gemini, tab_gemini);

            ui.separator();
            ui.label(label_language);
            egui::ComboBox::from_id_salt("language_selector")
                .selected_text(match self.language {
                    Language::English => "English",
                    Language::Japanese => "日本語",
                })
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut self.language, Language::English, "English");
                    ui.selectable_value(&mut self.language, Language::Japanese, "日本語");
                });

            ui.separator();
            if self.running {
                if ui.button(button_stop).clicked() {
                    self.stop_task();
                }
            } else {
                ui.label(label_idle);
            }
        });

        if self.language != previous_language {
            relocalize_logs(&mut self.logs, previous_language, self.language);
        }
    }

    fn ui_harvester(&mut self, ui: &mut egui::Ui) {
        let heading = self.tr("Replay Harvester", "リプレイ収集");
        let desc = self.tr(
            "Download replays for one player via ballchasing API.",
            "ballchasing API で1人の選手のリプレイをダウンロードします。",
        );
        let label_api_key = self.tr("API Key", "APIキー");
        let label_player = self.tr("Player", "選手名");
        let label_output_dir = self.tr("Output Dir", "出力先ディレクトリ");
        let label_browse = self.tr("Browse...", "参照...");
        let label_saved_layout = self.tr("Saved files layout:", "保存レイアウト:");
        let label_max_pages = self.tr("Max Pages", "最大ページ数");
        let label_request_interval = self.tr("Request Interval (sec)", "リクエスト間隔(秒)");
        let button_start_harvester = self.tr("Start Harvester", "収集開始");
        let mut settings_changed = false;

        ui.heading(heading);
        ui.label(desc);

        ui.horizontal(|ui| {
            ui.label(label_api_key);
            if ui
                .add(egui::TextEdit::singleline(&mut self.harvester.api_key).password(true))
                .changed()
            {
                settings_changed = true;
            }
        });
        ui.horizontal(|ui| {
            ui.label(label_player);
            if ui
                .text_edit_singleline(&mut self.harvester.player)
                .changed()
            {
                settings_changed = true;
            }
        });
        if ui_folder_field(
            ui,
            label_output_dir,
            &mut self.harvester.output_dir,
            label_browse,
        ) {
            settings_changed = true;
        }
        let player_slug = slugify_player_name(&self.harvester.player);
        let save_preview = if self.harvester.output_dir.trim().is_empty() {
            format!("replays/{player_slug}/yyyy-mm-dd/<replay_id>.replay")
        } else {
            format!(
                "{}/replays/{player_slug}/yyyy-mm-dd/<replay_id>.replay",
                self.harvester.output_dir.trim()
            )
        };
        ui.label(label_saved_layout);
        ui.monospace(save_preview);
        ui.horizontal(|ui| {
            ui.label(label_max_pages);
            if ui
                .add(egui::DragValue::new(&mut self.harvester.max_pages).range(1..=100))
                .changed()
            {
                settings_changed = true;
            }
        });
        ui.horizontal(|ui| {
            ui.label(label_request_interval);
            if ui
                .add(
                    egui::DragValue::new(&mut self.harvester.request_interval_seconds)
                        .range(MIN_REQUEST_INTERVAL_SECONDS..=300),
                )
                .changed()
            {
                settings_changed = true;
            }
        });
        if settings_changed {
            self.persist_settings();
        }

        if ui
            .add_enabled(!self.running, egui::Button::new(button_start_harvester))
            .clicked()
        {
            self.start_task(TaskKind::Harvester(self.harvester.clone()));
        }
    }

    fn ui_replay2json(&mut self, ui: &mut egui::Ui) {
        let heading = self.tr("Replay2JSON", "リプレイJSON変換");
        let desc = self.tr(
            "Convert .replay files to JSON.",
            ".replay ファイルを JSON に変換します。",
        );
        let label_input_dir = self.tr("Input Dir", "入力元ディレクトリ");
        let label_output_dir = self.tr("Output Dir", "出力先ディレクトリ");
        let label_browse = self.tr("Browse...", "参照...");
        let label_watch = self.tr(
            "Watch input folder and convert newly added files",
            "入力フォルダを監視して新規ファイルを自動変換",
        );
        let label_watch_interval = self.tr("Watch Interval (sec)", "監視間隔(秒)");
        let label_pretty = self.tr("Pretty JSON", "整形JSON");
        let button_start = self.tr("Start Replay2JSON", "変換開始");
        let mut settings_changed = false;

        ui.heading(heading);
        ui.label(desc);

        if ui_folder_field(
            ui,
            label_input_dir,
            &mut self.replay2json.input_dir,
            label_browse,
        ) {
            settings_changed = true;
        }
        if ui_folder_field(
            ui,
            label_output_dir,
            &mut self.replay2json.output_dir,
            label_browse,
        ) {
            settings_changed = true;
        }
        if ui
            .checkbox(&mut self.replay2json.watch_mode, label_watch)
            .changed()
        {
            settings_changed = true;
        }
        ui.horizontal(|ui| {
            ui.label(label_watch_interval);
            if ui
                .add(
                    egui::DragValue::new(&mut self.replay2json.watch_interval_seconds)
                        .range(MIN_WATCH_INTERVAL_SECONDS..=300),
                )
                .changed()
            {
                settings_changed = true;
            }
        });
        if ui
            .checkbox(&mut self.replay2json.pretty_json, label_pretty)
            .changed()
        {
            settings_changed = true;
        }
        if settings_changed {
            self.persist_settings();
        }

        if ui
            .add_enabled(!self.running, egui::Button::new(button_start))
            .clicked()
        {
            self.start_task(TaskKind::Replay2Json(self.replay2json.clone()));
        }
    }

    fn ui_coach(&mut self, ui: &mut egui::Ui) {
        let heading = self.tr("RL Coach", "RL Coach");
        let desc = self.tr(
            "Analyze replay JSON and review per-match plus batch summaries.",
            "リプレイ JSON を分析し、試合ごとの結果と全体集計を表示します。",
        );
        let label_input_mode = self.tr("Input Mode", "入力モード");
        let label_input_path = self.tr("Input Path", "入力パス");
        let label_output_dir = self.tr("Output Dir", "出力先ディレクトリ");
        let label_browse = self.tr("Browse...", "参照...");
        let label_pretty = self.tr("Pretty JSON", "整形JSON");
        let button_start = self.tr("Start RL Coach", "分析開始");
        let button_refresh = self.tr("Refresh Viewer", "ビューア更新");
        let label_file = self.tr("Single File", "単一ファイル");
        let label_dir = self.tr("Directory", "ディレクトリ");
        let label_output_layout = self.tr("Output layout:", "出力レイアウト:");
        let mut settings_changed = false;
        let previous_input_mode = self.coach.input_mode;

        ui.heading(heading);
        ui.label(desc);

        ui.horizontal(|ui| {
            ui.label(label_input_mode);
            egui::ComboBox::from_id_salt("coach_input_mode")
                .selected_text(match self.coach.input_mode {
                    CoachInputMode::File => label_file,
                    CoachInputMode::Directory => label_dir,
                })
                .show_ui(ui, |ui| {
                    ui.selectable_value(
                        &mut self.coach.input_mode,
                        CoachInputMode::File,
                        label_file,
                    );
                    ui.selectable_value(
                        &mut self.coach.input_mode,
                        CoachInputMode::Directory,
                        label_dir,
                    );
                });
        });
        if self.coach.input_mode != previous_input_mode {
            settings_changed = true;
        }

        if ui_path_field(
            ui,
            label_input_path,
            &mut self.coach.input_path,
            label_browse,
            match self.coach.input_mode {
                CoachInputMode::File => PathPicker::File,
                CoachInputMode::Directory => PathPicker::Folder,
            },
        ) {
            settings_changed = true;
        }
        if ui_folder_field(
            ui,
            label_output_dir,
            &mut self.coach.output_dir,
            label_browse,
        ) {
            settings_changed = true;
        }
        if ui
            .checkbox(&mut self.coach.pretty_json, label_pretty)
            .changed()
        {
            settings_changed = true;
        }
        if settings_changed {
            self.persist_settings();
        }

        let preview = if self.coach.output_dir.trim().is_empty() {
            "analysis/yyyy-mm-dd/<replay_id>.json".to_string()
        } else {
            format!(
                "{}/analysis/yyyy-mm-dd/<replay_id>.json",
                self.coach.output_dir.trim()
            )
        };
        ui.label(label_output_layout);
        ui.monospace(preview);

        ui.horizontal(|ui| {
            if ui
                .add_enabled(!self.running, egui::Button::new(button_start))
                .clicked()
            {
                self.start_task(TaskKind::Coach(self.coach.clone()));
            }
            if ui.button(button_refresh).clicked()
                && let Err(err) = self.refresh_coach_view()
            {
                self.coach_view.load_error = Some(err.to_string());
            }
        });

        egui::ScrollArea::vertical()
            .id_salt("coach_viewer_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                self.ui_coach_viewer(ui);
            });
    }

    fn ui_coach_viewer(&mut self, ui: &mut egui::Ui) {
        ui.separator();
        ui.heading(self.tr("Viewer", "ビューア"));

        if let Some(error) = &self.coach_view.load_error {
            ui.colored_label(egui::Color32::RED, error);
        }

        let Some(summary) = self.coach_view.summary.clone() else {
            ui.label(self.tr(
                "Run RL Coach or refresh the viewer after analysis results exist.",
                "分析結果が出力されたあとに RL Coach を実行するか、ビューアを更新してください。",
            ));
            return;
        };
        let selected_report = self.selected_coach_report().cloned();

        ui.label(format!(
            "{}: {}",
            self.tr("Matches", "試合数"),
            summary.matches.len()
        ));

        egui::CollapsingHeader::new(self.tr("Batch Aggregate", "全体集計"))
            .default_open(true)
            .show(ui, |ui| {
                ui.heading(self.tr("Teams", "チーム"));
                for team in &summary.team_aggregate {
                    ui.group(|ui| {
                        ui.label(format!(
                            "{} ({})",
                            team.name,
                            format_match_record(team.matches, team.wins)
                        ));
                        ui_metrics_table(ui, &team.metrics, self.language);
                    });
                }

                ui.separator();
                ui.heading(self.tr("Players", "プレイヤー"));
                egui::ScrollArea::vertical()
                    .max_height(220.0)
                    .show(ui, |ui| {
                        for player in &summary.player_aggregate {
                            ui.group(|ui| {
                                ui.label(format!(
                                    "{} [{}] ({})",
                                    player.player_name,
                                    team_name_label(player.team),
                                    format_match_record(player.matches, player.wins)
                                ));
                                ui_metrics_table(ui, &player.metrics, self.language);
                            });
                        }
                    });
            });

        ui.separator();
        ui.columns(2, |columns| {
            columns[0].heading(self.tr("Match List", "試合一覧"));
            egui::ScrollArea::vertical()
                .max_height(320.0)
                .show(&mut columns[0], |ui| {
                    for (index, manifest) in summary.matches.iter().enumerate() {
                        let label = format!(
                            "{} | {} | {} | {}-{} | {} | {} | {}",
                            manifest.date,
                            manifest.replay_id,
                            manifest.map,
                            manifest.final_score.blue,
                            manifest.final_score.orange,
                            manifest
                                .winner
                                .clone()
                                .unwrap_or_else(|| "draw".to_string()),
                            format!("{:?}", manifest.parse_quality).to_lowercase(),
                            manifest.diagnosis_count
                        );
                        if ui
                            .selectable_label(self.coach_view.selected_match_index == index, label)
                            .clicked()
                        {
                            self.coach_view.selected_match_index = index;
                        }
                    }
                });

            columns[1].heading(self.tr("Match Detail", "試合詳細"));
            if let Some(report) = selected_report.as_ref() {
                ui_coach_report_detail(&mut columns[1], report, self.language);
            }
        });
    }

    fn ui_gemini(&mut self, ui: &mut egui::Ui) {
        let heading = self.tr("Gemini Analysis", "Gemini分析");
        let desc = self.tr(
            "Send compact match statistics and pre-goal windows to Gemini.",
            "試合統計と失点前10秒の圧縮データを Gemini に送って分析します。",
        );
        let label_api_key = self.tr("Gemini API Key", "Gemini APIキー");
        let label_input_path = self.tr("Replay JSON File", "リプレイJSONファイル");
        let label_output_dir = self.tr("Output Dir", "出力先ディレクトリ");
        let label_model = self.tr("Model", "モデル");
        let label_target = self.tr("Analysis Target", "分析対象選手");
        let label_culprit = self.tr("Investigate Losing Player", "戦犯調査を行う");
        let label_browse = self.tr("Browse...", "参照...");
        let button_load_players = self.tr("Load Players", "選手読込");
        let button_start = self.tr("Start Gemini Analysis", "Gemini分析開始");
        let label_latest = self.tr("Latest Response", "最新の応答");
        let label_no_players = self.tr(
            "Load a replay JSON to choose a player.",
            "リプレイJSONを読み込むと選手を選択できます。",
        );
        let mut settings_changed = false;
        let mut input_path_changed = false;

        ui.heading(heading);
        ui.label(desc);

        ui.horizontal(|ui| {
            ui.label(label_api_key);
            if ui
                .add(egui::TextEdit::singleline(&mut self.gemini.api_key).password(true))
                .changed()
            {
                settings_changed = true;
            }
        });
        if ui_path_field(
            ui,
            label_input_path,
            &mut self.gemini.input_path,
            label_browse,
            PathPicker::File,
        ) {
            settings_changed = true;
            input_path_changed = true;
        }
        if ui_folder_field(
            ui,
            label_output_dir,
            &mut self.gemini.output_dir,
            label_browse,
        ) {
            settings_changed = true;
        }
        ui.horizontal(|ui| {
            ui.label(label_model);
            if ui.text_edit_singleline(&mut self.gemini.model).changed() {
                settings_changed = true;
            }
        });
        if ui
            .checkbox(&mut self.gemini.culprit_investigation, label_culprit)
            .changed()
        {
            settings_changed = true;
        }

        if input_path_changed {
            let input_path = self.gemini.input_path.trim();
            if Path::new(input_path).is_file() {
                if let Err(err) = self.refresh_gemini_players() {
                    self.gemini_view.player_load_error = Some(err.to_string());
                }
            } else {
                self.gemini_view.players.clear();
                self.gemini_view.players_input_path.clear();
                self.gemini_view.player_load_error = None;
            }
        }

        ui.horizontal(|ui| {
            ui.label(label_target);
            if ui
                .add_enabled(!self.running, egui::Button::new(button_load_players))
                .clicked()
            {
                match self.refresh_gemini_players() {
                    Ok(()) => settings_changed = true,
                    Err(err) => self.gemini_view.player_load_error = Some(err.to_string()),
                }
            }
        });
        if self.gemini_view.players.is_empty() {
            ui.label(label_no_players);
        } else {
            let selected_text = self
                .selected_gemini_player_from_loaded()
                .as_ref()
                .map(timeline_player_label)
                .unwrap_or_else(|| label_no_players.to_string());
            let players = self.gemini_view.players.clone();
            egui::ComboBox::from_id_salt("gemini_player_select")
                .selected_text(selected_text)
                .show_ui(ui, |ui| {
                    for player in &players {
                        let label = timeline_player_label(player);
                        let selected = self
                            .selected_gemini_player_from_loaded()
                            .as_ref()
                            .map(|selected| same_timeline_player(selected, player))
                            .unwrap_or(false);
                        if ui.selectable_label(selected, label).clicked() {
                            self.set_gemini_selected_player(player);
                            settings_changed = true;
                        }
                    }
                });
        }
        if let Some(err) = &self.gemini_view.player_load_error {
            ui.label(format!(
                "{}: {err}",
                self.tr("Player load failed", "選手読込失敗")
            ));
        }

        if settings_changed {
            self.persist_settings();
        }

        if ui
            .add_enabled(
                !self.running && !self.gemini.selected_player_name.trim().is_empty(),
                egui::Button::new(button_start),
            )
            .clicked()
        {
            self.start_task(TaskKind::Gemini(self.gemini.clone()));
        }

        ui.separator();
        ui.heading(label_latest);
        if let Some(path) = &self.gemini_view.latest_output_path {
            ui.monospace(path.display().to_string());
        }
        if let Some(response) = &self.gemini_view.latest_response {
            egui::ScrollArea::vertical()
                .id_salt("gemini_response_scroll")
                .max_height(360.0)
                .show(ui, |ui| {
                    ui.label(response);
                });
        } else {
            ui.label(self.tr(
                "Run Gemini Analysis to display the model response.",
                "Gemini分析を実行するとモデル応答を表示します。",
            ));
        }
    }

    fn ui_logs(&mut self, ui: &mut egui::Ui) {
        ui.separator();
        ui.heading(self.tr("Logs", "ログ"));

        egui::ScrollArea::vertical()
            .stick_to_bottom(true)
            .max_height(280.0)
            .show(ui, |ui| {
                for line in &self.logs {
                    ui.monospace(line);
                }
            });
    }
}

fn relocalize_logs(logs: &mut [String], from: Language, to: Language) {
    if from == to {
        return;
    }

    for line in logs {
        match (from, to) {
            (Language::English, Language::Japanese) => {
                *line = line.replace("starting task...", "タスクを開始しました...");
                *line = line.replace("task finished", "タスクが完了しました");
                *line = line.replace("task failed:", "タスク失敗:");
                *line = line.replace("viewer refresh failed:", "ビューア更新失敗:");

                *line = line.replace(": up to date (current v", ": 最新です (現在 v");
                *line = line.replace(": update available ", ": 更新があります ");
                *line = line.replace(": skipped by user", ": ユーザーが更新をスキップしました");
                *line = line.replace(": check failed:", ": 確認に失敗しました:");
                *line = line.replace(": update failed:", ": 更新失敗:");
            }
            (Language::Japanese, Language::English) => {
                *line = line.replace("タスクを開始しました...", "starting task...");
                *line = line.replace("タスクが完了しました", "task finished");
                *line = line.replace("タスク失敗:", "task failed:");
                *line = line.replace("ビューア更新失敗:", "viewer refresh failed:");

                *line = line.replace(": 最新です (現在 v", ": up to date (current v");
                *line = line.replace(": 更新があります ", ": update available ");
                *line = line.replace(": ユーザーが更新をスキップしました", ": skipped by user");
                *line = line.replace(": 確認に失敗しました:", ": check failed:");
                *line = line.replace(": 更新失敗:", ": update failed:");
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PathPicker {
    File,
    Folder,
}

fn ui_folder_field(ui: &mut egui::Ui, label: &str, value: &mut String, browse_label: &str) -> bool {
    ui_path_field(ui, label, value, browse_label, PathPicker::Folder)
}

fn ui_path_field(
    ui: &mut egui::Ui,
    label: &str,
    value: &mut String,
    browse_label: &str,
    picker: PathPicker,
) -> bool {
    let mut changed = false;

    ui.horizontal(|ui| {
        ui.label(label);
        if ui
            .add(egui::TextEdit::singleline(value).desired_width(420.0))
            .changed()
        {
            changed = true;
        }
        if ui.button(browse_label).clicked() {
            let mut dialog = FileDialog::new();
            if !value.trim().is_empty() {
                dialog = dialog.set_directory(value.as_str());
            }
            let selected = match picker {
                PathPicker::File => dialog.pick_file(),
                PathPicker::Folder => dialog.pick_folder(),
            };
            if let Some(path) = selected {
                *value = path.display().to_string();
                changed = true;
            }
        }
    });

    changed
}

fn load_coach_view_from_output_dir(path: &Path) -> Result<CoachBatchSummary> {
    coach_load_reports(path)
}

fn format_match_record(matches: Option<usize>, wins: Option<usize>) -> String {
    let matches = matches.unwrap_or(0);
    let wins = wins.unwrap_or(0);
    format!("{wins}/{matches}")
}

fn team_name_label(team: u8) -> &'static str {
    if team == 0 { "Blue" } else { "Orange" }
}

fn timeline_player_label(player: &CoachTimelinePlayer) -> String {
    format!("{} [{}]", player.player_name, team_name_label(player.team))
}

fn same_timeline_player(left: &CoachTimelinePlayer, right: &CoachTimelinePlayer) -> bool {
    match (left.unique_id.as_deref(), right.unique_id.as_deref()) {
        (Some(left_id), Some(right_id)) if !left_id.is_empty() && !right_id.is_empty() => {
            left_id == right_id
        }
        _ => left.team == right.team && left.player_name == right.player_name,
    }
}

fn team_name_label_for_language(team: u8, language: Language) -> &'static str {
    match (team, language) {
        (0, Language::Japanese) => "ブルー",
        (1, Language::Japanese) => "オレンジ",
        _ => team_name_label(team),
    }
}

fn localized_diagnosis_label(label: CoachDiagnosisLabel, language: Language) -> &'static str {
    match (label, language) {
        (CoachDiagnosisLabel::KickoffBreakdown, Language::Japanese) => "キックオフ崩れ",
        (CoachDiagnosisLabel::FailedClear, Language::Japanese) => "クリア失敗",
        (CoachDiagnosisLabel::DemoDisruption, Language::Japanese) => "デモ妨害",
        (CoachDiagnosisLabel::LowBoostDefense, Language::Japanese) => "低ブースト守備",
        (CoachDiagnosisLabel::DoubleCommit, Language::Japanese) => "ダブルコミット",
        (CoachDiagnosisLabel::RotationGap, Language::Japanese) => "ローテ崩れ",
        (CoachDiagnosisLabel::ReboundPressure, Language::Japanese) => "リバウンド圧力",
        (CoachDiagnosisLabel::KickoffBreakdown, Language::English) => "Kickoff Breakdown",
        (CoachDiagnosisLabel::FailedClear, Language::English) => "Failed Clear",
        (CoachDiagnosisLabel::DemoDisruption, Language::English) => "Demo Disruption",
        (CoachDiagnosisLabel::LowBoostDefense, Language::English) => "Low Boost Defense",
        (CoachDiagnosisLabel::DoubleCommit, Language::English) => "Double Commit",
        (CoachDiagnosisLabel::RotationGap, Language::English) => "Rotation Gap",
        (CoachDiagnosisLabel::ReboundPressure, Language::English) => "Rebound Pressure",
    }
}

fn localized_diagnosis_metric(metric: &str, language: Language) -> String {
    match (metric, language) {
        ("goal_after_kickoff_seconds", Language::Japanese) => "キックオフ後失点秒数".to_string(),
        ("last_touch_before_goal_seconds", Language::Japanese) => {
            "最終守備タッチから失点までの秒数".to_string()
        }
        ("demo_before_goal_seconds", Language::Japanese) => "デモから失点までの秒数".to_string(),
        ("average_defender_boost", Language::Japanese) => "守備側平均ブースト".to_string(),
        ("defenders_near_ball", Language::Japanese) => "ボール付近の守備人数".to_string(),
        ("closest_defender_to_own_goal", Language::Japanese) => {
            "自ゴール最寄り守備距離".to_string()
        }
        ("sustained_pressure_seconds", Language::Japanese) => "継続圧力時間".to_string(),
        _ => metric.to_string(),
    }
}

fn localized_diagnosis_context(context: &str, language: Language) -> String {
    if language == Language::English {
        return context.to_string();
    }

    if let Some(player) = context.strip_suffix(" was removed from the play before the goal") {
        return format!("{player} が失点前にプレーから外された");
    }

    match context {
        "goal arrived before either team settled after kickoff reset" => {
            "キックオフの陣形が整う前に失点した".to_string()
        }
        "defending team touched the ball shortly before conceding but did not exit the defensive half" => {
            "守備側が触れたが、自陣からボールを逃がせないまま失点した".to_string()
        }
        "defending team entered the goal sequence with low average boost" => {
            "守備側が低ブーストのまま失点シーケンスに入った".to_string()
        }
        "multiple defenders collapsed on the same ball in the defensive half" => {
            "自陣で複数人が同じボールに寄り過ぎた".to_string()
        }
        "no defender was close enough to cover the goal line during the final approach" => {
            "最終局面でゴール前をカバーできる守備者がいなかった".to_string()
        }
        "the ball stayed in the defending third under sustained attacking pressure" => {
            "自陣深くで攻撃圧を受け続けたまま失点した".to_string()
        }
        _ => context.to_string(),
    }
}

fn localized_metric_quality(quality: rl_coach::MetricQuality, language: Language) -> &'static str {
    match (quality, language) {
        (rl_coach::MetricQuality::Exact, Language::Japanese) => "正確",
        (rl_coach::MetricQuality::Estimated, Language::Japanese) => "推定",
        (rl_coach::MetricQuality::Unavailable, Language::Japanese) => "利用不可",
        (rl_coach::MetricQuality::Exact, Language::English) => "exact",
        (rl_coach::MetricQuality::Estimated, Language::English) => "estimated",
        (rl_coach::MetricQuality::Unavailable, Language::English) => "unavailable",
    }
}

fn localized_metric_note(note: &str, language: Language) -> String {
    match (note, language) {
        ("derived from network frame sampling", Language::Japanese) => {
            "network frame のサンプリングから算出".to_string()
        }
        ("aggregated across analyzed matches", Language::Japanese) => {
            "分析済み試合を集計した値".to_string()
        }
        ("network frames unavailable", Language::Japanese) => {
            "network frame が利用できません".to_string()
        }
        ("header or PRI stat unavailable", Language::Japanese) => {
            "header または PRI の統計が利用できません".to_string()
        }
        _ => note.to_string(),
    }
}

fn ui_metrics_table(
    ui: &mut egui::Ui,
    metrics: &std::collections::BTreeMap<String, CoachMetricValue>,
    language: Language,
) {
    egui::Grid::new(ui.next_auto_id())
        .striped(true)
        .show(ui, |ui| {
            for (name, value) in metrics {
                ui.monospace(name);
                ui.label(format_metric_value(value, language));
                ui.end_row();
            }
        });
}

fn ui_coach_report_detail(ui: &mut egui::Ui, report: &CoachAnalysisReport, language: Language) {
    ui.label(format!(
        "{} | {} | {}-{} | {}",
        report.meta.date,
        report.meta.map,
        report.meta.final_score.blue,
        report.meta.final_score.orange,
        report
            .meta
            .winner
            .clone()
            .unwrap_or_else(|| "draw".to_string())
    ));
    ui.label(format!(
        "{}: {:?}",
        tr_for_language(language, "Parse Quality", "解析品質"),
        report.availability.parse_quality
    ));
    if !report.warnings.is_empty() {
        ui.separator();
        ui.heading(tr_for_language(language, "Warnings", "警告"));
        for warning in &report.warnings {
            ui.label(warning);
        }
    }

    ui.separator();
    ui.heading(tr_for_language(language, "Team Metrics", "チーム指標"));
    for team in &report.team_metrics {
        ui.group(|ui| {
            ui.label(team.name.as_str());
            ui_metrics_table(ui, &team.metrics, language);
        });
    }

    ui.separator();
    ui.heading(tr_for_language(
        language,
        "Player Metrics",
        "プレイヤー指標",
    ));
    for player in &report.player_metrics {
        ui.group(|ui| {
            ui.label(format!(
                "{} [{}]",
                player.player_name,
                team_name_label(player.team)
            ));
            ui_metrics_table(ui, &player.metrics, language);
        });
    }

    ui.separator();
    ui.heading(tr_for_language(language, "Goal Diagnoses", "失点診断"));
    if report.concede_diagnoses.is_empty() {
        ui.label(tr_for_language(
            language,
            "No concede diagnoses available for this report.",
            "この試合では失点診断を表示できません。",
        ));
    } else {
        for diagnosis in &report.concede_diagnoses {
            ui.group(|ui| {
                let scoring_team = team_name_label_for_language(diagnosis.scoring_team, language);
                let conceding_team =
                    team_name_label_for_language(diagnosis.conceding_team, language);
                let goal_header = match language {
                    Language::English => {
                        format!(
                            "Goal {} | {} -> {}",
                            diagnosis.goal_index + 1,
                            scoring_team,
                            conceding_team
                        )
                    }
                    Language::Japanese => {
                        format!(
                            "ゴール {} | {} 得点 / {} 失点",
                            diagnosis.goal_index + 1,
                            scoring_team,
                            conceding_team
                        )
                    }
                };
                ui.label(goal_header);
                for label in &diagnosis.labels {
                    ui.label(format!(
                        "{} ({:.2})",
                        localized_diagnosis_label(label.label, language),
                        label.score
                    ));
                    for evidence in &label.evidence {
                        let metric = localized_diagnosis_metric(&evidence.metric, language);
                        let context =
                            localized_diagnosis_context(&evidence.frame_context, language);
                        ui.monospace(format!("{} | {} | {}", metric, evidence.value, context));
                    }
                }
            });
        }
    }
}

fn format_metric_value(value: &CoachMetricValue, language: Language) -> String {
    let quality = localized_metric_quality(value.quality, language);
    match value.value {
        Some(number) if (number.fract()).abs() < 0.001 => match &value.note {
            Some(note) => format!(
                "{number:.0} [{quality}] - {}",
                localized_metric_note(note, language)
            ),
            None => format!("{number:.0} [{quality}]"),
        },
        Some(number) => match &value.note {
            Some(note) => format!(
                "{number:.2} [{quality}] - {}",
                localized_metric_note(note, language)
            ),
            None => format!("{number:.2} [{quality}]"),
        },
        None => format!(
            "{} [{quality}]",
            value
                .note
                .as_deref()
                .map(|note| localized_metric_note(note, language))
                .unwrap_or_else(|| "n/a".to_string()),
        ),
    }
}

fn settings_directory() -> Result<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        let app_data = env::var("APPDATA").context("APPDATA environment variable is not set")?;
        if app_data.trim().is_empty() {
            anyhow::bail!("APPDATA environment variable is empty");
        }
        Ok(PathBuf::from(app_data).join("rl-toolkit"))
    }

    #[cfg(target_os = "macos")]
    {
        let home = env::var("HOME").context("HOME environment variable is not set")?;
        if home.trim().is_empty() {
            anyhow::bail!("HOME environment variable is empty");
        }
        Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("rl-toolkit"))
    }

    #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
    {
        let xdg_candidate = env::var("XDG_CONFIG_HOME")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from);

        if let Some(xdg_home) = xdg_candidate {
            Ok(xdg_home.join("rl-toolkit"))
        } else {
            let home = env::var("HOME").context("HOME environment variable is not set")?;
            if home.trim().is_empty() {
                anyhow::bail!("HOME environment variable is empty");
            }
            Ok(PathBuf::from(home).join(".config").join("rl-toolkit"))
        }
    }
}

fn saved_settings_path() -> Result<PathBuf> {
    Ok(settings_directory()?.join("gui-settings.json"))
}

fn load_saved_settings() -> SavedGuiSettings {
    let path = match saved_settings_path() {
        Ok(path) => path,
        Err(err) => {
            eprintln!("[settings] no config path, using defaults: {err}");
            return SavedGuiSettings::default();
        }
    };

    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) => {
            if err.kind() != std::io::ErrorKind::NotFound {
                eprintln!("[settings] failed to read {}: {err}", path.display());
            }
            return SavedGuiSettings::default();
        }
    };

    serde_json::from_str::<SavedGuiSettings>(&raw).unwrap_or_else(|err| {
        eprintln!("[settings] failed to parse {}: {err}", path.display());
        SavedGuiSettings::default()
    })
}

fn save_saved_settings(settings: &SavedGuiSettings) -> Result<()> {
    let path = saved_settings_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create settings directory {}", parent.display()))?;
    }

    let payload = serde_json::to_string_pretty(settings).context("failed to serialize settings")?;
    fs::write(&path, payload)
        .with_context(|| format!("failed to write settings file {}", path.display()))?;
    Ok(())
}

impl eframe::App for RlGuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_worker_events();

        egui::CentralPanel::default().show(ctx, |ui| {
            self.ui_header(ui);
            ui.separator();

            match self.tab {
                Tab::Harvester => self.ui_harvester(ui),
                Tab::Replay2Json => self.ui_replay2json(ui),
                Tab::Coach => self.ui_coach(ui),
                Tab::Gemini => self.ui_gemini(ui),
            }

            self.ui_logs(ui);
        });
    }
}

fn run_harvester_task(
    settings: HarvesterSettings,
    tx: &mpsc::Sender<WorkerEvent>,
    cancel: &AtomicBool,
) -> Result<()> {
    let api_key = settings.api_key.trim().to_string();
    let player = settings.player.trim().to_string();
    let output_dir = PathBuf::from(settings.output_dir.trim());
    let request_interval_seconds = settings
        .request_interval_seconds
        .max(MIN_REQUEST_INTERVAL_SECONDS);
    let max_pages = settings.max_pages.max(1) as usize;

    if api_key.is_empty() {
        return Err(anyhow!("API key is required"));
    }
    if player.is_empty() {
        return Err(anyhow!("player is required"));
    }
    if output_dir.as_os_str().is_empty() {
        return Err(anyhow!("output directory is required"));
    }

    let output_dir = to_absolute_path(&output_dir)?;
    let request_interval = Duration::from_secs(request_interval_seconds);
    let player_slug = slugify_player_name(&player);
    let save_root = output_dir.join("replays").join(&player_slug);

    emit_log(
        tx,
        format!(
            "harvester started: player={player}, output_base={}, max_pages={max_pages}, request_interval={}s",
            output_dir.display(),
            request_interval_seconds
        ),
    );
    emit_log(
        tx,
        format!(
            "replays will be saved under: {}",
            save_root.as_path().display()
        ),
    );

    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent("rl-common-gui/0.1.0")
        .build()
        .context("failed to build HTTP client")?;

    let mut next_url: Option<String> = None;
    let mut seen_ids = HashSet::new();
    let mut downloaded = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    for page_index in 0..max_pages {
        if cancel.load(Ordering::Relaxed) {
            emit_log(tx, "harvester cancelled");
            break;
        }

        let response = fetch_replay_page(
            &client,
            &api_key,
            &player,
            next_url.as_deref(),
            request_interval,
        )
        .with_context(|| format!("failed to fetch replay list page {}", page_index + 1))?;

        emit_log(
            tx,
            format!(
                "fetched replay list page {} ({} items)",
                page_index + 1,
                response.list.len()
            ),
        );

        for replay in response.list {
            if cancel.load(Ordering::Relaxed) {
                emit_log(tx, "harvester cancelled");
                break;
            }
            if !seen_ids.insert(replay.id.clone()) {
                continue;
            }

            let target_path = replay_output_path(&output_dir, &player_slug, &replay);
            if target_path.exists() {
                skipped += 1;
                continue;
            }

            let Some(parent) = target_path.parent() else {
                failed += 1;
                continue;
            };
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create replay directory for {}",
                    target_path.display()
                )
            })?;

            match download_replay_file(&client, &api_key, &replay.id, request_interval) {
                Ok(payload) => {
                    fs::write(&target_path, payload)
                        .with_context(|| format!("failed to save {}", target_path.display()))?;
                    downloaded += 1;
                    emit_log(tx, format!("downloaded {}", target_path.display()));
                }
                Err(err) => {
                    failed += 1;
                    emit_log(tx, format!("failed {} ({err})", replay.id));
                }
            }
        }

        next_url = normalize_next_url(response.next);
        if next_url.is_none() {
            break;
        }
    }

    emit_log(
        tx,
        format!("harvester done: downloaded={downloaded}, skipped={skipped}, failed={failed}"),
    );
    if downloaded == 0 && skipped > 0 && failed == 0 {
        emit_log(
            tx,
            format!(
                "all matching replays already existed under {}",
                save_root.as_path().display()
            ),
        );
    }
    Ok(())
}

fn run_replay2json_task(
    settings: Replay2JsonSettings,
    tx: &mpsc::Sender<WorkerEvent>,
    cancel: &AtomicBool,
) -> Result<()> {
    if settings.input_dir.trim().is_empty() {
        return Err(anyhow!("input directory is required"));
    }
    if settings.output_dir.trim().is_empty() {
        return Err(anyhow!("output directory is required"));
    }

    let input_dir = fs::canonicalize(settings.input_dir.trim())
        .with_context(|| format!("failed to access input directory {}", settings.input_dir))?;
    if !input_dir.is_dir() {
        return Err(anyhow!(
            "input path is not a directory: {}",
            input_dir.display()
        ));
    }

    let output_dir = PathBuf::from(settings.output_dir.trim());
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let watch_interval_seconds = settings
        .watch_interval_seconds
        .max(MIN_WATCH_INTERVAL_SECONDS);

    emit_log(
        tx,
        format!(
            "replay2json started: input={}, output={}, watch={}, watch_interval={}s",
            input_dir.display(),
            output_dir.display(),
            settings.watch_mode,
            watch_interval_seconds
        ),
    );

    let mut processed = HashSet::new();

    loop {
        if cancel.load(Ordering::Relaxed) {
            emit_log(tx, "replay2json cancelled");
            break;
        }

        let summary = scan_and_convert_replays(
            &input_dir,
            &output_dir,
            settings.pretty_json,
            &mut processed,
            tx,
            cancel,
        )?;

        emit_log(
            tx,
            format!(
                "scan done: converted={}, skipped={}, failed={}",
                summary.converted, summary.skipped, summary.failed
            ),
        );

        if !settings.watch_mode {
            break;
        }

        for _ in 0..watch_interval_seconds {
            if cancel.load(Ordering::Relaxed) {
                emit_log(tx, "replay2json cancelled");
                return Ok(());
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    Ok(())
}

fn run_coach_task(
    settings: CoachSettings,
    tx: &mpsc::Sender<WorkerEvent>,
    cancel: &AtomicBool,
) -> Result<()> {
    if settings.input_path.trim().is_empty() {
        return Err(anyhow!("input path is required"));
    }
    if settings.output_dir.trim().is_empty() {
        return Err(anyhow!("output directory is required"));
    }
    if cancel.load(Ordering::Relaxed) {
        emit_log(tx, "rl-coach cancelled");
        return Ok(());
    }

    let input_path = PathBuf::from(settings.input_path.trim());
    let output_dir = PathBuf::from(settings.output_dir.trim());
    let input_path = to_absolute_path(&input_path)?;
    let output_dir = to_absolute_path(&output_dir)?;

    emit_log(
        tx,
        format!(
            "rl-coach started: mode={:?}, input={}, output={}, pretty={}",
            settings.input_mode,
            input_path.display(),
            output_dir.display(),
            settings.pretty_json
        ),
    );

    let summary = coach_analyze_path(&input_path, &output_dir, settings.pretty_json)?;
    if cancel.load(Ordering::Relaxed) {
        emit_log(tx, "rl-coach cancelled");
        return Ok(());
    }

    emit_log(
        tx,
        format!(
            "rl-coach done: matches={}, team_aggregate={}, player_aggregate={}",
            summary.matches.len(),
            summary.team_aggregate.len(),
            summary.player_aggregate.len()
        ),
    );

    for manifest in summary.matches.iter().take(5) {
        emit_log(
            tx,
            format!(
                "report {} {}-{} {}",
                manifest.replay_id,
                manifest.final_score.blue,
                manifest.final_score.orange,
                manifest.report_path
            ),
        );
    }

    Ok(())
}

fn run_gemini_task(
    settings: GeminiSettings,
    tx: &mpsc::Sender<WorkerEvent>,
    cancel: &AtomicBool,
) -> Result<()> {
    let api_key = settings.api_key.trim().to_string();
    let input_path = PathBuf::from(settings.input_path.trim());
    let output_dir = PathBuf::from(settings.output_dir.trim());
    let model = normalize_gemini_model(&settings.model)?;

    if api_key.is_empty() {
        return Err(anyhow!("Gemini API key is required"));
    }
    if input_path.as_os_str().is_empty() {
        return Err(anyhow!("replay JSON file is required"));
    }
    if output_dir.as_os_str().is_empty() {
        return Err(anyhow!("output directory is required"));
    }
    let analysis_target = gemini_target_from_settings(&settings)?;
    if cancel.load(Ordering::Relaxed) {
        emit_log(tx, "gemini analysis cancelled");
        return Ok(());
    }

    let input_path = to_absolute_path(&input_path)?;
    if !input_path.is_file() {
        return Err(anyhow!(
            "replay JSON path is not a file: {}",
            input_path.display()
        ));
    }
    let output_dir = to_absolute_path(&output_dir)?;

    emit_log(
        tx,
        format!(
            "gemini analysis started: input={}, output={}, model={}",
            input_path.display(),
            output_dir.display(),
            model
        ),
    );

    let payload = build_gemini_match_payload_for_player(&input_path, Some(&analysis_target))?;
    let request = build_gemini_request(&payload, settings.culprit_investigation)?;
    if cancel.load(Ordering::Relaxed) {
        emit_log(tx, "gemini analysis cancelled");
        return Ok(());
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .user_agent("rl-common-gui/0.1.0")
        .build()
        .context("failed to build HTTP client")?;
    let url =
        format!("https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent");
    let response = client
        .post(url)
        .header("x-goog-api-key", &api_key)
        .json(&request)
        .send()
        .context("failed to call Gemini API")?;
    let status = response.status();
    let body = response
        .text()
        .context("failed to read Gemini API response body")?;
    if !status.is_success() {
        return Err(anyhow!("Gemini API returned {status}: {body}"));
    }

    let raw_response: serde_json::Value =
        serde_json::from_str(&body).context("failed to parse Gemini API response JSON")?;
    let parsed_response: GeminiGenerateResponse = serde_json::from_value(raw_response.clone())
        .context("failed to decode Gemini API response")?;
    let response_text = gemini_response_text(&parsed_response)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| anyhow!("Gemini API response did not contain text"))?;
    let finish_reasons = gemini_finish_reasons(&parsed_response);
    if finish_reasons.iter().any(|reason| reason == "MAX_TOKENS") {
        return Err(anyhow!(
            "Gemini API response was truncated by max output tokens ({GEMINI_MAX_OUTPUT_TOKENS}); try a shorter prompt or raise GEMINI_MAX_OUTPUT_TOKENS"
        ));
    }

    let json_output_path = output_dir
        .join("gemini")
        .join(&payload.match_summary.date)
        .join(format!("{}.json", payload.match_summary.replay_id));
    let html_output_path = output_dir
        .join("gemini")
        .join(&payload.match_summary.date)
        .join(format!("{}.html", payload.match_summary.replay_id));
    if let Some(parent) = json_output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }

    let output = GeminiAnalysisOutput {
        model: model.clone(),
        replay_id: payload.match_summary.replay_id.clone(),
        date: payload.match_summary.date.clone(),
        analysis_target: payload.analysis_target.clone(),
        culprit_investigation: settings.culprit_investigation,
        response_text: response_text.clone(),
        raw_response,
        finish_reasons,
        usage_metadata: parsed_response.usage_metadata,
    };
    let output_bytes =
        serde_json::to_vec_pretty(&output).context("failed to serialize Gemini analysis output")?;
    fs::write(&json_output_path, output_bytes)
        .with_context(|| format!("failed to write {}", json_output_path.display()))?;

    let html = build_gemini_report_html(
        &model,
        &payload.match_summary.replay_id,
        &payload.match_summary.date,
        &response_text,
    );
    fs::write(&html_output_path, html)
        .with_context(|| format!("failed to write {}", html_output_path.display()))?;

    emit_log(
        tx,
        format!("gemini analysis done: {}", html_output_path.display()),
    );
    let _ = tx.send(WorkerEvent::GeminiResult {
        response_text,
        output_path: html_output_path,
    });
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateRequest {
    contents: Vec<GeminiContent>,
    generation_config: GeminiGenerationConfig,
}

#[derive(Debug, Serialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiPart {
    text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerationConfig {
    temperature: f32,
    max_output_tokens: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    usage_metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    finish_reason: Option<String>,
    content: Option<GeminiResponseContent>,
}

#[derive(Debug, Deserialize)]
struct GeminiResponseContent {
    #[serde(default)]
    parts: Vec<GeminiResponsePart>,
}

#[derive(Debug, Deserialize)]
struct GeminiResponsePart {
    text: Option<String>,
}

#[derive(Debug, Serialize)]
struct GeminiAnalysisOutput {
    model: String,
    replay_id: String,
    date: String,
    analysis_target: Option<CoachTimelinePlayer>,
    culprit_investigation: bool,
    response_text: String,
    raw_response: serde_json::Value,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    finish_reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage_metadata: Option<serde_json::Value>,
}

fn gemini_target_from_settings(settings: &GeminiSettings) -> Result<CoachTimelinePlayer> {
    let player_name = settings.selected_player_name.trim();
    let team = settings
        .selected_player_team
        .ok_or_else(|| anyhow!("analysis target player is required"))?;
    if player_name.is_empty() {
        return Err(anyhow!("analysis target player is required"));
    }

    Ok(CoachTimelinePlayer {
        player_name: player_name.to_string(),
        team,
        unique_id: settings.selected_player_unique_id.clone(),
    })
}

fn normalize_gemini_model(model: &str) -> Result<String> {
    let model = model.trim().trim_start_matches("models/").to_string();
    if model.is_empty() {
        return Err(anyhow!("Gemini model is required"));
    }
    if model.contains('/') {
        return Err(anyhow!("Gemini model must not contain '/'"));
    }
    Ok(model)
}

fn build_gemini_request(
    payload: &rl_coach::GeminiMatchPayload,
    culprit_investigation: bool,
) -> Result<GeminiGenerateRequest> {
    Ok(GeminiGenerateRequest {
        contents: vec![GeminiContent {
            role: "user".to_string(),
            parts: vec![GeminiPart {
                text: build_gemini_prompt(payload, culprit_investigation)?,
            }],
        }],
        generation_config: GeminiGenerationConfig {
            temperature: 0.2,
            max_output_tokens: GEMINI_MAX_OUTPUT_TOKENS,
        },
    })
}

fn build_gemini_prompt(
    payload: &rl_coach::GeminiMatchPayload,
    culprit_investigation: bool,
) -> Result<String> {
    let payload_json =
        serde_json::to_string(payload).context("failed to serialize Gemini match payload")?;
    let data_scope = if payload.availability.network_frames {
        "データ状況: network frame は利用可能です。raw network_frames 全体は送っていませんが、失点前の位置/速度/boost は concede_windows.frames に network frame 由来の圧縮時系列として含まれています。「network frame データが利用できない」とは書かないでください。"
    } else {
        "データ状況: network frame は利用できません。concede_windows.frames が空または不足するため、詳細な動きやポジショニングは統計とヘッダー情報の範囲で慎重に推定してください。"
    };
    let target_scope = payload
        .analysis_target
        .as_ref()
        .map(|target| {
            format!(
                "分析対象: {} [{}] を主対象にしてください。試合全体の文脈は維持しつつ、この選手の失点関与、改善点、優先練習メニューを優先してください。",
                target.player_name,
                team_name_label(target.team)
            )
        })
        .unwrap_or_else(|| {
            "分析対象: 特定選手の指定はありません。チーム全体を対象に分析してください。".to_string()
        });
    let culprit_scope = if culprit_investigation {
        "戦犯調査: 実施してください。ここでの「戦犯」は負けた主因となった選手です。敗北チームの選手を対象に、シュート数、アシスト数、セーブ数だけでなく、失点前10秒の位置/速度/boost、ローテーションの乱れ、空振りやタッチ失敗の可能性、守備復帰、ダブルコミット、低boost守備、既存診断ヒントを総合して1人を特定してください。根拠が不足する場合は無理に断定せず「特定不能」とし、不足している根拠も書いてください。"
    } else {
        "戦犯調査: 実施しないでください。負けた原因を個人名で断定せず、通常の試合分析と改善点に集中してください。"
    };
    let output_headings = if culprit_investigation {
        "出力は次の見出しで簡潔にまとめてください: 1. 試合概要 2. 失点原因 3. 改善点 4. 優先練習メニュー 5. 戦犯調査。"
    } else {
        "出力は次の見出しで簡潔にまとめてください: 1. 試合概要 2. 失点原因 3. 改善点 4. 優先練習メニュー。"
    };
    Ok(format!(
        "あなたはRocket Leagueの戦術コーチです。以下のJSONだけを根拠に、試合の分析を日本語で返してください。\n\
         {data_scope}\n\
         {target_scope}\n\
         {culprit_scope}\n\
         raw replay全体は含まれていません。統計、失点前10秒の圧縮時系列、既存診断ヒントを優先してください。\n\
         availability.network_frames が true の場合、圧縮時系列から読み取れる範囲で動きやポジショニングを分析してください。\n\
         {output_headings}\n\n\
         JSON:\n{payload_json}"
    ))
}

fn gemini_response_text(response: &GeminiGenerateResponse) -> Option<String> {
    let parts: Vec<_> = response
        .candidates
        .iter()
        .filter_map(|candidate| candidate.content.as_ref())
        .flat_map(|content| content.parts.iter())
        .filter_map(|part| part.text.as_deref())
        .filter(|text| !text.trim().is_empty())
        .collect();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn gemini_finish_reasons(response: &GeminiGenerateResponse) -> Vec<String> {
    response
        .candidates
        .iter()
        .filter_map(|candidate| candidate.finish_reason.clone())
        .collect()
}

fn build_gemini_report_html(
    model: &str,
    replay_id: &str,
    date: &str,
    response_text: &str,
) -> String {
    let title = format!("Gemini Report - {replay_id}");
    format!(
        "<!doctype html>\n\
         <html lang=\"ja\">\n\
         <head>\n\
         <meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{title}</title>\n\
         <style>\n\
         :root {{ color-scheme: light dark; }}\n\
         body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; margin: 0; line-height: 1.6; }}\n\
         main {{ max-width: 960px; margin: 0 auto; padding: 32px 20px 48px; }}\n\
         header {{ border-bottom: 1px solid #d0d7de; margin-bottom: 24px; padding-bottom: 16px; }}\n\
         h1 {{ font-size: 1.8rem; margin: 0 0 8px; }}\n\
         dl {{ display: grid; grid-template-columns: max-content 1fr; gap: 6px 16px; margin: 0; }}\n\
         dt {{ font-weight: 700; }}\n\
         dd {{ margin: 0; }}\n\
         article {{ white-space: pre-wrap; overflow-wrap: anywhere; }}\n\
         </style>\n\
         </head>\n\
         <body>\n\
         <main>\n\
         <header>\n\
         <h1>{title}</h1>\n\
         <dl>\n\
         <dt>Replay</dt><dd>{replay_id}</dd>\n\
         <dt>Date</dt><dd>{date}</dd>\n\
         <dt>Model</dt><dd>{model}</dd>\n\
         </dl>\n\
         </header>\n\
         <article>{response_text}</article>\n\
         </main>\n\
         </body>\n\
         </html>\n",
        title = html_escape(&title),
        replay_id = html_escape(replay_id),
        date = html_escape(date),
        model = html_escape(model),
        response_text = html_escape(response_text),
    )
}

fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[derive(Debug)]
struct ScanSummary {
    converted: usize,
    skipped: usize,
    failed: usize,
}

fn scan_and_convert_replays(
    input_dir: &Path,
    output_dir: &Path,
    pretty_json: bool,
    processed: &mut HashSet<PathBuf>,
    tx: &mpsc::Sender<WorkerEvent>,
    cancel: &AtomicBool,
) -> Result<ScanSummary> {
    let replay_files = discover_replay_files(input_dir)?;
    let mut converted = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    for replay_path in replay_files {
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        if processed.contains(&replay_path) {
            continue;
        }

        match convert_replay_file(&replay_path, input_dir, output_dir, pretty_json) {
            Ok(ConvertResult::Converted {
                path,
                network_parse_error,
            }) => {
                converted += 1;
                processed.insert(replay_path);
                emit_log(tx, format!("converted {}", path.display()));
                if let Some(err) = network_parse_error {
                    emit_log(
                        tx,
                        format!(
                            "warning: network parse failed; wrote header-only fallback JSON ({err})"
                        ),
                    );
                }
            }
            Ok(ConvertResult::AlreadyExists(path)) => {
                skipped += 1;
                processed.insert(replay_path);
                emit_log(tx, format!("skip existing {}", path.display()));
            }
            Err(err) => {
                failed += 1;
                emit_log(tx, format!("failed {} ({err})", replay_path.display()));
            }
        }
    }

    Ok(ScanSummary {
        converted,
        skipped,
        failed,
    })
}

#[derive(Debug)]
enum ConvertResult {
    Converted {
        path: PathBuf,
        network_parse_error: Option<String>,
    },
    AlreadyExists(PathBuf),
}

fn convert_replay_file(
    replay_path: &Path,
    input_dir: &Path,
    output_dir: &Path,
    pretty_json: bool,
) -> Result<ConvertResult> {
    let output_filename = json_filename_from_replay_path(replay_path)?;
    let replay_id = replay_id_from_replay_path(replay_path)?;
    let date_segment = resolve_date_segment(replay_path, input_dir)?;
    let output_path = output_dir
        .join("json")
        .join(&date_segment)
        .join(output_filename);
    let timeline_path = output_dir
        .join("timeline")
        .join(date_segment)
        .join(format!("{replay_id}.positions.json"));

    if output_path.exists() && timeline_path.exists() {
        return Ok(ConvertResult::AlreadyExists(output_path));
    }

    let mut network_parse_error = None;
    if !output_path.exists() {
        let data = fs::read(replay_path)
            .with_context(|| format!("failed to read replay file {}", replay_path.display()))?;
        let parsed = parse_replay(&data)
            .with_context(|| format!("failed to parse replay file {}", replay_path.display()))?;

        let json_bytes = serialize_replay_json(&parsed, pretty_json)
            .context("failed to serialize replay to JSON")?;
        network_parse_error = parsed.network_parse_error.clone();

        let parent = output_path.parent().ok_or_else(|| {
            anyhow!(
                "failed to resolve output directory for {}",
                output_path.display()
            )
        })?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
        fs::write(&output_path, json_bytes)
            .with_context(|| format!("failed to write json file {}", output_path.display()))?;
    }

    if !timeline_path.exists() {
        write_position_timeline_file(&output_path, &timeline_path, pretty_json).with_context(
            || {
                format!(
                    "failed to write position timeline {}",
                    timeline_path.display()
                )
            },
        )?;
    }

    Ok(ConvertResult::Converted {
        path: output_path,
        network_parse_error,
    })
}

fn discover_replay_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_replay_files(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_replay_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let entries = fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?;
    for entry in entries {
        let entry = entry
            .with_context(|| format!("failed to read directory entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type for {}", path.display()))?;

        if file_type.is_dir() {
            collect_replay_files(&path, files)?;
        } else if file_type.is_file() && has_replay_extension(&path) {
            files.push(path);
        }
    }

    Ok(())
}

fn has_replay_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("replay"))
        .unwrap_or(false)
}

fn json_filename_from_replay_path(path: &Path) -> Result<String> {
    Ok(format!("{}.json", replay_id_from_replay_path(path)?))
}

fn replay_id_from_replay_path(path: &Path) -> Result<String> {
    let stem = path
        .file_stem()
        .and_then(|x| x.to_str())
        .ok_or_else(|| anyhow!("failed to derive replay filename from {}", path.display()))?;

    if stem.is_empty() {
        return Err(anyhow!("empty replay filename for {}", path.display()));
    }

    Ok(stem.to_string())
}

fn resolve_date_segment(replay_path: &Path, input_dir: &Path) -> Result<String> {
    if let Some(value) = date_from_ancestor_path(replay_path, input_dir) {
        return Ok(value);
    }

    let metadata = fs::metadata(replay_path)
        .with_context(|| format!("failed to read metadata for {}", replay_path.display()))?;
    let modified = metadata.modified().with_context(|| {
        format!(
            "failed to read modified timestamp for {}",
            replay_path.display()
        )
    })?;
    let modified: DateTime<Utc> = modified.into();
    Ok(modified.format("%Y-%m-%d").to_string())
}

fn date_from_ancestor_path(replay_path: &Path, input_dir: &Path) -> Option<String> {
    for ancestor in replay_path.ancestors() {
        if ancestor == input_dir {
            break;
        }

        let segment = ancestor.file_name()?.to_str()?;
        if is_ymd_segment(segment) {
            return Some(segment.to_string());
        }
    }

    None
}

fn is_ymd_segment(value: &str) -> bool {
    if value.len() != 10 {
        return false;
    }

    let bytes = value.as_bytes();
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, ch)| index == 4 || index == 7 || ch.is_ascii_digit())
}

#[derive(Debug)]
struct ParsedReplay {
    replay: boxcars::Replay,
    network_parse_error: Option<String>,
}

fn parse_replay(data: &[u8]) -> Result<ParsedReplay> {
    match ParserBuilder::new(data)
        .with_network_parse(NetworkParse::Always)
        .on_error_check_crc()
        .parse()
    {
        Ok(replay) => Ok(ParsedReplay {
            replay,
            network_parse_error: None,
        }),
        Err(network_err) => ParserBuilder::new(data)
            .with_network_parse(NetworkParse::Never)
            .on_error_check_crc()
            .parse()
            .map(|replay| ParsedReplay {
                replay,
                network_parse_error: Some(network_err.to_string()),
            })
            .with_context(|| {
                format!(
                    "network parse failed then fallback parse failed; first error: {network_err}"
                )
            }),
    }
}

fn serialize_replay_json(parsed: &ParsedReplay, pretty: bool) -> Result<Vec<u8>> {
    let mut value =
        serde_json::to_value(&parsed.replay).context("failed to serialize replay to JSON value")?;
    if let Some(err) = &parsed.network_parse_error
        && let Some(obj) = value.as_object_mut()
    {
        obj.insert(
            "_rl_toolkit".to_string(),
            json!({
                "network_parse_fallback": true,
                "network_parse_error": err
            }),
        );
    }

    if pretty {
        serde_json::to_vec_pretty(&value).context("failed to serialize replay JSON")
    } else {
        serde_json::to_vec(&value).context("failed to serialize replay JSON")
    }
}

#[derive(Debug, Deserialize)]
struct ReplayListResponse {
    list: Vec<ReplaySummary>,
    next: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReplaySummary {
    id: String,
    date: Option<String>,
    created: Option<String>,
}

fn fetch_replay_page(
    client: &Client,
    api_key: &str,
    player: &str,
    next_url: Option<&str>,
    request_interval: Duration,
) -> Result<ReplayListResponse> {
    let response = if let Some(url) = next_url {
        send_request(client.get(url), api_key, request_interval)?
    } else {
        send_request(
            client.get(format!("{API_BASE}/replays")).query(&[
                ("player-name", player),
                ("sort-by", "replay-date"),
                ("sort-dir", "desc"),
                ("count", "200"),
            ]),
            api_key,
            request_interval,
        )?
    };

    response
        .json::<ReplayListResponse>()
        .context("failed to decode replay list response JSON")
}

fn download_replay_file(
    client: &Client,
    api_key: &str,
    replay_id: &str,
    request_interval: Duration,
) -> Result<Vec<u8>> {
    let response = send_request(
        client.get(format!("{API_BASE}/replays/{replay_id}/file")),
        api_key,
        request_interval,
    )
    .with_context(|| format!("request failed for replay file {replay_id}"))?;

    response
        .bytes()
        .map(|bytes| bytes.to_vec())
        .with_context(|| format!("failed to read replay body for {replay_id}"))
}

fn send_request(
    request: reqwest::blocking::RequestBuilder,
    api_key: &str,
    request_interval: Duration,
) -> Result<Response> {
    let response = request
        .header(AUTHORIZATION, api_key)
        .send()
        .context("HTTP request failed")?;

    thread::sleep(request_interval);

    response
        .error_for_status()
        .context("unexpected HTTP status from ballchasing API")
}

fn replay_output_path(base_dir: &Path, player_slug: &str, replay: &ReplaySummary) -> PathBuf {
    let date_segment = replay_date_directory(replay);
    base_dir
        .join("replays")
        .join(player_slug)
        .join(date_segment)
        .join(format!("{}.replay", replay.id))
}

fn replay_date_directory(replay: &ReplaySummary) -> String {
    parse_date_ymd(replay.date.as_deref())
        .or_else(|| parse_date_ymd(replay.created.as_deref()))
        .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string())
}

fn parse_date_ymd(raw: Option<&str>) -> Option<String> {
    let value = raw?;

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.format("%Y-%m-%d").to_string());
    }

    let prefix = value.get(0..10)?;
    NaiveDate::parse_from_str(prefix, "%Y-%m-%d")
        .ok()
        .map(|d| d.format("%Y-%m-%d").to_string())
}

fn slugify_player_name(name: &str) -> String {
    let mut result = String::new();
    let mut in_separator = false;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            result.push(ch.to_ascii_lowercase());
            in_separator = false;
        } else if !in_separator {
            result.push('_');
            in_separator = true;
        }
    }

    let trimmed = result.trim_matches('_');
    if trimmed.is_empty() {
        "unknown_player".to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_next_url(next: Option<String>) -> Option<String> {
    match next {
        Some(value) if value.starts_with("http://") || value.starts_with("https://") => Some(value),
        Some(value) if value.starts_with('/') => Some(format!("https://ballchasing.com{value}")),
        Some(value) if !value.is_empty() => Some(format!("{API_BASE}/{value}")),
        _ => None,
    }
}

fn emit_log(tx: &mpsc::Sender<WorkerEvent>, message: impl Into<String>) {
    let _ = tx.send(WorkerEvent::Log(message.into()));
}

fn to_absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    Ok(cwd.join(path))
}

#[derive(Debug, Deserialize)]
struct LatestRelease {
    tag_name: String,
    html_url: Option<String>,
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug)]
struct UpdateCandidate {
    tag_name: String,
    version: Version,
    page_url: String,
    download_url: String,
}

#[derive(Debug, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
}

fn run_gui_update_flow(binary_name: &str, language: Language) -> Vec<String> {
    let mut logs = Vec::new();
    if !update_check_enabled() {
        return logs;
    }

    match find_update_candidate(binary_name) {
        Ok(Some(candidate)) => {
            logs.push(format!(
                "[update] {binary_name}: {} {} -> {} ({})",
                tr_for_language(language, "update available", "更新があります"),
                env!("CARGO_PKG_VERSION"),
                candidate.version,
                candidate.page_url
            ));

            let answer = MessageDialog::new()
                .set_title(tr_for_language(
                    language,
                    "Update Available",
                    "アップデートがあります",
                ))
                .set_description(format!(
                    "{}\n{}",
                    tr_for_language(
                        language,
                        &format!("{binary_name} {} is available.", candidate.tag_name),
                        &format!("{binary_name} {} が利用可能です。", candidate.tag_name),
                    ),
                    tr_for_language(language, "Install now?", "今すぐ更新しますか？"),
                ))
                .set_level(MessageLevel::Info)
                .set_buttons(MessageButtons::YesNo)
                .show();

            if answer == MessageDialogResult::Yes {
                match download_and_replace_executable(binary_name, &candidate.download_url) {
                    Ok(_message) => {
                        let message = tr_for_language(
                            language,
                            "Update started. The app is replacing executable and restarting.",
                            "更新を開始しました。実行ファイルを置き換えて再起動します。",
                        );
                        logs.push(format!("[update] {binary_name}: {message}"));
                        let _ = MessageDialog::new()
                            .set_title(tr_for_language(language, "Update", "更新"))
                            .set_description(tr_for_language(
                                language,
                                "Update started. The app is replacing executable and restarting.",
                                "更新を開始しました。実行ファイルを置き換えて再起動します。",
                            ))
                            .set_level(MessageLevel::Info)
                            .set_buttons(MessageButtons::Ok)
                            .show();
                        process::exit(0);
                    }
                    Err(err) => {
                        let message = format!(
                            "[update] {binary_name}: {}: {err}",
                            tr_for_language(language, "update failed", "更新失敗")
                        );
                        logs.push(message.clone());
                        let _ = MessageDialog::new()
                            .set_title(tr_for_language(language, "Update Failed", "更新失敗"))
                            .set_description(message)
                            .set_level(MessageLevel::Error)
                            .set_buttons(MessageButtons::Ok)
                            .show();
                    }
                }
            } else {
                logs.push(format!(
                    "[update] {binary_name}: {}",
                    tr_for_language(
                        language,
                        "skipped by user",
                        "ユーザーが更新をスキップしました"
                    )
                ));
            }
        }
        Ok(None) => logs.push(format!(
            "[update] {binary_name}: {} ({} v{})",
            tr_for_language(language, "up to date", "最新です"),
            tr_for_language(language, "current", "現在"),
            env!("CARGO_PKG_VERSION")
        )),
        Err(err) => logs.push(format!(
            "[update] {binary_name}: {}: {err}",
            tr_for_language(language, "check failed", "確認に失敗しました")
        )),
    }

    logs
}

fn tr_for_language<'a>(language: Language, en: &'a str, ja: &'a str) -> &'a str {
    match language {
        Language::English => en,
        Language::Japanese => ja,
    }
}

fn update_check_enabled() -> bool {
    match env::var(UPDATE_CHECK_ENV) {
        Ok(value) => !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "off"),
        Err(_) => true,
    }
}

fn find_update_candidate(binary_name: &str) -> Result<Option<UpdateCandidate>> {
    let release_api_url =
        env::var(RELEASE_API_URL_ENV).unwrap_or_else(|_| DEFAULT_RELEASE_API_URL.to_string());
    let current_version = Version::parse(env!("CARGO_PKG_VERSION"))
        .with_context(|| format!("invalid current version: {}", env!("CARGO_PKG_VERSION")))?;

    let client = Client::builder()
        .timeout(Duration::from_secs(UPDATE_TIMEOUT_SECONDS))
        .user_agent(format!("{binary_name}/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build update-check HTTP client")?;

    let response = github_api_get(&client, &release_api_url)
        .send()
        .context("failed to request latest release")?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let status = response.status();
    let release = response
        .error_for_status()
        .with_context(|| format!("release API returned unexpected status: {status}"))?
        .json::<LatestRelease>()
        .context("failed to decode latest release response")?;

    let latest_version = parse_version_tag(&release.tag_name)
        .ok_or_else(|| anyhow!("invalid release tag format: {}", release.tag_name))?;
    if latest_version <= current_version {
        return Ok(None);
    }

    let expected_asset = expected_asset_name(binary_name);
    let asset = release
        .assets
        .into_iter()
        .find(|asset| asset.name == expected_asset)
        .ok_or_else(|| {
            anyhow!(
                "release {} does not contain asset {}",
                release.tag_name,
                expected_asset
            )
        })?;

    Ok(Some(UpdateCandidate {
        tag_name: release.tag_name,
        version: latest_version,
        page_url: release.html_url.unwrap_or(release_api_url),
        download_url: asset.browser_download_url,
    }))
}

fn parse_version_tag(raw: &str) -> Option<Version> {
    let trimmed = raw.trim_start_matches(['v', 'V']);
    Version::parse(trimmed).ok()
}

#[cfg(target_os = "windows")]
fn expected_asset_name(binary_name: &str) -> String {
    format!("{binary_name}.exe")
}

#[cfg(not(target_os = "windows"))]
fn expected_asset_name(binary_name: &str) -> String {
    binary_name.to_string()
}

fn download_and_replace_executable(binary_name: &str, download_url: &str) -> Result<String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(UPDATE_TIMEOUT_SECONDS))
        .user_agent(format!("{binary_name}/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build updater HTTP client")?;

    let response = github_download_get(&client, download_url)
        .send()
        .context("failed to download update asset")?;
    let status = response.status();
    let bytes = response
        .error_for_status()
        .with_context(|| format!("asset download failed with status: {status}"))?
        .bytes()
        .context("failed to read downloaded bytes")?;

    let current_exe = env::current_exe().context("failed to resolve current executable path")?;
    let staged_path = staged_update_path(&current_exe);
    if staged_path.exists() {
        let _ = fs::remove_file(&staged_path);
    }

    fs::write(&staged_path, &bytes)
        .with_context(|| format!("failed to write staged update {}", staged_path.display()))?;
    set_executable_permission(&staged_path)?;

    replace_executable(&current_exe, &staged_path)
}

fn staged_update_path(current_exe: &Path) -> PathBuf {
    let mut path = current_exe.as_os_str().to_owned();
    path.push(".new");
    PathBuf::from(path)
}

#[cfg(unix)]
fn set_executable_permission(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms)
        .with_context(|| format!("failed to set executable permission on {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable_permission(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn replace_executable(current_exe: &Path, staged_path: &Path) -> Result<String> {
    fs::rename(staged_path, current_exe).with_context(|| {
        format!(
            "failed to replace executable {} with {}",
            current_exe.display(),
            staged_path.display()
        )
    })?;
    launch_new_process(current_exe).with_context(|| {
        format!(
            "failed to relaunch updated executable {}",
            current_exe.display()
        )
    })?;
    Ok(format!(
        "updated successfully. relaunching now ({})",
        current_exe.display()
    ))
}

#[cfg(target_os = "windows")]
fn replace_executable(current_exe: &Path, staged_path: &Path) -> Result<String> {
    spawn_windows_replacer(current_exe, staged_path, true)?;

    Ok("update staged. relaunching now after replacement".to_string())
}

#[cfg(target_os = "windows")]
fn spawn_windows_replacer(current_exe: &Path, staged_path: &Path, relaunch: bool) -> Result<()> {
    let current_literal = powershell_single_quoted(current_exe);
    let staged_literal = powershell_single_quoted(staged_path);
    let working_dir = current_exe.parent().unwrap_or(Path::new("."));
    let working_literal = powershell_single_quoted(working_dir);
    let relaunch_command = if relaunch {
        "Start-Process -FilePath $current -WorkingDirectory $working;"
    } else {
        ""
    };
    let script = format!(
        concat!(
            "$ErrorActionPreference='Stop';",
            "$current={current};",
            "$staged={staged};",
            "$working={working};",
            "for($i=0; $i -lt 120; $i++) {{",
            "  try {{",
            "    Move-Item -LiteralPath $staged -Destination $current -Force;",
            "    {relaunch}",
            "    exit 0",
            "  }} catch {{",
            "    Start-Sleep -Milliseconds 500",
            "  }}",
            "}}",
            "exit 1"
        ),
        current = current_literal,
        staged = staged_literal,
        working = working_literal,
        relaunch = relaunch_command
    );

    Command::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-NonInteractive")
        .arg("-WindowStyle")
        .arg("Hidden")
        .arg("-Command")
        .arg(script)
        .spawn()
        .with_context(|| {
            format!(
                "failed to launch Windows updater for {}",
                current_exe.display()
            )
        })?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn powershell_single_quoted(path: &Path) -> String {
    let escaped = path.as_os_str().to_string_lossy().replace('\'', "''");
    format!("'{escaped}'")
}

#[cfg(not(target_os = "windows"))]
fn launch_new_process(executable: &Path) -> Result<()> {
    Command::new(executable).spawn().with_context(|| {
        format!(
            "failed to launch updated executable {}",
            executable.display()
        )
    })?;
    Ok(())
}

fn github_api_get(client: &Client, url: &str) -> reqwest::blocking::RequestBuilder {
    let request = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json");
    apply_github_auth(request)
}

fn github_download_get(client: &Client, url: &str) -> reqwest::blocking::RequestBuilder {
    apply_github_auth(client.get(url))
}

fn apply_github_auth(
    mut request: reqwest::blocking::RequestBuilder,
) -> reqwest::blocking::RequestBuilder {
    if let Ok(token) = env::var(UPDATE_GITHUB_TOKEN_ENV).or_else(|_| env::var(GITHUB_TOKEN_ENV)) {
        let token = token.trim();
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
    }

    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn coach_fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../rl-coach/tests/fixtures")
            .join(name)
    }

    #[test]
    fn saved_gui_settings_round_trip_preserves_coach() {
        let settings = SavedGuiSettings {
            harvester: HarvesterSettings::default(),
            replay2json: Replay2JsonSettings::default(),
            coach: CoachSettings {
                input_mode: CoachInputMode::File,
                input_path: "/tmp/input.json".to_string(),
                output_dir: "/tmp/output".to_string(),
                pretty_json: true,
            },
            gemini: GeminiSettings {
                api_key: "secret-key".to_string(),
                input_path: "/tmp/replay.json".to_string(),
                output_dir: "/tmp/gemini".to_string(),
                model: "gemini-2.5-flash".to_string(),
                selected_player_name: "BlueOne".to_string(),
                selected_player_team: Some(0),
                selected_player_unique_id: Some("blue-one-id".to_string()),
                culprit_investigation: true,
            },
        };

        let raw = serde_json::to_string(&settings).expect("serialize settings");
        let decoded: SavedGuiSettings = serde_json::from_str(&raw).expect("deserialize settings");

        assert_eq!(decoded.coach.input_mode, CoachInputMode::File);
        assert_eq!(decoded.coach.input_path, "/tmp/input.json");
        assert_eq!(decoded.coach.output_dir, "/tmp/output");
        assert!(decoded.coach.pretty_json);
        assert_eq!(decoded.gemini.api_key, "secret-key");
        assert_eq!(decoded.gemini.model, "gemini-2.5-flash");
        assert_eq!(decoded.gemini.selected_player_name, "BlueOne");
        assert_eq!(decoded.gemini.selected_player_team, Some(0));
        assert_eq!(
            decoded.gemini.selected_player_unique_id.as_deref(),
            Some("blue-one-id")
        );
        assert!(decoded.gemini.culprit_investigation);
    }

    #[test]
    fn running_task_maps_from_task_kind() {
        assert_eq!(
            RunningTask::from_task(&TaskKind::Coach(CoachSettings::default())),
            RunningTask::Coach
        );
        assert_eq!(
            RunningTask::from_task(&TaskKind::Gemini(GeminiSettings::default())),
            RunningTask::Gemini
        );
    }

    #[test]
    fn gemini_request_contains_prompt_payload_without_api_key() {
        let players = rl_coach::list_replay_players(&coach_fixture("full_soccar.json"))
            .expect("replay players");
        let target = players.first().expect("target player");
        let payload = rl_coach::build_gemini_match_payload_for_player(
            &coach_fixture("full_soccar.json"),
            Some(target),
        )
        .expect("gemini payload");
        let request = build_gemini_request(&payload, false).expect("gemini request");
        let value = serde_json::to_value(&request).expect("request json");
        let text = value["contents"][0]["parts"][0]["text"]
            .as_str()
            .expect("prompt text");
        let max_output_tokens = value["generationConfig"]["maxOutputTokens"]
            .as_u64()
            .expect("max output tokens");

        assert!(text.contains("Rocket League"));
        assert!(text.contains("concede_windows"));
        assert!(text.contains("network frame は利用可能"));
        assert!(text.contains("network frame データが利用できない"));
        assert!(text.contains(&target.player_name));
        assert!(text.contains("主対象"));
        assert!(text.contains("戦犯調査: 実施しない"));
        assert_eq!(max_output_tokens, GEMINI_MAX_OUTPUT_TOKENS as u64);
        assert!(!text.contains("secret-key"));
    }

    #[test]
    fn gemini_prompt_can_request_culprit_investigation() {
        let players = rl_coach::list_replay_players(&coach_fixture("full_soccar.json"))
            .expect("replay players");
        let target = players.first().expect("target player");
        let payload = rl_coach::build_gemini_match_payload_for_player(
            &coach_fixture("full_soccar.json"),
            Some(target),
        )
        .expect("gemini payload");
        let request = build_gemini_request(&payload, true).expect("gemini request");
        let value = serde_json::to_value(&request).expect("request json");
        let text = value["contents"][0]["parts"][0]["text"]
            .as_str()
            .expect("prompt text");

        assert!(text.contains("戦犯調査: 実施してください"));
        assert!(text.contains("負けた主因となった選手"));
        assert!(text.contains("ローテーションの乱れ"));
        assert!(text.contains("空振り"));
        assert!(text.contains("5. 戦犯調査"));
    }

    #[test]
    fn gemini_prompt_marks_header_only_frame_data_unavailable() {
        let payload =
            rl_coach::build_gemini_match_payload(&coach_fixture("header_only_soccar.json"))
                .expect("gemini payload");
        let request = build_gemini_request(&payload, false).expect("gemini request");
        let value = serde_json::to_value(&request).expect("request json");
        let text = value["contents"][0]["parts"][0]["text"]
            .as_str()
            .expect("prompt text");

        assert!(text.contains("network frame は利用できません"));
        assert!(text.contains("詳細な動きやポジショニング"));
    }

    #[test]
    fn gemini_response_text_extracts_candidate_parts() {
        let response: GeminiGenerateResponse = serde_json::from_str(
            r#"{
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                { "text": "原因分析" },
                                { "text": "改善点" }
                            ]
                        }
                    }
                ],
                "usageMetadata": { "totalTokenCount": 42 }
            }"#,
        )
        .expect("response json");

        assert_eq!(
            gemini_response_text(&response).as_deref(),
            Some("原因分析\n改善点")
        );
        assert!(response.usage_metadata.is_some());
    }

    #[test]
    fn gemini_finish_reasons_extracts_max_tokens() {
        let response: GeminiGenerateResponse = serde_json::from_str(
            r#"{
                "candidates": [
                    {
                        "finishReason": "MAX_TOKENS",
                        "content": {
                            "parts": [
                                { "text": "途中までの分析" }
                            ]
                        }
                    }
                ]
            }"#,
        )
        .expect("response json");

        assert_eq!(gemini_finish_reasons(&response), vec!["MAX_TOKENS"]);
        assert_eq!(
            gemini_response_text(&response).as_deref(),
            Some("途中までの分析")
        );
    }

    #[test]
    fn gemini_report_html_escapes_response_text() {
        let html = build_gemini_report_html(
            "gemini-2.5-flash",
            "abc123",
            "2026-03-09",
            "改善点\n<script>alert('x')</script>",
        );

        assert!(html.contains("<!doctype html>"));
        assert!(html.contains("Gemini Report - abc123"));
        assert!(html.contains("gemini-2.5-flash"));
        assert!(html.contains("&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"));
        assert!(!html.contains("<script>alert"));
    }

    #[test]
    fn diagnosis_label_is_localized_for_japanese() {
        assert_eq!(
            localized_diagnosis_label(CoachDiagnosisLabel::DemoDisruption, Language::Japanese),
            "デモ妨害"
        );
    }

    #[test]
    fn diagnosis_context_is_localized_for_japanese() {
        assert_eq!(
            localized_diagnosis_context(
                "multiple defenders collapsed on the same ball in the defensive half",
                Language::Japanese
            ),
            "自陣で複数人が同じボールに寄り過ぎた"
        );
    }

    #[test]
    fn metric_note_is_localized_for_japanese() {
        assert_eq!(
            localized_metric_note("derived from network frame sampling", Language::Japanese),
            "network frame のサンプリングから算出"
        );
    }

    #[test]
    fn metric_value_display_is_localized_for_japanese() {
        let value = CoachMetricValue {
            value: Some(12.5),
            quality: rl_coach::MetricQuality::Estimated,
            note: Some("derived from network frame sampling".to_string()),
        };

        assert_eq!(
            format_metric_value(&value, Language::Japanese),
            "12.50 [推定] - network frame のサンプリングから算出"
        );
    }

    #[test]
    fn load_coach_view_reads_generated_reports() {
        let input_dir = tempdir().expect("input tempdir");
        let output_dir = tempdir().expect("output tempdir");
        fs::copy(
            coach_fixture("full_soccar.json"),
            input_dir.path().join("full_soccar.json"),
        )
        .expect("copy fixture");

        rl_coach::analyze_path(input_dir.path(), output_dir.path(), true).expect("analyze path");
        let summary =
            load_coach_view_from_output_dir(output_dir.path()).expect("load coach summary");

        assert_eq!(summary.matches.len(), 1);
        assert_eq!(summary.loaded_reports.len(), 1);
        assert_eq!(summary.loaded_reports[0].meta.replay_id, "full_soccar");
    }
}
