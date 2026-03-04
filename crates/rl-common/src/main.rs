use anyhow::{Context, Result, anyhow};
use boxcars::{NetworkParse, ParserBuilder};
use chrono::{DateTime, NaiveDate, Utc};
use eframe::egui;
use reqwest::blocking::{Client, Response};
use reqwest::header::AUTHORIZATION;
use rfd::{FileDialog, MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};
use semver::Version;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(target_os = "windows")]
use std::process::Command;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Language {
    English,
    Japanese,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug)]
enum TaskKind {
    Harvester(HarvesterSettings),
    Replay2Json(Replay2JsonSettings),
}

#[derive(Debug)]
enum WorkerEvent {
    Log(String),
    Finished(Result<(), String>),
}

struct RlGuiApp {
    language: Language,
    tab: Tab,
    harvester: HarvesterSettings,
    replay2json: Replay2JsonSettings,
    logs: Vec<String>,
    running: bool,
    worker_rx: Option<mpsc::Receiver<WorkerEvent>>,
    worker_cancel: Option<Arc<AtomicBool>>,
}

impl Default for RlGuiApp {
    fn default() -> Self {
        let language = detect_initial_language();
        let logs = run_gui_update_flow("rl-toolkit", language);

        Self {
            language,
            tab: Tab::Harvester,
            harvester: HarvesterSettings::default(),
            replay2json: Replay2JsonSettings::default(),
            logs,
            running: false,
            worker_rx: None,
            worker_cancel: None,
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

        self.logs.push(
            self.tr("starting task...", "タスクを開始しました...")
                .to_string(),
        );
        self.running = true;
        self.worker_cancel = Some(cancel);
        self.worker_rx = Some(rx);

        thread::spawn(move || {
            let result = match task {
                TaskKind::Harvester(settings) => {
                    run_harvester_task(settings, &tx, &cancel_for_worker)
                }
                TaskKind::Replay2Json(settings) => {
                    run_replay2json_task(settings, &tx, &cancel_for_worker)
                }
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

        if let Some(rx) = &self.worker_rx {
            while let Ok(event) = rx.try_recv() {
                match event {
                    WorkerEvent::Log(line) => self.logs.push(line),
                    WorkerEvent::Finished(result) => {
                        match result {
                            Ok(()) => self
                                .logs
                                .push(self.tr("task finished", "タスクが完了しました").to_string()),
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
        }
    }

    fn ui_header(&mut self, ui: &mut egui::Ui) {
        let tab_harvester = self.tr("Replay Harvester", "リプレイ収集");
        let tab_replay2json = self.tr("Replay2JSON", "リプレイJSON変換");
        let label_language = self.tr("Language", "言語");
        let button_stop = self.tr("Stop Task", "タスク停止");
        let label_idle = self.tr("Idle", "待機中");
        let previous_language = self.language;

        ui.horizontal(|ui| {
            ui.selectable_value(&mut self.tab, Tab::Harvester, tab_harvester);
            ui.selectable_value(&mut self.tab, Tab::Replay2Json, tab_replay2json);

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

        ui.heading(heading);
        ui.label(desc);

        ui.horizontal(|ui| {
            ui.label(label_api_key);
            ui.add(egui::TextEdit::singleline(&mut self.harvester.api_key).password(true));
        });
        ui.horizontal(|ui| {
            ui.label(label_player);
            ui.text_edit_singleline(&mut self.harvester.player);
        });
        ui_folder_field(
            ui,
            label_output_dir,
            &mut self.harvester.output_dir,
            label_browse,
        );
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
            ui.add(egui::DragValue::new(&mut self.harvester.max_pages).range(1..=100));
        });
        ui.horizontal(|ui| {
            ui.label(label_request_interval);
            ui.add(
                egui::DragValue::new(&mut self.harvester.request_interval_seconds)
                    .range(MIN_REQUEST_INTERVAL_SECONDS..=300),
            );
        });

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

        ui.heading(heading);
        ui.label(desc);

        ui_folder_field(
            ui,
            label_input_dir,
            &mut self.replay2json.input_dir,
            label_browse,
        );
        ui_folder_field(
            ui,
            label_output_dir,
            &mut self.replay2json.output_dir,
            label_browse,
        );
        ui.checkbox(&mut self.replay2json.watch_mode, label_watch);
        ui.horizontal(|ui| {
            ui.label(label_watch_interval);
            ui.add(
                egui::DragValue::new(&mut self.replay2json.watch_interval_seconds)
                    .range(MIN_WATCH_INTERVAL_SECONDS..=300),
            );
        });
        ui.checkbox(&mut self.replay2json.pretty_json, label_pretty);

        if ui
            .add_enabled(!self.running, egui::Button::new(button_start))
            .clicked()
        {
            self.start_task(TaskKind::Replay2Json(self.replay2json.clone()));
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

fn ui_folder_field(ui: &mut egui::Ui, label: &str, value: &mut String, browse_label: &str) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.add(egui::TextEdit::singleline(value).desired_width(420.0));
        if ui.button(browse_label).clicked() {
            let selected = if value.trim().is_empty() {
                FileDialog::new().pick_folder()
            } else {
                FileDialog::new()
                    .set_directory(value.as_str())
                    .pick_folder()
            };
            if let Some(path) = selected {
                *value = path.display().to_string();
            }
        }
    });
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
            Ok(ConvertResult::Converted(path)) => {
                converted += 1;
                processed.insert(replay_path);
                emit_log(tx, format!("converted {}", path.display()));
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
    Converted(PathBuf),
    AlreadyExists(PathBuf),
}

fn convert_replay_file(
    replay_path: &Path,
    input_dir: &Path,
    output_dir: &Path,
    pretty_json: bool,
) -> Result<ConvertResult> {
    let output_filename = json_filename_from_replay_path(replay_path)?;
    let date_segment = resolve_date_segment(replay_path, input_dir)?;
    let output_path = output_dir
        .join("json")
        .join(date_segment)
        .join(output_filename);

    if output_path.exists() {
        return Ok(ConvertResult::AlreadyExists(output_path));
    }

    let data = fs::read(replay_path)
        .with_context(|| format!("failed to read replay file {}", replay_path.display()))?;
    let replay = parse_replay(&data)
        .with_context(|| format!("failed to parse replay file {}", replay_path.display()))?;

    let json_bytes = if pretty_json {
        serde_json::to_vec_pretty(&replay).context("failed to serialize replay to JSON")?
    } else {
        serde_json::to_vec(&replay).context("failed to serialize replay to JSON")?
    };

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

    Ok(ConvertResult::Converted(output_path))
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
    let stem = path
        .file_stem()
        .and_then(|x| x.to_str())
        .ok_or_else(|| anyhow!("failed to derive replay filename from {}", path.display()))?;

    if stem.is_empty() {
        return Err(anyhow!("empty replay filename for {}", path.display()));
    }

    Ok(format!("{stem}.json"))
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

fn parse_replay(data: &[u8]) -> Result<boxcars::Replay> {
    match ParserBuilder::new(data)
        .with_network_parse(NetworkParse::Always)
        .on_error_check_crc()
        .parse()
    {
        Ok(replay) => Ok(replay),
        Err(network_err) => ParserBuilder::new(data)
            .with_network_parse(NetworkParse::Never)
            .on_error_check_crc()
            .parse()
            .with_context(|| {
                format!(
                    "network parse failed then fallback parse failed; first error: {network_err}"
                )
            }),
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
                    Ok(message) => {
                        logs.push(format!(
                            "[update] {binary_name}: {}",
                            tr_for_language(
                                language,
                                &message,
                                "更新を開始しました。完了後にアプリを再起動してください。",
                            )
                        ));
                        let _ = MessageDialog::new()
                            .set_title(tr_for_language(language, "Update", "更新"))
                            .set_description(tr_for_language(
                                language,
                                &message,
                                "更新を開始しました。完了後にアプリを再起動してください。",
                            ))
                            .set_level(MessageLevel::Info)
                            .set_buttons(MessageButtons::Ok)
                            .show();
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
    Ok(format!(
        "updated successfully. restart to use the new version ({})",
        current_exe.display()
    ))
}

#[cfg(target_os = "windows")]
fn replace_executable(current_exe: &Path, staged_path: &Path) -> Result<String> {
    let mut script_name = current_exe.as_os_str().to_owned();
    script_name.push(".update.cmd");
    let script_path = PathBuf::from(script_name);

    let script = format!(
        "@echo off\r\n:retry\r\nmove /Y \"{}\" \"{}\" >nul 2>nul\r\nif errorlevel 1 (\r\n  timeout /T 1 /NOBREAK >nul\r\n  goto retry\r\n)\r\ndel \"%~f0\"\r\n",
        staged_path.display(),
        current_exe.display()
    );

    fs::write(&script_path, script)
        .with_context(|| format!("failed to create updater script {}", script_path.display()))?;

    Command::new("cmd")
        .arg("/C")
        .arg(&script_path)
        .spawn()
        .with_context(|| format!("failed to launch updater script {}", script_path.display()))?;

    Ok("update staged. restart this app to complete replacement".to_string())
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
