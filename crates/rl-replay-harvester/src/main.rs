use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, NaiveDate, Utc};
use clap::Parser;
use reqwest::blocking::{Client, Response};
use reqwest::header::AUTHORIZATION;
use semver::Version;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
#[cfg(target_os = "windows")]
use std::process::Command;
use std::thread;
use std::time::Duration;

const API_BASE: &str = "https://ballchasing.com/api";
const API_KEY_ENV: &str = "BALLCHASING_API_KEY";
const REQUEST_INTERVAL_SECONDS_ENV: &str = "BALLCHASING_REQUEST_INTERVAL_SECONDS";
const LEGACY_REQUEST_SLEEP_MS_ENV: &str = "BALLCHASING_REQUEST_SLEEP_MS";
const MIN_REQUEST_INTERVAL_SECONDS: u64 = 2;
const DEFAULT_REQUEST_INTERVAL_SECONDS: u64 = 2;
const UPDATE_CHECK_ENV: &str = "RL_TOOLKIT_UPDATE_CHECK";
const RELEASE_API_URL_ENV: &str = "RL_TOOLKIT_RELEASE_API_URL";
const DEFAULT_RELEASE_API_URL: &str =
    "https://api.github.com/repos/k-zshiba/rl-toolkit/releases/latest";
const UPDATE_GITHUB_TOKEN_ENV: &str = "RL_TOOLKIT_GITHUB_TOKEN";
const GITHUB_TOKEN_ENV: &str = "GITHUB_TOKEN";
const UPDATE_TIMEOUT_SECONDS: u64 = 5;

#[derive(Debug, Parser)]
#[command(name = "rl-replay-harvester")]
#[command(
    about = "Download Rocket League replays for a player from ballchasing.com",
    version
)]
struct Args {
    #[arg(help = "Pro player name to query on ballchasing")]
    player: String,
    #[arg(
        short = 'o',
        long = "output-dir",
        value_name = "DIR",
        help = "Base directory where replay files are stored"
    )]
    output_dir: PathBuf,
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

fn main() -> Result<()> {
    maybe_self_update_cli("rl-replay-harvester");

    let args = Args::parse();
    let api_key = env::var(API_KEY_ENV)
        .with_context(|| format!("{API_KEY_ENV} environment variable is required"))?;
    let player_slug = slugify_player_name(&args.player);
    let request_interval = request_interval_duration();
    let client = build_client()?;

    let mut next_url: Option<String> = None;
    let mut seen_ids = HashSet::new();
    let mut downloaded = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let mut page_number = 0usize;

    loop {
        page_number += 1;
        let response = fetch_replay_page(
            &client,
            &api_key,
            &args.player,
            next_url.as_deref(),
            request_interval,
        )
        .with_context(|| format!("failed to fetch replay list page {page_number}"))?;

        for replay in response.list {
            if !seen_ids.insert(replay.id.clone()) {
                continue;
            }

            let target_path = replay_output_path(&args.output_dir, &player_slug, &replay);
            if target_path.exists() {
                skipped += 1;
                continue;
            }

            let Some(parent) = target_path.parent() else {
                failed += 1;
                eprintln!(
                    "failed to resolve parent directory for replay {}",
                    replay.id
                );
                continue;
            };

            if let Err(err) = fs::create_dir_all(parent) {
                failed += 1;
                eprintln!(
                    "failed to create directory for replay {} at {}: {err}",
                    replay.id,
                    parent.display()
                );
                continue;
            }

            match download_replay_file(&client, &api_key, &replay.id, request_interval) {
                Ok(payload) => {
                    if let Err(err) = fs::write(&target_path, payload) {
                        failed += 1;
                        eprintln!(
                            "failed to save replay {} at {}: {err}",
                            replay.id,
                            target_path.display()
                        );
                    } else {
                        downloaded += 1;
                        println!("{}", target_path.display());
                    }
                }
                Err(err) => {
                    failed += 1;
                    eprintln!("failed to download replay {}: {err}", replay.id);
                }
            }
        }

        next_url = normalize_next_url(response.next);
        if next_url.is_none() {
            break;
        }
    }

    eprintln!(
        "completed: downloaded={downloaded}, skipped_existing={skipped}, failed={failed}, player={player}, path={path}",
        player = args.player,
        path = args.output_dir.display()
    );

    Ok(())
}

fn build_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent("rl-replay-harvester/0.1.0")
        .build()
        .context("failed to build HTTP client")
}

fn fetch_replay_page(
    client: &Client,
    api_key: &str,
    player: &str,
    next_url: Option<&str>,
    request_interval: Duration,
) -> Result<ReplayListResponse> {
    let response = if let Some(url) = next_url {
        send(client.get(url), api_key, request_interval)?
    } else {
        send(
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
    let response = send(
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

fn send(
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

fn request_interval_duration() -> Duration {
    match env::var(REQUEST_INTERVAL_SECONDS_ENV) {
        Ok(value) => parse_request_interval_seconds(&value, REQUEST_INTERVAL_SECONDS_ENV),
        Err(_) => match env::var(LEGACY_REQUEST_SLEEP_MS_ENV) {
            Ok(value) => parse_legacy_sleep_ms(&value),
            Err(_) => Duration::from_secs(DEFAULT_REQUEST_INTERVAL_SECONDS),
        },
    }
}

fn parse_request_interval_seconds(raw: &str, env_name: &str) -> Duration {
    match raw.parse::<u64>() {
        Ok(seconds) if seconds >= MIN_REQUEST_INTERVAL_SECONDS => Duration::from_secs(seconds),
        Ok(seconds) => {
            eprintln!(
                "{env_name}={seconds} is too small, using minimum {MIN_REQUEST_INTERVAL_SECONDS}s"
            );
            Duration::from_secs(MIN_REQUEST_INTERVAL_SECONDS)
        }
        Err(_) => {
            eprintln!(
                "invalid {env_name}='{raw}', fallback to {DEFAULT_REQUEST_INTERVAL_SECONDS}s"
            );
            Duration::from_secs(DEFAULT_REQUEST_INTERVAL_SECONDS)
        }
    }
}

fn parse_legacy_sleep_ms(raw: &str) -> Duration {
    match raw.parse::<u64>() {
        Ok(ms) => {
            let seconds = ms.div_ceil(1000);
            let clamped = seconds.max(MIN_REQUEST_INTERVAL_SECONDS);
            eprintln!(
                "{LEGACY_REQUEST_SLEEP_MS_ENV} is deprecated; use {REQUEST_INTERVAL_SECONDS_ENV}. resolved interval={clamped}s"
            );
            Duration::from_secs(clamped)
        }
        Err(_) => {
            eprintln!(
                "invalid {LEGACY_REQUEST_SLEEP_MS_ENV}='{raw}', fallback to {DEFAULT_REQUEST_INTERVAL_SECONDS}s"
            );
            Duration::from_secs(DEFAULT_REQUEST_INTERVAL_SECONDS)
        }
    }
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

fn maybe_self_update_cli(binary_name: &str) {
    if !update_check_enabled() {
        return;
    }

    match find_update_candidate(binary_name) {
        Ok(Some(candidate)) => {
            eprintln!(
                "[update] {binary_name}: update available {} -> {} ({})",
                env!("CARGO_PKG_VERSION"),
                candidate.version,
                candidate.page_url
            );

            if confirm_update_from_stdin(binary_name, &candidate.tag_name) {
                match download_and_replace_executable(binary_name, &candidate.download_url) {
                    Ok(message) => eprintln!("[update] {binary_name}: {message}"),
                    Err(err) => eprintln!("[update] {binary_name}: update failed: {err}"),
                }
            } else {
                eprintln!("[update] {binary_name}: skipped by user");
            }
        }
        Ok(None) => eprintln!(
            "[update] {binary_name}: up to date (current v{})",
            env!("CARGO_PKG_VERSION")
        ),
        Err(err) => eprintln!("[update] {binary_name}: check failed: {err}"),
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

fn confirm_update_from_stdin(binary_name: &str, tag_name: &str) -> bool {
    eprint!("[update] {binary_name}: install {tag_name} now? [y/N]: ");
    let _ = io::stderr().flush();

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_err() {
        return false;
    }

    matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes")
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

    write_cmd_script_utf16le(&script_path, &script)?;

    Command::new("cmd")
        .arg("/C")
        .arg(&script_path)
        .spawn()
        .with_context(|| format!("failed to launch updater script {}", script_path.display()))?;

    Ok("update staged. restart this app to complete replacement".to_string())
}

#[cfg(target_os = "windows")]
fn write_cmd_script_utf16le(path: &Path, script: &str) -> Result<()> {
    let mut bytes = Vec::with_capacity(2 + script.len() * 2);
    bytes.extend_from_slice(&[0xFF, 0xFE]);
    for unit in script.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }

    fs::write(path, bytes)
        .with_context(|| format!("failed to create updater script {}", path.display()))?;
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

    #[test]
    fn slugify_player_name_normalizes_for_filesystem() {
        let slug = slugify_player_name("Seikoo / Team BDS!");
        assert_eq!(slug, "seikoo_team_bds");
    }

    #[test]
    fn parse_date_ymd_handles_rfc3339() {
        let parsed = parse_date_ymd(Some("2026-02-28T23:59:59+00:00"));
        assert_eq!(parsed.as_deref(), Some("2026-02-28"));
    }

    #[test]
    fn replay_date_directory_falls_back_to_created() {
        let replay = ReplaySummary {
            id: "abc".to_string(),
            date: None,
            created: Some("2026-01-15T12:00:00+00:00".to_string()),
        };
        assert_eq!(replay_date_directory(&replay), "2026-01-15");
    }

    #[test]
    fn parse_version_tag_handles_v_prefix() {
        let version = parse_version_tag("v1.2.3").expect("version");
        assert_eq!(version, Version::new(1, 2, 3));
    }
}
