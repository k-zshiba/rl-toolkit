use anyhow::{Context, Result, anyhow};
use boxcars::{NetworkParse, ParserBuilder};
use chrono::{DateTime, Utc};
use clap::Parser;
use reqwest::blocking::Client;
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

const POLL_INTERVAL: Duration = Duration::from_secs(10);
const UPDATE_CHECK_ENV: &str = "RL_TOOLKIT_UPDATE_CHECK";
const RELEASE_API_URL_ENV: &str = "RL_TOOLKIT_RELEASE_API_URL";
const DEFAULT_RELEASE_API_URL: &str =
    "https://api.github.com/repos/k-zshiba/rl-toolkit/releases/latest";
const UPDATE_GITHUB_TOKEN_ENV: &str = "RL_TOOLKIT_GITHUB_TOKEN";
const GITHUB_TOKEN_ENV: &str = "GITHUB_TOKEN";
const UPDATE_TIMEOUT_SECONDS: u64 = 5;

#[derive(Debug, Parser)]
#[command(name = "rl-replay2json")]
#[command(
    about = "Convert Rocket League .replay files to JSON with periodic directory polling",
    version
)]
struct Args {
    #[arg(
        short = 'i',
        long = "input-dir",
        value_name = "DIR",
        help = "Directory containing replay files to convert"
    )]
    input_dir: PathBuf,
    #[arg(
        short = 'o',
        long = "output-dir",
        value_name = "DIR",
        help = "Base directory where converted JSON files are stored"
    )]
    output_dir: PathBuf,
}

#[derive(Debug)]
enum ConvertStatus {
    Converted(PathBuf),
    AlreadyExists(PathBuf),
}

fn main() -> Result<()> {
    maybe_self_update_cli("rl-replay2json");

    let args = Args::parse();
    let input_dir = fs::canonicalize(&args.input_dir).with_context(|| {
        format!(
            "failed to access input directory: {}",
            args.input_dir.display()
        )
    })?;

    if !input_dir.is_dir() {
        return Err(anyhow!(
            "input path is not a directory: {}",
            input_dir.display()
        ));
    }

    fs::create_dir_all(&args.output_dir).with_context(|| {
        format!(
            "failed to create output directory: {}",
            args.output_dir.display()
        )
    })?;
    let output_dir = args.output_dir;

    eprintln!(
        "watching replay directory every {} seconds: {}",
        POLL_INTERVAL.as_secs(),
        input_dir.display()
    );

    let mut processed = HashSet::new();
    loop {
        run_scan(&input_dir, &output_dir, &mut processed)?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn run_scan(input_dir: &Path, output_dir: &Path, processed: &mut HashSet<PathBuf>) -> Result<()> {
    let replay_files = discover_replay_files(input_dir)?;
    let mut converted = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    for replay_path in replay_files {
        if processed.contains(&replay_path) {
            continue;
        }

        match convert_replay_file(&replay_path, input_dir, output_dir) {
            Ok(ConvertStatus::Converted(path)) => {
                converted += 1;
                processed.insert(replay_path);
                println!("{}", path.display());
            }
            Ok(ConvertStatus::AlreadyExists(path)) => {
                skipped += 1;
                processed.insert(replay_path);
                eprintln!("skip existing: {}", path.display());
            }
            Err(err) => {
                failed += 1;
                eprintln!("failed to convert {}: {err}", replay_path.display());
            }
        }
    }

    if converted > 0 || skipped > 0 || failed > 0 {
        eprintln!("scan completed: converted={converted}, skipped={skipped}, failed={failed}");
    }

    Ok(())
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

fn convert_replay_file(
    replay_path: &Path,
    input_dir: &Path,
    output_dir: &Path,
) -> Result<ConvertStatus> {
    let replay_id = replay_id_from_path(replay_path)?;
    let date_segment = resolve_date_segment(replay_path, input_dir)?;
    let output_path = output_dir
        .join("json")
        .join(date_segment)
        .join(format!("{replay_id}.replay"));

    if output_path.exists() {
        return Ok(ConvertStatus::AlreadyExists(output_path));
    }

    let data = fs::read(replay_path)
        .with_context(|| format!("failed to read replay file {}", replay_path.display()))?;
    let replay = parse_replay(&data)
        .with_context(|| format!("failed to parse replay file {}", replay_path.display()))?;
    let json_bytes = serde_json::to_vec(&replay).context("failed to serialize replay to JSON")?;

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

    Ok(ConvertStatus::Converted(output_path))
}

fn replay_id_from_path(path: &Path) -> Result<String> {
    let stem = path
        .file_stem()
        .and_then(|x| x.to_str())
        .ok_or_else(|| anyhow!("failed to derive replay_id from {}", path.display()))?;

    if stem.is_empty() {
        return Err(anyhow!("empty replay_id for {}", path.display()));
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
    spawn_windows_replacer(current_exe, staged_path, false)?;

    Ok("update staged. restart this app to complete replacement".to_string())
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
    fn is_ymd_segment_recognizes_expected_format() {
        assert!(is_ymd_segment("2026-03-02"));
        assert!(!is_ymd_segment("2026-3-2"));
        assert!(!is_ymd_segment("2026/03/02"));
    }

    #[test]
    fn date_from_ancestor_path_extracts_nested_date() {
        let input_dir = Path::new("/data/replays");
        let replay = Path::new("/data/replays/zen/2026-03-01/abc.replay");
        assert_eq!(
            date_from_ancestor_path(replay, input_dir),
            Some("2026-03-01".to_string())
        );
    }

    #[test]
    fn replay_id_from_path_uses_file_stem() {
        let replay = Path::new("/tmp/DEADBEEF.replay");
        let replay_id = replay_id_from_path(replay).expect("replay id");
        assert_eq!(replay_id, "DEADBEEF");
    }

    #[test]
    fn parse_version_tag_handles_v_prefix() {
        let version = parse_version_tag("v1.2.3").expect("version");
        assert_eq!(version, Version::new(1, 2, 3));
    }
}
