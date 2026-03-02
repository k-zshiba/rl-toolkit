use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::Parser;
use reqwest::blocking::{Client, Response};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const API_BASE: &str = "https://ballchasing.com/api";
const API_KEY_ENV: &str = "BALLCHASING_API_KEY";
const REQUEST_SLEEP_ENV: &str = "BALLCHASING_REQUEST_SLEEP_MS";
const DEFAULT_REQUEST_SLEEP_MS: u64 = 1000;

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
    let args = Args::parse();
    let api_key = env::var(API_KEY_ENV)
        .with_context(|| format!("{API_KEY_ENV} environment variable is required"))?;
    let player_slug = slugify_player_name(&args.player);
    let request_sleep = request_sleep_duration();
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
            request_sleep,
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

            match download_replay_file(&client, &api_key, &replay.id, request_sleep) {
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
    request_sleep: Duration,
) -> Result<ReplayListResponse> {
    let response = if let Some(url) = next_url {
        send(client.get(url), api_key, request_sleep)?
    } else {
        send(
            client.get(format!("{API_BASE}/replays")).query(&[
                ("player-name", player),
                ("sort-by", "replay-date"),
                ("sort-dir", "desc"),
                ("count", "200"),
            ]),
            api_key,
            request_sleep,
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
    request_sleep: Duration,
) -> Result<Vec<u8>> {
    let response = send(
        client.get(format!("{API_BASE}/replays/{replay_id}/file")),
        api_key,
        request_sleep,
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
    request_sleep: Duration,
) -> Result<Response> {
    let response = request
        .header(AUTHORIZATION, api_key)
        .send()
        .context("HTTP request failed")?;

    if !request_sleep.is_zero() {
        thread::sleep(request_sleep);
    }

    response
        .error_for_status()
        .context("unexpected HTTP status from ballchasing API")
}

fn request_sleep_duration() -> Duration {
    match env::var(REQUEST_SLEEP_ENV) {
        Ok(value) => match value.parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(_) => {
                eprintln!(
                    "invalid {REQUEST_SLEEP_ENV}='{value}', fallback to {DEFAULT_REQUEST_SLEEP_MS}ms"
                );
                Duration::from_millis(DEFAULT_REQUEST_SLEEP_MS)
            }
        },
        Err(_) => Duration::from_millis(DEFAULT_REQUEST_SLEEP_MS),
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
}
