use anyhow::Result;
use rl_coach::{MetricQuality, ParseQuality, analyze_file, analyze_path, load_reports};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn copy_fixture_dir(target: &Path) -> Result<()> {
    fs::create_dir_all(target)?;
    for name in [
        "full_soccar.json",
        "header_only_soccar.json",
        "unsupported_mode.json",
        "out_of_order_boost_link.json",
    ] {
        fs::copy(fixture_path(name), target.join(name))?;
    }
    Ok(())
}

#[test]
fn full_soccar_fixture_reports_full_quality_and_diagnoses() -> Result<()> {
    let dir = tempdir()?;
    let report = analyze_file(fixture_path("full_soccar.json"), dir.path(), true)?;

    assert_eq!(report.availability.parse_quality, ParseQuality::Full);
    assert!(report.availability.diagnostics);
    assert_eq!(report.meta.final_score.orange, 1);
    assert_eq!(report.goals.len(), 1);
    assert!(!report.concede_diagnoses.is_empty());
    let labels: Vec<_> = report.concede_diagnoses[0]
        .labels
        .iter()
        .map(|label| label.label)
        .collect();
    assert!(
        labels
            .iter()
            .any(|label| format!("{:?}", label).contains("FailedClear"))
    );
    assert!(
        labels
            .iter()
            .any(|label| format!("{:?}", label).contains("DemoDisruption"))
    );
    Ok(())
}

#[test]
fn header_only_fixture_marks_derived_metrics_unavailable() -> Result<()> {
    let dir = tempdir()?;
    let report = analyze_file(fixture_path("header_only_soccar.json"), dir.path(), true)?;

    assert_eq!(report.availability.parse_quality, ParseQuality::HeaderOnly);
    assert!(!report.availability.diagnostics);
    let blue = report
        .player_metrics
        .iter()
        .find(|player| player.player_name == "BlueOne")
        .expect("blue player metrics");
    assert_eq!(blue.metrics["score"].quality, MetricQuality::Exact);
    assert_eq!(
        blue.metrics["avg_speed"].quality,
        MetricQuality::Unavailable
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("network frames unavailable"))
    );
    Ok(())
}

#[test]
fn unsupported_fixture_marks_parse_quality_unsupported() -> Result<()> {
    let dir = tempdir()?;
    let report = analyze_file(fixture_path("unsupported_mode.json"), dir.path(), true)?;

    assert_eq!(report.availability.parse_quality, ParseQuality::Unsupported);
    assert!(!report.availability.supported_mode);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("unsupported replay mode"))
    );
    Ok(())
}

#[test]
fn out_of_order_boost_component_link_still_computes_avg_boost() -> Result<()> {
    let dir = tempdir()?;
    let report = analyze_file(
        fixture_path("out_of_order_boost_link.json"),
        dir.path(),
        true,
    )?;

    let blue = report
        .player_metrics
        .iter()
        .find(|player| player.player_name == "BlueOne")
        .expect("blue player metrics");
    assert_eq!(blue.metrics["avg_boost"].quality, MetricQuality::Estimated);
    assert!(blue.metrics["avg_boost"].value.unwrap_or_default() > 0.0);
    Ok(())
}

#[test]
fn analyze_directory_writes_summary_and_loads_reports() -> Result<()> {
    let input_dir = tempdir()?;
    let output_dir = tempdir()?;
    copy_fixture_dir(input_dir.path())?;

    let summary = analyze_path(input_dir.path(), output_dir.path(), true)?;
    assert_eq!(summary.matches.len(), 4);
    assert!(
        output_dir
            .path()
            .join("analysis")
            .join("summary.json")
            .exists()
    );

    let loaded = load_reports(output_dir.path())?;
    assert_eq!(loaded.matches.len(), 4);
    assert_eq!(loaded.loaded_reports.len(), 4);
    assert!(!loaded.player_aggregate.is_empty());
    Ok(())
}

#[test]
fn cli_supports_single_file_and_directory_inputs() -> Result<()> {
    let binary = env!("CARGO_BIN_EXE_rl-coach");
    let single_output = tempdir()?;
    let output = Command::new(binary)
        .arg("--input")
        .arg(fixture_path("header_only_soccar.json"))
        .arg("--output-dir")
        .arg(single_output.path())
        .arg("--pretty-json")
        .output()?;
    assert!(output.status.success());
    assert!(
        single_output
            .path()
            .join("analysis")
            .join("2026-03-09")
            .join("header_only_soccar.json")
            .exists()
    );

    let dir_input = tempdir()?;
    let dir_output = tempdir()?;
    copy_fixture_dir(dir_input.path())?;
    let output = Command::new(binary)
        .arg("--input")
        .arg(dir_input.path())
        .arg("--output-dir")
        .arg(dir_output.path())
        .output()?;
    assert!(output.status.success());
    assert!(
        dir_output
            .path()
            .join("analysis")
            .join("summary.json")
            .exists()
    );
    Ok(())
}

#[test]
fn full_report_snapshot_matches_expected_schema() -> Result<()> {
    let dir = tempdir()?;
    let report = analyze_file(fixture_path("full_soccar.json"), dir.path(), true)?;
    let mut actual = serde_json::to_value(report)?;
    actual["source"]["input_path"] = Value::String("<input>".to_string());

    let expected: Value = serde_json::from_str(&fs::read_to_string(fixture_path(
        "expected_full_report.json",
    ))?)?;
    assert_eq!(actual, expected);
    Ok(())
}
