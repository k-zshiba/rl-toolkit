mod analyzer;
mod input;
pub mod report;

use crate::analyzer::{
    analyze_replay_file, build_batch_summary, report_output_path, scan_json_files, write_report,
    write_summary,
};
use crate::report::ANALYSIS_DIRNAME;
use anyhow::{Context, Result, anyhow};
use std::fs;
use std::path::{Path, PathBuf};

pub use crate::report::{
    ANALYSIS_VERSION, AnalysisReport, Availability, BatchSummary, ConcedeDiagnosis,
    DiagnosisEvidence, DiagnosisLabel, DiagnosisLabelReport, GoalReport, MatchManifest,
    MatchMeta, MetricQuality, MetricValue, ParseQuality, PlayerMetricsReport, ScoreLine,
    TeamMetricsReport,
};

pub fn analyze_file(input: impl AsRef<Path>, output_dir: impl AsRef<Path>, pretty: bool) -> Result<AnalysisReport> {
    let input = input.as_ref();
    if !input.is_file() {
        return Err(anyhow!("input path is not a file: {}", input.display()));
    }

    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let report = analyze_replay_file(input)?;
    let output_path = report_output_path(output_dir, input, &report);
    write_report(&output_path, &report, pretty)?;
    Ok(report)
}

pub fn analyze_path(input: impl AsRef<Path>, output_dir: impl AsRef<Path>, pretty: bool) -> Result<BatchSummary> {
    let input = input.as_ref();
    let output_dir = output_dir.as_ref();
    if input.is_file() {
        let report = analyze_file(input, output_dir, pretty)?;
        let mut summary = build_batch_summary(vec![report.clone()]);
        let report_path = report_output_path(output_dir, input, &report);
        if let Some(manifest) = summary.matches.first_mut() {
            manifest.report_path = report_path.display().to_string();
        }
        summary.loaded_reports = vec![report];
        return Ok(summary);
    }

    if !input.is_dir() {
        return Err(anyhow!("input path does not exist: {}", input.display()));
    }

    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let files = scan_json_files(input)?;
    let mut reports = Vec::new();
    let mut output_paths = Vec::new();
    for file in files {
        let report = analyze_replay_file(&file)?;
        let output_path = report_output_path(output_dir, &file, &report);
        write_report(&output_path, &report, pretty)?;
        output_paths.push(output_path);
        reports.push(report);
    }

    let mut summary = build_batch_summary(reports.clone());
    for (manifest, output_path) in summary.matches.iter_mut().zip(output_paths.iter()) {
        manifest.report_path = output_path.display().to_string();
    }
    summary.loaded_reports = reports;

    let summary_path = output_dir.join(ANALYSIS_DIRNAME).join("summary.json");
    write_summary(&summary_path, &summary, pretty)?;

    Ok(summary)
}

pub fn load_reports(output_dir: impl AsRef<Path>) -> Result<BatchSummary> {
    let output_dir = output_dir.as_ref();
    let analysis_dir = output_dir.join(ANALYSIS_DIRNAME);
    if !analysis_dir.exists() {
        return Ok(BatchSummary::empty());
    }

    let mut report_paths = Vec::new();
    collect_analysis_reports(&analysis_dir, &mut report_paths)?;
    report_paths.sort();

    let mut reports = Vec::new();
    for path in &report_paths {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read analysis report {}", path.display()))?;
        let report = serde_json::from_str::<AnalysisReport>(&raw)
            .with_context(|| format!("failed to parse analysis report {}", path.display()))?;
        reports.push(report);
    }

    let mut summary = build_batch_summary(reports.clone());
    for (manifest, path) in summary.matches.iter_mut().zip(report_paths.iter()) {
        manifest.report_path = path.display().to_string();
    }
    summary.loaded_reports = reports;
    Ok(summary)
}

fn collect_analysis_reports(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type for {}", path.display()))?;
        if file_type.is_dir() {
            collect_analysis_reports(&path, files)?;
        } else if file_type.is_file()
            && path.extension().and_then(|ext| ext.to_str()) == Some("json")
            && path.file_name().and_then(|name| name.to_str()) != Some("summary.json")
        {
            files.push(path);
        }
    }
    Ok(())
}
