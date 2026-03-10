use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const ANALYSIS_VERSION: &str = "1";
pub const ANALYSIS_DIRNAME: &str = "analysis";

pub type MetricsMap = BTreeMap<String, MetricValue>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricQuality {
    Exact,
    Estimated,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricValue {
    pub value: Option<f64>,
    pub quality: MetricQuality,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl MetricValue {
    pub fn exact(value: f64) -> Self {
        Self {
            value: Some(value),
            quality: MetricQuality::Exact,
            note: None,
        }
    }

    pub fn estimated(value: f64, note: impl Into<String>) -> Self {
        Self {
            value: Some(value),
            quality: MetricQuality::Estimated,
            note: Some(note.into()),
        }
    }

    pub fn unavailable(note: impl Into<String>) -> Self {
        Self {
            value: None,
            quality: MetricQuality::Unavailable,
            note: Some(note.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParseQuality {
    Full,
    HeaderOnly,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnalysisSource {
    pub input_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScoreLine {
    pub blue: u32,
    pub orange: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MatchMeta {
    pub replay_id: String,
    pub date: String,
    pub map: String,
    pub mode: String,
    pub duration: f64,
    pub overtime: bool,
    pub final_score: ScoreLine,
    pub winner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Availability {
    pub parse_quality: ParseQuality,
    pub supported_mode: bool,
    pub network_frames: bool,
    pub diagnostics: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TeamMetricsReport {
    pub team: u8,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matches: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wins: Option<usize>,
    pub metrics: MetricsMap,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlayerMetricsReport {
    pub player_name: String,
    pub team: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matches: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wins: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    pub metrics: MetricsMap,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GoalReport {
    pub goal_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scorer_name: Option<String>,
    pub scoring_team: u8,
    pub conceding_team: u8,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosisLabel {
    KickoffBreakdown,
    FailedClear,
    DemoDisruption,
    LowBoostDefense,
    DoubleCommit,
    RotationGap,
    ReboundPressure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiagnosisEvidence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub player: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
    pub metric: String,
    pub value: String,
    pub frame_context: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiagnosisLabelReport {
    pub label: DiagnosisLabel,
    pub score: f64,
    pub evidence: Vec<DiagnosisEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConcedeDiagnosis {
    pub goal_index: usize,
    pub scoring_team: u8,
    pub conceding_team: u8,
    pub window_start: f64,
    pub window_end: f64,
    pub labels: Vec<DiagnosisLabelReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MatchManifest {
    pub replay_id: String,
    pub date: String,
    pub map: String,
    pub winner: Option<String>,
    pub final_score: ScoreLine,
    pub parse_quality: ParseQuality,
    pub diagnosis_count: usize,
    pub report_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnalysisReport {
    pub analysis_version: String,
    pub source: AnalysisSource,
    pub meta: MatchMeta,
    pub availability: Availability,
    pub team_metrics: Vec<TeamMetricsReport>,
    pub player_metrics: Vec<PlayerMetricsReport>,
    pub goals: Vec<GoalReport>,
    pub concede_diagnoses: Vec<ConcedeDiagnosis>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchSummary {
    pub analysis_version: String,
    pub matches: Vec<MatchManifest>,
    pub team_aggregate: Vec<TeamMetricsReport>,
    pub player_aggregate: Vec<PlayerMetricsReport>,
    pub warnings: Vec<String>,
    #[serde(skip)]
    pub loaded_reports: Vec<AnalysisReport>,
}

impl BatchSummary {
    pub fn empty() -> Self {
        Self {
            analysis_version: ANALYSIS_VERSION.to_string(),
            matches: Vec::new(),
            team_aggregate: Vec::new(),
            player_aggregate: Vec::new(),
            warnings: Vec::new(),
            loaded_reports: Vec::new(),
        }
    }
}
