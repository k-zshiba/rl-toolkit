use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct ReplayInput {
    #[serde(default)]
    pub game_type: String,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
    #[serde(default)]
    pub network_frames: Option<NetworkFramesInput>,
    #[serde(default)]
    pub levels: Vec<String>,
    #[serde(default)]
    pub objects: Vec<String>,
    #[serde(default)]
    pub names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkFramesInput {
    #[serde(default)]
    pub frames: Vec<FrameInput>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FrameInput {
    #[serde(default)]
    pub time: f32,
    #[serde(default)]
    pub delta: f32,
    #[serde(default)]
    pub new_actors: Vec<NewActorInput>,
    #[serde(default)]
    pub deleted_actors: Vec<i32>,
    #[serde(default)]
    pub updated_actors: Vec<UpdatedActorInput>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NewActorInput {
    pub actor_id: i32,
    #[serde(default)]
    pub name_id: Option<i32>,
    pub object_id: i32,
    #[serde(default)]
    pub initial_trajectory: TrajectoryInput,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TrajectoryInput {
    #[serde(default)]
    pub location: Option<Vector3iInput>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Vector3iInput {
    pub x: i32,
    pub y: i32,
    pub z: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdatedActorInput {
    pub actor_id: i32,
    pub object_id: i32,
    pub attribute: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeaderGoal {
    pub frame: Option<i32>,
    pub scorer_name: Option<String>,
    pub scoring_team: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeaderPlayerStats {
    pub player_name: String,
    pub team: u8,
    pub score: Option<i32>,
    pub goals: Option<i32>,
    pub assists: Option<i32>,
    pub shots: Option<i32>,
    pub saves: Option<i32>,
    pub demos: Option<i32>,
    pub self_demos: Option<i32>,
}

pub fn load_replay(path: &Path) -> Result<ReplayInput> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read replay json {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse replay json {}", path.display()))
}

pub fn property<'a>(properties: &'a BTreeMap<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| properties.get(*key))
}

pub fn property_string(properties: &BTreeMap<String, Value>, keys: &[&str]) -> Option<String> {
    property(properties, keys).and_then(value_string)
}

pub fn property_bool(properties: &BTreeMap<String, Value>, keys: &[&str]) -> Option<bool> {
    property(properties, keys).and_then(Value::as_bool)
}

pub fn property_i32(properties: &BTreeMap<String, Value>, keys: &[&str]) -> Option<i32> {
    property(properties, keys).and_then(value_i32)
}

pub fn parse_header_goals(properties: &BTreeMap<String, Value>) -> Vec<HeaderGoal> {
    property(properties, &["Goals"])
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| {
            let obj = value.as_object()?;
            let team = obj
                .get("PlayerTeam")
                .and_then(value_i32)
                .and_then(|value| u8::try_from(value).ok())?;
            Some(HeaderGoal {
                frame: obj.get("frame").and_then(value_i32),
                scorer_name: obj.get("PlayerName").and_then(value_string),
                scoring_team: team,
            })
        })
        .collect()
}

pub fn parse_header_player_stats(properties: &BTreeMap<String, Value>) -> Vec<HeaderPlayerStats> {
    property(properties, &["PlayerStats", "PlayerStats2"])
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| parse_header_player_stats_entry(value.as_object()?))
        .collect()
}

fn parse_header_player_stats_entry(obj: &Map<String, Value>) -> Option<HeaderPlayerStats> {
    let team = obj
        .get("PlayerTeam")
        .or_else(|| obj.get("Team"))
        .and_then(value_i32)
        .and_then(|value| u8::try_from(value).ok())?;
    let player_name = obj
        .get("PlayerName")
        .or_else(|| obj.get("Name"))
        .and_then(value_string)?;

    Some(HeaderPlayerStats {
        player_name,
        team,
        score: obj
            .get("Score")
            .or_else(|| obj.get("MatchScore"))
            .and_then(value_i32),
        goals: obj
            .get("Goals")
            .or_else(|| obj.get("MatchGoals"))
            .and_then(value_i32),
        assists: obj
            .get("Assists")
            .or_else(|| obj.get("MatchAssists"))
            .and_then(value_i32),
        shots: obj
            .get("Shots")
            .or_else(|| obj.get("MatchShots"))
            .and_then(value_i32),
        saves: obj
            .get("Saves")
            .or_else(|| obj.get("MatchSaves"))
            .and_then(value_i32),
        demos: obj
            .get("Demolitions")
            .or_else(|| obj.get("CarDemolitions"))
            .or_else(|| obj.get("MatchDemolishes"))
            .and_then(value_i32),
        self_demos: obj
            .get("SelfDemolitions")
            .or_else(|| obj.get("SelfDemolishes"))
            .and_then(value_i32),
    })
}

pub fn object_name(replay: &ReplayInput, object_id: i32) -> Option<&str> {
    replay.objects.get(object_id as usize).map(String::as_str)
}

pub fn name_from_id(replay: &ReplayInput, name_id: Option<i32>) -> Option<String> {
    replay
        .names
        .get(name_id? as usize)
        .map(|value| value.to_string())
}

pub fn variant<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value.as_object()?.get(name)
}

pub fn value_string(value: &Value) -> Option<String> {
    value.as_str().map(ToOwned::to_owned)
}

pub fn value_i32(value: &Value) -> Option<i32> {
    value
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .or_else(|| value.as_u64().and_then(|number| i32::try_from(number).ok()))
}

pub fn value_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value_i32(value).map(f64::from))
}

pub fn value_u8(value: &Value) -> Option<u8> {
    value
        .as_u64()
        .and_then(|number| u8::try_from(number).ok())
        .or_else(|| value_i32(value).and_then(|number| u8::try_from(number).ok()))
}

pub fn trajectory_location_to_vec3(location: Option<Vector3iInput>) -> Option<(f64, f64, f64)> {
    location.map(|location| {
        (
            location.x as f64 / 100.0,
            location.y as f64 / 100.0,
            location.z as f64 / 100.0,
        )
    })
}
