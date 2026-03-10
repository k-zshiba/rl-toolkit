use crate::input::{
    HeaderGoal, HeaderPlayerStats, ReplayInput, UpdatedActorInput, load_replay, name_from_id,
    object_name, parse_header_goals, parse_header_player_stats, property_bool, property_i32,
    property_string, trajectory_location_to_vec3, value_f64, value_i32, value_string, value_u8,
    variant,
};
use crate::report::{
    ANALYSIS_VERSION, AnalysisReport, AnalysisSource, Availability, BatchSummary,
    ConcedeDiagnosis, DiagnosisEvidence, DiagnosisLabel, DiagnosisLabelReport, GoalReport,
    MatchManifest, MatchMeta, MetricQuality, MetricValue, ParseQuality, PlayerMetricsReport,
    ScoreLine, TeamMetricsReport,
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

const GOAL_WINDOW_SECONDS: f64 = 10.0;
const SUPERSONIC_SPEED: f64 = 2200.0;
const LOW_BOOST_THRESHOLD: f64 = 33.0;
const PRESSURE_DISTANCE: f64 = 3500.0;
const DOUBLE_COMMIT_DISTANCE: f64 = 2000.0;
const OWN_GOAL_Y: f64 = 5120.0;
const DEFENSIVE_THIRD_Y: f64 = 1700.0;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct Vec3 {
    x: f64,
    y: f64,
    z: f64,
}

impl Vec3 {
    fn magnitude(self) -> f64 {
        (self.x * self.x + self.y * self.y + self.z * self.z).sqrt()
    }

    fn distance(self, other: Self) -> f64 {
        Vec3 {
            x: self.x - other.x,
            y: self.y - other.y,
            z: self.z - other.z,
        }
        .magnitude()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorKind {
    Ball,
    Car,
    Pri,
    Team(u8),
    BoostComponent,
    GameEventSoccar,
    Other,
}

#[derive(Debug, Clone)]
struct ActorState {
    position: Option<Vec3>,
    velocity: Option<Vec3>,
    boost: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct PlayerStatsState {
    score: Option<i32>,
    goals: Option<i32>,
    assists: Option<i32>,
    shots: Option<i32>,
    saves: Option<i32>,
    demos: Option<i32>,
    self_demos: Option<i32>,
}

#[derive(Debug, Clone)]
struct PlayerState {
    actor_id: i32,
    name: Option<String>,
    team: Option<u8>,
    car_actor: Option<i32>,
    boost_component: Option<i32>,
    unique_id: Option<String>,
    stats: PlayerStatsState,
}

impl Default for PlayerState {
    fn default() -> Self {
        Self {
            actor_id: 0,
            name: None,
            team: None,
            car_actor: None,
            boost_component: None,
            unique_id: None,
            stats: PlayerStatsState::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct DemoEvent {
    time: f64,
    _attacker_car: Option<i32>,
    _victim_car: Option<i32>,
    _attacker_player: Option<String>,
    victim_player: Option<String>,
    _attacker_team: Option<u8>,
    victim_team: Option<u8>,
}

#[derive(Debug, Clone)]
struct TouchEvent {
    time: f64,
    team: u8,
}

#[derive(Debug, Clone)]
struct PlayerFrameState {
    player_actor: i32,
    name: String,
    team: u8,
    position: Option<Vec3>,
    speed: Option<f64>,
    boost: Option<f64>,
}

#[derive(Debug, Clone)]
struct FrameSnapshot {
    time: f64,
    delta: f64,
    ball_position: Option<Vec3>,
    players: Vec<PlayerFrameState>,
}

#[derive(Debug, Clone, Default)]
struct DerivedAccumulator {
    sample_time: f64,
    speed_time_sum: f64,
    speed_samples: f64,
    distance_traveled: f64,
    supersonic_time: f64,
    boost_time: f64,
    boost_sum: f64,
    low_boost_time: f64,
    empty_boost_time: f64,
    boost_spent: f64,
    boost_gained: f64,
    offensive_half_time: f64,
    defensive_half_time: f64,
    closest_to_ball_time: f64,
    pressure_time: f64,
    last_position: Option<Vec3>,
    last_boost: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct MatchRuntime {
    actors: HashMap<i32, ActorState>,
    players: HashMap<i32, PlayerState>,
    car_to_player: HashMap<i32, i32>,
    car_to_boost_component: HashMap<i32, i32>,
    team_actor_to_num: HashMap<i32, u8>,
    team_scores: HashMap<u8, i32>,
    ball_actor: Option<i32>,
    game_event_actor: Option<i32>,
    overtime: bool,
    demos: Vec<DemoEvent>,
    touches: Vec<TouchEvent>,
    snapshots: Vec<FrameSnapshot>,
    warnings: Vec<String>,
    demo_keys: HashSet<(i32, i32, i32)>,
}

#[derive(Debug, Clone)]
struct GoalDiagnosisContext<'a> {
    goal: &'a GoalReport,
    goal_time: f64,
    previous_goal_time: Option<f64>,
    window: Vec<&'a FrameSnapshot>,
    demos: Vec<&'a DemoEvent>,
    touches: Vec<&'a TouchEvent>,
}

#[derive(Debug, Clone)]
struct MetricAggregateState {
    sum: f64,
    weight: f64,
    count: usize,
    exact_only: bool,
    has_estimated: bool,
}

impl Default for MetricAggregateState {
    fn default() -> Self {
        Self {
            sum: 0.0,
            weight: 0.0,
            count: 0,
            exact_only: true,
            has_estimated: false,
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedPlayer {
    player_name: String,
    team: u8,
    unique_id: Option<String>,
    stats: PlayerStatsState,
    derived: DerivedAccumulator,
}

pub fn analyze_replay_file(path: &Path) -> Result<AnalysisReport> {
    let replay = load_replay(path)?;
    analyze_loaded_replay(path, &replay)
}

pub fn analyze_loaded_replay(path: &Path, replay: &ReplayInput) -> Result<AnalysisReport> {
    let parse_quality = determine_parse_quality(replay);
    let mut runtime = MatchRuntime::default();
    let goals = parse_header_goals(&replay.properties);
    let header_player_stats = parse_header_player_stats(&replay.properties);

    if matches!(parse_quality, ParseQuality::Full) {
        build_runtime(replay, &mut runtime);
    }

    let replay_id = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| anyhow!("failed to derive replay id from {}", path.display()))?
        .to_string();
    let date = resolve_date(path, replay);
    let map_name = property_string(&replay.properties, &["MapName", "Map", "LevelName"])
        .or_else(|| replay.levels.first().cloned())
        .unwrap_or_else(|| "unknown".to_string());
    let mode = if replay.game_type.trim().is_empty() {
        property_string(&replay.properties, &["MatchType"]).unwrap_or_else(|| "unknown".to_string())
    } else {
        replay.game_type.clone()
    };

    let mut warnings = runtime.warnings.clone();
    if matches!(parse_quality, ParseQuality::Unsupported) {
        warnings.push("unsupported replay mode: only soccar is fully analyzed in v1".to_string());
    }
    if matches!(parse_quality, ParseQuality::HeaderOnly) {
        warnings.push("network frames unavailable; derived metrics and concede diagnosis are omitted".to_string());
    }

    let duration = resolve_duration(replay, &runtime);
    let resolved_players = resolve_players(&runtime, &header_player_stats, duration);
    let team_scores = resolve_team_scores(&runtime, &goals);
    let winner = winner_from_score(&team_scores);
    let goal_reports = build_goal_reports(&goals, replay, &team_scores);

    let player_metrics = build_player_reports(&resolved_players, parse_quality, duration);
    let team_metrics = build_team_reports(&player_metrics, &team_scores, parse_quality, duration);
    let diagnostics_enabled = matches!(parse_quality, ParseQuality::Full);
    let concede_diagnoses = if diagnostics_enabled {
        build_diagnoses(&goal_reports, &runtime, duration)
    } else {
        Vec::new()
    };

    let overtime = runtime.overtime
        || property_bool(&replay.properties, &["bOverTime"])
            .unwrap_or_else(|| duration > 300.0 && team_scores.blue == team_scores.orange);

    Ok(AnalysisReport {
        analysis_version: ANALYSIS_VERSION.to_string(),
        source: AnalysisSource {
            input_path: path.display().to_string(),
        },
        meta: MatchMeta {
            replay_id,
            date,
            map: map_name,
            mode,
            duration,
            overtime,
            final_score: team_scores,
            winner,
        },
        availability: Availability {
            parse_quality,
            supported_mode: !matches!(parse_quality, ParseQuality::Unsupported),
            network_frames: replay
                .network_frames
                .as_ref()
                .map(|frames| !frames.frames.is_empty())
                .unwrap_or(false),
            diagnostics: diagnostics_enabled,
        },
        team_metrics,
        player_metrics,
        goals: goal_reports,
        concede_diagnoses,
        warnings,
    })
}

fn determine_parse_quality(replay: &ReplayInput) -> ParseQuality {
    let mode = replay.game_type.to_ascii_lowercase();
    if !mode.is_empty() && !mode.contains("soccar") {
        return ParseQuality::Unsupported;
    }

    match replay.network_frames.as_ref() {
        Some(frames) if !frames.frames.is_empty() => ParseQuality::Full,
        _ => ParseQuality::HeaderOnly,
    }
}

fn build_runtime(replay: &ReplayInput, runtime: &mut MatchRuntime) {
    let Some(frames) = replay.network_frames.as_ref() else {
        return;
    };

    for frame in &frames.frames {
        for actor_id in &frame.deleted_actors {
            runtime.actors.remove(actor_id);
            runtime.car_to_player.retain(|car, player| car != actor_id && player != actor_id);
            runtime
                .car_to_boost_component
                .retain(|car, component| car != actor_id && component != actor_id);
            runtime.players.remove(actor_id);
            for player in runtime.players.values_mut() {
                if player.car_actor == Some(*actor_id) {
                    player.car_actor = None;
                }
                if player.boost_component == Some(*actor_id) {
                    player.boost_component = None;
                }
            }
        }

        for new_actor in &frame.new_actors {
            let object = object_name(replay, new_actor.object_id).unwrap_or("unknown");
            let kind = classify_actor_kind(object);
            let position = trajectory_location_to_vec3(new_actor.initial_trajectory.location)
                .map(|(x, y, z)| Vec3 { x, y, z });

            runtime.actors.insert(
                new_actor.actor_id,
                ActorState {
                    position,
                    velocity: None,
                    boost: None,
                },
            );

            match kind {
                ActorKind::Ball => runtime.ball_actor = Some(new_actor.actor_id),
                ActorKind::GameEventSoccar => runtime.game_event_actor = Some(new_actor.actor_id),
                ActorKind::Pri => {
                    let mut player = runtime.players.remove(&new_actor.actor_id).unwrap_or_default();
                    player.actor_id = new_actor.actor_id;
                    player.name = player.name.or_else(|| name_from_id(replay, new_actor.name_id));
                    runtime.players.insert(new_actor.actor_id, player);
                }
                ActorKind::Team(team) => {
                    runtime.team_actor_to_num.insert(new_actor.actor_id, team);
                }
                _ => {}
            }
        }

        for updated in &frame.updated_actors {
            apply_update(replay, runtime, updated, frame.time as f64);
        }

        runtime.snapshots.push(FrameSnapshot {
            time: frame.time as f64,
            delta: frame.delta as f64,
            ball_position: runtime
                .ball_actor
                .and_then(|actor_id| runtime.actors.get(&actor_id))
                .and_then(|actor| actor.position),
            players: snapshot_players(runtime),
        });
    }
}

fn classify_actor_kind(object_name: &str) -> ActorKind {
    if object_name.contains("Archetypes.Ball") || object_name.contains("TAGame.Ball") {
        ActorKind::Ball
    } else if object_name.contains("Archetypes.Car") || object_name.contains("TAGame.Car") {
        ActorKind::Car
    } else if object_name.contains("PRI") || object_name.contains("PlayerReplicationInfo") {
        ActorKind::Pri
    } else if object_name.contains("Archetypes.Teams.Team0") {
        ActorKind::Team(0)
    } else if object_name.contains("Archetypes.Teams.Team1") {
        ActorKind::Team(1)
    } else if object_name.contains("CarComponent_Boost") {
        ActorKind::BoostComponent
    } else if object_name.contains("GameEvent_Soccar") {
        ActorKind::GameEventSoccar
    } else {
        ActorKind::Other
    }
}

fn apply_update(replay: &ReplayInput, runtime: &mut MatchRuntime, updated: &UpdatedActorInput, time: f64) {
    let Some(property_name) = object_name(replay, updated.object_id) else {
        return;
    };

    match property_name {
        "TAGame.RBActor_TA:ReplicatedRBState" => {
            if let Some((position, velocity)) = parse_rigid_body(&updated.attribute) {
                if let Some(actor) = runtime.actors.get_mut(&updated.actor_id) {
                    actor.position = Some(position);
                    actor.velocity = velocity;
                }
            }
        }
        "Engine.PlayerReplicationInfo:PlayerName" => {
            if let Some(name) = variant(&updated.attribute, "String").and_then(value_string) {
                let player = runtime.players.entry(updated.actor_id).or_default();
                player.actor_id = updated.actor_id;
                player.name = Some(name);
            }
        }
        "Engine.PlayerReplicationInfo:UniqueId" => {
            if let Some(unique_id) = variant(&updated.attribute, "UniqueId") {
                let player = runtime.players.entry(updated.actor_id).or_default();
                player.actor_id = updated.actor_id;
                player.unique_id = Some(unique_id.to_string());
            }
        }
        "Engine.PlayerReplicationInfo:Team" => {
            if let Some(team_actor) = parse_active_actor(&updated.attribute) {
                if let Some(team) = runtime.team_actor_to_num.get(&team_actor).copied() {
                    let player = runtime.players.entry(updated.actor_id).or_default();
                    player.actor_id = updated.actor_id;
                    player.team = Some(team);
                }
            }
        }
        "Engine.Pawn:PlayerReplicationInfo" => {
            if let Some(player_actor) = parse_active_actor(&updated.attribute) {
                link_player_to_car(runtime, player_actor, updated.actor_id);
            }
        }
        "TAGame.CarComponent_TA:Vehicle" => {
            if let Some(car_actor) = parse_active_actor(&updated.attribute) {
                link_boost_component_to_car(runtime, updated.actor_id, car_actor);
            }
        }
        "TAGame.CarComponent_Boost_TA:ReplicatedBoost" => {
            if let Some(boost) = parse_replicated_boost(&updated.attribute) {
                if let Some(actor) = runtime.actors.get_mut(&updated.actor_id) {
                    actor.boost = Some(boost);
                }
            }
        }
        "TAGame.CarComponent_Boost_TA:ReplicatedBoostAmount" => {
            if let Some(boost) = variant(&updated.attribute, "Byte").and_then(value_u8) {
                if let Some(actor) = runtime.actors.get_mut(&updated.actor_id) {
                    actor.boost = Some(boost_to_percent(boost));
                }
            }
        }
        "TAGame.Ball_TA:HitTeamNum"
        | "TAGame.Ball_Breakout_TA:LastTeamTouch"
        | "TAGame.Ball_Haunted_TA:LastTeamTouch" => {
            if let Some(team) = variant(&updated.attribute, "Byte").and_then(value_u8) {
                runtime.touches.push(TouchEvent { time, team });
            }
        }
        "TAGame.GameEvent_Soccar_TA:bOverTime" => {
            if let Some(value) = variant(&updated.attribute, "Boolean").and_then(Value::as_bool) {
                runtime.overtime = value;
            }
        }
        "TAGame.PRI_TA:MatchScore" => set_player_stat(runtime, updated.actor_id, |stats, value| stats.score = Some(value), &updated.attribute),
        "TAGame.PRI_TA:MatchGoals" => set_player_stat(runtime, updated.actor_id, |stats, value| stats.goals = Some(value), &updated.attribute),
        "TAGame.PRI_TA:MatchAssists" => set_player_stat(runtime, updated.actor_id, |stats, value| stats.assists = Some(value), &updated.attribute),
        "TAGame.PRI_TA:MatchShots" => set_player_stat(runtime, updated.actor_id, |stats, value| stats.shots = Some(value), &updated.attribute),
        "TAGame.PRI_TA:MatchSaves" => set_player_stat(runtime, updated.actor_id, |stats, value| stats.saves = Some(value), &updated.attribute),
        "TAGame.PRI_TA:MatchDemolishes" | "TAGame.PRI_TA:CarDemolitions" => {
            set_player_stat(runtime, updated.actor_id, |stats, value| stats.demos = Some(value), &updated.attribute)
        }
        "TAGame.PRI_TA:SelfDemolitions" => {
            set_player_stat(runtime, updated.actor_id, |stats, value| stats.self_demos = Some(value), &updated.attribute)
        }
        "Engine.PlayerReplicationInfo:Score" => {
            set_player_stat(runtime, updated.actor_id, |stats, value| stats.score = Some(value), &updated.attribute)
        }
        "TAGame.Team_Soccar_TA:GameScore" | "Engine.TeamInfo:Score" => {
            if let Some(score) = variant(&updated.attribute, "Int").and_then(value_i32)
                && let Some(team) = runtime.team_actor_to_num.get(&updated.actor_id).copied()
            {
                runtime.team_scores.insert(team, score);
            }
        }
        "TAGame.Car_TA:ReplicatedDemolish"
        | "TAGame.Car_TA:ReplicatedDemolishExtended"
        | "TAGame.Car_TA:ReplicatedDemolish_CustomFX"
        | "TAGame.Car_TA:ReplicatedDemolishGoalExplosion" => {
            if let Some((attacker, victim)) = parse_demo_pair(&updated.attribute) {
                let key = ((time * 10.0).round() as i32, attacker.unwrap_or(-1), victim.unwrap_or(-1));
                if runtime.demo_keys.insert(key) {
                    runtime.demos.push(DemoEvent {
                        time,
                        _attacker_car: attacker,
                        _victim_car: victim,
                        _attacker_player: player_name_from_car(runtime, attacker),
                        victim_player: player_name_from_car(runtime, victim),
                        _attacker_team: player_team_from_car(runtime, attacker),
                        victim_team: player_team_from_car(runtime, victim),
                    });
                }
            }
        }
        _ => {}
    }
}

fn set_player_stat(
    runtime: &mut MatchRuntime,
    actor_id: i32,
    setter: impl FnOnce(&mut PlayerStatsState, i32),
    attribute: &Value,
) {
    if let Some(value) = variant(attribute, "Int").and_then(value_i32) {
        let player = runtime.players.entry(actor_id).or_default();
        player.actor_id = actor_id;
        setter(&mut player.stats, value);
    }
}

fn link_player_to_car(runtime: &mut MatchRuntime, player_actor: i32, car_actor: i32) {
    let player = runtime.players.entry(player_actor).or_default();
    if let Some(previous_car) = player.car_actor
        && previous_car != car_actor
    {
        runtime.car_to_player.remove(&previous_car);
    }
    player.actor_id = player_actor;
    player.car_actor = Some(car_actor);
    runtime.car_to_player.insert(car_actor, player_actor);

    if let Some(boost_component) = runtime.car_to_boost_component.get(&car_actor).copied() {
        if let Some(player) = runtime.players.get_mut(&player_actor) {
            player.boost_component = Some(boost_component);
        }
    }
}

fn link_boost_component_to_car(runtime: &mut MatchRuntime, boost_component_actor: i32, car_actor: i32) {
    runtime
        .car_to_boost_component
        .retain(|_, component| *component != boost_component_actor);
    runtime
        .car_to_boost_component
        .insert(car_actor, boost_component_actor);

    if let Some(player_actor) = runtime.car_to_player.get(&car_actor).copied() {
        if let Some(player) = runtime.players.get_mut(&player_actor) {
            player.boost_component = Some(boost_component_actor);
        }
    }
}

fn snapshot_players(runtime: &MatchRuntime) -> Vec<PlayerFrameState> {
    let mut players = Vec::new();

    for player in runtime.players.values() {
        let Some(team) = player.team else {
            continue;
        };
        let Some(car_actor) = player.car_actor else {
            continue;
        };
        let Some(car_state) = runtime.actors.get(&car_actor) else {
            continue;
        };
        let speed = car_state.velocity.map(Vec3::magnitude);
        let boost = player
            .boost_component
            .and_then(|actor_id| runtime.actors.get(&actor_id))
            .and_then(|actor| actor.boost);

        players.push(PlayerFrameState {
            player_actor: player.actor_id,
            name: player
                .name
                .clone()
                .unwrap_or_else(|| format!("player_{}", player.actor_id)),
            team,
            position: car_state.position,
            speed,
            boost,
        });
    }

    players.sort_by(|left, right| {
        (left.team, left.name.as_str(), left.player_actor).cmp(&(right.team, right.name.as_str(), right.player_actor))
    });
    players
}

fn parse_active_actor(attribute: &Value) -> Option<i32> {
    let value = variant(attribute, "ActiveActor")?;
    let object = value.as_object()?;
    if !object
        .get("active")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return None;
    }
    object.get("actor").and_then(value_i32)
}

fn parse_rigid_body(attribute: &Value) -> Option<(Vec3, Option<Vec3>)> {
    let value = variant(attribute, "RigidBody")?.as_object()?;
    let position = value.get("location").and_then(parse_vec3)?;
    let velocity = value.get("linear_velocity").and_then(parse_vec3);
    Some((position, velocity))
}

fn parse_vec3(value: &Value) -> Option<Vec3> {
    let object = value.as_object()?;
    Some(Vec3 {
        x: object.get("x").and_then(value_f64)?,
        y: object.get("y").and_then(value_f64)?,
        z: object.get("z").and_then(value_f64)?,
    })
}

fn parse_replicated_boost(attribute: &Value) -> Option<f64> {
    let value = variant(attribute, "ReplicatedBoost")?.as_object()?;
    let amount = value.get("boost_amount").and_then(value_u8)?;
    Some(boost_to_percent(amount))
}

fn boost_to_percent(amount: u8) -> f64 {
    amount as f64 / 255.0 * 100.0
}

fn parse_demo_pair(attribute: &Value) -> Option<(Option<i32>, Option<i32>)> {
    if let Some(value) = variant(attribute, "Demolish").and_then(Value::as_object) {
        return Some((
            value.get("attacker").and_then(value_i32),
            value.get("victim").and_then(value_i32),
        ));
    }
    if let Some(value) = variant(attribute, "DemolishFx").and_then(Value::as_object) {
        return Some((
            value.get("attacker").and_then(value_i32),
            value.get("victim").and_then(value_i32),
        ));
    }
    if let Some(value) = variant(attribute, "DemolishExtended").and_then(Value::as_object) {
        return Some((
            value
                .get("attacker")
                .and_then(parse_nested_active_actor),
            value.get("victim").and_then(parse_nested_active_actor),
        ));
    }
    None
}

fn parse_nested_active_actor(value: &Value) -> Option<i32> {
    let object = value.as_object()?;
    if !object
        .get("active")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return None;
    }
    object.get("actor").and_then(value_i32)
}

fn player_name_from_car(runtime: &MatchRuntime, car_actor: Option<i32>) -> Option<String> {
    let car_actor = car_actor?;
    runtime
        .players
        .values()
        .find(|player| player.car_actor == Some(car_actor))
        .and_then(|player| player.name.clone())
}

fn player_team_from_car(runtime: &MatchRuntime, car_actor: Option<i32>) -> Option<u8> {
    let car_actor = car_actor?;
    runtime
        .players
        .values()
        .find(|player| player.car_actor == Some(car_actor))
        .and_then(|player| player.team)
}

fn resolve_duration(replay: &ReplayInput, runtime: &MatchRuntime) -> f64 {
    runtime
        .snapshots
        .last()
        .map(|snapshot| snapshot.time)
        .or_else(|| property_i32(&replay.properties, &["NumFrames"]).map(|frames| frames as f64 / 30.0))
        .unwrap_or(0.0)
}

fn resolve_date(path: &Path, replay: &ReplayInput) -> String {
    property_string(&replay.properties, &["Date", "MatchDate"])
        .filter(|value| !value.trim().is_empty())
        .or_else(|| date_from_ancestor(path))
        .or_else(|| {
            fs::metadata(path)
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .map(|value| {
                    let value: DateTime<Utc> = value.into();
                    value.format("%Y-%m-%d").to_string()
                })
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn date_from_ancestor(path: &Path) -> Option<String> {
    for ancestor in path.ancestors() {
        let segment = ancestor.file_name()?.to_str()?;
        if segment.len() == 10
            && segment.as_bytes().get(4) == Some(&b'-')
            && segment.as_bytes().get(7) == Some(&b'-')
        {
            return Some(segment.to_string());
        }
    }
    None
}

fn resolve_players(
    runtime: &MatchRuntime,
    header_player_stats: &[HeaderPlayerStats],
    duration: f64,
) -> Vec<ResolvedPlayer> {
    let mut derived = compute_derived_metrics(&runtime.snapshots, duration);
    let mut resolved: BTreeMap<(u8, String), ResolvedPlayer> = BTreeMap::new();

    for player in runtime.players.values() {
        let Some(team) = player.team else {
            continue;
        };
        let name = player
            .name
            .clone()
            .unwrap_or_else(|| format!("player_{}", player.actor_id));
        let accumulator = derived.remove(&player.actor_id).unwrap_or_default();
        resolved.insert(
            (team, name.clone()),
            ResolvedPlayer {
                player_name: name,
                team,
                unique_id: player.unique_id.clone(),
                stats: player.stats.clone(),
                derived: accumulator,
            },
        );
    }

    for header in header_player_stats {
        let key = (header.team, header.player_name.clone());
        let entry = resolved.entry(key).or_insert_with(|| ResolvedPlayer {
            player_name: header.player_name.clone(),
            team: header.team,
            unique_id: None,
            stats: PlayerStatsState::default(),
            derived: DerivedAccumulator::default(),
        });
        merge_optional_stat(&mut entry.stats.score, header.score);
        merge_optional_stat(&mut entry.stats.goals, header.goals);
        merge_optional_stat(&mut entry.stats.assists, header.assists);
        merge_optional_stat(&mut entry.stats.shots, header.shots);
        merge_optional_stat(&mut entry.stats.saves, header.saves);
        merge_optional_stat(&mut entry.stats.demos, header.demos);
        merge_optional_stat(&mut entry.stats.self_demos, header.self_demos);
    }

    resolved.into_values().collect()
}

fn merge_optional_stat(target: &mut Option<i32>, source: Option<i32>) {
    if target.is_none() {
        *target = source;
    }
}

fn compute_derived_metrics(
    snapshots: &[FrameSnapshot],
    _duration: f64,
) -> HashMap<i32, DerivedAccumulator> {
    let mut accumulators: HashMap<i32, DerivedAccumulator> = HashMap::new();

    for snapshot in snapshots {
        let dt = if snapshot.delta > 0.0 {
            snapshot.delta
        } else {
            0.0
        };
        if dt <= 0.0 {
            continue;
        }

        let closest_player = snapshot.ball_position.and_then(|ball| {
            snapshot
                .players
                .iter()
                .filter_map(|player| player.position.map(|position| (player.player_actor, position.distance(ball))))
                .min_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal))
                .map(|(player_actor, _)| player_actor)
        });

        let mut pressured_teams = HashSet::new();

        for player in &snapshot.players {
            let accumulator = accumulators.entry(player.player_actor).or_default();
            accumulator.sample_time += dt;

            let speed = match (player.speed, accumulator.last_position, player.position) {
                (Some(speed), _, _) => Some(speed),
                (None, Some(previous), Some(current)) if dt > 0.0 => Some(previous.distance(current) / dt),
                _ => None,
            };
            if let Some(speed) = speed {
                accumulator.speed_time_sum += speed * dt;
                accumulator.speed_samples += dt;
                if speed >= SUPERSONIC_SPEED {
                    accumulator.supersonic_time += dt;
                }
            }

            if let (Some(previous), Some(current)) = (accumulator.last_position, player.position) {
                accumulator.distance_traveled += previous.distance(current);
            }
            if let Some(position) = player.position {
                if is_offensive_half(player.team, position.y) {
                    accumulator.offensive_half_time += dt;
                } else {
                    accumulator.defensive_half_time += dt;
                }
                accumulator.last_position = Some(position);
            }

            if let Some(boost) = player.boost {
                accumulator.boost_time += dt;
                accumulator.boost_sum += boost * dt;
                if boost < LOW_BOOST_THRESHOLD {
                    accumulator.low_boost_time += dt;
                }
                if boost <= 1.0 {
                    accumulator.empty_boost_time += dt;
                }
                if let Some(previous) = accumulator.last_boost {
                    let delta = boost - previous;
                    if delta > 0.0 {
                        accumulator.boost_gained += delta;
                    } else {
                        accumulator.boost_spent += delta.abs();
                    }
                }
                accumulator.last_boost = Some(boost);
            }

            if closest_player == Some(player.player_actor) {
                accumulator.closest_to_ball_time += dt;
            }

            if let (Some(ball), Some(position)) = (snapshot.ball_position, player.position) {
                let ball_offensive = is_offensive_half(player.team, ball.y);
                if ball_offensive
                    && is_offensive_half(player.team, position.y)
                    && position.distance(ball) <= PRESSURE_DISTANCE
                {
                    accumulator.pressure_time += dt;
                    pressured_teams.insert(player.team);
                }
            }
        }

        for team in pressured_teams {
            let team_key = team_aggregate_actor_id(team);
            accumulators.entry(team_key).or_default().pressure_time += dt;
        }
        if let Some(player_actor) = closest_player {
            let team = snapshot
                .players
                .iter()
                .find(|player| player.player_actor == player_actor)
                .map(|player| player.team);
            if let Some(team) = team {
                let team_key = team_aggregate_actor_id(team);
                accumulators.entry(team_key).or_default().closest_to_ball_time += dt;
            }
        }
    }

    accumulators
}

fn build_player_reports(
    players: &[ResolvedPlayer],
    parse_quality: ParseQuality,
    duration: f64,
) -> Vec<PlayerMetricsReport> {
    let mut reports = Vec::new();

    for player in players {
        let mut metrics = BTreeMap::new();
        insert_count_metric(&mut metrics, "score", player.stats.score);
        insert_count_metric(&mut metrics, "goals", player.stats.goals);
        insert_count_metric(&mut metrics, "assists", player.stats.assists);
        insert_count_metric(&mut metrics, "shots", player.stats.shots);
        insert_count_metric(&mut metrics, "saves", player.stats.saves);
        insert_count_metric(&mut metrics, "demos", player.stats.demos);
        insert_count_metric(&mut metrics, "self_demos", player.stats.self_demos);
        insert_derived_metrics(&mut metrics, &player.derived, parse_quality, duration, false);

        reports.push(PlayerMetricsReport {
            player_name: player.player_name.clone(),
            team: player.team,
            matches: None,
            wins: None,
            unique_id: player.unique_id.clone(),
            metrics,
        });
    }

    reports.sort_by(|left, right| (left.team, left.player_name.as_str()).cmp(&(right.team, right.player_name.as_str())));
    reports
}

fn build_team_reports(
    player_metrics: &[PlayerMetricsReport],
    team_scores: &ScoreLine,
    parse_quality: ParseQuality,
    duration: f64,
) -> Vec<TeamMetricsReport> {
    let team_winners = winner_from_score(team_scores);
    let mut reports = Vec::new();

    for team in [0_u8, 1_u8] {
        let mut metrics = BTreeMap::new();
        let relevant_players: Vec<_> = player_metrics.iter().filter(|player| player.team == team).collect();
        for metric_name in metric_names() {
            let aggregated = aggregate_metric_values(
                relevant_players
                    .iter()
                    .filter_map(|player| player.metrics.get(*metric_name))
                    .collect(),
                metric_behavior(metric_name),
                duration,
            );
            metrics.insert(metric_name.to_string(), aggregated);
        }

        let score_value = if team == 0 { team_scores.blue } else { team_scores.orange };
        metrics.insert("goals".to_string(), MetricValue::exact(score_value as f64));

        reports.push(TeamMetricsReport {
            team,
            name: team_name(team).to_string(),
            matches: None,
            wins: None,
            metrics,
        });
    }

    if matches!(parse_quality, ParseQuality::HeaderOnly | ParseQuality::Unsupported) {
        for report in &mut reports {
            for metric_name in derived_metric_names() {
                report.metrics.insert(
                    metric_name.to_string(),
                    MetricValue::unavailable("network frames unavailable"),
                );
            }
        }
    }

    if let Some(winner) = team_winners {
        let winning_team = if winner == "blue" { 0 } else { 1 };
        if let Some(report) = reports.iter_mut().find(|report| report.team == winning_team) {
            report.wins = Some(1);
        }
    }

    reports
}

fn build_goal_reports(goals: &[HeaderGoal], replay: &ReplayInput, team_scores: &ScoreLine) -> Vec<GoalReport> {
    goals.iter()
        .enumerate()
        .map(|(index, goal)| GoalReport {
            goal_index: index,
            frame: goal.frame,
            time: goal_time(goal, replay),
            scorer_name: goal.scorer_name.clone(),
            scoring_team: goal.scoring_team,
            conceding_team: if goal.scoring_team == 0 { 1 } else { 0 },
        })
        .collect::<Vec<_>>()
        .into_iter()
        .take((team_scores.blue + team_scores.orange) as usize)
        .collect()
}

fn goal_time(goal: &HeaderGoal, replay: &ReplayInput) -> Option<f64> {
    let frame = goal.frame?;
    replay
        .network_frames
        .as_ref()
        .and_then(|frames| frames.frames.get(frame as usize))
        .map(|frame| frame.time as f64)
        .or_else(|| Some(frame as f64 / 30.0))
}

fn build_diagnoses(goals: &[GoalReport], runtime: &MatchRuntime, duration: f64) -> Vec<ConcedeDiagnosis> {
    goals.iter()
        .enumerate()
        .filter_map(|(index, goal)| {
            let goal_time = goal.time?;
            let previous_goal_time = goals.get(index.wrapping_sub(1)).and_then(|goal| goal.time);
            let window_start = (goal_time - GOAL_WINDOW_SECONDS).max(0.0);
            let window_end = (goal_time + GOAL_WINDOW_SECONDS).min(duration.max(goal_time));
            let context = GoalDiagnosisContext {
                goal,
                goal_time,
                previous_goal_time,
                window: runtime
                    .snapshots
                    .iter()
                    .filter(|snapshot| snapshot.time >= window_start && snapshot.time <= window_end)
                    .collect(),
                demos: runtime
                    .demos
                    .iter()
                    .filter(|demo| demo.time >= window_start && demo.time <= goal_time)
                    .collect(),
                touches: runtime
                    .touches
                    .iter()
                    .filter(|touch| touch.time >= window_start && touch.time <= goal_time)
                    .collect(),
            };
            Some(ConcedeDiagnosis {
                goal_index: goal.goal_index,
                scoring_team: goal.scoring_team,
                conceding_team: goal.conceding_team,
                window_start,
                window_end,
                labels: diagnose_goal(&context),
            })
        })
        .collect()
}

fn diagnose_goal(context: &GoalDiagnosisContext<'_>) -> Vec<DiagnosisLabelReport> {
    let mut labels = Vec::new();

    if let Some(label) = diagnose_kickoff_breakdown(context) {
        labels.push(label);
    }
    if let Some(label) = diagnose_failed_clear(context) {
        labels.push(label);
    }
    if let Some(label) = diagnose_demo_disruption(context) {
        labels.push(label);
    }
    if let Some(label) = diagnose_low_boost_defense(context) {
        labels.push(label);
    }
    if let Some(label) = diagnose_double_commit(context) {
        labels.push(label);
    }
    if let Some(label) = diagnose_rotation_gap(context) {
        labels.push(label);
    }
    if let Some(label) = diagnose_rebound_pressure(context) {
        labels.push(label);
    }

    labels.sort_by(|left, right| right.score.partial_cmp(&left.score).unwrap_or(Ordering::Equal));
    labels.truncate(3);
    labels
}

fn diagnose_kickoff_breakdown(context: &GoalDiagnosisContext<'_>) -> Option<DiagnosisLabelReport> {
    let kickoff_delta = context
        .previous_goal_time
        .map(|previous| context.goal_time - previous)
        .unwrap_or(context.goal_time);

    if kickoff_delta > 12.0 {
        return None;
    }

    Some(DiagnosisLabelReport {
        label: DiagnosisLabel::KickoffBreakdown,
        score: 0.95,
        evidence: vec![DiagnosisEvidence {
            timestamp: Some(context.goal_time),
            player: context.goal.scorer_name.clone(),
            team: Some(team_name(context.goal.scoring_team).to_string()),
            metric: "goal_after_kickoff_seconds".to_string(),
            value: format!("{kickoff_delta:.1}"),
            frame_context: "goal arrived before either team settled after kickoff reset".to_string(),
        }],
    })
}

fn diagnose_failed_clear(context: &GoalDiagnosisContext<'_>) -> Option<DiagnosisLabelReport> {
    let latest_touch = context
        .touches
        .iter()
        .rev()
        .find(|touch| touch.team == context.goal.conceding_team && context.goal_time - touch.time <= 3.0)?;
    let ball_in_defense = context
        .window
        .iter()
        .rev()
        .find(|snapshot| snapshot.time <= context.goal_time)
        .and_then(|snapshot| snapshot.ball_position)
        .map(|ball| is_defensive_half(context.goal.conceding_team, ball.y))
        .unwrap_or(false);

    if !ball_in_defense {
        return None;
    }

    Some(DiagnosisLabelReport {
        label: DiagnosisLabel::FailedClear,
        score: 0.82,
        evidence: vec![DiagnosisEvidence {
            timestamp: Some(latest_touch.time),
            player: None,
            team: Some(team_name(context.goal.conceding_team).to_string()),
            metric: "last_touch_before_goal_seconds".to_string(),
            value: format!("{:.1}", context.goal_time - latest_touch.time),
            frame_context: "defending team touched the ball shortly before conceding but did not exit the defensive half".to_string(),
        }],
    })
}

fn diagnose_demo_disruption(context: &GoalDiagnosisContext<'_>) -> Option<DiagnosisLabelReport> {
    let demo = context
        .demos
        .iter()
        .rev()
        .find(|demo| demo.victim_team == Some(context.goal.conceding_team) && context.goal_time - demo.time <= 5.0)?;

    Some(DiagnosisLabelReport {
        label: DiagnosisLabel::DemoDisruption,
        score: 0.78,
        evidence: vec![DiagnosisEvidence {
            timestamp: Some(demo.time),
            player: demo.victim_player.clone(),
            team: Some(team_name(context.goal.conceding_team).to_string()),
            metric: "demo_before_goal_seconds".to_string(),
            value: format!("{:.1}", context.goal_time - demo.time),
            frame_context: format!(
                "{} was removed from the play before the goal",
                demo.victim_player.clone().unwrap_or_else(|| "a defender".to_string())
            ),
        }],
    })
}

fn diagnose_low_boost_defense(context: &GoalDiagnosisContext<'_>) -> Option<DiagnosisLabelReport> {
    let recent_window: Vec<_> = context
        .window
        .iter()
        .filter(|snapshot| snapshot.time >= context.goal_time - 3.0 && snapshot.time <= context.goal_time)
        .collect();
    if recent_window.is_empty() {
        return None;
    }

    let mut boost_sum = 0.0;
    let mut samples = 0usize;
    for snapshot in recent_window {
        for player in snapshot.players.iter().filter(|player| player.team == context.goal.conceding_team) {
            if let Some(boost) = player.boost {
                boost_sum += boost;
                samples += 1;
            }
        }
    }

    if samples == 0 {
        return None;
    }

    let average_boost = boost_sum / samples as f64;
    if average_boost >= 20.0 {
        return None;
    }

    Some(DiagnosisLabelReport {
        label: DiagnosisLabel::LowBoostDefense,
        score: 0.74,
        evidence: vec![DiagnosisEvidence {
            timestamp: Some(context.goal_time),
            player: None,
            team: Some(team_name(context.goal.conceding_team).to_string()),
            metric: "average_defender_boost".to_string(),
            value: format!("{average_boost:.1}"),
            frame_context: "defending team entered the goal sequence with low average boost".to_string(),
        }],
    })
}

fn diagnose_double_commit(context: &GoalDiagnosisContext<'_>) -> Option<DiagnosisLabelReport> {
    let snapshot = context
        .window
        .iter()
        .rev()
        .find(|snapshot| snapshot.time <= context.goal_time && snapshot.time >= context.goal_time - 2.0)?;
    let ball = snapshot.ball_position?;
    if !is_defensive_half(context.goal.conceding_team, ball.y) {
        return None;
    }

    let committed: Vec<_> = snapshot
        .players
        .iter()
        .filter(|player| player.team == context.goal.conceding_team)
        .filter_map(|player| {
            let position = player.position?;
            (position.distance(ball) <= DOUBLE_COMMIT_DISTANCE).then_some(player.name.clone())
        })
        .collect();

    if committed.len() < 2 {
        return None;
    }

    Some(DiagnosisLabelReport {
        label: DiagnosisLabel::DoubleCommit,
        score: 0.68,
        evidence: vec![DiagnosisEvidence {
            timestamp: Some(snapshot.time),
            player: Some(committed.join(", ")),
            team: Some(team_name(context.goal.conceding_team).to_string()),
            metric: "defenders_near_ball".to_string(),
            value: committed.len().to_string(),
            frame_context: "multiple defenders collapsed on the same ball in the defensive half".to_string(),
        }],
    })
}

fn diagnose_rotation_gap(context: &GoalDiagnosisContext<'_>) -> Option<DiagnosisLabelReport> {
    let snapshot = context
        .window
        .iter()
        .rev()
        .find(|snapshot| snapshot.time <= context.goal_time && snapshot.time >= context.goal_time - 2.5)?;

    let defenders: Vec<_> = snapshot
        .players
        .iter()
        .filter(|player| player.team == context.goal.conceding_team)
        .filter_map(|player| player.position.map(|position| (player.name.clone(), distance_to_own_goal(player.team, position))))
        .collect();

    if defenders.is_empty() {
        return None;
    }

    let min_distance = defenders
        .iter()
        .map(|(_, distance)| *distance)
        .fold(f64::INFINITY, f64::min);
    if min_distance <= 3000.0 {
        return None;
    }

    Some(DiagnosisLabelReport {
        label: DiagnosisLabel::RotationGap,
        score: 0.72,
        evidence: vec![DiagnosisEvidence {
            timestamp: Some(snapshot.time),
            player: None,
            team: Some(team_name(context.goal.conceding_team).to_string()),
            metric: "closest_defender_to_own_goal".to_string(),
            value: format!("{min_distance:.0}"),
            frame_context: "no defender was close enough to cover the goal line during the final approach".to_string(),
        }],
    })
}

fn diagnose_rebound_pressure(context: &GoalDiagnosisContext<'_>) -> Option<DiagnosisLabelReport> {
    let relevant: Vec<_> = context
        .window
        .iter()
        .filter(|snapshot| snapshot.time >= context.goal_time - 6.0 && snapshot.time <= context.goal_time)
        .collect();
    if relevant.is_empty() {
        return None;
    }

    let mut defensive_time = 0.0;
    let mut attacking_presence = 0.0;
    for snapshot in &relevant {
        let dt = snapshot.delta.max(0.0);
        if let Some(ball) = snapshot.ball_position {
            if in_defensive_third(context.goal.conceding_team, ball.y) {
                defensive_time += dt;
            }
            let attackers_near_ball = snapshot
                .players
                .iter()
                .filter(|player| player.team == context.goal.scoring_team)
                .filter_map(|player| player.position.map(|position| position.distance(ball)))
                .filter(|distance| *distance <= PRESSURE_DISTANCE)
                .count();
            if attackers_near_ball > 0 {
                attacking_presence += dt;
            }
        }
    }

    if defensive_time < 4.0 || attacking_presence < 2.0 {
        return None;
    }

    Some(DiagnosisLabelReport {
        label: DiagnosisLabel::ReboundPressure,
        score: 0.7,
        evidence: vec![DiagnosisEvidence {
            timestamp: Some(context.goal_time),
            player: None,
            team: Some(team_name(context.goal.scoring_team).to_string()),
            metric: "sustained_pressure_seconds".to_string(),
            value: format!("{defensive_time:.1}"),
            frame_context: "the ball stayed in the defending third under sustained attacking pressure".to_string(),
        }],
    })
}

fn resolve_team_scores(runtime: &MatchRuntime, goals: &[HeaderGoal]) -> ScoreLine {
    let blue = runtime
        .team_scores
        .get(&0)
        .copied()
        .unwrap_or_else(|| goals.iter().filter(|goal| goal.scoring_team == 0).count() as i32);
    let orange = runtime
        .team_scores
        .get(&1)
        .copied()
        .unwrap_or_else(|| goals.iter().filter(|goal| goal.scoring_team == 1).count() as i32);

    ScoreLine {
        blue: blue.max(0) as u32,
        orange: orange.max(0) as u32,
    }
}

fn winner_from_score(score: &ScoreLine) -> Option<String> {
    match score.blue.cmp(&score.orange) {
        Ordering::Greater => Some("blue".to_string()),
        Ordering::Less => Some("orange".to_string()),
        Ordering::Equal => None,
    }
}

fn insert_count_metric(metrics: &mut BTreeMap<String, MetricValue>, name: &str, value: Option<i32>) {
    let metric = match value {
        Some(value) => MetricValue::exact(value as f64),
        None => MetricValue::unavailable("header or PRI stat unavailable"),
    };
    metrics.insert(name.to_string(), metric);
}

fn insert_derived_metrics(
    metrics: &mut BTreeMap<String, MetricValue>,
    derived: &DerivedAccumulator,
    parse_quality: ParseQuality,
    duration: f64,
    is_team: bool,
) {
    if !matches!(parse_quality, ParseQuality::Full) {
        for metric_name in derived_metric_names() {
            metrics.insert(
                metric_name.to_string(),
                MetricValue::unavailable("network frames unavailable"),
            );
        }
        return;
    }

    let note = "derived from network frame sampling";
    metrics.insert(
        "avg_speed".to_string(),
        maybe_estimated(derived.speed_samples > 0.0, derived.speed_time_sum / derived.speed_samples, note),
    );
    metrics.insert(
        "supersonic_time".to_string(),
        maybe_estimated(derived.sample_time > 0.0, derived.supersonic_time, note),
    );
    metrics.insert(
        "distance_traveled".to_string(),
        maybe_estimated(derived.sample_time > 0.0, derived.distance_traveled, note),
    );
    metrics.insert(
        "avg_boost".to_string(),
        maybe_estimated(derived.boost_time > 0.0, derived.boost_sum / derived.boost_time, note),
    );
    metrics.insert(
        "low_boost_time".to_string(),
        maybe_estimated(derived.boost_time > 0.0, derived.low_boost_time, note),
    );
    metrics.insert(
        "empty_boost_time".to_string(),
        maybe_estimated(derived.boost_time > 0.0, derived.empty_boost_time, note),
    );
    metrics.insert(
        "boost_spent".to_string(),
        maybe_estimated(derived.boost_time > 0.0, derived.boost_spent, note),
    );
    metrics.insert(
        "boost_gained".to_string(),
        maybe_estimated(derived.boost_time > 0.0, derived.boost_gained, note),
    );
    metrics.insert(
        "offensive_half_time".to_string(),
        maybe_estimated(derived.sample_time > 0.0, derived.offensive_half_time, note),
    );
    metrics.insert(
        "defensive_half_time".to_string(),
        maybe_estimated(derived.sample_time > 0.0, derived.defensive_half_time, note),
    );
    let ratio_denominator = if is_team { duration.max(1.0) } else { duration.max(1.0) };
    metrics.insert(
        "closest_to_ball_proxy".to_string(),
        maybe_estimated(duration > 0.0, derived.closest_to_ball_time / ratio_denominator, note),
    );
    metrics.insert(
        "pressure_proxy".to_string(),
        maybe_estimated(duration > 0.0, derived.pressure_time / ratio_denominator, note),
    );
}

fn maybe_estimated(available: bool, value: f64, note: &str) -> MetricValue {
    if available {
        MetricValue::estimated(value, note)
    } else {
        MetricValue::unavailable(note)
    }
}

fn team_name(team: u8) -> &'static str {
    if team == 0 { "Blue" } else { "Orange" }
}

fn is_offensive_half(team: u8, y: f64) -> bool {
    if team == 0 { y > 0.0 } else { y < 0.0 }
}

fn is_defensive_half(team: u8, y: f64) -> bool {
    !is_offensive_half(team, y)
}

fn in_defensive_third(team: u8, y: f64) -> bool {
    if team == 0 {
        y <= -DEFENSIVE_THIRD_Y
    } else {
        y >= DEFENSIVE_THIRD_Y
    }
}

fn distance_to_own_goal(team: u8, position: Vec3) -> f64 {
    let own_goal_y = if team == 0 { -OWN_GOAL_Y } else { OWN_GOAL_Y };
    Vec3 {
        x: position.x,
        y: position.y - own_goal_y,
        z: position.z,
    }
    .magnitude()
}

fn team_aggregate_actor_id(team: u8) -> i32 {
    100_000 + i32::from(team)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricBehavior {
    Sum,
    Average,
}

fn metric_names() -> &'static [&'static str] {
    &[
        "score",
        "goals",
        "assists",
        "shots",
        "saves",
        "demos",
        "self_demos",
        "avg_speed",
        "supersonic_time",
        "distance_traveled",
        "avg_boost",
        "low_boost_time",
        "empty_boost_time",
        "boost_spent",
        "boost_gained",
        "offensive_half_time",
        "defensive_half_time",
        "closest_to_ball_proxy",
        "pressure_proxy",
    ]
}

fn derived_metric_names() -> &'static [&'static str] {
    &[
        "avg_speed",
        "supersonic_time",
        "distance_traveled",
        "avg_boost",
        "low_boost_time",
        "empty_boost_time",
        "boost_spent",
        "boost_gained",
        "offensive_half_time",
        "defensive_half_time",
        "closest_to_ball_proxy",
        "pressure_proxy",
    ]
}

fn metric_behavior(metric: &str) -> MetricBehavior {
    match metric {
        "avg_speed" | "avg_boost" | "closest_to_ball_proxy" | "pressure_proxy" => MetricBehavior::Average,
        _ => MetricBehavior::Sum,
    }
}

fn aggregate_metric_values(
    values: Vec<&MetricValue>,
    behavior: MetricBehavior,
    _duration: f64,
) -> MetricValue {
    if values.is_empty() {
        return MetricValue::unavailable("metric unavailable");
    }

    let mut state = MetricAggregateState::default();
    for value in values {
        match value.quality {
            MetricQuality::Unavailable => continue,
            MetricQuality::Exact => state.exact_only &= true,
            MetricQuality::Estimated => {
                state.exact_only = false;
                state.has_estimated = true;
            }
        }
        if let Some(number) = value.value {
            state.sum += number;
            state.weight += 1.0;
            state.count += 1;
        }
    }

    if state.count == 0 {
        return MetricValue::unavailable("metric unavailable");
    }

    let aggregated_value = match behavior {
        MetricBehavior::Sum => state.sum,
        MetricBehavior::Average => state.sum / state.weight.max(1.0),
    };

    if state.exact_only && !state.has_estimated {
        MetricValue::exact(aggregated_value)
    } else {
        MetricValue::estimated(aggregated_value, "aggregated across analyzed matches")
    }
}

pub fn build_batch_summary(reports: Vec<AnalysisReport>) -> BatchSummary {
    let mut summary = BatchSummary::empty();
    summary.loaded_reports = reports.clone();
    summary.matches = reports
        .iter()
        .map(|report| MatchManifest {
            replay_id: report.meta.replay_id.clone(),
            date: report.meta.date.clone(),
            map: report.meta.map.clone(),
            winner: report.meta.winner.clone(),
            final_score: report.meta.final_score.clone(),
            parse_quality: report.availability.parse_quality,
            diagnosis_count: report.concede_diagnoses.iter().map(|diagnosis| diagnosis.labels.len()).sum(),
            report_path: String::new(),
        })
        .collect();
    summary.warnings = reports.iter().flat_map(|report| report.warnings.clone()).collect();
    summary.team_aggregate = aggregate_teams(&reports);
    summary.player_aggregate = aggregate_players(&reports);
    summary
}

fn aggregate_teams(reports: &[AnalysisReport]) -> Vec<TeamMetricsReport> {
    let mut grouped: BTreeMap<u8, Vec<&TeamMetricsReport>> = BTreeMap::new();
    let mut wins: HashMap<u8, usize> = HashMap::new();

    for report in reports {
        if let Some(winner) = &report.meta.winner {
            let team = if winner == "blue" { 0 } else { 1 };
            *wins.entry(team).or_default() += 1;
        }
        for team in &report.team_metrics {
            grouped.entry(team.team).or_default().push(team);
        }
    }

    grouped
        .into_iter()
        .map(|(team, reports)| TeamMetricsReport {
            team,
            name: team_name(team).to_string(),
            matches: Some(reports.len()),
            wins: Some(wins.get(&team).copied().unwrap_or(0)),
            metrics: metric_names()
                .iter()
                .map(|metric| {
                    let values = reports
                        .iter()
                        .filter_map(|report| report.metrics.get(*metric))
                        .collect();
                    (
                        (*metric).to_string(),
                        aggregate_metric_values(values, metric_behavior(metric), 0.0),
                    )
                })
                .collect(),
        })
        .collect()
}

fn aggregate_players(reports: &[AnalysisReport]) -> Vec<PlayerMetricsReport> {
    let mut grouped: BTreeMap<String, Vec<&PlayerMetricsReport>> = BTreeMap::new();
    let mut wins: HashMap<String, usize> = HashMap::new();

    for report in reports {
        for player in &report.player_metrics {
            grouped.entry(player.player_name.clone()).or_default().push(player);
            if let Some(winner) = &report.meta.winner {
                let team = if winner == "blue" { 0 } else { 1 };
                if player.team == team {
                    *wins.entry(player.player_name.clone()).or_default() += 1;
                }
            }
        }
    }

    grouped
        .into_iter()
        .map(|(player_name, reports)| {
            let team = reports.first().map(|report| report.team).unwrap_or(0);
            PlayerMetricsReport {
                player_name: player_name.clone(),
                team,
                matches: Some(reports.len()),
                wins: Some(wins.get(&player_name).copied().unwrap_or(0)),
                unique_id: reports.iter().find_map(|report| report.unique_id.clone()),
                metrics: metric_names()
                    .iter()
                    .map(|metric| {
                        let values = reports
                            .iter()
                            .filter_map(|report| report.metrics.get(*metric))
                            .collect();
                        (
                            (*metric).to_string(),
                            aggregate_metric_values(values, metric_behavior(metric), 0.0),
                        )
                    })
                    .collect(),
            }
        })
        .collect()
}

pub fn write_report(path: &Path, report: &AnalysisReport, pretty: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }

    let payload = if pretty {
        serde_json::to_vec_pretty(report).context("failed to serialize report")?
    } else {
        serde_json::to_vec(report).context("failed to serialize report")?
    };
    fs::write(path, payload).with_context(|| format!("failed to write {}", path.display()))
}

pub fn write_summary(path: &Path, summary: &BatchSummary, pretty: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }

    let payload = if pretty {
        serde_json::to_vec_pretty(summary).context("failed to serialize batch summary")?
    } else {
        serde_json::to_vec(summary).context("failed to serialize batch summary")?
    };
    fs::write(path, payload).with_context(|| format!("failed to write {}", path.display()))
}

pub fn scan_json_files(input: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_json_files(input, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_json_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type for {}", path.display()))?;
        if file_type.is_dir() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name == "analysis")
                .unwrap_or(false)
            {
                continue;
            }
            collect_json_files(&path, files)?;
        } else if file_type.is_file()
            && path.extension().and_then(|ext| ext.to_str()) == Some("json")
        {
            files.push(path);
        }
    }
    Ok(())
}

pub fn report_output_path(output_dir: &Path, input_file: &Path, report: &AnalysisReport) -> PathBuf {
    let file_name = input_file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or(&report.meta.replay_id);
    output_dir
        .join("analysis")
        .join(&report.meta.date)
        .join(format!("{file_name}.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn player(
        player_actor: i32,
        name: &str,
        team: u8,
        position: Vec3,
        boost: Option<f64>,
    ) -> PlayerFrameState {
        PlayerFrameState {
            player_actor,
            name: name.to_string(),
            team,
            position: Some(position),
            speed: Some(1000.0),
            boost,
        }
    }

    fn goal() -> GoalReport {
        GoalReport {
            goal_index: 0,
            frame: Some(10),
            time: Some(10.0),
            scorer_name: Some("OrangeOne".to_string()),
            scoring_team: 1,
            conceding_team: 0,
        }
    }

    fn base_context<'a>(
        goal: &'a GoalReport,
        snapshots: &'a [FrameSnapshot],
        demos: &'a [DemoEvent],
        touches: &'a [TouchEvent],
    ) -> GoalDiagnosisContext<'a> {
        GoalDiagnosisContext {
            goal,
            goal_time: 10.0,
            previous_goal_time: Some(0.0),
            window: snapshots.iter().collect(),
            demos: demos.iter().collect(),
            touches: touches.iter().collect(),
        }
    }

    #[test]
    fn diagnosis_kickoff_breakdown_triggers() {
        let goal = goal();
        let snapshots = vec![];
        let demos = vec![];
        let touches = vec![];
        let context = base_context(&goal, &snapshots, &demos, &touches);
        assert!(diagnose_kickoff_breakdown(&context).is_some());
    }

    #[test]
    fn diagnosis_failed_clear_triggers() {
        let goal = goal();
        let snapshots = vec![FrameSnapshot {
            time: 10.0,
            delta: 1.0,
            ball_position: Some(Vec3 { x: 0.0, y: -5000.0, z: 0.0 }),
            players: vec![player(1, "BlueOne", 0, Vec3 { x: 0.0, y: -4500.0, z: 0.0 }, Some(10.0))],
        }];
        let demos = vec![];
        let touches = vec![TouchEvent { time: 9.5, team: 0 }];
        let context = base_context(&goal, &snapshots, &demos, &touches);
        assert!(diagnose_failed_clear(&context).is_some());
    }

    #[test]
    fn diagnosis_demo_disruption_triggers() {
        let goal = goal();
        let snapshots = vec![];
        let demos = vec![DemoEvent {
            time: 7.0,
            _attacker_car: Some(2),
            _victim_car: Some(1),
            _attacker_player: Some("OrangeOne".to_string()),
            victim_player: Some("BlueOne".to_string()),
            _attacker_team: Some(1),
            victim_team: Some(0),
        }];
        let touches = vec![];
        let context = base_context(&goal, &snapshots, &demos, &touches);
        assert!(diagnose_demo_disruption(&context).is_some());
    }

    #[test]
    fn diagnosis_low_boost_defense_triggers() {
        let goal = goal();
        let snapshots = vec![
            FrameSnapshot {
                time: 8.0,
                delta: 1.0,
                ball_position: Some(Vec3 { x: 0.0, y: -4300.0, z: 0.0 }),
                players: vec![player(1, "BlueOne", 0, Vec3 { x: 0.0, y: -4200.0, z: 0.0 }, Some(8.0))],
            },
            FrameSnapshot {
                time: 10.0,
                delta: 1.0,
                ball_position: Some(Vec3 { x: 0.0, y: -5100.0, z: 0.0 }),
                players: vec![player(1, "BlueOne", 0, Vec3 { x: 0.0, y: -5000.0, z: 0.0 }, Some(6.0))],
            },
        ];
        let demos = vec![];
        let touches = vec![];
        let context = base_context(&goal, &snapshots, &demos, &touches);
        assert!(diagnose_low_boost_defense(&context).is_some());
    }

    #[test]
    fn diagnosis_double_commit_triggers() {
        let goal = goal();
        let snapshots = vec![FrameSnapshot {
            time: 9.5,
            delta: 1.0,
            ball_position: Some(Vec3 { x: 0.0, y: -4800.0, z: 0.0 }),
            players: vec![
                player(1, "BlueOne", 0, Vec3 { x: 100.0, y: -4700.0, z: 0.0 }, Some(20.0)),
                player(2, "BlueTwo", 0, Vec3 { x: -100.0, y: -4700.0, z: 0.0 }, Some(30.0)),
            ],
        }];
        let demos = vec![];
        let touches = vec![];
        let context = base_context(&goal, &snapshots, &demos, &touches);
        assert!(diagnose_double_commit(&context).is_some());
    }

    #[test]
    fn diagnosis_rotation_gap_triggers() {
        let goal = goal();
        let snapshots = vec![FrameSnapshot {
            time: 9.0,
            delta: 1.0,
            ball_position: Some(Vec3 { x: 0.0, y: -3000.0, z: 0.0 }),
            players: vec![player(1, "BlueOne", 0, Vec3 { x: 0.0, y: 1000.0, z: 0.0 }, Some(40.0))],
        }];
        let demos = vec![];
        let touches = vec![];
        let context = base_context(&goal, &snapshots, &demos, &touches);
        assert!(diagnose_rotation_gap(&context).is_some());
    }

    #[test]
    fn diagnosis_rebound_pressure_triggers() {
        let goal = goal();
        let snapshots = vec![
            FrameSnapshot {
                time: 5.0,
                delta: 2.0,
                ball_position: Some(Vec3 { x: 0.0, y: -4200.0, z: 0.0 }),
                players: vec![player(3, "OrangeOne", 1, Vec3 { x: 0.0, y: -3500.0, z: 0.0 }, Some(50.0))],
            },
            FrameSnapshot {
                time: 8.0,
                delta: 2.0,
                ball_position: Some(Vec3 { x: 0.0, y: -5000.0, z: 0.0 }),
                players: vec![player(3, "OrangeOne", 1, Vec3 { x: 0.0, y: -4100.0, z: 0.0 }, Some(50.0))],
            },
            FrameSnapshot {
                time: 10.0,
                delta: 1.0,
                ball_position: Some(Vec3 { x: 0.0, y: -5100.0, z: 0.0 }),
                players: vec![player(3, "OrangeOne", 1, Vec3 { x: 0.0, y: -4300.0, z: 0.0 }, Some(50.0))],
            },
        ];
        let demos = vec![];
        let touches = vec![];
        let context = base_context(&goal, &snapshots, &demos, &touches);
        assert!(diagnose_rebound_pressure(&context).is_some());
    }
}
