use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use serde_json::Value;

use uuid::Uuid;

use crate::persistence::query_events;
use yarli_core::domain::{
    CancellationActorKind, CancellationProvenance, CancellationSource, CancellationStage,
    EntityType, Event,
};
use yarli_core::entities::command_execution::{CommandResourceUsage, TokenUsage};
use yarli_core::entities::merge_intent::MergeIntent;
use yarli_core::entities::worktree_binding::{SubmoduleMode, WorktreeBinding};
use yarli_core::explain::DeteriorationReport;
use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_core::fsm::worktree::WorktreeState;
use yarli_store::event_store::EventQuery;
use yarli_store::EventStore;

use crate::observers::MemoryHintsReport;
use crate::plan::{PlannedTranche, RunTokenTotals};
use crate::GateType;

#[derive(Debug, Clone)]
pub(crate) struct TaskProjection {
    pub(crate) task_id: Uuid,
    pub(crate) task_key: Option<String>,
    pub(crate) state: TaskState,
    pub(crate) correlation_id: Uuid,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
    pub(crate) last_event_id: Uuid,
    pub(crate) last_event_type: String,
    pub(crate) reason: Option<String>,
    pub(crate) attempt_no: Option<u32>,
    pub(crate) failed_gates: Vec<(GateType, String)>,
    pub(crate) resource_usage: Option<CommandResourceUsage>,
    pub(crate) token_usage: Option<TokenUsage>,
    pub(crate) budget_breach_reason: Option<String>,
    pub(crate) memory_hints: Option<MemoryHintsReport>,
    pub(crate) last_error: Option<String>,
    pub(crate) blocker_detail: Option<String>,
    pub(crate) worker_actor: Option<String>,
    pub(crate) workspace_dir: Option<String>,
    pub(crate) tranche_key: Option<String>,
    pub(crate) tranche_group: Option<String>,
    pub(crate) allowed_paths: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RunProjection {
    pub(crate) run_id: Uuid,
    pub(crate) state: RunState,
    pub(crate) exit_reason: Option<String>,
    pub(crate) cancellation_source: Option<CancellationSource>,
    pub(crate) cancellation_provenance: Option<CancellationProvenance>,
    pub(crate) correlation_id: Uuid,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
    pub(crate) last_event_type: String,
    pub(crate) objective: Option<String>,
    pub(crate) tasks: Vec<TaskProjection>,
    pub(crate) failed_gates: Vec<(GateType, String)>,
    pub(crate) merge_finalization_blockers: Vec<String>,
    pub(crate) deterioration: Option<DeteriorationReport>,
    pub(crate) memory_hints: Option<MemoryHintsReport>,
    pub(crate) tranche_plan: Vec<PlannedTranche>,
}

#[derive(Debug, Clone)]
pub(crate) struct EntityProjection {
    pub(crate) entity_id: String,
    pub(crate) state: String,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
    pub(crate) last_event_type: String,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct WorktreeProjection {
    pub(crate) worktree_id: Uuid,
    pub(crate) state: WorktreeState,
    pub(crate) correlation_id: Uuid,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
    pub(crate) last_event_id: Uuid,
    pub(crate) last_event_type: String,
    pub(crate) reason: Option<String>,
    pub(crate) run_id: Option<Uuid>,
    pub(crate) task_id: Option<Uuid>,
    pub(crate) repo_root: Option<PathBuf>,
    pub(crate) worktree_path: Option<PathBuf>,
    pub(crate) branch_name: Option<String>,
    pub(crate) base_ref: Option<String>,
    pub(crate) head_ref: Option<String>,
    pub(crate) submodule_mode: Option<SubmoduleMode>,
}

#[derive(Debug, Clone)]
pub(crate) struct MergeProjection {
    pub(crate) merge_id: Uuid,
    pub(crate) state: MergeState,
    pub(crate) run_id: Option<Uuid>,
    pub(crate) worktree_id: Option<Uuid>,
    pub(crate) correlation_id: Uuid,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
    pub(crate) last_event_id: Uuid,
    pub(crate) last_event_type: String,
    pub(crate) reason: Option<String>,
    pub(crate) source_ref: Option<String>,
    pub(crate) target_ref: Option<String>,
    pub(crate) strategy: Option<String>,
}

pub(crate) fn collect_run_token_totals(
    store: &dyn EventStore,
    correlation_id: Uuid,
) -> Result<RunTokenTotals> {
    let events = query_events(store, &EventQuery::by_correlation(correlation_id))?;
    let mut totals = RunTokenTotals::default();
    let mut seen_command_ids: HashSet<String> = HashSet::new();

    for event in events {
        if event.entity_type != EntityType::Command {
            continue;
        }
        if !matches!(
            event.event_type.as_str(),
            "command.exited" | "command.timed_out" | "command.killed" | "command.completed"
        ) {
            continue;
        }
        if !seen_command_ids.insert(event.entity_id.clone()) {
            continue;
        }

        let Some(raw_usage) = event.payload.get("token_usage").cloned() else {
            continue;
        };
        let Ok(usage) = serde_json::from_value::<TokenUsage>(raw_usage) else {
            continue;
        };
        totals.prompt_tokens = totals.prompt_tokens.saturating_add(usage.prompt_tokens);
        totals.completion_tokens = totals
            .completion_tokens
            .saturating_add(usage.completion_tokens);
        totals.total_tokens = totals.total_tokens.saturating_add(usage.total_tokens);
    }

    Ok(totals)
}

pub(crate) fn run_state_from_event(event: &Event) -> Option<RunState> {
        event
            .payload
            .get("to")
            .and_then(|v| v.as_str())
            .and_then(crate::parse_run_state)
        .or(match event.event_type.as_str() {
            "run.activated" => Some(RunState::RunActive),
            "run.blocked" => Some(RunState::RunBlocked),
            "run.verifying" => Some(RunState::RunVerifying),
            "run.completed" => Some(RunState::RunCompleted),
            "run.parallel_merge_failed" => Some(RunState::RunFailed),
            "run.failed" | "run.gate_failed" => Some(RunState::RunFailed),
            "run.cancelled" => Some(RunState::RunCancelled),
            _ => None,
        })
}

pub(crate) fn task_state_from_event(event: &Event) -> Option<TaskState> {
    event
        .payload
        .get("to")
        .and_then(|v| v.as_str())
        .and_then(crate::parse_task_state)
        .or(match event.event_type.as_str() {
            "task.ready" | "task.retrying" | "task.unblocked" => Some(TaskState::TaskReady),
            "task.executing" => Some(TaskState::TaskExecuting),
            "task.verifying" => Some(TaskState::TaskVerifying),
            "task.completed" => Some(TaskState::TaskComplete),
            "task.failed" | "task.gate_failed" => Some(TaskState::TaskFailed),
            "task.blocked" => Some(TaskState::TaskBlocked),
            "task.cancelled" => Some(TaskState::TaskCancelled),
            _ => None,
        })
}

pub(crate) fn event_reason(event: &Event) -> Option<String> {
    event
        .payload
        .get("reason")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn event_string_list(payload: Option<&Value>, key: &str) -> Vec<String> {
    let Some(values) = payload.and_then(|value| value.get(key)).and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut entries = values
        .iter()
        .filter_map(|entry| entry.as_str().map(ToString::to_string))
        .collect::<Vec<_>>();
    entries.sort_unstable();
    entries.dedup();
    entries
}

pub(crate) fn parse_cancellation_source(raw: &str) -> Option<CancellationSource> {
    match raw {
        "operator" => Some(CancellationSource::Operator),
        "sigint" => Some(CancellationSource::Sigint),
        "sigterm" => Some(CancellationSource::Sigterm),
        "sw4rm_preemption" => Some(CancellationSource::Sw4rmPreemption),
        "unknown" => Some(CancellationSource::Unknown),
        _ => None,
    }
}

pub(crate) fn parse_cancellation_actor_kind(raw: &str) -> Option<CancellationActorKind> {
    match raw {
        "operator" => Some(CancellationActorKind::Operator),
        "system" => Some(CancellationActorKind::System),
        "supervisor" => Some(CancellationActorKind::Supervisor),
        "unknown" => Some(CancellationActorKind::Unknown),
        _ => None,
    }
}

pub(crate) fn parse_cancellation_stage(raw: &str) -> Option<CancellationStage> {
    match raw {
        "executing" => Some(CancellationStage::Executing),
        "retrying" => Some(CancellationStage::Retrying),
        "verifying" => Some(CancellationStage::Verifying),
        "unknown" => Some(CancellationStage::Unknown),
        _ => None,
    }
}

pub(crate) fn event_exit_reason(event: &Event) -> Option<String> {
    event
        .payload
        .get("exit_reason")
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
}

pub(crate) fn event_cancellation_source(event: &Event) -> Option<CancellationSource> {
    event
        .payload
        .get("cancellation_source")
        .and_then(|v| v.as_str())
        .and_then(parse_cancellation_source)
}

pub(crate) fn event_cancellation_provenance(event: &Event) -> Option<CancellationProvenance> {
    if let Some(value) = event.payload.get("cancellation_provenance") {
        if let Ok(parsed) = serde_json::from_value::<CancellationProvenance>(value.clone()) {
            return Some(parsed);
        }
    }

    if event.event_type == "run.cancel_provenance" {
        let source = event
            .payload
            .get("cancellation_source")
            .and_then(|v| v.as_str())
            .and_then(parse_cancellation_source)
            .unwrap_or(CancellationSource::Unknown);
        let actor_kind = event
            .payload
            .get("actor_kind")
            .and_then(|v| v.as_str())
            .and_then(parse_cancellation_actor_kind);
        let stage = event
            .payload
            .get("stage")
            .and_then(|v| v.as_str())
            .and_then(parse_cancellation_stage);
        return Some(CancellationProvenance {
            cancellation_source: source,
            signal_name: event
                .payload
                .get("signal_name")
                .and_then(|v| v.as_str())
                .map(ToString::to_string),
            signal_number: event
                .payload
                .get("signal_number")
                .and_then(|v| v.as_i64())
                .and_then(|v| i32::try_from(v).ok()),
            sender_pid: event
                .payload
                .get("sender_pid")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            receiver_pid: event
                .payload
                .get("receiver_pid")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            parent_pid: event
                .payload
                .get("parent_pid")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            process_group_id: event
                .payload
                .get("process_group_id")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            session_id: event
                .payload
                .get("session_id")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            tty: event
                .payload
                .get("tty")
                .and_then(|v| v.as_str())
                .map(ToString::to_string),
            actor_kind,
            actor_detail: event
                .payload
                .get("actor_detail")
                .and_then(|v| v.as_str())
                .map(ToString::to_string),
            stage,
        });
    }

    None
}

pub(crate) fn extract_entity_state(event: &Event) -> String {
    for key in ["to", "state", "status"] {
        if let Some(value) = event.payload.get(key).and_then(|v| v.as_str()) {
            return value.to_string();
        }
    }
    event.event_type.clone()
}

pub(crate) fn parse_gate_failure_entry(entry: &str) -> Option<(GateType, String)> {
    let (raw_gate, raw_reason) = entry.split_once(':')?;
    let gate_name = raw_gate
        .trim()
        .strip_prefix("gate.")
        .unwrap_or(raw_gate.trim());
    let gate_type = crate::parse_gate_type(gate_name)?;
    Some((gate_type, raw_reason.trim().to_string()))
}

pub(crate) fn gate_failures_from_event(event: &Event) -> Vec<(GateType, String)> {
    let mut failures = Vec::new();

    if let Some(items) = event.payload.get("failures").and_then(|v| v.as_array()) {
        for item in items {
            if let Some(value) = item.as_str().and_then(parse_gate_failure_entry) {
                failures.push(value);
            }
        }
    }

    if failures.is_empty() {
        if let Some(reason) = event.payload.get("reason").and_then(|v| v.as_str()) {
            if let Some(value) = parse_gate_failure_entry(reason) {
                failures.push(value);
            }
        }
    }

    failures
}

pub(crate) fn deterioration_from_event(event: &Event) -> Option<DeteriorationReport> {
    if event.event_type != "run.observer.deterioration" {
        return None;
    }
    serde_json::from_value(event.payload.clone()).ok()
}

#[derive(Debug, Clone)]
pub(crate) struct TaskCatalogProjection {
    pub(crate) task_key: Option<String>,
    pub(crate) workspace_dir: Option<String>,
    pub(crate) tranche_key: Option<String>,
    pub(crate) tranche_group: Option<String>,
    pub(crate) allowed_paths: Vec<String>,
}

pub(crate) fn task_catalog_entries_from_event(
    event: &Event,
) -> HashMap<Uuid, TaskCatalogProjection> {
    if event.event_type != "run.task_catalog" {
        return HashMap::new();
    }
    event
        .payload
        .get("tasks")
        .and_then(|value| value.as_array())
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| {
                    let task_id = task
                        .get("task_id")
                        .and_then(|value| value.as_str())
                        .and_then(|raw| raw.parse::<Uuid>().ok())?;
                    let task_key = task
                        .get("task_key")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string)
                        .filter(|value| !value.trim().is_empty());
                    let workspace_dir = task
                        .get("workspace_dir")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string)
                        .filter(|value| !value.trim().is_empty());
                    let tranche_key = task
                        .get("tranche_key")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string)
                        .filter(|value| !value.trim().is_empty());
                    let tranche_group = task
                        .get("tranche_group")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string)
                        .filter(|value| !value.trim().is_empty());
                    let allowed_paths = task
                        .get("allowed_paths")
                        .and_then(|value| value.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.as_str().map(ToString::to_string))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    Some((
                        task_id,
                        TaskCatalogProjection {
                            task_key,
                            workspace_dir,
                            tranche_key,
                            tranche_group,
                            allowed_paths,
                        },
                    ))
                })
                .collect::<HashMap<Uuid, TaskCatalogProjection>>()
        })
        .unwrap_or_default()
}

pub(crate) fn collect_task_projections(events: &[Event]) -> Vec<TaskProjection> {
    let mut tasks: BTreeMap<Uuid, TaskProjection> = BTreeMap::new();

    for event in events {
        let task_id = match event.entity_id.parse::<Uuid>() {
            Ok(id) => id,
            Err(_) => continue,
        };

        let entry = tasks.entry(task_id).or_insert_with(|| TaskProjection {
            task_id,
            task_key: None,
            state: TaskState::TaskOpen,
            correlation_id: event.correlation_id,
            updated_at: event.occurred_at,
            last_event_id: event.event_id,
            last_event_type: event.event_type.clone(),
            reason: None,
            attempt_no: None,
            failed_gates: Vec::new(),
            resource_usage: None,
            token_usage: None,
            budget_breach_reason: None,
            memory_hints: None,
            last_error: None,
            blocker_detail: None,
            worker_actor: None,
            workspace_dir: None,
            tranche_key: None,
            tranche_group: None,
            allowed_paths: Vec::new(),
        });

        entry.correlation_id = event.correlation_id;
        entry.updated_at = event.occurred_at;
        entry.last_event_id = event.event_id;
        entry.last_event_type = event.event_type.clone();

        if let Some(next_state) = task_state_from_event(event) {
            entry.state = next_state;
        }

        if let Some(task_key) = event.payload.get("task_key").and_then(|v| v.as_str()) {
            entry.task_key = Some(task_key.to_string());
        }
        if let Some(worker_id) = event.payload.get("worker").and_then(|v| v.as_str()) {
            entry.worker_actor = Some(worker_id.to_string());
        } else if matches!(
            event.event_type.as_str(),
            "task.executing" | "task.ready" | "task.retrying"
        ) {
            entry.worker_actor = Some(event.actor.clone());
        }

        if let Some(reason) = event_reason(event) {
            entry.reason = Some(reason);
        }

        if let Some(attempt_no) = event
            .payload
            .get("attempt_no")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
        {
            entry.attempt_no = Some(attempt_no);
        }

        if event.event_type == "task.gate_failed" {
            entry.failed_gates = gate_failures_from_event(event);
        } else if matches!(
            entry.state,
            TaskState::TaskReady
                | TaskState::TaskExecuting
                | TaskState::TaskVerifying
                | TaskState::TaskComplete
                | TaskState::TaskCancelled
        ) {
            entry.failed_gates.clear();
        }

        // Extract resource and token usage from command/task events.
        if let Some(ru) = event
            .payload
            .get("resource_usage")
            .or_else(|| event.payload.get("command_resource_usage"))
        {
            if let Ok(usage) = serde_json::from_value::<CommandResourceUsage>(ru.clone()) {
                entry.resource_usage = Some(usage);
            }
        }
        if let Some(tu) = event
            .payload
            .get("token_usage")
            .or_else(|| event.payload.get("command_token_usage"))
        {
            if let Ok(usage) = serde_json::from_value::<TokenUsage>(tu.clone()) {
                entry.token_usage = Some(usage);
            }
        }

        // Detect budget breach reason.
        if event.payload.get("reason").and_then(|v| v.as_str()) == Some("budget_exceeded") {
            let detail = event
                .payload
                .get("detail")
                .and_then(|v| v.as_str())
                .unwrap_or("budget_exceeded");
            entry.budget_breach_reason = Some(detail.to_string());
        }

        if event.event_type == "task.observer.memory_hints" {
            if let Ok(report) = serde_json::from_value::<MemoryHintsReport>(event.payload.clone()) {
                entry.memory_hints = Some(report);
            }
        }

        // Extract last_error from failure events (preserve first occurrence).
        if (event.event_type == "task.failed" || event.event_type == "task.gate_failed")
            && entry.last_error.is_none()
        {
            let error_msg = event
                .payload
                .get("detail")
                .and_then(|v| v.as_str())
                .or_else(|| event.payload.get("reason").and_then(|v| v.as_str()));
            if let Some(msg) = error_msg {
                entry.last_error = Some(msg.to_string());
            }
        }

        // Extract blocker_detail from annotate events.
        if event.event_type == "task.annotated" {
            entry.blocker_detail = event
                .payload
                .get("blocker_detail")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
    }

    let mut values: Vec<_> = tasks.into_values().collect();
    values.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.task_id.cmp(&b.task_id))
    });
    values
}

pub(crate) fn load_task_projection(
    store: &dyn EventStore,
    task_id: Uuid,
) -> Result<Option<TaskProjection>> {
    let task_events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Task, task_id.to_string()),
    )?;
    if task_events.is_empty() {
        return Ok(None);
    }

    let projections = collect_task_projections(&task_events);
    Ok(projections.into_iter().next())
}

pub(crate) fn load_run_projection(
    store: &dyn EventStore,
    run_id: Uuid,
) -> Result<Option<RunProjection>> {
    let run_events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Run, run_id.to_string()),
    )?;
    if run_events.is_empty() {
        return Ok(None);
    }

    let mut state = RunState::RunOpen;
    let mut objective = None;
    let mut exit_reason = None;
    let mut cancellation_source = None;
    let mut cancellation_provenance = None;
    let mut correlation_id = run_events[0].correlation_id;
    let mut updated_at = run_events[0].occurred_at;
    let mut last_event_type = run_events[0].event_type.clone();
    let mut failed_gates = Vec::new();
    let mut merge_finalization_blockers = Vec::new();
    let mut deterioration = None;
    let mut memory_hints = None;
    let mut tranche_plan = Vec::new();
    let mut task_catalog_by_id: HashMap<Uuid, TaskCatalogProjection> = HashMap::new();

    for event in &run_events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = run_state_from_event(event) {
            state = next_state;
        }
        if let Some(next_exit_reason) = event_exit_reason(event) {
            exit_reason = Some(next_exit_reason);
        }
        if let Some(next_source) = event_cancellation_source(event) {
            cancellation_source = Some(next_source);
        }
        if let Some(next_provenance) = event_cancellation_provenance(event) {
            cancellation_provenance = Some(next_provenance);
        }

        if event.event_type == "run.config_snapshot" {
            objective = event
                .payload
                .get("objective")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(config_snapshot) = event.payload.get("config_snapshot") {
                tranche_plan = crate::plan::parse_tranche_plan_from_snapshot(config_snapshot);
            }
        }

        if event.event_type == "run.task_catalog" {
            task_catalog_by_id = task_catalog_entries_from_event(event);
        }

        if event.event_type == "run.gate_failed" {
            failed_gates = gate_failures_from_event(event);
        } else if matches!(
            event.event_type.as_str(),
            "run.activated" | "run.verifying" | "run.completed" | "run.cancelled"
        ) {
            failed_gates.clear();
        } else if event.event_type == "run.parallel_merge_failed" {
            let reason = event
                .payload
                .get("reason")
                .and_then(|value| value.as_str())
                .unwrap_or("parallel merge finalization failed");
            let mut detail_lines = vec![reason.to_string()];
            if let Some(workspace_root) = event
                .payload
                .get("workspace_root")
                .and_then(|value| value.as_str())
            {
                detail_lines.push(format!("workspace_root={workspace_root}"));
            }
            if let Some(source_workdir) = event
                .payload
                .get("source_workdir")
                .and_then(|value| value.as_str())
            {
                detail_lines.push(format!("source_workdir={source_workdir}"));
            }
            if let Some(task_key) = event.payload.get("task_key").and_then(|value| value.as_str()) {
                detail_lines.push(format!("task_key={task_key}"));
            }
            if let Some(patch_path) = event.payload.get("patch_path").and_then(|value| value.as_str()) {
                detail_lines.push(format!("patch_path={patch_path}"));
            }
            if let Some(workspace_path) = event.payload.get("workspace_path").and_then(|value| value.as_str()) {
                detail_lines.push(format!("workspace_path={workspace_path}"));
            }
            if let Some(repo_status) = event.payload.get("repo_status").and_then(|value| value.as_str()) {
                detail_lines.push("repo_status:".to_string());
                for line in repo_status.lines() {
                    detail_lines.push(format!("- {line}"));
                }
            }
            let conflicted_files = event_string_list(Some(&event.payload), "conflicted_files");
            if !conflicted_files.is_empty() {
                detail_lines.push("conflicted files:".to_string());
                for file in conflicted_files {
                    detail_lines.push(format!("- {file}"));
                }
            }
            let recovery_hints = event_string_list(Some(&event.payload), "recovery_hints");
            if !recovery_hints.is_empty() {
                detail_lines.push("recovery_hints:".to_string());
                for hint in recovery_hints {
                    detail_lines.push(format!("- {hint}"));
                }
            }
            let message = detail_lines.join("\n");
            if !merge_finalization_blockers.contains(&message) {
                merge_finalization_blockers.push(message);
            }
        } else if event.event_type == "run.parallel_merge_succeeded" {
            merge_finalization_blockers.clear();
        }

        if let Some(report) = deterioration_from_event(event) {
            deterioration = Some(report);
        }

        if event.event_type == "run.observer.memory_hints" {
            if let Ok(report) = serde_json::from_value::<MemoryHintsReport>(event.payload.clone()) {
                memory_hints = Some(report);
            }
        }
    }

    let by_correlation = query_events(store, &EventQuery::by_correlation(correlation_id))?;
    let task_events: Vec<Event> = by_correlation
        .into_iter()
        .filter(|event| event.entity_type == EntityType::Task)
        .collect();
    let mut tasks = collect_task_projections(&task_events);
    for task in &mut tasks {
        if let Some(metadata) = task_catalog_by_id.get(&task.task_id) {
            if task.task_key.is_none() {
                task.task_key = metadata.task_key.clone();
            }
            if task.tranche_key.is_none() {
                task.tranche_key = metadata.tranche_key.clone();
            }
            if task.workspace_dir.is_none() {
                task.workspace_dir = metadata.workspace_dir.clone();
            }
            if task.tranche_group.is_none() {
                task.tranche_group = metadata.tranche_group.clone();
            }
            if task.allowed_paths.is_empty() {
                task.allowed_paths = metadata.allowed_paths.clone();
            }
        }
    }

    if let Some(latest_task) = tasks.iter().max_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.task_id.cmp(&b.task_id))
    }) {
        if latest_task.updated_at > updated_at {
            updated_at = latest_task.updated_at;
            last_event_type = latest_task.last_event_type.clone();
        }
    }

    Ok(Some(RunProjection {
        run_id,
        state,
        exit_reason,
        cancellation_source,
        cancellation_provenance,
        correlation_id,
        updated_at,
        last_event_type,
        objective,
        tasks,
        failed_gates,
        merge_finalization_blockers,
        deterioration,
        memory_hints,
        tranche_plan,
    }))
}

pub(crate) fn load_worktree_projection(
    store: &dyn EventStore,
    worktree_id: Uuid,
) -> Result<Option<WorktreeProjection>> {
    let events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Worktree, worktree_id.to_string()),
    )?;
    if events.is_empty() {
        return Ok(None);
    }

    let mut state = WorktreeState::WtUnbound;
    let mut correlation_id = events[0].correlation_id;
    let mut updated_at = events[0].occurred_at;
    let mut last_event_id = events[0].event_id;
    let mut last_event_type = events[0].event_type.clone();
    let mut reason = None;
    let mut run_id = None;
    let mut task_id = None;
    let mut repo_root = None;
    let mut worktree_path = None;
    let mut branch_name = None;
    let mut base_ref = None;
    let mut head_ref = None;
    let mut submodule_mode = None;

    for event in &events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_id = event.event_id;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = crate::parse_worktree_state(&extract_entity_state(event)) {
            state = next_state;
        }

        if let Some(next_reason) = event_reason(event) {
            reason = Some(next_reason);
        }

        if let Some(value) = event
            .payload
            .get("run_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            run_id = Some(value);
        }

        if let Some(value) = event
            .payload
            .get("task_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            task_id = Some(value);
        }

        if let Some(value) = event
            .payload
            .get("repo_root")
            .and_then(|value| value.as_str())
        {
            repo_root = Some(PathBuf::from(value));
        }

        if let Some(value) = event
            .payload
            .get("worktree_path")
            .and_then(|value| value.as_str())
        {
            worktree_path = Some(PathBuf::from(value));
        }

        if let Some(value) = event
            .payload
            .get("branch_name")
            .and_then(|value| value.as_str())
        {
            branch_name = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("base_ref")
            .and_then(|value| value.as_str())
        {
            base_ref = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("head_ref")
            .and_then(|value| value.as_str())
        {
            head_ref = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("submodule_mode")
            .and_then(|value| value.as_str())
            .and_then(crate::parse_submodule_mode)
        {
            submodule_mode = Some(value);
        }
    }

    Ok(Some(WorktreeProjection {
        worktree_id,
        state,
        correlation_id,
        updated_at,
        last_event_id,
        last_event_type,
        reason,
        run_id,
        task_id,
        repo_root,
        worktree_path,
        branch_name,
        base_ref,
        head_ref,
        submodule_mode,
    }))
}

pub(crate) fn load_merge_projection(
    store: &dyn EventStore,
    merge_id: Uuid,
) -> Result<Option<MergeProjection>> {
    let events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Merge, merge_id.to_string()),
    )?;
    if events.is_empty() {
        return Ok(None);
    }

    let mut state = MergeState::MergeRequested;
    let mut run_id = None;
    let mut worktree_id = None;
    let mut correlation_id = events[0].correlation_id;
    let mut updated_at = events[0].occurred_at;
    let mut last_event_id = events[0].event_id;
    let mut last_event_type = events[0].event_type.clone();
    let mut reason = None;
    let mut source_ref = None;
    let mut target_ref = None;
    let mut strategy = None;

    for event in &events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_id = event.event_id;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = crate::parse_merge_state(&extract_entity_state(event)) {
            state = next_state;
        }

        if let Some(next_reason) = event_reason(event) {
            reason = Some(next_reason);
        }

        if let Some(value) = event
            .payload
            .get("run_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            run_id = Some(value);
        }

        if let Some(value) = event
            .payload
            .get("worktree_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            worktree_id = Some(value);
        }

        if let Some(value) = event.payload.get("source").and_then(|value| value.as_str()) {
            source_ref = Some(value.to_string());
        }

        if let Some(value) = event.payload.get("target").and_then(|value| value.as_str()) {
            target_ref = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("strategy")
            .and_then(|value| value.as_str())
        {
            strategy = Some(value.to_string());
        }
    }

    Ok(Some(MergeProjection {
        merge_id,
        state,
        run_id,
        worktree_id,
        correlation_id,
        updated_at,
        last_event_id,
        last_event_type,
        reason,
        source_ref,
        target_ref,
        strategy,
    }))
}

pub(crate) fn load_latest_worktree_projection_for_run(
    store: &dyn EventStore,
    run_id: Uuid,
    correlation_id: Uuid,
) -> Result<Option<WorktreeProjection>> {
    let events = query_events(store, &EventQuery::by_correlation(correlation_id))?;
    let mut worktree_ids: Vec<Uuid> = events
        .iter()
        .filter(|event| event.entity_type == EntityType::Worktree)
        .filter_map(|event| event.entity_id.parse::<Uuid>().ok())
        .collect();
    worktree_ids.sort();
    worktree_ids.dedup();

    let mut projections = Vec::new();
    for worktree_id in worktree_ids {
        if let Some(projection) = load_worktree_projection(store, worktree_id)? {
            if projection.run_id == Some(run_id) {
                projections.push(projection);
            }
        }
    }

    projections.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.worktree_id.cmp(&b.worktree_id))
    });

    Ok(projections.pop())
}

pub(crate) fn build_worktree_binding(projection: &WorktreeProjection) -> Result<WorktreeBinding> {
    let run_id = projection
        .run_id
        .ok_or_else(|| anyhow!("worktree {} missing run_id context", projection.worktree_id))?;
    let repo_root = projection
        .repo_root
        .as_ref()
        .ok_or_else(|| {
            anyhow!(
                "worktree {} missing repo_root context",
                projection.worktree_id
            )
        })?
        .clone();
    let branch_name = projection
        .branch_name
        .as_ref()
        .ok_or_else(|| {
            anyhow!(
                "worktree {} missing branch_name context",
                projection.worktree_id
            )
        })?
        .clone();
    let worktree_path = projection
        .worktree_path
        .as_ref()
        .ok_or_else(|| {
            anyhow!(
                "worktree {} missing worktree_path context",
                projection.worktree_id
            )
        })?
        .clone();
    let base_ref = projection
        .base_ref
        .clone()
        .or_else(|| projection.head_ref.clone())
        .unwrap_or_else(|| "HEAD".to_string());

    let mut binding = WorktreeBinding::new(
        run_id,
        repo_root,
        branch_name,
        base_ref,
        projection.correlation_id,
    );
    binding.id = projection.worktree_id;
    binding.state = projection.state;
    binding.updated_at = projection.updated_at;
    binding.set_worktree_path(worktree_path);
    if let Some(task_id) = projection.task_id {
        binding = binding.with_task(task_id);
    }
    if let Some(head_ref) = projection.head_ref.as_ref() {
        binding.head_ref = head_ref.clone();
    }
    if let Some(mode) = projection.submodule_mode {
        binding = binding.with_submodule_mode(mode);
    }
    Ok(binding)
}

pub(crate) fn build_merge_intent(
    merge: &MergeProjection,
    worktree_id: Uuid,
) -> Result<MergeIntent> {
    let run_id = merge
        .run_id
        .ok_or_else(|| anyhow!("merge intent {} missing run_id context", merge.merge_id))?;
    let source_ref = merge
        .source_ref
        .as_ref()
        .ok_or_else(|| anyhow!("merge intent {} missing source ref", merge.merge_id))?
        .clone();
    let target_ref = merge
        .target_ref
        .as_ref()
        .ok_or_else(|| anyhow!("merge intent {} missing target ref", merge.merge_id))?
        .clone();

    let mut intent = MergeIntent::new(
        run_id,
        worktree_id,
        source_ref,
        target_ref,
        merge.correlation_id,
    );
    intent.id = merge.merge_id;
    intent.state = merge.state;
    intent.updated_at = merge.updated_at;
    if let Some(strategy_raw) = merge.strategy.as_deref() {
        let strategy = crate::parse_merge_strategy_value(strategy_raw).ok_or_else(|| {
            anyhow!(
                "merge intent {} has unsupported strategy {strategy_raw}",
                merge.merge_id
            )
        })?;
        intent.strategy = strategy;
    }
    Ok(intent)
}

pub(crate) fn resolve_merge_worktree_projection(
    store: &dyn EventStore,
    merge: &MergeProjection,
) -> Result<WorktreeProjection> {
    if let Some(worktree_id) = merge.worktree_id {
        return load_worktree_projection(store, worktree_id)?.ok_or_else(|| {
            anyhow!(
                "worktree {} referenced by merge intent {} not found",
                worktree_id,
                merge.merge_id
            )
        });
    }

    let run_id = merge
        .run_id
        .ok_or_else(|| anyhow!("merge intent {} missing run_id context", merge.merge_id))?;
    load_latest_worktree_projection_for_run(store, run_id, merge.correlation_id)?.ok_or_else(|| {
        anyhow!(
            "no worktree context found for merge intent {}",
            merge.merge_id
        )
    })
}

pub(crate) fn collect_entity_projections(events: &[Event]) -> Vec<EntityProjection> {
    let mut entities: BTreeMap<String, EntityProjection> = BTreeMap::new();

    for event in events {
        let entry = entities
            .entry(event.entity_id.clone())
            .or_insert_with(|| EntityProjection {
                entity_id: event.entity_id.clone(),
                state: extract_entity_state(event),
                updated_at: event.occurred_at,
                last_event_type: event.event_type.clone(),
                reason: event_reason(event),
            });

        entry.state = extract_entity_state(event);
        entry.updated_at = event.occurred_at;
        entry.last_event_type = event.event_type.clone();
        entry.reason = event_reason(event);
    }

    let mut values: Vec<_> = entities.into_values().collect();
    values.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.entity_id.cmp(&b.entity_id))
    });
    values
}

pub(crate) fn cancellation_stage_for_task_projection(task: &TaskProjection) -> CancellationStage {
    match task.state {
        TaskState::TaskExecuting => CancellationStage::Executing,
        TaskState::TaskVerifying => CancellationStage::Verifying,
        TaskState::TaskReady if task.attempt_no.unwrap_or(0) > 1 => CancellationStage::Retrying,
        _ => CancellationStage::Unknown,
    }
}
