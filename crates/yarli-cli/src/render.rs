use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use anyhow::Result;
use uuid::Uuid;

use yarli_cli::stream::StreamEvent;
use yarli_core::domain::{EntityType, Event};
use yarli_core::explain::{explain_run, explain_task, GateResult, RunSnapshot, TaskSnapshot};
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_gates::all_passed;
use yarli_store::event_store::EventQuery;
use yarli_store::EventStore;

use crate::commands::format_cancel_provenance_summary;
use crate::events::task_id_from_command_event;
use crate::persistence::query_events;
use crate::projection::*;

pub(crate) fn render_run_status(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let total_tasks = run.tasks.len();
    let complete = run
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::TaskComplete)
        .count();
    let failed = run
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::TaskFailed)
        .count();
    let blocked = run
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::TaskBlocked)
        .count();
    let active = run
        .tasks
        .iter()
        .filter(|task| {
            !matches!(
                task.state,
                TaskState::TaskComplete | TaskState::TaskFailed | TaskState::TaskCancelled
            )
        })
        .count();

    let mut out = String::new();
    writeln!(&mut out, "Run {}", run.run_id)?;
    writeln!(&mut out, "State: {:?}", run.state)?;
    writeln!(&mut out, "Last event: {}", run.last_event_type)?;
    writeln!(
        &mut out,
        "Updated: {}",
        run.updated_at.format("%Y-%m-%d %H:%M:%S")
    )?;
    if let Some(objective) = run.objective.as_ref() {
        writeln!(&mut out, "Objective: {objective}")?;
    }
    writeln!(
        &mut out,
        "Tasks: {total_tasks} total ({complete} complete, {failed} failed, {blocked} blocked, {active} active)"
    )?;
    if run.state == RunState::RunCancelled
        || run.cancellation_source.is_some()
        || run.cancellation_provenance.is_some()
    {
        writeln!(
            &mut out,
            "Cancellation source: {}",
            run.cancellation_source
                .map(|source| source.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )?;
        writeln!(
            &mut out,
            "Cancellation provenance: {}",
            format_cancel_provenance_summary(run.cancellation_provenance.as_ref())
        )?;
    }
    if let Some(report) = run.deterioration.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Deterioration: score={:.1} trend={:?} window_size={}",
            report.score, report.trend, report.window_size
        )?;
        for factor in &report.factors {
            writeln!(
                &mut out,
                "  factor {} impact={:.2} ({})",
                factor.name, factor.impact, factor.detail
            )?;
        }
    }

    if let Some(hints) = run.memory_hints.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Memories: {} hint(s) (query={:?})",
            hints.results.len(),
            hints.query_text
        )?;
        for hint in hints.results.iter().take(3) {
            writeln!(
                &mut out,
                "  {} score={:.2} {}",
                hint.memory_id, hint.relevance_score, hint.content_snippet
            )?;
        }
    }

    if run.tasks.is_empty() {
        writeln!(&mut out, "No task events recorded for this run.")?;
    } else {
        writeln!(&mut out)?;
        writeln!(&mut out, "Task states:")?;
        for task in &run.tasks {
            let task_key = task.task_key.as_deref().unwrap_or("-");
            writeln!(
                &mut out,
                "  {} [{}]  {:?}  ({})",
                task_key, task.task_id, task.state, task.last_event_type
            )?;
            if let Some(worker_actor) = task.worker_actor.as_ref() {
                writeln!(&mut out, "    worker_actor: {worker_actor}")?;
            }
            if let Some(workspace_dir) = task.workspace_dir.as_ref() {
                writeln!(&mut out, "    workspace_dir: {workspace_dir}")?;
            }
            if task.tranche_key.is_some()
                || task.tranche_group.is_some()
                || !task.allowed_paths.is_empty()
            {
                writeln!(
                    &mut out,
                    "    scope: tranche={} group={} allowed_paths={}",
                    task.tranche_key.as_deref().unwrap_or("-"),
                    task.tranche_group.as_deref().unwrap_or("-"),
                    if task.allowed_paths.is_empty() {
                        "-".to_string()
                    } else {
                        task.allowed_paths.join(", ")
                    }
                )?;
            }
            if let Some(ref last_error) = task.last_error {
                writeln!(&mut out, "    last_error: {last_error}")?;
            }
            if let Some(ref detail) = task.blocker_detail {
                writeln!(&mut out, "    blocker_detail: {detail}")?;
            }
            if let Some(ref breach) = task.budget_breach_reason {
                writeln!(&mut out, "    budget_exceeded: {breach}")?;
            }
            if let Some(ref tu) = task.token_usage {
                writeln!(
                    &mut out,
                    "    token_usage: prompt_tokens={} completion_tokens={} total_tokens={}",
                    tu.prompt_tokens, tu.completion_tokens, tu.total_tokens
                )?;
            }
            if let Some(ref ru) = task.resource_usage {
                let rss = ru
                    .max_rss_bytes
                    .map(|v| format!("{v}"))
                    .unwrap_or_else(|| "-".into());
                writeln!(&mut out, "    resource_usage: max_rss_bytes={rss}")?;
            }
            if let Some(ref hints) = task.memory_hints {
                writeln!(
                    &mut out,
                    "    memory_hints: {} (query={:?})",
                    hints.results.len(),
                    hints.query_text
                )?;
            }
        }
    }

    if !run.tranche_plan.is_empty() {
        let tasks_by_key: HashMap<&str, &TaskProjection> = run
            .tasks
            .iter()
            .filter_map(|task| task.task_key.as_deref().map(|key| (key, task)))
            .collect();
        writeln!(&mut out)?;
        writeln!(&mut out, "Tranche mapping:")?;
        for tranche in &run.tranche_plan {
            writeln!(
                &mut out,
                "  tranche={} group={}",
                tranche.key,
                tranche.tranche_group.as_deref().unwrap_or("-")
            )?;
            for task_key in &tranche.task_keys {
                if let Some(task) = tasks_by_key.get(task_key.as_str()) {
                    writeln!(
                        &mut out,
                        "    task_key={} task_id={} worker_actor={}",
                        task_key,
                        task.task_id,
                        task.worker_actor.as_deref().unwrap_or("-")
                    )?;
                } else {
                    writeln!(
                        &mut out,
                        "    task_key={} task_id=- worker_actor=-",
                        task_key
                    )?;
                }
            }
        }
    }

    Ok(out.trim_end().to_string())
}

pub(crate) fn render_run_explain(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let snapshot = RunSnapshot {
        run_id,
        state: run.state,
        tasks: run
            .tasks
            .iter()
            .map(|task| TaskSnapshot {
                task_id: task.task_id,
                name: task.task_id.to_string(),
                state: task.state,
                blocked_by: Vec::new(),
                gates: task
                    .failed_gates
                    .iter()
                    .map(|(gate_type, reason)| {
                        (
                            *gate_type,
                            GateResult::Failed {
                                reason: reason.clone(),
                            },
                        )
                    })
                    .collect(),
                last_transition_at: Some(task.updated_at),
                resource_usage: task.resource_usage.clone(),
                token_usage: task.token_usage.clone(),
                budget_breach_reason: task.budget_breach_reason.clone(),
            })
            .collect(),
        gates: run
            .failed_gates
            .iter()
            .map(|(gate_type, reason)| {
                (
                    *gate_type,
                    GateResult::Failed {
                        reason: reason.clone(),
                    },
                )
            })
            .collect(),
    };
    let explain = explain_run(&snapshot);

    let mut out = String::new();
    writeln!(&mut out, "Run explain for {run_id}")?;
    writeln!(&mut out, "Status: {:?}", explain.status)?;
    writeln!(
        &mut out,
        "Exit reason: {}",
        run.exit_reason.as_deref().unwrap_or("none")
    )?;
    writeln!(
        &mut out,
        "Cancellation source: {}",
        run.cancellation_source
            .map(|source| source.to_string())
            .unwrap_or_else(|| {
                if run.state == RunState::RunCancelled {
                    "unknown".to_string()
                } else {
                    "none".to_string()
                }
            })
    )?;
    if run.state == RunState::RunCancelled || run.cancellation_provenance.is_some() {
        writeln!(
            &mut out,
            "Cancellation provenance: {}",
            format_cancel_provenance_summary(run.cancellation_provenance.as_ref())
        )?;
    }
    writeln!(&mut out, "Blocking tasks: {}", explain.blocking_tasks.len())?;
    writeln!(&mut out, "Failed gates: {}", explain.failed_gates.len())?;

    if !explain.blocking_tasks.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Blocking tasks:")?;
        for blocker in &explain.blocking_tasks {
            writeln!(
                &mut out,
                "  {} ({:?}) - {}",
                blocker.task_name, blocker.state, blocker.reason
            )?;
        }
    }

    if !explain.failed_gates.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Failed gates:")?;
        for failure in &explain.failed_gates {
            writeln!(
                &mut out,
                "  {} ({}) - {}",
                failure.gate_type.label(),
                failure.entity_id,
                failure.reason
            )?;
        }
    }

    if !explain.budget_breaches.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Budget breaches:")?;
        for breach in &explain.budget_breaches {
            writeln!(&mut out, "  {} - {}", breach.task_name, breach.reason)?;
        }
    }

    if !explain.suggested_actions.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Suggested actions:")?;
        for action in &explain.suggested_actions {
            writeln!(&mut out, "  - {}", action.description)?;
        }
    }

    if let Some(report) = run.deterioration.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Sequence deterioration: score={:.1} trend={:?} window_size={}",
            report.score, report.trend, report.window_size
        )?;
        for factor in &report.factors {
            writeln!(
                &mut out,
                "  {} impact={:.2} ({})",
                factor.name, factor.impact, factor.detail
            )?;
        }
    }

    if let Some(hints) = run.memory_hints.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Memories: {} hint(s) (query={:?})",
            hints.results.len(),
            hints.query_text
        )?;
        for hint in hints.results.iter().take(5) {
            writeln!(
                &mut out,
                "  {} score={:.2} {}",
                hint.memory_id, hint.relevance_score, hint.content_snippet
            )?;
        }
    }

    let hinted_tasks: Vec<_> = run
        .tasks
        .iter()
        .filter(|task| task.memory_hints.is_some())
        .collect();
    if !hinted_tasks.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Task memory hints:")?;
        for task in hinted_tasks {
            let hints = task.memory_hints.as_ref().unwrap();
            writeln!(
                &mut out,
                "  {} {:?}: {} hint(s) (query={:?})",
                task.task_id,
                task.state,
                hints.results.len(),
                hints.query_text
            )?;
        }
    }

    Ok(out.trim_end().to_string())
}

pub(crate) fn render_task_explain(store: &dyn EventStore, task_id: Uuid) -> Result<String> {
    let task = match load_task_projection(store, task_id)? {
        Some(task) => task,
        None => return Ok(format!("Task {task_id} not found in persisted event log.")),
    };

    let snapshot = TaskSnapshot {
        task_id,
        name: task.task_id.to_string(),
        state: task.state,
        blocked_by: Vec::new(),
        gates: task
            .failed_gates
            .iter()
            .map(|(gate_type, reason)| {
                (
                    *gate_type,
                    GateResult::Failed {
                        reason: reason.clone(),
                    },
                )
            })
            .collect(),
        last_transition_at: Some(task.updated_at),
        resource_usage: task.resource_usage.clone(),
        token_usage: task.token_usage.clone(),
        budget_breach_reason: task.budget_breach_reason.clone(),
    };
    let explain = explain_task(&snapshot);

    let mut out = String::new();
    writeln!(&mut out, "Task explain for {task_id}")?;
    writeln!(&mut out, "Status: {:?}", explain.status)?;
    writeln!(&mut out, "State: {:?}", task.state)?;
    writeln!(&mut out, "Failed gates: {}", explain.failed_gates.len())?;

    if let Some(reason) = task.reason.as_ref() {
        writeln!(&mut out, "Last reason: {reason}")?;
    }

    if let Some(ref last_error) = task.last_error {
        writeln!(&mut out, "Last error: {last_error}")?;
    }

    if let Some(ref detail) = task.blocker_detail {
        writeln!(&mut out, "Blocker detail: {detail}")?;
    }

    if !explain.budget_breaches.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Budget breaches:")?;
        for breach in &explain.budget_breaches {
            writeln!(&mut out, "  budget_exceeded: {}", breach.reason)?;
        }
    }

    if let Some(ref tu) = task.token_usage {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Token usage: prompt_tokens={} completion_tokens={} total_tokens={}",
            tu.prompt_tokens, tu.completion_tokens, tu.total_tokens
        )?;
    }

    if let Some(ref ru) = task.resource_usage {
        let rss = ru
            .max_rss_bytes
            .map(|v| format!("{v}"))
            .unwrap_or_else(|| "-".into());
        writeln!(&mut out, "Resource usage: max_rss_bytes={rss}")?;
    }

    if !explain.failed_gates.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Failed gates:")?;
        for failure in &explain.failed_gates {
            writeln!(
                &mut out,
                "  {} - {}",
                failure.gate_type.label(),
                failure.reason
            )?;
        }
    }

    if !explain.suggested_actions.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Suggested actions:")?;
        for action in &explain.suggested_actions {
            writeln!(&mut out, "  - {}", action.description)?;
        }
    }

    Ok(out.trim_end().to_string())
}

pub(crate) fn render_task_list(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let mut out = String::new();
    writeln!(
        &mut out,
        "Tasks for run {} ({} total):",
        run.run_id,
        run.tasks.len()
    )?;
    if run.tasks.is_empty() {
        writeln!(&mut out, "No task events recorded.")?;
    } else {
        for task in &run.tasks {
            writeln!(
                &mut out,
                "  {}  {:?}  last={}{}",
                task.task_id,
                task.state,
                task.last_event_type,
                task.reason
                    .as_ref()
                    .map(|reason| format!(" reason={reason}"))
                    .unwrap_or_default()
            )?;
        }
    }
    Ok(out.trim_end().to_string())
}

pub(crate) fn render_gate_rerun_output(
    task_id: Uuid,
    run_id: Uuid,
    selected_count: usize,
    evaluations: &[yarli_gates::GateEvaluation],
) -> Result<String> {
    let mut output = String::new();
    writeln!(&mut output, "Gate re-run for task {task_id}")?;
    writeln!(&mut output, "Run: {run_id}")?;
    writeln!(&mut output, "Evaluated {} gate(s):", selected_count)?;

    for evaluation in evaluations {
        match &evaluation.result {
            GateResult::Passed { evidence_ids } => {
                writeln!(
                    &mut output,
                    "  {}: PASS ({} evidence id(s))",
                    evaluation.gate_type.label(),
                    evidence_ids.len()
                )?;
            }
            GateResult::Failed { reason } => {
                writeln!(
                    &mut output,
                    "  {}: FAIL ({reason})",
                    evaluation.gate_type.label()
                )?;
            }
            GateResult::Pending => {
                writeln!(&mut output, "  {}: PENDING", evaluation.gate_type.label())?;
            }
        }
        if let Some(remediation) = evaluation.remediation.as_ref() {
            writeln!(&mut output, "    remediation: {remediation}")?;
        }
    }

    writeln!(
        &mut output,
        "Overall: {}",
        if all_passed(evaluations) {
            "all selected gates passed"
        } else {
            "one or more selected gates failed"
        }
    )?;

    Ok(output.trim_end().to_string())
}

pub(crate) fn render_worktree_status(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let events = query_events(store, &EventQuery::by_correlation(run.correlation_id))?;
    let worktree_events: Vec<Event> = events
        .into_iter()
        .filter(|event| event.entity_type == yarli_core::domain::EntityType::Worktree)
        .collect();
    let worktrees = collect_entity_projections(&worktree_events);

    let mut out = String::new();
    writeln!(
        &mut out,
        "Worktrees for run {} ({} total):",
        run.run_id,
        worktrees.len()
    )?;

    if worktrees.is_empty() {
        writeln!(&mut out, "No persisted worktree events recorded yet.")?;
    } else {
        for worktree in &worktrees {
            writeln!(
                &mut out,
                "  {}  {}  last={}",
                worktree.entity_id, worktree.state, worktree.last_event_type
            )?;
        }
    }

    Ok(out.trim_end().to_string())
}

pub(crate) fn render_merge_status(store: &dyn EventStore, merge_id: Uuid) -> Result<String> {
    let merge = match load_merge_projection(store, merge_id)? {
        Some(merge) => merge,
        None => {
            return Ok(format!(
                "Merge intent {merge_id} not found in persisted event log."
            ))
        }
    };

    let mut out = String::new();
    writeln!(&mut out, "Merge intent {}", merge.merge_id)?;
    writeln!(&mut out, "State: {:?}", merge.state)?;
    writeln!(&mut out, "Last event: {}", merge.last_event_type)?;
    writeln!(
        &mut out,
        "Updated: {}",
        merge.updated_at.format("%Y-%m-%d %H:%M:%S")
    )?;
    if let Some(run_id) = merge.run_id {
        writeln!(&mut out, "Run: {run_id}")?;
    }
    if let Some(source) = merge.source_ref {
        writeln!(&mut out, "Source: {source}")?;
    }
    if let Some(target) = merge.target_ref {
        writeln!(&mut out, "Target: {target}")?;
    }
    if let Some(strategy) = merge.strategy {
        writeln!(&mut out, "Strategy: {strategy}")?;
    }
    if let Some(reason) = merge.reason {
        writeln!(&mut out, "Reason: {reason}")?;
    }

    Ok(out.trim_end().to_string())
}

pub(crate) fn render_run_list(store: &dyn EventStore) -> Result<String> {
    let run_events = query_events(
        store,
        &EventQuery::by_entity_type(yarli_core::domain::EntityType::Run),
    )?;

    if run_events.is_empty() {
        return Ok("No runs found in event store.".to_string());
    }

    // Group events by run entity_id to discover unique runs.
    let mut runs: BTreeMap<String, (RunState, Option<String>, chrono::DateTime<chrono::Utc>, u32)> =
        BTreeMap::new();
    for event in &run_events {
        let entry = runs.entry(event.entity_id.clone()).or_insert((
            RunState::RunOpen,
            None,
            event.occurred_at,
            0,
        ));
        entry.2 = event.occurred_at; // last update
        entry.3 += 1; // event count

        if let Some(next_state) = run_state_from_event(event) {
            entry.0 = next_state;
        }
        if event.event_type == "run.config_snapshot" {
            entry.1 = event
                .payload
                .get("objective")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
    }

    // Count tasks per run.
    let task_events = query_events(
        store,
        &EventQuery::by_entity_type(yarli_core::domain::EntityType::Task),
    )?;
    let mut task_counts: HashMap<String, (u32, u32, u32)> = HashMap::new(); // total, complete, failed
                                                                            // Map task_id -> run_id via correlation_id.
    let mut corr_to_run: HashMap<Uuid, String> = HashMap::new();
    for event in &run_events {
        corr_to_run
            .entry(event.correlation_id)
            .or_insert_with(|| event.entity_id.clone());
    }
    // Track task states per run.
    let mut task_states: HashMap<String, HashMap<String, TaskState>> = HashMap::new();
    for event in &task_events {
        if let Some(run_id) = corr_to_run.get(&event.correlation_id) {
            if let Some((_, _, updated_at, _)) = runs.get_mut(run_id) {
                if event.occurred_at > *updated_at {
                    *updated_at = event.occurred_at;
                }
            }
            let tasks = task_states.entry(run_id.clone()).or_default();
            if let Some(state) = task_state_from_event(event) {
                tasks.insert(event.entity_id.clone(), state);
            } else if !tasks.contains_key(&event.entity_id) {
                tasks.insert(event.entity_id.clone(), TaskState::TaskOpen);
            }
        }
    }
    for (run_id, tasks) in &task_states {
        let total = tasks.len() as u32;
        let complete = tasks
            .values()
            .filter(|s| **s == TaskState::TaskComplete)
            .count() as u32;
        let failed = tasks
            .values()
            .filter(|s| **s == TaskState::TaskFailed)
            .count() as u32;
        task_counts.insert(run_id.clone(), (total, complete, failed));
    }

    let run_id_prefixes = unique_run_id_prefixes(runs.keys().cloned().collect::<Vec<_>>(), 10);

    let mut out = String::new();
    out.push_str(&format!(
        "{:<14} {:<14} {:<30} {:<16} {}\n",
        "RUN ID", "STATE", "OBJECTIVE", "TASKS (C/F/T)", "UPDATED"
    ));
    out.push_str(&"-".repeat(85));
    out.push('\n');

    for (run_id_str, (state, objective, updated_at, _event_count)) in &runs {
        let short_id = run_id_prefixes
            .get(run_id_str)
            .cloned()
            .unwrap_or_else(|| compact_run_id(run_id_str));
        let obj = objective.as_deref().unwrap_or("-");
        let obj_display = if obj.len() > 28 {
            format!("{}…", &obj[..27])
        } else {
            obj.to_string()
        };
        let (total, complete, failed) = task_counts
            .get(run_id_str.as_str())
            .copied()
            .unwrap_or((0, 0, 0));
        let tasks_str = format!("{complete}/{failed}/{total}");
        let time_str = updated_at.format("%Y-%m-%d %H:%M");

        out.push_str(&format!(
            "{:<14} {:<14} {:<30} {:<16} {}\n",
            short_id,
            format!("{:?}", state),
            obj_display,
            tasks_str,
            time_str
        ));
    }

    Ok(out.trim_end().to_string())
}

pub(crate) fn compact_run_id(run_id: &str) -> String {
    if let Ok(parsed) = Uuid::parse_str(run_id) {
        parsed.simple().to_string()
    } else {
        run_id.chars().filter(|c| *c != '-').collect()
    }
}

pub(crate) fn unique_run_id_prefixes(
    run_ids: Vec<String>,
    min_len: usize,
) -> HashMap<String, String> {
    let compact: Vec<(String, String)> = run_ids
        .into_iter()
        .map(|run_id| {
            let compact = compact_run_id(&run_id);
            (run_id, compact)
        })
        .collect();
    let mut prefixes = HashMap::new();

    for (run_id, compact_id) in &compact {
        let mut chosen = compact_id.clone();
        let start = min_len.min(compact_id.len()).max(1);
        for len in start..=compact_id.len() {
            let prefix = &compact_id[..len];
            let is_unique = compact
                .iter()
                .filter(|(other_id, _)| other_id != run_id)
                .all(|(_, other_compact)| !other_compact.starts_with(prefix));
            if is_unique {
                chosen = prefix.to_string();
                break;
            }
        }
        prefixes.insert(run_id.clone(), chosen);
    }

    prefixes
}

pub(crate) fn render_task_output(store: &dyn EventStore, task_id: Uuid) -> Result<String> {
    let command_events = query_events(store, &EventQuery::by_entity_type(EntityType::Command))?;

    let mut output_lines: Vec<(Uuid, String)> = Vec::new();

    for event in &command_events {
        if event.event_type != "command.output" {
            continue;
        }
        if task_id_from_command_event(event) != Some(task_id) {
            continue;
        }
        let chunks = event
            .payload
            .get("chunks")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for chunk in &chunks {
            if let Some(data) = chunk.get("data").and_then(|v| v.as_str()) {
                if !data.trim().is_empty() {
                    output_lines.push((event.event_id, data.to_string()));
                }
            }
        }
    }

    if output_lines.is_empty() {
        return Ok(format!("No command output found for task {task_id}.\n"));
    }

    let mut out = String::new();
    for (_event_id, line) in &output_lines {
        out.push_str(line);
        out.push('\n');
    }
    Ok(out)
}
