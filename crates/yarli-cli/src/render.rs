use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::PathBuf;

use anyhow::Result;
use uuid::Uuid;

use yarli_cli::yarli_core::domain::{EntityType, Event};
use yarli_cli::yarli_core::explain::{
    explain_run, explain_task, GateResult, RunSnapshot, TaskSnapshot,
};
use yarli_cli::yarli_core::fsm::run::RunState;
use yarli_cli::yarli_core::fsm::task::TaskState;
use yarli_cli::yarli_gates::all_passed;
use yarli_cli::yarli_store::event_store::EventQuery;
use yarli_cli::yarli_store::EventStore;

use crate::commands::format_cancel_provenance_summary;
use crate::events::task_id_from_command_event;
use crate::persistence::query_events;
use crate::projection::*;

fn write_blocker_lines(out: &mut String, blocker: &str) -> Result<(), std::fmt::Error> {
    for line in blocker.lines() {
        writeln!(&mut *out, "  {line}")?;
    }
    Ok(())
}

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
    if run.drain_requested && !run.state.is_terminal() {
        writeln!(
            &mut out,
            "Drain requested: yes ({})",
            run.drain_reason.as_deref().unwrap_or("operator request")
        )?;
    }
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

    if !run.merge_finalization_blockers.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Merge-finalization blockers:")?;
        for blocker in &run.merge_finalization_blockers {
            write_blocker_lines(&mut out, blocker)?;
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
            if !task.depends_on.is_empty() {
                writeln!(&mut out, "    depends_on: {}", task.depends_on.join(", "))?;
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
    if run.drain_requested && !run.state.is_terminal() {
        writeln!(
            &mut out,
            "Drain requested: {}",
            run.drain_reason.as_deref().unwrap_or("operator request")
        )?;
    }
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
    if !run.merge_finalization_blockers.is_empty() {
        writeln!(
            &mut out,
            "Merge-finalization blockers: {}",
            run.merge_finalization_blockers.len()
        )?;
    }

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

    if !run.merge_finalization_blockers.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Merge-finalization blockers:")?;
        for blocker in &run.merge_finalization_blockers {
            write_blocker_lines(&mut out, blocker)?;
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
    evaluations: &[yarli_cli::yarli_gates::GateEvaluation],
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
        .filter(|event| event.entity_type == yarli_cli::yarli_core::domain::EntityType::Worktree)
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
        &EventQuery::by_entity_type(yarli_cli::yarli_core::domain::EntityType::Run),
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
        &EventQuery::by_entity_type(yarli_cli::yarli_core::domain::EntityType::Task),
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
            let mut end = 27;
            while end > 0 && !obj.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}…", &obj[..end])
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
    if let Some(artifact_lines) = read_task_output_artifact(task_id)? {
        if !artifact_lines.is_empty() {
            let mut out = String::new();
            for line in &artifact_lines {
                out.push_str(line);
                out.push('\n');
            }
            return Ok(out);
        }
    }

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

fn task_output_artifact_path(task_id: Uuid) -> PathBuf {
    PathBuf::from(".yarl/runs").join(format!("{task_id}.jsonl"))
}

fn read_task_output_artifact(task_id: Uuid) -> Result<Option<Vec<String>>> {
    let path = task_output_artifact_path(task_id);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed to read command output artifact {}: {err}",
                path.display()
            ));
        }
    };

    let mut lines = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(payload) = serde_json::from_str::<serde_json::Value>(&line) {
            if let Some(data) = payload.get("data").and_then(serde_json::Value::as_str) {
                if !data.trim().is_empty() {
                    lines.push(data.to_string());
                }
            }
        }
    }

    Ok(Some(lines))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{execute_task_annotate, list_runs_by_latest_state, resolve_run_id_input};
    use crate::events::*;
    use crate::test_helpers::make_event;
    use chrono::Utc;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::Path;
    use std::process::Command;
    use tokio::sync::mpsc;
    use uuid::Uuid;
    use yarli_cli::stream::StreamEvent;
    use yarli_cli::yarli_core::domain::{EntityType, Event};
    use yarli_cli::yarli_core::entities::worktree_binding::WorktreeBinding;
    use yarli_cli::yarli_store::InMemoryEventStore;

    #[allow(dead_code)]
    fn run_git(repo: &Path, args: &[&str]) -> (bool, String, String) {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .expect("git command should run");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    }

    #[allow(dead_code)]
    fn run_git_expect_ok(repo: &Path, args: &[&str]) {
        let (ok, _stdout, stderr) = run_git(repo, args);
        assert!(ok, "git {:?} failed: {stderr}", args);
    }

    #[allow(dead_code)]
    fn seed_worktree_event_payload(
        binding: &WorktreeBinding,
        from: &str,
        to: &str,
        reason: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "from": from,
            "to": to,
            "run_id": binding.run_id,
            "task_id": binding.task_id,
            "repo_root": binding.repo_root.display().to_string(),
            "worktree_path": binding.worktree_path.display().to_string(),
            "branch_name": binding.branch_name.clone(),
            "base_ref": binding.base_ref.clone(),
            "head_ref": binding.head_ref.clone(),
            "submodule_mode": "locked",
            "reason": reason,
        })
    }

    #[test]
    fn render_run_status_reconstructs_persisted_state() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                corr,
                serde_json::json!({ "objective": "ship it" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.ready",
                corr,
                serde_json::json!({ "from": "TaskOpen", "to": "TaskReady" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskVerifying", "to": "TaskComplete" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.completed",
                corr,
                serde_json::json!({ "from": "RunVerifying", "to": "RunCompleted" }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("RunCompleted"));
        assert!(output.contains(&task_id.to_string()));
        assert!(!output.contains("requires a persistent store"));
    }

    #[test]
    fn render_run_status_surfaces_pending_drain_request() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.drain_requested",
                corr,
                serde_json::json!({ "reason": "stop after current" }),
            ))
            .unwrap();

        let status = render_run_status(&store, run_id).unwrap();
        assert!(status.contains("Drain requested: yes (stop after current)"));

        let explain = render_run_explain(&store, run_id).unwrap();
        assert!(explain.contains("Drain requested: stop after current"));
    }

    #[test]
    fn render_run_status_updated_tracks_latest_task_activity() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let base = Utc::now();

        let mut run_event = make_event(
            EntityType::Run,
            run_id.to_string(),
            "run.activated",
            corr,
            serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
        );
        run_event.occurred_at = base;
        store.append(run_event).unwrap();

        let mut task_event = make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.executing",
            corr,
            serde_json::json!({ "from": "TaskReady", "to": "TaskExecuting" }),
        );
        task_event.occurred_at = base + chrono::Duration::seconds(42);
        store.append(task_event).unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Last event: task.executing"));
        assert!(output.contains(
            &(base + chrono::Duration::seconds(42))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        ));
    }

    #[test]
    fn list_runs_by_latest_state_ignores_non_transition_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.progress",
                corr,
                serde_json::json!({
                    "tick": 3,
                    "summary": "heartbeat"
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.task_catalog",
                corr,
                serde_json::json!({
                    "tasks": []
                }),
            ))
            .unwrap();

        let states = list_runs_by_latest_state(&store).unwrap();
        assert_eq!(states.get(&run_id), Some(&RunState::RunActive));
    }

    #[test]
    fn render_run_list_updated_tracks_latest_task_event() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let base = Utc::now();

        let mut run_event = make_event(
            EntityType::Run,
            run_id.to_string(),
            "run.activated",
            corr,
            serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
        );
        run_event.occurred_at = base;
        store.append(run_event).unwrap();

        let mut task_event = make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.ready",
            corr,
            serde_json::json!({ "from": "TaskOpen", "to": "TaskReady" }),
        );
        task_event.occurred_at = base + chrono::Duration::minutes(3);
        store.append(task_event).unwrap();

        let output = render_run_list(&store).unwrap();
        assert!(output.contains(
            &(base + chrono::Duration::minutes(3))
                .format("%Y-%m-%d %H:%M")
                .to_string()
        ));
    }

    #[test]
    fn render_run_status_surfaces_tranche_group_task_worker_mapping() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                corr,
                serde_json::json!({
                    "objective": "ship grouped tranche",
                    "config_snapshot": {
                        "runtime": {
                            "tranche_plan": [
                                {
                                    "key": "group-001-core",
                                    "objective": "core work",
                                    "task_keys": ["core_task"],
                                    "tranche_group": "core"
                                }
                            ]
                        }
                    }
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.task_catalog",
                corr,
                serde_json::json!({
                    "tasks": [
                        {
                            "task_id": task_id,
                            "task_key": "core_task",
                            "tranche_key": "group-001-core",
                            "tranche_group": "core",
                            "allowed_paths": ["src/main.rs"]
                        }
                    ]
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.executing",
                corr,
                serde_json::json!({
                    "from": "TaskReady",
                    "to": "TaskExecuting",
                    "task_key": "core_task",
                    "worker": "worker-a"
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({
                    "from": "TaskVerifying",
                    "to": "TaskComplete",
                    "task_key": "core_task"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("scope: tranche=group-001-core group=core allowed_paths=src/main.rs")
        );
        assert!(output.contains("Tranche mapping:"));
        assert!(output.contains("tranche=group-001-core group=core"));
        assert!(output.contains("task_key=core_task"));
        assert!(output.contains(&task_id.to_string()));
        assert!(output.contains("worker_actor=worker-a"));
    }

    #[test]
    fn event_to_stream_event_maps_observer_progress_to_transient_status() {
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let event = make_event(
            EntityType::Run,
            run_id.to_string(),
            "run.observer.progress",
            corr,
            serde_json::json!({
                "summary": "heartbeat pending=1 leased=0"
            }),
        );

        let mapped = event_to_stream_event(&event, &[], false).expect("progress event should map");
        match mapped {
            StreamEvent::TransientStatus { message, .. } => {
                assert_eq!(message, "heartbeat pending=1 leased=0");
            }
            other => panic!("expected transient status, got {other:?}"),
        }
    }

    #[test]
    fn event_to_stream_event_maps_command_output_chunks_to_task_output() {
        let corr = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let command_id = Uuid::now_v7();
        let event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Command,
            entity_id: command_id.to_string(),
            event_type: "command.output".to_string(),
            payload: serde_json::json!({
                "chunk_count": 2,
                "chunks": [
                    { "seq": 1, "stream": "stdout", "data": "line one" },
                    { "seq": 2, "stream": "stderr", "data": "line two" }
                ]
            }),
            correlation_id: corr,
            causation_id: None,
            actor: "worker".to_string(),
            idempotency_key: Some(format!("{task_id}:cmd:1:output")),
        };

        let mapped =
            event_to_stream_event(&event, &[(task_id, "tranche-001-i5".to_string())], false)
                .expect("command output should map");
        match mapped {
            StreamEvent::CommandOutput {
                task_id: mapped_task_id,
                task_name,
                line,
            } => {
                assert_eq!(mapped_task_id, task_id);
                assert_eq!(task_name, "tranche-001-i5");
                assert_eq!(line, "line one\nline two");
            }
            other => panic!("expected command output stream event, got {other:?}"),
        }
    }

    #[test]
    fn event_to_stream_event_skips_empty_command_output_chunks() {
        let corr = Uuid::now_v7();
        let event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Command,
            entity_id: Uuid::now_v7().to_string(),
            event_type: "command.output".to_string(),
            payload: serde_json::json!({
                "chunk_count": 1,
                "chunks": [
                    { "seq": 1, "stream": "stdout", "data": "   " }
                ]
            }),
            correlation_id: corr,
            causation_id: None,
            actor: "worker".to_string(),
            idempotency_key: Some(format!("{}:cmd:1:output", Uuid::now_v7())),
        };

        assert!(
            event_to_stream_event(&event, &[], false).is_none(),
            "empty command output should not emit stream event"
        );
    }

    #[test]
    fn render_task_output_uses_task_artifact_when_available() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();

        // Use a dedicated tempdir as cwd to isolate from concurrent tests.
        let tmp = tempfile::TempDir::new().unwrap();
        let artifact_dir = tmp.path().join(".yarl/runs");
        let artifact_path = artifact_dir.join(format!("{task_id}.jsonl"));

        fs::create_dir_all(&artifact_dir).unwrap();
        let mut artifact = File::create(&artifact_path).unwrap();
        writeln!(
            &mut artifact,
            "{{\"artifact_type\":\"command_output\",\"run_id\":\"{}\",\"task_id\":\"{}\"}}",
            task_id, task_id
        )
        .unwrap();
        writeln!(
            &mut artifact,
            "{{\"command_id\":\"{}\",\"seq\":1,\"stream\":\"stdout\",\"data\":\"from artifact\"}}",
            task_id
        )
        .unwrap();
        drop(artifact);

        let restore_dir = std::env::current_dir().unwrap_or_else(|_| std::env::temp_dir());
        std::env::set_current_dir(tmp.path()).unwrap();
        let output = render_task_output(&store, task_id).unwrap();
        let _ = std::env::set_current_dir(&restore_dir);

        assert!(output.contains("from artifact"));
    }

    #[test]
    fn stream_task_catalog_entries_extracts_task_ids_and_keys() {
        let corr = Uuid::now_v7();
        let task_a = Uuid::now_v7();
        let task_b = Uuid::now_v7();
        let event = make_event(
            EntityType::Run,
            Uuid::now_v7().to_string(),
            "run.task_catalog",
            corr,
            serde_json::json!({
                "tasks": [
                    { "task_id": task_a, "task_key": "first" },
                    { "task_id": task_b }
                ]
            }),
        );

        let entries = stream_task_catalog_entries(&event);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (task_a, "first".to_string(), Vec::new()));
        assert_eq!(entries[1].0, task_b);
        assert_eq!(entries[1].2, Vec::<String>::new());
    }

    #[test]
    fn task_catalog_entries_from_event_extracts_workspace_dir() {
        let corr = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let event = make_event(
            EntityType::Run,
            Uuid::now_v7().to_string(),
            "run.task_catalog",
            corr,
            serde_json::json!({
                "tasks": [
                    {
                        "task_id": task_id,
                        "task_key": "first",
                        "workspace_dir": "/tmp/yarli-ws/task-001"
                    }
                ]
            }),
        );

        let entries = task_catalog_entries_from_event(&event);
        let entry = entries.get(&task_id).expect("task should be present");
        assert_eq!(
            entry.workspace_dir.as_deref(),
            Some("/tmp/yarli-ws/task-001")
        );
    }

    #[test]
    fn emit_initial_stream_state_emits_run_and_task_discovery() {
        let run_id = Uuid::now_v7();
        let task_a = Uuid::now_v7();
        let task_b = Uuid::now_v7();
        let task_names = vec![
            (task_a, "tranche-001".to_string()),
            (task_b, "tranche-002".to_string()),
        ];
        let (tx, mut rx) = mpsc::unbounded_channel::<StreamEvent>();

        emit_initial_stream_state(&tx, run_id, "objective", &task_names);

        match rx.try_recv().expect("run started event") {
            StreamEvent::RunStarted {
                run_id: received_run_id,
                objective,
                ..
            } => {
                assert_eq!(received_run_id, run_id);
                assert_eq!(objective, "objective");
            }
            other => panic!("expected run started event, got {other:?}"),
        }

        match rx.try_recv().expect("task A discovery") {
            StreamEvent::TaskDiscovered {
                task_id,
                task_name,
                depends_on,
            } => {
                assert_eq!(task_id, task_a);
                assert_eq!(task_name, "tranche-001");
                assert!(depends_on.is_empty());
            }
            other => panic!("expected task discovered event, got {other:?}"),
        }

        match rx.try_recv().expect("task B discovery") {
            StreamEvent::TaskDiscovered {
                task_id,
                task_name,
                depends_on,
            } => {
                assert_eq!(task_id, task_b);
                assert_eq!(task_name, "tranche-002");
                assert!(depends_on.is_empty());
            }
            other => panic!("expected task discovered event, got {other:?}"),
        }
    }

    #[test]
    fn render_run_explain_reads_persisted_gate_failures() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.tests_passed: 2 test(s) failing",
                    "failures": ["gate.tests_passed: 2 test(s) failing"],
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.required_tasks_closed: 1 required task(s) not complete",
                    "failures": ["gate.required_tasks_closed: 1 required task(s) not complete"],
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(output.contains("Failed gates: 2"));
        assert!(output.contains("gate.tests_passed"));
        assert!(output.contains("gate.required_tasks_closed"));
    }

    #[test]
    fn render_task_explain_reads_persisted_gate_failures() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.required_evidence_present: no evidence records found",
                    "failures": ["gate.required_evidence_present: no evidence records found"],
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(output.contains("Task explain for"));
        assert!(output.contains("Failed gates: 1"));
        assert!(output.contains("gate.required_evidence_present"));
        assert!(output.contains("no evidence records found"));
    }

    #[test]
    fn render_worktree_status_reads_persisted_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let worktree_id = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.bound",
                corr,
                serde_json::json!({ "to": "WtBoundHome" }),
            ))
            .unwrap();

        let output = render_worktree_status(&store, run_id).unwrap();
        assert!(output.contains(&worktree_id.to_string()));
        assert!(output.contains("WtBoundHome"));
    }

    #[test]
    fn render_merge_status_reads_persisted_events() {
        let store = InMemoryEventStore::new();
        let merge_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({ "state": "MergeRequested", "reason": "pending approval" }),
            ))
            .unwrap();

        let output = render_merge_status(&store, merge_id).unwrap();
        assert!(output.contains("MergeRequested"));
        assert!(output.contains("pending approval"));
    }

    #[test]
    fn render_run_list_empty_store() {
        let store = InMemoryEventStore::new();
        let output = render_run_list(&store).unwrap();
        assert!(output.contains("No runs found"));
    }

    #[test]
    fn render_run_list_shows_active_run() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                corr,
                serde_json::json!({ "objective": "ship it" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let output = render_run_list(&store).unwrap();
        let map = unique_run_id_prefixes(vec![run_id.to_string()], 10);
        let prefix = map.get(&run_id.to_string()).unwrap();
        assert!(output.contains(prefix.as_str()));
        assert!(output.contains("RunActive"));
        assert!(output.contains("ship it"));
    }

    #[test]
    fn render_run_list_counts_tasks() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskExecuting", "to": "TaskComplete" }),
            ))
            .unwrap();

        let output = render_run_list(&store).unwrap();
        // 1 complete, 0 failed, 1 total => "1/0/1"
        assert!(output.contains("1/0/1"));
    }

    #[test]
    fn resolve_run_id_input_accepts_unique_prefix() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::parse_str("019c5056-d8a7-7133-9ad0-77652b8be1e8").unwrap();
        let corr = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let resolved = resolve_run_id_input(&store, "019c5056d8a7").unwrap();
        assert_eq!(resolved, run_id);
    }

    #[test]
    fn resolve_run_id_input_rejects_ambiguous_prefix() {
        let store = InMemoryEventStore::new();
        let a = Uuid::parse_str("019c4f51-aaaa-7000-8000-000000000001").unwrap();
        let b = Uuid::parse_str("019c4f51-bbbb-7000-8000-000000000002").unwrap();
        let corr_a = Uuid::now_v7();
        let corr_b = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                a.to_string(),
                "run.activated",
                corr_a,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                b.to_string(),
                "run.activated",
                corr_b,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let err = resolve_run_id_input(&store, "019c4f51").unwrap_err();
        assert!(err.to_string().contains("ambiguous run ID prefix"));
    }

    #[test]
    fn resolve_run_id_input_rejects_unknown_prefix() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let err = resolve_run_id_input(&store, "deadbeef").unwrap_err();
        assert!(err.to_string().contains("unique run-list prefix"));
    }

    #[test]
    fn unique_run_id_prefixes_expand_on_collision() {
        let a = "019c4f51-aaaa-7000-8000-000000000001".to_string();
        let b = "019c4f51-bbbb-7000-8000-000000000002".to_string();
        let map = unique_run_id_prefixes(vec![a.clone(), b.clone()], 10);
        let pa = map.get(&a).unwrap();
        let pb = map.get(&b).unwrap();
        assert_ne!(pa, pb);
        assert!(compact_run_id(&a).starts_with(pa));
        assert!(compact_run_id(&b).starts_with(pb));
    }

    #[test]
    fn render_run_status_surfaces_budget_exceeded_and_token_usage() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    },
                    "command_resource_usage": {
                        "max_rss_bytes": 1048576,
                        "cpu_user_ticks": 100,
                        "cpu_system_ticks": 50,
                        "io_read_bytes": 4096,
                        "io_write_bytes": 2048
                    }
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("budget_exceeded"),
            "status output must surface budget_exceeded: {output}"
        );
        assert!(
            output.contains("prompt_tokens=2500"),
            "status output must surface prompt_tokens: {output}"
        );
        assert!(
            output.contains("completion_tokens=2500"),
            "status output must surface completion_tokens: {output}"
        );
        assert!(
            output.contains("total_tokens=5000"),
            "status output must surface total_tokens: {output}"
        );
        assert!(
            output.contains("max_rss_bytes=1048576"),
            "status output must surface resource_usage: {output}"
        );
    }

    #[test]
    fn render_run_explain_surfaces_budget_breach() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    }
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(
            output.contains("Budget breaches:"),
            "explain output must show budget breaches section: {output}"
        );
        assert!(
            output.contains("budget_exceeded") || output.contains("max_task_total_tokens"),
            "explain output must reference budget breach detail: {output}"
        );
    }

    #[test]
    fn render_run_status_surfaces_deterioration_report() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.deterioration",
                corr,
                serde_json::json!({
                    "score": 72.5,
                    "window_size": 32,
                    "factors": [
                        {
                            "name": "runtime_drift",
                            "impact": 0.8,
                            "detail": "runtime trend for repeated command keys"
                        }
                    ],
                    "trend": "deteriorating"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Deterioration: score=72.5"));
        assert!(output.contains("runtime_drift"));
        assert!(output.contains("Deteriorating"));
    }

    #[test]
    fn render_run_status_surfaces_memory_hints() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.memory_hints",
                corr,
                serde_json::json!({
                    "query_text": "objective: verify",
                    "limit": 8,
                    "results": [
                        {
                            "memory_id": "m1",
                            "scope_id": "project/test",
                            "memory_class": "semantic",
                            "relevance_score": 0.9,
                            "content_snippet": "remember to run migrations",
                            "metadata": { "fingerprint": "abc" }
                        }
                    ]
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.observer.memory_hints",
                corr,
                serde_json::json!({
                    "query_text": "failed reason=budget_exceeded",
                    "limit": 8,
                    "results": [
                        {
                            "memory_id": "m2",
                            "scope_id": "project/test",
                            "memory_class": "semantic",
                            "relevance_score": 0.2,
                            "content_snippet": "previous budget exceeded fix: lower parallelism",
                            "metadata": {}
                        }
                    ]
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Memories: 1 hint(s)"));
        assert!(output.contains("remember to run migrations"));
        assert!(output.contains("memory_hints: 1"));
    }

    #[test]
    fn render_run_explain_surfaces_deterioration_report() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.deterioration",
                corr,
                serde_json::json!({
                    "score": 58.0,
                    "window_size": 20,
                    "factors": [
                        {
                            "name": "failure_rate_drift",
                            "impact": 0.6,
                            "detail": "failure bucket rate drift"
                        }
                    ],
                    "trend": "deteriorating"
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(output.contains("Sequence deterioration: score=58.0"));
        assert!(output.contains("failure_rate_drift"));
        assert!(output.contains("Deteriorating"));
    }

    #[test]
    fn render_task_explain_surfaces_budget_breach_and_token_usage() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    },
                    "command_resource_usage": {
                        "max_rss_bytes": 1048576
                    }
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(
            output.contains("budget_exceeded"),
            "task explain must surface budget_exceeded: {output}"
        );
        assert!(
            output.contains("prompt_tokens=2500"),
            "task explain must surface prompt_tokens: {output}"
        );
        assert!(
            output.contains("completion_tokens=2500"),
            "task explain must surface completion_tokens: {output}"
        );
        assert!(
            output.contains("total_tokens=5000"),
            "task explain must surface total_tokens: {output}"
        );
        assert!(
            output.contains("max_rss_bytes=1048576"),
            "task explain must surface resource_usage: {output}"
        );
    }

    #[test]
    fn task_annotate_persists_and_displays_in_explain() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        // Create a task event first.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.started",
                corr,
                serde_json::json!({"from": "TaskOpen", "to": "TaskReady"}),
            ))
            .unwrap();

        // Annotate the task.
        let result = execute_task_annotate(&store, task_id, "see blocker-001.md").unwrap();
        assert!(
            result.contains("blocker-001.md"),
            "annotate result must contain detail: {result}"
        );

        // Verify explain output includes the annotation.
        let explain_output = render_task_explain(&store, task_id).unwrap();
        assert!(
            explain_output.contains("blocker-001.md"),
            "explain must show blocker detail: {explain_output}"
        );
    }

    #[test]
    fn task_annotate_nonexistent_task_returns_not_found() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let result = execute_task_annotate(&store, task_id, "detail").unwrap();
        assert!(
            result.contains("not found"),
            "should report not found: {result}"
        );
    }

    #[test]
    fn run_status_displays_blocker_detail() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        // Create run event.
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.started",
                corr,
                serde_json::json!({"from": "RunOpen", "to": "RunActive"}),
            ))
            .unwrap();

        // Create task event linked by correlation.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.started",
                corr,
                serde_json::json!({"from": "TaskOpen", "to": "TaskReady"}),
            ))
            .unwrap();

        // Annotate task.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.annotated",
                corr,
                serde_json::json!({"blocker_detail": "see blocker-002.md"}),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("blocker-002.md"),
            "run status must show blocker detail: {output}"
        );
    }

    #[test]
    fn run_status_surfaces_merge_finalization_blockers() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.parallel_merge_failed",
                corr,
                serde_json::json!({
                    "reason": "parallel merge did not finalize due to conflict markers",
                    "workspace_root": "/tmp/yarli-runs/demo",
                    "task_key": "task-alpha",
                    "patch_path": "/tmp/yarli-runs/demo/patches/task-alpha.patch",
                    "conflicted_files": ["src/lib.rs", "src/main.rs"],
                    "recovery_hints": [
                        "Inspect conflicted files: git -C \"/tmp/yarli-runs/demo\" diff --name-only --diff-filter=U",
                        "Review task patch: git -C \"/tmp/yarli-runs/demo\" apply --stat \"/tmp/yarli-runs/demo/patches/task-alpha.patch\"",
                        "Retry patch manually: git -C \"/tmp/yarli-runs/demo\" apply --3way --whitespace=nowarn \"/tmp/yarli-runs/demo/patches/task-alpha.patch\""
                    ],
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("Merge-finalization blockers:"),
            "run status must show merge finalization blocker header: {output}"
        );
        assert!(
            output.contains("parallel merge did not finalize due to conflict markers"),
            "run status must include blocker detail: {output}"
        );
        assert!(
            output.contains("/tmp/yarli-runs/demo"),
            "run status must include blocker workspace path"
        );
        assert!(
            output.contains("task_key=task-alpha"),
            "run status must include task key"
        );
        assert!(
            output.contains("conflicted files:"),
            "run status must include conflicted files block"
        );
        assert!(
            output.contains("  - src/lib.rs"),
            "run status must list conflicted file"
        );
        assert!(
            output.contains("recovery_hints:"),
            "run status must include recovery hints block"
        );
        assert!(
            output.contains("Retry patch manually: git -C \"/tmp/yarli-runs/demo\" apply --3way --whitespace=nowarn"),
            "run status must include recovery hint"
        );
    }

    #[test]
    fn task_explain_displays_last_error() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "nonzero_exit",
                    "detail": "command exited with code 1"
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(
            output.contains("Last error: command exited with code 1"),
            "explain must show last_error: {output}"
        );
    }

    #[test]
    fn run_status_displays_last_error() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.started",
                corr,
                serde_json::json!({"from": "RunOpen", "to": "RunActive"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "nonzero_exit",
                    "detail": "command exited with code 42"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("last_error: command exited with code 42"),
            "run status must show last_error: {output}"
        );
    }

    #[test]
    fn render_run_explain_surfaces_merge_finalization_blockers() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.parallel_merge_failed",
                corr,
                serde_json::json!({
                    "reason": "parallel merge did not finalize due to conflict markers",
                    "workspace_root": "/tmp/yarli-runs/demo",
                    "task_key": "task-alpha",
                    "patch_path": "/tmp/yarli-runs/demo/patches/task-alpha.patch",
                    "conflicted_files": ["src/lib.rs", "src/main.rs"],
                    "recovery_hints": [
                        "Inspect conflicted files: git -C \"/tmp/yarli-runs/demo\" diff --name-only --diff-filter=U",
                        "Review task patch: git -C \"/tmp/yarli-runs/demo\" apply --stat \"/tmp/yarli-runs/demo/patches/task-alpha.patch\"",
                        "Retry patch manually: git -C \"/tmp/yarli-runs/demo\" apply --3way --whitespace=nowarn \"/tmp/yarli-runs/demo/patches/task-alpha.patch\""
                    ],
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(
            output.contains("Merge-finalization blockers: 1"),
            "run explain must show blocker count: {output}"
        );
        assert!(
            output.contains("parallel merge did not finalize due to conflict markers"),
            "run explain must include blocker detail: {output}"
        );
        assert!(
            output.contains("/tmp/yarli-runs/demo"),
            "run explain must include blocker workspace path"
        );
        assert!(
            output.contains("task_key=task-alpha"),
            "run explain must include task key"
        );
        assert!(
            output.contains("conflicted files:"),
            "run explain must include conflicted files block"
        );
        assert!(
            output.contains("  - src/main.rs"),
            "run explain must list conflicted file"
        );
        assert!(
            output.contains("recovery_hints:"),
            "run explain must include recovery hints block"
        );
    }

    #[test]
    fn run_status_and_explain_surface_cancel_provenance_summary() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::new_v4();
        let corr = Uuid::new_v4();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({
                    "from": "RunOpen",
                    "to": "RunActive",
                    "reason": "scheduler start"
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.cancelled",
                corr,
                serde_json::json!({
                    "from": "RunActive",
                    "to": "RunCancelled",
                    "reason": "cancelled by SIGTERM",
                    "exit_reason": "cancelled_by_operator",
                    "cancellation_source": "sigterm",
                    "cancellation_provenance": {
                        "cancellation_source": "sigterm",
                        "signal_name": "SIGTERM",
                        "signal_number": 15,
                        "sender_pid": null,
                        "receiver_pid": 4321,
                        "parent_pid": 1,
                        "process_group_id": 4321,
                        "session_id": 4321,
                        "tty": null,
                        "actor_kind": "supervisor",
                        "actor_detail": "SIGTERM likely originated from a supervisor",
                        "stage": "executing"
                    }
                }),
            ))
            .unwrap();

        let status = render_run_status(&store, run_id).unwrap();
        assert!(
            status.contains("Cancellation source: sigterm"),
            "status must show cancellation source: {status}"
        );
        assert!(
            status.contains(
                "Cancellation provenance: signal=SIGTERM sender=unknown receiver=yarli(4321) actor=supervisor stage=executing"
            ),
            "status must show provenance summary: {status}"
        );

        let explain = render_run_explain(&store, run_id).unwrap();
        assert!(
            explain.contains(
                "Cancellation provenance: signal=SIGTERM sender=unknown receiver=yarli(4321) actor=supervisor stage=executing"
            ),
            "explain must show provenance summary: {explain}"
        );
    }
}
