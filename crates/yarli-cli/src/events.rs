use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use yarli_cli::stream::StreamEvent;
use yarli_cli::stream::{normalize_output_lines, normalize_output_lines_with_options};
use yarli_cli::yarli_core::domain::Event;
use yarli_cli::yarli_core::fsm::run::RunState;
use yarli_cli::yarli_core::fsm::task::TaskState;

pub(crate) fn emit_initial_stream_state(
    tx: &mpsc::UnboundedSender<StreamEvent>,
    run_id: Uuid,
    objective: &str,
    task_names: &[(Uuid, String)],
) {
    let _ = tx.send(StreamEvent::RunStarted {
        run_id,
        objective: objective.to_string(),
        at: Utc::now(),
    });
    for (task_id, task_name) in task_names {
        let _ = tx.send(StreamEvent::TaskDiscovered {
            task_id: *task_id,
            task_name: task_name.clone(),
            depends_on: Vec::new(),
        });
    }
}

pub(crate) fn stream_task_catalog_entries(event: &Event) -> Vec<(Uuid, String, Vec<String>)> {
    if event.event_type != "run.task_catalog" {
        return Vec::new();
    }
    event
        .payload
        .get("tasks")
        .and_then(|tasks| tasks.as_array())
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| {
                    let task_id = task.get("task_id")?.as_str()?.parse::<Uuid>().ok()?;
                    let task_name = task
                        .get("task_key")
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| task_id.to_string()[..8].to_string());
                    let depends_on = task
                        .get("depends_on")
                        .and_then(|value| value.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.as_str().map(ToString::to_string))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    Some((task_id, task_name, depends_on))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Convert a domain Event to a StreamEvent for the renderer.
///
/// When `suppress_command_output` is true, `command.output` events are skipped
/// because those chunks were already forwarded via the live streaming channel.
pub(crate) fn event_to_stream_events(
    event: &Event,
    task_names: &[(Uuid, String)],
    suppress_command_output: bool,
    verbose_command_output: bool,
) -> Vec<StreamEvent> {
    let task_name = |entity_id: &str| -> String {
        if let Ok(id) = entity_id.parse::<Uuid>() {
            task_names
                .iter()
                .find(|(tid, _)| *tid == id)
                .map(|(_, name)| name.clone())
                .unwrap_or_else(|| entity_id[..8].to_string())
        } else {
            entity_id.to_string()
        }
    };

    match event.event_type.as_str() {
        "task.ready" | "task.executing" | "task.verifying" | "task.completed" | "task.failed"
        | "task.retrying" | "task.cancelled" => {
            let from_str = event.payload.get("from").and_then(|v| v.as_str());
            let to_str = event.payload.get("to").and_then(|v| v.as_str());

            let from = from_str
                .and_then(parse_task_state)
                .unwrap_or(TaskState::TaskOpen);
            let to = to_str
                .and_then(parse_task_state)
                .unwrap_or(TaskState::TaskOpen);

            let exit_code = event
                .payload
                .get("exit_code")
                .and_then(|v| v.as_i64())
                .map(|c| c as i32);

            let name = task_name(&event.entity_id);

            vec![StreamEvent::TaskTransition {
                task_id: event.entity_id.parse().unwrap_or(Uuid::nil()),
                task_name: name,
                from,
                to,
                elapsed: None,
                exit_code,
                detail: event
                    .payload
                    .get("detail")
                    .and_then(|v| v.as_str())
                    .or_else(|| event.payload.get("reason").and_then(|v| v.as_str()))
                    .map(String::from),
                at: event.occurred_at,
            }]
        }
        "run.activated" | "run.verifying" | "run.blocked" | "run.completed" | "run.failed"
        | "run.cancelled" => {
            let from_str = event.payload.get("from").and_then(|v| v.as_str());
            let to_str = event.payload.get("to").and_then(|v| v.as_str());

            let from = from_str
                .and_then(parse_run_state)
                .unwrap_or(RunState::RunOpen);
            let to = to_str
                .and_then(parse_run_state)
                .unwrap_or(RunState::RunOpen);

            let reason = event
                .payload
                .get("detail")
                .and_then(|v| v.as_str())
                .or_else(|| event.payload.get("reason").and_then(|v| v.as_str()))
                .map(String::from);

            let run_id = event.entity_id.parse().unwrap_or(Uuid::nil());

            vec![StreamEvent::RunTransition {
                run_id,
                from,
                to,
                reason,
                at: event.occurred_at,
            }]
        }
        "run.observer.progress" => {
            let summary = event
                .payload
                .get("summary")
                .and_then(|v| v.as_str())
                .unwrap_or("progress update")
                .to_string();
            vec![StreamEvent::TransientStatus { message: summary }]
        }
        "command.output" => {
            if suppress_command_output {
                return Vec::new();
            }
            let line = extract_command_output_line(&event.payload);
            if line.trim().is_empty() {
                return Vec::new();
            }
            let task_id = task_id_from_command_event(event).unwrap_or_else(Uuid::nil);
            let task_entity = if task_id == Uuid::nil() {
                event.entity_id.clone()
            } else {
                task_id.to_string()
            };
            let name = task_name(&task_entity);
            normalize_output_lines_with_options(&line, verbose_command_output)
                .into_iter()
                .map(|line| StreamEvent::CommandOutput {
                    task_id,
                    task_name: name.clone(),
                    line,
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

pub(crate) fn task_id_from_command_event(event: &Event) -> Option<Uuid> {
    let idempotency_key = event.idempotency_key.as_deref()?;
    let (task_id_raw, _) = idempotency_key.split_once(":cmd:")?;
    task_id_raw.parse().ok()
}

pub(crate) fn extract_command_output_line(payload: &Value) -> String {
    let chunk_lines = payload
        .get("chunks")
        .and_then(|value| value.as_array())
        .map(|chunks| {
            chunks
                .iter()
                .filter_map(|chunk| chunk.get("data").and_then(|value| value.as_str()))
                .flat_map(normalize_output_lines)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !chunk_lines.is_empty() {
        return chunk_lines.join("\n");
    }
    normalize_output_lines(
        payload
            .get("line")
            .and_then(|value| value.as_str())
            .unwrap_or(""),
    )
    .join("\n")
}

/// Parse a TaskState from its serialized form.
pub(crate) fn parse_task_state(s: &str) -> Option<TaskState> {
    match s {
        "TaskOpen" => Some(TaskState::TaskOpen),
        "TaskReady" => Some(TaskState::TaskReady),
        "TaskExecuting" => Some(TaskState::TaskExecuting),
        "TaskWaiting" => Some(TaskState::TaskWaiting),
        "TaskBlocked" => Some(TaskState::TaskBlocked),
        "TaskVerifying" => Some(TaskState::TaskVerifying),
        "TaskComplete" => Some(TaskState::TaskComplete),
        "TaskFailed" => Some(TaskState::TaskFailed),
        "TaskCancelled" => Some(TaskState::TaskCancelled),
        _ => None,
    }
}

/// Parse a RunState from its serialized form.
pub(crate) fn parse_run_state(s: &str) -> Option<RunState> {
    match s {
        "RunOpen" => Some(RunState::RunOpen),
        "RunActive" => Some(RunState::RunActive),
        "RunBlocked" => Some(RunState::RunBlocked),
        "RunVerifying" => Some(RunState::RunVerifying),
        "RunCompleted" => Some(RunState::RunCompleted),
        "RunFailed" => Some(RunState::RunFailed),
        "RunCancelled" => Some(RunState::RunCancelled),
        "RunDrained" => Some(RunState::RunDrained),
        _ => None,
    }
}
