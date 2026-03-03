//! Headless renderer — structured output to stderr when no TTY is available.
//!
//! Replaces the silent drain loop for non-TTY environments (CI, pipes,
//! redirected output). Logs task state transitions, forwards command output,
//! and prints a final run summary with pass/fail counts.

use std::io::{self, Write};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::events::StreamEvent;
use yarli_core::domain::CancellationProvenance;
use yarli_core::entities::continuation::TaskHealthAction;
use yarli_core::explain::DeteriorationTrend;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;

/// Counts for the final run summary.
#[derive(Default)]
struct RunSummary {
    run_id: Option<uuid::Uuid>,
    run_state: Option<RunState>,
    tasks_complete: u32,
    tasks_failed: u32,
    tasks_cancelled: u32,
    transitions: u32,
}

/// A renderer that writes structured log lines to stderr.
///
/// Used when neither stream nor dashboard mode can initialize
/// (no TTY, too small terminal, etc.).
#[derive(Default)]
pub struct HeadlessRenderer {
    summary: RunSummary,
    last_transient_status_emit_at: Option<DateTime<Utc>>,
}

impl HeadlessRenderer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Consume events from the channel until it closes, writing structured
    /// output to stderr.
    pub fn run(mut self, mut rx: mpsc::UnboundedReceiver<StreamEvent>) {
        while let Some(event) = rx.blocking_recv() {
            self.handle_event(event);
        }
        self.print_summary();
    }

    fn handle_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::TaskDiscovered {
                task_id: _,
                task_name: _,
                depends_on: _,
            } => {
                // Catalog/discovery event only; no terminal transition yet.
            }
            StreamEvent::TaskTransition {
                task_id,
                task_name,
                from,
                to,
                elapsed,
                exit_code,
                detail,
                at,
            } => {
                self.summary.transitions += 1;
                match to {
                    TaskState::TaskComplete => self.summary.tasks_complete += 1,
                    TaskState::TaskFailed => self.summary.tasks_failed += 1,
                    TaskState::TaskCancelled => self.summary.tasks_cancelled += 1,
                    _ => {}
                }

                let elapsed_str = elapsed
                    .map(|d| format!(" ({:.1}s)", d.as_secs_f64()))
                    .unwrap_or_default();
                let exit_str = exit_code.map(|c| format!(", exit {c}")).unwrap_or_default();
                let detail_str = detail.map(|d| format!(", {d}")).unwrap_or_default();
                let time_str = at.format("%H:%M:%S");

                let line = format!(
                    "{time_str} task/{task_name} {from:?} -> {to:?}{elapsed_str}{exit_str}{detail_str} [{task_id}]"
                );

                if to == TaskState::TaskFailed {
                    warn!("{}", line);
                } else {
                    info!("{}", line);
                }
                let _ = writeln!(io::stderr(), "{line}");
            }
            StreamEvent::RunTransition {
                run_id,
                from,
                to,
                reason,
                at,
            } => {
                let time_str = at.format("%H:%M:%S");
                let reason_str = reason.map(|r| format!(" ({r})")).unwrap_or_default();
                let line = format!(
                    "{time_str} run/{} {from:?} -> {to:?}{reason_str}",
                    display_run_id(run_id)
                );
                if to.is_terminal() {
                    self.summary.run_state = Some(to);
                }
                info!("{}", line);
                let _ = writeln!(io::stderr(), "{line}");
            }
            StreamEvent::RunStarted {
                run_id,
                objective,
                at,
            } => {
                self.summary.run_id = Some(run_id);
                let time_str = at.format("%H:%M:%S");
                let line = format!(
                    "{time_str} run/{} started: {objective}",
                    display_run_id(run_id)
                );
                info!("{}", line);
                let _ = writeln!(io::stderr(), "{line}");
            }
            StreamEvent::CommandOutput {
                task_id: _,
                task_name,
                line,
            } => {
                let _ = writeln!(io::stderr(), "  [{task_name}] {line}");
            }
            StreamEvent::ExplainUpdate { summary } => {
                info!(summary = %summary, "explain update");
                let _ = writeln!(io::stderr(), "  WHY: {summary}");
            }
            StreamEvent::TransientStatus { message } => {
                debug!(message = %message, "transient status");
                let now = Utc::now();
                let should_emit = message.starts_with("operator ")
                    || self
                        .last_transient_status_emit_at
                        .map(|last| now.signed_duration_since(last).num_seconds() >= 30)
                        .unwrap_or(true);
                if should_emit {
                    self.last_transient_status_emit_at = Some(now);
                    let time_str = now.format("%H:%M:%S");
                    let line = format!("{time_str} status {message}");
                    let _ = writeln!(io::stderr(), "{line}");
                }
            }
            StreamEvent::TaskWorker {
                task_id: _,
                worker_id: _,
            } => {
                // No output needed for headless mode.
            }
            StreamEvent::RunExited { payload } => {
                self.summary.run_id = Some(payload.run_id);
                self.summary.run_state = Some(payload.exit_state);
                self.summary.tasks_complete = payload.summary.completed;
                self.summary.tasks_failed = payload.summary.failed;
                self.summary.tasks_cancelled = payload.summary.cancelled;
                if payload.exit_state == RunState::RunCancelled
                    || payload.cancellation_provenance.is_some()
                {
                    let summary =
                        format_cancel_provenance_summary(payload.cancellation_provenance.as_ref());
                    let _ = writeln!(io::stderr(), "  Cancel provenance: {summary}");
                }

                if let Some(quality_gate) = payload.quality_gate.as_ref() {
                    if matches!(
                        quality_gate.task_health_action,
                        TaskHealthAction::ForcePivot
                    ) {
                        if let Some(guidance) = force_pivot_guidance(quality_gate.trend.as_ref()) {
                            let _ = writeln!(io::stderr(), "{guidance}");
                        }
                    }
                    if matches!(
                        quality_gate.task_health_action,
                        TaskHealthAction::StopAndSummarize
                    ) {
                        let guidance =
                            format!("  Stop-and-summarize guidance: {}", quality_gate.reason);
                        let _ = writeln!(io::stderr(), "{guidance}");
                    }
                    if matches!(
                        quality_gate.task_health_action,
                        TaskHealthAction::CheckpointNow
                    ) {
                        let guidance =
                            format!("  Checkpoint-now guidance: {}", quality_gate.reason);
                        let _ = writeln!(io::stderr(), "{guidance}");
                    }
                }

                // Print machine-readable JSON to stdout (not stderr).
                if let Ok(json) = serde_json::to_string(&payload) {
                    let _ = writeln!(io::stdout(), "{json}");
                }
            }
            StreamEvent::Tick => {
                // No spinners in headless mode.
            }
        }
    }

    fn print_summary(&self) {
        let s = &self.summary;
        let status = match s.run_state {
            Some(RunState::RunCompleted) => "OK",
            Some(RunState::RunFailed | RunState::RunBlocked) => "FAILED",
            Some(RunState::RunCancelled) => "CANCELLED",
            Some(_) => "DONE",
            None => {
                if s.tasks_failed > 0 {
                    "FAILED"
                } else {
                    "OK"
                }
            }
        };
        let run_label = s
            .run_id
            .map(|id| format!(" [{}]", display_run_id(id)))
            .unwrap_or_default();
        let line = format!(
            "--- Run {status}{run_label}: {} complete, {} failed, {} cancelled ({} transitions) ---",
            s.tasks_complete, s.tasks_failed, s.tasks_cancelled, s.transitions
        );
        info!("{}", line);
        let _ = writeln!(io::stderr(), "{line}");
    }
}

fn display_run_id(run_id: uuid::Uuid) -> String {
    const RUN_ID_DISPLAY_LEN: usize = 12;
    let compact = run_id.simple().to_string();
    compact[..RUN_ID_DISPLAY_LEN.min(compact.len())].to_string()
}

fn format_cancel_provenance_summary(provenance: Option<&CancellationProvenance>) -> String {
    let signal = provenance
        .and_then(|p| p.signal_name.as_deref())
        .unwrap_or("unknown");
    let sender = provenance
        .and_then(|p| p.sender_pid)
        .map(|pid| pid.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let receiver = provenance
        .and_then(|p| p.receiver_pid)
        .map(|pid| format!("yarli({pid})"))
        .unwrap_or_else(|| "unknown".to_string());
    let actor = provenance
        .and_then(|p| p.actor_kind)
        .map(|kind| kind.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let stage = provenance
        .and_then(|p| p.stage)
        .map(|stage| stage.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    format!("signal={signal} sender={sender} receiver={receiver} actor={actor} stage={stage}")
}

fn force_pivot_guidance(trend: Option<&DeteriorationTrend>) -> Option<String> {
    if matches!(trend, Some(DeteriorationTrend::Deteriorating)) {
        Some(
            "  Force-pivot guidance: sequence quality is deteriorating; narrow scope and shift task focus before continuing."
                .to_string(),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;
    use uuid::Uuid;
    use yarli_core::fsm::run::RunState;

    #[test]
    fn headless_counts_complete_tasks() {
        let mut renderer = HeadlessRenderer::new();
        renderer.handle_event(StreamEvent::TaskTransition {
            task_id: Uuid::new_v4(),
            task_name: "build".into(),
            from: TaskState::TaskExecuting,
            to: TaskState::TaskComplete,
            elapsed: Some(Duration::from_secs(5)),
            exit_code: Some(0),
            detail: None,
            at: Utc::now(),
        });
        assert_eq!(renderer.summary.tasks_complete, 1);
        assert_eq!(renderer.summary.tasks_failed, 0);
        assert_eq!(renderer.summary.transitions, 1);
    }

    #[test]
    fn headless_counts_failed_tasks() {
        let mut renderer = HeadlessRenderer::new();
        renderer.handle_event(StreamEvent::TaskTransition {
            task_id: Uuid::new_v4(),
            task_name: "test".into(),
            from: TaskState::TaskExecuting,
            to: TaskState::TaskFailed,
            elapsed: Some(Duration::from_secs(2)),
            exit_code: Some(1),
            detail: Some("nonzero exit".into()),
            at: Utc::now(),
        });
        assert_eq!(renderer.summary.tasks_failed, 1);
        assert_eq!(renderer.summary.tasks_complete, 0);
    }

    #[test]
    fn headless_forwards_command_output() {
        // Just verify it doesn't panic.
        let mut renderer = HeadlessRenderer::new();
        renderer.handle_event(StreamEvent::CommandOutput {
            task_id: Uuid::new_v4(),
            task_name: "build".into(),
            line: "Compiling yarli v0.1.0".into(),
        });
    }

    #[test]
    fn headless_records_run_id_from_run_started() {
        let mut renderer = HeadlessRenderer::new();
        let run_id = Uuid::new_v4();
        renderer.handle_event(StreamEvent::RunStarted {
            run_id,
            objective: "build everything".into(),
            at: Utc::now(),
        });
        assert_eq!(renderer.summary.run_id, Some(run_id));
        // RunStarted doesn't affect task counts.
        assert_eq!(renderer.summary.transitions, 0);
    }

    #[test]
    fn headless_summary_includes_run_id() {
        let mut renderer = HeadlessRenderer::new();
        let run_id = Uuid::new_v4();
        renderer.handle_event(StreamEvent::RunStarted {
            run_id,
            objective: "test".into(),
            at: Utc::now(),
        });
        renderer.handle_event(StreamEvent::TaskTransition {
            task_id: Uuid::new_v4(),
            task_name: "build".into(),
            from: TaskState::TaskExecuting,
            to: TaskState::TaskComplete,
            elapsed: None,
            exit_code: Some(0),
            detail: None,
            at: Utc::now(),
        });
        // Verify run_id is captured for summary.
        assert_eq!(renderer.summary.run_id, Some(run_id));
        assert_eq!(renderer.summary.tasks_complete, 1);
    }

    #[test]
    fn headless_handles_run_transition() {
        let mut renderer = HeadlessRenderer::new();
        renderer.handle_event(StreamEvent::RunTransition {
            run_id: Uuid::new_v4(),
            from: RunState::RunOpen,
            to: RunState::RunActive,
            reason: Some("started".into()),
            at: Utc::now(),
        });
        // Run transitions don't affect task counts.
        assert_eq!(renderer.summary.transitions, 0);
    }

    #[test]
    fn headless_summary_reflects_all_events() {
        let mut renderer = HeadlessRenderer::new();
        for _ in 0..3 {
            renderer.handle_event(StreamEvent::TaskTransition {
                task_id: Uuid::new_v4(),
                task_name: "task".into(),
                from: TaskState::TaskExecuting,
                to: TaskState::TaskComplete,
                elapsed: None,
                exit_code: Some(0),
                detail: None,
                at: Utc::now(),
            });
        }
        renderer.handle_event(StreamEvent::TaskTransition {
            task_id: Uuid::new_v4(),
            task_name: "fail".into(),
            from: TaskState::TaskExecuting,
            to: TaskState::TaskFailed,
            elapsed: None,
            exit_code: Some(1),
            detail: None,
            at: Utc::now(),
        });
        renderer.handle_event(StreamEvent::TaskTransition {
            task_id: Uuid::new_v4(),
            task_name: "cancel".into(),
            from: TaskState::TaskExecuting,
            to: TaskState::TaskCancelled,
            elapsed: None,
            exit_code: None,
            detail: None,
            at: Utc::now(),
        });

        assert_eq!(renderer.summary.tasks_complete, 3);
        assert_eq!(renderer.summary.tasks_failed, 1);
        assert_eq!(renderer.summary.tasks_cancelled, 1);
        assert_eq!(renderer.summary.transitions, 5);
    }

    #[test]
    fn headless_handles_run_exited_without_panic() {
        use yarli_core::domain::{CommandClass, SafeMode};
        use yarli_core::entities::continuation::ContinuationPayload;
        use yarli_core::entities::run::Run;
        use yarli_core::entities::task::Task;

        let run = Run::new("test", SafeMode::Execute);
        let mut t = Task::new(
            run.id,
            "build",
            "do build",
            CommandClass::Io,
            run.correlation_id,
        );
        t.state = TaskState::TaskComplete;
        let payload = ContinuationPayload::build(&run, &[&t]);

        let mut renderer = HeadlessRenderer::new();
        renderer.handle_event(StreamEvent::RunExited { payload });
        // No panic — success.
    }

    #[test]
    fn run_exited_summary_overrides_transition_failure_counts() {
        use yarli_core::entities::continuation::{ContinuationPayload, RunSummary, TrancheSpec};

        let mut renderer = HeadlessRenderer::new();
        let run_id = Uuid::new_v4();
        renderer.handle_event(StreamEvent::TaskTransition {
            task_id: Uuid::new_v4(),
            task_name: "retryable".into(),
            from: TaskState::TaskExecuting,
            to: TaskState::TaskFailed,
            elapsed: None,
            exit_code: Some(1),
            detail: None,
            at: Utc::now(),
        });

        renderer.handle_event(StreamEvent::RunExited {
            payload: ContinuationPayload {
                run_id,
                objective: "retry flow".into(),
                exit_state: RunState::RunCompleted,
                exit_reason: None,
                cancellation_source: None,
                cancellation_provenance: None,
                completed_at: Utc::now(),
                tasks: Vec::new(),
                summary: RunSummary {
                    total: 1,
                    completed: 1,
                    failed: 0,
                    cancelled: 0,
                    pending: 0,
                },
                next_tranche: Some(TrancheSpec {
                    suggested_objective: "next".into(),
                    kind: yarli_core::entities::continuation::TrancheKind::PlannedNext,
                    retry_task_keys: Vec::new(),
                    unfinished_task_keys: Vec::new(),
                    planned_task_keys: vec!["t2".into()],
                    planned_tranche_key: Some("t2".into()),
                    cursor: None,
                    config_snapshot: serde_json::json!({}),
                    interventions: Vec::new(),
                }),
                quality_gate: None,
                retry_recommendation: None,
            },
        });

        assert_eq!(renderer.summary.run_state, Some(RunState::RunCompleted));
        assert_eq!(renderer.summary.tasks_failed, 0);
    }
}
