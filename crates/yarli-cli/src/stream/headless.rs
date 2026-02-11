//! Headless renderer — structured output to stderr when no TTY is available.
//!
//! Replaces the silent drain loop for non-TTY environments (CI, pipes,
//! redirected output). Logs task state transitions, forwards command output,
//! and prints a final run summary with pass/fail counts.

use std::io::{self, Write};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::events::StreamEvent;
use yarli_core::fsm::task::TaskState;

/// Counts for the final run summary.
#[derive(Default)]
struct RunSummary {
    tasks_complete: u32,
    tasks_failed: u32,
    tasks_cancelled: u32,
    transitions: u32,
}

/// A renderer that writes structured log lines to stderr.
///
/// Used when neither stream nor dashboard mode can initialize
/// (no TTY, too small terminal, etc.).
pub struct HeadlessRenderer {
    summary: RunSummary,
}

impl HeadlessRenderer {
    pub fn new() -> Self {
        Self {
            summary: RunSummary::default(),
        }
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
                let exit_str = exit_code
                    .map(|c| format!(", exit {c}"))
                    .unwrap_or_default();
                let detail_str = detail
                    .map(|d| format!(", {d}"))
                    .unwrap_or_default();
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
                let reason_str = reason
                    .map(|r| format!(" ({r})"))
                    .unwrap_or_default();
                let line = format!(
                    "{time_str} run/{} {from:?} -> {to:?}{reason_str}",
                    &run_id.to_string()[..8]
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
            }
            StreamEvent::TaskWorker {
                task_id: _,
                worker_id: _,
            } => {
                // No output needed for headless mode.
            }
            StreamEvent::Tick => {
                // No spinners in headless mode.
            }
        }
    }

    fn print_summary(&self) {
        let s = &self.summary;
        let status = if s.tasks_failed > 0 { "FAILED" } else { "OK" };
        let line = format!(
            "--- Run {status}: {} complete, {} failed, {} cancelled ({} transitions) ---",
            s.tasks_complete, s.tasks_failed, s.tasks_cancelled, s.transitions
        );
        info!("{}", line);
        let _ = writeln!(io::stderr(), "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;
    use uuid::Uuid;
    use yarli_core::fsm::run::RunState;

    fn make_renderer_and_channel() -> (HeadlessRenderer, mpsc::UnboundedSender<StreamEvent>) {
        let (tx, _rx) = mpsc::unbounded_channel();
        let renderer = HeadlessRenderer::new();
        (renderer, tx)
    }

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
}
