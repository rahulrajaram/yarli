//! Stream mode renderer using ratatui `Viewport::Inline`.
//!
//! Bottom N lines are a live status area with braille spinners.
//! Completed transitions are pushed above the viewport via `insert_before()`
//! into native terminal scrollback (copy-pasteable). Section 30.

use std::collections::HashMap;
use std::io::{self, Stdout};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ratatui::backend::CrosstermBackend;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};
use ratatui::{Terminal, TerminalOptions, Viewport};

use yarli_core::domain::TaskId;
use yarli_core::fsm::task::TaskState;

use super::events::{StreamEvent, TaskView};
use super::spinner::{Spinner, GLYPH_BLOCKED, GLYPH_COMPLETE, GLYPH_FAILED, GLYPH_PENDING};
use super::style::Tier;

/// Default number of lines for the inline viewport.
const DEFAULT_VIEWPORT_HEIGHT: u16 = 8;

/// Configuration for the stream renderer.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Height of the inline viewport in lines.
    pub viewport_height: u16,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            viewport_height: DEFAULT_VIEWPORT_HEIGHT,
        }
    }
}

/// The stream mode renderer.
///
/// Manages an inline viewport at the bottom of the terminal showing
/// active task status with spinners, and pushes completed transitions
/// to the native scrollback above.
pub struct StreamRenderer {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    /// Active tasks being tracked in the viewport.
    tasks: HashMap<TaskId, TaskView>,
    /// Insertion order of task IDs for stable rendering.
    task_order: Vec<TaskId>,
    /// Per-task spinners.
    spinners: HashMap<TaskId, Spinner>,
    /// Current "Why Not Done?" summary.
    explain_summary: Option<String>,
    /// Transient status message (shown only in viewport).
    transient_status: Option<String>,
}

impl StreamRenderer {
    /// Create a new stream renderer with inline viewport.
    pub fn new(config: StreamConfig) -> io::Result<Self> {
        let backend = CrosstermBackend::new(io::stdout());
        let terminal = Terminal::with_options(
            backend,
            TerminalOptions {
                viewport: Viewport::Inline(config.viewport_height),
            },
        )?;

        Ok(Self {
            terminal,
            tasks: HashMap::new(),
            task_order: Vec::new(),
            spinners: HashMap::new(),
            explain_summary: None,
            transient_status: None,
        })
    }

    /// Process a stream event: update state, push scrollback, refresh viewport.
    pub fn handle_event(&mut self, event: StreamEvent) -> io::Result<()> {
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
                self.handle_task_transition(
                    task_id, &task_name, from, to, elapsed, exit_code, detail.as_deref(), at,
                )?;
            }
            StreamEvent::RunTransition {
                run_id,
                from,
                to,
                reason,
                at,
            } => {
                self.push_run_transition(run_id, from, to, reason.as_deref(), at)?;
            }
            StreamEvent::CommandOutput {
                task_id,
                task_name: _,
                line,
            } => {
                if let Some(view) = self.tasks.get_mut(&task_id) {
                    view.last_output_line = Some(line);
                }
            }
            StreamEvent::TransientStatus { message } => {
                self.transient_status = Some(message);
            }
            StreamEvent::ExplainUpdate { summary } => {
                self.explain_summary = Some(summary);
            }
            StreamEvent::TaskWorker {
                task_id,
                worker_id,
            } => {
                if let Some(view) = self.tasks.get_mut(&task_id) {
                    view.worker_id = Some(worker_id);
                }
            }
            StreamEvent::Tick => {
                for spinner in self.spinners.values_mut() {
                    spinner.tick();
                }
            }
        }

        self.draw_viewport()?;
        Ok(())
    }

    /// Handle a task state transition: push to scrollback and update viewport state.
    fn handle_task_transition(
        &mut self,
        task_id: TaskId,
        task_name: &str,
        from: TaskState,
        to: TaskState,
        elapsed: Option<Duration>,
        exit_code: Option<i32>,
        detail: Option<&str>,
        at: DateTime<Utc>,
    ) -> io::Result<()> {
        // Push transition line to scrollback (permanent, copy-pasteable).
        self.push_task_transition(task_id, task_name, from, to, elapsed, exit_code, detail, at)?;

        if to.is_terminal() {
            // Remove from active viewport.
            self.tasks.remove(&task_id);
            self.task_order.retain(|id| *id != task_id);
            self.spinners.remove(&task_id);
        } else {
            // Insert into task_order if new.
            if !self.tasks.contains_key(&task_id) {
                self.task_order.push(task_id);
                self.tasks.insert(
                    task_id,
                    TaskView {
                        task_id,
                        name: task_name.to_string(),
                        state: to,
                        elapsed,
                        last_output_line: None,
                        blocked_by: None,
                        worker_id: None,
                    },
                );
            } else {
                let view = self.tasks.get_mut(&task_id).unwrap();
                view.state = to;
                view.elapsed = elapsed;
            }

            if to == TaskState::TaskExecuting {
                self.spinners.entry(task_id).or_insert_with(Spinner::new);
            }
        }

        Ok(())
    }

    /// Push a task transition line above the viewport into scrollback.
    ///
    /// Format (Section 33):
    /// `14:32:01 ▸ task/build  EXECUTING → COMPLETE  (34.2s, exit 0)`
    fn push_task_transition(
        &mut self,
        _task_id: TaskId,
        task_name: &str,
        from: TaskState,
        to: TaskState,
        elapsed: Option<Duration>,
        exit_code: Option<i32>,
        detail: Option<&str>,
        at: DateTime<Utc>,
    ) -> io::Result<()> {
        let tier = tier_for_task_state(to);
        let time_str = at.format("%H:%M:%S").to_string();

        let mut spans = vec![
            Span::styled(time_str, Tier::Contextual.style()),
            Span::styled(" ▸ ", Tier::Background.style()),
            Span::styled(format!("task/{:<16}", task_name), tier.style()),
            Span::styled(format!("{:?}", from), Tier::Contextual.style()),
            Span::styled(" → ", Tier::Background.style()),
            Span::styled(format!("{:?}", to), tier.style()),
        ];

        // Append timing/exit info.
        let mut meta_parts = Vec::new();
        if let Some(d) = elapsed {
            meta_parts.push(format_duration(d));
        }
        if let Some(code) = exit_code {
            meta_parts.push(format!("exit {code}"));
        }
        if let Some(d) = detail {
            meta_parts.push(d.to_string());
        }
        if !meta_parts.is_empty() {
            spans.push(Span::styled(
                format!("  ({})", meta_parts.join(", ")),
                Tier::Contextual.style(),
            ));
        }

        let line = Line::from(spans);

        self.terminal.insert_before(1, |buf| {
            Paragraph::new(line).render(buf.area, buf);
        })?;

        Ok(())
    }

    /// Push a run transition line to scrollback.
    fn push_run_transition(
        &mut self,
        run_id: uuid::Uuid,
        from: yarli_core::fsm::run::RunState,
        to: yarli_core::fsm::run::RunState,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> io::Result<()> {
        let tier = tier_for_run_state(to);
        let time_str = at.format("%H:%M:%S").to_string();
        let short_id = &run_id.to_string()[..8];

        let mut spans = vec![
            Span::styled(time_str, Tier::Contextual.style()),
            Span::styled(" ▸ ", Tier::Background.style()),
            Span::styled(format!("run/{:<16}", short_id), tier.style()),
            Span::styled(format!("{:?}", from), Tier::Contextual.style()),
            Span::styled(" → ", Tier::Background.style()),
            Span::styled(format!("{:?}", to), tier.style()),
        ];

        if let Some(r) = reason {
            spans.push(Span::styled(
                format!("  (reason: {r})"),
                Tier::Contextual.style(),
            ));
        }

        let line = Line::from(spans);

        self.terminal.insert_before(1, |buf| {
            Paragraph::new(line).render(buf.area, buf);
        })?;

        Ok(())
    }

    /// Redraw the inline viewport with current task status.
    fn draw_viewport(&mut self) -> io::Result<()> {
        let tasks: Vec<_> = self
            .task_order
            .iter()
            .filter_map(|id| self.tasks.get(id))
            .cloned()
            .collect();
        let spinners = &self.spinners;
        let transient = self.transient_status.take();
        let explain = self.explain_summary.clone();

        self.terminal.draw(|frame| {
            let area = frame.area();
            let mut lines = Vec::new();

            // Render each tracked task.
            for task in &tasks {
                let (glyph, tier) = match task.state {
                    TaskState::TaskExecuting => {
                        let sp = spinners
                            .get(&task.task_id)
                            .map(|s| s.frame())
                            .unwrap_or('⠋');
                        (sp, Tier::Active)
                    }
                    TaskState::TaskWaiting => ('⠿', Tier::Active),
                    TaskState::TaskBlocked => (GLYPH_BLOCKED, Tier::Contextual),
                    TaskState::TaskReady => (GLYPH_PENDING, Tier::Contextual),
                    TaskState::TaskOpen => (GLYPH_PENDING, Tier::Contextual),
                    TaskState::TaskComplete => (GLYPH_COMPLETE, Tier::Contextual),
                    TaskState::TaskFailed => (GLYPH_FAILED, Tier::Urgent),
                    TaskState::TaskCancelled => (GLYPH_BLOCKED, Tier::Contextual),
                    TaskState::TaskVerifying => (GLYPH_PENDING, Tier::Active),
                };

                let elapsed_str = task
                    .elapsed
                    .map(|d| format!("{}s", d.as_secs()))
                    .unwrap_or_default();

                let mut spans = vec![
                    Span::styled("  ", Style::default()),
                    Span::styled(format!("{glyph} "), tier.style()),
                    Span::styled(format!("task/{:<14}", task.name), tier.style()),
                    Span::styled(
                        format!("{:<8}", elapsed_str),
                        Tier::Contextual.style(),
                    ),
                ];

                // Show last output line for executing tasks.
                if let Some(ref output) = task.last_output_line {
                    let max_len = area.width.saturating_sub(40) as usize;
                    let truncated = if output.len() > max_len {
                        &output[..max_len]
                    } else {
                        output.as_str()
                    };
                    spans.push(Span::styled(truncated.to_string(), tier.accent()));
                } else if task.state == TaskState::TaskBlocked {
                    if let Some(ref by) = task.blocked_by {
                        spans.push(Span::styled(
                            format!("blocked-by: {by}"),
                            Tier::Contextual.accent(),
                        ));
                    }
                } else if task.state == TaskState::TaskReady || task.state == TaskState::TaskOpen {
                    spans.push(Span::styled("waiting", Tier::Contextual.accent()));
                }

                lines.push(Line::from(spans));
            }

            // Transient status (only in viewport).
            if let Some(msg) = transient {
                lines.push(Line::from(vec![Span::styled(
                    format!("  {msg}"),
                    Tier::Background.style(),
                )]));
            }

            // "Why Not Done?" summary at bottom of viewport.
            if let Some(ref summary) = explain {
                lines.push(Line::from(vec![
                    Span::styled("  WHY: ", Tier::Urgent.accent()),
                    Span::styled(summary.clone(), Tier::Urgent.style()),
                ]));
            }

            let paragraph = Paragraph::new(lines);
            frame.render_widget(paragraph, area);
        })?;

        Ok(())
    }

    /// Get mutable access to the terminal (for cleanup, etc.).
    pub fn terminal_mut(&mut self) -> &mut Terminal<CrosstermBackend<Stdout>> {
        &mut self.terminal
    }
}

/// Determine visual tier for a task state.
fn tier_for_task_state(state: TaskState) -> Tier {
    match state {
        TaskState::TaskFailed => Tier::Urgent,
        TaskState::TaskBlocked => Tier::Contextual,
        TaskState::TaskExecuting | TaskState::TaskWaiting => Tier::Active,
        TaskState::TaskComplete => Tier::Contextual,
        _ => Tier::Contextual,
    }
}

/// Determine visual tier for a run state.
fn tier_for_run_state(state: yarli_core::fsm::run::RunState) -> Tier {
    use yarli_core::fsm::run::RunState;
    match state {
        RunState::RunFailed | RunState::RunBlocked => Tier::Urgent,
        RunState::RunActive | RunState::RunVerifying => Tier::Active,
        RunState::RunCompleted => Tier::Contextual,
        _ => Tier::Contextual,
    }
}

/// Format a Duration as human-readable (e.g. "3.2s", "1m 4s").
fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let millis = d.subsec_millis();
    if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs >= 10 {
        format!("{secs}s")
    } else {
        format!("{secs}.{millis_h}s", millis_h = millis / 100)
    }
}

// Unit tests for pure functions (renderer itself requires a terminal).
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_duration_short() {
        assert_eq!(format_duration(Duration::from_millis(3200)), "3.2s");
    }

    #[test]
    fn format_duration_medium() {
        assert_eq!(format_duration(Duration::from_secs(34)), "34s");
    }

    #[test]
    fn format_duration_long() {
        assert_eq!(format_duration(Duration::from_secs(64)), "1m 4s");
    }

    #[test]
    fn format_duration_zero() {
        assert_eq!(format_duration(Duration::ZERO), "0.0s");
    }

    #[test]
    fn tier_for_failed_task() {
        assert_eq!(tier_for_task_state(TaskState::TaskFailed), Tier::Urgent);
    }

    #[test]
    fn tier_for_executing_task() {
        assert_eq!(tier_for_task_state(TaskState::TaskExecuting), Tier::Active);
    }

    #[test]
    fn tier_for_complete_task() {
        assert_eq!(
            tier_for_task_state(TaskState::TaskComplete),
            Tier::Contextual
        );
    }

    #[test]
    fn tier_for_blocked_run() {
        use yarli_core::fsm::run::RunState;
        assert_eq!(tier_for_run_state(RunState::RunBlocked), Tier::Urgent);
    }

    #[test]
    fn tier_for_active_run() {
        use yarli_core::fsm::run::RunState;
        assert_eq!(tier_for_run_state(RunState::RunActive), Tier::Active);
    }

    #[test]
    fn tier_for_completed_run() {
        use yarli_core::fsm::run::RunState;
        assert_eq!(
            tier_for_run_state(RunState::RunCompleted),
            Tier::Contextual
        );
    }
}
