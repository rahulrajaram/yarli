//! Stream mode renderer using ratatui `Viewport::Inline`.
//!
//! Bottom N lines are a live status area with braille spinners.
//! Completed transitions are pushed above the viewport via `insert_before()`
//! into native terminal scrollback (copy-pasteable). Section 30.

use std::collections::HashMap;
use std::io::{self, Stdout, Write};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ratatui::backend::CrosstermBackend;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};
use ratatui::{Terminal, TerminalOptions, Viewport};

use yarli_core::domain::CancellationProvenance;
use yarli_core::domain::TaskId;
use yarli_core::entities::continuation::TaskHealthAction;
use yarli_core::explain::DeteriorationTrend;
use yarli_core::fsm::task::TaskState;

use super::events::{StreamEvent, TaskView};
use super::spinner::{Spinner, GLYPH_BLOCKED, GLYPH_COMPLETE, GLYPH_FAILED, GLYPH_PENDING};
use super::style::Tier;

/// Default number of lines for the inline viewport.
const DEFAULT_VIEWPORT_HEIGHT: u16 = 8;
const RUN_ID_DISPLAY_LEN: usize = 12;
const TRANSIENT_STATUS_EMIT_SECS: i64 = 30;

/// Configuration for the stream renderer.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Height of the inline viewport in lines.
    pub viewport_height: u16,
    /// When true, stream command output lines to scrollback.
    pub verbose_output: bool,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            viewport_height: DEFAULT_VIEWPORT_HEIGHT,
            verbose_output: false,
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
    config: StreamConfig,
    /// Active tasks being tracked in the viewport.
    tasks: HashMap<TaskId, TaskView>,
    /// Latest known lifecycle state for each known task in the run.
    task_states: HashMap<TaskId, TaskState>,
    /// Insertion order of task IDs for stable rendering.
    task_order: Vec<TaskId>,
    /// Per-task spinners.
    spinners: HashMap<TaskId, Spinner>,
    /// Current "Why Not Done?" summary.
    explain_summary: Option<String>,
    /// Transient status message (shown only in viewport).
    transient_status: Option<String>,
    /// Last time we emitted a transient status line to scrollback.
    last_transient_status_emit_at: Option<DateTime<Utc>>,
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
            config,
            tasks: HashMap::new(),
            task_states: HashMap::new(),
            task_order: Vec::new(),
            spinners: HashMap::new(),
            explain_summary: None,
            transient_status: None,
            last_transient_status_emit_at: None,
        })
    }

    /// Process a stream event: update state, push scrollback, refresh viewport.
    pub fn handle_event(&mut self, event: StreamEvent) -> io::Result<()> {
        match event {
            StreamEvent::TaskDiscovered { task_id, task_name } => {
                if let std::collections::hash_map::Entry::Vacant(e) = self.tasks.entry(task_id) {
                    self.task_order.push(task_id);
                    e.insert(TaskView {
                        task_id,
                        name: task_name,
                        state: TaskState::TaskOpen,
                        elapsed: None,
                        last_output_line: None,
                        blocked_by: None,
                        worker_id: None,
                    });
                }
                self.task_states
                    .entry(task_id)
                    .or_insert(TaskState::TaskOpen);
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
                self.handle_task_transition(
                    task_id,
                    &task_name,
                    from,
                    to,
                    elapsed,
                    exit_code,
                    detail.as_deref(),
                    at,
                )?;
            }
            StreamEvent::RunTransition {
                run_id,
                from,
                to,
                reason,
                at,
            } => {
                let progress = self.progress_snapshot();
                self.push_run_transition(run_id, from, to, reason.as_deref(), at, progress)?;
            }
            StreamEvent::RunStarted {
                run_id,
                objective,
                at,
            } => {
                self.push_run_started(run_id, &objective, at)?;
            }
            StreamEvent::CommandOutput {
                task_id,
                task_name,
                line,
            } => {
                if self.config.verbose_output {
                    let output_line = Line::from(vec![
                        Span::styled(format!("  [{task_name}] "), Tier::Background.style()),
                        Span::styled(line.clone(), Tier::Contextual.style()),
                    ]);
                    self.terminal.insert_before(1, |buf| {
                        Paragraph::new(output_line).render(buf.area, buf);
                    })?;
                }
                if let Some(view) = self.tasks.get_mut(&task_id) {
                    view.last_output_line = Some(line);
                }
            }
            StreamEvent::TransientStatus { message } => {
                if should_emit_transient_status_line(
                    self.last_transient_status_emit_at,
                    &message,
                    Utc::now(),
                ) {
                    let progress = self.progress_snapshot();
                    self.push_transient_status_line(&message, Utc::now(), progress)?;
                }
                self.transient_status = Some(message);
            }
            StreamEvent::ExplainUpdate { summary } => {
                self.explain_summary = Some(summary);
            }
            StreamEvent::TaskWorker { task_id, worker_id } => {
                if let Some(view) = self.tasks.get_mut(&task_id) {
                    view.worker_id = Some(worker_id);
                }
            }
            StreamEvent::RunExited { payload } => {
                self.push_continuation_summary(&payload)?;
            }
            StreamEvent::Tick => {
                for spinner in self.spinners.values_mut() {
                    spinner.tick();
                }
            }
        }

        self.draw_viewport()?;
        // Flush stdout immediately so state transitions appear without delay.
        io::stdout().flush()?;
        Ok(())
    }

    /// Handle a task state transition: push to scrollback and update viewport state.
    #[allow(clippy::too_many_arguments)]
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
        self.task_states.insert(task_id, to);
        let progress = self.progress_snapshot();

        // Push transition line to scrollback (permanent, copy-pasteable).
        self.push_task_transition(
            task_id, task_name, from, to, elapsed, exit_code, detail, at, progress,
        )?;

        if to.is_terminal() {
            // Remove from active viewport.
            self.tasks.remove(&task_id);
            self.task_order.retain(|id| *id != task_id);
            self.spinners.remove(&task_id);
        } else {
            // Insert into task_order if new.
            if let std::collections::hash_map::Entry::Vacant(e) = self.tasks.entry(task_id) {
                self.task_order.push(task_id);
                e.insert(TaskView {
                    task_id,
                    name: task_name.to_string(),
                    state: to,
                    elapsed,
                    last_output_line: None,
                    blocked_by: None,
                    worker_id: None,
                });
            } else {
                let view = self.tasks.get_mut(&task_id).unwrap();
                view.state = to;
                view.elapsed = elapsed;
            }

            if to == TaskState::TaskExecuting {
                self.spinners.entry(task_id).or_default();
            }
        }

        Ok(())
    }

    /// Push a task transition line above the viewport into scrollback.
    ///
    /// Format (Section 33):
    /// `14:32:01 ▸ task/build  EXECUTING → COMPLETE  (34.2s, exit 0)`
    #[allow(clippy::too_many_arguments)]
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
        progress: ProgressSnapshot,
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
        spans.push(Span::styled(
            format!("  progress {}", format_ascii_progress(progress, 20)),
            Tier::Contextual.style(),
        ));

        let line = Line::from(spans);

        self.terminal.insert_before(1, |buf| {
            Paragraph::new(line).render(buf.area, buf);
        })?;

        Ok(())
    }

    /// Push a "run started" banner to scrollback.
    fn push_run_started(
        &mut self,
        run_id: uuid::Uuid,
        objective: &str,
        at: DateTime<Utc>,
    ) -> io::Result<()> {
        let time_str = at.format("%H:%M:%S").to_string();
        let display_id = display_run_id(run_id);

        let spans = vec![
            Span::styled(time_str, Tier::Contextual.style()),
            Span::styled(" ▸ ", Tier::Background.style()),
            Span::styled(format!("run/{display_id}"), Tier::Active.style()),
            Span::styled(format!(" started: {objective}"), Tier::Contextual.style()),
        ];

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
        progress: ProgressSnapshot,
    ) -> io::Result<()> {
        let tier = tier_for_run_state(to);
        let time_str = at.format("%H:%M:%S").to_string();
        let display_id = display_run_id(run_id);

        let mut spans = vec![
            Span::styled(time_str, Tier::Contextual.style()),
            Span::styled(" ▸ ", Tier::Background.style()),
            Span::styled(format!("run/{:<20}", display_id), tier.style()),
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
        spans.push(Span::styled(
            format!("  progress {}", format_ascii_progress(progress, 20)),
            Tier::Contextual.style(),
        ));

        let line = Line::from(spans);

        self.terminal.insert_before(1, |buf| {
            Paragraph::new(line).render(buf.area, buf);
        })?;

        Ok(())
    }

    /// Push a continuation summary block to scrollback.
    fn push_continuation_summary(
        &mut self,
        payload: &yarli_core::entities::ContinuationPayload,
    ) -> io::Result<()> {
        let s = &payload.summary;

        // Header line.
        let header = Line::from(vec![Span::styled(
            "── Continuation ──",
            Tier::Active.style(),
        )]);
        self.terminal.insert_before(1, |buf| {
            Paragraph::new(header).render(buf.area, buf);
        })?;

        // Counts line.
        let counts = format!(
            "  {} completed, {} failed, {} pending",
            s.completed, s.failed, s.pending
        );
        let counts_line = Line::from(vec![Span::styled(counts, Tier::Contextual.style())]);
        self.terminal.insert_before(1, |buf| {
            Paragraph::new(counts_line).render(buf.area, buf);
        })?;

        if let Some(reason) = payload.exit_reason {
            let reason_line = Line::from(vec![Span::styled(
                format!("  Exit reason: {reason}"),
                Tier::Contextual.style(),
            )]);
            self.terminal.insert_before(1, |buf| {
                Paragraph::new(reason_line).render(buf.area, buf);
            })?;
        }

        let cancelled = payload.exit_state == yarli_core::fsm::run::RunState::RunCancelled;
        if cancelled || payload.cancellation_source.is_some() {
            let source = payload
                .cancellation_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let source_line = Line::from(vec![Span::styled(
                format!("  Cancel source: {source}"),
                Tier::Contextual.style(),
            )]);
            self.terminal.insert_before(1, |buf| {
                Paragraph::new(source_line).render(buf.area, buf);
            })?;
        }

        if cancelled || payload.cancellation_provenance.is_some() {
            let summary =
                format_cancel_provenance_summary(payload.cancellation_provenance.as_ref());
            let provenance_line = Line::from(vec![Span::styled(
                format!("  Cancel provenance: {summary}"),
                Tier::Contextual.style(),
            )]);
            self.terminal.insert_before(1, |buf| {
                Paragraph::new(provenance_line).render(buf.area, buf);
            })?;
        }

        // Retry/next lines if there's a tranche.
        if let Some(tranche) = &payload.next_tranche {
            if !tranche.retry_task_keys.is_empty() {
                let retry = format!("  Retry: [{}]", tranche.retry_task_keys.join(", "));
                let retry_line = Line::from(vec![Span::styled(retry, Tier::Urgent.style())]);
                self.terminal.insert_before(1, |buf| {
                    Paragraph::new(retry_line).render(buf.area, buf);
                })?;
            }
            if !tranche.unfinished_task_keys.is_empty() {
                let unfinished = format!(
                    "  Unfinished: [{}]",
                    tranche.unfinished_task_keys.join(", ")
                );
                let unfinished_line =
                    Line::from(vec![Span::styled(unfinished, Tier::Contextual.style())]);
                self.terminal.insert_before(1, |buf| {
                    Paragraph::new(unfinished_line).render(buf.area, buf);
                })?;
            }
            let next = format!("  Next: \"{}\"", tranche.suggested_objective);
            let next_line = Line::from(vec![Span::styled(next, Tier::Active.style())]);
            self.terminal.insert_before(1, |buf| {
                Paragraph::new(next_line).render(buf.area, buf);
            })?;
        }

        if let Some(quality_gate) = payload.quality_gate.as_ref() {
            if matches!(quality_gate.task_health_action, TaskHealthAction::ForcePivot) {
                if let Some(guidance) = Self::force_pivot_guidance(quality_gate.trend.as_ref()) {
                    let guidance_line =
                        Line::from(vec![Span::styled(guidance, Tier::Urgent.style())]);
                    self.terminal.insert_before(1, |buf| {
                        Paragraph::new(guidance_line).render(buf.area, buf);
                    })?;
                }
            }
            if matches!(quality_gate.task_health_action, TaskHealthAction::StopAndSummarize) {
                let guidance = format!(
                    "  Stop-and-summarize guidance: {}",
                    quality_gate.reason
                );
                let guidance_line = Line::from(vec![Span::styled(guidance, Tier::Urgent.style())]);
                self.terminal.insert_before(1, |buf| {
                    Paragraph::new(guidance_line).render(buf.area, buf);
                })?;
            }
            if matches!(quality_gate.task_health_action, TaskHealthAction::CheckpointNow) {
                let guidance = format!("  Checkpoint-now guidance: {}", quality_gate.reason);
                let guidance_line = Line::from(vec![Span::styled(guidance, Tier::Urgent.style())]);
                self.terminal.insert_before(1, |buf| {
                    Paragraph::new(guidance_line).render(buf.area, buf);
                })?;
            }
        }

        Ok(())
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
                    Span::styled(format!("{:<8}", elapsed_str), Tier::Contextual.style()),
                ];

                // Show last output line for executing tasks.
                if let Some(ref output) = task.last_output_line {
                    let max_len = area.width.saturating_sub(40) as usize;
                    let truncated = if output.len() > max_len {
                        // Walk back to the nearest char boundary at or before max_len.
                        let mut end = max_len;
                        while end > 0 && !output.is_char_boundary(end) {
                            end -= 1;
                        }
                        &output[..end]
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

    fn push_transient_status_line(
        &mut self,
        message: &str,
        at: DateTime<Utc>,
        progress: ProgressSnapshot,
    ) -> io::Result<()> {
        self.last_transient_status_emit_at = Some(at);
        let spans = vec![
            Span::styled(at.format("%H:%M:%S").to_string(), Tier::Contextual.style()),
            Span::styled(" ▸ ", Tier::Background.style()),
            Span::styled("status", Tier::Active.style()),
            Span::styled(
                format!(
                    " {message}  progress {}",
                    format_ascii_progress(progress, 20)
                ),
                Tier::Contextual.style(),
            ),
        ];
        let line = Line::from(spans);
        self.terminal.insert_before(1, |buf| {
            Paragraph::new(line).render(buf.area, buf);
        })?;
        Ok(())
    }

    fn progress_snapshot(&self) -> ProgressSnapshot {
        let mut snapshot = ProgressSnapshot {
            total: self.task_states.len() as u32,
            ..ProgressSnapshot::default()
        };
        for state in self.task_states.values() {
            match state {
                TaskState::TaskComplete => snapshot.completed += 1,
                TaskState::TaskFailed => snapshot.failed += 1,
                TaskState::TaskCancelled => snapshot.cancelled += 1,
                _ => {}
            }
        }
        snapshot
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ProgressSnapshot {
    total: u32,
    completed: u32,
    failed: u32,
    cancelled: u32,
}

impl ProgressSnapshot {
    fn terminal_count(self) -> u32 {
        self.completed + self.failed + self.cancelled
    }
}

fn format_ascii_progress(snapshot: ProgressSnapshot, width: usize) -> String {
    let done = snapshot.terminal_count();
    let total = snapshot.total;
    let (filled, percent) = if total == 0 {
        (0usize, 0u32)
    } else {
        let filled = ((done as f64 / total as f64) * width as f64).round() as usize;
        let percent = ((done as f64 / total as f64) * 100.0).round() as u32;
        (filled.min(width), percent.min(100))
    };
    let bar = format!("{}{}", "#".repeat(filled), ".".repeat(width - filled));
    format!("[{bar}] {done}/{total} ({percent}%)")
}

fn should_emit_transient_status_line(
    last_emit_at: Option<DateTime<Utc>>,
    message: &str,
    now: DateTime<Utc>,
) -> bool {
    if message.starts_with("operator ") {
        return true;
    }
    let Some(last) = last_emit_at else {
        return true;
    };
    now.signed_duration_since(last).num_seconds() >= TRANSIENT_STATUS_EMIT_SECS
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

fn display_run_id(run_id: uuid::Uuid) -> String {
    let compact = run_id.simple().to_string();
    compact[..RUN_ID_DISPLAY_LEN.min(compact.len())].to_string()
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
        assert_eq!(tier_for_run_state(RunState::RunCompleted), Tier::Contextual);
    }

    #[test]
    fn display_run_id_uses_compact_prefix() {
        let run_id =
            uuid::Uuid::parse_str("019c4f51-5f70-7d84-a0c8-2f5c6bb8ef12").expect("valid uuid");
        assert_eq!(display_run_id(run_id), "019c4f515f70");
    }

    #[test]
    fn transient_status_line_throttles_regular_heartbeats() {
        let now = Utc::now();
        assert!(!should_emit_transient_status_line(
            Some(now),
            "heartbeat: pending=1 leased=1",
            now + chrono::Duration::seconds(10),
        ));
        assert!(should_emit_transient_status_line(
            Some(now),
            "heartbeat: pending=1 leased=1",
            now + chrono::Duration::seconds(31),
        ));
    }

    #[test]
    fn transient_status_line_always_emits_operator_messages() {
        let now = Utc::now();
        assert!(should_emit_transient_status_line(
            Some(now),
            "operator pause: maintenance",
            now + chrono::Duration::seconds(1),
        ));
    }

    #[test]
    fn ascii_progress_formats_empty_snapshot() {
        let snapshot = ProgressSnapshot::default();
        assert_eq!(format_ascii_progress(snapshot, 10), "[..........] 0/0 (0%)");
    }

    #[test]
    fn ascii_progress_formats_partial_completion() {
        let snapshot = ProgressSnapshot {
            total: 5,
            completed: 2,
            failed: 0,
            cancelled: 0,
        };
        assert_eq!(
            format_ascii_progress(snapshot, 10),
            "[####......] 2/5 (40%)"
        );
    }

    #[test]
    fn ascii_progress_counts_all_terminal_states() {
        let snapshot = ProgressSnapshot {
            total: 4,
            completed: 1,
            failed: 1,
            cancelled: 1,
        };
        assert_eq!(
            format_ascii_progress(snapshot, 10),
            "[########..] 3/4 (75%)"
        );
    }
}
