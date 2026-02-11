//! Dashboard mode renderer — fullscreen TUI (Section 16.3).
//!
//! Manages a fullscreen terminal with panel layout: task list (left),
//! output viewer (right), gates panel, Why Not Done bar, and key hints.

use std::io::{self, Stdout};
use std::time::Duration;

use crossterm::event::{self, Event, KeyEvent};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Terminal;

use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;

use super::copy_mode::CopyMode;
use super::input::{map_key_event, DashboardAction};
use super::overlay::{OverlayEntry, OverlayStack};
use super::state::{PanelId, PanelManager, PanelState};
use super::widgets::CollapsiblePanel;
use crate::stream::events::StreamEvent;
use crate::stream::spinner::{Spinner, GLYPH_BLOCKED, GLYPH_COMPLETE, GLYPH_FAILED, GLYPH_PENDING};
use crate::stream::style::Tier;

/// Configuration for the dashboard renderer.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    /// Tick rate for UI refresh (milliseconds).
    pub tick_rate_ms: u64,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self { tick_rate_ms: 100 }
    }
}

/// The fullscreen dashboard renderer.
pub struct DashboardRenderer {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    state: PanelManager,
    spinners: std::collections::HashMap<yarli_core::domain::TaskId, Spinner>,
    config: DashboardConfig,
    copy_mode: CopyMode,
    overlays: OverlayStack,
}

impl DashboardRenderer {
    /// Create a new dashboard renderer, entering alternate screen + raw mode.
    pub fn new(config: DashboardConfig) -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        stdout.execute(EnterAlternateScreen)?;

        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;

        Ok(Self {
            terminal,
            state: PanelManager::new(),
            spinners: std::collections::HashMap::new(),
            config,
            copy_mode: CopyMode::new(),
            overlays: OverlayStack::new(),
        })
    }

    /// Restore terminal state on exit.
    pub fn restore(&mut self) -> io::Result<()> {
        disable_raw_mode()?;
        self.terminal.backend_mut().execute(LeaveAlternateScreen)?;
        self.terminal.show_cursor()?;
        Ok(())
    }

    /// Process a stream event and update internal state.
    pub fn handle_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::TaskTransition {
                task_id,
                task_name,
                from: _,
                to,
                elapsed,
                exit_code: _,
                detail: _,
                at: _,
            } => {
                self.state.update_task(task_id, &task_name, to, elapsed);
                if to == TaskState::TaskExecuting {
                    self.spinners.entry(task_id).or_default();
                }
                if to.is_terminal() {
                    self.spinners.remove(&task_id);
                }
            }
            StreamEvent::RunTransition {
                run_id,
                from: _,
                to,
                reason: _,
                at: _,
            } => {
                self.state.run_id = Some(run_id);
                self.state.run_state = Some(to);
            }
            StreamEvent::CommandOutput {
                task_id,
                task_name: _,
                line,
            } => {
                self.state.append_output(task_id, line);
            }
            StreamEvent::ExplainUpdate { summary } => {
                self.state.explain_summary = Some(summary);
            }
            StreamEvent::TaskWorker { task_id, worker_id } => {
                if let Some(view) = self.state.tasks.get_mut(&task_id) {
                    view.worker_id = Some(worker_id);
                }
            }
            StreamEvent::TransientStatus { .. } => {
                // Transient status is stream-mode only; dashboard ignores.
            }
            StreamEvent::Tick => {
                for spinner in self.spinners.values_mut() {
                    spinner.tick();
                }
            }
        }
    }

    /// Process a keyboard event and return whether to quit.
    pub fn handle_key_event(&mut self, key_event: KeyEvent) -> bool {
        let action = map_key_event(key_event, self.state.focused);
        match action {
            DashboardAction::Quit => return true,
            DashboardAction::DismissOverlay => {
                if self.overlays.has_focus() {
                    self.overlays.pop();
                }
            }
            DashboardAction::FocusNext => self.state.focus_next(),
            DashboardAction::FocusPrev => self.state.focus_prev(),
            DashboardAction::FocusPanel(p) => self.state.focus_panel(p),
            DashboardAction::Collapse => self.state.collapse_focused(),
            DashboardAction::Expand => self.state.expand_focused(),
            DashboardAction::RestoreAll => self.state.restore_all(),
            DashboardAction::ScrollUp => self.state.scroll_up(1),
            DashboardAction::ScrollDown => self.state.scroll_down(1),
            DashboardAction::ScrollHalfPageUp => self.state.scroll_up(10),
            DashboardAction::ScrollHalfPageDown => self.state.scroll_down(10),
            DashboardAction::ScrollToTop => self.state.scroll_to_top(),
            DashboardAction::ScrollToBottom => self.state.scroll_to_bottom(),
            DashboardAction::SelectNextTask => self.state.select_next_task(),
            DashboardAction::SelectPrevTask => self.state.select_prev_task(),
            DashboardAction::ToggleCopyMode => {
                self.state.copy_mode = !self.state.copy_mode;
                let _ = self.copy_mode.toggle(&mut io::stdout());
            }
            DashboardAction::ToggleHelp => {
                self.state.show_help = !self.state.show_help;
                let help_content = build_help_text();
                self.overlays.toggle(OverlayEntry::help(help_content));
            }
            DashboardAction::None => {}
        }
        false
    }

    /// Get the overlay stack for testing.
    #[cfg(test)]
    pub fn overlays(&self) -> &OverlayStack {
        &self.overlays
    }

    /// Poll for crossterm input events with timeout. Returns true if quit was requested.
    pub fn poll_input(&mut self) -> io::Result<bool> {
        if event::poll(Duration::from_millis(self.config.tick_rate_ms))? {
            if let Event::Key(key_event) = event::read()? {
                return Ok(self.handle_key_event(key_event));
            }
        }
        Ok(false)
    }

    /// Draw the full dashboard to the terminal.
    pub fn draw(&mut self) -> io::Result<()> {
        let state = &self.state;
        let spinners = &self.spinners;
        let borderless = self.copy_mode.strip_borders();

        // Pre-compute all data needed for rendering.
        let task_lines = build_task_list_lines(state, spinners);
        let output_lines = build_output_lines(state);
        let gate_lines = build_gate_lines(state);
        let explain_line = build_explain_line(state);
        let key_hints_line = build_key_hints_line(state);
        let title_line = if borderless {
            self.copy_mode
                .banner_line()
                .unwrap_or_else(|| build_title_line(state))
        } else {
            build_title_line(state)
        };

        let task_panel_state = state.panel_state(PanelId::TaskList);
        let output_panel_state = state.panel_state(PanelId::Output);
        let gates_panel_state = state.panel_state(PanelId::Gates);
        let focused = state.focused;
        let task_scroll = state
            .scroll_offsets
            .get(&PanelId::TaskList)
            .copied()
            .unwrap_or(0);
        let output_scroll = state
            .scroll_offsets
            .get(&PanelId::Output)
            .copied()
            .unwrap_or(0);
        let gate_scroll = state
            .scroll_offsets
            .get(&PanelId::Gates)
            .copied()
            .unwrap_or(0);
        let auto_scroll = state.output_auto_scroll;

        // Capture overlay stack reference for use inside the closure.
        let overlays = &self.overlays;

        self.terminal.draw(|frame| {
            let full_area = frame.area();

            // Main layout: [title] [body] [explain] [key_hints]
            let main_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Title bar
                    Constraint::Min(4),    // Body panels
                    Constraint::Length(1), // WHY NOT DONE bar
                    Constraint::Length(1), // Key hints bar
                ])
                .split(full_area);

            // Title bar.
            frame.render_widget(Paragraph::new(title_line), main_chunks[0]);

            // Body: horizontal split [TaskList | Output+Gates]
            let body_area = main_chunks[1];

            // Determine column widths based on terminal width and panel states.
            let (left_constraint, right_constraint) = match (task_panel_state, output_panel_state) {
                (PanelState::Hidden, _) => (Constraint::Length(0), Constraint::Min(10)),
                (_, PanelState::Hidden) => (Constraint::Min(10), Constraint::Length(0)),
                (PanelState::Collapsed, _) => (Constraint::Length(1), Constraint::Min(10)),
                _ => {
                    if body_area.width >= 120 {
                        (Constraint::Percentage(30), Constraint::Percentage(70))
                    } else {
                        (Constraint::Percentage(40), Constraint::Percentage(60))
                    }
                }
            };

            let body_cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([left_constraint, right_constraint])
                .split(body_area);

            // Left column: Task list.
            let task_panel = CollapsiblePanel::new("Tasks", task_panel_state)
                .content(task_lines)
                .focused(focused == PanelId::TaskList)
                .scroll_offset(task_scroll)
                .shortcut(Some('1'))
                .borderless(borderless);
            frame.render_widget(task_panel, body_cols[0]);

            // Right column: vertical split [Output | Gates].
            let right_constraints = match gates_panel_state {
                PanelState::Hidden => vec![Constraint::Min(3)],
                PanelState::Collapsed => vec![Constraint::Min(3), Constraint::Length(1)],
                PanelState::Expanded => {
                    if body_area.height >= 40 {
                        vec![Constraint::Percentage(70), Constraint::Percentage(30)]
                    } else {
                        vec![Constraint::Min(5), Constraint::Length(5)]
                    }
                }
            };

            let right_rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints(right_constraints.clone())
                .split(body_cols[1]);

            // Output panel.
            let output_title = if !auto_scroll {
                "Output [PAUSED]"
            } else {
                "Output"
            };
            let output_panel = CollapsiblePanel::new(output_title, output_panel_state)
                .content(output_lines)
                .focused(focused == PanelId::Output)
                .scroll_offset(output_scroll)
                .shortcut(Some('2'))
                .borderless(borderless);
            frame.render_widget(output_panel, right_rows[0]);

            // Gates panel (if visible).
            if right_constraints.len() > 1 {
                let gate_panel = CollapsiblePanel::new("Gates", gates_panel_state)
                    .content(gate_lines)
                    .focused(focused == PanelId::Gates)
                    .scroll_offset(gate_scroll)
                    .shortcut(Some('3'))
                    .borderless(borderless);
                frame.render_widget(gate_panel, right_rows[1]);
            }

            // WHY NOT DONE bar.
            frame.render_widget(Paragraph::new(explain_line), main_chunks[2]);

            // Key hints bar.
            frame.render_widget(Paragraph::new(key_hints_line), main_chunks[3]);

            // Overlays (rendered on top of everything).
            if !overlays.is_empty() {
                overlays.render(frame, full_area);
            }
        })?;

        Ok(())
    }

    /// Get the panel manager for testing.
    #[cfg(test)]
    pub fn state(&self) -> &PanelManager {
        &self.state
    }

    /// Get mutable panel manager for testing.
    #[cfg(test)]
    pub fn state_mut(&mut self) -> &mut PanelManager {
        &mut self.state
    }
}

impl Drop for DashboardRenderer {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

// ---------------------------------------------------------------------------
// Line builders — pure functions for testability
// ---------------------------------------------------------------------------

/// Build task list lines with state glyphs and timing.
pub fn build_task_list_lines<'a>(
    state: &PanelManager,
    spinners: &std::collections::HashMap<yarli_core::domain::TaskId, Spinner>,
) -> Vec<Line<'a>> {
    let mut lines = Vec::new();

    for (idx, task_id) in state.task_order.iter().enumerate() {
        let Some(task) = state.tasks.get(task_id) else {
            continue;
        };

        let is_selected = idx == state.selected_task_idx;

        let (glyph, tier) = match task.state {
            TaskState::TaskExecuting => {
                let sp = spinners.get(task_id).map(|s| s.frame()).unwrap_or('⠋');
                (sp, Tier::Active)
            }
            TaskState::TaskWaiting => ('⠿', Tier::Active),
            TaskState::TaskBlocked => (GLYPH_BLOCKED, Tier::Contextual),
            TaskState::TaskReady | TaskState::TaskOpen => (GLYPH_PENDING, Tier::Contextual),
            TaskState::TaskComplete => (GLYPH_COMPLETE, Tier::Contextual),
            TaskState::TaskFailed => (GLYPH_FAILED, Tier::Urgent),
            TaskState::TaskCancelled => (GLYPH_BLOCKED, Tier::Contextual),
            TaskState::TaskVerifying => (GLYPH_PENDING, Tier::Active),
        };

        let elapsed_str = task
            .elapsed
            .map(format_compact_duration)
            .unwrap_or_default();

        let cursor = if is_selected { ">" } else { " " };
        let cursor_style = if is_selected {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };

        lines.push(Line::from(vec![
            Span::styled(cursor.to_string(), cursor_style),
            Span::styled(format!("{glyph} "), tier.style()),
            Span::styled(format!("{:<14}", task.name), tier.style()),
            Span::styled(format!(" {:<6}", elapsed_str), Tier::Contextual.style()),
        ]));
    }

    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            " No tasks",
            Tier::Contextual.style(),
        )));
    }

    lines
}

/// Build output lines for the selected task.
pub fn build_output_lines<'a>(state: &PanelManager) -> Vec<Line<'a>> {
    if state.output_lines.is_empty() {
        let task_name = state
            .selected_task()
            .map(|t| t.name.as_str())
            .unwrap_or("none");
        return vec![Line::from(Span::styled(
            format!(" Waiting for output from {task_name}..."),
            Tier::Contextual.style(),
        ))];
    }

    state
        .output_lines
        .iter()
        .map(|line| Line::from(Span::raw(line.clone())))
        .collect()
}

/// Build gate status lines.
pub fn build_gate_lines<'a>(state: &PanelManager) -> Vec<Line<'a>> {
    if state.gate_results.is_empty() {
        return vec![Line::from(Span::styled(
            " No gate results yet",
            Tier::Contextual.style(),
        ))];
    }

    state
        .gate_results
        .iter()
        .map(|(name, passed, reason)| {
            let (glyph, tier) = if *passed {
                (GLYPH_COMPLETE, Tier::Contextual)
            } else {
                (GLYPH_FAILED, Tier::Urgent)
            };
            let mut spans = vec![
                Span::styled(format!(" {glyph} "), tier.style()),
                Span::styled(name.clone(), tier.style()),
            ];
            if let Some(r) = reason {
                spans.push(Span::styled(format!("  ({r})"), Tier::Contextual.style()));
            }
            Line::from(spans)
        })
        .collect()
}

/// Build the "WHY NOT DONE" explain line.
pub fn build_explain_line<'a>(state: &PanelManager) -> Line<'a> {
    if let Some(ref summary) = state.explain_summary {
        Line::from(vec![
            Span::styled(" WHY: ", Tier::Urgent.accent()),
            Span::styled(summary.clone(), Tier::Urgent.style()),
        ])
    } else {
        let run_state_str = state
            .run_state
            .map(|s| format!("{s:?}"))
            .unwrap_or_else(|| "pending".to_string());
        let summary = state.task_summary();
        Line::from(vec![Span::styled(
            format!(
                " {run_state_str} | {}/{} complete, {} active, {} failed",
                summary.complete, summary.total, summary.active, summary.failed
            ),
            Tier::Contextual.style(),
        )])
    }
}

/// Build the key hints bar.
pub fn build_key_hints_line<'a>(state: &PanelManager) -> Line<'a> {
    let copy_indicator = if state.copy_mode { "[COPY] " } else { "" };
    Line::from(vec![
        Span::styled(
            format!(" {copy_indicator}q:quit  Tab:focus  j/k:scroll  -/+:collapse/expand  =:restore  ?:help  c:copy"),
            Tier::Background.style(),
        ),
    ])
}

/// Build the title bar line.
pub fn build_title_line<'a>(state: &PanelManager) -> Line<'a> {
    let run_id_str = state
        .run_id
        .map(|id| format!("run/{}", &id.to_string()[..8]))
        .unwrap_or_else(|| "yarli".to_string());

    let run_state_str = state
        .run_state
        .map(|s| format!("{s:?}"))
        .unwrap_or_default();

    let tier = state
        .run_state
        .map(|s| match s {
            RunState::RunFailed | RunState::RunBlocked => Tier::Urgent,
            RunState::RunActive | RunState::RunVerifying => Tier::Active,
            RunState::RunCompleted => Tier::Contextual,
            _ => Tier::Contextual,
        })
        .unwrap_or(Tier::Contextual);

    Line::from(vec![
        Span::styled(
            " YARLI Dashboard ".to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(format!("| {run_id_str} "), Tier::Contextual.style()),
        Span::styled(run_state_str, tier.style()),
    ])
}

/// Build the help text content as a Vec<String> for the overlay stack.
pub fn build_help_text() -> Vec<String> {
    vec![
        "  YARLI Dashboard Help".into(),
        "".into(),
        "  Global:".into(),
        "    q / Ctrl-C    Quit".into(),
        "    Tab           Cycle focus to next panel".into(),
        "    Shift+Tab     Cycle focus to previous panel".into(),
        "    1-3           Jump to panel".into(),
        "    c             Toggle copy mode".into(),
        "    ?             Toggle this help".into(),
        "    Esc           Dismiss overlay".into(),
        "".into(),
        "  Panels:".into(),
        "    - / [         Collapse focused panel".into(),
        "    + / ]         Expand focused panel".into(),
        "    =             Restore all panels".into(),
        "".into(),
        "  Scrolling:".into(),
        "    j / k         Scroll line / Select task".into(),
        "    Ctrl+D/U      Half-page scroll".into(),
        "    PgUp/PgDn     Page scroll".into(),
        "    g / G         Top / Bottom".into(),
        "".into(),
        "  Press ? or Esc to close".into(),
    ]
}

/// Format duration compactly for the task list.
fn format_compact_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn format_compact_duration_seconds() {
        assert_eq!(format_compact_duration(Duration::from_secs(42)), "42s");
    }

    #[test]
    fn format_compact_duration_minutes() {
        assert_eq!(format_compact_duration(Duration::from_secs(125)), "2m5s");
    }

    #[test]
    fn format_compact_duration_hours() {
        assert_eq!(format_compact_duration(Duration::from_secs(3661)), "1h1m");
    }

    #[test]
    fn build_task_list_empty() {
        let state = PanelManager::new();
        let spinners = std::collections::HashMap::new();
        let lines = build_task_list_lines(&state, &spinners);
        assert_eq!(lines.len(), 1);
    }

    #[test]
    fn build_task_list_with_tasks() {
        let mut state = PanelManager::new();
        state.update_task(
            Uuid::new_v4(),
            "lint",
            TaskState::TaskComplete,
            Some(Duration::from_secs(3)),
        );
        state.update_task(
            Uuid::new_v4(),
            "build",
            TaskState::TaskExecuting,
            Some(Duration::from_secs(34)),
        );
        state.update_task(
            Uuid::new_v4(),
            "test",
            TaskState::TaskFailed,
            Some(Duration::from_secs(14)),
        );

        let spinners = std::collections::HashMap::new();
        let lines = build_task_list_lines(&state, &spinners);
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn build_output_lines_empty() {
        let state = PanelManager::new();
        let lines = build_output_lines(&state);
        assert_eq!(lines.len(), 1);
    }

    #[test]
    fn build_output_lines_with_content() {
        let mut state = PanelManager::new();
        let id = Uuid::new_v4();
        state.update_task(id, "t1", TaskState::TaskExecuting, None);
        state.append_output(id, "Compiling crate1".into());
        state.append_output(id, "Compiling crate2".into());

        let lines = build_output_lines(&state);
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn build_gate_lines_empty() {
        let state = PanelManager::new();
        let lines = build_gate_lines(&state);
        assert_eq!(lines.len(), 1); // "No gate results yet"
    }

    #[test]
    fn build_gate_lines_with_results() {
        let mut state = PanelManager::new();
        state.gate_results.push(("tests_passed".into(), true, None));
        state
            .gate_results
            .push(("policy_clean".into(), false, Some("denied".into())));

        let lines = build_gate_lines(&state);
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn build_explain_line_with_summary() {
        let mut state = PanelManager::new();
        state.explain_summary = Some("2 tasks blocked".into());
        let line = build_explain_line(&state);
        assert!(!line.spans.is_empty());
    }

    #[test]
    fn build_explain_line_without_summary() {
        let state = PanelManager::new();
        let line = build_explain_line(&state);
        assert!(!line.spans.is_empty());
    }

    #[test]
    fn build_key_hints_contains_shortcuts() {
        let state = PanelManager::new();
        let line = build_key_hints_line(&state);
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(text.contains("quit"));
        assert!(text.contains("Tab"));
    }

    #[test]
    fn build_title_line_with_run() {
        let mut state = PanelManager::new();
        state.run_id = Some(Uuid::new_v4());
        state.run_state = Some(RunState::RunActive);
        let line = build_title_line(&state);
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(text.contains("YARLI"));
        assert!(text.contains("run/"));
    }

    #[test]
    fn build_title_line_no_run() {
        let state = PanelManager::new();
        let line = build_title_line(&state);
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(text.contains("yarli"));
    }

    #[test]
    fn build_key_hints_copy_mode() {
        let mut state = PanelManager::new();
        state.copy_mode = true;
        let line = build_key_hints_line(&state);
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(text.contains("[COPY]"));
    }

    #[test]
    fn build_help_text_contains_shortcuts() {
        let text = build_help_text();
        let joined = text.join("\n");
        assert!(joined.contains("Quit"));
        assert!(joined.contains("copy mode"));
        assert!(joined.contains("Esc"));
    }

    #[test]
    fn build_help_text_not_empty() {
        let text = build_help_text();
        assert!(text.len() > 10);
    }
}
