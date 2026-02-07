//! Panel state management for the dashboard TUI.
//!
//! `PanelManager` maintains retained state for focus, collapse/expand,
//! scroll positions, and selected task across all panels (~300 LOC, Section 32).

use std::collections::HashMap;
use std::time::Duration;

use uuid::Uuid;

use yarli_core::domain::TaskId;
use yarli_core::fsm::task::TaskState;

use crate::stream::TaskView;

/// Identifies a panel in the dashboard layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PanelId {
    /// Left panel: task list with state glyphs and timing.
    TaskList,
    /// Right panel: selected task's live output.
    Output,
    /// Gate/merge status panel.
    Gates,
    /// Key hints bar (not focusable).
    KeyHints,
}

impl PanelId {
    /// Panels in focus-cycling order (Tab/Shift+Tab).
    pub const FOCUSABLE: &[PanelId] = &[PanelId::TaskList, PanelId::Output, PanelId::Gates];

    /// Panel title for display.
    pub fn title(self) -> &'static str {
        match self {
            PanelId::TaskList => "Tasks",
            PanelId::Output => "Output",
            PanelId::Gates => "Gates",
            PanelId::KeyHints => "Keys",
        }
    }

    /// Keyboard shortcut number for direct panel jump.
    pub fn shortcut(self) -> Option<char> {
        match self {
            PanelId::TaskList => Some('1'),
            PanelId::Output => Some('2'),
            PanelId::Gates => Some('3'),
            PanelId::KeyHints => None,
        }
    }
}

/// Visual state of a panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanelState {
    /// Fully visible (default).
    Expanded,
    /// Header-only (collapsed).
    Collapsed,
    /// Completely hidden.
    Hidden,
}

impl PanelState {
    /// Cycle: Expanded -> Collapsed -> Hidden -> Expanded.
    pub fn collapse(self) -> PanelState {
        match self {
            PanelState::Expanded => PanelState::Collapsed,
            PanelState::Collapsed => PanelState::Hidden,
            PanelState::Hidden => PanelState::Expanded,
        }
    }

    /// Reverse cycle.
    pub fn expand(self) -> PanelState {
        match self {
            PanelState::Hidden => PanelState::Collapsed,
            PanelState::Collapsed => PanelState::Expanded,
            PanelState::Expanded => PanelState::Expanded,
        }
    }
}

/// Retained state for the dashboard panels.
pub struct PanelManager {
    /// Which panel has focus.
    pub focused: PanelId,
    /// Per-panel visual state.
    pub panel_states: HashMap<PanelId, PanelState>,
    /// Per-panel scroll offset (lines from top).
    pub scroll_offsets: HashMap<PanelId, u16>,
    /// Task list: ordered task IDs.
    pub task_order: Vec<TaskId>,
    /// Task list: task views keyed by ID.
    pub tasks: HashMap<TaskId, TaskView>,
    /// Selected task index in task_order (for cursor).
    pub selected_task_idx: usize,
    /// Output lines for the selected task.
    pub output_lines: Vec<String>,
    /// Whether output auto-scrolls (true until user scrolls up).
    pub output_auto_scroll: bool,
    /// Current "Why Not Done?" summary.
    pub explain_summary: Option<String>,
    /// Gate results (gate_name -> passed).
    pub gate_results: Vec<(String, bool, Option<String>)>,
    /// Whether copy mode is active (strips borders, disables mouse capture).
    pub copy_mode: bool,
    /// Current run ID being displayed.
    pub run_id: Option<Uuid>,
    /// Current run state.
    pub run_state: Option<yarli_core::fsm::run::RunState>,
    /// Help overlay visible.
    pub show_help: bool,
}

impl PanelManager {
    pub fn new() -> Self {
        let mut panel_states = HashMap::new();
        panel_states.insert(PanelId::TaskList, PanelState::Expanded);
        panel_states.insert(PanelId::Output, PanelState::Expanded);
        panel_states.insert(PanelId::Gates, PanelState::Expanded);

        Self {
            focused: PanelId::TaskList,
            panel_states,
            scroll_offsets: HashMap::new(),
            task_order: Vec::new(),
            tasks: HashMap::new(),
            selected_task_idx: 0,
            output_lines: Vec::new(),
            output_auto_scroll: true,
            explain_summary: None,
            gate_results: Vec::new(),
            copy_mode: false,
            run_id: None,
            run_state: None,
            show_help: false,
        }
    }

    /// Cycle focus to the next panel (Tab).
    pub fn focus_next(&mut self) {
        let focusable = PanelId::FOCUSABLE;
        if let Some(idx) = focusable.iter().position(|p| *p == self.focused) {
            // Skip hidden panels.
            for offset in 1..=focusable.len() {
                let next = focusable[(idx + offset) % focusable.len()];
                if self.panel_state(next) != PanelState::Hidden {
                    self.focused = next;
                    return;
                }
            }
        }
    }

    /// Cycle focus to the previous panel (Shift+Tab).
    pub fn focus_prev(&mut self) {
        let focusable = PanelId::FOCUSABLE;
        if let Some(idx) = focusable.iter().position(|p| *p == self.focused) {
            for offset in 1..=focusable.len() {
                let prev_idx = (idx + focusable.len() - offset) % focusable.len();
                let prev = focusable[prev_idx];
                if self.panel_state(prev) != PanelState::Hidden {
                    self.focused = prev;
                    return;
                }
            }
        }
    }

    /// Jump to panel by shortcut number.
    pub fn focus_panel(&mut self, panel: PanelId) {
        if self.panel_state(panel) != PanelState::Hidden {
            self.focused = panel;
        }
    }

    /// Get panel state, defaulting to Expanded.
    pub fn panel_state(&self, panel: PanelId) -> PanelState {
        self.panel_states
            .get(&panel)
            .copied()
            .unwrap_or(PanelState::Expanded)
    }

    /// Collapse the focused panel.
    pub fn collapse_focused(&mut self) {
        let state = self.panel_state(self.focused);
        self.panel_states.insert(self.focused, state.collapse());
        // If panel becomes hidden, move focus.
        if self.panel_state(self.focused) == PanelState::Hidden {
            self.focus_next();
        }
    }

    /// Expand the focused panel.
    pub fn expand_focused(&mut self) {
        let state = self.panel_state(self.focused);
        self.panel_states.insert(self.focused, state.expand());
    }

    /// Restore all panels to expanded.
    pub fn restore_all(&mut self) {
        for state in self.panel_states.values_mut() {
            *state = PanelState::Expanded;
        }
    }

    /// Move task list cursor down.
    pub fn select_next_task(&mut self) {
        if !self.task_order.is_empty() {
            self.selected_task_idx = (self.selected_task_idx + 1).min(self.task_order.len() - 1);
            self.update_output_for_selected_task();
        }
    }

    /// Move task list cursor up.
    pub fn select_prev_task(&mut self) {
        if self.selected_task_idx > 0 {
            self.selected_task_idx -= 1;
            self.update_output_for_selected_task();
        }
    }

    /// Get the currently selected task.
    pub fn selected_task(&self) -> Option<&TaskView> {
        self.task_order
            .get(self.selected_task_idx)
            .and_then(|id| self.tasks.get(id))
    }

    /// Scroll the focused panel up by N lines.
    pub fn scroll_up(&mut self, lines: u16) {
        let offset = self.scroll_offsets.entry(self.focused).or_insert(0);
        *offset = offset.saturating_sub(lines);
        if self.focused == PanelId::Output {
            self.output_auto_scroll = false;
        }
    }

    /// Scroll the focused panel down by N lines.
    pub fn scroll_down(&mut self, lines: u16) {
        let offset = self.scroll_offsets.entry(self.focused).or_insert(0);
        *offset = offset.saturating_add(lines);
        // Re-engage auto-scroll if we scroll to the bottom.
        if self.focused == PanelId::Output {
            let max_scroll = self.output_lines.len().saturating_sub(1) as u16;
            if *offset >= max_scroll {
                self.output_auto_scroll = true;
            }
        }
    }

    /// Scroll to top of focused panel.
    pub fn scroll_to_top(&mut self) {
        self.scroll_offsets.insert(self.focused, 0);
        if self.focused == PanelId::Output {
            self.output_auto_scroll = false;
        }
    }

    /// Scroll to bottom of focused panel (re-engages auto-scroll for output).
    pub fn scroll_to_bottom(&mut self) {
        if self.focused == PanelId::Output {
            let max = self.output_lines.len().saturating_sub(1) as u16;
            self.scroll_offsets.insert(PanelId::Output, max);
            self.output_auto_scroll = true;
        }
    }

    /// Update a task's state from a stream event.
    pub fn update_task(
        &mut self,
        task_id: TaskId,
        name: &str,
        state: TaskState,
        elapsed: Option<Duration>,
    ) {
        if !self.task_order.contains(&task_id) {
            self.task_order.push(task_id);
        }

        let view = self.tasks.entry(task_id).or_insert_with(|| TaskView {
            task_id,
            name: name.to_string(),
            state,
            elapsed,
            last_output_line: None,
            blocked_by: None,
            worker_id: None,
        });
        view.state = state;
        view.elapsed = elapsed;
    }

    /// Append an output line for a task.
    pub fn append_output(&mut self, task_id: TaskId, line: String) {
        // Update last_output_line on task.
        if let Some(view) = self.tasks.get_mut(&task_id) {
            view.last_output_line = Some(line.clone());
        }
        // Append to output buffer if this is the selected task.
        if self.task_order.get(self.selected_task_idx) == Some(&task_id) {
            self.output_lines.push(line);
            if self.output_auto_scroll {
                let max = self.output_lines.len().saturating_sub(1) as u16;
                self.scroll_offsets.insert(PanelId::Output, max);
            }
        }
    }

    /// Update output lines when selected task changes.
    fn update_output_for_selected_task(&mut self) {
        // In the current implementation, we don't persist per-task output history.
        // When switching tasks, we clear the output panel.
        // A future enhancement could maintain per-task output buffers.
        self.output_lines.clear();
        self.scroll_offsets.insert(PanelId::Output, 0);
        self.output_auto_scroll = true;
    }

    /// Get total number of tasks in each state for the status bar.
    pub fn task_summary(&self) -> TaskSummary {
        let mut summary = TaskSummary::default();
        for view in self.tasks.values() {
            match view.state {
                TaskState::TaskComplete => summary.complete += 1,
                TaskState::TaskFailed => summary.failed += 1,
                TaskState::TaskExecuting | TaskState::TaskWaiting => summary.active += 1,
                TaskState::TaskBlocked => summary.blocked += 1,
                _ => summary.pending += 1,
            }
        }
        summary.total = self.tasks.len();
        summary
    }
}

impl Default for PanelManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of task states for the status bar.
#[derive(Debug, Default)]
pub struct TaskSummary {
    pub total: usize,
    pub complete: usize,
    pub failed: usize,
    pub active: usize,
    pub blocked: usize,
    pub pending: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panel_state_collapse_cycle() {
        let s = PanelState::Expanded;
        assert_eq!(s.collapse(), PanelState::Collapsed);
        assert_eq!(s.collapse().collapse(), PanelState::Hidden);
        assert_eq!(s.collapse().collapse().collapse(), PanelState::Expanded);
    }

    #[test]
    fn panel_state_expand_cycle() {
        let s = PanelState::Hidden;
        assert_eq!(s.expand(), PanelState::Collapsed);
        assert_eq!(s.expand().expand(), PanelState::Expanded);
        assert_eq!(s.expand().expand().expand(), PanelState::Expanded); // stays
    }

    #[test]
    fn focus_cycling() {
        let mut mgr = PanelManager::new();
        assert_eq!(mgr.focused, PanelId::TaskList);
        mgr.focus_next();
        assert_eq!(mgr.focused, PanelId::Output);
        mgr.focus_next();
        assert_eq!(mgr.focused, PanelId::Gates);
        mgr.focus_next();
        assert_eq!(mgr.focused, PanelId::TaskList); // wraps
    }

    #[test]
    fn focus_prev_cycling() {
        let mut mgr = PanelManager::new();
        mgr.focus_prev();
        assert_eq!(mgr.focused, PanelId::Gates); // wraps backward
        mgr.focus_prev();
        assert_eq!(mgr.focused, PanelId::Output);
    }

    #[test]
    fn focus_skips_hidden_panels() {
        let mut mgr = PanelManager::new();
        mgr.panel_states.insert(PanelId::Output, PanelState::Hidden);
        mgr.focus_next();
        assert_eq!(mgr.focused, PanelId::Gates); // skipped Output
    }

    #[test]
    fn focus_panel_by_shortcut() {
        let mut mgr = PanelManager::new();
        mgr.focus_panel(PanelId::Gates);
        assert_eq!(mgr.focused, PanelId::Gates);
    }

    #[test]
    fn focus_panel_ignores_hidden() {
        let mut mgr = PanelManager::new();
        mgr.panel_states.insert(PanelId::Gates, PanelState::Hidden);
        mgr.focus_panel(PanelId::Gates);
        assert_eq!(mgr.focused, PanelId::TaskList); // unchanged
    }

    #[test]
    fn collapse_focused_moves_focus_when_hidden() {
        let mut mgr = PanelManager::new();
        mgr.collapse_focused(); // Expanded -> Collapsed
        assert_eq!(mgr.panel_state(PanelId::TaskList), PanelState::Collapsed);
        assert_eq!(mgr.focused, PanelId::TaskList); // still focused

        mgr.collapse_focused(); // Collapsed -> Hidden
        assert_eq!(mgr.panel_state(PanelId::TaskList), PanelState::Hidden);
        assert_eq!(mgr.focused, PanelId::Output); // focus moved
    }

    #[test]
    fn restore_all_resets_panels() {
        let mut mgr = PanelManager::new();
        mgr.panel_states
            .insert(PanelId::TaskList, PanelState::Hidden);
        mgr.panel_states
            .insert(PanelId::Output, PanelState::Collapsed);
        mgr.restore_all();
        assert_eq!(mgr.panel_state(PanelId::TaskList), PanelState::Expanded);
        assert_eq!(mgr.panel_state(PanelId::Output), PanelState::Expanded);
    }

    #[test]
    fn task_selection() {
        let mut mgr = PanelManager::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        mgr.update_task(id1, "task-1", TaskState::TaskExecuting, None);
        mgr.update_task(id2, "task-2", TaskState::TaskReady, None);

        assert_eq!(mgr.selected_task_idx, 0);
        assert_eq!(mgr.selected_task().unwrap().name, "task-1");

        mgr.select_next_task();
        assert_eq!(mgr.selected_task_idx, 1);
        assert_eq!(mgr.selected_task().unwrap().name, "task-2");

        mgr.select_next_task(); // at end, stays
        assert_eq!(mgr.selected_task_idx, 1);

        mgr.select_prev_task();
        assert_eq!(mgr.selected_task_idx, 0);
    }

    #[test]
    fn scroll_up_down() {
        let mut mgr = PanelManager::new();
        mgr.scroll_down(5);
        assert_eq!(mgr.scroll_offsets[&PanelId::TaskList], 5);
        mgr.scroll_up(3);
        assert_eq!(mgr.scroll_offsets[&PanelId::TaskList], 2);
        mgr.scroll_up(10); // doesn't go below 0
        assert_eq!(mgr.scroll_offsets[&PanelId::TaskList], 0);
    }

    #[test]
    fn output_auto_scroll_disengages_on_scroll_up() {
        let mut mgr = PanelManager::new();
        mgr.focused = PanelId::Output;
        assert!(mgr.output_auto_scroll);
        mgr.scroll_up(1);
        assert!(!mgr.output_auto_scroll);
    }

    #[test]
    fn task_summary_counts() {
        let mut mgr = PanelManager::new();
        mgr.update_task(Uuid::new_v4(), "t1", TaskState::TaskComplete, None);
        mgr.update_task(Uuid::new_v4(), "t2", TaskState::TaskFailed, None);
        mgr.update_task(Uuid::new_v4(), "t3", TaskState::TaskExecuting, None);
        mgr.update_task(Uuid::new_v4(), "t4", TaskState::TaskBlocked, None);
        mgr.update_task(Uuid::new_v4(), "t5", TaskState::TaskOpen, None);

        let s = mgr.task_summary();
        assert_eq!(s.total, 5);
        assert_eq!(s.complete, 1);
        assert_eq!(s.failed, 1);
        assert_eq!(s.active, 1);
        assert_eq!(s.blocked, 1);
        assert_eq!(s.pending, 1);
    }

    #[test]
    fn append_output_for_selected_task() {
        let mut mgr = PanelManager::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        mgr.update_task(id1, "t1", TaskState::TaskExecuting, None);
        mgr.update_task(id2, "t2", TaskState::TaskExecuting, None);

        // Selected task is t1 (idx 0).
        mgr.append_output(id1, "line1".into());
        mgr.append_output(id1, "line2".into());
        assert_eq!(mgr.output_lines.len(), 2);

        // Output for non-selected task is not appended to panel.
        mgr.append_output(id2, "other".into());
        assert_eq!(mgr.output_lines.len(), 2);
    }

    #[test]
    fn panel_id_titles_and_shortcuts() {
        assert_eq!(PanelId::TaskList.title(), "Tasks");
        assert_eq!(PanelId::Output.title(), "Output");
        assert_eq!(PanelId::TaskList.shortcut(), Some('1'));
        assert_eq!(PanelId::KeyHints.shortcut(), None);
    }
}
