//! Keyboard input handling for the dashboard TUI (Section 16.3).
//!
//! Maps crossterm key events to dashboard actions.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::state::PanelId;

/// Actions the dashboard can perform in response to keyboard input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DashboardAction {
    /// Quit the application.
    Quit,
    /// Cycle focus to next panel (Tab).
    FocusNext,
    /// Cycle focus to previous panel (Shift+Tab).
    FocusPrev,
    /// Jump to specific panel (1-4).
    FocusPanel(PanelId),
    /// Collapse the focused panel (-/[).
    Collapse,
    /// Expand the focused panel (+/]).
    Expand,
    /// Restore all panels to expanded (=).
    RestoreAll,
    /// Scroll up by one line (k).
    ScrollUp,
    /// Scroll down by one line (j).
    ScrollDown,
    /// Scroll up half-page (Ctrl+U).
    ScrollHalfPageUp,
    /// Scroll down half-page (Ctrl+D).
    ScrollHalfPageDown,
    /// Scroll to top (g).
    ScrollToTop,
    /// Scroll to bottom / re-engage auto-scroll (G / End).
    ScrollToBottom,
    /// Toggle copy mode (c) — strips borders, disables mouse capture.
    ToggleCopyMode,
    /// Toggle help overlay (?).
    ToggleHelp,
    /// Select next task in task list (Down arrow in task list).
    SelectNextTask,
    /// Select previous task in task list (Up arrow in task list).
    SelectPrevTask,
    /// Dismiss topmost overlay (Esc).
    DismissOverlay,
    /// No action for this key.
    None,
}

/// Map a crossterm key event to a dashboard action.
pub fn map_key_event(event: KeyEvent, focused: PanelId) -> DashboardAction {
    // Ctrl+C always quits — never blocked.
    if event.modifiers.contains(KeyModifiers::CONTROL) && event.code == KeyCode::Char('c') {
        return DashboardAction::Quit;
    }

    // Esc dismisses topmost overlay.
    if event.code == KeyCode::Esc {
        return DashboardAction::DismissOverlay;
    }

    // Global shortcuts (work regardless of focus).
    match event.code {
        KeyCode::Char('q') => return DashboardAction::Quit,
        KeyCode::Char('?') => return DashboardAction::ToggleHelp,
        KeyCode::Char('c') => return DashboardAction::ToggleCopyMode,
        KeyCode::Char('=') => return DashboardAction::RestoreAll,
        KeyCode::Tab => return DashboardAction::FocusNext,
        KeyCode::BackTab => return DashboardAction::FocusPrev,
        KeyCode::Char('1') => return DashboardAction::FocusPanel(PanelId::TaskList),
        KeyCode::Char('2') => return DashboardAction::FocusPanel(PanelId::Output),
        KeyCode::Char('3') => return DashboardAction::FocusPanel(PanelId::Gates),
        KeyCode::Char('4') => return DashboardAction::FocusPanel(PanelId::Audit),
        _ => {}
    }

    // Scrolling (j/k/g/G, Ctrl+U/D, PgUp/PgDn).
    match event.code {
        KeyCode::Char('k') | KeyCode::Up => {
            if focused == PanelId::TaskList {
                return DashboardAction::SelectPrevTask;
            }
            return DashboardAction::ScrollUp;
        }
        KeyCode::Char('j') | KeyCode::Down => {
            if focused == PanelId::TaskList {
                return DashboardAction::SelectNextTask;
            }
            return DashboardAction::ScrollDown;
        }
        KeyCode::Char('u') if event.modifiers.contains(KeyModifiers::CONTROL) => {
            return DashboardAction::ScrollHalfPageUp;
        }
        KeyCode::Char('d') if event.modifiers.contains(KeyModifiers::CONTROL) => {
            return DashboardAction::ScrollHalfPageDown;
        }
        KeyCode::PageUp => return DashboardAction::ScrollHalfPageUp,
        KeyCode::PageDown => return DashboardAction::ScrollHalfPageDown,
        KeyCode::Char('g') => return DashboardAction::ScrollToTop,
        KeyCode::Char('G') | KeyCode::End => return DashboardAction::ScrollToBottom,
        _ => {}
    }

    // Panel collapse/expand (-/[/+/]).
    match event.code {
        KeyCode::Char('-') | KeyCode::Char('[') => return DashboardAction::Collapse,
        KeyCode::Char('+') | KeyCode::Char(']') => return DashboardAction::Expand,
        _ => {}
    }

    DashboardAction::None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::CONTROL)
    }

    fn shift_tab() -> KeyEvent {
        KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT)
    }

    #[test]
    fn ctrl_c_always_quits() {
        assert_eq!(
            map_key_event(ctrl(KeyCode::Char('c')), PanelId::TaskList),
            DashboardAction::Quit
        );
    }

    #[test]
    fn q_quits() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('q')), PanelId::TaskList),
            DashboardAction::Quit
        );
    }

    #[test]
    fn tab_cycles_focus() {
        assert_eq!(
            map_key_event(key(KeyCode::Tab), PanelId::TaskList),
            DashboardAction::FocusNext
        );
    }

    #[test]
    fn shift_tab_cycles_backward() {
        assert_eq!(
            map_key_event(shift_tab(), PanelId::Output),
            DashboardAction::FocusPrev
        );
    }

    #[test]
    fn number_keys_jump_to_panel() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('1')), PanelId::Output),
            DashboardAction::FocusPanel(PanelId::TaskList)
        );
        assert_eq!(
            map_key_event(key(KeyCode::Char('2')), PanelId::TaskList),
            DashboardAction::FocusPanel(PanelId::Output)
        );
        assert_eq!(
            map_key_event(key(KeyCode::Char('3')), PanelId::TaskList),
            DashboardAction::FocusPanel(PanelId::Gates)
        );
        assert_eq!(
            map_key_event(key(KeyCode::Char('4')), PanelId::TaskList),
            DashboardAction::FocusPanel(PanelId::Audit)
        );
    }

    #[test]
    fn jk_scroll_in_output_panel() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('j')), PanelId::Output),
            DashboardAction::ScrollDown
        );
        assert_eq!(
            map_key_event(key(KeyCode::Char('k')), PanelId::Output),
            DashboardAction::ScrollUp
        );
    }

    #[test]
    fn jk_select_in_task_list() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('j')), PanelId::TaskList),
            DashboardAction::SelectNextTask
        );
        assert_eq!(
            map_key_event(key(KeyCode::Char('k')), PanelId::TaskList),
            DashboardAction::SelectPrevTask
        );
    }

    #[test]
    fn ctrl_d_u_half_page() {
        assert_eq!(
            map_key_event(ctrl(KeyCode::Char('d')), PanelId::Output),
            DashboardAction::ScrollHalfPageDown
        );
        assert_eq!(
            map_key_event(ctrl(KeyCode::Char('u')), PanelId::Output),
            DashboardAction::ScrollHalfPageUp
        );
    }

    #[test]
    fn g_and_shift_g_scroll_extremes() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('g')), PanelId::Output),
            DashboardAction::ScrollToTop
        );
        assert_eq!(
            map_key_event(key(KeyCode::Char('G')), PanelId::Output),
            DashboardAction::ScrollToBottom
        );
    }

    #[test]
    fn collapse_expand_keys() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('-')), PanelId::Output),
            DashboardAction::Collapse
        );
        assert_eq!(
            map_key_event(key(KeyCode::Char('+')), PanelId::Output),
            DashboardAction::Expand
        );
    }

    #[test]
    fn toggle_help() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('?')), PanelId::TaskList),
            DashboardAction::ToggleHelp
        );
    }

    #[test]
    fn toggle_copy_mode() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('c')), PanelId::TaskList),
            DashboardAction::ToggleCopyMode
        );
    }

    #[test]
    fn esc_dismisses_overlay() {
        assert_eq!(
            map_key_event(key(KeyCode::Esc), PanelId::TaskList),
            DashboardAction::DismissOverlay
        );
    }

    #[test]
    fn unknown_key_returns_none() {
        assert_eq!(
            map_key_event(key(KeyCode::Char('z')), PanelId::TaskList),
            DashboardAction::None
        );
    }
}
