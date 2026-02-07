//! Overlay stack — focus management for floating panels (Section 30, 32).
//!
//! Manages a stack of floating overlays rendered above dashboard content.
//! Overlays are dismissible with `Esc` and never block `Ctrl+C`.

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

/// Identifies an overlay type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OverlayKind {
    /// Help dialog (keyboard shortcuts reference).
    Help,
    /// Confirmation dialog (e.g., retry task, unblock).
    Confirm,
    /// Gate detail view (expanded gate evaluation info).
    GateDetail,
}

impl OverlayKind {
    /// Title for the overlay border.
    pub fn title(self) -> &'static str {
        match self {
            OverlayKind::Help => " Help ",
            OverlayKind::Confirm => " Confirm ",
            OverlayKind::GateDetail => " Gate Details ",
        }
    }
}

/// Content for a confirmation dialog.
#[derive(Debug, Clone)]
pub struct ConfirmContent {
    /// The question/prompt to display.
    pub message: String,
    /// Label for the confirm action.
    pub confirm_label: String,
    /// Label for the cancel action.
    pub cancel_label: String,
}

impl Default for ConfirmContent {
    fn default() -> Self {
        Self {
            message: String::new(),
            confirm_label: "Yes (y)".into(),
            cancel_label: "No (n/Esc)".into(),
        }
    }
}

/// Content for a gate detail view.
#[derive(Debug, Clone)]
pub struct GateDetailContent {
    /// Gate name.
    pub name: String,
    /// Whether the gate passed.
    pub passed: bool,
    /// Reason or detail text.
    pub detail: String,
}

/// An overlay entry on the stack.
#[derive(Debug, Clone)]
pub struct OverlayEntry {
    /// What kind of overlay this is.
    pub kind: OverlayKind,
    /// Content lines to display.
    pub content: Vec<String>,
    /// Width as a fraction of terminal width (0.0-1.0).
    pub width_fraction: f32,
    /// Height as a fraction of terminal height (0.0-1.0).
    pub height_fraction: f32,
}

impl OverlayEntry {
    /// Create a help overlay.
    pub fn help(content: Vec<String>) -> Self {
        Self {
            kind: OverlayKind::Help,
            content,
            width_fraction: 0.6,
            height_fraction: 0.7,
        }
    }

    /// Create a confirmation dialog overlay.
    pub fn confirm(confirm: &ConfirmContent) -> Self {
        let content = vec![
            confirm.message.clone(),
            String::new(),
            format!("  {} / {}", confirm.confirm_label, confirm.cancel_label),
        ];
        Self {
            kind: OverlayKind::Confirm,
            content,
            width_fraction: 0.4,
            height_fraction: 0.25,
        }
    }

    /// Create a gate detail overlay.
    pub fn gate_detail(detail: &GateDetailContent) -> Self {
        let status = if detail.passed { "PASSED" } else { "FAILED" };
        let content = vec![
            format!("Gate: {}", detail.name),
            format!("Status: {status}"),
            String::new(),
            detail.detail.clone(),
        ];
        Self {
            kind: OverlayKind::GateDetail,
            content,
            width_fraction: 0.5,
            height_fraction: 0.4,
        }
    }

    /// Compute the centered rectangle for this overlay within the given area.
    pub fn compute_rect(&self, area: Rect) -> Rect {
        let width = ((area.width as f32 * self.width_fraction) as u16)
            .max(20)
            .min(area.width.saturating_sub(2));
        let height = ((area.height as f32 * self.height_fraction) as u16)
            .max(5)
            .min(area.height.saturating_sub(2));
        let x = (area.width.saturating_sub(width)) / 2;
        let y = (area.height.saturating_sub(height)) / 2;
        Rect::new(x, y, width, height)
    }
}

/// A stack of overlays with focus management.
///
/// The topmost overlay receives keyboard input. Overlays are dismissed
/// with `Esc` (pops the top) and never block `Ctrl+C`.
pub struct OverlayStack {
    /// Stack of active overlays (last = topmost = focused).
    entries: Vec<OverlayEntry>,
}

impl OverlayStack {
    /// Create an empty overlay stack.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Whether there are any active overlays.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of active overlays.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the overlay stack has focus (any overlay is showing).
    pub fn has_focus(&self) -> bool {
        !self.entries.is_empty()
    }

    /// Get the topmost overlay kind (if any).
    pub fn top_kind(&self) -> Option<OverlayKind> {
        self.entries.last().map(|e| e.kind)
    }

    /// Push an overlay onto the stack. If an overlay of the same kind
    /// already exists, it is replaced (moved to top).
    pub fn push(&mut self, entry: OverlayEntry) {
        // Remove existing overlay of same kind to avoid duplicates.
        self.entries.retain(|e| e.kind != entry.kind);
        self.entries.push(entry);
    }

    /// Toggle an overlay: if the kind is already showing, dismiss it;
    /// otherwise push it.
    pub fn toggle(&mut self, entry: OverlayEntry) {
        let kind = entry.kind;
        if self.entries.iter().any(|e| e.kind == kind) {
            self.dismiss(kind);
        } else {
            self.push(entry);
        }
    }

    /// Dismiss (pop) the topmost overlay. Returns the dismissed entry.
    pub fn pop(&mut self) -> Option<OverlayEntry> {
        self.entries.pop()
    }

    /// Dismiss a specific overlay kind.
    pub fn dismiss(&mut self, kind: OverlayKind) {
        self.entries.retain(|e| e.kind != kind);
    }

    /// Dismiss all overlays.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Render all overlays onto the frame, from bottom to top.
    pub fn render(&self, frame: &mut ratatui::Frame, area: Rect) {
        for entry in &self.entries {
            let overlay_rect = entry.compute_rect(area);

            // Clear the area behind the overlay.
            frame.render_widget(Clear, overlay_rect);

            // Build the bordered block.
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(Span::styled(
                    entry.kind.title(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ));

            let inner = block.inner(overlay_rect);
            frame.render_widget(block, overlay_rect);

            // Render content lines.
            let lines: Vec<Line<'_>> = entry
                .content
                .iter()
                .map(|s| Line::from(s.as_str()))
                .collect();
            frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
        }
    }

    /// Get an iterator over overlay entries (bottom to top).
    pub fn iter(&self) -> impl Iterator<Item = &OverlayEntry> {
        self.entries.iter()
    }
}

impl Default for OverlayStack {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn help_lines() -> Vec<String> {
        vec![
            "q: quit".into(),
            "Tab: focus next".into(),
            "?: toggle help".into(),
        ]
    }

    #[test]
    fn new_stack_is_empty() {
        let stack = OverlayStack::new();
        assert!(stack.is_empty());
        assert_eq!(stack.len(), 0);
        assert!(!stack.has_focus());
        assert_eq!(stack.top_kind(), None);
    }

    #[test]
    fn push_and_top() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(help_lines()));
        assert!(!stack.is_empty());
        assert_eq!(stack.len(), 1);
        assert!(stack.has_focus());
        assert_eq!(stack.top_kind(), Some(OverlayKind::Help));
    }

    #[test]
    fn push_replaces_same_kind() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(vec!["old".into()]));
        stack.push(OverlayEntry::help(vec!["new".into()]));
        assert_eq!(stack.len(), 1);
        assert_eq!(stack.entries[0].content[0], "new");
    }

    #[test]
    fn push_stacks_different_kinds() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(help_lines()));
        stack.push(OverlayEntry::confirm(&ConfirmContent {
            message: "Retry?".into(),
            ..Default::default()
        }));
        assert_eq!(stack.len(), 2);
        assert_eq!(stack.top_kind(), Some(OverlayKind::Confirm));
    }

    #[test]
    fn pop_removes_topmost() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(help_lines()));
        stack.push(OverlayEntry::confirm(&ConfirmContent::default()));
        let popped = stack.pop().unwrap();
        assert_eq!(popped.kind, OverlayKind::Confirm);
        assert_eq!(stack.top_kind(), Some(OverlayKind::Help));
    }

    #[test]
    fn dismiss_by_kind() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(help_lines()));
        stack.push(OverlayEntry::confirm(&ConfirmContent::default()));
        stack.dismiss(OverlayKind::Help);
        assert_eq!(stack.len(), 1);
        assert_eq!(stack.top_kind(), Some(OverlayKind::Confirm));
    }

    #[test]
    fn clear_removes_all() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(help_lines()));
        stack.push(OverlayEntry::confirm(&ConfirmContent::default()));
        stack.clear();
        assert!(stack.is_empty());
    }

    #[test]
    fn toggle_pushes_when_absent() {
        let mut stack = OverlayStack::new();
        stack.toggle(OverlayEntry::help(help_lines()));
        assert_eq!(stack.len(), 1);
        assert_eq!(stack.top_kind(), Some(OverlayKind::Help));
    }

    #[test]
    fn toggle_dismisses_when_present() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(help_lines()));
        stack.toggle(OverlayEntry::help(help_lines()));
        assert!(stack.is_empty());
    }

    #[test]
    fn compute_rect_centered() {
        let area = Rect::new(0, 0, 100, 50);
        let entry = OverlayEntry::help(help_lines());
        let rect = entry.compute_rect(area);
        // Should be centered.
        assert!(rect.x > 0);
        assert!(rect.y > 0);
        assert!(rect.x + rect.width <= area.width);
        assert!(rect.y + rect.height <= area.height);
    }

    #[test]
    fn compute_rect_small_terminal() {
        let area = Rect::new(0, 0, 30, 10);
        let entry = OverlayEntry::help(help_lines());
        let rect = entry.compute_rect(area);
        // Should be at least minimum size.
        assert!(rect.width >= 20);
        assert!(rect.height >= 5);
    }

    #[test]
    fn overlay_kind_titles() {
        assert_eq!(OverlayKind::Help.title(), " Help ");
        assert_eq!(OverlayKind::Confirm.title(), " Confirm ");
        assert_eq!(OverlayKind::GateDetail.title(), " Gate Details ");
    }

    #[test]
    fn confirm_entry_content() {
        let confirm = ConfirmContent {
            message: "Retry this task?".into(),
            confirm_label: "Yes (y)".into(),
            cancel_label: "No (n)".into(),
        };
        let entry = OverlayEntry::confirm(&confirm);
        assert_eq!(entry.kind, OverlayKind::Confirm);
        assert!(entry.content[0].contains("Retry"));
    }

    #[test]
    fn gate_detail_entry_content() {
        let detail = GateDetailContent {
            name: "tests_passed".into(),
            passed: false,
            detail: "Exit code 1".into(),
        };
        let entry = OverlayEntry::gate_detail(&detail);
        assert_eq!(entry.kind, OverlayKind::GateDetail);
        assert!(entry.content.iter().any(|l| l.contains("FAILED")));
    }

    #[test]
    fn default_stack_is_empty() {
        let stack = OverlayStack::default();
        assert!(stack.is_empty());
    }

    #[test]
    fn iter_returns_bottom_to_top() {
        let mut stack = OverlayStack::new();
        stack.push(OverlayEntry::help(help_lines()));
        stack.push(OverlayEntry::confirm(&ConfirmContent::default()));
        let kinds: Vec<_> = stack.iter().map(|e| e.kind).collect();
        assert_eq!(kinds, vec![OverlayKind::Help, OverlayKind::Confirm]);
    }
}
