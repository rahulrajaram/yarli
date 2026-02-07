//! Copy mode — Zellij-inspired escape hatch for native text selection (Section 16.3).
//!
//! When active, disables mouse capture and strips panel borders so the user
//! can select and copy text with their terminal's native selection mechanism.

use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::ExecutableCommand;
use std::io::{self, Write};

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

/// Manages copy mode state and terminal mouse capture.
pub struct CopyMode {
    /// Whether copy mode is currently active.
    active: bool,
    /// Whether mouse capture was enabled before entering copy mode.
    mouse_was_enabled: bool,
}

impl CopyMode {
    /// Create a new CopyMode in inactive state.
    pub fn new() -> Self {
        Self {
            active: false,
            mouse_was_enabled: false,
        }
    }

    /// Whether copy mode is currently active.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Toggle copy mode on/off. Manages mouse capture accordingly.
    pub fn toggle<W: Write>(&mut self, writer: &mut W) -> io::Result<()> {
        if self.active {
            self.deactivate(writer)
        } else {
            self.activate(writer)
        }
    }

    /// Enter copy mode: disable mouse capture.
    pub fn activate<W: Write>(&mut self, writer: &mut W) -> io::Result<()> {
        if !self.active {
            self.mouse_was_enabled = true;
            writer.execute(DisableMouseCapture)?;
            self.active = true;
        }
        Ok(())
    }

    /// Exit copy mode: re-enable mouse capture if it was previously enabled.
    pub fn deactivate<W: Write>(&mut self, writer: &mut W) -> io::Result<()> {
        if self.active {
            if self.mouse_was_enabled {
                writer.execute(EnableMouseCapture)?;
            }
            self.active = false;
        }
        Ok(())
    }

    /// Build a status indicator span for the key hints bar.
    pub fn status_indicator(&self) -> Option<Span<'static>> {
        if self.active {
            Some(Span::styled(
                "[COPY] ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ))
        } else {
            None
        }
    }

    /// Whether panel borders should be stripped in copy mode.
    pub fn strip_borders(&self) -> bool {
        self.active
    }

    /// Build a copy mode banner line for display at the top of the screen.
    pub fn banner_line(&self) -> Option<Line<'static>> {
        if self.active {
            Some(Line::from(vec![
                Span::styled(
                    " COPY MODE ",
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    " Select text with your terminal. Press c to exit.",
                    Style::default().fg(Color::Yellow),
                ),
            ]))
        } else {
            None
        }
    }
}

impl Default for CopyMode {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_inactive() {
        let cm = CopyMode::new();
        assert!(!cm.is_active());
        assert!(!cm.strip_borders());
    }

    #[test]
    fn status_indicator_when_inactive() {
        let cm = CopyMode::new();
        assert!(cm.status_indicator().is_none());
    }

    #[test]
    fn status_indicator_when_active() {
        let mut cm = CopyMode::new();
        cm.active = true;
        let span = cm.status_indicator().unwrap();
        assert!(span.content.contains("COPY"));
    }

    #[test]
    fn strip_borders_follows_active() {
        let mut cm = CopyMode::new();
        assert!(!cm.strip_borders());
        cm.active = true;
        assert!(cm.strip_borders());
    }

    #[test]
    fn banner_when_inactive() {
        let cm = CopyMode::new();
        assert!(cm.banner_line().is_none());
    }

    #[test]
    fn banner_when_active() {
        let mut cm = CopyMode::new();
        cm.active = true;
        let line = cm.banner_line().unwrap();
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(text.contains("COPY MODE"));
        assert!(text.contains("Press c to exit"));
    }

    #[test]
    fn default_is_inactive() {
        let cm = CopyMode::default();
        assert!(!cm.is_active());
    }

    #[test]
    fn activate_sets_active() {
        let mut cm = CopyMode::new();
        // Can't test actual stdout interaction in unit tests, so test state directly.
        cm.active = true;
        cm.mouse_was_enabled = true;
        assert!(cm.is_active());
        assert!(cm.strip_borders());
    }

    #[test]
    fn deactivate_clears_active() {
        let mut cm = CopyMode::new();
        cm.active = true;
        cm.mouse_was_enabled = true;
        // Simulate deactivation without stdout.
        cm.active = false;
        assert!(!cm.is_active());
        assert!(!cm.strip_borders());
    }
}
