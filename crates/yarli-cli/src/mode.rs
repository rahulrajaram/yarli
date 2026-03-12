//! Render mode auto-detection (Section 16.3).
//!
//! Determines the appropriate rendering mode based on:
//! - TTY detection: non-TTY (pipe/redirect) forces stream mode.
//! - Terminal size: < 80 cols or < 24 rows forces stream mode.
//! - CLI flags: `--stream` or `--tui` override auto-detection.

use std::io::{self, IsTerminal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalColorSupport {
    None,
    Ansi16,
    TrueColor,
}

impl TerminalColorSupport {
    pub fn detect_from_env() -> Self {
        Self::detect_with(|key| std::env::var(key).ok())
    }

    fn detect_with(get_env: impl Fn(&str) -> Option<String>) -> Self {
        if get_env("NO_COLOR").is_some() {
            return Self::None;
        }

        let colorterm = get_env("COLORTERM")
            .unwrap_or_default()
            .to_ascii_lowercase();
        if colorterm.contains("truecolor") || colorterm.contains("24bit") {
            return Self::TrueColor;
        }

        let term = get_env("TERM").unwrap_or_default().to_ascii_lowercase();
        if term.contains("truecolor") || term.contains("24bit") || term.contains("direct") {
            return Self::TrueColor;
        }

        if get_env("TERM_PROGRAM")
            .unwrap_or_default()
            .eq_ignore_ascii_case("wezterm")
        {
            return Self::TrueColor;
        }

        if get_env("KITTY_WINDOW_ID").is_some() {
            return Self::TrueColor;
        }

        Self::Ansi16
    }

    pub fn is_truecolor(self) -> bool {
        matches!(self, Self::TrueColor)
    }
}

/// Rendering mode for the CLI output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderMode {
    /// Inline viewport with live status, completed output in native scrollback.
    /// Used for CI, pipes, small terminals, non-TTY.
    Stream,
    /// Fullscreen panel layout (Milestone 4 — not yet implemented).
    Dashboard,
}

/// Minimum terminal width for dashboard mode.
const MIN_DASHBOARD_COLS: u16 = 80;
/// Minimum terminal height for dashboard mode.
const MIN_DASHBOARD_ROWS: u16 = 24;

/// Terminal capability information used for mode detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalInfo {
    pub is_tty: bool,
    pub cols: u16,
    pub rows: u16,
    pub color_support: TerminalColorSupport,
}

impl TerminalInfo {
    /// Probe the current terminal.
    pub fn detect() -> Self {
        let is_tty = io::stdout().is_terminal();
        let (cols, rows) = crossterm::terminal::size().unwrap_or((0, 0));
        let color_support = if is_tty {
            TerminalColorSupport::detect_from_env()
        } else {
            TerminalColorSupport::None
        };
        Self {
            is_tty,
            cols,
            rows,
            color_support,
        }
    }

    /// Whether the terminal meets minimum size requirements for dashboard mode.
    pub fn supports_dashboard(&self) -> bool {
        self.is_tty && self.cols >= MIN_DASHBOARD_COLS && self.rows >= MIN_DASHBOARD_ROWS
    }
}

/// Select the render mode based on terminal info and CLI flags.
///
/// Priority:
/// 1. `--stream` flag → Stream
/// 2. `--tui` flag → Dashboard (requires TTY with sufficient size)
/// 3. Auto-detect from terminal capabilities
pub fn select_render_mode(
    info: &TerminalInfo,
    force_stream: bool,
    force_tui: bool,
) -> Result<RenderMode, RenderModeError> {
    if force_stream {
        return Ok(RenderMode::Stream);
    }

    if force_tui {
        if !info.supports_dashboard() {
            return Err(RenderModeError::TerminalTooSmall {
                cols: info.cols,
                rows: info.rows,
            });
        }
        return Ok(RenderMode::Dashboard);
    }

    // Auto-detect: dashboard if TTY meets minimum size, stream otherwise.
    if info.supports_dashboard() {
        Ok(RenderMode::Dashboard)
    } else {
        Ok(RenderMode::Stream)
    }
}

/// Errors from render mode selection.
#[derive(Debug, thiserror::Error)]
pub enum RenderModeError {
    #[error("terminal too small for dashboard mode ({cols}x{rows}, need {MIN_DASHBOARD_COLS}x{MIN_DASHBOARD_ROWS})")]
    TerminalTooSmall { cols: u16, rows: u16 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tty_large() -> TerminalInfo {
        TerminalInfo {
            is_tty: true,
            cols: 120,
            rows: 40,
            color_support: TerminalColorSupport::Ansi16,
        }
    }

    fn tty_small() -> TerminalInfo {
        TerminalInfo {
            is_tty: true,
            cols: 60,
            rows: 20,
            color_support: TerminalColorSupport::Ansi16,
        }
    }

    fn pipe() -> TerminalInfo {
        TerminalInfo {
            is_tty: false,
            cols: 120,
            rows: 40,
            color_support: TerminalColorSupport::None,
        }
    }

    fn tty_narrow() -> TerminalInfo {
        TerminalInfo {
            is_tty: true,
            cols: 79,
            rows: 40,
            color_support: TerminalColorSupport::Ansi16,
        }
    }

    fn tty_short() -> TerminalInfo {
        TerminalInfo {
            is_tty: true,
            cols: 120,
            rows: 23,
            color_support: TerminalColorSupport::Ansi16,
        }
    }

    // --- supports_dashboard ---

    #[test]
    fn large_tty_supports_dashboard() {
        assert!(tty_large().supports_dashboard());
    }

    #[test]
    fn pipe_does_not_support_dashboard() {
        assert!(!pipe().supports_dashboard());
    }

    #[test]
    fn small_tty_does_not_support_dashboard() {
        assert!(!tty_small().supports_dashboard());
    }

    #[test]
    fn narrow_tty_does_not_support_dashboard() {
        assert!(!tty_narrow().supports_dashboard());
    }

    #[test]
    fn short_tty_does_not_support_dashboard() {
        assert!(!tty_short().supports_dashboard());
    }

    #[test]
    fn exact_minimum_supports_dashboard() {
        let info = TerminalInfo {
            is_tty: true,
            cols: MIN_DASHBOARD_COLS,
            rows: MIN_DASHBOARD_ROWS,
            color_support: TerminalColorSupport::Ansi16,
        };
        assert!(info.supports_dashboard());
    }

    #[test]
    fn detects_truecolor_from_colorterm() {
        let detected = TerminalColorSupport::detect_with(|key| match key {
            "COLORTERM" => Some("truecolor".to_string()),
            _ => None,
        });
        assert_eq!(detected, TerminalColorSupport::TrueColor);
    }

    #[test]
    fn detects_truecolor_from_term_suffix() {
        let detected = TerminalColorSupport::detect_with(|key| match key {
            "TERM" => Some("xterm-direct".to_string()),
            _ => None,
        });
        assert_eq!(detected, TerminalColorSupport::TrueColor);
    }

    #[test]
    fn no_color_disables_color_output() {
        let detected = TerminalColorSupport::detect_with(|key| match key {
            "NO_COLOR" => Some("1".to_string()),
            "COLORTERM" => Some("truecolor".to_string()),
            _ => None,
        });
        assert_eq!(detected, TerminalColorSupport::None);
    }

    // --- select_render_mode ---

    #[test]
    fn force_stream_always_stream() {
        let mode = select_render_mode(&tty_large(), true, false).unwrap();
        assert_eq!(mode, RenderMode::Stream);
    }

    #[test]
    fn force_tui_returns_dashboard_on_large_tty() {
        let mode = select_render_mode(&tty_large(), false, true).unwrap();
        assert_eq!(mode, RenderMode::Dashboard);
    }

    #[test]
    fn force_tui_errors_on_small_terminal() {
        let result = select_render_mode(&tty_small(), false, true);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too small"));
    }

    #[test]
    fn force_stream_wins_over_tui() {
        // --stream flag is checked first, so it takes precedence.
        let mode = select_render_mode(&tty_large(), true, true).unwrap();
        assert_eq!(mode, RenderMode::Stream);
    }

    #[test]
    fn auto_detect_pipe_returns_stream() {
        let mode = select_render_mode(&pipe(), false, false).unwrap();
        assert_eq!(mode, RenderMode::Stream);
    }

    #[test]
    fn auto_detect_small_tty_returns_stream() {
        let mode = select_render_mode(&tty_small(), false, false).unwrap();
        assert_eq!(mode, RenderMode::Stream);
    }

    #[test]
    fn auto_detect_large_tty_returns_dashboard() {
        let mode = select_render_mode(&tty_large(), false, false).unwrap();
        assert_eq!(mode, RenderMode::Dashboard);
    }

    #[test]
    fn auto_detect_narrow_returns_stream() {
        let mode = select_render_mode(&tty_narrow(), false, false).unwrap();
        assert_eq!(mode, RenderMode::Stream);
    }

    #[test]
    fn auto_detect_short_returns_stream() {
        let mode = select_render_mode(&tty_short(), false, false).unwrap();
        assert_eq!(mode, RenderMode::Stream);
    }

    #[test]
    fn terminal_info_detect_does_not_panic() {
        // Should not panic even in non-TTY (CI) environments.
        let _info = TerminalInfo::detect();
    }
}
