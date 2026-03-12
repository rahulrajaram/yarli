//! Visual hierarchy system (Section 28).
//!
//! 4-tier visual weight system:
//! - URGENT: Bold + Red/Yellow — errors, blockers, conflicts
//! - ACTIVE: Bold + White/Cyan — executing tasks, live output
//! - CONTEXTUAL: Normal + Gray — completed tasks, metadata
//! - BACKGROUND: Dim + Dark Gray — decorative elements

use ratatui::style::{Color, Modifier, Style};

use crate::mode::TerminalColorSupport;

/// Visual tier for the 4-level hierarchy system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Bold + Red/Yellow — errors, blockers, gate failures, action needed.
    Urgent,
    /// Bold + White/Cyan — currently executing task, live output, progress.
    Active,
    /// Normal + Gray — completed tasks, timestamps, metadata, historical output.
    Contextual,
    /// Dim + Dark Gray — decorative borders, dividers, inactive content.
    Background,
}

impl Tier {
    /// Primary style for this tier.
    pub fn style(self) -> Style {
        self.style_for(TerminalColorSupport::detect_from_env())
    }

    pub fn style_for(self, support: TerminalColorSupport) -> Style {
        match self {
            Tier::Urgent => Style::default()
                .fg(urgent_primary(support))
                .add_modifier(Modifier::BOLD),
            Tier::Active => Style::default()
                .fg(active_primary(support))
                .add_modifier(Modifier::BOLD),
            Tier::Contextual => Style::default().fg(contextual_primary(support)),
            Tier::Background => Style::default()
                .fg(background_primary(support))
                .add_modifier(Modifier::DIM),
        }
    }

    /// Secondary/accent style for this tier.
    pub fn accent(self) -> Style {
        self.accent_for(TerminalColorSupport::detect_from_env())
    }

    pub fn accent_for(self, support: TerminalColorSupport) -> Style {
        match self {
            Tier::Urgent => Style::default()
                .fg(urgent_accent(support))
                .add_modifier(Modifier::BOLD),
            Tier::Active => Style::default().fg(active_accent(support)),
            Tier::Contextual => Style::default().fg(contextual_accent(support)),
            Tier::Background => Style::default()
                .fg(background_primary(support))
                .add_modifier(Modifier::DIM),
        }
    }
}

fn urgent_primary(support: TerminalColorSupport) -> Color {
    match support {
        TerminalColorSupport::TrueColor => Color::Rgb(255, 107, 107),
        TerminalColorSupport::Ansi16 | TerminalColorSupport::None => Color::Red,
    }
}

fn urgent_accent(support: TerminalColorSupport) -> Color {
    match support {
        TerminalColorSupport::TrueColor => Color::Rgb(255, 209, 102),
        TerminalColorSupport::Ansi16 | TerminalColorSupport::None => Color::Yellow,
    }
}

fn active_primary(support: TerminalColorSupport) -> Color {
    match support {
        TerminalColorSupport::TrueColor => Color::Rgb(245, 247, 250),
        TerminalColorSupport::Ansi16 | TerminalColorSupport::None => Color::White,
    }
}

fn active_accent(support: TerminalColorSupport) -> Color {
    match support {
        TerminalColorSupport::TrueColor => Color::Rgb(78, 205, 196),
        TerminalColorSupport::Ansi16 | TerminalColorSupport::None => Color::Cyan,
    }
}

fn contextual_primary(support: TerminalColorSupport) -> Color {
    match support {
        TerminalColorSupport::TrueColor => Color::Rgb(146, 154, 171),
        TerminalColorSupport::Ansi16 | TerminalColorSupport::None => Color::DarkGray,
    }
}

fn contextual_accent(support: TerminalColorSupport) -> Color {
    match support {
        TerminalColorSupport::TrueColor => Color::Rgb(176, 184, 200),
        TerminalColorSupport::Ansi16 | TerminalColorSupport::None => Color::Gray,
    }
}

fn background_primary(support: TerminalColorSupport) -> Color {
    match support {
        TerminalColorSupport::TrueColor => Color::Rgb(92, 99, 112),
        TerminalColorSupport::Ansi16 | TerminalColorSupport::None => Color::DarkGray,
    }
}

/// Style for the tree-drawing characters (├─, │, etc.).
pub fn tree_style() -> Style {
    Tier::Background.style()
}

/// Style for timestamps.
pub fn timestamp_style() -> Style {
    Tier::Contextual.style()
}

/// Style for the "▸" transition arrow.
pub fn arrow_style() -> Style {
    Tier::Background.style()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urgent_is_bold_red() {
        let s = Tier::Urgent.style_for(TerminalColorSupport::Ansi16);
        assert_eq!(s.fg, Some(Color::Red));
        assert!(s.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn active_is_bold_white() {
        let s = Tier::Active.style_for(TerminalColorSupport::Ansi16);
        assert_eq!(s.fg, Some(Color::White));
        assert!(s.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn contextual_is_dark_gray() {
        let s = Tier::Contextual.style_for(TerminalColorSupport::Ansi16);
        assert_eq!(s.fg, Some(Color::DarkGray));
    }

    #[test]
    fn background_is_dim_dark_gray() {
        let s = Tier::Background.style_for(TerminalColorSupport::Ansi16);
        assert_eq!(s.fg, Some(Color::DarkGray));
        assert!(s.add_modifier.contains(Modifier::DIM));
    }

    #[test]
    fn urgent_accent_is_yellow() {
        let s = Tier::Urgent.accent_for(TerminalColorSupport::Ansi16);
        assert_eq!(s.fg, Some(Color::Yellow));
    }

    #[test]
    fn active_accent_is_cyan() {
        let s = Tier::Active.accent_for(TerminalColorSupport::Ansi16);
        assert_eq!(s.fg, Some(Color::Cyan));
    }

    #[test]
    fn truecolor_active_accent_uses_rgb_palette() {
        let s = Tier::Active.accent_for(TerminalColorSupport::TrueColor);
        assert_eq!(s.fg, Some(Color::Rgb(78, 205, 196)));
    }
}
