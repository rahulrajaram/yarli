//! Visual hierarchy system (Section 28).
//!
//! 4-tier visual weight system:
//! - URGENT: Bold + Red/Yellow — errors, blockers, conflicts
//! - ACTIVE: Bold + White/Cyan — executing tasks, live output
//! - CONTEXTUAL: Normal + Gray — completed tasks, metadata
//! - BACKGROUND: Dim + Dark Gray — decorative elements

use ratatui::style::{Color, Modifier, Style};

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
        match self {
            Tier::Urgent => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            Tier::Active => Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
            Tier::Contextual => Style::default().fg(Color::DarkGray),
            Tier::Background => Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        }
    }

    /// Secondary/accent style for this tier.
    pub fn accent(self) -> Style {
        match self {
            Tier::Urgent => Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
            Tier::Active => Style::default().fg(Color::Cyan),
            Tier::Contextual => Style::default().fg(Color::Gray),
            Tier::Background => Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        }
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
        let s = Tier::Urgent.style();
        assert_eq!(s.fg, Some(Color::Red));
        assert!(s.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn active_is_bold_white() {
        let s = Tier::Active.style();
        assert_eq!(s.fg, Some(Color::White));
        assert!(s.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn contextual_is_dark_gray() {
        let s = Tier::Contextual.style();
        assert_eq!(s.fg, Some(Color::DarkGray));
    }

    #[test]
    fn background_is_dim_dark_gray() {
        let s = Tier::Background.style();
        assert_eq!(s.fg, Some(Color::DarkGray));
        assert!(s.add_modifier.contains(Modifier::DIM));
    }

    #[test]
    fn urgent_accent_is_yellow() {
        let s = Tier::Urgent.accent();
        assert_eq!(s.fg, Some(Color::Yellow));
    }

    #[test]
    fn active_accent_is_cyan() {
        let s = Tier::Active.accent();
        assert_eq!(s.fg, Some(Color::Cyan));
    }
}
