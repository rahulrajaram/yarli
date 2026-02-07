//! Custom TUI widgets for the dashboard (Section 32).
//!
//! `CollapsiblePanel` — panels that expand, collapse to header-only, or hide entirely (~200 LOC).

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Widget};

use super::state::PanelState;
use crate::stream::style::Tier;

/// A panel that can be expanded, collapsed (header-only), or hidden.
///
/// In expanded state, renders a bordered block with title and content.
/// In collapsed state, renders only the title bar (1 line).
/// In hidden state, renders nothing (0 height).
pub struct CollapsiblePanel<'a> {
    title: &'a str,
    content: Vec<Line<'a>>,
    state: PanelState,
    focused: bool,
    scroll_offset: u16,
    shortcut: Option<char>,
    borderless: bool,
}

impl<'a> CollapsiblePanel<'a> {
    pub fn new(title: &'a str, state: PanelState) -> Self {
        Self {
            title,
            content: Vec::new(),
            state,
            focused: false,
            scroll_offset: 0,
            shortcut: None,
            borderless: false,
        }
    }

    pub fn content(mut self, content: Vec<Line<'a>>) -> Self {
        self.content = content;
        self
    }

    pub fn focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    pub fn scroll_offset(mut self, offset: u16) -> Self {
        self.scroll_offset = offset;
        self
    }

    pub fn shortcut(mut self, shortcut: Option<char>) -> Self {
        self.shortcut = shortcut;
        self
    }

    /// Set borderless mode (copy mode — strips borders to maximize text area).
    pub fn borderless(mut self, borderless: bool) -> Self {
        self.borderless = borderless;
        self
    }

    /// The minimum height this panel needs in its current state.
    pub fn min_height(&self) -> u16 {
        match self.state {
            PanelState::Hidden => 0,
            PanelState::Collapsed => 1,
            PanelState::Expanded if self.borderless => 1, // no borders, just content
            PanelState::Expanded => 3, // top border + 1 line content + bottom border
        }
    }

    /// Build the block with appropriate styling for focus state.
    fn build_block(&self) -> Block<'a> {
        let border_style = if self.focused {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Tier::Background.style()
        };

        let state_indicator = match self.state {
            PanelState::Expanded => "[-]",
            PanelState::Collapsed => "[+]",
            PanelState::Hidden => "",
        };

        let shortcut_str = self.shortcut.map(|c| format!(" {c}:")).unwrap_or_default();

        let title = format!("{shortcut_str}{} {state_indicator}", self.title);

        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(Span::styled(title, border_style))
    }
}

impl Widget for CollapsiblePanel<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self.state {
            PanelState::Hidden => {
                // Render nothing.
            }
            PanelState::Collapsed => {
                // Render just a single-line header.
                if area.height == 0 {
                    return;
                }
                let block = self.build_block();
                let header_area = Rect {
                    height: 1.min(area.height),
                    ..area
                };
                // Render as a single line: "▶ Title [+]"
                let border_style = if self.focused {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Tier::Background.style()
                };
                let shortcut_str = self.shortcut.map(|c| format!("{c}:")).unwrap_or_default();
                let line = Line::from(vec![
                    Span::styled("▶ ", border_style),
                    Span::styled(format!("{shortcut_str}{} [+]", self.title), border_style),
                ]);
                let _ = block; // not used in collapsed mode
                Paragraph::new(line).render(header_area, buf);
            }
            PanelState::Expanded if self.borderless => {
                // Copy mode: render content directly without borders.
                let visible_lines: Vec<Line<'_>> = self
                    .content
                    .into_iter()
                    .skip(self.scroll_offset as usize)
                    .take(area.height as usize)
                    .collect();

                Paragraph::new(visible_lines).render(area, buf);
            }
            PanelState::Expanded => {
                let block = self.build_block();
                let inner = block.inner(area);
                block.render(area, buf);

                // Apply scroll offset.
                let visible_lines: Vec<Line<'_>> = self
                    .content
                    .into_iter()
                    .skip(self.scroll_offset as usize)
                    .take(inner.height as usize)
                    .collect();

                Paragraph::new(visible_lines).render(inner, buf);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_height_by_state() {
        let panel = CollapsiblePanel::new("Test", PanelState::Expanded);
        assert_eq!(panel.min_height(), 3);

        let panel = CollapsiblePanel::new("Test", PanelState::Collapsed);
        assert_eq!(panel.min_height(), 1);

        let panel = CollapsiblePanel::new("Test", PanelState::Hidden);
        assert_eq!(panel.min_height(), 0);
    }

    #[test]
    fn collapsed_renders_in_one_line() {
        let panel = CollapsiblePanel::new("Tasks", PanelState::Collapsed).shortcut(Some('1'));
        let area = Rect::new(0, 0, 40, 1);
        let mut buf = Buffer::empty(area);
        panel.render(area, &mut buf);

        // Should have rendered something in the first line.
        let line: String = (0..40)
            .map(|x| {
                buf.cell((x, 0))
                    .unwrap()
                    .symbol()
                    .chars()
                    .next()
                    .unwrap_or(' ')
            })
            .collect();
        assert!(line.contains("Tasks"));
    }

    #[test]
    fn hidden_renders_nothing() {
        let panel = CollapsiblePanel::new("Tasks", PanelState::Hidden);
        let area = Rect::new(0, 0, 40, 5);
        let mut buf = Buffer::empty(area);
        // Fill with markers to detect changes.
        for y in 0..5 {
            for x in 0..40 {
                buf.cell_mut((x, y)).unwrap().set_char('X');
            }
        }
        panel.render(area, &mut buf);
        // All cells should still be 'X' (nothing rendered).
        for y in 0..5 {
            for x in 0..40 {
                assert_eq!(buf.cell((x, y)).unwrap().symbol(), "X");
            }
        }
    }

    #[test]
    fn expanded_renders_block_with_content() {
        let content = vec![Line::from("Hello"), Line::from("World")];
        let panel = CollapsiblePanel::new("Test", PanelState::Expanded)
            .content(content)
            .focused(true);
        let area = Rect::new(0, 0, 40, 5);
        let mut buf = Buffer::empty(area);
        panel.render(area, &mut buf);

        // Check that content appears (inside the borders).
        let line: String = (0..40)
            .map(|x| {
                buf.cell((x, 1))
                    .unwrap()
                    .symbol()
                    .chars()
                    .next()
                    .unwrap_or(' ')
            })
            .collect();
        assert!(line.contains("Hello"));
    }

    #[test]
    fn borderless_min_height() {
        let panel = CollapsiblePanel::new("Test", PanelState::Expanded).borderless(true);
        assert_eq!(panel.min_height(), 1); // no borders
    }

    #[test]
    fn borderless_renders_without_borders() {
        let content = vec![Line::from("Hello"), Line::from("World")];
        let panel = CollapsiblePanel::new("Test", PanelState::Expanded)
            .content(content)
            .borderless(true);
        let area = Rect::new(0, 0, 40, 5);
        let mut buf = Buffer::empty(area);
        panel.render(area, &mut buf);

        // First line should contain content directly (no border).
        let line: String = (0..40)
            .map(|x| {
                buf.cell((x, 0))
                    .unwrap()
                    .symbol()
                    .chars()
                    .next()
                    .unwrap_or(' ')
            })
            .collect();
        assert!(line.contains("Hello"));
    }

    #[test]
    fn scroll_offset_skips_lines() {
        let content = vec![
            Line::from("Line 0"),
            Line::from("Line 1"),
            Line::from("Line 2"),
        ];
        let panel = CollapsiblePanel::new("Test", PanelState::Expanded)
            .content(content)
            .scroll_offset(1);
        let area = Rect::new(0, 0, 40, 5);
        let mut buf = Buffer::empty(area);
        panel.render(area, &mut buf);

        // First visible line should be "Line 1" (skipped "Line 0").
        let line: String = (0..40)
            .map(|x| {
                buf.cell((x, 1))
                    .unwrap()
                    .symbol()
                    .chars()
                    .next()
                    .unwrap_or(' ')
            })
            .collect();
        assert!(line.contains("Line 1"));
    }
}
