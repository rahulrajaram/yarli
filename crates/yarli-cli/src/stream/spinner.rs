//! Braille spinner for active task indicators.
//!
//! Uses fixed-width braille dots (`⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`) to avoid layout jitter.
//! Transitions to `✓`/`✗` on completion (Section 30).

/// Braille spinner frames — each character occupies the same visual width.
const FRAMES: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// A braille spinner that advances through frames.
#[derive(Debug, Clone)]
pub struct Spinner {
    frame_idx: usize,
}

impl Default for Spinner {
    fn default() -> Self {
        Self::new()
    }
}

impl Spinner {
    pub fn new() -> Self {
        Self { frame_idx: 0 }
    }

    /// Get the current frame character.
    pub fn frame(&self) -> char {
        FRAMES[self.frame_idx % FRAMES.len()]
    }

    /// Advance to the next frame.
    pub fn tick(&mut self) {
        self.frame_idx = (self.frame_idx + 1) % FRAMES.len();
    }
}

/// Terminal glyphs for task states (Section 16.3).
pub const GLYPH_COMPLETE: char = '✓';
pub const GLYPH_FAILED: char = '✗';
pub const GLYPH_BLOCKED: char = '◌';
pub const GLYPH_PENDING: char = '○';

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spinner_cycles_through_frames() {
        let mut s = Spinner::new();
        assert_eq!(s.frame(), '⠋');
        s.tick();
        assert_eq!(s.frame(), '⠙');
        for _ in 0..9 {
            s.tick();
        }
        // Back to start after 10 ticks total
        assert_eq!(s.frame(), '⠋');
    }

    #[test]
    fn spinner_wraps_around() {
        let mut s = Spinner::new();
        for _ in 0..25 {
            s.tick();
        }
        // 25 % 10 = 5
        assert_eq!(s.frame(), FRAMES[5]);
    }

    #[test]
    fn all_frames_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for &f in FRAMES {
            assert!(seen.insert(f), "duplicate frame: {f}");
        }
    }
}
