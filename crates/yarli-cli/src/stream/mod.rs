//! Stream mode renderer (Section 30).
//!
//! Plain structured ANSI output to stdout with inline viewport for live status.
//! Completed output enters native terminal scrollback (copy-pasteable).
//! Active tasks shown via ratatui `Viewport::Inline` with braille spinners.
//! Used in CI, pipes, small terminals.

pub mod backend_output;
pub mod events;
pub mod headless;
pub mod renderer;
pub mod spinner;
pub mod style;

pub use backend_output::{normalize_output_lines, normalize_output_lines_with_options};
pub use events::{StreamEvent, TaskView};
pub use headless::HeadlessRenderer;
pub use renderer::{StreamConfig, StreamRenderer};
pub use spinner::Spinner;
pub use style::Tier;
