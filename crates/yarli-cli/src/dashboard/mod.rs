//! Dashboard mode renderer (Section 16.3).
//!
//! Fullscreen panel layout with task list, output viewer, gate status,
//! and persistent "Why Not Done?" bar. Panels are collapsible, scrollable,
//! and support focus cycling.

pub mod copy_mode;
pub mod input;
pub mod overlay;
pub mod renderer;
pub mod state;
pub mod widgets;

pub use copy_mode::CopyMode;
pub use overlay::{OverlayEntry, OverlayKind, OverlayStack};
pub use renderer::{DashboardConfig, DashboardRenderer};
pub use state::{PanelId, PanelManager, PanelState};
pub use widgets::CollapsiblePanel;
