//! yarli-cli: CLI, stream mode renderer, and interactive TUI.
//!
//! This crate provides the user-facing interfaces for YARLI:
//! - **Stream mode**: inline viewport with live status (Section 30)
//! - **Dashboard mode**: fullscreen TUI with panel layout (Section 16.3)
//! - **CLI commands**: `yarli run`, `yarli task`, etc.
//! - **Mode detection**: auto-detect rendering mode from terminal capabilities

pub mod dashboard;
pub mod config;
pub mod mode;
pub mod stream;
