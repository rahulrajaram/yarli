//! yarli-cli: CLI, stream mode renderer, and interactive TUI.
//!
//! This crate provides the user-facing interfaces for YARLI:
//! - **Stream mode**: inline viewport with live status (Section 30)
//! - **Dashboard mode**: fullscreen TUI with panel layout (Section 16.3)
//! - **CLI commands**: `yarli run`, `yarli task`, etc.
//! - **Mode detection**: auto-detect rendering mode from terminal capabilities

pub const BUILD_COMMIT: &str = env!("YARLI_BUILD_COMMIT");
pub const BUILD_DATE: &str = env!("YARLI_BUILD_DATE");
pub const BUILD_ID: &str = env!("YARLI_BUILD_ID");
pub const YARLI_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (commit ",
    env!("YARLI_BUILD_COMMIT"),
    ", date ",
    env!("YARLI_BUILD_DATE"),
    ", build ",
    env!("YARLI_BUILD_ID"),
    ")"
);
pub const DEFAULT_CONTINUATION_FILE: &str = ".yarli/continuation.json";

pub mod cli;
pub mod config;
pub mod dashboard;
pub mod mode;
pub mod prompt;
pub mod stream;
