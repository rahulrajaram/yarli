//! yarli-exec: Command runner, streaming, and execution journal.
//!
//! This crate provides:
//! - [`CommandRunner`] trait and [`LocalCommandRunner`] implementation for
//!   spawning child processes with streaming stdout/stderr capture.
//! - [`CommandJournal`] that wraps a runner and persists all execution events
//!   to an [`EventStore`](yarli_store::EventStore).
//! - Timeout and cancellation support via `tokio_util::CancellationToken`.

pub mod error;
pub mod journal;
pub mod runner;

pub use error::ExecError;
pub use journal::CommandJournal;
pub use runner::{CommandRequest, CommandResult, CommandRunner, LocalCommandRunner};
