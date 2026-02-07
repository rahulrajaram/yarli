//! Error types for yarli-exec.

use thiserror::Error;

/// Errors from command execution operations.
#[derive(Debug, Error)]
pub enum ExecError {
    /// Failed to spawn the child process.
    #[error("spawn failed: {0}")]
    SpawnFailed(#[source] std::io::Error),

    /// I/O error reading process output.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Command timed out after the configured duration.
    #[error("command timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Invalid state transition on the command entity.
    #[error("transition error: {0}")]
    Transition(#[from] yarli_core::error::TransitionError),

    /// The command was killed (e.g. during shutdown).
    #[error("command killed: {reason}")]
    Killed { reason: String },

    /// Journal persistence error.
    #[error("journal error: {0}")]
    Journal(String),
}
