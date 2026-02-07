//! Memory adapter error types.

use thiserror::Error;

/// Errors from memory operations.
#[derive(Error, Debug)]
pub enum MemoryError {
    /// Memory record not found.
    #[error("memory not found: {0}")]
    NotFound(String),

    /// Project or scope does not exist.
    #[error("scope not found: project={project}, scope={scope}")]
    ScopeNotFound { project: String, scope: String },

    /// Connection to memory backend failed.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Memory operation timed out.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// Backend returned an error.
    #[error("backend error: {0}")]
    Backend(String),

    /// Content failed redaction check (secrets detected).
    #[error("redaction required: content may contain secrets")]
    RedactionRequired,

    /// Scope is closed (read-only).
    #[error("scope is closed: {0}")]
    ScopeClosed(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}
