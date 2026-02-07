//! Policy engine error types.

use thiserror::Error;
use uuid::Uuid;

/// Errors from policy evaluation.
#[derive(Debug, Error)]
pub enum PolicyError {
    /// The requested action was denied by policy.
    #[error("policy denied action '{action}': {reason} (rule: {rule_id})")]
    Denied {
        action: String,
        rule_id: String,
        reason: String,
    },

    /// The action requires an approval token that is missing or invalid.
    #[error("action '{action}' requires approval: {reason}")]
    ApprovalRequired { action: String, reason: String },

    /// An approval token was provided but is expired.
    #[error("approval token {token_id} expired")]
    TokenExpired { token_id: Uuid },

    /// An approval token was provided but doesn't match the requested scope.
    #[error("approval token {token_id} scope mismatch: {details}")]
    TokenScopeMismatch { token_id: Uuid, details: String },

    /// Safe mode does not permit this action.
    #[error("safe mode '{mode}' does not permit action '{action}'")]
    SafeModeViolation { mode: String, action: String },

    /// Internal error during policy evaluation.
    #[error("policy evaluation error: {0}")]
    Internal(String),
}
