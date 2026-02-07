//! Gate engine error types.

use thiserror::Error;
use uuid::Uuid;
use yarli_core::explain::GateType;

/// Errors from gate evaluation.
#[derive(Debug, Error)]
pub enum GateError {
    /// Evidence required for gate evaluation is missing.
    #[error("missing evidence for gate {gate:?}: {details}")]
    MissingEvidence { gate: GateType, details: String },

    /// Evidence validation failed (malformed payload, wrong schema).
    #[error("invalid evidence {evidence_id}: {details}")]
    InvalidEvidence { evidence_id: Uuid, details: String },

    /// Gate evaluation encountered an internal error.
    #[error("gate evaluation error for {gate:?}: {source}")]
    EvaluationFailed {
        gate: GateType,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Event store error during gate evaluation.
    #[error("store error: {0}")]
    Store(#[from] yarli_store::StoreError),

    /// Task or run not found.
    #[error("entity not found: {0}")]
    NotFound(String),
}
