//! yarli-gates: Gate engine and evidence validation.
//!
//! Implements the verification gate model (Section 11) with 7 gate types.
//! All required gates must pass for `TASK_COMPLETE` and `RUN_COMPLETED`.
//! Gates are deterministic functions over persisted evidence and state.

pub mod engine;
pub mod error;
pub mod evidence;

// Re-exports for convenience
pub use engine::{
    all_passed, collect_failures, default_run_gates, default_task_gates, evaluate_all,
    evaluate_gate, GateContext, GateEvaluation,
};
pub use error::GateError;
pub use evidence::{
    validate_evidence_schema, CommandEvidence, EvidenceKind, GitMutationEvidence,
    MemoryOperationEvidence, TestFailure, TestReportEvidence, ValidatedEvidence,
};
