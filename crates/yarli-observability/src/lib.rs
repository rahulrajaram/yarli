//! yarli-observability: Tracing, metrics, and audit sink.

pub mod audit;
pub mod metrics;
pub mod run_analyzer;
pub mod tracing_init;

pub use audit::{
    AuditCategory, AuditEntry, AuditError, AuditSink, InMemoryAuditSink, JsonlAuditSink,
};

pub use metrics::{encode_metrics, Registry, YarliMetrics};

pub use run_analyzer::{
    analyze_run, pattern_names, FailurePattern, RetryScope, RunAnalysis,
    TaskOutcome as AnalyzerTaskOutcome,
};

pub use tracing_init::{init_tracing, TracingConfig, TracingInitError};
