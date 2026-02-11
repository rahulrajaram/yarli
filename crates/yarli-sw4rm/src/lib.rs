//! yarli-sw4rm: sw4rm agent integration for yarli.
//!
//! Makes yarli an orchestrator agent in the sw4rm protocol. Receives objectives
//! from the sw4rm scheduler, dispatches implementation work to LLM agents,
//! verifies results via the yarli Scheduler, and iterates until success or
//! max retries.

pub mod agent;
pub mod bridge;
pub mod config;
pub mod messages;
pub mod orchestrator;

/// Mock implementations for testing. Only available with `test-support` feature
/// or in test builds.
#[cfg(any(test, feature = "test-support"))]
pub mod mock;

pub use agent::YarliAgent;
pub use bridge::ShutdownBridge;
pub use config::Sw4rmConfig;
pub use messages::{
    ImplementationRequest, ImplementationResponse, OrchestrationReport, VerificationFailure,
};
pub use orchestrator::{ObjectiveParams, OrchestratorLoop, OrchestratorResult, RouterSender};
