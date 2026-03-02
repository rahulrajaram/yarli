//! yarli-memory: Memory backend adapter.
//!
//! Provides a [`MemoryAdapter`] trait for memory operations (query, store, link)
//! and an in-memory implementation for testing. The Memory-backend gRPC adapter will be
//! available behind the `memory-backend` feature flag.
//!
//! # Memory Classes (Section 14.2)
//!
//! - **Working**: Transient run-local facts, scoped to a single run.
//! - **Semantic**: Durable heuristics and lessons, persists across runs.
//! - **Episodic**: Run timelines and incident narratives.
//!
//! # Safety (Section 14.4)
//!
//! - No secrets/tokens stored in plaintext — redaction check on all writes.
//! - Memory operation failures produce explicit errors, never silently pass.
//! - Large outputs stored as artifacts; memory stores summaries + references.

pub mod adapter;
pub mod error;
pub mod in_memory;
pub mod memory_cli;
pub mod types;

pub use adapter::MemoryAdapter;
pub use error::MemoryError;
pub use in_memory::InMemoryAdapter;
pub use memory_cli::MemoryCliAdapter;
pub use types::{
    content_may_contain_secrets, InsertMemory, LinkMemories, MemoryClass, MemoryQuery,
    MemoryRecord, RelationshipKind, ScopeId,
};
