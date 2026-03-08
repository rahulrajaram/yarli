//! MemoryAdapter trait — abstraction over memory backends.
//!
//! Section 14.3 requires:
//! - `memory.query` before planning/repair loops
//! - `memory.store` for outcomes, blockers, recovery incidents
//! - `memory.link` for cause/effect relationships
//!
//! Section 14.4 safety:
//! - No secrets/tokens stored in plaintext
//! - Memory writes require redaction pass
//! - Large outputs stored as artifacts; memory stores summaries + references
//! - Memory operation failures produce explicit warnings/events

use crate::yarli_memory::error::MemoryError;
use crate::yarli_memory::types::{InsertMemory, LinkMemories, MemoryQuery, MemoryRecord, ScopeId};

/// Async memory adapter trait.
///
/// Implementations must be thread-safe. All operations are fallible and
/// produce explicit errors (Section 14.4 — no silent failures).
pub trait MemoryAdapter: Send + Sync {
    /// Store a new memory record.
    ///
    /// Content is checked for secrets before storage (Section 14.4).
    fn store(
        &self,
        project: &str,
        request: InsertMemory,
    ) -> impl std::future::Future<Output = Result<MemoryRecord, MemoryError>> + Send;

    /// Query memories by text search within a scope.
    fn query(
        &self,
        project: &str,
        query: MemoryQuery,
    ) -> impl std::future::Future<Output = Result<Vec<MemoryRecord>, MemoryError>> + Send;

    /// Retrieve a single memory by ID.
    fn get(
        &self,
        project: &str,
        memory_id: &str,
    ) -> impl std::future::Future<Output = Result<MemoryRecord, MemoryError>> + Send;

    /// Delete a memory.
    fn delete(
        &self,
        project: &str,
        memory_id: &str,
    ) -> impl std::future::Future<Output = Result<(), MemoryError>> + Send;

    /// Link two memories with a relationship (Section 14.3).
    fn link(
        &self,
        project: &str,
        link: LinkMemories,
    ) -> impl std::future::Future<Output = Result<(), MemoryError>> + Send;

    /// Unlink two memories.
    fn unlink(
        &self,
        project: &str,
        from_memory_id: &str,
        to_memory_id: &str,
    ) -> impl std::future::Future<Output = Result<(), MemoryError>> + Send;

    /// Create a scope for organizing memories.
    fn create_scope(
        &self,
        project: &str,
        scope_id: &ScopeId,
        parent: Option<&ScopeId>,
    ) -> impl std::future::Future<Output = Result<(), MemoryError>> + Send;

    /// Close a scope (make read-only).
    fn close_scope(
        &self,
        project: &str,
        scope_id: &ScopeId,
    ) -> impl std::future::Future<Output = Result<(), MemoryError>> + Send;

    /// Check if the backend is healthy.
    fn health_check(&self) -> impl std::future::Future<Output = Result<bool, MemoryError>> + Send;
}
