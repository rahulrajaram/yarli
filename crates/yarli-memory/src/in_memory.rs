//! In-memory adapter for testing.
//!
//! No gRPC dependency; stores records in a thread-safe HashMap.

use std::collections::HashMap;
use std::sync::RwLock;

use chrono::Utc;
use uuid::Uuid;

use crate::yarli_memory::adapter::MemoryAdapter;
use crate::yarli_memory::error::MemoryError;
use crate::yarli_memory::types::{
    content_may_contain_secrets, InsertMemory, LinkMemories, MemoryQuery, MemoryRecord,
    RelationshipKind, ScopeId,
};

/// Scope state tracking.
#[derive(Debug, Clone)]
struct ScopeState {
    #[allow(dead_code)]
    parent: Option<ScopeId>,
    closed: bool,
}

/// A link between two memories.
#[derive(Debug, Clone)]
struct MemoryLink {
    from_memory_id: String,
    to_memory_id: String,
    #[allow(dead_code)]
    relationship: RelationshipKind,
    #[allow(dead_code)]
    metadata: HashMap<String, String>,
}

/// In-memory adapter state.
struct InMemoryState {
    /// project -> scope_id -> ScopeState
    scopes: HashMap<String, HashMap<String, ScopeState>>,
    /// project -> memory_id -> MemoryRecord
    memories: HashMap<String, HashMap<String, MemoryRecord>>,
    /// project -> links
    links: HashMap<String, Vec<MemoryLink>>,
}

/// In-memory implementation of [`MemoryAdapter`].
///
/// Suitable for testing and development. Not persistent.
pub struct InMemoryAdapter {
    state: RwLock<InMemoryState>,
}

impl InMemoryAdapter {
    /// Create a new empty adapter.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(InMemoryState {
                scopes: HashMap::new(),
                memories: HashMap::new(),
                links: HashMap::new(),
            }),
        }
    }

    /// Count all memories across all projects.
    pub fn memory_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.memories.values().map(|m| m.len()).sum()
    }

    /// Count links for a project.
    pub fn link_count(&self, project: &str) -> usize {
        let state = self.state.read().unwrap();
        state.links.get(project).map(|l| l.len()).unwrap_or(0)
    }
}

impl Default for InMemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryAdapter for InMemoryAdapter {
    async fn store(
        &self,
        project: &str,
        request: InsertMemory,
    ) -> Result<MemoryRecord, MemoryError> {
        // Section 14.4: redaction check
        if content_may_contain_secrets(&request.content) {
            return Err(MemoryError::RedactionRequired);
        }

        let mut state = self.state.write().unwrap();

        // Check scope exists and is not closed
        if let Some(scopes) = state.scopes.get(project) {
            if let Some(scope) = scopes.get(request.scope_id.as_str()) {
                if scope.closed {
                    return Err(MemoryError::ScopeClosed(
                        request.scope_id.as_str().to_string(),
                    ));
                }
            }
        }

        let now = Utc::now();
        let memory_id = Uuid::now_v7().to_string();

        let record = MemoryRecord {
            memory_id: memory_id.clone(),
            scope_id: request.scope_id,
            memory_class: request.memory_class,
            content: request.content,
            metadata: request.metadata,
            relevance_score: 0.0,
            retrieval_count: 0,
            created_at: now,
            updated_at: now,
        };

        state
            .memories
            .entry(project.to_string())
            .or_default()
            .insert(memory_id, record.clone());

        Ok(record)
    }

    async fn query(
        &self,
        project: &str,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryRecord>, MemoryError> {
        let state = self.state.read().unwrap();

        let memories = match state.memories.get(project) {
            Some(m) => m,
            None => return Ok(vec![]),
        };

        let query_lower = query.query_text.to_lowercase();
        let query_terms: Vec<&str> = query_lower.split_whitespace().collect();

        let mut results: Vec<MemoryRecord> = memories
            .values()
            .filter(|m| {
                // Filter by scope
                if m.scope_id != query.scope_id {
                    return false;
                }
                // Filter by class
                if let Some(class) = query.memory_class {
                    if m.memory_class != class {
                        return false;
                    }
                }
                // Simple text matching (BM25 approximation for in-memory)
                if query_terms.is_empty() {
                    return true;
                }
                let content_lower = m.content.to_lowercase();
                query_terms.iter().any(|term| content_lower.contains(term))
            })
            .cloned()
            .enumerate()
            .map(|(i, mut m)| {
                // Simple relevance scoring: count matching terms
                let content_lower = m.content.to_lowercase();
                let match_count = query_terms
                    .iter()
                    .filter(|t| content_lower.contains(**t))
                    .count();
                m.relevance_score = match_count as f64 / query_terms.len().max(1) as f64;
                m.retrieval_count += 1;
                let _ = i; // suppress unused
                m
            })
            .collect();

        // Sort by relevance descending
        results.sort_by(|a, b| {
            b.relevance_score
                .partial_cmp(&a.relevance_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Apply limit
        results.truncate(query.limit as usize);

        Ok(results)
    }

    async fn get(&self, project: &str, memory_id: &str) -> Result<MemoryRecord, MemoryError> {
        let state = self.state.read().unwrap();

        state
            .memories
            .get(project)
            .and_then(|m| m.get(memory_id))
            .cloned()
            .ok_or_else(|| MemoryError::NotFound(memory_id.to_string()))
    }

    async fn delete(&self, project: &str, memory_id: &str) -> Result<(), MemoryError> {
        let mut state = self.state.write().unwrap();

        let removed = state
            .memories
            .get_mut(project)
            .and_then(|m| m.remove(memory_id));

        if removed.is_some() {
            // Also remove any links involving this memory
            if let Some(links) = state.links.get_mut(project) {
                links.retain(|l| l.from_memory_id != memory_id && l.to_memory_id != memory_id);
            }
            Ok(())
        } else {
            Err(MemoryError::NotFound(memory_id.to_string()))
        }
    }

    async fn link(&self, project: &str, link: LinkMemories) -> Result<(), MemoryError> {
        let mut state = self.state.write().unwrap();

        // Verify both memories exist
        let memories = state
            .memories
            .get(project)
            .ok_or_else(|| MemoryError::NotFound(link.from_memory_id.clone()))?;

        if !memories.contains_key(&link.from_memory_id) {
            return Err(MemoryError::NotFound(link.from_memory_id));
        }
        if !memories.contains_key(&link.to_memory_id) {
            return Err(MemoryError::NotFound(link.to_memory_id));
        }

        state
            .links
            .entry(project.to_string())
            .or_default()
            .push(MemoryLink {
                from_memory_id: link.from_memory_id,
                to_memory_id: link.to_memory_id,
                relationship: link.relationship,
                metadata: link.metadata,
            });

        Ok(())
    }

    async fn unlink(
        &self,
        project: &str,
        from_memory_id: &str,
        to_memory_id: &str,
    ) -> Result<(), MemoryError> {
        let mut state = self.state.write().unwrap();

        if let Some(links) = state.links.get_mut(project) {
            let before = links.len();
            links.retain(|l| {
                !(l.from_memory_id == from_memory_id && l.to_memory_id == to_memory_id)
            });
            if links.len() == before {
                return Err(MemoryError::NotFound(format!(
                    "link {from_memory_id} -> {to_memory_id}"
                )));
            }
        } else {
            return Err(MemoryError::NotFound(format!(
                "link {from_memory_id} -> {to_memory_id}"
            )));
        }

        Ok(())
    }

    async fn create_scope(
        &self,
        project: &str,
        scope_id: &ScopeId,
        parent: Option<&ScopeId>,
    ) -> Result<(), MemoryError> {
        let mut state = self.state.write().unwrap();

        state.scopes.entry(project.to_string()).or_default().insert(
            scope_id.as_str().to_string(),
            ScopeState {
                parent: parent.cloned(),
                closed: false,
            },
        );

        Ok(())
    }

    async fn close_scope(&self, project: &str, scope_id: &ScopeId) -> Result<(), MemoryError> {
        let mut state = self.state.write().unwrap();

        let scope = state
            .scopes
            .get_mut(project)
            .and_then(|s| s.get_mut(scope_id.as_str()))
            .ok_or_else(|| MemoryError::ScopeNotFound {
                project: project.to_string(),
                scope: scope_id.as_str().to_string(),
            })?;

        scope.closed = true;
        Ok(())
    }

    async fn health_check(&self) -> Result<bool, MemoryError> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yarli_memory::types::MemoryClass;

    fn test_scope() -> ScopeId {
        ScopeId::for_run(uuid::Uuid::nil())
    }

    #[tokio::test]
    async fn store_and_get() {
        let adapter = InMemoryAdapter::new();
        let req = InsertMemory::new(test_scope(), MemoryClass::Working, "test content");
        let record = adapter.store("proj", req).await.unwrap();
        assert_eq!(record.content, "test content");
        assert_eq!(record.memory_class, MemoryClass::Working);

        let fetched = adapter.get("proj", &record.memory_id).await.unwrap();
        assert_eq!(fetched.memory_id, record.memory_id);
    }

    #[tokio::test]
    async fn store_rejects_secrets() {
        let adapter = InMemoryAdapter::new();
        let req = InsertMemory::new(test_scope(), MemoryClass::Working, "my password=hunter2");
        let err = adapter.store("proj", req).await.unwrap_err();
        assert!(matches!(err, MemoryError::RedactionRequired));
    }

    #[tokio::test]
    async fn store_rejects_private_key() {
        let adapter = InMemoryAdapter::new();
        let req = InsertMemory::new(
            test_scope(),
            MemoryClass::Working,
            "-----BEGIN RSA PRIVATE KEY-----\nMIIE...",
        );
        let err = adapter.store("proj", req).await.unwrap_err();
        assert!(matches!(err, MemoryError::RedactionRequired));
    }

    #[tokio::test]
    async fn store_rejects_api_key() {
        let adapter = InMemoryAdapter::new();
        let req = InsertMemory::new(
            test_scope(),
            MemoryClass::Working,
            "config: api_key=sk-proj-abc123",
        );
        let err = adapter.store("proj", req).await.unwrap_err();
        assert!(matches!(err, MemoryError::RedactionRequired));
    }

    #[tokio::test]
    async fn query_returns_matching() {
        let adapter = InMemoryAdapter::new();
        let scope = test_scope();

        adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Semantic, "cargo build failed"),
            )
            .await
            .unwrap();
        adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Semantic, "tests passed ok"),
            )
            .await
            .unwrap();

        let results = adapter
            .query("proj", MemoryQuery::new(scope, "cargo"))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].content.contains("cargo"));
    }

    #[tokio::test]
    async fn query_filters_by_class() {
        let adapter = InMemoryAdapter::new();
        let scope = test_scope();

        adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Working, "working note"),
            )
            .await
            .unwrap();
        adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Semantic, "semantic lesson"),
            )
            .await
            .unwrap();

        let results = adapter
            .query(
                "proj",
                MemoryQuery::new(scope, "note lesson").with_class(MemoryClass::Working),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].content.contains("working"));
    }

    #[tokio::test]
    async fn query_respects_limit() {
        let adapter = InMemoryAdapter::new();
        let scope = test_scope();

        for i in 0..10 {
            adapter
                .store(
                    "proj",
                    InsertMemory::new(scope.clone(), MemoryClass::Working, format!("item {i}")),
                )
                .await
                .unwrap();
        }

        let results = adapter
            .query("proj", MemoryQuery::new(scope, "item").with_limit(3))
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn query_empty_project() {
        let adapter = InMemoryAdapter::new();
        let results = adapter
            .query("nonexistent", MemoryQuery::new(test_scope(), "anything"))
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn delete_memory() {
        let adapter = InMemoryAdapter::new();
        let record = adapter
            .store(
                "proj",
                InsertMemory::new(test_scope(), MemoryClass::Working, "to delete"),
            )
            .await
            .unwrap();

        adapter.delete("proj", &record.memory_id).await.unwrap();
        let err = adapter.get("proj", &record.memory_id).await.unwrap_err();
        assert!(matches!(err, MemoryError::NotFound(_)));
    }

    #[tokio::test]
    async fn delete_not_found() {
        let adapter = InMemoryAdapter::new();
        let err = adapter.delete("proj", "nonexistent").await.unwrap_err();
        assert!(matches!(err, MemoryError::NotFound(_)));
    }

    #[tokio::test]
    async fn link_and_unlink() {
        let adapter = InMemoryAdapter::new();
        let scope = test_scope();

        let m1 = adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Episodic, "incident occurred"),
            )
            .await
            .unwrap();
        let m2 = adapter
            .store(
                "proj",
                InsertMemory::new(scope, MemoryClass::Semantic, "fix applied"),
            )
            .await
            .unwrap();

        adapter
            .link(
                "proj",
                LinkMemories {
                    from_memory_id: m1.memory_id.clone(),
                    to_memory_id: m2.memory_id.clone(),
                    relationship: RelationshipKind::CauseEffect,
                    metadata: HashMap::new(),
                },
            )
            .await
            .unwrap();

        assert_eq!(adapter.link_count("proj"), 1);

        adapter
            .unlink("proj", &m1.memory_id, &m2.memory_id)
            .await
            .unwrap();
        assert_eq!(adapter.link_count("proj"), 0);
    }

    #[tokio::test]
    async fn link_not_found() {
        let adapter = InMemoryAdapter::new();
        let err = adapter
            .link(
                "proj",
                LinkMemories {
                    from_memory_id: "no-such".to_string(),
                    to_memory_id: "no-such-2".to_string(),
                    relationship: RelationshipKind::RelatesTo,
                    metadata: HashMap::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MemoryError::NotFound(_)));
    }

    #[tokio::test]
    async fn scope_lifecycle() {
        let adapter = InMemoryAdapter::new();
        let scope = ScopeId::for_run(uuid::Uuid::nil());

        adapter.create_scope("proj", &scope, None).await.unwrap();

        // Can store into open scope
        adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Working, "before close"),
            )
            .await
            .unwrap();

        // Close the scope
        adapter.close_scope("proj", &scope).await.unwrap();

        // Writes to closed scope are rejected
        let err = adapter
            .store(
                "proj",
                InsertMemory::new(scope, MemoryClass::Working, "after close"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MemoryError::ScopeClosed(_)));
    }

    #[tokio::test]
    async fn close_nonexistent_scope() {
        let adapter = InMemoryAdapter::new();
        let scope = ScopeId::for_agent("ghost");
        let err = adapter.close_scope("proj", &scope).await.unwrap_err();
        assert!(matches!(err, MemoryError::ScopeNotFound { .. }));
    }

    #[tokio::test]
    async fn health_check_ok() {
        let adapter = InMemoryAdapter::new();
        assert!(adapter.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn scope_id_constructors() {
        let run_scope = ScopeId::for_run(uuid::Uuid::nil());
        assert!(run_scope.as_str().starts_with("run/"));

        let task_scope = ScopeId::for_task(uuid::Uuid::nil());
        assert!(task_scope.as_str().starts_with("task/"));

        let agent_scope = ScopeId::for_agent("my-agent");
        assert_eq!(agent_scope.as_str(), "agent/my-agent");
    }

    #[tokio::test]
    async fn memory_class_roundtrip() {
        let json = serde_json::to_string(&MemoryClass::Semantic).unwrap();
        assert_eq!(json, "\"semantic\"");
        let decoded: MemoryClass = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, MemoryClass::Semantic);
    }

    #[tokio::test]
    async fn insert_with_metadata() {
        let adapter = InMemoryAdapter::new();
        let req = InsertMemory::new(test_scope(), MemoryClass::Episodic, "incident timeline")
            .with_metadata("run_id", "abc123")
            .with_metadata("severity", "high");
        let record = adapter.store("proj", req).await.unwrap();
        assert_eq!(record.metadata.get("run_id").unwrap(), "abc123");
        assert_eq!(record.metadata.get("severity").unwrap(), "high");
    }

    #[tokio::test]
    async fn delete_removes_associated_links() {
        let adapter = InMemoryAdapter::new();
        let scope = test_scope();

        let m1 = adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Working, "first"),
            )
            .await
            .unwrap();
        let m2 = adapter
            .store(
                "proj",
                InsertMemory::new(scope, MemoryClass::Working, "second"),
            )
            .await
            .unwrap();

        adapter
            .link(
                "proj",
                LinkMemories {
                    from_memory_id: m1.memory_id.clone(),
                    to_memory_id: m2.memory_id.clone(),
                    relationship: RelationshipKind::DependsOn,
                    metadata: HashMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(adapter.link_count("proj"), 1);

        // Deleting m1 should also remove the link
        adapter.delete("proj", &m1.memory_id).await.unwrap();
        assert_eq!(adapter.link_count("proj"), 0);
    }

    #[tokio::test]
    async fn query_relevance_scoring() {
        let adapter = InMemoryAdapter::new();
        let scope = test_scope();

        adapter
            .store(
                "proj",
                InsertMemory::new(
                    scope.clone(),
                    MemoryClass::Semantic,
                    "rust cargo build test",
                ),
            )
            .await
            .unwrap();
        adapter
            .store(
                "proj",
                InsertMemory::new(scope.clone(), MemoryClass::Semantic, "just cargo"),
            )
            .await
            .unwrap();

        let results = adapter
            .query("proj", MemoryQuery::new(scope, "cargo build"))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        // First result should have higher relevance (matches more terms)
        assert!(results[0].relevance_score >= results[1].relevance_score);
    }

    #[tokio::test]
    async fn memory_count() {
        let adapter = InMemoryAdapter::new();
        assert_eq!(adapter.memory_count(), 0);

        adapter
            .store(
                "proj",
                InsertMemory::new(test_scope(), MemoryClass::Working, "one"),
            )
            .await
            .unwrap();
        assert_eq!(adapter.memory_count(), 1);
    }

    #[tokio::test]
    async fn scope_with_parent() {
        let adapter = InMemoryAdapter::new();
        let parent = ScopeId::for_run(uuid::Uuid::nil());
        let child = ScopeId::for_task(uuid::Uuid::nil());

        adapter.create_scope("proj", &parent, None).await.unwrap();
        adapter
            .create_scope("proj", &child, Some(&parent))
            .await
            .unwrap();

        // Both scopes accept writes
        adapter
            .store(
                "proj",
                InsertMemory::new(parent, MemoryClass::Working, "parent note"),
            )
            .await
            .unwrap();
        adapter
            .store(
                "proj",
                InsertMemory::new(child, MemoryClass::Working, "child note"),
            )
            .await
            .unwrap();

        assert_eq!(adapter.memory_count(), 2);
    }

    #[tokio::test]
    async fn content_secret_detection() {
        assert!(content_may_contain_secrets("password=hunter2"));
        assert!(content_may_contain_secrets(
            "ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        ));
        assert!(content_may_contain_secrets(
            "-----BEGIN RSA PRIVATE KEY-----"
        ));
        assert!(content_may_contain_secrets("token=abc123"));
        assert!(content_may_contain_secrets(
            "export AWS_SECRET_ACCESS_KEY=xxx"
        ));
        assert!(content_may_contain_secrets(
            "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ));
        assert!(content_may_contain_secrets("sk-proj-abc123"));

        assert!(!content_may_contain_secrets("normal text about code"));
        assert!(!content_may_contain_secrets("the password field was empty"));
    }

    #[tokio::test]
    async fn relationship_kind_roundtrip() {
        let json = serde_json::to_string(&RelationshipKind::CauseEffect).unwrap();
        assert_eq!(json, "\"cause_effect\"");
        let decoded: RelationshipKind = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, RelationshipKind::CauseEffect);
    }

    #[tokio::test]
    async fn query_different_scopes_isolated() {
        let adapter = InMemoryAdapter::new();
        let scope1 = ScopeId::for_run(uuid::Uuid::from_u128(1));
        let scope2 = ScopeId::for_run(uuid::Uuid::from_u128(2));

        adapter
            .store(
                "proj",
                InsertMemory::new(scope1.clone(), MemoryClass::Working, "scope1 data"),
            )
            .await
            .unwrap();
        adapter
            .store(
                "proj",
                InsertMemory::new(scope2.clone(), MemoryClass::Working, "scope2 data"),
            )
            .await
            .unwrap();

        let r1 = adapter
            .query("proj", MemoryQuery::new(scope1, "data"))
            .await
            .unwrap();
        assert_eq!(r1.len(), 1);
        assert!(r1[0].content.contains("scope1"));

        let r2 = adapter
            .query("proj", MemoryQuery::new(scope2, "data"))
            .await
            .unwrap();
        assert_eq!(r2.len(), 1);
        assert!(r2[0].content.contains("scope2"));
    }
}
