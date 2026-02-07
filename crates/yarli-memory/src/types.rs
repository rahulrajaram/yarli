//! Memory domain types.
//!
//! Section 14.2: Memory classes — Working (transient run-local facts),
//! Semantic (durable heuristics and lessons), Episodic (run timelines
//! and incident narratives).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use yarli_core::domain::{RunId, TaskId};

/// Memory class (Section 14.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryClass {
    /// Transient run-local facts. Scoped to a single run.
    Working,
    /// Durable heuristics and lessons. Persists across runs.
    Semantic,
    /// Run timelines and incident narratives.
    Episodic,
}

/// Scope identifier for memory operations (Section 14.1).
///
/// Convention:
/// - `run/{run_id}` for run-scoped memories
/// - `task/{task_id}` for task-scoped memories
/// - `agent/{agent_name}` for agent-scoped memories
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeId(pub String);

impl ScopeId {
    /// Create a run-scoped ID: `run/{run_id}`.
    pub fn for_run(run_id: RunId) -> Self {
        Self(format!("run/{run_id}"))
    }

    /// Create a task-scoped ID: `task/{task_id}`.
    pub fn for_task(task_id: TaskId) -> Self {
        Self(format!("task/{task_id}"))
    }

    /// Create an agent-scoped ID: `agent/{name}`.
    pub fn for_agent(name: &str) -> Self {
        Self(format!("agent/{name}"))
    }

    /// Get the raw scope string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A memory record stored in the adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecord {
    /// Unique memory ID (assigned by backend).
    pub memory_id: String,
    /// Scope this memory belongs to.
    pub scope_id: ScopeId,
    /// Memory class.
    pub memory_class: MemoryClass,
    /// Content of the memory (text).
    pub content: String,
    /// Key-value metadata.
    pub metadata: HashMap<String, String>,
    /// Relevance score (from query, 0.0 if not queried).
    pub relevance_score: f64,
    /// Number of times this memory has been retrieved.
    pub retrieval_count: u64,
    /// When the memory was created.
    pub created_at: DateTime<Utc>,
    /// When the memory was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Parameters for inserting a memory.
#[derive(Debug, Clone)]
pub struct InsertMemory {
    /// Scope to store in.
    pub scope_id: ScopeId,
    /// Memory class.
    pub memory_class: MemoryClass,
    /// Text content.
    pub content: String,
    /// Key-value metadata.
    pub metadata: HashMap<String, String>,
}

impl InsertMemory {
    /// Create a new insert request.
    pub fn new(scope_id: ScopeId, memory_class: MemoryClass, content: impl Into<String>) -> Self {
        Self {
            scope_id,
            memory_class,
            content: content.into(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the insert.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Parameters for querying memories.
#[derive(Debug, Clone)]
pub struct MemoryQuery {
    /// Scope to search in.
    pub scope_id: ScopeId,
    /// Text query (BM25 full-text search).
    pub query_text: String,
    /// Maximum results to return.
    pub limit: u32,
    /// Filter by memory class (None = all classes).
    pub memory_class: Option<MemoryClass>,
}

impl MemoryQuery {
    /// Create a query for a scope.
    pub fn new(scope_id: ScopeId, query_text: impl Into<String>) -> Self {
        Self {
            scope_id,
            query_text: query_text.into(),
            limit: 100,
            memory_class: None,
        }
    }

    /// Limit the number of results.
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Filter by memory class.
    pub fn with_class(mut self, class: MemoryClass) -> Self {
        self.memory_class = Some(class);
        self
    }
}

/// Parameters for linking two memories.
#[derive(Debug, Clone)]
pub struct LinkMemories {
    /// Source memory ID.
    pub from_memory_id: String,
    /// Target memory ID.
    pub to_memory_id: String,
    /// Relationship type (cause_effect, relates_to, etc.).
    pub relationship: RelationshipKind,
    /// Optional metadata on the link.
    pub metadata: HashMap<String, String>,
}

/// Relationship types between memories (Section 14.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipKind {
    /// A caused B (incident → fix).
    CauseEffect,
    /// A relates to B.
    RelatesTo,
    /// A supersedes B.
    Supersedes,
    /// A depends on B.
    DependsOn,
}

/// Secret patterns used for redaction checks (Section 14.4).
const SECRET_PATTERNS: &[&str] = &[
    "-----BEGIN",
    "PRIVATE KEY",
    "password=",
    "secret=",
    "token=",
    "api_key=",
    "apikey=",
    "AWS_SECRET",
    "AKIA",       // AWS access key prefix
    "ghp_",       // GitHub personal access token
    "ghs_",       // GitHub server-to-server token
    "sk-",        // OpenAI / Stripe key prefix
    "Bearer ",
];

/// Check if content may contain secrets (Section 14.4).
///
/// Returns `true` if the content likely contains secret material.
pub fn content_may_contain_secrets(content: &str) -> bool {
    let lower = content.to_lowercase();
    SECRET_PATTERNS.iter().any(|pat| lower.contains(&pat.to_lowercase()))
}
