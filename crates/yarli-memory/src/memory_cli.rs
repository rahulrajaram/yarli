//! Memory backend CLI-backed memory adapter.
//!
//! This adapter shells out to the memory backend CLI and uses the local project's
//! `.backend.yml` / `.backend-data` (resolved relative to `project_dir`).
//!
//! It is intentionally conservative:
//! - Uses `memory-backend query ... --output json` for query results.
//! - Uses `memory-backend memory insert ...` for inserts.
//! - Emits explicit errors on unsupported operations.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::process::Command;
use uuid::Uuid;

use crate::adapter::MemoryAdapter;
use crate::error::MemoryError;
use crate::types::{
    content_may_contain_secrets, InsertMemory, LinkMemories, MemoryClass, MemoryQuery,
    MemoryRecord, RelationshipKind, ScopeId,
};

#[derive(Debug, Clone)]
pub struct MemoryCliAdapter {
    command: String,
    project_dir: PathBuf,
}

impl MemoryCliAdapter {
    pub fn new(command: impl Into<String>, project_dir: impl Into<PathBuf>) -> Self {
        Self {
            command: command.into(),
            project_dir: project_dir.into(),
        }
    }

    pub fn project_dir(&self) -> &Path {
        &self.project_dir
    }

    async fn run(&self, args: &[String]) -> Result<String, MemoryError> {
        let child_fut = Command::new(&self.command)
            .args(args)
            .current_dir(&self.project_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output();

        let output = tokio::time::timeout(std::time::Duration::from_secs(10), child_fut)
            .await
            .map_err(|_| {
                MemoryError::ConnectionFailed("memory backend CLI timed out after 10s".to_string())
            })?
            .map_err(|e| MemoryError::ConnectionFailed(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let msg = if stderr.is_empty() { stdout } else { stderr };
            return Err(MemoryError::Backend(msg));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn class_to_backend(class: MemoryClass) -> &'static str {
        match class {
            MemoryClass::Working => "working",
            MemoryClass::Semantic => "semantic",
            MemoryClass::Episodic => "episodic",
        }
    }

    fn relation_to_backend(kind: RelationshipKind) -> &'static str {
        match kind {
            RelationshipKind::RelatesTo => "relates-to",
            RelationshipKind::CauseEffect => "caused-by",
            RelationshipKind::Supersedes => "supersedes",
            RelationshipKind::DependsOn => "references",
        }
    }

    fn parse_datetime(value: &Value) -> Option<DateTime<Utc>> {
        let s = value.as_str()?;
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn map_query_item(scope_fallback: &ScopeId, item: &Value) -> Option<MemoryRecord> {
        let obj = item.as_object()?;

        let memory_id = obj
            .get("memory_id")
            .or_else(|| obj.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if memory_id.trim().is_empty() {
            return None;
        }

        let content = obj
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let scope_id = obj
            .get("scope_id")
            .and_then(|v| v.as_str())
            .map(|s| ScopeId(s.to_string()))
            .unwrap_or_else(|| scope_fallback.clone());

        let memory_class = obj
            .get("memory_type")
            .or_else(|| obj.get("memory_class"))
            .and_then(|v| v.as_str())
            .and_then(|raw| match raw {
                "working" => Some(MemoryClass::Working),
                "semantic" => Some(MemoryClass::Semantic),
                "episodic" => Some(MemoryClass::Episodic),
                _ => None,
            })
            .unwrap_or(MemoryClass::Semantic);

        let relevance_score = obj
            .get("relevance_score")
            .or_else(|| obj.get("score"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let retrieval_count = obj
            .get("retrieval_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let mut metadata = HashMap::new();
        if let Some(meta) = obj.get("metadata").and_then(|v| v.as_object()) {
            for (k, v) in meta {
                if let Some(s) = v.as_str() {
                    metadata.insert(k.clone(), s.to_string());
                } else {
                    metadata.insert(k.clone(), v.to_string());
                }
            }
        }

        let created_at = obj
            .get("created_at")
            .and_then(Self::parse_datetime)
            .unwrap_or_else(Utc::now);
        let updated_at = obj
            .get("updated_at")
            .and_then(Self::parse_datetime)
            .unwrap_or(created_at);

        Some(MemoryRecord {
            memory_id,
            scope_id,
            memory_class,
            content,
            metadata,
            relevance_score,
            retrieval_count,
            created_at,
            updated_at,
        })
    }

    fn parse_insert_memory_id(stdout: &str) -> Option<String> {
        if stdout.trim().is_empty() {
            return None;
        }
        if let Ok(value) = serde_json::from_str::<Value>(stdout) {
            if let Some(id) = value
                .get("memory_id")
                .or_else(|| value.get("id"))
                .and_then(|v| v.as_str())
            {
                return Some(id.to_string());
            }
        }

        for token in stdout.split_whitespace() {
            if Uuid::parse_str(token).is_ok() {
                return Some(token.to_string());
            }
        }
        None
    }
}

impl MemoryAdapter for MemoryCliAdapter {
    async fn store(
        &self,
        _project: &str,
        request: InsertMemory,
    ) -> Result<MemoryRecord, MemoryError> {
        if content_may_contain_secrets(&request.content) {
            return Err(MemoryError::RedactionRequired);
        }

        let mut args = vec![
            "memory".to_string(),
            "insert".to_string(),
            request.scope_id.as_str().to_string(),
            request.content.clone(),
            "--memory-type".to_string(),
            Self::class_to_backend(request.memory_class).to_string(),
        ];

        for (k, v) in &request.metadata {
            args.push("--meta".to_string());
            args.push(format!("{k}={v}"));
        }

        let stdout = self.run(&args).await?;
        let memory_id = Self::parse_insert_memory_id(&stdout).unwrap_or_else(|| "unknown".into());
        let now = Utc::now();

        Ok(MemoryRecord {
            memory_id,
            scope_id: request.scope_id,
            memory_class: request.memory_class,
            content: request.content,
            metadata: request.metadata,
            relevance_score: 0.0,
            retrieval_count: 0,
            created_at: now,
            updated_at: now,
        })
    }

    async fn query(
        &self,
        _project: &str,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryRecord>, MemoryError> {
        let args = vec![
            "query".to_string(),
            query.scope_id.as_str().to_string(),
            query.query_text.clone(),
            "--output".to_string(),
            "json".to_string(),
            "--limit".to_string(),
            query.limit.to_string(),
        ];

        let stdout = self.run(&args).await?;
        let value: Value =
            serde_json::from_str(&stdout).map_err(|e| MemoryError::Serialization(e.to_string()))?;

        let items = match value {
            Value::Array(items) => items,
            Value::Object(obj) => obj
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            _ => Vec::new(),
        };

        let mut out = Vec::new();
        for item in &items {
            if let Some(record) = Self::map_query_item(&query.scope_id, item) {
                if let Some(class) = query.memory_class {
                    if record.memory_class != class {
                        continue;
                    }
                }
                out.push(record);
            }
        }
        Ok(out)
    }

    async fn get(&self, project: &str, memory_id: &str) -> Result<MemoryRecord, MemoryError> {
        let scope = ScopeId(format!("project/{project}"));
        let args = vec![
            "memory".to_string(),
            "get".to_string(),
            scope.as_str().to_string(),
            memory_id.to_string(),
        ];
        let stdout = self.run(&args).await?;
        let now = Utc::now();
        Ok(MemoryRecord {
            memory_id: memory_id.to_string(),
            scope_id: scope,
            memory_class: MemoryClass::Semantic,
            content: stdout,
            metadata: HashMap::new(),
            relevance_score: 0.0,
            retrieval_count: 0,
            created_at: now,
            updated_at: now,
        })
    }

    async fn delete(&self, _project: &str, memory_id: &str) -> Result<(), MemoryError> {
        let args = vec![
            "memory".to_string(),
            "delete".to_string(),
            "--yes".to_string(),
            memory_id.to_string(),
        ];
        let _ = self.run(&args).await?;
        Ok(())
    }

    async fn link(&self, _project: &str, link: LinkMemories) -> Result<(), MemoryError> {
        let args = vec![
            "memory".to_string(),
            "link".to_string(),
            "--relation".to_string(),
            Self::relation_to_backend(link.relationship).to_string(),
            link.from_memory_id,
            link.to_memory_id,
        ];
        let _ = self.run(&args).await?;
        Ok(())
    }

    async fn unlink(
        &self,
        _project: &str,
        _from_memory_id: &str,
        _to_memory_id: &str,
    ) -> Result<(), MemoryError> {
        Err(MemoryError::Backend(
            "memory backend CLI does not support unlink".to_string(),
        ))
    }

    async fn create_scope(
        &self,
        _project: &str,
        _scope_id: &ScopeId,
        _parent: Option<&ScopeId>,
    ) -> Result<(), MemoryError> {
        // Memory backend scopes are implicit; no explicit creation step.
        Ok(())
    }

    async fn close_scope(&self, _project: &str, _scope_id: &ScopeId) -> Result<(), MemoryError> {
        // Memory backend scopes are implicit; no explicit close step.
        Ok(())
    }

    async fn health_check(&self) -> Result<bool, MemoryError> {
        let args = vec!["doctor".to_string()];
        match self.run(&args).await {
            Ok(_) => Ok(true),
            Err(MemoryError::Backend(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    use super::*;

    fn write_fake_memory_cli(dir: &Path) -> PathBuf {
        let path = dir.join("memory-backend");
        let script = r#"#!/bin/sh
set -eu
log="${MEMORY_BACKEND_LOG:-}"
if [ -n "$log" ]; then
  printf "%s\n" "$*" >> "$log"
fi

if [ "$1" = "query" ]; then
  # memory-backend query <scope> <query> --output json --limit N
  printf '[{"id":"11111111-1111-1111-1111-111111111111","scope_id":"%s","memory_type":"semantic","content":"remember to run migrations","score":0.9,"metadata":{"fingerprint":"abc"}}]\n' "$2"
  exit 0
fi

if [ "$1" = "memory" ] && [ "$2" = "insert" ]; then
  # memory-backend memory insert <scope> <content> --memory-type semantic ...
  echo "22222222-2222-2222-2222-222222222222"
  exit 0
fi

if [ "$1" = "doctor" ]; then
  echo "ok"
  exit 0
fi

if [ "$1" = "memory" ] && [ "$2" = "delete" ]; then
  exit 0
fi

if [ "$1" = "memory" ] && [ "$2" = "link" ]; then
  exit 0
fi

if [ "$1" = "memory" ] && [ "$2" = "get" ]; then
  echo "content"
  exit 0
fi

echo "unsupported" 1>&2
exit 2
"#;
        fs::write(&path, script).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    #[tokio::test]
    async fn memory_cli_store_and_query_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let log_path = tmp.path().join("memory-backend.log");
        let fake = write_fake_memory_cli(tmp.path());

        std::env::set_var("MEMORY_BACKEND_LOG", &log_path);
        let adapter = MemoryCliAdapter::new(fake.to_string_lossy().to_string(), tmp.path());

        let mut insert =
            InsertMemory::new(ScopeId("project/test".into()), MemoryClass::Semantic, "hi");
        insert
            .metadata
            .insert("run_id".to_string(), "r1".to_string());

        let stored = adapter.store("test", insert).await.unwrap();
        assert_eq!(stored.memory_id, "22222222-2222-2222-2222-222222222222");

        let query = MemoryQuery::new(ScopeId("project/test".into()), "migrations").with_limit(3);
        let results = adapter.query("test", query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].memory_id, "11111111-1111-1111-1111-111111111111");

        // Ensure we called the fake CLI with expected verbs.
        let log = fs::read_to_string(log_path).unwrap();
        assert!(log.contains("memory insert project/test hi --memory-type semantic"));
        assert!(log.contains("query project/test migrations --output json --limit 3"));
    }

    #[tokio::test]
    async fn memory_cli_redaction_required() {
        let tmp = TempDir::new().unwrap();
        let fake = write_fake_memory_cli(tmp.path());
        let adapter = MemoryCliAdapter::new(fake.to_string_lossy().to_string(), tmp.path());

        let insert = InsertMemory::new(
            ScopeId("project/test".into()),
            MemoryClass::Semantic,
            "password=hunter2",
        );
        let err = adapter.store("test", insert).await.unwrap_err();
        matches!(err, MemoryError::RedactionRequired);
    }
}
