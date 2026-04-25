use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::yarli_core::domain::TaskId;

/// Per-task scheduler bindings supplied by run/tranche setup.
#[derive(Debug, Clone)]
pub(crate) struct TaskBindings {
    working_dirs: Arc<RwLock<HashMap<TaskId, String>>>,
    allowed_paths: Arc<RwLock<HashMap<TaskId, Vec<String>>>>,
}

impl TaskBindings {
    pub(crate) fn new() -> Self {
        Self {
            working_dirs: Arc::new(RwLock::new(HashMap::new())),
            allowed_paths: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) async fn bind_working_dir(&self, task_id: TaskId, working_dir: impl Into<String>) {
        let mut dirs = self.working_dirs.write().await;
        dirs.insert(task_id, working_dir.into());
    }

    pub(crate) async fn bind_allowed_paths(&self, task_id: TaskId, allowed_paths: Vec<String>) {
        let mut paths = self.allowed_paths.write().await;
        paths.insert(task_id, allowed_paths);
    }

    pub(crate) async fn working_dir(&self, task_id: TaskId, default_working_dir: &str) -> String {
        let dirs = self.working_dirs.read().await;
        dirs.get(&task_id)
            .cloned()
            .unwrap_or_else(|| default_working_dir.to_string())
    }

    pub(crate) async fn allowed_paths(&self, task_id: TaskId) -> Vec<String> {
        let paths = self.allowed_paths.read().await;
        paths.get(&task_id).cloned().unwrap_or_default()
    }

    pub(crate) async fn allowed_write_roots(
        &self,
        task_id: TaskId,
        working_dir: &str,
    ) -> Vec<String> {
        let paths = self.allowed_paths.read().await;
        paths
            .get(&task_id)
            .map(|allowed_paths| resolve_allowed_write_roots(working_dir, allowed_paths))
            .unwrap_or_default()
    }
}

impl Default for TaskBindings {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize_allowed_write_root(working_dir: &str, allowed_path: &str) -> Option<String> {
    let trimmed = allowed_path.trim();
    if trimmed.is_empty() {
        return None;
    }

    let joined = Path::new(working_dir).join(trimmed);
    let mut normalized = PathBuf::new();
    for component in joined.components() {
        match component {
            std::path::Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    Some(normalized.to_string_lossy().to_string())
}

fn resolve_allowed_write_roots(working_dir: &str, allowed_paths: &[String]) -> Vec<String> {
    let mut roots = Vec::new();
    let mut seen = HashSet::new();
    for allowed_path in allowed_paths {
        let Some(root) = normalize_allowed_write_root(working_dir, allowed_path) else {
            continue;
        };
        if seen.insert(root.clone()) {
            roots.push(root);
        }
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn allowed_write_roots_normalizes_and_deduplicates_bound_paths() {
        let bindings = TaskBindings::new();
        let task_id = uuid::Uuid::now_v7();

        bindings
            .bind_allowed_paths(
                task_id,
                vec![
                    "src".to_string(),
                    "./src".to_string(),
                    " ".to_string(),
                    "docs/guide.md".to_string(),
                ],
            )
            .await;

        assert_eq!(
            bindings.allowed_write_roots(task_id, "/workspace").await,
            vec![
                "/workspace/src".to_string(),
                "/workspace/docs/guide.md".to_string()
            ]
        );
    }
}
