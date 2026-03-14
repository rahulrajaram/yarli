use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io::Read;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;
use uuid::Uuid;

use yarli_cli::yarli_core::domain::Event;
use yarli_cli::yarli_core::explain::{
    DeteriorationFactor, DeteriorationReport, DeteriorationTrend,
};
use yarli_cli::yarli_memory::{
    InsertMemory, MemoryAdapter, MemoryClass, MemoryCliAdapter, MemoryQuery, ScopeId,
};
use yarli_cli::yarli_store::event_store::EventQuery;
use yarli_cli::yarli_store::EventStore;

use crate::config::{LoadedConfig, MemoryProviderKind};
use crate::plan::RunPlan;
use crate::prompt;
use crate::{append_event, IncrementalEventCursor};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct MemoryHint {
    pub(crate) memory_id: String,
    pub(crate) scope_id: String,
    pub(crate) memory_class: MemoryClass,
    pub(crate) relevance_score: f64,
    pub(crate) content_snippet: String,
    pub(crate) metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct MemoryHintsReport {
    pub(crate) query_text: String,
    pub(crate) limit: u32,
    pub(crate) results: Vec<MemoryHint>,
}

pub(crate) struct MemoryObserver {
    enabled: bool,
    project_id: String,
    project_scope: ScopeId,
    run_id: Uuid,
    correlation_id: Uuid,
    query_limit: u32,
    inject_on_run_start: bool,
    inject_on_failure: bool,
    adapter: MemoryCliAdapter,
    run_objective: String,
    task_keys: Vec<String>,
    task_names: BTreeMap<Uuid, String>,
    audit_sink: Option<yarli_cli::yarli_observability::JsonlAuditSink>,
    run_start_done: bool,
}

impl MemoryObserver {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        project_id: String,
        run_id: Uuid,
        correlation_id: Uuid,
        run_objective: String,
        adapter: MemoryCliAdapter,
        query_limit: u32,
        inject_on_run_start: bool,
        inject_on_failure: bool,
        task_keys: Vec<String>,
        task_names: &[(Uuid, String)],
        audit_sink: Option<yarli_cli::yarli_observability::JsonlAuditSink>,
    ) -> Self {
        let project_scope = ScopeId(format!("project/{project_id}"));
        Self {
            enabled: true,
            project_id,
            project_scope,
            run_id,
            correlation_id,
            query_limit,
            inject_on_run_start,
            inject_on_failure,
            adapter,
            run_objective,
            task_keys,
            task_names: task_names.iter().cloned().collect(),
            run_start_done: false,
            audit_sink,
        }
    }

    pub(crate) async fn observe_run_start(&mut self, store: &dyn EventStore) {
        if !self.enabled || self.run_start_done || !self.inject_on_run_start {
            self.run_start_done = true;
            return;
        }
        self.run_start_done = true;

        let query_text = format!(
            "objective: {} tasks: {}",
            self.run_objective,
            self.task_keys.join(",")
        );
        let query = MemoryQuery {
            scope_id: self.project_scope.clone(),
            query_text: query_text.clone(),
            limit: self.query_limit,
            memory_class: Some(MemoryClass::Semantic),
        };

        match self.adapter.query(&self.project_id, query).await {
            Ok(records) => {
                let report = MemoryHintsReport {
                    query_text,
                    limit: self.query_limit,
                    results: records
                        .into_iter()
                        .map(|r| MemoryHint {
                            memory_id: r.memory_id,
                            scope_id: r.scope_id.0,
                            memory_class: r.memory_class,
                            relevance_score: r.relevance_score,
                            content_snippet: truncate_for_snippet(&r.content),
                            metadata: r.metadata.into_iter().collect(),
                        })
                        .collect(),
                };

                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: Utc::now(),
                        entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                        entity_id: self.run_id.to_string(),
                        event_type: "run.observer.memory_hints".to_string(),
                        payload: serde_json::to_value(&report).unwrap_or_else(|_| {
                            serde_json::json!({
                                "query_text": report.query_text,
                                "limit": report.limit,
                                "results": [],
                            })
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: None,
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
            Err(err) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: Utc::now(),
                        entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                        entity_id: self.run_id.to_string(),
                        event_type: "run.observer.memory_query_failed".to_string(),
                        payload: serde_json::json!({
                            "error": err.to_string(),
                            "scope_id": self.project_scope.as_str(),
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: None,
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
        }
    }

    pub(crate) async fn observe_events(&mut self, store: &dyn EventStore, events: &[Event]) {
        if !self.enabled {
            return;
        }

        for event in events {
            match event.event_type.as_str() {
                "task.failed" => {
                    self.on_task_failed(store, event).await;
                    if self.inject_on_failure {
                        self.emit_task_hints(store, event, "failed").await;
                    }
                }
                "task.blocked" => {
                    if self.inject_on_failure {
                        self.emit_task_hints(store, event, "blocked").await;
                    }
                }
                _ => {}
            }
        }
    }

    /// Analyze the completed run and store a semantic memory with the run-level lesson.
    ///
    /// Skips storage for clean completions (no patterns detected).
    /// Emits a `run.observer.analysis` event with the analysis JSON.
    pub(crate) async fn observe_run_end(
        &self,
        store: &dyn EventStore,
        payload: &yarli_cli::yarli_core::entities::ContinuationPayload,
        gate_failures: &[String],
    ) {
        if !self.enabled {
            return;
        }

        let analyzer_tasks: Vec<yarli_cli::yarli_observability::run_analyzer::TaskOutcome> =
            payload
                .tasks
                .iter()
                .map(
                    |t| yarli_cli::yarli_observability::run_analyzer::TaskOutcome {
                        task_key: t.task_key.clone(),
                        state: t.state,
                        last_error: t.last_error.clone(),
                        blocker: t.blocker.clone(),
                    },
                )
                .collect();

        let analysis = yarli_cli::yarli_observability::run_analyzer::analyze_run(
            payload.exit_state,
            payload.exit_reason,
            &analyzer_tasks,
            gate_failures,
        );

        // Emit analysis event
        let pattern_names =
            yarli_cli::yarli_observability::run_analyzer::pattern_names(&analysis.patterns);
        let recommendation_str = serde_json::to_string(&analysis.retry_recommendation)
            .unwrap_or_else(|_| "unknown".to_string());

        if let Some(lesson) = &analysis.run_lesson {
            if let Some(sink) = &self.audit_sink {
                let tranche_token_correlation = build_tranche_token_correlation_details(payload);
                let audit_entry = yarli_cli::yarli_observability::AuditEntry::run_analysis(
                    &pattern_names,
                    recommendation_str.as_str(),
                    analysis.confidence,
                    Some(lesson),
                    self.run_id,
                    tranche_token_correlation,
                );
                if let Err(err) =
                    yarli_cli::yarli_observability::AuditSink::append(sink, &audit_entry)
                {
                    warn!(error = %err, "failed to append run analysis audit entry");
                }
            }
        }

        let _ = append_event(
            store,
            Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                entity_id: self.run_id.to_string(),
                event_type: "run.observer.analysis".to_string(),
                payload: serde_json::to_value(&analysis).unwrap_or_else(|_| {
                    serde_json::json!({
                        "patterns": pattern_names,
                        "confidence": analysis.confidence,
                    })
                }),
                correlation_id: self.correlation_id,
                causation_id: None,
                actor: "observer.run_analyzer".to_string(),
                idempotency_key: None,
            },
        );

        // Store semantic memory only for non-trivial runs (has patterns)
        if let Some(lesson) = &analysis.run_lesson {
            let content = truncate_for_memory(&format!(
                "run_analysis: objective={} patterns=[{}] recommendation={} lesson={}",
                self.run_objective,
                pattern_names.join(","),
                recommendation_str,
                lesson
            ));

            let mut semantic = yarli_cli::yarli_memory::InsertMemory::new(
                self.project_scope.clone(),
                yarli_cli::yarli_memory::MemoryClass::Semantic,
                content,
            );
            semantic
                .metadata
                .insert("run_id".to_string(), self.run_id.to_string());
            semantic
                .metadata
                .insert("patterns".to_string(), pattern_names.join(","));
            semantic.metadata.insert(
                "confidence".to_string(),
                format!("{:.2}", analysis.confidence),
            );

            match self.adapter.store(&self.project_id, semantic).await {
                Ok(record) => {
                    let _ = append_event(
                        store,
                        Event {
                            event_id: Uuid::now_v7(),
                            occurred_at: Utc::now(),
                            entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                            entity_id: self.run_id.to_string(),
                            event_type: "run.observer.analysis_memory_stored".to_string(),
                            payload: serde_json::json!({
                                "memory_id": record.memory_id,
                                "scope_id": record.scope_id.as_str(),
                                "memory_class": record.memory_class,
                                "patterns": pattern_names,
                            }),
                            correlation_id: self.correlation_id,
                            causation_id: None,
                            actor: "observer.run_analyzer".to_string(),
                            idempotency_key: None,
                        },
                    );
                }
                Err(err) => {
                    let _ = append_event(
                        store,
                        Event {
                            event_id: Uuid::now_v7(),
                            occurred_at: Utc::now(),
                            entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                            entity_id: self.run_id.to_string(),
                            event_type: "run.observer.analysis_memory_store_failed".to_string(),
                            payload: serde_json::json!({
                                "error": err.to_string(),
                                "scope_id": self.project_scope.as_str(),
                            }),
                            correlation_id: self.correlation_id,
                            causation_id: None,
                            actor: "observer.run_analyzer".to_string(),
                            idempotency_key: None,
                        },
                    );
                }
            }
        }
    }

    async fn on_task_failed(&self, store: &dyn EventStore, event: &Event) {
        let Ok(task_id) = event.entity_id.parse::<Uuid>() else {
            return;
        };
        let task_key = self
            .task_names
            .get(&task_id)
            .cloned()
            .unwrap_or_else(|| event.entity_id[..8].to_string());

        let reason = event
            .payload
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let exit_code = event.payload.get("exit_code").and_then(|v| v.as_i64());
        let detail = event
            .payload
            .get("detail")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let content = truncate_for_memory(&format!(
            "task_failed: task_key={task_key} reason={reason} exit_code={} detail={detail}",
            exit_code
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));

        let mut semantic =
            InsertMemory::new(self.project_scope.clone(), MemoryClass::Semantic, content);
        semantic
            .metadata
            .insert("run_id".to_string(), self.run_id.to_string());
        semantic
            .metadata
            .insert("task_id".to_string(), task_id.to_string());
        semantic
            .metadata
            .insert("task_key".to_string(), task_key.clone());
        semantic
            .metadata
            .insert("reason".to_string(), reason.clone());
        semantic
            .metadata
            .insert("event_id".to_string(), event.event_id.to_string());

        match self.adapter.store(&self.project_id, semantic).await {
            Ok(record) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: Utc::now(),
                        entity_type: yarli_cli::yarli_core::domain::EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_stored".to_string(),
                        payload: serde_json::json!({
                            "memory_id": record.memory_id,
                            "scope_id": record.scope_id.as_str(),
                            "memory_class": record.memory_class,
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
            Err(err) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: Utc::now(),
                        entity_type: yarli_cli::yarli_core::domain::EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_store_failed".to_string(),
                        payload: serde_json::json!({
                            "error": err.to_string(),
                            "scope_id": self.project_scope.as_str(),
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
        }
    }

    async fn emit_task_hints(&self, store: &dyn EventStore, event: &Event, state: &str) {
        let Ok(task_id) = event.entity_id.parse::<Uuid>() else {
            return;
        };

        let task_key = self
            .task_names
            .get(&task_id)
            .cloned()
            .unwrap_or_else(|| event.entity_id[..8].to_string());
        let reason = event
            .payload
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let query_text = format!("{state} task_key={task_key} reason={reason}");

        let query = MemoryQuery {
            scope_id: self.project_scope.clone(),
            query_text: query_text.clone(),
            limit: self.query_limit,
            memory_class: Some(MemoryClass::Semantic),
        };

        match self.adapter.query(&self.project_id, query).await {
            Ok(records) => {
                let report = MemoryHintsReport {
                    query_text,
                    limit: self.query_limit,
                    results: records
                        .into_iter()
                        .map(|r| MemoryHint {
                            memory_id: r.memory_id,
                            scope_id: r.scope_id.0,
                            memory_class: r.memory_class,
                            relevance_score: r.relevance_score,
                            content_snippet: truncate_for_snippet(&r.content),
                            metadata: r.metadata.into_iter().collect(),
                        })
                        .collect(),
                };
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: Utc::now(),
                        entity_type: yarli_cli::yarli_core::domain::EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_hints".to_string(),
                        payload: serde_json::to_value(&report).unwrap_or_else(|_| {
                            serde_json::json!({
                                "query_text": report.query_text,
                                "limit": report.limit,
                                "results": [],
                            })
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
            Err(err) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: Utc::now(),
                        entity_type: yarli_cli::yarli_core::domain::EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_query_failed".to_string(),
                        payload: serde_json::json!({
                            "error": err.to_string(),
                            "scope_id": self.project_scope.as_str(),
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
        }
    }
}

fn truncate_for_snippet(content: &str) -> String {
    const MAX: usize = 160;
    let trimmed = content.trim();
    if trimmed.len() <= MAX {
        return trimmed.to_string();
    }
    let mut end = MAX;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &trimmed[..end])
}

fn truncate_for_memory(content: &str) -> String {
    const MAX: usize = 1024;
    let trimmed = content.trim();
    if trimmed.len() <= MAX {
        return trimmed.to_string();
    }
    let mut end = MAX;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &trimmed[..end])
}

pub(crate) const OBSERVER_EVENT_BATCH_LIMIT: usize = 256;
pub(crate) const OBSERVER_WINDOW_SIZE: usize = 64;
pub(crate) const OBSERVER_FAILURE_RING_SIZE: usize = 32;
const OBSERVER_TREND_CYCLE_MAX_PATTERN: usize = 8;
const OBSERVER_FAILURE_REPEAT_THRESHOLD: usize = 3;
const OBSERVER_FAILURE_ALT_REPEAT_THRESHOLD: usize = 2;

#[derive(Debug, Clone)]
pub(crate) struct DeteriorationObserver {
    run_id: Uuid,
    correlation_id: Uuid,
    cursor: IncrementalEventCursor,
    state: DeteriorationObserverState,
    latest_report: Option<DeteriorationReport>,
}

#[derive(Debug, Clone)]
struct ArtifactTailCursor {
    byte_offset: u64,
}

impl ArtifactTailCursor {
    fn new() -> Self {
        Self { byte_offset: 0 }
    }
}

#[derive(Debug, Clone)]
struct ToolOutcomeObservation {
    tool: String,
    status: String,
    outcome: serde_json::Value,
    detail: Option<String>,
    score: Option<f64>,
    metric: Option<String>,
}

impl ToolOutcomeObservation {
    fn as_payload(&self, task_id: Uuid, task_key: &str, run_id: Uuid) -> serde_json::Value {
        serde_json::json!({
            "task_id": task_id,
            "task_key": task_key,
            "run_id": run_id,
            "tool": self.tool,
            "status": self.status,
            "outcome": self.outcome,
            "detail": self.detail,
            "score": self.score,
            "metric": self.metric,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TaskHealthArtifactObserver {
    run_id: Uuid,
    correlation_id: Uuid,
    task_keys: BTreeMap<Uuid, String>,
    tail: HashMap<Uuid, ArtifactTailCursor>,
}

impl TaskHealthArtifactObserver {
    pub(crate) fn new(run_id: Uuid, correlation_id: Uuid, task_names: &[(Uuid, String)]) -> Self {
        Self {
            run_id,
            correlation_id,
            task_keys: task_names.iter().cloned().collect(),
            tail: task_names
                .iter()
                .map(|(task_id, _)| (*task_id, ArtifactTailCursor::new()))
                .collect(),
        }
    }

    pub(crate) fn observe_store(&mut self, store: &dyn EventStore) -> usize {
        let mut emitted = 0usize;

        let tasks: Vec<(Uuid, String)> = self
            .task_keys
            .iter()
            .map(|(task_id, task_key)| (*task_id, task_key.clone()))
            .collect();

        for (task_id, task_key) in tasks {
            let path = task_output_artifact_path(task_id);
            let Some(events) = self.read_new_tool_outcomes(task_id, &path) else {
                continue;
            };

            for (outcome, command_event_id, captured_at) in events {
                emitted += 1;
                let payload = outcome.as_payload(task_id, &task_key, self.run_id);
                let status_event = Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: captured_at,
                    entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                    entity_id: self.run_id.to_string(),
                    event_type: "run.observer.task_health".to_string(),
                    payload,
                    correlation_id: self.correlation_id,
                    causation_id: command_event_id,
                    actor: "observer.task_health".to_string(),
                    idempotency_key: None,
                };
                if let Err(err) = append_event(store, status_event) {
                    warn!(error = %err, task_id = %task_id, "failed to append tool outcome observation event");
                    continue;
                }
            }
        }

        emitted
    }

    #[allow(clippy::type_complexity)]
    fn read_new_tool_outcomes(
        &mut self,
        task_id: Uuid,
        path: &PathBuf,
    ) -> Option<
        Vec<(
            ToolOutcomeObservation,
            Option<Uuid>,
            chrono::DateTime<chrono::Utc>,
        )>,
    > {
        let cursor = self.tail.get(&task_id).cloned()?;
        let mut file = OpenOptions::new().read(true).open(path).ok()?;

        if let Err(err) = file.seek(SeekFrom::Start(cursor.byte_offset)) {
            warn!(error = %err, task_id = %task_id, "failed to seek backend artifact" );
            return None;
        }

        let mut data = String::new();
        let bytes_read = match file.read_to_string(&mut data) {
            Ok(0) => return Some(Vec::new()),
            Ok(bytes_read) => bytes_read,
            Err(err) => {
                warn!(error = %err, task_id = %task_id, "failed to read backend artifact");
                return None;
            }
        };

        let cursor = self
            .tail
            .get_mut(&task_id)
            .expect("cursor must exist for observed task");
        cursor.byte_offset = cursor.byte_offset.saturating_add(bytes_read as u64);

        let mut observations = Vec::new();

        for raw_line in data.lines() {
            let Some(value) = serde_json::from_str::<serde_json::Value>(raw_line).ok() else {
                continue;
            };

            let command_event_id = value
                .get("command_id")
                .and_then(|value| value.as_str())
                .and_then(|value| Uuid::parse_str(value).ok());
            let captured_at = value
                .get("captured_at")
                .and_then(|value| value.as_str())
                .and_then(|value| {
                    chrono::DateTime::parse_from_rfc3339(value)
                        .ok()
                        .map(|parsed| parsed.with_timezone(&chrono::Utc))
                })
                .unwrap_or_else(Utc::now);

            if value
                .get("artifact_type")
                .and_then(|v| v.as_str())
                .is_some_and(|value| value == "command_output")
            {
                continue;
            }

            for parsed in expand_tool_outcome_payload_candidates(&value) {
                if let Some(observation) = parse_tool_health_observation(&parsed) {
                    observations.push((observation, command_event_id, captured_at));
                    break;
                }
            }
        }

        Some(observations)
    }
}

fn task_output_artifact_path(task_id: Uuid) -> PathBuf {
    PathBuf::from(".yarl/runs").join(format!("{task_id}.jsonl"))
}

fn expand_tool_outcome_payload_candidates(value: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut candidates = Vec::new();

    if let Some(data) = value.get("data").and_then(|value| value.as_str()) {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(data) {
            candidates.push(parsed);
        }
    }
    if let Some(inner) = value.get("data").filter(|value| Value::is_object(value)) {
        candidates.push(inner.to_owned());
    }

    candidates.push(value.clone());
    candidates
}

fn parse_tool_health_observation(value: &serde_json::Value) -> Option<ToolOutcomeObservation> {
    let event_type = value
        .get("type")
        .and_then(|value| value.as_str())
        .map(|raw| raw.to_lowercase());
    let event_type = event_type.as_deref();

    let is_health_event = matches!(
        event_type,
        Some("task_health")
            | Some("task-health")
            | Some("tool_health")
            | Some("tool-health")
            | Some("tool_outcome")
            | Some("tool-outcome")
    );

    let tool = value
        .get("tool")
        .and_then(|value| value.as_str())
        .or_else(|| value.get("name").and_then(|value| value.as_str()))
        .or_else(|| value.get("source").and_then(|value| value.as_str()))
        .map(ToString::to_string)?;

    let status = value
        .get("status")
        .and_then(|value| value.as_str())
        .or_else(|| {
            value
                .get("outcome")
                .and_then(|value| value.get("status"))
                .and_then(|value| value.as_str())
        })
        .or_else(|| value.get("result").and_then(|value| value.as_str()))
        .unwrap_or("unknown");

    let outcome = if let Some(outcome) = value.get("outcome") {
        outcome.to_owned()
    } else {
        value.clone()
    };

    if !(is_health_event || value.get("status").is_some() || value.get("outcome").is_some()) {
        return None;
    }

    let score = outcome
        .get("score")
        .and_then(|value| value.as_f64())
        .or_else(|| value.get("score").and_then(|value| value.as_f64()));

    let metric = outcome
        .get("metric")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("metric")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        });

    let detail = outcome
        .get("detail")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("message")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        });

    Some(ToolOutcomeObservation {
        tool,
        status: status.to_owned(),
        outcome,
        score,
        metric,
        detail,
    })
}

impl DeteriorationObserver {
    pub(crate) fn new(run_id: Uuid, correlation_id: Uuid, window_size: usize) -> Self {
        Self {
            run_id,
            correlation_id,
            cursor: IncrementalEventCursor::new(
                EventQuery::by_correlation(correlation_id),
                OBSERVER_EVENT_BATCH_LIMIT,
            ),
            state: DeteriorationObserverState::new(window_size),
            latest_report: None,
        }
    }

    pub(crate) fn observe_store(&mut self, store: &dyn EventStore) -> Result<()> {
        let events = self.cursor.read_new_events(store)?;
        if events.is_empty() {
            return Ok(());
        }

        let has_relevant = self.state.ingest(&events);
        if !has_relevant {
            return Ok(());
        }

        let report = self.state.report();
        self.latest_report = Some(report.clone());
        for signal in self.state.take_deterioration_detected() {
            append_event(
                store,
                Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: Utc::now(),
                    entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                    entity_id: self.run_id.to_string(),
                    event_type: "run.observer.deterioration_detected".to_string(),
                    payload: signal.to_payload(&report),
                    correlation_id: self.correlation_id,
                    causation_id: events.last().map(|event| event.event_id),
                    actor: "observer.deterioration".to_string(),
                    idempotency_key: None,
                },
            )?;
        }
        if let Some(cycle) = self.state.take_cycle_event() {
            append_event(
                store,
                Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: Utc::now(),
                    entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                    entity_id: self.run_id.to_string(),
                    event_type: "run.observer.deterioration_cycle".to_string(),
                    payload: serde_json::json!({
                        "pattern": cycle
                            .pattern
                            .iter()
                            .map(deterioration_trend_name)
                            .collect::<Vec<_>>(),
                        "pattern_length": cycle.pattern.len(),
                        "repeat_count": cycle.repeat_count,
                        "score": report.score,
                        "window_size": report.window_size,
                        "trend": deterioration_trend_name(&report.trend),
                    }),
                    correlation_id: self.correlation_id,
                    causation_id: events.last().map(|event| event.event_id),
                    actor: "observer.deterioration".to_string(),
                    idempotency_key: None,
                },
            )?;
        }

        append_event(
            store,
            Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: yarli_cli::yarli_core::domain::EntityType::Run,
                entity_id: self.run_id.to_string(),
                event_type: "run.observer.deterioration".to_string(),
                payload: serde_json::json!({
                    "score": report.score,
                    "window_size": report.window_size,
                    "factors": report.factors,
                    "trend": report.trend,
                }),
                correlation_id: self.correlation_id,
                causation_id: events.last().map(|event| event.event_id),
                actor: "observer.deterioration".to_string(),
                idempotency_key: None,
            },
        )?;

        Ok(())
    }

    pub(crate) fn latest_report(&self) -> Option<&DeteriorationReport> {
        self.latest_report.as_ref()
    }

    pub(crate) fn has_deterioration_cycle(&self) -> bool {
        self.state.has_deterioration_cycle()
    }
}

#[derive(Debug, Clone)]
struct RuntimeSample {
    command_key: String,
    duration_ms: f64,
}

#[derive(Debug, Clone)]
struct BudgetSample {
    headroom: f64,
}

#[derive(Debug, Clone)]
enum ObserverSignal {
    Runtime(RuntimeSample),
    Retry,
    Blocked,
    Failure {
        #[allow(dead_code)]
        reason_bucket: String,
    },
    Budget(BudgetSample),
}

#[derive(Debug, Clone)]
pub(crate) struct DeteriorationObserverState {
    window_size: usize,
    signals: VecDeque<ObserverSignal>,
    failure_signatures: VecDeque<String>,
    failure_signature_last_repeat_key: Option<String>,
    failure_signature_last_alt_key: Option<String>,
    pending_commands: HashMap<String, String>,
    previous_score: Option<f64>,
    trend_history: VecDeque<DeteriorationTrend>,
    last_cycle_signature: Option<String>,
    has_cycle_detected: bool,
    latest_cycle: Option<DeteriorationCycle>,
    latest_deterioration_detected: Vec<DeteriorationDetectedSignatureSignal>,
}

impl DeteriorationObserverState {
    pub(crate) fn new(window_size: usize) -> Self {
        Self {
            window_size,
            signals: VecDeque::new(),
            failure_signatures: VecDeque::new(),
            failure_signature_last_repeat_key: None,
            failure_signature_last_alt_key: None,
            pending_commands: HashMap::new(),
            previous_score: None,
            trend_history: VecDeque::new(),
            last_cycle_signature: None,
            has_cycle_detected: false,
            latest_cycle: None,
            latest_deterioration_detected: Vec::new(),
        }
    }

    pub(crate) fn has_deterioration_cycle(&self) -> bool {
        self.has_cycle_detected
    }

    pub(crate) fn ingest(&mut self, events: &[Event]) -> bool {
        let mut changed = false;

        for event in events {
            match event.event_type.as_str() {
                "command.started" => {
                    let command_key = event
                        .payload
                        .get("command")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    self.pending_commands
                        .insert(event.entity_id.clone(), command_key);
                }
                "command.exited" | "command.timed_out" | "command.killed" => {
                    let duration_ms = event
                        .payload
                        .get("duration_ms")
                        .and_then(|v| v.as_f64())
                        .or_else(|| {
                            event
                                .payload
                                .get("duration_ms")
                                .and_then(|v| v.as_i64())
                                .map(|v| v as f64)
                        })
                        .unwrap_or(0.0)
                        .max(0.0);
                    let command_key = self
                        .pending_commands
                        .remove(&event.entity_id)
                        .unwrap_or_else(|| "unknown".to_string());
                    self.push_signal(ObserverSignal::Runtime(RuntimeSample {
                        command_key,
                        duration_ms,
                    }));
                    changed = true;
                }
                "task.retrying" => {
                    self.push_signal(ObserverSignal::Retry);
                    changed = true;
                }
                "task.blocked" => {
                    self.push_signal(ObserverSignal::Blocked);
                    changed = true;
                }
                "task.failed" => {
                    let bucket = normalize_error_signature(event);
                    self.push_signal(ObserverSignal::Failure {
                        reason_bucket: bucket.clone(),
                    });
                    self.push_failure_signature(bucket);
                    changed = true;

                    if let (Some(observed), Some(limit)) = (
                        event.payload.get("observed").and_then(|v| v.as_f64()),
                        event.payload.get("limit").and_then(|v| v.as_f64()),
                    ) {
                        if limit > 0.0 {
                            let headroom = (1.0 - (observed / limit)).clamp(0.0, 1.0);
                            self.push_signal(ObserverSignal::Budget(BudgetSample { headroom }));
                        }
                    }
                }
                "run.observer.task_health" => {
                    if !is_backend_task_health_failure(event) {
                        continue;
                    }

                    let bucket = normalize_error_signature(event);
                    self.push_signal(ObserverSignal::Failure {
                        reason_bucket: bucket.clone(),
                    });
                    self.push_failure_signature(bucket);
                    changed = true;
                }
                _ => {}
            }
        }

        changed
    }

    fn push_signal(&mut self, signal: ObserverSignal) {
        self.signals.push_back(signal);
        while self.signals.len() > self.window_size {
            self.signals.pop_front();
        }
    }

    fn push_failure_signature(&mut self, signature: String) {
        self.failure_signatures.push_back(signature);
        while self.failure_signatures.len() > OBSERVER_FAILURE_RING_SIZE {
            self.failure_signatures.pop_front();
        }
    }

    pub(crate) fn report(&mut self) -> DeteriorationReport {
        let runtime_drift = self.runtime_drift_score();
        let retry_inflation = self.retry_inflation_score();
        let blocker_churn = self.blocker_churn_score();
        let failure_drift = self.failure_drift_score();
        let budget_erosion = self.budget_erosion_score();

        let mut factors = vec![
            DeteriorationFactor {
                name: "runtime_drift".to_string(),
                impact: runtime_drift,
                detail: "runtime trend for repeated command keys".to_string(),
            },
            DeteriorationFactor {
                name: "retry_inflation".to_string(),
                impact: retry_inflation,
                detail: "retry events per rolling window".to_string(),
            },
            DeteriorationFactor {
                name: "blocker_churn".to_string(),
                impact: blocker_churn,
                detail: "task.blocked churn in rolling window".to_string(),
            },
            DeteriorationFactor {
                name: "failure_rate_drift".to_string(),
                impact: failure_drift,
                detail: "failure bucket rate drift".to_string(),
            },
            DeteriorationFactor {
                name: "budget_headroom_erosion".to_string(),
                impact: budget_erosion,
                detail: "budget headroom trend".to_string(),
            },
        ];

        factors.sort_by(|a, b| b.impact.total_cmp(&a.impact));
        factors.truncate(3);

        let score = (runtime_drift * 30.0
            + retry_inflation * 20.0
            + blocker_churn * 15.0
            + failure_drift * 25.0
            + budget_erosion * 10.0)
            .clamp(0.0, 100.0);

        let trend = match self.previous_score {
            Some(prev) if score - prev > 5.0 => DeteriorationTrend::Deteriorating,
            Some(prev) if prev - score > 5.0 => DeteriorationTrend::Improving,
            Some(_) => DeteriorationTrend::Stable,
            None => DeteriorationTrend::Stable,
        };
        self.push_trend(trend);
        self.latest_deterioration_detected = self.detect_deterioration_signals();
        self.latest_cycle = self.detect_cycle_event();
        self.previous_score = Some(score);

        DeteriorationReport {
            score,
            window_size: self.signals.len(),
            factors,
            trend,
        }
    }

    fn runtime_drift_score(&self) -> f64 {
        let mut per_key: HashMap<&str, Vec<f64>> = HashMap::new();
        for signal in &self.signals {
            if let ObserverSignal::Runtime(sample) = signal {
                per_key
                    .entry(sample.command_key.as_str())
                    .or_default()
                    .push(sample.duration_ms);
            }
        }

        let mut drifts = Vec::new();
        for durations in per_key.values() {
            if durations.len() < 2 {
                continue;
            }
            let split = durations.len() / 2;
            if split == 0 {
                continue;
            }
            let first_avg = durations[..split].iter().sum::<f64>() / split as f64;
            let second_avg =
                durations[split..].iter().sum::<f64>() / (durations.len() - split) as f64;
            let drift = ((second_avg - first_avg) / first_avg.max(1.0)).max(0.0);
            drifts.push(drift.min(1.0));
        }

        if drifts.is_empty() {
            0.0
        } else {
            drifts.iter().sum::<f64>() / drifts.len() as f64
        }
    }

    fn retry_inflation_score(&self) -> f64 {
        let retries = self
            .signals
            .iter()
            .filter(|signal| matches!(signal, ObserverSignal::Retry))
            .count() as f64;
        (retries / self.window_size.max(1) as f64).clamp(0.0, 1.0)
    }

    fn blocker_churn_score(&self) -> f64 {
        let blocked = self
            .signals
            .iter()
            .filter(|signal| matches!(signal, ObserverSignal::Blocked))
            .count() as f64;
        (blocked / self.window_size.max(1) as f64).clamp(0.0, 1.0)
    }

    fn failure_drift_score(&self) -> f64 {
        let failures: Vec<&str> = self.failure_signatures.iter().map(String::as_str).collect();
        if failures.len() < 2 {
            return 0.0;
        }

        let split = failures.len() / 2;
        if split == 0 {
            return 0.0;
        }

        let mut first_counts: HashMap<&str, usize> = HashMap::new();
        let mut second_counts: HashMap<&str, usize> = HashMap::new();
        for bucket in &failures[..split] {
            *first_counts.entry(*bucket).or_default() += 1;
        }
        for bucket in &failures[split..] {
            *second_counts.entry(*bucket).or_default() += 1;
        }

        let first_total = split as f64;
        let second_total = (failures.len() - split) as f64;
        let mut max_increase: f64 = 0.0;

        for bucket in second_counts.keys() {
            let first_rate = *first_counts.get(bucket).unwrap_or(&0) as f64 / first_total.max(1.0);
            let second_rate =
                *second_counts.get(bucket).unwrap_or(&0) as f64 / second_total.max(1.0);
            max_increase = max_increase.max((second_rate - first_rate).max(0.0));
        }

        max_increase.clamp(0.0, 1.0)
    }

    fn budget_erosion_score(&self) -> f64 {
        let headrooms: Vec<f64> = self
            .signals
            .iter()
            .filter_map(|signal| {
                if let ObserverSignal::Budget(sample) = signal {
                    Some(sample.headroom)
                } else {
                    None
                }
            })
            .collect();
        if headrooms.len() < 2 {
            return 0.0;
        }

        let split = headrooms.len() / 2;
        if split == 0 {
            return 0.0;
        }

        let first_avg = headrooms[..split].iter().sum::<f64>() / split as f64;
        let second_avg = headrooms[split..].iter().sum::<f64>() / (headrooms.len() - split) as f64;
        (first_avg - second_avg).clamp(0.0, 1.0)
    }

    fn push_trend(&mut self, trend: DeteriorationTrend) {
        self.trend_history.push_back(trend);
        while self.trend_history.len() > self.window_size {
            self.trend_history.pop_front();
        }
    }

    fn detect_cycle_event(&mut self) -> Option<DeteriorationCycle> {
        let Some(cycle) = detect_deterioration_cycle(&self.trend_history) else {
            self.last_cycle_signature = None;
            return None;
        };

        let signature = deterioration_cycle_signature(&cycle.pattern, cycle.repeat_count);
        if self.last_cycle_signature.as_deref() == Some(&signature) {
            return None;
        }

        self.has_cycle_detected = true;
        self.last_cycle_signature = Some(signature);
        Some(cycle)
    }

    fn take_cycle_event(&mut self) -> Option<DeteriorationCycle> {
        self.latest_cycle.take()
    }

    fn detect_deterioration_signals(&mut self) -> Vec<DeteriorationDetectedSignatureSignal> {
        let mut signals = Vec::new();
        if let Some(repeat) = self.detect_repeated_failure_signature() {
            signals.push(repeat);
        }
        if let Some(cycle) = self.detect_alternating_failure_cycle() {
            signals.push(cycle);
        }
        signals
    }

    fn take_deterioration_detected(&mut self) -> Vec<DeteriorationDetectedSignatureSignal> {
        std::mem::take(&mut self.latest_deterioration_detected)
    }

    fn detect_repeated_failure_signature(
        &mut self,
    ) -> Option<DeteriorationDetectedSignatureSignal> {
        let signature = self.failure_signatures.back()?;
        let mut repeat_count = 0usize;
        for known in self.failure_signatures.iter().rev() {
            if known == signature {
                repeat_count += 1;
                continue;
            }
            break;
        }

        if repeat_count < OBSERVER_FAILURE_REPEAT_THRESHOLD {
            self.failure_signature_last_repeat_key = None;
            return None;
        }

        let key = format!("repeat:{signature}:{repeat_count}");
        if self.failure_signature_last_repeat_key.as_deref() == Some(&key) {
            return None;
        }
        self.failure_signature_last_repeat_key = Some(key);

        Some(DeteriorationDetectedSignatureSignal {
            kind: DeteriorationDetectedSignatureKind::RepeatedSignature,
            signatures: vec![signature.clone()],
            repeat_count,
        })
    }

    fn detect_alternating_failure_cycle(&mut self) -> Option<DeteriorationDetectedSignatureSignal> {
        let Some(cycle) = detect_signature_cycle(&self.failure_signatures) else {
            self.failure_signature_last_alt_key = None;
            return None;
        };

        let key = failure_cycle_signature(&cycle.pattern, cycle.repeat_count);
        if self.failure_signature_last_alt_key.as_deref() == Some(&key) {
            return None;
        }
        self.failure_signature_last_alt_key = Some(key);

        Some(DeteriorationDetectedSignatureSignal {
            kind: DeteriorationDetectedSignatureKind::AlternatingSignatureCycle,
            signatures: cycle.pattern,
            repeat_count: cycle.repeat_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DeteriorationCycle {
    pattern: Vec<DeteriorationTrend>,
    repeat_count: usize,
}

#[derive(Debug, Clone)]
struct DeteriorationDetectedSignatureSignal {
    kind: DeteriorationDetectedSignatureKind,
    signatures: Vec<String>,
    repeat_count: usize,
}

impl DeteriorationDetectedSignatureSignal {
    fn to_payload(&self, report: &DeteriorationReport) -> serde_json::Value {
        match self.kind {
            DeteriorationDetectedSignatureKind::RepeatedSignature => serde_json::json!({
                "signal": "backend_failure_repeated_signature",
                "signature_count": self.repeat_count,
                "signatures": self.signatures,
                "score": report.score,
                "window_size": report.window_size,
                "trend": deterioration_trend_name(&report.trend),
            }),
            DeteriorationDetectedSignatureKind::AlternatingSignatureCycle => serde_json::json!({
                "signal": "backend_failure_alternating_cycle",
                "signature_count": self.signatures.len() * self.repeat_count,
                "repeat_count": self.repeat_count,
                "signatures": self.signatures,
                "score": report.score,
                "window_size": report.window_size,
                "trend": deterioration_trend_name(&report.trend),
            }),
        }
    }
}

#[derive(Debug, Clone)]
enum DeteriorationDetectedSignatureKind {
    RepeatedSignature,
    AlternatingSignatureCycle,
}

#[derive(Debug, Clone, PartialEq)]
struct FailureSignatureCycle {
    pattern: Vec<String>,
    repeat_count: usize,
}

fn detect_deterioration_cycle(
    trend_history: &VecDeque<DeteriorationTrend>,
) -> Option<DeteriorationCycle> {
    if trend_history.len() < 4 {
        return None;
    }

    let values: Vec<_> = trend_history.iter().copied().collect();
    let trend_count = values.len();
    let max_pattern_length = (trend_count / 2).min(OBSERVER_TREND_CYCLE_MAX_PATTERN);

    for pattern_len in 2..=max_pattern_length {
        let candidate = &values[(trend_count - pattern_len)..trend_count];
        if all_same_trend(candidate) {
            continue;
        }

        let mut repeats = 1;
        let mut start = trend_count - pattern_len;
        while start >= pattern_len {
            let previous_start = start - pattern_len;
            if values[previous_start..start] != *candidate {
                break;
            }
            repeats += 1;
            start = previous_start;
            if start == 0 {
                break;
            }
        }

        if repeats >= 2 {
            return Some(DeteriorationCycle {
                pattern: candidate.to_vec(),
                repeat_count: repeats,
            });
        }
    }

    None
}

fn deterioration_cycle_signature(pattern: &[DeteriorationTrend], repeat_count: usize) -> String {
    let joined = pattern
        .iter()
        .map(deterioration_trend_name)
        .collect::<Vec<_>>()
        .join(",");
    format!("{repeat_count}:{joined}")
}

fn detect_signature_cycle(failure_signatures: &VecDeque<String>) -> Option<FailureSignatureCycle> {
    if failure_signatures.len() < 4 {
        return None;
    }

    if failure_signatures
        .iter()
        .skip(failure_signatures.len().saturating_sub(2))
        .any(|signature| signature.is_empty())
    {
        return None;
    }

    let last_two_start = failure_signatures.len().saturating_sub(2);
    let pattern = vec![
        failure_signatures.get(last_two_start)?.to_owned(),
        failure_signatures
            .get(last_two_start.saturating_add(1))?
            .to_owned(),
    ];

    if pattern[0] == pattern[1] {
        return None;
    }

    let mut repeat_count = 1usize;
    let mut idx = last_two_start;
    while idx >= 2 {
        let previous = [
            failure_signatures.get(idx.saturating_sub(2))?,
            failure_signatures.get(idx.saturating_sub(1))?,
        ];

        if previous != [&pattern[0], &pattern[1]] {
            break;
        }
        repeat_count += 1;
        if idx == 2 {
            break;
        }
        idx -= 2;
    }

    if repeat_count >= OBSERVER_FAILURE_ALT_REPEAT_THRESHOLD {
        return Some(FailureSignatureCycle {
            pattern,
            repeat_count,
        });
    }

    None
}

fn failure_cycle_signature(pattern: &[String], repeat_count: usize) -> String {
    let joined = pattern.join("|");
    format!("{repeat_count}:{joined}")
}

fn all_same_trend(trends: &[DeteriorationTrend]) -> bool {
    let mut iter = trends.iter();
    let Some(first) = iter.next() else {
        return true;
    };

    iter.all(|trend| trend == first)
}

const fn deterioration_trend_name(trend: &DeteriorationTrend) -> &'static str {
    match trend {
        DeteriorationTrend::Improving => "improving",
        DeteriorationTrend::Stable => "stable",
        DeteriorationTrend::Deteriorating => "deteriorating",
    }
}

pub(crate) fn normalize_error_signature(event: &Event) -> String {
    if event.event_type == "run.observer.task_health" {
        return normalize_task_health_error_signature(event);
    }

    let reason = event
        .payload
        .get("reason")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let detail = event
        .payload
        .get("detail")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let scope = event
        .payload
        .get("scope")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let metric = event
        .payload
        .get("metric")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(reason) = reason {
        let normalized = reason.to_ascii_lowercase();
        match normalized.as_str() {
            "nonzero_exit" | "timeout" | "killed" | "exec_error" => {
                return normalized.to_string();
            }
            "budget_exceeded" => {
                if let (Some(scope), Some(metric)) = (scope, metric) {
                    return format!("budget_exceeded:{scope}:{metric}");
                }
                return "budget_exceeded".to_string();
            }
            _ => {
                if let Some(bucket) = normalized.split(':').next() {
                    return bucket.to_string();
                }
            }
        }
    }

    if let Some(detail) = detail {
        let detail = detail.to_ascii_lowercase();
        if detail.contains("command exited with code") || detail.contains("exit code") {
            return "nonzero_exit".to_string();
        }
        if detail.contains("command timed out") || detail.contains("timed out") {
            return "timeout".to_string();
        }
        if detail.contains("execution error") {
            return "exec_error".to_string();
        }
        if detail.contains("killed") {
            return "killed".to_string();
        }
    }

    "unknown".to_string()
}

fn normalize_task_health_error_signature(event: &Event) -> String {
    let status = event
        .payload
        .get("status")
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    let status = if status == "failed" || status == "error" {
        "fail"
    } else {
        status.as_str()
    };

    let tool = event
        .payload
        .get("tool")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let metric = event
        .payload
        .get("metric")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("signal");

    let detail = event
        .payload
        .get("detail")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            event
                .payload
                .get("outcome")
                .and_then(|outcome| outcome.get("detail"))
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
        });

    if let Some(detail) = detail {
        let normalized = detail.to_ascii_lowercase();
        if normalized.contains("timed out") || normalized.contains("timeout") {
            return format!("{status}:{tool}:{metric}:timeout");
        }
        if normalized.contains("exit code")
            || normalized.contains("exited with code")
            || normalized.contains("nonzero")
        {
            return format!("{status}:{tool}:{metric}:nonzero_exit");
        }
        if normalized.contains("execution error") || normalized.contains("permission") {
            return format!("{status}:{tool}:{metric}:exec_error");
        }
    }

    format!("{status}:{tool}:{metric}")
}

fn is_backend_task_health_failure(event: &Event) -> bool {
    if event.event_type != "run.observer.task_health" {
        return false;
    }
    let status = event
        .payload
        .get("status")
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    matches!(status.as_str(), "fail" | "failed" | "error")
}

pub(crate) fn build_memory_observer(
    loaded_config: &LoadedConfig,
    run_id: Uuid,
    correlation_id: Uuid,
    plan: &RunPlan,
    task_names: &[(Uuid, String)],
) -> Result<Option<MemoryObserver>> {
    let memory_cfg = &loaded_config.config().memory;
    let Some(provider) = memory_cfg.resolve_provider()? else {
        return Ok(None);
    };

    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    let prompt_root = prompt::find_prompt_upwards(cwd.clone())
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or(cwd);

    let project_id = memory_cfg
        .project_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            prompt_root
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "default".to_string());

    let project_dir = provider
        .project_dir
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .map(|raw| {
            let path = PathBuf::from(raw);
            if path.is_absolute() {
                path
            } else {
                prompt_root.join(path)
            }
        })
        .unwrap_or_else(|| prompt_root.clone());

    let adapter = match provider.kind {
        MemoryProviderKind::Cli => MemoryCliAdapter::new(provider.command, project_dir),
    };

    let task_keys = plan.tasks.iter().map(|t| t.task_key.clone()).collect();
    let audit_sink = crate::config::prepare_audit_sink(loaded_config)?;

    Ok(Some(MemoryObserver::new(
        project_id,
        run_id,
        correlation_id,
        plan.objective.clone(),
        adapter,
        provider.query_limit,
        provider.inject_on_run_start,
        provider.inject_on_failure,
        task_keys,
        task_names,
        audit_sink,
    )))
}

pub(crate) fn build_task_health_observer(
    run_id: Uuid,
    correlation_id: Uuid,
    task_names: &[(Uuid, String)],
) -> Option<TaskHealthArtifactObserver> {
    (!task_names.is_empty()).then_some(TaskHealthArtifactObserver::new(
        run_id,
        correlation_id,
        task_names,
    ))
}

fn build_tranche_token_correlation_details(
    payload: &yarli_cli::yarli_core::entities::ContinuationPayload,
) -> Option<serde_json::Value> {
    use yarli_cli::yarli_core::entities::continuation::TrancheTokenAdvisoryLevel;

    if payload.tranche_token_usage.is_empty() {
        return None;
    }

    let high_cost_tranches = payload
        .tranche_token_usage
        .iter()
        .filter(|tranche| {
            matches!(
                tranche.advisory,
                Some(TrancheTokenAdvisoryLevel::Warning | TrancheTokenAdvisoryLevel::Exceeded)
            )
        })
        .count();
    let failed_tranches = payload
        .tranche_token_usage
        .iter()
        .filter(|tranche| tranche.failed_tasks > 0)
        .count();
    let retried_tranches = payload
        .tranche_token_usage
        .iter()
        .filter(|tranche| tranche.retried_tasks > 0)
        .count();
    let high_cost_failed_tranches = payload
        .tranche_token_usage
        .iter()
        .filter(|tranche| {
            tranche.failed_tasks > 0
                && matches!(
                    tranche.advisory,
                    Some(TrancheTokenAdvisoryLevel::Warning | TrancheTokenAdvisoryLevel::Exceeded)
                )
        })
        .count();
    let high_cost_retried_tranches = payload
        .tranche_token_usage
        .iter()
        .filter(|tranche| {
            tranche.retried_tasks > 0
                && matches!(
                    tranche.advisory,
                    Some(TrancheTokenAdvisoryLevel::Warning | TrancheTokenAdvisoryLevel::Exceeded)
                )
        })
        .count();

    Some(serde_json::json!({
        "summary": {
            "high_cost_tranches": high_cost_tranches,
            "failed_tranches": failed_tranches,
            "retried_tranches": retried_tranches,
            "high_cost_failed_tranches": high_cost_failed_tranches,
            "high_cost_retried_tranches": high_cost_retried_tranches,
            "continuation_suggested": payload.next_tranche.is_some(),
        },
        "thresholds": payload.tranche_token_thresholds,
        "retry_recommendation": payload.retry_recommendation,
        "next_tranche_kind": payload.next_tranche.as_ref().map(|tranche| format!("{:?}", tranche.kind)),
        "tranches": payload.tranche_token_usage.iter().map(|tranche| serde_json::json!({
            "tranche_key": tranche.tranche_key,
            "tranche_group": tranche.tranche_group,
            "task_count": tranche.task_count,
            "completed_tasks": tranche.completed_tasks,
            "failed_tasks": tranche.failed_tasks,
            "retried_tasks": tranche.retried_tasks,
            "prompt_tokens": tranche.prompt_tokens,
            "completion_tokens": tranche.completion_tokens,
            "total_tokens": tranche.total_tokens,
            "rehydration_tokens": tranche.rehydration_tokens,
            "advisory": tranche.advisory,
            "warning": tranche.warning,
        })).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{make_command_started, make_command_terminal, make_event};
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use uuid::Uuid;
    use yarli_cli::yarli_core::domain::EntityType;
    use yarli_cli::yarli_core::explain::DeteriorationTrend;
    use yarli_cli::yarli_store::event_store::EventQuery;
    use yarli_cli::yarli_store::InMemoryEventStore;

    #[test]
    fn deterioration_scoring_distinguishes_stable_vs_deteriorating_trails() {
        let corr = Uuid::now_v7();
        let mut stable = DeteriorationObserverState::new(64);
        let mut stable_events = Vec::new();
        for _ in 0..8 {
            let command_id = Uuid::now_v7();
            stable_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            stable_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                120,
                Some(0),
            ));
        }
        assert!(stable.ingest(&stable_events));
        let stable_report = stable.report();
        assert!(
            stable_report.score < 25.0,
            "stable score={}",
            stable_report.score
        );

        let mut degrading = DeteriorationObserverState::new(64);
        let mut baseline_events = Vec::new();
        for _ in 0..6 {
            let command_id = Uuid::now_v7();
            baseline_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            baseline_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                100,
                Some(0),
            ));
        }
        degrading.ingest(&baseline_events);
        let baseline_report = degrading.report();

        let mut degrade_events = Vec::new();
        for i in 0..6 {
            let command_id = Uuid::now_v7();
            degrade_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            degrade_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                400 + (i * 250),
                Some(1),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.retrying",
                corr,
                serde_json::json!({"attempt_no": 2}),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.blocked",
                corr,
                serde_json::json!({"reason": "policy_denial"}),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "reason": if i % 2 == 0 { "budget_exceeded" } else { "nonzero_exit" },
                    "observed": 120.0 + (i as f64 * 10.0),
                    "limit": 100.0,
                }),
            ));
        }
        assert!(degrading.ingest(&degrade_events));
        let degrading_report = degrading.report();

        assert!(degrading_report.score > baseline_report.score);
        assert_eq!(degrading_report.trend, DeteriorationTrend::Deteriorating);
    }

    #[test]
    fn deterioration_observer_emits_incremental_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let command_id = Uuid::now_v7();

        store
            .append(make_command_started(command_id, corr, "cargo test", "io"))
            .unwrap();
        store
            .append(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                100,
                Some(0),
            ))
            .unwrap();

        let mut observer = DeteriorationObserver::new(run_id, corr, 32);
        observer.observe_store(&store).unwrap();
        observer.observe_store(&store).unwrap();

        let observer_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(
            observer_events
                .iter()
                .filter(|event| event.event_type == "run.observer.deterioration")
                .count(),
            1
        );

        store
            .append(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.retrying",
                corr,
                serde_json::json!({"attempt_no": 2}),
            ))
            .unwrap();
        observer.observe_store(&store).unwrap();
        let observer_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(
            observer_events
                .iter()
                .filter(|event| event.event_type == "run.observer.deterioration")
                .count(),
            2
        );
    }

    #[test]
    fn parse_tool_health_observation_reads_nested_tool_payload() {
        let direct = serde_json::json!({
            "tool": "runner",
            "status": "pass",
            "outcome": {
                "score": 0.77,
                "detail": "nested parsed from data payload",
            },
        });
        let candidates = expand_tool_outcome_payload_candidates(&serde_json::json!({
            "data": direct.to_string(),
            "seq": 1,
            "stream": "stdout",
        }));
        let observation = candidates
            .into_iter()
            .find_map(|candidate| parse_tool_health_observation(&candidate))
            .expect("tool outcome should parse");

        assert_eq!(observation.tool, "runner");
        assert_eq!(observation.status, "pass");
        assert_eq!(observation.score, Some(0.77));
        assert_eq!(
            observation.detail.as_deref(),
            Some("nested parsed from data payload")
        );
    }

    #[test]
    fn normalize_error_signature_handles_backend_task_health() {
        let corr = Uuid::now_v7();
        let task_id = Uuid::new_v4();

        let fail_signature = normalize_error_signature(&make_event(
            EntityType::Run,
            task_id.to_string(),
            "run.observer.task_health",
            corr,
            serde_json::json!({
                "tool": "lint",
                "status": "failed",
                "metric": "quality",
                "detail": "command exited with code 127",
            }),
        ));
        assert_eq!(fail_signature, "fail:lint:quality:nonzero_exit");

        let timeout_signature = normalize_error_signature(&make_event(
            EntityType::Run,
            task_id.to_string(),
            "run.observer.task_health",
            corr,
            serde_json::json!({
                "tool": "test",
                "status": "error",
                "metric": "coverage",
                "detail": "timed out waiting for response",
            }),
        ));
        assert_eq!(timeout_signature, "fail:test:coverage:timeout");
    }

    #[test]
    fn deterioration_detected_for_repeated_backend_failure_signatures() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        for _ in 0..3 {
            store
                .append(make_event(
                    EntityType::Run,
                    run_id.to_string(),
                    "run.observer.task_health",
                    corr,
                    serde_json::json!({
                        "tool": "lint",
                        "status": "fail",
                        "metric": "quality",
                        "detail": "command exited with code 2",
                    }),
                ))
                .unwrap();
        }

        let mut observer = DeteriorationObserver::new(run_id, corr, 64);
        observer.observe_store(&store).unwrap();

        let events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let detected = events
            .iter()
            .filter(|event| event.event_type == "run.observer.deterioration_detected")
            .collect::<Vec<_>>();

        assert_eq!(detected.len(), 1);
        let payload = &detected[0].payload;
        assert_eq!(
            payload.get("signal").and_then(Value::as_str),
            Some("backend_failure_repeated_signature")
        );
        assert_eq!(
            payload.get("signature_count").and_then(Value::as_u64),
            Some(3)
        );
        assert_eq!(
            payload
                .get("signatures")
                .and_then(Value::as_array)
                .and_then(|values| values.first())
                .and_then(Value::as_str),
            Some("fail:lint:quality:nonzero_exit")
        );
    }

    #[test]
    fn deterioration_detected_for_alternating_backend_failure_signatures() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        for i in 0..4 {
            let (tool, detail) = if i % 2 == 0 {
                ("lint", "command exited with code 2")
            } else {
                ("test", "timed out waiting for resource")
            };
            store
                .append(make_event(
                    EntityType::Run,
                    run_id.to_string(),
                    "run.observer.task_health",
                    corr,
                    serde_json::json!({
                        "tool": tool,
                        "status": "fail",
                        "metric": "quality",
                        "detail": detail,
                    }),
                ))
                .unwrap();
        }

        let mut observer = DeteriorationObserver::new(run_id, corr, 64);
        observer.observe_store(&store).unwrap();

        let events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let detected = events
            .iter()
            .filter(|event| event.event_type == "run.observer.deterioration_detected")
            .collect::<Vec<_>>();

        assert_eq!(detected.len(), 1);
        let payload = &detected[0].payload;
        assert_eq!(
            payload.get("signal").and_then(Value::as_str),
            Some("backend_failure_alternating_cycle")
        );
        assert_eq!(payload.get("repeat_count").and_then(Value::as_u64), Some(2));
        let signatures = payload
            .get("signatures")
            .and_then(Value::as_array)
            .expect("alternating signatures emitted")
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            signatures,
            vec![
                "fail:lint:quality:nonzero_exit",
                "fail:test:quality:timeout"
            ]
        );
        assert_eq!(
            payload.get("signature_count").and_then(Value::as_u64),
            Some(4)
        );
    }

    #[test]
    fn task_health_observer_reads_task_output_artifact_incrementally() {
        let run_id = Uuid::now_v7();
        let correlation_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let task_names = vec![(task_id, "task-1".to_string())];
        let artifact_path = PathBuf::from(".yarl/runs").join(format!("{task_id}.jsonl"));
        let _ = fs::remove_file(&artifact_path);
        fs::create_dir_all(".yarl/runs").unwrap();

        let first_command_id = Uuid::now_v7();
        let second_command_id = Uuid::now_v7();
        {
            let mut artifact = File::create(&artifact_path).unwrap();
            writeln!(
                &mut artifact,
                "{}",
                serde_json::json!({
                    "run_id": run_id,
                    "task_id": task_id,
                    "artifact_type": "command_output",
                })
            )
            .unwrap();
            let first_payload = serde_json::json!({
                "tool": "lint",
                "status": "pass",
                "score": 0.99,
                "metric": "quality",
                "detail": "first check",
            });
            writeln!(
                &mut artifact,
                "{}",
                serde_json::json!({
                    "command_id": first_command_id,
                    "seq": 1,
                    "stream": "stdout",
                    "data": first_payload.to_string(),
                    "captured_at": "2026-02-22T10:00:00Z",
                })
            )
            .unwrap();
        }

        let store = InMemoryEventStore::new();
        let mut observer = TaskHealthArtifactObserver::new(run_id, correlation_id, &task_names);
        let emitted = observer.observe_store(&store);
        assert_eq!(emitted, 1);

        let events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].payload.get("tool").and_then(|v| v.as_str()),
            Some("lint")
        );
        assert_eq!(
            events[0].payload.get("status").and_then(|v| v.as_str()),
            Some("pass")
        );
        assert_eq!(
            events[0].causation_id,
            Some(first_command_id),
            "first line causation id is preserved"
        );
        assert_eq!(
            events[0]
                .payload
                .get("metric")
                .and_then(|value| value.as_str()),
            Some("quality")
        );

        let second_payload = serde_json::json!({
            "tool": "lint",
            "status": "fail",
            "score": 0.33,
            "metric": "quality",
            "detail": "second check",
        });
        {
            let mut artifact = OpenOptions::new()
                .append(true)
                .open(&artifact_path)
                .unwrap();
            writeln!(
                &mut artifact,
                "{}",
                serde_json::json!({
                    "command_id": second_command_id,
                    "seq": 2,
                    "stream": "stdout",
                    "data": second_payload.to_string(),
                    "captured_at": "2026-02-22T10:00:01Z",
                })
            )
            .unwrap();
        }

        let emitted = observer.observe_store(&store);
        assert_eq!(emitted, 1);
        let events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[1].payload.get("status").and_then(|v| v.as_str()),
            Some("fail")
        );
        assert_eq!(
            events[1].causation_id,
            Some(second_command_id),
            "append line causation id is preserved"
        );

        let _ = fs::remove_file(&artifact_path);
    }

    #[test]
    fn detect_deterioration_cycle_reports_patterns() {
        let mut history = VecDeque::new();
        history.push_back(DeteriorationTrend::Stable);
        history.push_back(DeteriorationTrend::Deteriorating);
        history.push_back(DeteriorationTrend::Stable);
        history.push_back(DeteriorationTrend::Deteriorating);

        let cycle = detect_deterioration_cycle(&history).expect("cycle expected");
        assert_eq!(cycle.repeat_count, 2);
        assert_eq!(
            cycle.pattern,
            vec![
                DeteriorationTrend::Stable,
                DeteriorationTrend::Deteriorating
            ]
        );
    }

    #[test]
    fn stable_trend_does_not_trigger_cycle() {
        let mut history = VecDeque::new();
        history.push_back(DeteriorationTrend::Stable);
        history.push_back(DeteriorationTrend::Stable);
        history.push_back(DeteriorationTrend::Stable);
        history.push_back(DeteriorationTrend::Stable);

        assert!(detect_deterioration_cycle(&history).is_none());
    }

    #[test]
    fn normalize_error_signature_handles_noisy_failure_details() {
        let corr = Uuid::now_v7();
        let task_id = Uuid::now_v7();

        let nonzero_exit = normalize_error_signature(&make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.failed",
            corr,
            serde_json::json!({
                "reason": "nonzero_exit",
                "detail": "command exited with code 1",
            }),
        ));
        assert_eq!(nonzero_exit, "nonzero_exit");

        let nonzero_exit_variant = normalize_error_signature(&make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.failed",
            corr,
            serde_json::json!({
                "reason": "nonzero_exit",
                "detail": "command exited with code 127",
            }),
        ));
        assert_eq!(nonzero_exit_variant, "nonzero_exit");

        let exec_error = normalize_error_signature(&make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.failed",
            corr,
            serde_json::json!({
                "reason": "exec_error",
                "detail": "execution error: permission denied",
            }),
        ));
        assert_eq!(exec_error, "exec_error");

        let budget_exceeded = normalize_error_signature(&make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.failed",
            corr,
            serde_json::json!({
                "reason": "budget_exceeded",
                "scope": "task",
                "metric": "total_tokens",
            }),
        ));
        assert_eq!(budget_exceeded, "budget_exceeded:task:total_tokens");
    }

    #[test]
    fn failure_signature_ring_is_bounded() {
        let mut state = DeteriorationObserverState::new(16);
        let corr = Uuid::now_v7();

        for i in 0..(OBSERVER_FAILURE_RING_SIZE + 6) {
            let event = make_event(
                EntityType::Task,
                Uuid::new_v4().to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "reason": "nonzero_exit",
                    "detail": format!("command exited with code {i}"),
                }),
            );
            let changed = state.ingest(std::slice::from_ref(&event));
            assert!(changed);
        }

        assert_eq!(state.failure_signatures.len(), OBSERVER_FAILURE_RING_SIZE);
    }

    // --- observe_run_end integration (run_analyzer path) ---

    use yarli_cli::yarli_core::entities::continuation::{
        ContinuationPayload, RetryScope, RunSummary, TrancheKind, TrancheSpec,
        TrancheTokenAdvisoryLevel, TrancheTokenThresholds, TrancheTokenUsageSummary,
    };
    use yarli_cli::yarli_core::fsm::run::RunState;
    use yarli_cli::yarli_core::fsm::task::TaskState;
    use yarli_cli::yarli_observability::run_analyzer;

    fn make_continuation_payload(
        exit_state: RunState,
        exit_reason: Option<yarli_cli::yarli_core::domain::ExitReason>,
        tasks: Vec<yarli_cli::yarli_core::entities::continuation::TaskOutcome>,
    ) -> ContinuationPayload {
        let total = tasks.len() as u32;
        let completed = tasks
            .iter()
            .filter(|t| t.state == TaskState::TaskComplete)
            .count() as u32;
        let failed = tasks
            .iter()
            .filter(|t| t.state == TaskState::TaskFailed)
            .count() as u32;
        ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "test objective".to_string(),
            exit_state,
            exit_reason,
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks,
            summary: RunSummary {
                total,
                completed,
                failed,
                cancelled: 0,
                pending: 0,
            },
            tranche_token_usage: Vec::new(),
            tranche_token_thresholds: None,
            next_tranche: None,
            quality_gate: None,
            retry_recommendation: None,
        }
    }

    fn make_task_outcome(
        key: &str,
        state: TaskState,
    ) -> yarli_cli::yarli_core::entities::continuation::TaskOutcome {
        yarli_cli::yarli_core::entities::continuation::TaskOutcome {
            task_id: Uuid::new_v4(),
            task_key: key.to_string(),
            state,
            attempt_no: 1,
            last_error: None,
            blocker: None,
        }
    }

    #[test]
    fn observe_run_end_analyzer_stores_lesson_on_failure() {
        let payload = make_continuation_payload(
            RunState::RunFailed,
            Some(yarli_cli::yarli_core::domain::ExitReason::BlockedOpenTasks),
            vec![make_task_outcome("build", TaskState::TaskComplete), {
                let mut t = make_task_outcome("test", TaskState::TaskFailed);
                t.last_error = Some("exit code 1".to_string());
                t
            }],
        );

        let analyzer_tasks: Vec<run_analyzer::TaskOutcome> = payload
            .tasks
            .iter()
            .map(|t| run_analyzer::TaskOutcome {
                task_key: t.task_key.clone(),
                state: t.state,
                last_error: t.last_error.clone(),
                blocker: t.blocker.clone(),
            })
            .collect();

        let analysis = run_analyzer::analyze_run(
            payload.exit_state,
            payload.exit_reason,
            &analyzer_tasks,
            &[],
        );
        assert!(
            analysis.run_lesson.is_some(),
            "should have a lesson for failures"
        );
        assert!(!analysis.patterns.is_empty());
    }

    #[test]
    fn observe_run_end_analyzer_skips_clean_completion() {
        let payload = make_continuation_payload(
            RunState::RunCompleted,
            Some(yarli_cli::yarli_core::domain::ExitReason::CompletedAllGates),
            vec![
                make_task_outcome("build", TaskState::TaskComplete),
                make_task_outcome("test", TaskState::TaskComplete),
            ],
        );

        let analyzer_tasks: Vec<run_analyzer::TaskOutcome> = payload
            .tasks
            .iter()
            .map(|t| run_analyzer::TaskOutcome {
                task_key: t.task_key.clone(),
                state: t.state,
                last_error: t.last_error.clone(),
                blocker: t.blocker.clone(),
            })
            .collect();

        let analysis = run_analyzer::analyze_run(
            payload.exit_state,
            payload.exit_reason,
            &analyzer_tasks,
            &[],
        );
        assert!(
            analysis.run_lesson.is_none(),
            "clean completion should not produce a lesson"
        );
        assert!(analysis.patterns.is_empty());
    }

    #[test]
    fn observe_run_end_analyzer_emits_analysis_event() {
        let payload = make_continuation_payload(
            RunState::RunFailed,
            Some(yarli_cli::yarli_core::domain::ExitReason::BlockedGateFailure),
            vec![
                make_task_outcome("build", TaskState::TaskComplete),
                make_task_outcome("test", TaskState::TaskComplete),
            ],
        );

        let analyzer_tasks: Vec<run_analyzer::TaskOutcome> = payload
            .tasks
            .iter()
            .map(|t| run_analyzer::TaskOutcome {
                task_key: t.task_key.clone(),
                state: t.state,
                last_error: t.last_error.clone(),
                blocker: t.blocker.clone(),
            })
            .collect();

        let gate_failures = vec!["tests_passed".to_string()];
        let analysis = run_analyzer::analyze_run(
            payload.exit_state,
            payload.exit_reason,
            &analyzer_tasks,
            &gate_failures,
        );
        let pattern_names = run_analyzer::pattern_names(&analysis.patterns);

        // Verify analysis event payload can be serialized
        let event_payload = serde_json::to_value(&analysis).unwrap();
        assert!(event_payload.get("patterns").is_some());
        assert!(event_payload.get("confidence").is_some());
        assert!(!pattern_names.is_empty());
        assert!(pattern_names.iter().any(|n| n == "gate_only_failure"));
    }

    #[test]
    fn observe_run_end_analyzer_includes_gate_failures() {
        let payload = make_continuation_payload(
            RunState::RunFailed,
            Some(yarli_cli::yarli_core::domain::ExitReason::BlockedGateFailure),
            vec![make_task_outcome("build", TaskState::TaskComplete)],
        );

        let analyzer_tasks: Vec<run_analyzer::TaskOutcome> = payload
            .tasks
            .iter()
            .map(|t| run_analyzer::TaskOutcome {
                task_key: t.task_key.clone(),
                state: t.state,
                last_error: t.last_error.clone(),
                blocker: t.blocker.clone(),
            })
            .collect();

        let gate_failures = vec!["tests_passed".to_string(), "lint_clean".to_string()];
        let analysis = run_analyzer::analyze_run(
            payload.exit_state,
            payload.exit_reason,
            &analyzer_tasks,
            &gate_failures,
        );
        let lesson = analysis.run_lesson.expect("should have a lesson");
        assert!(
            lesson.contains("tests_passed"),
            "lesson should mention gate failures"
        );
        assert!(
            lesson.contains("lint_clean"),
            "lesson should mention all gate failures"
        );
    }

    #[test]
    fn build_tranche_token_correlation_details_summarizes_high_cost_failures_and_retries() {
        let mut payload = make_continuation_payload(
            RunState::RunFailed,
            Some(yarli_cli::yarli_core::domain::ExitReason::BlockedOpenTasks),
            vec![
                make_task_outcome("build", TaskState::TaskComplete),
                make_task_outcome("test", TaskState::TaskFailed),
            ],
        );
        payload.retry_recommendation = Some(RetryScope::Subset {
            keys: vec!["test".to_string()],
        });
        payload.tranche_token_thresholds = Some(TrancheTokenThresholds {
            selector: Some("codex".to_string()),
            target_tokens: 70_000,
            max_recommended_tokens: 100_000,
        });
        payload.tranche_token_usage = vec![
            TrancheTokenUsageSummary {
                tranche_key: "NXT-042".to_string(),
                tranche_group: Some("next-todos".to_string()),
                task_count: 2,
                completed_tasks: 1,
                failed_tasks: 1,
                retried_tasks: 1,
                prompt_tokens: 52_000,
                completion_tokens: 28_000,
                total_tokens: 80_000,
                rehydration_tokens: Some(12_000),
                advisory: Some(TrancheTokenAdvisoryLevel::Warning),
                warning: Some("above advisory tranche warning".to_string()),
            },
            TrancheTokenUsageSummary {
                tranche_key: "NXT-043".to_string(),
                tranche_group: Some("next-todos".to_string()),
                task_count: 1,
                completed_tasks: 1,
                failed_tasks: 0,
                retried_tasks: 0,
                prompt_tokens: 10_000,
                completion_tokens: 8_000,
                total_tokens: 18_000,
                rehydration_tokens: Some(0),
                advisory: Some(TrancheTokenAdvisoryLevel::Healthy),
                warning: None,
            },
        ];
        payload.next_tranche = Some(TrancheSpec {
            suggested_objective: "Retry failed tasks: test".to_string(),
            kind: TrancheKind::RetryUnfinished,
            retry_task_keys: vec!["test".to_string()],
            unfinished_task_keys: Vec::new(),
            planned_task_keys: Vec::new(),
            planned_tranche_key: None,
            cursor: None,
            config_snapshot: serde_json::json!({}),
            interventions: Vec::new(),
        });

        let details = build_tranche_token_correlation_details(&payload)
            .expect("tranche token correlation details should exist");

        assert_eq!(details["summary"]["high_cost_tranches"], 1);
        assert_eq!(details["summary"]["failed_tranches"], 1);
        assert_eq!(details["summary"]["retried_tranches"], 1);
        assert_eq!(details["summary"]["high_cost_failed_tranches"], 1);
        assert_eq!(details["summary"]["high_cost_retried_tranches"], 1);
        assert_eq!(details["summary"]["continuation_suggested"], true);
        assert_eq!(details["retry_recommendation"]["scope"], "subset");
        assert_eq!(details["retry_recommendation"]["keys"][0], "test");
        assert_eq!(details["next_tranche_kind"], "RetryUnfinished");
        assert_eq!(details["tranches"][0]["tranche_key"], "NXT-042");
        assert_eq!(
            details["tranches"][0]["warning"],
            "above advisory tranche warning"
        );
    }
}
