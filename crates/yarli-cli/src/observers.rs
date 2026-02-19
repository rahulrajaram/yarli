use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use yarli_core::domain::Event;
use yarli_core::explain::{DeteriorationFactor, DeteriorationReport, DeteriorationTrend};
use yarli_memory::{
    MemoryCliAdapter, InsertMemory, MemoryAdapter, MemoryClass, MemoryQuery, ScopeId,
};
use yarli_store::event_store::EventQuery;
use yarli_store::EventStore;

use crate::config::LoadedConfig;
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

#[derive(Debug, Clone)]
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
                        entity_type: yarli_core::domain::EntityType::Run,
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
                        entity_type: yarli_core::domain::EntityType::Run,
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
                        entity_type: yarli_core::domain::EntityType::Task,
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
                        entity_type: yarli_core::domain::EntityType::Task,
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
                        entity_type: yarli_core::domain::EntityType::Task,
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
                        entity_type: yarli_core::domain::EntityType::Task,
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

#[derive(Debug, Clone)]
pub(crate) struct DeteriorationObserver {
    run_id: Uuid,
    correlation_id: Uuid,
    cursor: IncrementalEventCursor,
    state: DeteriorationObserverState,
    latest_report: Option<DeteriorationReport>,
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
        append_event(
            store,
            Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: yarli_core::domain::EntityType::Run,
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
    Failure { reason_bucket: String },
    Budget(BudgetSample),
}

#[derive(Debug, Clone)]
pub(crate) struct DeteriorationObserverState {
    window_size: usize,
    signals: VecDeque<ObserverSignal>,
    pending_commands: HashMap<String, String>,
    previous_score: Option<f64>,
}

impl DeteriorationObserverState {
    pub(crate) fn new(window_size: usize) -> Self {
        Self {
            window_size,
            signals: VecDeque::new(),
            pending_commands: HashMap::new(),
            previous_score: None,
        }
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
                    let bucket = event
                        .payload
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    self.push_signal(ObserverSignal::Failure {
                        reason_bucket: bucket,
                    });
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
        let failures: Vec<&str> = self
            .signals
            .iter()
            .filter_map(|signal| {
                if let ObserverSignal::Failure { reason_bucket } = signal {
                    Some(reason_bucket.as_str())
                } else {
                    None
                }
            })
            .collect();
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
}

pub(crate) fn build_memory_observer(
    loaded_config: &LoadedConfig,
    run_id: Uuid,
    correlation_id: Uuid,
    plan: &RunPlan,
    task_names: &[(Uuid, String)],
) -> Result<Option<MemoryObserver>> {
    let memory_cfg = &loaded_config.config().memory;
    let mem = &memory_cfg.backend;
    let enabled = memory_cfg.enabled.unwrap_or(mem.enabled);
    if !enabled || !mem.enabled {
        return Ok(None);
    }

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

    let project_dir = mem
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

    let adapter = MemoryCliAdapter::new(mem.command.clone(), project_dir);

    let task_keys = plan.tasks.iter().map(|t| t.task_key.clone()).collect();

    Ok(Some(MemoryObserver::new(
        project_id,
        run_id,
        correlation_id,
        plan.objective.clone(),
        adapter,
        mem.query_limit,
        mem.inject_on_run_start,
        mem.inject_on_failure,
        task_keys,
        task_names,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{make_command_started, make_command_terminal, make_event};
    use uuid::Uuid;
    use yarli_core::domain::EntityType;
    use yarli_core::explain::DeteriorationTrend;
    use yarli_store::event_store::EventQuery;
    use yarli_store::InMemoryEventStore;

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
}
