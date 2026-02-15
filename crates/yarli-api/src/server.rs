use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;
use yarli_core::domain::{EntityType, Event};
use yarli_core::explain::DeteriorationReport;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_store::event_store::EventQuery;
use yarli_store::{EventStore, StoreError};

#[derive(Clone)]
pub struct ApiState {
    store: Arc<dyn EventStore>,
}

impl ApiState {
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        Self { store }
    }
}

#[derive(Debug, Error)]
pub enum ApiServerError {
    #[error("server failed: {0}")]
    Serve(std::io::Error),
}

pub fn router(store: Arc<dyn EventStore>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/runs/:run_id/status", get(run_status))
        .route("/v1/tasks/:task_id", get(task_status))
        .with_state(ApiState::new(store))
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    store: Arc<dyn EventStore>,
) -> Result<(), ApiServerError> {
    axum::serve(listener, router(store))
        .await
        .map_err(ApiServerError::Serve)
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    status: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

#[derive(Debug, Serialize)]
pub struct RunStatusResponse {
    run_id: Uuid,
    state: String,
    last_event_type: String,
    updated_at: DateTime<Utc>,
    correlation_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    objective: Option<String>,
    /// Latest rolling sequence-deterioration report (if emitted by the observer).
    #[serde(skip_serializing_if = "Option::is_none")]
    deterioration: Option<DeteriorationReport>,
    task_summary: TaskStatusSummary,
}

#[derive(Debug, Serialize)]
pub struct TaskStatusResponse {
    task_id: Uuid,
    state: String,
    last_event_type: String,
    updated_at: DateTime<Utc>,
    correlation_id: Uuid,
}

#[derive(Debug, Default, Serialize)]
pub struct TaskStatusSummary {
    total: usize,
    open: usize,
    ready: usize,
    executing: usize,
    waiting: usize,
    blocked: usize,
    verifying: usize,
    complete: usize,
    failed: usize,
    cancelled: usize,
}

async fn run_status(
    Path(run_id): Path<String>,
    State(state): State<ApiState>,
) -> Result<Json<RunStatusResponse>, ApiError> {
    let run_id = run_id.parse::<Uuid>().map_err(|_| ApiError::InvalidRunId)?;
    let status =
        load_run_status(state.store.as_ref(), run_id)?.ok_or(ApiError::RunNotFound(run_id))?;
    Ok(Json(status))
}

async fn task_status(
    Path(task_id): Path<String>,
    State(state): State<ApiState>,
) -> Result<Json<TaskStatusResponse>, ApiError> {
    let task_id = task_id
        .parse::<Uuid>()
        .map_err(|_| ApiError::InvalidTaskId)?;
    let status =
        load_task_status(state.store.as_ref(), task_id)?.ok_or(ApiError::TaskNotFound(task_id))?;
    Ok(Json(status))
}

fn load_run_status(
    store: &dyn EventStore,
    run_id: Uuid,
) -> Result<Option<RunStatusResponse>, ApiError> {
    let run_events = store
        .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
        .map_err(ApiError::Store)?;
    if run_events.is_empty() {
        return Ok(None);
    }

    let mut state = RunState::RunOpen;
    let mut objective = None;
    let mut correlation_id = run_events[0].correlation_id;
    let mut updated_at = run_events[0].occurred_at;
    let mut last_event_type = run_events[0].event_type.clone();
    let mut deterioration: Option<DeteriorationReport> = None;

    for event in &run_events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = run_state_from_event(event) {
            state = next_state;
        }

        if event.event_type == "run.config_snapshot" {
            objective = event
                .payload
                .get("objective")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string());
        }

        // Observer output: best-effort parse; does not affect status computation.
        if event.event_type == "run.observer.deterioration" {
            if let Ok(report) = serde_json::from_value::<DeteriorationReport>(event.payload.clone())
            {
                deterioration = Some(report);
            }
        }
    }

    let task_summary = summarize_tasks(store, correlation_id)?;
    Ok(Some(RunStatusResponse {
        run_id,
        state: format!("{state:?}"),
        last_event_type,
        updated_at,
        correlation_id,
        objective,
        deterioration,
        task_summary,
    }))
}

fn load_task_status(
    store: &dyn EventStore,
    task_id: Uuid,
) -> Result<Option<TaskStatusResponse>, ApiError> {
    let task_events = store
        .query(&EventQuery::by_entity(
            EntityType::Task,
            task_id.to_string(),
        ))
        .map_err(ApiError::Store)?;
    if task_events.is_empty() {
        return Ok(None);
    }

    let mut state = TaskState::TaskOpen;
    let mut updated_at = task_events[0].occurred_at;
    let mut last_event_type = task_events[0].event_type.clone();
    let mut correlation_id = task_events[0].correlation_id;

    for event in &task_events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = task_state_from_event(event) {
            state = next_state;
        }
    }

    let run_events = store
        .query(&EventQuery::by_correlation(correlation_id))
        .map_err(ApiError::Store)?;
    if run_events
        .iter()
        .all(|event| event.entity_type != EntityType::Run)
    {
        return Err(ApiError::CorrelatedRunMissing(task_id));
    }

    Ok(Some(TaskStatusResponse {
        task_id,
        state: format!("{state:?}"),
        last_event_type,
        updated_at,
        correlation_id,
    }))
}

fn summarize_tasks(
    store: &dyn EventStore,
    correlation_id: Uuid,
) -> Result<TaskStatusSummary, ApiError> {
    let events = store
        .query(&EventQuery::by_correlation(correlation_id))
        .map_err(ApiError::Store)?;

    let mut states: BTreeMap<Uuid, TaskState> = BTreeMap::new();
    for event in events
        .iter()
        .filter(|event| event.entity_type == EntityType::Task)
    {
        let task_id = match event.entity_id.parse::<Uuid>() {
            Ok(task_id) => task_id,
            Err(_) => continue,
        };

        let next_state = task_state_from_event(event).unwrap_or(TaskState::TaskOpen);
        states.insert(task_id, next_state);
    }

    let mut summary = TaskStatusSummary::default();
    summary.total = states.len();
    for state in states.values() {
        match state {
            TaskState::TaskOpen => summary.open += 1,
            TaskState::TaskReady => summary.ready += 1,
            TaskState::TaskExecuting => summary.executing += 1,
            TaskState::TaskWaiting => summary.waiting += 1,
            TaskState::TaskBlocked => summary.blocked += 1,
            TaskState::TaskVerifying => summary.verifying += 1,
            TaskState::TaskComplete => summary.complete += 1,
            TaskState::TaskFailed => summary.failed += 1,
            TaskState::TaskCancelled => summary.cancelled += 1,
        }
    }

    Ok(summary)
}

fn run_state_from_event(event: &Event) -> Option<RunState> {
    event
        .payload
        .get("to")
        .and_then(|value| value.as_str())
        .and_then(parse_run_state)
        .or_else(|| match event.event_type.as_str() {
            "run.activated" => Some(RunState::RunActive),
            "run.verifying" => Some(RunState::RunVerifying),
            "run.completed" => Some(RunState::RunCompleted),
            "run.failed" | "run.gate_failed" => Some(RunState::RunFailed),
            "run.cancelled" => Some(RunState::RunCancelled),
            _ => None,
        })
}

fn task_state_from_event(event: &Event) -> Option<TaskState> {
    event
        .payload
        .get("to")
        .and_then(|value| value.as_str())
        .and_then(parse_task_state)
        .or_else(|| match event.event_type.as_str() {
            "task.ready" | "task.retrying" | "task.unblocked" => Some(TaskState::TaskReady),
            "task.executing" => Some(TaskState::TaskExecuting),
            "task.verifying" => Some(TaskState::TaskVerifying),
            "task.completed" => Some(TaskState::TaskComplete),
            "task.failed" | "task.gate_failed" => Some(TaskState::TaskFailed),
            "task.blocked" => Some(TaskState::TaskBlocked),
            "task.cancelled" => Some(TaskState::TaskCancelled),
            _ => None,
        })
}

fn parse_run_state(value: &str) -> Option<RunState> {
    match value {
        "RunOpen" => Some(RunState::RunOpen),
        "RunActive" => Some(RunState::RunActive),
        "RunBlocked" => Some(RunState::RunBlocked),
        "RunVerifying" => Some(RunState::RunVerifying),
        "RunCompleted" => Some(RunState::RunCompleted),
        "RunFailed" => Some(RunState::RunFailed),
        "RunCancelled" => Some(RunState::RunCancelled),
        _ => None,
    }
}

fn parse_task_state(value: &str) -> Option<TaskState> {
    match value {
        "TaskOpen" => Some(TaskState::TaskOpen),
        "TaskReady" => Some(TaskState::TaskReady),
        "TaskExecuting" => Some(TaskState::TaskExecuting),
        "TaskWaiting" => Some(TaskState::TaskWaiting),
        "TaskBlocked" => Some(TaskState::TaskBlocked),
        "TaskVerifying" => Some(TaskState::TaskVerifying),
        "TaskComplete" => Some(TaskState::TaskComplete),
        "TaskFailed" => Some(TaskState::TaskFailed),
        "TaskCancelled" => Some(TaskState::TaskCancelled),
        _ => None,
    }
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("invalid run ID (expected UUID)")]
    InvalidRunId,
    #[error("invalid task ID (expected UUID)")]
    InvalidTaskId,
    #[error("run {0} not found")]
    RunNotFound(Uuid),
    #[error("task {0} not found")]
    TaskNotFound(Uuid),
    #[error("task {0} has no correlated run events")]
    CorrelatedRunMissing(Uuid),
    #[error("failed to read persisted state")]
    Store(StoreError),
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::InvalidRunId => (StatusCode::BAD_REQUEST, self.to_string()),
            ApiError::InvalidTaskId => (StatusCode::BAD_REQUEST, self.to_string()),
            ApiError::RunNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            ApiError::TaskNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            ApiError::CorrelatedRunMissing(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
            ApiError::Store(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        (status, Json(ErrorResponse { error: message })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use chrono::Duration;
    use serde_json::json;
    use tower::ServiceExt;
    use yarli_store::InMemoryEventStore;

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload, json!({"status":"ok"}));
    }

    #[tokio::test]
    async fn run_status_endpoint_replays_persisted_events() {
        let store = Arc::new(InMemoryEventStore::new());
        let run_id = Uuid::now_v7();
        let correlation_id = Uuid::now_v7();
        let task_complete = Uuid::now_v7();
        let task_failed = Uuid::now_v7();
        let now = Utc::now();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                correlation_id,
                now,
                json!({"objective":"ship API"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                correlation_id,
                now + Duration::seconds(1),
                json!({"to":"RunActive"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_complete.to_string(),
                "task.completed",
                correlation_id,
                now + Duration::seconds(2),
                json!({"to":"TaskComplete"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_failed.to_string(),
                "task.failed",
                correlation_id,
                now + Duration::seconds(3),
                json!({"to":"TaskFailed"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.deterioration",
                correlation_id,
                now + Duration::seconds(4),
                json!({
                    "score": 72.5,
                    "window_size": 32,
                    "factors": [{"name":"runtime_drift","impact":0.8,"detail":"runtime trend"}],
                    "trend": "deteriorating"
                }),
            ))
            .unwrap();

        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/runs/{run_id}/status"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["run_id"], json!(run_id));
        assert_eq!(payload["state"], json!("RunActive"));
        assert_eq!(payload["objective"], json!("ship API"));
        assert_eq!(payload["task_summary"]["total"], json!(2));
        assert_eq!(payload["task_summary"]["complete"], json!(1));
        assert_eq!(payload["task_summary"]["failed"], json!(1));
        assert_eq!(payload["deterioration"]["score"], json!(72.5));
        assert_eq!(payload["deterioration"]["trend"], json!("deteriorating"));
    }

    #[tokio::test]
    async fn run_status_endpoint_returns_not_found_for_unknown_run() {
        let run_id = Uuid::now_v7();
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/runs/{run_id}/status"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn task_status_endpoint_replays_persisted_task_events() {
        let store = Arc::new(InMemoryEventStore::new());
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let correlation_id = Uuid::now_v7();
        let now = Utc::now();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                correlation_id,
                now,
                json!({"objective":"read task surface"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.ready",
                correlation_id,
                now + Duration::seconds(1),
                json!({"to":"TaskReady"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                correlation_id,
                now + Duration::seconds(2),
                json!({"to":"TaskComplete"}),
            ))
            .unwrap();

        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tasks/{task_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["task_id"], json!(task_id));
        assert_eq!(payload["state"], json!("TaskComplete"));
        assert_eq!(payload["last_event_type"], json!("task.completed"));
        assert_eq!(payload["correlation_id"], json!(correlation_id));
    }

    #[tokio::test]
    async fn task_status_endpoint_returns_not_found_for_unknown_task() {
        let task_id = Uuid::now_v7();
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tasks/{task_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn task_status_endpoint_rejects_invalid_task_id() {
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .uri("/v1/tasks/not-a-task-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["error"]
            .as_str()
            .unwrap()
            .contains("invalid task ID"));
    }

    fn make_event(
        entity_type: EntityType,
        entity_id: String,
        event_type: &str,
        correlation_id: Uuid,
        occurred_at: DateTime<Utc>,
        payload: serde_json::Value,
    ) -> Event {
        Event {
            event_id: Uuid::now_v7(),
            occurred_at,
            entity_type,
            entity_id,
            event_type: event_type.to_string(),
            payload,
            correlation_id,
            causation_id: None,
            actor: "test".to_string(),
            idempotency_key: None,
        }
    }
}
