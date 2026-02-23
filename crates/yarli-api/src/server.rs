use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration as StdDuration, Instant};

use axum::extract::{
    ws::{Message, WebSocket, WebSocketUpgrade},
    Extension, Path, Query, State,
};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures_util::SinkExt as _;
use prometheus_client::registry::Registry;
use reqwest::Client;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;
use yarli_core::domain::{EntityType, Event};
#[cfg(feature = "debug-api")]
use yarli_core::entities::command_execution::{CommandResourceUsage, TokenUsage};
use yarli_core::explain::DeteriorationReport;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_observability::{encode_metrics, YarliMetrics};
#[cfg(feature = "debug-api")]
use yarli_queue::QueueError;
#[cfg(feature = "debug-api")]
use yarli_queue::TaskQueue;
use yarli_store::event_store::EventQuery;
use yarli_store::{EventStore, StoreError};

const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const API_KEYS_ENV: &str = "YARLI_API_KEYS";
const API_RATE_LIMIT_RPM_ENV: &str = "YARLI_API_RATE_LIMIT_PER_MINUTE";
const API_BEARER_PREFIX: &str = "Bearer ";
const API_TOKEN_PREFIX: &str = "Token ";
const DEFAULT_RATE_LIMIT_RPM: usize = 120;
const API_RATE_LIMIT_WINDOW_SECONDS: u64 = 60;
const ANONYMOUS_API_ACTOR: &str = "api.anonymous";
const WEBHOOK_DISPATCH_INTERVAL_MS: u64 = 500;
const WEBHOOK_MAX_RETRY_ATTEMPTS: u8 = 5;
const WEBHOOK_INITIAL_RETRY_BACKOFF_MS: u64 = 250;
const WEBHOOK_BACKOFF_MAX_MS: u64 = 10_000;
const EVENT_STREAM_POLL_MS_DEFAULT: u64 = 250;
const EVENT_STREAM_BATCH_LIMIT: usize = 128;
const API_LIST_LIMIT_DEFAULT: usize = 50;
const API_LIST_LIMIT_MAX: usize = 200;

#[derive(Clone)]
pub struct ApiState {
    store: Arc<dyn EventStore>,
    metrics_registry: Arc<Registry>,
    webhooks: Arc<RwLock<Vec<WebhookRegistration>>>,
    webhook_dispatcher_started: Arc<AtomicBool>,
    webhook_last_seen_event_id: Arc<RwLock<Option<Uuid>>>,
    security: ApiSecurityConfig,
    rate_limits: Arc<RwLock<HashMap<String, ApiRateLimitBucket>>>,
    #[cfg(feature = "debug-api")]
    queue: Option<Arc<dyn TaskQueue>>,
}

impl ApiState {
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        let mut metrics_registry = Registry::default();
        let _ = YarliMetrics::new(&mut metrics_registry);
        Self {
            store,
            metrics_registry: Arc::new(metrics_registry),
            webhooks: Arc::new(RwLock::new(Vec::new())),
            webhook_dispatcher_started: Arc::new(AtomicBool::new(false)),
            webhook_last_seen_event_id: Arc::new(RwLock::new(None)),
            security: ApiSecurityConfig::from_environment(),
            rate_limits: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "debug-api")]
            queue: None,
        }
    }

    pub fn new_with_security(store: Arc<dyn EventStore>, security: ApiSecurityConfig) -> Self {
        let mut metrics_registry = Registry::default();
        let _ = YarliMetrics::new(&mut metrics_registry);
        Self {
            store,
            metrics_registry: Arc::new(metrics_registry),
            webhooks: Arc::new(RwLock::new(Vec::new())),
            webhook_dispatcher_started: Arc::new(AtomicBool::new(false)),
            webhook_last_seen_event_id: Arc::new(RwLock::new(None)),
            security,
            rate_limits: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "debug-api")]
            queue: None,
        }
    }

    #[cfg(feature = "debug-api")]
    pub fn new_with_queue(store: Arc<dyn EventStore>, queue: Arc<dyn TaskQueue>) -> Self {
        let mut metrics_registry = Registry::default();
        let _ = YarliMetrics::new(&mut metrics_registry);
        Self {
            store,
            metrics_registry: Arc::new(metrics_registry),
            webhooks: Arc::new(RwLock::new(Vec::new())),
            webhook_dispatcher_started: Arc::new(AtomicBool::new(false)),
            webhook_last_seen_event_id: Arc::new(RwLock::new(None)),
            security: ApiSecurityConfig::from_environment(),
            rate_limits: Arc::new(RwLock::new(HashMap::new())),
            queue: Some(queue),
        }
    }

    #[cfg(feature = "debug-api")]
    pub fn new_with_queue_and_security(
        store: Arc<dyn EventStore>,
        queue: Arc<dyn TaskQueue>,
        security: ApiSecurityConfig,
    ) -> Self {
        let mut metrics_registry = Registry::default();
        let _ = YarliMetrics::new(&mut metrics_registry);
        Self {
            store,
            metrics_registry: Arc::new(metrics_registry),
            webhooks: Arc::new(RwLock::new(Vec::new())),
            webhook_dispatcher_started: Arc::new(AtomicBool::new(false)),
            webhook_last_seen_event_id: Arc::new(RwLock::new(None)),
            security,
            rate_limits: Arc::new(RwLock::new(HashMap::new())),
            queue: Some(queue),
        }
    }
}

#[derive(Clone)]
struct ApiSecurityConfig {
    api_keys: Arc<Vec<String>>,
    require_api_key: bool,
    rate_limit_per_minute: usize,
}

impl ApiSecurityConfig {
    fn from_environment() -> Self {
        let api_keys = parse_api_keys(env::var(API_KEYS_ENV).ok());
        let require_api_key = !api_keys.is_empty();
        let rate_limit_per_minute = parse_rate_limit_per_minute();

        Self {
            api_keys: Arc::new(api_keys),
            require_api_key,
            rate_limit_per_minute,
        }
    }

    fn has_api_key(&self, key: &str) -> bool {
        self.api_keys.iter().any(|value| value == key)
    }

    fn requires_auth(&self) -> bool {
        self.require_api_key
    }
}

impl ApiSecurityConfig {
    #[cfg(test)]
    fn with_test_keys(api_keys: &[&str], rate_limit_per_minute: usize) -> Self {
        Self {
            api_keys: Arc::new(
                api_keys
                    .iter()
                    .filter_map(|value| {
                        let value = value.trim();
                        if value.is_empty() {
                            None
                        } else {
                            Some(value.to_string())
                        }
                    })
                    .collect(),
            ),
            require_api_key: true,
            rate_limit_per_minute,
        }
    }
}

#[derive(Debug, Default)]
struct ApiRateLimitBucket {
    window_start: Option<Instant>,
    request_count: usize,
}

impl ApiRateLimitBucket {
    fn next_request(&mut self, now: Instant, limit: usize, window: StdDuration) -> Option<u64> {
        let Some(window_start) = self.window_start else {
            self.window_start = Some(now);
            self.request_count = 1;
            return None;
        };

        if now.duration_since(window_start) >= window {
            self.window_start = Some(now);
            self.request_count = 1;
            return None;
        }

        if self.request_count >= limit {
            let elapsed = now.duration_since(window_start);
            let retry_after = window.saturating_sub(elapsed);
            return Some(retry_after.as_secs().max(1));
        }

        self.request_count += 1;
        None
    }
}

#[derive(Clone)]
struct ApiRequestContext {
    actor: String,
}

fn parse_api_keys(raw: Option<String>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };

    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_rate_limit_per_minute() -> usize {
    env::var(API_RATE_LIMIT_RPM_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_RATE_LIMIT_RPM)
}

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        if let Ok(raw) = value.to_str() {
            let normalized = raw.trim();
            if let Some(token) = normalized.strip_prefix(API_BEARER_PREFIX) {
                return Some(token.trim().to_string());
            }
            if let Some(token) = normalized.strip_prefix(API_TOKEN_PREFIX) {
                return Some(token.trim().to_string());
            }
            if !normalized.is_empty() {
                return Some(normalized.to_string());
            }
        }
    }

    headers
        .get(HeaderName::from_static("x-api-key"))
        .or_else(|| headers.get(HeaderName::from_static("api-key")))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn actor_from_api_key(api_key: Option<&str>) -> String {
    let Some(api_key) = api_key else {
        return ANONYMOUS_API_ACTOR.to_string();
    };

    let mut suffix_chars = api_key.chars().rev().take(6).collect::<Vec<_>>();
    suffix_chars.reverse();
    let suffix: String = suffix_chars.into_iter().collect();
    format!("api.key:{suffix}")
}

fn is_public_endpoint(path: &str) -> bool {
    matches!(path, "/health" | "/metrics")
}

fn is_mutating_request(method: &Method, path: &str) -> bool {
    method == Method::POST
        && (path.starts_with("/v1/runs")
            || path.starts_with("/v1/tasks")
            || path.starts_with("/v1/webhooks"))
}

async fn enforce_rate_limit(
    security: &ApiSecurityConfig,
    rate_limits: &RwLock<HashMap<String, ApiRateLimitBucket>>,
    rate_key: String,
) -> Result<(), ApiError> {
    let limit = security.rate_limit_per_minute;
    if limit == 0 {
        return Ok(());
    }

    let now = Instant::now();
    let mut buckets = rate_limits.write().await;
    let bucket = buckets.entry(rate_key).or_default();
    if let Some(retry_after_seconds) = bucket.next_request(
        now,
        limit,
        StdDuration::from_secs(API_RATE_LIMIT_WINDOW_SECONDS),
    ) {
        return Err(ApiError::RateLimitExceeded {
            retry_after_seconds,
        });
    }

    Ok(())
}

fn build_api_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/events/ws", get(event_stream_ws))
        .route("/v1/webhooks", post(register_webhook))
        .route("/v1/runs", post(create_run).get(list_runs))
        .route("/v1/runs/:run_id/status", get(run_status))
        .route("/v1/runs/:run_id/pause", post(pause_run))
        .route("/v1/runs/:run_id/resume", post(resume_run))
        .route("/v1/runs/:run_id/cancel", post(cancel_run))
        .route("/v1/tasks", get(list_tasks))
        .route("/v1/tasks/:task_id", get(task_status))
        .route("/v1/tasks/:task_id/annotate", post(task_annotate))
        .route("/v1/tasks/:task_id/unblock", post(task_unblock))
        .route("/v1/tasks/:task_id/retry", post(task_retry))
        .route("/v1/audit", get(audit_log))
        .route("/v1/metrics/reporting", get(reporting_metrics))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            api_security_guard,
        ))
        .with_state(state)
}

async fn api_security_guard(
    State(state): State<ApiState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    let is_public = is_public_endpoint(&path);

    let api_key = extract_api_key(request.headers());
    if state.security.requires_auth() && !is_public {
        let Some(api_key) = api_key.as_deref() else {
            return ApiError::Unauthorized {
                reason: "missing API key",
            }
            .into_response();
        };
        if !state.security.has_api_key(api_key) {
            return ApiError::Unauthorized {
                reason: "invalid API key",
            }
            .into_response();
        }
    }

    let actor = actor_from_api_key(api_key.as_deref());
    if is_mutating_request(request.method(), &path) {
        info!(
            actor = actor,
            method = request.method().as_str(),
            path = path,
            "api mutation request"
        );
    }

    if !is_public {
        let rate_key = api_key.unwrap_or_else(|| ANONYMOUS_API_ACTOR.to_string());
        if let Err(error) = enforce_rate_limit(&state.security, &state.rate_limits, rate_key).await
        {
            let retry_after = match &error {
                ApiError::RateLimitExceeded {
                    retry_after_seconds,
                } => *retry_after_seconds,
                _ => 1,
            };
            let mut response = error.into_response();
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_str(&retry_after.to_string())
                    .expect("retry-after fits within ASCII header values"),
            );
            return response;
        }
    }

    request.extensions_mut().insert(ApiRequestContext { actor });
    next.run(request).await
}

#[derive(Debug, Error)]
pub enum ApiServerError {
    #[error("server failed: {0}")]
    Serve(std::io::Error),
}

pub fn router(store: Arc<dyn EventStore>) -> Router {
    let state = ApiState::new(store);
    start_webhook_dispatcher(state.clone());
    build_api_router(state)
}

#[cfg(feature = "debug-api")]
pub fn router_with_queue(store: Arc<dyn EventStore>, queue: Arc<dyn TaskQueue>) -> Router {
    let state = ApiState::new_with_queue(store, queue);
    start_webhook_dispatcher(state.clone());

    build_api_router(state.clone())
        .route("/v1/tasks/:task_id/priority", post(task_override_priority))
        .route("/debug/queue-depth", get(debug_queue_depth))
        .route("/debug/active-leases", get(debug_active_leases))
        .route("/debug/resource-usage/:run_id", get(debug_resource_usage))
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    store: Arc<dyn EventStore>,
) -> Result<(), ApiServerError> {
    axum::serve(listener, router(store))
        .await
        .map_err(ApiServerError::Serve)
}

#[cfg(feature = "debug-api")]
pub async fn serve_with_queue(
    listener: tokio::net::TcpListener,
    store: Arc<dyn EventStore>,
    queue: Arc<dyn TaskQueue>,
) -> Result<(), ApiServerError> {
    axum::serve(listener, router_with_queue(store, queue))
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

async fn metrics(State(state): State<ApiState>) -> String {
    encode_metrics(&state.metrics_registry)
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct EventFilter {
    run_id: Option<Uuid>,
    task_id: Option<Uuid>,
    event_type: Option<String>,
    severity: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RunListQuery {
    state: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TaskListQuery {
    run_id: Option<Uuid>,
    state: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AuditQuery {
    run_id: Option<Uuid>,
    after: Option<Uuid>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct EventStreamQuery {
    #[serde(flatten)]
    filters: EventFilter,
    after_event_id: Option<Uuid>,
    poll_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct EventStreamEnvelope {
    event: Event,
    run_id: Option<Uuid>,
    task_id: Option<Uuid>,
    severity: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RegisterWebhookRequest {
    callback_url: String,
    #[serde(flatten)]
    filters: EventFilter,
}

#[derive(Debug, Clone, Serialize)]
struct RegisterWebhookResponse {
    webhook_id: Uuid,
    callback_url: String,
    filters: EventFilter,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct WebhookRegistration {
    id: Uuid,
    callback_url: String,
    filters: EventFilter,
    created_at: DateTime<Utc>,
}

#[derive(Debug)]
struct StreamFilters {
    run_id: Option<Uuid>,
    task_id: Option<Uuid>,
    event_type: Option<String>,
    severity: Option<String>,
    after_event_id: Option<Uuid>,
    poll_ms: u64,
}

#[derive(Debug, Serialize)]
struct RunListItem {
    run_id: Uuid,
    state: String,
    last_event_type: String,
    updated_at: DateTime<Utc>,
    correlation_id: Uuid,
    objective: Option<String>,
    task_summary: TaskStatusSummary,
    run_created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct RunListResponse {
    runs: Vec<RunListItem>,
    total: usize,
    limit: usize,
    offset: usize,
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct TaskListItem {
    task_id: Uuid,
    run_id: Option<Uuid>,
    state: String,
    last_event_type: String,
    updated_at: DateTime<Utc>,
    correlation_id: Uuid,
    attempt_no: u32,
}

#[derive(Debug, Serialize)]
struct TaskListResponse {
    tasks: Vec<TaskListItem>,
    total: usize,
    limit: usize,
    offset: usize,
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct AuditLogResponse {
    run_id: Uuid,
    correlation_id: Uuid,
    after: Option<Uuid>,
    limit: usize,
    events: Vec<Event>,
}

#[derive(Debug, Serialize)]
struct ReportingMetricsResponse {
    runs_by_state: BTreeMap<String, usize>,
    tasks_by_state: BTreeMap<String, usize>,
    budget_breaches: BudgetBreachMetrics,
    total_events: usize,
}

#[derive(Debug, Serialize)]
struct BudgetBreachMetrics {
    runs: usize,
    tasks: usize,
    by_reason: BTreeMap<String, usize>,
}

#[derive(Debug)]
struct RunListProjection {
    run_id: Uuid,
    state: RunState,
    correlation_id: Uuid,
    updated_at: DateTime<Utc>,
    run_created_at: DateTime<Utc>,
    last_event_type: String,
    objective: Option<String>,
    last_event_id: Uuid,
}

#[derive(Debug)]
struct TaskListProjection {
    task_id: Uuid,
    correlation_id: Uuid,
    state: TaskState,
    updated_at: DateTime<Utc>,
    last_event_type: String,
    attempt_no: u32,
    last_event_id: Uuid,
    budget_breach_reason: Option<String>,
}

fn normalized_stream_filters(mut query: EventStreamQuery) -> StreamFilters {
    let mut event_type = query.filters.event_type.take();
    if let Some(value) = event_type.as_mut() {
        *value = value.trim().to_string();
        if value.is_empty() {
            event_type = None;
        }
    }
    let mut severity = query.filters.severity.take();
    if let Some(value) = severity.as_mut() {
        *value = value.trim().to_lowercase();
        if value.is_empty() {
            severity = None;
        }
    }

    StreamFilters {
        run_id: query.filters.run_id,
        task_id: query.filters.task_id,
        event_type,
        severity,
        after_event_id: query.after_event_id,
        poll_ms: query.poll_ms.unwrap_or(EVENT_STREAM_POLL_MS_DEFAULT),
    }
}

fn normalized_event_filter(mut filters: EventFilter) -> EventFilter {
    if let Some(value) = filters.event_type.as_mut() {
        *value = value.trim().to_string();
        if value.is_empty() {
            filters.event_type = None;
        }
    }
    if let Some(value) = filters.severity.as_mut() {
        *value = value.trim().to_lowercase();
        if value.is_empty() {
            filters.severity = None;
        }
    }
    filters
}

async fn event_stream_ws(
    State(state): State<ApiState>,
    Query(query): Query<EventStreamQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let filters = normalized_stream_filters(query);
    ws.on_upgrade(move |socket| async move {
        handle_event_stream(socket, filters, state).await;
    })
}

async fn handle_event_stream(mut socket: WebSocket, filters: StreamFilters, state: ApiState) {
    let mut after_event_id = filters.after_event_id;
    let poll_interval = std::time::Duration::from_millis(filters.poll_ms.max(50));
    let mut correlation_cache: HashMap<Uuid, Option<Uuid>> = HashMap::new();

    loop {
        let query = event_query_with_after_limit(after_event_id, Some(EVENT_STREAM_BATCH_LIMIT));
        let events = match state.store.query(&query) {
            Ok(events) => events,
            Err(StoreError::EventNotFound(_)) => {
                after_event_id = None;
                Vec::new()
            }
            Err(_) => {
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        for event in events {
            if !matches_stream_filters(&state, &event, &filters, &mut correlation_cache) {
                after_event_id = Some(event.event_id);
                continue;
            }

            let payload = api_event_payload(&state, &event, &mut correlation_cache);
            if socket
                .send(Message::Text(
                    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string()),
                ))
                .await
                .is_err()
            {
                return;
            }

            after_event_id = Some(event.event_id);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn register_webhook(
    State(state): State<ApiState>,
    Json(payload): Json<RegisterWebhookRequest>,
) -> Result<Json<RegisterWebhookResponse>, ApiError> {
    let callback_url = Url::parse(&payload.callback_url)
        .map_err(|_| ApiError::InvalidWebhookUrl)?
        .to_string();

    match callback_url.as_str() {
        url if url.starts_with("http://") || url.starts_with("https://") => {}
        _ => return Err(ApiError::InvalidWebhookUrl),
    }

    let registration = WebhookRegistration {
        id: Uuid::now_v7(),
        callback_url: callback_url.clone(),
        filters: normalized_event_filter(payload.filters),
        created_at: Utc::now(),
    };
    {
        let mut webhooks = state.webhooks.write().await;
        webhooks.push(registration.clone());
    }

    Ok(Json(RegisterWebhookResponse {
        webhook_id: registration.id,
        callback_url,
        filters: registration.filters,
        created_at: registration.created_at,
    }))
}

fn start_webhook_dispatcher(state: ApiState) {
    if state
        .webhook_dispatcher_started
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        tokio::spawn(webhook_dispatcher(state));
    }
}

async fn webhook_dispatcher(state: ApiState) {
    let client = Client::new();
    let query_interval = std::time::Duration::from_millis(WEBHOOK_DISPATCH_INTERVAL_MS);
    let mut last_seen_event_id = {
        let last_seen_event_id = state.webhook_last_seen_event_id.read().await;
        *last_seen_event_id
    };

    loop {
        let query =
            event_query_with_after_limit(last_seen_event_id, Some(EVENT_STREAM_BATCH_LIMIT));
        let events = match state.store.query(&query) {
            Ok(events) => events,
            Err(StoreError::EventNotFound(_)) => {
                let mut cursor = state.webhook_last_seen_event_id.write().await;
                *cursor = None;
                last_seen_event_id = None;
                Vec::new()
            }
            Err(_) => {
                tokio::time::sleep(query_interval).await;
                continue;
            }
        };

        if !events.is_empty() {
            let webhooks = state.webhooks.read().await.clone();
            let mut correlation_cache: HashMap<Uuid, Option<Uuid>> = HashMap::new();

            for event in events.iter() {
                let payload = api_event_payload(&state, event, &mut correlation_cache);
                for webhook in webhooks.iter() {
                    if !matches_stream_filters(
                        &state,
                        event,
                        &webhook.filters_for_query(),
                        &mut correlation_cache,
                    ) {
                        continue;
                    }
                    let webhook = webhook.clone();
                    let client = client.clone();
                    let payload = payload.clone();
                    tokio::spawn(async move {
                        deliver_webhook_with_retry(&client, webhook, payload).await;
                    });
                }
                last_seen_event_id = Some(event.event_id);
            }
            let mut cursor = state.webhook_last_seen_event_id.write().await;
            *cursor = last_seen_event_id;
        }

        tokio::time::sleep(query_interval).await;
    }
}

impl WebhookRegistration {
    fn filters_for_query(&self) -> StreamFilters {
        StreamFilters {
            run_id: self.filters.run_id,
            task_id: self.filters.task_id,
            event_type: self.filters.event_type.clone(),
            severity: self.filters.severity.clone(),
            after_event_id: None,
            poll_ms: EVENT_STREAM_POLL_MS_DEFAULT,
        }
    }
}

async fn deliver_webhook_with_retry(
    client: &Client,
    webhook: WebhookRegistration,
    payload: EventStreamEnvelope,
) {
    for attempt in 0..WEBHOOK_MAX_RETRY_ATTEMPTS {
        let result = client
            .post(&webhook.callback_url)
            .json(&payload)
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => return,
            Ok(_) => {}
            Err(_) => {}
        }

        if attempt + 1 >= WEBHOOK_MAX_RETRY_ATTEMPTS {
            break;
        }
        tokio::time::sleep(webhook_backoff_delay(attempt)).await;
    }
}

fn webhook_backoff_delay(attempt: u8) -> std::time::Duration {
    let exponent = attempt.min(10);
    let multiplier = 1_u64 << exponent;
    let candidate = WEBHOOK_INITIAL_RETRY_BACKOFF_MS.saturating_mul(multiplier);
    std::time::Duration::from_millis(candidate.min(WEBHOOK_BACKOFF_MAX_MS))
}

async fn list_runs(
    State(state): State<ApiState>,
    Query(query): Query<RunListQuery>,
) -> Result<Json<RunListResponse>, ApiError> {
    let limit = normalize_list_limit(query.limit);
    let requested_state = parse_run_state_filter(query.state.as_deref())?;
    let run_projections = query_run_list_projections(state.store.as_ref())?;
    let mut projections: Vec<_> = run_projections
        .into_values()
        .filter(|projection| match requested_state {
            Some(state) => projection.state == state,
            None => true,
        })
        .collect();
    projections.sort_by(|left, right| match right.updated_at.cmp(&left.updated_at) {
        std::cmp::Ordering::Equal => right.last_event_id.cmp(&left.last_event_id),
        ordering => ordering,
    });
    let filtered_len = projections.len();
    let offset = normalize_list_offset(query.offset, filtered_len);
    let end = (offset + limit).min(filtered_len);
    let has_more = end < filtered_len;

    let runs: Vec<_> = projections
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|projection| {
            let task_summary = summarize_tasks(state.store.as_ref(), projection.correlation_id)?;
            Ok(RunListItem {
                run_id: projection.run_id,
                state: format!("{:?}", projection.state),
                last_event_type: projection.last_event_type,
                updated_at: projection.updated_at,
                correlation_id: projection.correlation_id,
                objective: projection.objective,
                task_summary,
                run_created_at: projection.run_created_at,
            })
        })
        .collect::<Result<_, ApiError>>()?;

    Ok(Json(RunListResponse {
        runs,
        total: filtered_len,
        limit,
        offset,
        has_more,
    }))
}

async fn list_tasks(
    State(state): State<ApiState>,
    Query(query): Query<TaskListQuery>,
) -> Result<Json<TaskListResponse>, ApiError> {
    let limit = normalize_list_limit(query.limit);
    let requested_state = parse_task_state_filter(query.state.as_deref())?;
    let task_projections = query_task_list_projections(state.store.as_ref())?;
    let run_id_by_correlation = query_run_id_by_correlation(state.store.as_ref())?;
    let mut projections: Vec<_> = task_projections
        .into_values()
        .filter(|projection| match requested_state {
            Some(state) => projection.state == state,
            None => true,
        })
        .filter(|projection| match query.run_id {
            Some(run_id) => {
                run_id_by_correlation
                    .get(&projection.correlation_id)
                    .copied()
                    == Some(run_id)
            }
            None => true,
        })
        .collect();
    projections.sort_by(|left, right| match right.updated_at.cmp(&left.updated_at) {
        std::cmp::Ordering::Equal => right.last_event_id.cmp(&left.last_event_id),
        ordering => ordering,
    });
    let filtered_len = projections.len();
    let offset = normalize_list_offset(query.offset, filtered_len);
    let end = (offset + limit).min(filtered_len);
    let has_more = end < filtered_len;

    let tasks: Vec<_> = projections
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|projection| TaskListItem {
            task_id: projection.task_id,
            run_id: run_id_by_correlation
                .get(&projection.correlation_id)
                .copied(),
            state: format!("{:?}", projection.state),
            last_event_type: projection.last_event_type,
            updated_at: projection.updated_at,
            correlation_id: projection.correlation_id,
            attempt_no: projection.attempt_no,
        })
        .collect();

    Ok(Json(TaskListResponse {
        tasks,
        total: filtered_len,
        limit,
        offset,
        has_more,
    }))
}

async fn audit_log(
    State(state): State<ApiState>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<AuditLogResponse>, ApiError> {
    let run_id = query.run_id.ok_or(ApiError::MissingRunId)?;
    let correlation_id = query_correlation_for_run(state.store.as_ref(), run_id)?
        .ok_or(ApiError::RunNotFound(run_id))?;
    let limit = normalize_list_limit(query.limit);
    let after = query.after;

    let mut eq = EventQuery::by_correlation(correlation_id);
    eq.limit = Some(limit);
    eq.after_event_id = after;

    let events = state.store.query(&eq).map_err(ApiError::Store)?;
    Ok(Json(AuditLogResponse {
        run_id,
        correlation_id,
        after: eq.after_event_id,
        limit,
        events,
    }))
}

async fn reporting_metrics(
    State(state): State<ApiState>,
) -> Result<Json<ReportingMetricsResponse>, ApiError> {
    Ok(Json(collect_reporting_metrics(state.store.as_ref())?))
}

fn normalize_list_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(API_LIST_LIMIT_DEFAULT)
        .clamp(1, API_LIST_LIMIT_MAX)
}

fn normalize_list_offset(offset: Option<usize>, total: usize) -> usize {
    offset.unwrap_or(0).min(total)
}

fn query_run_list_projections(
    store: &dyn EventStore,
) -> Result<HashMap<Uuid, RunListProjection>, ApiError> {
    let run_events = store
        .query(&EventQuery::by_entity_type(EntityType::Run))
        .map_err(ApiError::Store)?;
    let mut projections = HashMap::with_capacity(run_events.len());

    for event in run_events {
        let run_id = match event.entity_id.parse::<Uuid>() {
            Ok(value) => value,
            Err(_) => continue,
        };

        let projection = projections
            .entry(run_id)
            .or_insert_with(|| RunListProjection {
                run_id,
                state: RunState::RunOpen,
                correlation_id: event.correlation_id,
                updated_at: event.occurred_at,
                run_created_at: event.occurred_at,
                last_event_type: event.event_type.clone(),
                objective: None,
                last_event_id: event.event_id,
            });
        projection.correlation_id = event.correlation_id;
        projection.updated_at = event.occurred_at;
        projection.last_event_type = event.event_type.clone();
        projection.last_event_id = event.event_id;

        if let Some(next_state) = run_state_from_event(&event) {
            projection.state = next_state;
        }

        if event.event_type == "run.config_snapshot" {
            projection.objective = event
                .payload
                .get("objective")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string());
        }
    }

    Ok(projections)
}

fn query_task_list_projections(
    store: &dyn EventStore,
) -> Result<HashMap<Uuid, TaskListProjection>, ApiError> {
    let task_events = store
        .query(&EventQuery::by_entity_type(EntityType::Task))
        .map_err(ApiError::Store)?;
    let mut projections = HashMap::with_capacity(task_events.len());

    for event in task_events {
        let task_id = match event.entity_id.parse::<Uuid>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let projection = projections
            .entry(task_id)
            .or_insert_with(|| TaskListProjection {
                task_id,
                correlation_id: event.correlation_id,
                state: TaskState::TaskOpen,
                updated_at: event.occurred_at,
                last_event_type: event.event_type.clone(),
                attempt_no: 0,
                last_event_id: event.event_id,
                budget_breach_reason: None,
            });
        projection.correlation_id = event.correlation_id;
        projection.updated_at = event.occurred_at;
        projection.last_event_type = event.event_type.clone();
        projection.last_event_id = event.event_id;

        if let Some(next_state) = task_state_from_event(&event) {
            projection.state = next_state;
        }

        if let Some(attempt_no) = event
            .payload
            .get("attempt_no")
            .and_then(|attempt_no| attempt_no.as_u64())
        {
            projection.attempt_no = attempt_no as u32;
        }

        projection.budget_breach_reason =
            detect_budget_breach_reason(&event).or_else(|| projection.budget_breach_reason.clone());
    }

    Ok(projections)
}

fn query_run_id_by_correlation(store: &dyn EventStore) -> Result<HashMap<Uuid, Uuid>, ApiError> {
    let mut map = HashMap::new();
    let run_events = store
        .query(&EventQuery::by_entity_type(EntityType::Run))
        .map_err(ApiError::Store)?;

    for event in run_events {
        let run_id = match event.entity_id.parse::<Uuid>() {
            Ok(run_id) => run_id,
            Err(_) => continue,
        };
        map.insert(event.correlation_id, run_id);
    }

    Ok(map)
}

fn query_correlation_for_run(
    store: &dyn EventStore,
    run_id: Uuid,
) -> Result<Option<Uuid>, ApiError> {
    let run_events = store
        .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
        .map_err(ApiError::Store)?;
    Ok(run_events.last().map(|event| event.correlation_id))
}

fn parse_run_state_filter(value: Option<&str>) -> Result<Option<RunState>, ApiError> {
    let Some(raw) = value else {
        return Ok(None);
    };
    match raw.trim().to_lowercase().as_str() {
        "open" => Ok(Some(RunState::RunOpen)),
        "active" => Ok(Some(RunState::RunActive)),
        "blocked" => Ok(Some(RunState::RunBlocked)),
        "verifying" => Ok(Some(RunState::RunVerifying)),
        "completed" => Ok(Some(RunState::RunCompleted)),
        "failed" => Ok(Some(RunState::RunFailed)),
        "cancelled" => Ok(Some(RunState::RunCancelled)),
        other => Err(ApiError::InvalidStateFilter {
            entity: "run",
            value: other.to_string(),
        }),
    }
}

fn parse_task_state_filter(value: Option<&str>) -> Result<Option<TaskState>, ApiError> {
    let Some(raw) = value else {
        return Ok(None);
    };
    match raw.trim().to_lowercase().as_str() {
        "open" => Ok(Some(TaskState::TaskOpen)),
        "ready" => Ok(Some(TaskState::TaskReady)),
        "executing" => Ok(Some(TaskState::TaskExecuting)),
        "waiting" => Ok(Some(TaskState::TaskWaiting)),
        "blocked" => Ok(Some(TaskState::TaskBlocked)),
        "verifying" => Ok(Some(TaskState::TaskVerifying)),
        "complete" | "completed" => Ok(Some(TaskState::TaskComplete)),
        "failed" => Ok(Some(TaskState::TaskFailed)),
        "cancelled" => Ok(Some(TaskState::TaskCancelled)),
        other => Err(ApiError::InvalidStateFilter {
            entity: "task",
            value: other.to_string(),
        }),
    }
}

fn collect_reporting_metrics(store: &dyn EventStore) -> Result<ReportingMetricsResponse, ApiError> {
    let mut runs_by_state = BTreeMap::new();
    let run_projections = query_run_list_projections(store)?;
    let mut tasks_by_state = BTreeMap::new();
    let task_projections = query_task_list_projections(store)?;
    let run_events = store
        .query(&EventQuery::by_entity_type(EntityType::Run))
        .map_err(ApiError::Store)?;
    let mut budget_breaches = BudgetBreachMetrics {
        runs: 0,
        tasks: 0,
        by_reason: BTreeMap::new(),
    };

    for projection in run_projections.values() {
        *runs_by_state
            .entry(format!("{:?}", projection.state))
            .or_default() += 1;
    }

    for projection in task_projections.values() {
        *tasks_by_state
            .entry(format!("{:?}", projection.state))
            .or_default() += 1;

        if projection.state == TaskState::TaskFailed {
            if let Some(reason) = projection.budget_breach_reason.as_deref() {
                budget_breaches.tasks = budget_breaches.tasks.saturating_add(1);
                *budget_breaches
                    .by_reason
                    .entry(reason.to_string())
                    .or_default() += 1;
            }
        }
    }

    for event in run_events {
        if let Some("budget_exceeded") =
            event.payload.get("reason").and_then(|value| value.as_str())
        {
            budget_breaches.runs = budget_breaches.runs.saturating_add(1);
            *budget_breaches
                .by_reason
                .entry("run:budget_exceeded".to_string())
                .or_default() += 1;
        }
    }

    Ok(ReportingMetricsResponse {
        runs_by_state,
        tasks_by_state,
        budget_breaches,
        total_events: store.len(),
    })
}

fn detect_budget_breach_reason(event: &Event) -> Option<String> {
    if event.payload.get("reason").and_then(|value| value.as_str()) != Some("budget_exceeded") {
        return None;
    }
    let scope = event
        .payload
        .get("scope")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let metric = event
        .payload
        .get("metric")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    if scope.is_empty() && metric.is_empty() {
        return Some("budget_exceeded".to_string());
    }
    if metric.is_empty() {
        Some(format!("budget_exceeded:{scope}"))
    } else if scope.is_empty() {
        Some(format!("budget_exceeded:{metric}"))
    } else {
        Some(format!("budget_exceeded:{scope}:{metric}"))
    }
}

fn event_query_with_after_limit(after_event_id: Option<Uuid>, limit: Option<usize>) -> EventQuery {
    let mut query = EventQuery::default();
    query.after_event_id = after_event_id;
    query.limit = limit;
    query
}

fn event_task_id(event: &Event) -> Option<Uuid> {
    if event.entity_type == EntityType::Task {
        event.entity_id.parse::<Uuid>().ok()
    } else {
        None
    }
}

fn event_severity(event: &Event) -> Option<String> {
    event
        .payload
        .get("severity")
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_lowercase())
}

fn run_id_from_correlation(
    store: &dyn EventStore,
    run_cache: &mut HashMap<Uuid, Option<Uuid>>,
    correlation_id: Uuid,
) -> Option<Uuid> {
    if let Some(cached) = run_cache.get(&correlation_id) {
        return *cached;
    }

    let run_id = store
        .query(&EventQuery::by_correlation(correlation_id))
        .ok()
        .and_then(|events| {
            events
                .into_iter()
                .find(|event| event.entity_type == EntityType::Run)
                .and_then(|run_event| run_event.entity_id.parse().ok())
        });

    run_cache.insert(correlation_id, run_id);
    run_id
}

fn api_event_payload(
    state: &ApiState,
    event: &Event,
    run_cache: &mut HashMap<Uuid, Option<Uuid>>,
) -> EventStreamEnvelope {
    let run_id = if event.entity_type == EntityType::Run {
        event.entity_id.parse::<Uuid>().ok()
    } else {
        run_id_from_correlation(state.store.as_ref(), run_cache, event.correlation_id)
    };
    let task_id = event_task_id(event);
    let severity = event_severity(event);

    EventStreamEnvelope {
        event: event.clone(),
        run_id,
        task_id,
        severity,
    }
}

fn matches_stream_filters(
    state: &ApiState,
    event: &Event,
    filters: &StreamFilters,
    run_cache: &mut HashMap<Uuid, Option<Uuid>>,
) -> bool {
    if let Some(run_id) = filters.run_id {
        let event_run_id = if event.entity_type == EntityType::Run {
            event.entity_id.parse::<Uuid>().ok()
        } else {
            run_id_from_correlation(state.store.as_ref(), run_cache, event.correlation_id)
        };
        if Some(run_id) != event_run_id {
            return false;
        }
    }

    if let Some(task_id) = filters.task_id {
        if Some(task_id) != event_task_id(event) {
            return false;
        }
    }

    if let Some(raw_event_type) = &filters.event_type {
        if event.event_type != *raw_event_type {
            return false;
        }
    }

    if let Some(raw_severity) = &filters.severity {
        if raw_severity != event_severity(event).as_deref().unwrap_or_default() {
            return false;
        }
    }

    true
}

async fn create_run(
    Extension(context): Extension<ApiRequestContext>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<CreateRunRequest>,
) -> Result<Json<RunStatusResponse>, ApiError> {
    let objective = payload
        .objective
        .unwrap_or_else(|| "API-created run".to_string());
    let reason = payload
        .reason
        .unwrap_or_else(|| "create run requested by API".to_string());

    let idempotency_key = idempotency_key_from_headers(&headers)?;
    let run_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let now = Utc::now();

    let config_key = idempotency_key
        .as_ref()
        .map(|key| mutation_idempotency_key("run", None, key, "config_snapshot"));
    let catalog_key = idempotency_key
        .as_ref()
        .map(|key| mutation_idempotency_key("run", Some(run_id), key, "task_catalog"));
    let activated_key = idempotency_key
        .as_ref()
        .map(|key| mutation_idempotency_key("run", Some(run_id), key, "activated"));

    if let Some(key) = &config_key {
        if let Some(existing_run_id) = run_id_for_idempotency_key(state.store.as_ref(), key)? {
            return load_existing_run_status(&*state.store, existing_run_id);
        }
    }

    let config_event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: now,
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: "run.config_snapshot".to_string(),
        payload: json!({
            "objective": objective,
            "safe_mode": false,
            "config_snapshot": {
                "source": "api",
            },
        }),
        correlation_id,
        causation_id: None,
        actor: context.actor.clone(),
        idempotency_key: config_key.clone(),
    };
    if let Err(error) = state.store.append(config_event) {
        match handle_store_error_for_idempotent_append(
            &*state.store,
            &error,
            config_key.as_ref(),
            None,
        ) {
            Some(existing_run_id) => {
                return load_existing_run_status(&*state.store, existing_run_id);
            }
            None => return Err(ApiError::Store(error)),
        }
    }

    let catalog_event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: now + chrono::Duration::seconds(1),
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: "run.task_catalog".to_string(),
        payload: json!({ "tasks": [] }),
        correlation_id,
        causation_id: None,
        actor: context.actor.clone(),
        idempotency_key: catalog_key.clone(),
    };
    if let Err(error) = state.store.append(catalog_event) {
        match handle_store_error_for_idempotent_append(
            &*state.store,
            &error,
            catalog_key.as_ref(),
            Some(run_id),
        ) {
            Some(existing_run_id) => {
                if existing_run_id != run_id {
                    return Err(ApiError::Store(error));
                }
                return load_existing_run_status(&*state.store, run_id);
            }
            None => return Err(ApiError::Store(error)),
        }
    }

    let activated_event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: now + chrono::Duration::seconds(2),
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: "run.activated".to_string(),
        payload: json!({
            "from": "RunOpen",
            "to": "RunActive",
            "reason": reason,
            "detail": "run activated",
        }),
        correlation_id,
        causation_id: None,
        actor: context.actor.clone(),
        idempotency_key: activated_key.clone(),
    };
    if let Err(error) = state.store.append(activated_event) {
        match handle_store_error_for_idempotent_append(
            &*state.store,
            &error,
            activated_key.as_ref(),
            Some(run_id),
        ) {
            Some(existing_run_id) => {
                if existing_run_id != run_id {
                    return Err(ApiError::Store(error));
                }
                return load_existing_run_status(&*state.store, run_id);
            }
            None => return Err(ApiError::Store(error)),
        }
    }

    load_run_status(state.store.as_ref(), run_id)?
        .ok_or(ApiError::RunNotFound(run_id))
        .map(Json)
}

async fn pause_run(
    Path(run_id): Path<String>,
    Extension(context): Extension<ApiRequestContext>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<RunMutationRequest>,
) -> Result<Json<RunStatusResponse>, ApiError> {
    apply_run_transition(
        &context.actor,
        run_id,
        State(state),
        headers,
        payload
            .reason
            .unwrap_or_else(|| "pause requested by API".to_string()),
        RunState::RunBlocked,
        "pause",
        "run.blocked",
    )
    .await
}

async fn resume_run(
    Path(run_id): Path<String>,
    Extension(context): Extension<ApiRequestContext>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<RunMutationRequest>,
) -> Result<Json<RunStatusResponse>, ApiError> {
    apply_run_transition(
        &context.actor,
        run_id,
        State(state),
        headers,
        payload
            .reason
            .unwrap_or_else(|| "resume requested by API".to_string()),
        RunState::RunActive,
        "resume",
        "run.activated",
    )
    .await
}

async fn cancel_run(
    Path(run_id): Path<String>,
    Extension(context): Extension<ApiRequestContext>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<RunMutationRequest>,
) -> Result<Json<RunStatusResponse>, ApiError> {
    apply_run_transition(
        &context.actor,
        run_id,
        State(state),
        headers,
        payload
            .reason
            .unwrap_or_else(|| "cancel requested by API".to_string()),
        RunState::RunCancelled,
        "cancel",
        "run.cancelled",
    )
    .await
}

async fn task_annotate(
    Path(task_id): Path<String>,
    Extension(context): Extension<ApiRequestContext>,
    State(state): State<ApiState>,
    Json(payload): Json<TaskAnnotateRequest>,
) -> Result<Json<TaskStatusResponse>, ApiError> {
    let task_id = task_id
        .parse::<Uuid>()
        .map_err(|_| ApiError::InvalidTaskId)?;

    let projection = load_task_projection_for_control(state.store.as_ref(), task_id)?
        .ok_or(ApiError::TaskNotFound(task_id))?;
    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Task,
        entity_id: task_id.to_string(),
        event_type: "task.annotated".to_string(),
        payload: serde_json::json!({
            "blocker_detail": payload.blocker_detail,
        }),
        correlation_id: projection.correlation_id,
        causation_id: projection.last_event_id,
        actor: context.actor,
        idempotency_key: None,
    };
    state.store.append(event).map_err(ApiError::Store)?;

    load_task_status(state.store.as_ref(), task_id)?
        .ok_or(ApiError::TaskNotFound(task_id))
        .map(Json)
}

async fn task_unblock(
    Path(task_id): Path<String>,
    Extension(context): Extension<ApiRequestContext>,
    State(state): State<ApiState>,
    Json(payload): Json<TaskUnblockRequest>,
) -> Result<Json<TaskStatusResponse>, ApiError> {
    apply_task_transition_to_state(
        &context.actor,
        task_id,
        State(state),
        payload
            .reason
            .unwrap_or_else(|| "manually unblocked".to_string()),
        TaskState::TaskReady,
        "task unblocked",
        "task.unblocked",
    )
    .await
}

async fn task_retry(
    Path(task_id): Path<String>,
    Extension(context): Extension<ApiRequestContext>,
    State(state): State<ApiState>,
) -> Result<Json<TaskStatusResponse>, ApiError> {
    let parsed_task_id = task_id
        .parse::<Uuid>()
        .map_err(|_| ApiError::InvalidTaskId)?;
    let projection = load_task_projection_for_control(state.store.as_ref(), parsed_task_id)?
        .ok_or(ApiError::TaskNotFound(parsed_task_id))?;

    if projection.state != TaskState::TaskFailed {
        return Err(ApiError::InvalidTaskTransition {
            task_id: parsed_task_id,
            current: projection.state,
            target: TaskState::TaskReady,
            action: "task retry".to_string(),
        });
    }

    apply_task_transition_to_state(
        &context.actor,
        task_id,
        State(state),
        "operator retry".to_string(),
        TaskState::TaskReady,
        "task retry",
        "task.retrying",
    )
    .await
}

#[cfg(feature = "debug-api")]
async fn task_override_priority(
    Path(task_id): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<TaskPriorityOverrideRequest>,
) -> Result<Json<TaskStatusResponse>, ApiError> {
    let task_id = task_id
        .parse::<Uuid>()
        .map_err(|_| ApiError::InvalidTaskId)?;

    let _projection = load_task_projection_for_control(state.store.as_ref(), task_id)?
        .ok_or(ApiError::TaskNotFound(task_id))?;
    let queue = state.queue.as_ref().ok_or(ApiError::DebugQueueMissing)?;
    match queue.override_priority(task_id, payload.priority) {
        Ok(()) => {}
        Err(QueueError::NotFound(_)) => return Err(ApiError::TaskNotFound(task_id)),
        Err(error) => return Err(ApiError::TaskQueue(error)),
    }

    load_task_status(state.store.as_ref(), task_id)?
        .ok_or(ApiError::TaskNotFound(task_id))
        .map(Json)
}

fn idempotency_key_from_headers(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY_HEADER) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| ApiError::InvalidIdempotencyKey)?;
    let value = raw.trim();
    if value.is_empty() {
        return Err(ApiError::InvalidIdempotencyKey);
    }
    Ok(Some(value.to_string()))
}

fn mutation_idempotency_key(
    scope: &str,
    run_id: Option<Uuid>,
    client_key: &str,
    event_variant: &str,
) -> String {
    match run_id {
        Some(run_id) => format!("api:{scope}:{run_id}:{event_variant}:{client_key}"),
        None => format!("api:{scope}:{event_variant}:{client_key}"),
    }
}

fn run_id_for_idempotency_key(
    store: &dyn EventStore,
    idempotency_key: &str,
) -> Result<Option<Uuid>, ApiError> {
    for event in store.all().map_err(ApiError::Store)? {
        if event.idempotency_key.as_deref() == Some(idempotency_key) {
            if let Ok(run_id) = event.entity_id.parse::<Uuid>() {
                return Ok(Some(run_id));
            }
        }
    }

    Ok(None)
}

fn handle_store_error_for_idempotent_append(
    store: &dyn EventStore,
    error: &StoreError,
    idempotency_key: Option<&String>,
    expected_run_id: Option<Uuid>,
) -> Option<Uuid> {
    let StoreError::DuplicateIdempotencyKey(_) = error else {
        return None;
    };

    let idempotency_key = idempotency_key?;

    let found_run_id = run_id_for_idempotency_key(store, idempotency_key).ok()??;

    if expected_run_id.is_none() || Some(found_run_id) == expected_run_id {
        Some(found_run_id)
    } else {
        None
    }
}

fn load_existing_run_status(
    store: &dyn EventStore,
    run_id: Uuid,
) -> Result<Json<RunStatusResponse>, ApiError> {
    load_run_status(store, run_id)?
        .ok_or(ApiError::RunNotFound(run_id))
        .map(Json)
}

async fn apply_run_transition(
    actor: &str,
    run_id: String,
    State(state): State<ApiState>,
    headers: HeaderMap,
    reason: String,
    target_state: RunState,
    action: &str,
    event_type: &str,
) -> Result<Json<RunStatusResponse>, ApiError> {
    let run_id = run_id.parse::<Uuid>().map_err(|_| ApiError::InvalidRunId)?;
    let projection =
        load_run_projection(state.store.as_ref(), run_id)?.ok_or(ApiError::RunNotFound(run_id))?;

    if projection.state == target_state {
        return load_existing_run_status(&*state.store, run_id);
    }
    if !projection.state.can_transition_to(target_state) {
        return Err(ApiError::InvalidRunTransition {
            run_id,
            current: projection.state,
            target: target_state,
            action: action.to_string(),
        });
    }

    let idempotency_key = idempotency_key_from_headers(&headers)?;
    let event_idempotency_key = idempotency_key
        .as_ref()
        .map(|key| mutation_idempotency_key("run", Some(run_id), key, event_type));

    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: event_type.to_string(),
        payload: json!({
            "from": format!("{:?}", projection.state),
            "to": format!("{:?}", target_state),
            "reason": reason,
            "detail": action,
            "exit_reason": "cancelled_by_operator",
        }),
        correlation_id: projection.correlation_id,
        causation_id: None,
        actor: actor.to_string(),
        idempotency_key: event_idempotency_key.clone(),
    };
    if let Err(error) = state.store.append(event) {
        match handle_store_error_for_idempotent_append(
            &*state.store,
            &error,
            event_idempotency_key.as_ref(),
            Some(run_id),
        ) {
            Some(found_run_id) => {
                if found_run_id != run_id {
                    return Err(ApiError::Store(error));
                }
                return load_existing_run_status(&*state.store, run_id);
            }
            None => {
                return Err(ApiError::Store(error));
            }
        }
    }

    load_existing_run_status(&*state.store, run_id)
}

fn load_run_projection(
    store: &dyn EventStore,
    run_id: Uuid,
) -> Result<Option<RunStateProjection>, ApiError> {
    let run_events = store
        .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
        .map_err(ApiError::Store)?;
    if run_events.is_empty() {
        return Ok(None);
    }

    let mut projection = RunStateProjection {
        state: RunState::RunOpen,
        correlation_id: run_events[0].correlation_id,
    };

    for event in &run_events {
        projection.correlation_id = event.correlation_id;
        if let Some(next_state) = run_state_from_event(event) {
            projection.state = next_state;
        }
    }

    Ok(Some(projection))
}

async fn apply_task_transition_to_state(
    actor: &str,
    task_id: String,
    State(state): State<ApiState>,
    reason: String,
    target_state: TaskState,
    action: &str,
    event_type: &str,
) -> Result<Json<TaskStatusResponse>, ApiError> {
    let task_id = task_id
        .parse::<Uuid>()
        .map_err(|_| ApiError::InvalidTaskId)?;
    let projection = load_task_projection_for_control(state.store.as_ref(), task_id)?
        .ok_or(ApiError::TaskNotFound(task_id))?;

    if projection.state == target_state {
        return load_task_status(state.store.as_ref(), task_id)?
            .ok_or(ApiError::TaskNotFound(task_id))
            .map(Json);
    }
    if !projection.state.can_transition_to(target_state) {
        return Err(ApiError::InvalidTaskTransition {
            task_id,
            current: projection.state,
            target: target_state,
            action: action.to_string(),
        });
    }

    let mut payload = serde_json::json!({
        "from": format!("{:?}", projection.state),
        "to": format!("{:?}", target_state),
        "reason": reason,
    });
    if event_type == "task.retrying" {
        payload["attempt_no"] = serde_json::json!(projection.attempt_no.saturating_add(1));
    }
    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Task,
        entity_id: task_id.to_string(),
        event_type: event_type.to_string(),
        payload,
        correlation_id: projection.correlation_id,
        causation_id: projection.last_event_id,
        actor: actor.to_string(),
        idempotency_key: None,
    };
    state.store.append(event).map_err(ApiError::Store)?;

    load_task_status(state.store.as_ref(), task_id)?
        .ok_or(ApiError::TaskNotFound(task_id))
        .map(Json)
}

#[derive(Debug)]
struct TaskMutationProjection {
    state: TaskState,
    correlation_id: Uuid,
    last_event_id: Option<Uuid>,
    attempt_no: u32,
    last_event_type: String,
}

fn load_task_projection_for_control(
    store: &dyn EventStore,
    task_id: Uuid,
) -> Result<Option<TaskMutationProjection>, ApiError> {
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
    let mut correlation_id = task_events[0].correlation_id;
    let mut last_event_id = None;
    let mut attempt_no = 0u32;
    let mut last_event_type = String::new();

    for event in &task_events {
        correlation_id = event.correlation_id;
        last_event_id = Some(event.event_id);
        if let Some(next_state) = task_state_from_event(event) {
            state = next_state;
        }
        if let Some(raw_attempt) = event
            .payload
            .get("attempt_no")
            .and_then(|value| value.as_u64())
        {
            attempt_no = raw_attempt as u32;
        }
        last_event_type = event.event_type.clone();
    }

    Ok(Some(TaskMutationProjection {
        state,
        correlation_id,
        last_event_id,
        attempt_no,
        last_event_type,
    }))
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

#[derive(Debug, Deserialize)]
struct CreateRunRequest {
    objective: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RunMutationRequest {
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskAnnotateRequest {
    blocker_detail: String,
}

#[derive(Debug, Deserialize)]
struct TaskUnblockRequest {
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskPriorityOverrideRequest {
    priority: u32,
}

#[derive(Debug)]
struct RunStateProjection {
    state: RunState,
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

#[cfg(feature = "debug-api")]
#[derive(Debug, Default)]
struct QueueDepthTotals {
    pending: usize,
    leased: usize,
    completed: usize,
    failed: usize,
    cancelled: usize,
}

#[cfg(feature = "debug-api")]
impl QueueDepthTotals {
    fn add(&mut self, status: &str) {
        match status {
            "pending" => self.pending = self.pending.saturating_add(1),
            "leased" => self.leased = self.leased.saturating_add(1),
            "completed" => self.completed = self.completed.saturating_add(1),
            "failed" => self.failed = self.failed.saturating_add(1),
            "cancelled" => self.cancelled = self.cancelled.saturating_add(1),
            _ => {}
        }
    }

    fn total(&self) -> usize {
        self.pending
            .saturating_add(self.leased)
            .saturating_add(self.completed)
            .saturating_add(self.failed)
            .saturating_add(self.cancelled)
    }
}

#[cfg(feature = "debug-api")]
#[derive(Debug, Default, Serialize)]
struct ResourceUsageBudget {
    max_run_total_tokens: Option<u64>,
    max_run_peak_rss_bytes: Option<u64>,
    max_run_cpu_user_ticks: Option<u64>,
    max_run_cpu_system_ticks: Option<u64>,
    max_run_io_read_bytes: Option<u64>,
    max_run_io_write_bytes: Option<u64>,
}

#[cfg(feature = "debug-api")]
#[derive(Debug, Default, Serialize)]
struct ResourceUsageTotals {
    total_cpu_user_ticks: u64,
    total_cpu_system_ticks: u64,
    total_io_read_bytes: u64,
    total_io_write_bytes: u64,
    total_tokens: u64,
    peak_rss_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    budgets: Option<ResourceUsageBudget>,
}

#[cfg(feature = "debug-api")]
#[derive(Debug, Serialize)]
struct QueueDepthEntry {
    key: String,
    pending: usize,
    leased: usize,
    completed: usize,
    failed: usize,
    cancelled: usize,
    total: usize,
}

#[cfg(feature = "debug-api")]
#[derive(Debug, Serialize)]
struct QueueDepthResponse {
    overall: QueueDepthEntry,
    by_run: Vec<QueueDepthEntry>,
    by_class: Vec<QueueDepthEntry>,
}

#[cfg(feature = "debug-api")]
#[derive(Debug, Serialize)]
struct ActiveLeaseResponse {
    queue_id: Uuid,
    run_id: Uuid,
    task_id: Uuid,
    owner: Option<String>,
    command_class: String,
    attempt_no: u32,
    lease_expires_at: Option<DateTime<Utc>>,
    ttl_seconds: Option<i64>,
    available_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

#[cfg(feature = "debug-api")]
#[derive(Debug, Serialize)]
struct ActiveLeasesResponse {
    active_leases: Vec<ActiveLeaseResponse>,
    count: usize,
}

#[cfg(feature = "debug-api")]
#[derive(Debug, Serialize)]
struct ResourceUsageResponse {
    run_id: Uuid,
    correlation_id: Uuid,
    totals: ResourceUsageTotals,
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

#[cfg(feature = "debug-api")]
async fn debug_queue_depth(
    State(state): State<ApiState>,
) -> Result<Json<QueueDepthResponse>, ApiError> {
    let Some(queue) = state.queue.as_ref() else {
        return Err(ApiError::DebugQueueMissing);
    };

    let entries = queue.entries();
    let mut overall = QueueDepthTotals::default();
    let mut by_run: BTreeMap<Uuid, QueueDepthTotals> = BTreeMap::new();
    let mut by_class: BTreeMap<String, QueueDepthTotals> = BTreeMap::new();

    for entry in entries {
        let status = entry.status;
        let status_key = match status {
            yarli_queue::queue::QueueStatus::Pending => "pending",
            yarli_queue::queue::QueueStatus::Leased => "leased",
            yarli_queue::queue::QueueStatus::Completed => "completed",
            yarli_queue::queue::QueueStatus::Failed => "failed",
            yarli_queue::queue::QueueStatus::Cancelled => "cancelled",
        };

        overall.add(status_key);
        by_run.entry(entry.run_id).or_default().add(status_key);
        by_class
            .entry(format!("{:?}", entry.command_class))
            .or_default()
            .add(status_key);
    }

    Ok(Json(QueueDepthResponse {
        overall: QueueDepthEntry {
            key: "overall".to_string(),
            pending: overall.pending,
            leased: overall.leased,
            completed: overall.completed,
            failed: overall.failed,
            cancelled: overall.cancelled,
            total: overall.total(),
        },
        by_run: by_run
            .into_iter()
            .map(|(run_id, totals)| QueueDepthEntry {
                key: run_id.to_string(),
                pending: totals.pending,
                leased: totals.leased,
                completed: totals.completed,
                failed: totals.failed,
                cancelled: totals.cancelled,
                total: totals.total(),
            })
            .collect(),
        by_class: by_class
            .into_iter()
            .map(|(class, totals)| QueueDepthEntry {
                key: class,
                pending: totals.pending,
                leased: totals.leased,
                completed: totals.completed,
                failed: totals.failed,
                cancelled: totals.cancelled,
                total: totals.total(),
            })
            .collect(),
    }))
}

#[cfg(feature = "debug-api")]
async fn debug_active_leases(
    State(state): State<ApiState>,
) -> Result<Json<ActiveLeasesResponse>, ApiError> {
    let Some(queue) = state.queue.as_ref() else {
        return Err(ApiError::DebugQueueMissing);
    };
    let now = Utc::now();

    let active_leases = queue
        .entries()
        .into_iter()
        .filter(|entry| matches!(entry.status, yarli_queue::queue::QueueStatus::Leased))
        .map(|entry| ActiveLeaseResponse {
            queue_id: entry.queue_id,
            run_id: entry.run_id,
            task_id: entry.task_id,
            owner: entry.lease_owner,
            command_class: format!("{:?}", entry.command_class),
            attempt_no: entry.attempt_no,
            lease_expires_at: entry.lease_expires_at,
            ttl_seconds: entry
                .lease_expires_at
                .map(|expires_at| expires_at.signed_duration_since(now).num_seconds()),
            available_at: entry.available_at,
            created_at: entry.created_at,
        })
        .collect::<Vec<_>>();

    let count = active_leases.len();
    Ok(Json(ActiveLeasesResponse {
        active_leases,
        count,
    }))
}

#[cfg(feature = "debug-api")]
async fn debug_resource_usage(
    Path(run_id): Path<String>,
    State(state): State<ApiState>,
) -> Result<Json<ResourceUsageResponse>, ApiError> {
    let run_id = run_id.parse::<Uuid>().map_err(|_| ApiError::InvalidRunId)?;
    let run_events = state
        .store
        .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
        .map_err(ApiError::Store)?;
    if run_events.is_empty() {
        return Err(ApiError::RunNotFound(run_id));
    }
    let correlation_id = run_events[run_events.len() - 1].correlation_id;
    let events = state
        .store
        .query(&EventQuery::by_correlation(correlation_id))
        .map_err(ApiError::Store)?;
    let budgets = collect_api_resource_budgets(&events);
    let totals = collect_api_resource_totals(&events);
    Ok(Json(ResourceUsageResponse {
        run_id,
        correlation_id,
        totals: ResourceUsageTotals {
            total_cpu_user_ticks: totals.total_cpu_user_ticks,
            total_cpu_system_ticks: totals.total_cpu_system_ticks,
            total_io_read_bytes: totals.total_io_read_bytes,
            total_io_write_bytes: totals.total_io_write_bytes,
            total_tokens: totals.total_tokens,
            peak_rss_bytes: totals.peak_rss_bytes,
            budgets: Some(budgets),
        },
    }))
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

    let mut summary = TaskStatusSummary {
        total: states.len(),
        ..Default::default()
    };
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
        .or(match event.event_type.as_str() {
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
        .or(match event.event_type.as_str() {
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
    #[error("unauthorized: {reason}")]
    Unauthorized { reason: &'static str },
    #[error("rate limit exceeded; retry after {retry_after_seconds} seconds")]
    RateLimitExceeded { retry_after_seconds: u64 },
    #[error("invalid run ID (expected UUID)")]
    InvalidRunId,
    #[error("invalid task ID (expected UUID)")]
    InvalidTaskId,
    #[error("invalid idempotency key")]
    InvalidIdempotencyKey,
    #[error("invalid webhook registration payload")]
    InvalidWebhookUrl,
    #[error("missing required query parameter run_id")]
    MissingRunId,
    #[error("invalid {entity} state filter: {value}")]
    InvalidStateFilter { entity: &'static str, value: String },
    #[error("run {run_id} cannot transition to {target:?} from {current:?} with action {action}")]
    InvalidRunTransition {
        run_id: Uuid,
        current: RunState,
        target: RunState,
        action: String,
    },
    #[error(
        "task {task_id} cannot transition to {target:?} from {current:?} with action {action}"
    )]
    InvalidTaskTransition {
        task_id: Uuid,
        current: TaskState,
        target: TaskState,
        action: String,
    },
    #[error("run {0} not found")]
    RunNotFound(Uuid),
    #[error("task {0} not found")]
    TaskNotFound(Uuid),
    #[error("task {0} has no correlated run events")]
    CorrelatedRunMissing(Uuid),
    #[error("debug endpoint is not wired with queue access")]
    #[cfg_attr(not(feature = "debug-api"), allow(dead_code))]
    DebugQueueMissing,
    #[cfg(feature = "debug-api")]
    #[error("task queue operation failed: {0}")]
    TaskQueue(QueueError),
    #[error("failed to read persisted state")]
    Store(StoreError),
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            ApiError::RateLimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            ApiError::InvalidRunId => StatusCode::BAD_REQUEST,
            ApiError::InvalidTaskId => StatusCode::BAD_REQUEST,
            ApiError::InvalidIdempotencyKey => StatusCode::BAD_REQUEST,
            ApiError::MissingRunId => StatusCode::BAD_REQUEST,
            ApiError::InvalidStateFilter { .. } => StatusCode::BAD_REQUEST,
            ApiError::InvalidRunTransition { .. } => StatusCode::CONFLICT,
            ApiError::InvalidTaskTransition { .. } => StatusCode::CONFLICT,
            ApiError::InvalidWebhookUrl => StatusCode::BAD_REQUEST,
            ApiError::RunNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::TaskNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::DebugQueueMissing => StatusCode::SERVICE_UNAVAILABLE,
            #[cfg(feature = "debug-api")]
            ApiError::TaskQueue(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::CorrelatedRunMissing(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut response = (
            status,
            Json(ErrorResponse {
                error: self.to_string(),
            }),
        )
            .into_response();

        if let ApiError::RateLimitExceeded {
            retry_after_seconds,
        } = self
        {
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_str(&retry_after_seconds.to_string())
                    .expect("retry-after fits within ASCII header values"),
            );
        }

        response
    }
}

#[cfg(feature = "debug-api")]
fn collect_api_resource_budgets(events: &[Event]) -> ResourceUsageBudget {
    let mut latest_snapshot = None;
    for event in events {
        if event.event_type == "run.config_snapshot" {
            latest_snapshot = event.payload.get("config_snapshot");
        }
    }

    let Some(snapshot) = latest_snapshot else {
        return ResourceUsageBudget::default();
    };
    let budgets = match snapshot
        .get("config")
        .and_then(|value| value.get("budgets"))
    {
        Some(value) => value,
        None => return ResourceUsageBudget::default(),
    };

    ResourceUsageBudget {
        max_run_cpu_user_ticks: budgets
            .get("max_run_cpu_user_ticks")
            .and_then(|value| value.as_u64()),
        max_run_cpu_system_ticks: budgets
            .get("max_run_cpu_system_ticks")
            .and_then(|value| value.as_u64()),
        max_run_io_read_bytes: budgets
            .get("max_run_io_read_bytes")
            .and_then(|value| value.as_u64()),
        max_run_io_write_bytes: budgets
            .get("max_run_io_write_bytes")
            .and_then(|value| value.as_u64()),
        max_run_total_tokens: budgets
            .get("max_run_total_tokens")
            .and_then(|value| value.as_u64()),
        max_run_peak_rss_bytes: budgets
            .get("max_run_peak_rss_bytes")
            .and_then(|value| value.as_u64()),
    }
}

#[cfg(feature = "debug-api")]
fn collect_api_resource_totals(events: &[Event]) -> ResourceUsageTotals {
    let mut totals = ResourceUsageTotals::default();
    let mut seen_command_ids = HashSet::new();

    for event in events {
        if event.entity_type != EntityType::Command {
            continue;
        }
        if !matches!(
            event.event_type.as_str(),
            "command.exited" | "command.timed_out" | "command.killed" | "command.completed"
        ) {
            continue;
        }
        if !seen_command_ids.insert(event.entity_id.clone()) {
            continue;
        }

        if let Some(raw_usage) = event.payload.get("resource_usage") {
            if let Ok(usage) = serde_json::from_value::<CommandResourceUsage>(raw_usage.clone()) {
                if let Some(value) = usage.cpu_user_ticks {
                    totals.total_cpu_user_ticks = totals.total_cpu_user_ticks.saturating_add(value);
                }
                if let Some(value) = usage.cpu_system_ticks {
                    totals.total_cpu_system_ticks =
                        totals.total_cpu_system_ticks.saturating_add(value);
                }
                if let Some(value) = usage.io_read_bytes {
                    totals.total_io_read_bytes = totals.total_io_read_bytes.saturating_add(value);
                }
                if let Some(value) = usage.io_write_bytes {
                    totals.total_io_write_bytes = totals.total_io_write_bytes.saturating_add(value);
                }
                if let Some(value) = usage.max_rss_bytes {
                    totals.peak_rss_bytes = totals.peak_rss_bytes.max(value);
                }
            }
        }

        if let Some(raw_tokens) = event.payload.get("token_usage") {
            if let Ok(usage) = serde_json::from_value::<TokenUsage>(raw_tokens.clone()) {
                totals.total_tokens = totals.total_tokens.saturating_add(usage.total_tokens);
            }
        }
    }

    totals
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use chrono::Duration;
    use serde_json::json;
    use tower::ServiceExt;
    #[cfg(feature = "debug-api")]
    use yarli_core::domain::CommandClass;
    #[cfg(feature = "debug-api")]
    use yarli_queue::InMemoryTaskQueue;
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
    async fn metrics_endpoint_returns_prometheus_format() {
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload = std::str::from_utf8(&body).unwrap();

        assert!(payload.contains("yarli_queue_depth"));
    }

    #[tokio::test]
    async fn create_run_endpoint_creates_active_run() {
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"objective":"build API run","reason":"ci test"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["state"], "RunActive");
        assert_eq!(payload["objective"], "build API run");
        assert_eq!(payload["task_summary"]["total"], 0);
    }

    #[tokio::test]
    async fn create_run_endpoint_replays_with_idempotency_key() {
        let store = Arc::new(InMemoryEventStore::new());
        let app = router(store.clone());
        let mut request = Request::builder()
            .method("POST")
            .uri("/v1/runs")
            .header("content-type", "application/json")
            .header("idempotency-key", "run-create-001")
            .body(Body::from(json!({"objective":"replay me"}).to_string()))
            .unwrap();

        let first = app.clone().oneshot(request).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_payload: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        let first_run_id = first_payload["run_id"].as_str().unwrap().to_string();

        request = Request::builder()
            .method("POST")
            .uri("/v1/runs")
            .header("content-type", "application/json")
            .header("idempotency-key", "run-create-001")
            .body(Body::from(json!({"objective":"replay me"}).to_string()))
            .unwrap();
        let second = app.oneshot(request).await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_payload: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(first_run_id, second_payload["run_id"].as_str().unwrap());
        assert_eq!(store.len(), 3);
    }

    #[tokio::test]
    async fn create_run_endpoint_requires_api_key_when_configured() {
        let app = build_api_router(ApiState::new_with_security(
            Arc::new(InMemoryEventStore::new()),
            ApiSecurityConfig::with_test_keys(&["expected-key"], DEFAULT_RATE_LIMIT_RPM),
        ));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"objective":"missing key"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let bad_key_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .header("x-api-key", "bad-key")
                    .body(Body::from(json!({"objective":"bad key"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_key_response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn mutation_events_include_api_actor_identity() {
        let state = ApiState::new_with_security(
            Arc::new(InMemoryEventStore::new()),
            ApiSecurityConfig::with_test_keys(&["operator-key"], DEFAULT_RATE_LIMIT_RPM),
        );
        let app = build_api_router(state.clone());
        let key = "operator-key";
        let actor = actor_from_api_key(Some(key));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .header("x-api-key", key)
                    .body(Body::from(json!({"objective":"audited run"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let run_id: Uuid = payload["run_id"].as_str().unwrap().parse().unwrap();

        for event in state.store.all().unwrap() {
            if event.entity_id == run_id.to_string() && event.entity_type == EntityType::Run {
                assert_eq!(event.actor, actor);
            }
        }
    }

    #[tokio::test]
    async fn api_rate_limits_are_enforced_per_key() {
        let app = build_api_router(ApiState::new_with_security(
            Arc::new(InMemoryEventStore::new()),
            ApiSecurityConfig::with_test_keys(&["bounded-key"], 2),
        ));

        let first_request = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .header("x-api-key", "bounded-key")
                    .header("idempotency-key", "rate-limit-1")
                    .body(Body::from(json!({"objective":"limit me"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_request.status(), StatusCode::OK);

        let second_request = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .header("x-api-key", "bounded-key")
                    .header("idempotency-key", "rate-limit-2")
                    .body(Body::from(json!({"objective":"limit me"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_request.status(), StatusCode::OK);

        let third_request = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .header("x-api-key", "bounded-key")
                    .header("idempotency-key", "rate-limit-3")
                    .body(Body::from(json!({"objective":"limit me"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(third_request.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(third_request
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_some());
    }

    #[tokio::test]
    async fn run_pause_resume_cancel_endpoints_mutate_run_state() {
        let store = Arc::new(InMemoryEventStore::new());
        let run_id = Uuid::now_v7();
        let correlation_id = Uuid::now_v7();
        let now = Utc::now();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                correlation_id,
                now,
                json!({"objective":"operator flow"}),
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

        let pause_response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}/pause"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "pause-1")
                    .body(Body::from(json!({"reason":"quiesce"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(pause_response.status(), StatusCode::OK);
        let pause_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(pause_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(pause_payload["state"], "RunBlocked");

        let pause_replay_response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}/pause"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "pause-1")
                    .body(Body::from(json!({"reason":"quiesce"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(pause_replay_response.status(), StatusCode::OK);

        let resume_response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}/resume"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "resume-1")
                    .body(Body::from(json!({"reason":"continue"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resume_response.status(), StatusCode::OK);
        let resume_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(resume_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(resume_payload["state"], "RunActive");

        let cancel_response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}/cancel"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "cancel-1")
                    .body(Body::from(json!({"reason":"stop now"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel_response.status(), StatusCode::OK);
        let cancel_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(cancel_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(cancel_payload["state"], "RunCancelled");

        let cancel_replay_response = router(store)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}/cancel"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "cancel-1")
                    .body(Body::from(json!({"reason":"stop now"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel_replay_response.status(), StatusCode::OK);
        let cancel_replay_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(cancel_replay_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(cancel_replay_payload["state"], "RunCancelled");
        assert_eq!(cancel_replay_payload["run_id"], json!(run_id));
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
    async fn task_annotate_endpoint_attaches_blocker_detail() {
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
                json!({"objective":"annotation surface"}),
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

        let response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/tasks/{task_id}/annotate"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"blocker_detail":"waiting on external dependency"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["task_id"], json!(task_id));
        assert_eq!(payload["state"], json!("TaskReady"));
    }

    #[tokio::test]
    async fn list_runs_endpoint_supports_state_filter_and_pagination() {
        let store = Arc::new(InMemoryEventStore::new());
        let now = Utc::now();
        let run_active_latest = Uuid::now_v7();
        let run_active_older = Uuid::now_v7();
        let run_completed = Uuid::now_v7();
        let correlation_active_latest = Uuid::now_v7();
        let correlation_active_older = Uuid::now_v7();
        let correlation_completed = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_active_latest.to_string(),
                "run.config_snapshot",
                correlation_active_latest,
                now,
                json!({"objective": "latest active"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_active_latest.to_string(),
                "run.activated",
                correlation_active_latest,
                now + Duration::seconds(10),
                json!({"to":"RunActive"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Run,
                run_active_older.to_string(),
                "run.config_snapshot",
                correlation_active_older,
                now + Duration::seconds(1),
                json!({"objective": "older active"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_active_older.to_string(),
                "run.activated",
                correlation_active_older,
                now + Duration::seconds(2),
                json!({"to":"RunActive"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Run,
                run_completed.to_string(),
                "run.config_snapshot",
                correlation_completed,
                now + Duration::seconds(3),
                json!({"objective": "completed run"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_completed.to_string(),
                "run.activated",
                correlation_completed,
                now + Duration::seconds(4),
                json!({"to":"RunActive"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_completed.to_string(),
                "run.completed",
                correlation_completed,
                now + Duration::seconds(5),
                json!({"to":"RunCompleted"}),
            ))
            .unwrap();

        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri("/v1/runs?state=active&limit=1&offset=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let runs = payload["runs"].as_array().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["run_id"], json!(run_active_older));
        assert_eq!(runs[0]["state"], json!("RunActive"));
        assert_eq!(payload["total"], json!(2));
        assert_eq!(payload["limit"], json!(1));
        assert_eq!(payload["offset"], json!(1));
        assert_eq!(payload["has_more"], json!(false));
    }

    #[tokio::test]
    async fn list_tasks_endpoint_filters_by_run_and_state() {
        let store = Arc::new(InMemoryEventStore::new());
        let now = Utc::now();
        let run_alpha = Uuid::now_v7();
        let run_beta = Uuid::now_v7();
        let correlation_alpha = Uuid::now_v7();
        let correlation_beta = Uuid::now_v7();
        let task_failed_alpha = Uuid::now_v7();
        let task_ready_alpha = Uuid::now_v7();
        let task_failed_beta = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_alpha.to_string(),
                "run.config_snapshot",
                correlation_alpha,
                now,
                json!({"objective":"alpha"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_alpha.to_string(),
                "run.activated",
                correlation_alpha,
                now + Duration::seconds(1),
                json!({"to":"RunActive"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_beta.to_string(),
                "run.config_snapshot",
                correlation_beta,
                now + Duration::seconds(1),
                json!({"objective":"beta"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_beta.to_string(),
                "run.activated",
                correlation_beta,
                now + Duration::seconds(2),
                json!({"to":"RunActive"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Task,
                task_failed_alpha.to_string(),
                "task.failed",
                correlation_alpha,
                now + Duration::seconds(3),
                json!({"to":"TaskFailed"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_ready_alpha.to_string(),
                "task.ready",
                correlation_alpha,
                now + Duration::seconds(4),
                json!({"to":"TaskReady"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_failed_beta.to_string(),
                "task.failed",
                correlation_beta,
                now + Duration::seconds(5),
                json!({"to":"TaskFailed"}),
            ))
            .unwrap();

        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/tasks?run_id={run_alpha}&state=failed&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let tasks = payload["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"], json!(task_failed_alpha));
        assert_eq!(tasks[0]["state"], json!("TaskFailed"));
        assert_eq!(tasks[0]["run_id"], json!(run_alpha));
        assert_eq!(payload["total"], json!(1));
    }

    #[tokio::test]
    async fn audit_log_endpoint_supports_cursoring() {
        let store = Arc::new(InMemoryEventStore::new());
        let now = Utc::now();
        let run_id = Uuid::now_v7();
        let correlation_id = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                correlation_id,
                now,
                json!({"objective":"audit"}),
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
                EntityType::Run,
                run_id.to_string(),
                "run.cancelled",
                correlation_id,
                now + Duration::seconds(2),
                json!({"to":"RunCancelled"}),
            ))
            .unwrap();

        let first_response = router(store.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/audit?run_id={run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(first_response.status(), StatusCode::OK);
        let first_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(first_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let first_events = first_payload["events"].as_array().unwrap();
        assert_eq!(first_events.len(), 3);

        let cursor = first_events[0]["event_id"].as_str().unwrap().to_string();
        let second_response = router(store)
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/audit?run_id={run_id}&after={cursor}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(second_response.status(), StatusCode::OK);
        let second_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(second_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let second_events = second_payload["events"].as_array().unwrap();
        assert_eq!(second_events.len(), 2);
        assert_eq!(second_payload["after"], json!(cursor));
        assert_eq!(second_events[0]["event_id"], first_events[1]["event_id"]);
        assert_eq!(second_payload["limit"], json!(50));
    }

    #[tokio::test]
    async fn reporting_metrics_endpoint_reports_states_and_breaches() {
        let store = Arc::new(InMemoryEventStore::new());
        let now = Utc::now();
        let run_completed = Uuid::now_v7();
        let run_failed = Uuid::now_v7();
        let correlation_completed = Uuid::now_v7();
        let correlation_failed = Uuid::now_v7();
        let task_complete = Uuid::now_v7();
        let task_failed = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_completed.to_string(),
                "run.config_snapshot",
                correlation_completed,
                now,
                json!({"objective":"completed"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_completed.to_string(),
                "run.activated",
                correlation_completed,
                now + Duration::seconds(1),
                json!({"to":"RunActive"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_completed.to_string(),
                "run.completed",
                correlation_completed,
                now + Duration::seconds(2),
                json!({"to":"RunCompleted"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Run,
                run_failed.to_string(),
                "run.config_snapshot",
                correlation_failed,
                now + Duration::seconds(3),
                json!({"objective":"failed"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_failed.to_string(),
                "run.activated",
                correlation_failed,
                now + Duration::seconds(4),
                json!({"to":"RunActive"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_failed.to_string(),
                "run.failed",
                correlation_failed,
                now + Duration::seconds(5),
                json!({"reason": "budget_exceeded", "scope": "run", "metric": "cpu"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Task,
                task_complete.to_string(),
                "task.completed",
                correlation_completed,
                now + Duration::seconds(6),
                json!({"to":"TaskComplete"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_failed.to_string(),
                "task.failed",
                correlation_failed,
                now + Duration::seconds(7),
                json!({
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "scope": "task",
                    "metric": "memory"
                }),
            ))
            .unwrap();

        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/reporting")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["runs_by_state"]["RunCompleted"], json!(1));
        assert_eq!(payload["runs_by_state"]["RunFailed"], json!(1));
        assert_eq!(payload["tasks_by_state"]["TaskComplete"], json!(1));
        assert_eq!(payload["tasks_by_state"]["TaskFailed"], json!(1));
        assert_eq!(payload["budget_breaches"]["runs"], json!(1));
        assert_eq!(payload["budget_breaches"]["tasks"], json!(1));
        assert_eq!(
            payload["budget_breaches"]["by_reason"]["run:budget_exceeded"],
            json!(1)
        );
        assert_eq!(
            payload["budget_breaches"]["by_reason"]["budget_exceeded:task:memory"],
            json!(1)
        );
        assert_eq!(payload["total_events"], json!(8));
    }

    #[tokio::test]
    async fn task_unblock_transitions_blocked_to_ready() {
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
                json!({"objective":"blocker surface"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.blocked",
                correlation_id,
                now + Duration::seconds(1),
                json!({"to":"TaskBlocked", "reason":"blocked by external"}),
            ))
            .unwrap();

        let response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/tasks/{task_id}/unblock"))
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"reason":"manual unblock"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["task_id"], json!(task_id));
        assert_eq!(payload["state"], json!("TaskReady"));
        assert_eq!(payload["last_event_type"], json!("task.unblocked"));
    }

    #[tokio::test]
    async fn task_retry_endpoint_retries_failed_task() {
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
                json!({"objective":"retry surface"}),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                correlation_id,
                now + Duration::seconds(1),
                json!({"to":"TaskFailed", "attempt_no": 1}),
            ))
            .unwrap();

        let response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/tasks/{task_id}/retry"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["task_id"], json!(task_id));
        assert_eq!(payload["state"], json!("TaskReady"));
        assert_eq!(payload["last_event_type"], json!("task.retrying"));
    }

    #[tokio::test]
    async fn task_retry_endpoint_rejects_invalid_transition() {
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
                json!({"objective":"retry guard"}),
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

        let response = router(store.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/tasks/{task_id}/retry"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert!(payload["error"]
            .as_str()
            .unwrap()
            .contains("cannot transition"));
    }

    #[cfg(feature = "debug-api")]
    #[tokio::test]
    async fn task_priority_endpoint_overrides_queue_priority() {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
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
                json!({"objective":"priority override"}),
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
        queue
            .enqueue(task_id, run_id, 5, CommandClass::Io, None)
            .unwrap();

        let app = router_with_queue(store.clone(), queue.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/tasks/{task_id}/priority"))
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "priority": 1 }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response_body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(response_body["task_id"], json!(task_id));
        assert_eq!(response_body["state"], json!("TaskReady"));

        let updated_entry = queue
            .entries()
            .into_iter()
            .find(|entry| entry.task_id == task_id)
            .expect("task entry should exist");
        assert_eq!(updated_entry.priority, 1);
    }

    #[tokio::test]
    async fn register_webhook_rejects_invalid_callback_url() {
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/webhooks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"callback_url":"ftp://invalid.example"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn register_webhook_persists_filters_for_event_dispatch() {
        let response = router(Arc::new(InMemoryEventStore::new()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/webhooks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "callback_url": "http://127.0.0.1:9999/webhook",
                            "run_id": null,
                            "event_type": "run.activated",
                            "severity": "info"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value = to_bytes(response.into_body(), usize::MAX)
            .await
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap();
        assert_eq!(payload["webhook_id"].as_str().unwrap().len(), 36);
        assert_eq!(payload["filters"]["event_type"], "run.activated");
        assert_eq!(payload["filters"]["severity"], "info");
    }

    #[test]
    fn webhook_backoff_is_exponential() {
        assert_eq!(
            webhook_backoff_delay(0),
            std::time::Duration::from_millis(250)
        );
        assert_eq!(
            webhook_backoff_delay(1),
            std::time::Duration::from_millis(500)
        );
        assert_eq!(
            webhook_backoff_delay(2),
            std::time::Duration::from_millis(1000)
        );
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
