use std::path::Path;

use anyhow::{anyhow, Context, Result};
use tracing::warn;
use yarli_core::domain::{EntityType, Event};
use yarli_core::entities::ContinuationPayload;
use yarli_store::event_store::EventQuery;
use yarli_store::EventStore;

use crate::config::{with_event_store, LoadedConfig};

pub(crate) const DEFAULT_CONTINUATION_FILE: &str = ".yarli/continuation.json";
pub(crate) const RUN_CONTINUATION_EVENT_TYPE: &str = "run.continuation";

pub(crate) fn query_events(store: &dyn EventStore, query: &EventQuery) -> Result<Vec<Event>> {
    store
        .query(query)
        .map_err(|e| anyhow!("event query failed: {e}"))
}

pub(crate) fn append_event(store: &dyn EventStore, event: Event) -> Result<()> {
    store
        .append(event)
        .map_err(|e| anyhow!("failed to append event: {e}"))
}

pub(crate) fn read_continuation_payload_from_file_if_exists(
    file: &Path,
) -> Result<Option<ContinuationPayload>> {
    let content = match std::fs::read_to_string(file) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read continuation file: {}", file.display()));
        }
    };

    let payload = serde_json::from_str(&content).context("failed to parse continuation file")?;
    Ok(Some(payload))
}

pub(crate) fn load_latest_continuation_payload_from_store(
    store: &dyn EventStore,
) -> Result<Option<ContinuationPayload>> {
    let mut run_events = query_events(store, &EventQuery::by_entity_type(EntityType::Run))?;
    run_events.sort_by(|a, b| {
        a.occurred_at
            .cmp(&b.occurred_at)
            .then_with(|| a.event_id.cmp(&b.event_id))
    });

    for event in run_events.into_iter().rev() {
        if event.event_type != RUN_CONTINUATION_EVENT_TYPE {
            continue;
        }
        let Some(raw_payload) = event.payload.get("continuation_payload").cloned() else {
            warn!(
                event_id = %event.event_id,
                run_id = %event.entity_id,
                "ignoring continuation event missing continuation_payload"
            );
            continue;
        };
        match serde_json::from_value::<ContinuationPayload>(raw_payload) {
            Ok(payload) => return Ok(Some(payload)),
            Err(e) => {
                warn!(
                    event_id = %event.event_id,
                    run_id = %event.entity_id,
                    error = %e,
                    "ignoring malformed continuation payload event"
                );
            }
        }
    }

    Ok(None)
}

pub(crate) fn try_load_continuation_payload_for_continue(
    file: &Path,
    loaded_config: &LoadedConfig,
) -> Result<Option<ContinuationPayload>> {
    if file == Path::new(DEFAULT_CONTINUATION_FILE) {
        match with_event_store(loaded_config, load_latest_continuation_payload_from_store) {
            Ok(Some(payload)) => return Ok(Some(payload)),
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "failed to load continuation payload from event store; trying file artifact"
                )
            }
        }
    }

    read_continuation_payload_from_file_if_exists(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{make_event, sample_continuation_payload};
    use chrono::Utc;
    use tempfile::TempDir;
    use uuid::Uuid;
    use yarli_core::domain::EntityType;
    use yarli_core::fsm::run::RunState;
    use yarli_core::fsm::task::TaskState;
    use yarli_store::InMemoryEventStore;

    #[test]
    fn read_continuation_payload_from_file_if_exists_returns_none_for_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("missing-continuation.json");
        let payload = read_continuation_payload_from_file_if_exists(&file_path).unwrap();
        assert!(payload.is_none());
    }

    #[test]
    fn read_continuation_payload_from_file_if_exists_reads_payload() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("continuation.json");
        let payload = sample_continuation_payload(Uuid::new_v4(), "from-file");
        let json = serde_json::to_string_pretty(&payload).unwrap();
        std::fs::write(&file_path, json).unwrap();

        let loaded = read_continuation_payload_from_file_if_exists(&file_path)
            .unwrap()
            .expect("expected payload");
        assert_eq!(loaded.run_id, payload.run_id);
        assert_eq!(loaded.objective, payload.objective);
    }

    #[test]
    fn load_latest_continuation_payload_prefers_most_recent_event() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::new_v4();
        let older = sample_continuation_payload(Uuid::new_v4(), "older");
        let newer = sample_continuation_payload(Uuid::new_v4(), "newer");

        store
            .append(make_event(
                EntityType::Run,
                older.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": older,
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                newer.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": newer.clone(),
                }),
            ))
            .unwrap();

        let loaded = load_latest_continuation_payload_from_store(&store)
            .unwrap()
            .expect("expected continuation payload");
        assert_eq!(loaded.run_id, newer.run_id);
        assert_eq!(loaded.objective, "newer");
    }

    #[test]
    fn load_latest_continuation_payload_skips_malformed_latest_event() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::new_v4();
        let valid = sample_continuation_payload(Uuid::new_v4(), "valid");

        store
            .append(make_event(
                EntityType::Run,
                valid.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": valid.clone(),
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                Uuid::new_v4().to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": {
                        "run_id": "not-a-uuid"
                    },
                }),
            ))
            .unwrap();

        let loaded = load_latest_continuation_payload_from_store(&store)
            .unwrap()
            .expect("expected fallback continuation payload");
        assert_eq!(loaded.run_id, valid.run_id);
        assert_eq!(loaded.objective, "valid");
    }

    #[test]
    fn continuation_payload_round_trips_through_file() {
        use yarli_core::entities::continuation::{ContinuationPayload, RunSummary, TrancheSpec};

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "build everything".into(),
            exit_state: RunState::RunFailed,
            exit_reason: Some(yarli_core::domain::ExitReason::FailedRuntimeError),
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: vec![
                yarli_core::entities::continuation::TaskOutcome {
                    task_id: Uuid::new_v4(),
                    task_key: "build".into(),
                    state: TaskState::TaskComplete,
                    attempt_no: 1,
                    last_error: None,
                    blocker: None,
                },
                yarli_core::entities::continuation::TaskOutcome {
                    task_id: Uuid::new_v4(),
                    task_key: "test".into(),
                    state: TaskState::TaskFailed,
                    attempt_no: 2,
                    last_error: Some("3 tests failed".into()),
                    blocker: None,
                },
            ],
            summary: RunSummary {
                total: 2,
                completed: 1,
                failed: 1,
                cancelled: 0,
                pending: 0,
            },
            next_tranche: Some(TrancheSpec {
                suggested_objective: "Retry failed tasks: test".into(),
                kind: yarli_core::entities::continuation::TrancheKind::RetryUnfinished,
                retry_task_keys: vec!["test".into()],
                unfinished_task_keys: vec![],
                planned_task_keys: vec![],
                planned_tranche_key: None,
                cursor: None,
                config_snapshot: serde_json::json!({"tasks": [{"task_key": "test", "command": "cargo test"}]}),
                interventions: Vec::new(),
            }),
            quality_gate: None,
        };

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("continuation.json");
        let json = serde_json::to_string_pretty(&payload).unwrap();
        std::fs::write(&file_path, &json).unwrap();

        let read_back: ContinuationPayload =
            serde_json::from_str(&std::fs::read_to_string(&file_path).unwrap()).unwrap();

        assert_eq!(read_back.run_id, payload.run_id);
        assert_eq!(read_back.summary.failed, 1);
        assert_eq!(read_back.summary.completed, 1);
        let tranche = read_back.next_tranche.unwrap();
        assert_eq!(tranche.retry_task_keys, vec!["test"]);
    }
}
