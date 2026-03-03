//! Shared test helpers used across multiple test modules.

use chrono::Utc;
use std::path::Path;
use uuid::Uuid;

use yarli_core::domain::{EntityType, Event};
use yarli_core::entities::continuation::{ContinuationPayload, RunSummary};
use yarli_core::fsm::run::RunState;

use crate::config::LoadedConfig;

pub(crate) const VALID_UUID: &str = "00000000-0000-0000-0000-000000000000";

pub(crate) fn write_test_config(contents: &str) -> LoadedConfig {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("yarli.toml");
    std::fs::write(&path, contents).unwrap();
    LoadedConfig::load(path).unwrap()
}

pub(crate) fn write_test_config_at(path: &Path, contents: &str) -> LoadedConfig {
    std::fs::write(path, contents).unwrap();
    LoadedConfig::load(path).unwrap()
}

pub(crate) fn make_event(
    entity_type: EntityType,
    entity_id: impl Into<String>,
    event_type: &str,
    correlation_id: Uuid,
    payload: serde_json::Value,
) -> Event {
    Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type,
        entity_id: entity_id.into(),
        event_type: event_type.to_string(),
        payload,
        correlation_id,
        causation_id: None,
        actor: "test".to_string(),
        idempotency_key: None,
    }
}

pub(crate) fn make_command_started(
    command_id: Uuid,
    correlation_id: Uuid,
    command: &str,
    command_class: &str,
) -> Event {
    Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Command,
        entity_id: command_id.to_string(),
        event_type: "command.started".to_string(),
        payload: serde_json::json!({
            "command": command,
            "command_class": command_class,
            "working_dir": "/tmp",
        }),
        correlation_id,
        causation_id: None,
        actor: "test".to_string(),
        idempotency_key: None,
    }
}

pub(crate) fn make_command_terminal(
    command_id: Uuid,
    correlation_id: Uuid,
    event_type: &str,
    duration_ms: u64,
    exit_code: Option<i32>,
) -> Event {
    Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Command,
        entity_id: command_id.to_string(),
        event_type: event_type.to_string(),
        payload: serde_json::json!({
            "duration_ms": duration_ms,
            "exit_code": exit_code,
        }),
        correlation_id,
        causation_id: None,
        actor: "test".to_string(),
        idempotency_key: None,
    }
}

pub(crate) fn sample_continuation_payload(run_id: Uuid, objective: &str) -> ContinuationPayload {
    ContinuationPayload {
        run_id,
        objective: objective.to_string(),
        exit_state: RunState::RunCompleted,
        exit_reason: None,
        cancellation_source: None,
        cancellation_provenance: None,
        completed_at: Utc::now(),
        tasks: Vec::new(),
        summary: RunSummary {
            total: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            pending: 0,
        },
        next_tranche: None,
        quality_gate: None,
        retry_recommendation: None,
    }
}
