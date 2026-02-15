use std::path::Path;

use anyhow::{anyhow, Context, Result};
use tracing::warn;
use uuid::Uuid;

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
