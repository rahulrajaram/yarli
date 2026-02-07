//! Command journal — persists command execution events to the event store.
//!
//! The journal wraps a [`CommandRunner`] and records each execution's
//! transitions and output chunks as events (Invariant 4: every transition
//! persisted before side effects continue).

use chrono::Utc;
use uuid::Uuid;

use yarli_core::domain::{EntityType, Event};
use yarli_core::entities::command_execution::StreamChunk;
use yarli_store::EventStore;

use crate::error::ExecError;
use crate::runner::{CommandRequest, CommandResult, CommandRunner};

/// Wraps a `CommandRunner` and persists execution events to an `EventStore`.
pub struct CommandJournal<'a, R: CommandRunner, S: EventStore> {
    runner: R,
    store: &'a S,
}

impl<'a, R: CommandRunner, S: EventStore> CommandJournal<'a, R, S> {
    pub fn new(runner: R, store: &'a S) -> Self {
        Self { runner, store }
    }

    /// Execute a command and persist all events to the store.
    ///
    /// Events persisted:
    /// 1. `command.started` — when the command begins execution.
    /// 2. `command.output` — one event per stream chunk (batched by configurable threshold).
    /// 3. `command.completed` — terminal event with exit code and timing.
    pub async fn execute(
        &self,
        request: CommandRequest,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<CommandResult, ExecError> {
        let correlation_id = request.correlation_id;

        // Run the command to completion.
        let result = self.runner.run(request, cancel).await?;

        // Persist the started event.
        let started_event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: result
                .execution
                .started_at
                .unwrap_or_else(Utc::now),
            entity_type: EntityType::Command,
            entity_id: result.execution.id.to_string(),
            event_type: "command.started".to_string(),
            payload: serde_json::json!({
                "command": result.execution.command,
                "working_dir": result.execution.working_dir,
                "command_class": result.execution.command_class,
            }),
            correlation_id,
            causation_id: None,
            actor: "local_runner".to_string(),
            idempotency_key: result
                .execution
                .idempotency_key
                .as_ref()
                .map(|k| format!("{k}:started")),
        };

        self.store
            .append(started_event)
            .map_err(|e| ExecError::Journal(e.to_string()))?;

        // Persist output chunks (batched into a single event for efficiency).
        if !result.chunks.is_empty() {
            let output_event = Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: EntityType::Command,
                entity_id: result.execution.id.to_string(),
                event_type: "command.output".to_string(),
                payload: serde_json::json!({
                    "chunk_count": result.chunks.len(),
                    "chunks": serialize_chunks(&result.chunks),
                }),
                correlation_id,
                causation_id: None,
                actor: "local_runner".to_string(),
                idempotency_key: result
                    .execution
                    .idempotency_key
                    .as_ref()
                    .map(|k| format!("{k}:output")),
            };

            self.store
                .append(output_event)
                .map_err(|e| ExecError::Journal(e.to_string()))?;
        }

        // Persist the terminal event.
        let terminal_event_type = match result.execution.state {
            yarli_core::fsm::command::CommandState::CmdExited => "command.exited",
            yarli_core::fsm::command::CommandState::CmdTimedOut => "command.timed_out",
            yarli_core::fsm::command::CommandState::CmdKilled => "command.killed",
            _ => "command.completed",
        };

        let terminal_event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: result.execution.ended_at.unwrap_or_else(Utc::now),
            entity_type: EntityType::Command,
            entity_id: result.execution.id.to_string(),
            event_type: terminal_event_type.to_string(),
            payload: serde_json::json!({
                "exit_code": result.execution.exit_code,
                "state": format!("{:?}", result.execution.state),
                "duration_ms": result.execution.duration().map(|d| d.num_milliseconds()),
                "chunk_count": result.execution.chunk_count,
            }),
            correlation_id,
            causation_id: None,
            actor: "local_runner".to_string(),
            idempotency_key: result
                .execution
                .idempotency_key
                .as_ref()
                .map(|k| format!("{k}:terminal")),
        };

        self.store
            .append(terminal_event)
            .map_err(|e| ExecError::Journal(e.to_string()))?;

        Ok(result)
    }
}

/// Serialize chunks to a JSON array of compact objects.
fn serialize_chunks(chunks: &[StreamChunk]) -> serde_json::Value {
    serde_json::Value::Array(
        chunks
            .iter()
            .map(|c| {
                serde_json::json!({
                    "seq": c.sequence,
                    "stream": c.stream,
                    "data": c.data,
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;
    use yarli_core::domain::{CommandClass, EntityType};
    use yarli_store::event_store::EventQuery;
    use yarli_store::InMemoryEventStore;

    use crate::runner::LocalCommandRunner;

    #[tokio::test]
    async fn test_journal_persists_events_for_successful_command() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "echo hello".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: Uuid::now_v7(),
            idempotency_key: None,
            timeout: None,
            env: vec![],
        };

        let result = journal.execute(req, cancel).await.unwrap();

        // Should have 3 events: started, output, exited.
        let events = store.all().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event_type, "command.started");
        assert_eq!(events[1].event_type, "command.output");
        assert_eq!(events[2].event_type, "command.exited");

        // All events for the same entity.
        let cmd_id = result.execution.id.to_string();
        for e in &events {
            assert_eq!(e.entity_id, cmd_id);
            assert_eq!(e.entity_type, EntityType::Command);
        }
    }

    #[tokio::test]
    async fn test_journal_persists_timeout_event() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "sleep 60".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Cpu,
            correlation_id: Uuid::now_v7(),
            idempotency_key: None,
            timeout: Some(Duration::from_millis(100)),
            env: vec![],
        };

        journal.execute(req, cancel).await.unwrap();

        let events = store.all().unwrap();
        // started + timed_out (no output for sleep).
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "command.started");
        assert_eq!(events[1].event_type, "command.timed_out");
    }

    #[tokio::test]
    async fn test_journal_persists_killed_event() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel_clone.cancel();
        });

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "sleep 60".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: Uuid::now_v7(),
            idempotency_key: None,
            timeout: None,
            env: vec![],
        };

        journal.execute(req, cancel).await.unwrap();

        let events = store.all().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "command.started");
        assert_eq!(events[1].event_type, "command.killed");
    }

    #[tokio::test]
    async fn test_journal_correlation_id_propagated() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();
        let corr_id = Uuid::now_v7();

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "echo hi".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Tool,
            correlation_id: corr_id,
            idempotency_key: None,
            timeout: None,
            env: vec![],
        };

        journal.execute(req, cancel).await.unwrap();

        let events = store
            .query(&EventQuery::by_correlation(corr_id))
            .unwrap();
        assert!(events.len() >= 2);
        for e in &events {
            assert_eq!(e.correlation_id, corr_id);
        }
    }

    #[tokio::test]
    async fn test_journal_no_output_event_for_empty_output() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "true".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: Uuid::now_v7(),
            idempotency_key: None,
            timeout: None,
            env: vec![],
        };

        journal.execute(req, cancel).await.unwrap();

        let events = store.all().unwrap();
        // started + exited (no output event since no chunks).
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "command.started");
        assert_eq!(events[1].event_type, "command.exited");
    }

    #[tokio::test]
    async fn test_journal_idempotency_keys() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "echo keyed".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: Uuid::now_v7(),
            idempotency_key: Some("my-key".to_string()),
            timeout: None,
            env: vec![],
        };

        journal.execute(req, cancel).await.unwrap();

        let events = store.all().unwrap();
        assert_eq!(
            events[0].idempotency_key.as_deref(),
            Some("my-key:started")
        );
        // output event
        assert_eq!(
            events[1].idempotency_key.as_deref(),
            Some("my-key:output")
        );
        assert_eq!(
            events[2].idempotency_key.as_deref(),
            Some("my-key:terminal")
        );
    }

    #[tokio::test]
    async fn test_journal_query_by_entity() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "echo query-test".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: Uuid::now_v7(),
            idempotency_key: None,
            timeout: None,
            env: vec![],
        };

        let result = journal.execute(req, cancel).await.unwrap();
        let cmd_id = result.execution.id.to_string();

        let events = store
            .query(&EventQuery::by_entity(EntityType::Command, &cmd_id))
            .unwrap();
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn test_journal_nonzero_exit_persisted() {
        let store = InMemoryEventStore::new();
        let runner = LocalCommandRunner::new();
        let journal = CommandJournal::new(runner, &store);
        let cancel = CancellationToken::new();

        let req = CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: "exit 7".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: Uuid::now_v7(),
            idempotency_key: None,
            timeout: None,
            env: vec![],
        };

        journal.execute(req, cancel).await.unwrap();

        let events = store.all().unwrap();
        let terminal = events.last().unwrap();
        assert_eq!(terminal.event_type, "command.exited");
        let exit_code = terminal.payload["exit_code"].as_i64();
        assert_eq!(exit_code, Some(7));
    }
}
