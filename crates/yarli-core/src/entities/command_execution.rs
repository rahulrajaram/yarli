//! Command execution — immutable record of command invocation and result.
//!
//! Every command run by the orchestrator produces a `CommandExecution` record
//! that captures the full invocation context, timing, exit status, and output
//! stream references (Sections 6, 7.5).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{CommandClass, CorrelationId, EventId, RunId, TaskId};
use crate::error::TransitionError;
use crate::fsm::command::CommandState;

use super::transition::Transition;

/// An immutable record of a command execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandExecution {
    /// Unique command execution ID.
    pub id: Uuid,
    /// The task that owns this command.
    pub task_id: TaskId,
    /// The run this command belongs to.
    pub run_id: RunId,
    /// Command string (program + arguments).
    pub command: String,
    /// Working directory for the command.
    pub working_dir: String,
    /// Command class for concurrency accounting.
    pub command_class: CommandClass,
    /// Current FSM state.
    pub state: CommandState,
    /// When the command was queued.
    pub queued_at: DateTime<Utc>,
    /// When the command started executing.
    pub started_at: Option<DateTime<Utc>>,
    /// When the command finished (exited/timed-out/killed).
    pub ended_at: Option<DateTime<Utc>>,
    /// Process exit code (None if not yet exited or killed).
    pub exit_code: Option<i32>,
    /// Idempotency key for deduplication of side-effecting commands.
    pub idempotency_key: Option<String>,
    /// Correlation ID.
    pub correlation_id: CorrelationId,
    /// Number of stdout/stderr chunks collected.
    pub chunk_count: u64,
}

/// A chunk of command output (stdout or stderr).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamChunk {
    /// The command this chunk belongs to.
    pub command_id: Uuid,
    /// Sequence number for ordering.
    pub sequence: u64,
    /// Stream type: "stdout" or "stderr".
    pub stream: StreamType,
    /// The content of this chunk.
    pub data: String,
    /// When this chunk was captured.
    pub captured_at: DateTime<Utc>,
}

/// Which output stream a chunk came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamType {
    Stdout,
    Stderr,
}

impl CommandExecution {
    /// Create a new command execution in `CmdQueued` state.
    pub fn new(
        task_id: TaskId,
        run_id: RunId,
        command: impl Into<String>,
        working_dir: impl Into<String>,
        command_class: CommandClass,
        correlation_id: CorrelationId,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            task_id,
            run_id,
            command: command.into(),
            working_dir: working_dir.into(),
            command_class,
            state: CommandState::CmdQueued,
            queued_at: Utc::now(),
            started_at: None,
            ended_at: None,
            exit_code: None,
            idempotency_key: None,
            correlation_id,
            chunk_count: 0,
        }
    }

    /// Set an idempotency key for deduplication.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Attempt a state transition. Returns a `Transition` event on success.
    pub fn transition(
        &mut self,
        to: CommandState,
        reason: impl Into<String>,
        actor: impl Into<String>,
        causation_id: Option<EventId>,
    ) -> Result<Transition, TransitionError> {
        let from = self.state;

        if from.is_terminal() {
            return Err(TransitionError::TerminalState(format!("{from:?}")));
        }

        if !from.can_transition_to(to) {
            return Err(TransitionError::InvalidCommandTransition { from, to });
        }

        let reason_str = reason.into();
        let actor_str = actor.into();
        let now = Utc::now();

        // Track timing.
        match to {
            CommandState::CmdStarted | CommandState::CmdStreaming => {
                if self.started_at.is_none() {
                    self.started_at = Some(now);
                }
            }
            CommandState::CmdExited | CommandState::CmdTimedOut | CommandState::CmdKilled => {
                self.ended_at = Some(now);
            }
            _ => {}
        }

        self.state = to;

        Ok(Transition::new(
            "command",
            self.id,
            format!("{from:?}"),
            format!("{to:?}"),
            reason_str,
            actor_str,
            self.correlation_id,
            causation_id,
        ))
    }

    /// Record a successful exit with exit code.
    pub fn exit(
        &mut self,
        exit_code: i32,
        actor: impl Into<String>,
        causation_id: Option<EventId>,
    ) -> Result<Transition, TransitionError> {
        self.exit_code = Some(exit_code);
        self.transition(
            CommandState::CmdExited,
            format!("exit_code={exit_code}"),
            actor,
            causation_id,
        )
    }

    /// Duration of the command (from start to end or now).
    pub fn duration(&self) -> Option<chrono::Duration> {
        let start = self.started_at?;
        let end = self.ended_at.unwrap_or_else(Utc::now);
        Some(end - start)
    }
}
