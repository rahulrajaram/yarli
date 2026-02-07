//! Command execution state machine (Section 7.5).
//!
//! Rules:
//! - Every command has start timestamp, end timestamp, exit reason, and exit code.
//! - Stderr/stdout chunks must be associated with command ID and sequence number.

use serde::{Deserialize, Serialize};

/// Command execution lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandState {
    CmdQueued,
    CmdStarted,
    CmdStreaming,
    CmdExited,
    CmdTimedOut,
    CmdKilled,
}

impl CommandState {
    /// Terminal states cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            CommandState::CmdExited | CommandState::CmdTimedOut | CommandState::CmdKilled
        )
    }

    /// Returns the set of states reachable from this state.
    pub fn valid_transitions(self) -> &'static [CommandState] {
        use CommandState::*;
        match self {
            CmdQueued => &[CmdStarted, CmdKilled],
            CmdStarted => &[CmdStreaming, CmdExited, CmdTimedOut, CmdKilled],
            CmdStreaming => &[CmdExited, CmdTimedOut, CmdKilled],
            CmdExited => &[],
            CmdTimedOut => &[],
            CmdKilled => &[],
        }
    }

    /// Check if transitioning to `target` is valid.
    pub fn can_transition_to(self, target: CommandState) -> bool {
        self.valid_transitions().contains(&target)
    }
}
