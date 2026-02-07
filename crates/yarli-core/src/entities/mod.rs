//! Entity models for YARLI orchestration.
//!
//! These are the active domain objects that wrap FSM state enums with
//! metadata, lifecycle methods, and event generation. Each entity's
//! `transition()` method validates state changes and produces a
//! `Transition` event record.

pub mod command_execution;
pub mod merge_intent;
pub mod run;
pub mod task;
pub mod transition;
pub mod worktree_binding;

pub use command_execution::{CommandExecution, StreamChunk, StreamType};
pub use merge_intent::{ConflictRecord, ConflictType, MergeIntent, MergeStrategy};
pub use run::Run;
pub use task::{BlockerCode, Task};
pub use transition::Transition;
pub use worktree_binding::{SubmoduleMode, WorktreeBinding};

#[cfg(test)]
mod tests;
