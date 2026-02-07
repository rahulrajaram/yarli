//! yarli-core: State machines, domain types, invariants, and explain engine.
//!
//! This crate contains the core domain model for YARLI including:
//! - Run, Task, Worktree, Merge, and Command state machines
//! - Core domain primitives (Run, Task, Evidence, Gate, etc.)
//! - System invariants and transition validation
//! - "Why Not Done?" explain engine

pub mod domain;
pub mod entities;
pub mod error;
pub mod explain;
pub mod fsm;
pub mod shutdown;
