//! yarli-store: Event store and repositories.
//!
//! Provides the [`EventStore`] trait and an in-memory implementation
//! for development and testing.
//!
//! SQL migrations for durable backends are stored under `migrations/`.

pub mod error;
pub mod event_store;
pub mod memory;
pub mod postgres;

pub const MIGRATION_0001_INIT: &str = include_str!("../migrations/0001_init.sql");
pub const MIGRATION_0002_INDEXES: &str = include_str!("../migrations/0002_indexes.sql");

pub use error::StoreError;
pub use event_store::EventStore;
pub use memory::InMemoryEventStore;
pub use postgres::PostgresEventStore;

#[cfg(test)]
mod migration_tests {
    use super::{MIGRATION_0001_INIT, MIGRATION_0002_INDEXES};

    #[test]
    fn init_migration_contains_required_tables() {
        let required_tables = [
            "events",
            "runs",
            "tasks",
            "task_dependencies",
            "worktrees",
            "merge_intents",
            "commands",
            "command_stream_chunks",
            "evidence",
            "gates",
            "gate_results",
            "policy_decisions",
            "leases",
            "task_queue",
        ];

        for table in required_tables {
            assert!(
                MIGRATION_0001_INIT.contains(&format!("CREATE TABLE IF NOT EXISTS {table}")),
                "missing table in init migration: {table}"
            );
        }
    }

    #[test]
    fn index_migration_contains_idempotency_and_queue_indexes() {
        assert!(
            MIGRATION_0002_INDEXES.contains("ux_events_idempotency_key"),
            "expected idempotency uniqueness index"
        );
        assert!(
            MIGRATION_0002_INDEXES.contains("idx_task_queue_claim"),
            "expected queue claim index for SKIP LOCKED path"
        );
    }
}
