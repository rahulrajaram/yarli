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
pub const MIGRATION_0001_DOWN: &str = r#"
DROP TABLE IF EXISTS task_queue CASCADE;
DROP TABLE IF EXISTS leases CASCADE;
DROP TABLE IF EXISTS gate_results CASCADE;
DROP TABLE IF EXISTS policy_decisions CASCADE;
DROP TABLE IF EXISTS gates CASCADE;
DROP TABLE IF EXISTS evidence CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS command_stream_chunks CASCADE;
DROP TABLE IF EXISTS commands CASCADE;
DROP TABLE IF EXISTS merge_intents CASCADE;
DROP TABLE IF EXISTS worktrees CASCADE;
DROP TABLE IF EXISTS task_dependencies CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS runs CASCADE;
"#;
pub const MIGRATION_0002_DOWN: &str = r#"
DROP INDEX IF EXISTS idx_events_occurred_at;
DROP INDEX IF EXISTS idx_events_entity;
DROP INDEX IF EXISTS idx_events_correlation;
DROP INDEX IF EXISTS ux_events_idempotency_key;
DROP INDEX IF EXISTS idx_tasks_run_state;
DROP INDEX IF EXISTS idx_tasks_state_updated;
DROP INDEX IF EXISTS idx_task_dependencies_depends_on;
DROP INDEX IF EXISTS idx_worktrees_run_state;
DROP INDEX IF EXISTS idx_merge_intents_run_state;
DROP INDEX IF EXISTS idx_merge_intents_worktree;
DROP INDEX IF EXISTS idx_commands_task;
DROP INDEX IF EXISTS idx_commands_run_state;
DROP INDEX IF EXISTS idx_commands_idempotency;
DROP INDEX IF EXISTS idx_command_chunks_captured_at;
DROP INDEX IF EXISTS idx_evidence_task_created;
DROP INDEX IF EXISTS idx_evidence_run_created;
DROP INDEX IF EXISTS ux_gates_scope;
DROP INDEX IF EXISTS idx_gate_results_gate;
DROP INDEX IF EXISTS idx_gate_results_run_task;
DROP INDEX IF EXISTS idx_policy_decisions_run_time;
DROP INDEX IF EXISTS idx_policy_decisions_outcome_time;
DROP INDEX IF EXISTS ux_leases_active_resource;
DROP INDEX IF EXISTS idx_leases_expiry;
DROP INDEX IF EXISTS idx_task_queue_claim;
DROP INDEX IF EXISTS idx_task_queue_lease_expiry;
DROP INDEX IF EXISTS idx_task_queue_run_status;
DROP INDEX IF EXISTS idx_task_queue_class_status;
DROP INDEX IF EXISTS ux_task_queue_active_task;
"#;

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

    #[test]
    fn down_migrations_are_defined() {
        assert!(
            super::MIGRATION_0001_DOWN.contains("DROP TABLE IF EXISTS runs"),
            "expected rollback for runs table"
        );
        assert!(
            super::MIGRATION_0002_DOWN.contains("DROP INDEX IF EXISTS idx_events_occurred_at"),
            "expected down migration rollback for index"
        );
    }
}
