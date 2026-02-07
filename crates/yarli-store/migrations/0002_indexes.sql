-- YARLI Loop-2 baseline indexes.
-- Includes idempotency uniqueness and queue claim/reclaim support.

-- events
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events (occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_entity ON events (entity_type, entity_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_correlation ON events (correlation_id, occurred_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_idempotency_key
    ON events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- runs/tasks
CREATE INDEX IF NOT EXISTS idx_tasks_run_state ON tasks (run_id, state);
CREATE INDEX IF NOT EXISTS idx_tasks_state_updated ON tasks (state, updated_at);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_depends_on ON task_dependencies (depends_on_task_id);

-- worktrees/merge
CREATE INDEX IF NOT EXISTS idx_worktrees_run_state ON worktrees (run_id, state);
CREATE INDEX IF NOT EXISTS idx_merge_intents_run_state ON merge_intents (run_id, state);
CREATE INDEX IF NOT EXISTS idx_merge_intents_worktree ON merge_intents (worktree_id);

-- command execution streams
CREATE INDEX IF NOT EXISTS idx_commands_task ON commands (task_id, queued_at);
CREATE INDEX IF NOT EXISTS idx_commands_run_state ON commands (run_id, state);
CREATE INDEX IF NOT EXISTS idx_commands_idempotency ON commands (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_command_chunks_captured_at ON command_stream_chunks (captured_at);

-- evidence / gates / policy lineage
CREATE INDEX IF NOT EXISTS idx_evidence_task_created ON evidence (task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_evidence_run_created ON evidence (run_id, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS ux_gates_scope
    ON gates (run_id, task_id, gate_type);
CREATE INDEX IF NOT EXISTS idx_gate_results_gate ON gate_results (gate_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_gate_results_run_task ON gate_results (run_id, task_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_policy_decisions_run_time ON policy_decisions (run_id, decided_at);
CREATE INDEX IF NOT EXISTS idx_policy_decisions_outcome_time ON policy_decisions (outcome, decided_at);

-- leases
CREATE UNIQUE INDEX IF NOT EXISTS ux_leases_active_resource
    ON leases (resource_type, resource_id)
    WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_leases_expiry ON leases (expires_at)
    WHERE released_at IS NULL;

-- task_queue claim path (FOR UPDATE SKIP LOCKED) and lease maintenance
CREATE INDEX IF NOT EXISTS idx_task_queue_claim
    ON task_queue (priority, available_at, queue_id)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_task_queue_lease_expiry
    ON task_queue (lease_expires_at)
    WHERE status = 'leased';
CREATE INDEX IF NOT EXISTS idx_task_queue_run_status ON task_queue (run_id, status);
CREATE INDEX IF NOT EXISTS idx_task_queue_class_status ON task_queue (command_class, status);
CREATE UNIQUE INDEX IF NOT EXISTS ux_task_queue_active_task
    ON task_queue (task_id)
    WHERE status IN ('pending', 'leased');
