-- YARLI Loop-2 baseline schema.
-- Creates required materialized/state, policy/gate lineage, and queue tables.

CREATE TABLE IF NOT EXISTS runs (
    run_id UUID PRIMARY KEY,
    objective TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'RUN_OPEN',
            'RUN_ACTIVE',
            'RUN_VERIFYING',
            'RUN_BLOCKED',
            'RUN_FAILED',
            'RUN_COMPLETED',
            'RUN_CANCELLED',
            'RUN_DRAINED'
        )
    ),
    safe_mode TEXT NOT NULL CHECK (
        safe_mode IN ('observe', 'execute', 'restricted', 'breakglass')
    ),
    exit_reason TEXT NULL CHECK (
        exit_reason IS NULL
        OR exit_reason IN (
            'completed_all_gates',
            'blocked_open_tasks',
            'blocked_gate_failure',
            'failed_policy_denial',
            'failed_runtime_error',
            'cancelled_by_operator',
            'drained_by_operator',
            'timed_out',
            'stalled_no_progress'
        )
    ),
    correlation_id UUID NOT NULL,
    config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    task_key TEXT NOT NULL,
    description TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'TASK_OPEN',
            'TASK_READY',
            'TASK_EXECUTING',
            'TASK_WAITING',
            'TASK_BLOCKED',
            'TASK_VERIFYING',
            'TASK_COMPLETE',
            'TASK_FAILED',
            'TASK_CANCELLED'
        )
    ),
    command_class TEXT NOT NULL CHECK (command_class IN ('io', 'cpu', 'git', 'tool')),
    attempt_no INTEGER NOT NULL DEFAULT 1 CHECK (attempt_no >= 1),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    blocker_code TEXT NULL,
    correlation_id UUID NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3 CHECK (priority >= 0 AND priority <= 100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, task_key),
    UNIQUE (task_id, run_id)
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id UUID NOT NULL,
    depends_on_task_id UUID NOT NULL,
    run_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (task_id, depends_on_task_id),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE CASCADE,
    FOREIGN KEY (depends_on_task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE CASCADE,
    CHECK (task_id <> depends_on_task_id)
);

CREATE TABLE IF NOT EXISTS worktrees (
    worktree_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    task_id UUID NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'WT_UNBOUND',
            'WT_CREATING',
            'WT_BOUND_HOME',
            'WT_SWITCH_PENDING',
            'WT_BOUND_NON_HOME',
            'WT_MERGING',
            'WT_CONFLICT',
            'WT_RECOVERING',
            'WT_CLEANUP_PENDING',
            'WT_CLOSED'
        )
    ),
    repo_root TEXT NOT NULL,
    worktree_path TEXT NOT NULL,
    branch_name TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    head_ref TEXT NOT NULL,
    dirty BOOLEAN NOT NULL DEFAULT FALSE,
    submodule_state_hash TEXT NULL,
    submodule_mode TEXT NOT NULL DEFAULT 'locked' CHECK (
        submodule_mode IN ('locked', 'allow_fast_forward', 'allow_any')
    ),
    lease_owner TEXT NULL,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (worktree_id, run_id),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS merge_intents (
    merge_intent_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    worktree_id UUID NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'MERGE_REQUESTED',
            'MERGE_PRECHECK',
            'MERGE_DRY_RUN',
            'MERGE_APPLY',
            'MERGE_VERIFY',
            'MERGE_DONE',
            'MERGE_CONFLICT',
            'MERGE_ABORTED'
        )
    ),
    source_ref TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    source_sha TEXT NULL,
    target_sha TEXT NULL,
    strategy TEXT NOT NULL DEFAULT 'merge_no_ff' CHECK (
        strategy IN ('merge_no_ff', 'rebase_then_ff', 'squash_merge')
    ),
    result_sha TEXT NULL,
    conflicts JSONB NOT NULL DEFAULT '[]'::jsonb,
    approval_token_id TEXT NULL,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (worktree_id, run_id) REFERENCES worktrees(worktree_id, run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS commands (
    command_id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    command TEXT NOT NULL,
    working_dir TEXT NOT NULL,
    command_class TEXT NOT NULL CHECK (command_class IN ('io', 'cpu', 'git', 'tool')),
    state TEXT NOT NULL CHECK (
        state IN (
            'CMD_QUEUED',
            'CMD_STARTED',
            'CMD_STREAMING',
            'CMD_EXITED',
            'CMD_TIMED_OUT',
            'CMD_KILLED'
        )
    ),
    queued_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NULL,
    ended_at TIMESTAMPTZ NULL,
    exit_code INTEGER NULL,
    idempotency_key TEXT NULL,
    correlation_id UUID NOT NULL,
    chunk_count BIGINT NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS command_stream_chunks (
    command_id UUID NOT NULL REFERENCES commands(command_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL CHECK (sequence >= 1),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    data TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (command_id, sequence)
);

CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    entity_type TEXT NOT NULL CHECK (
        entity_type IN ('run', 'task', 'worktree', 'merge', 'command', 'gate', 'policy')
    ),
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID NULL REFERENCES events(event_id) ON DELETE SET NULL,
    actor TEXT NOT NULL,
    idempotency_key TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    evidence_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS gates (
    gate_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    task_id UUID NULL,
    scope TEXT NOT NULL CHECK (scope IN ('run', 'task')),
    gate_type TEXT NOT NULL CHECK (
        gate_type IN (
            'gate.required_tasks_closed',
            'gate.required_evidence_present',
            'gate.tests_passed',
            'gate.no_unapproved_git_ops',
            'gate.no_unresolved_conflicts',
            'gate.worktree_consistent',
            'gate.policy_clean'
        )
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE CASCADE,
    CHECK (
        (scope = 'run' AND task_id IS NULL)
        OR (scope = 'task' AND task_id IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS policy_decisions (
    decision_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    task_id UUID NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('ALLOW', 'DENY', 'REQUIRE_APPROVAL')),
    rule_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL,
    approval_token_id TEXT NULL,
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS gate_results (
    gate_result_id UUID PRIMARY KEY,
    gate_id UUID NOT NULL REFERENCES gates(gate_id) ON DELETE CASCADE,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    task_id UUID NULL,
    status TEXT NOT NULL CHECK (status IN ('passed', 'failed', 'pending')),
    reason TEXT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
    policy_decision_id UUID NULL REFERENCES policy_decisions(decision_id) ON DELETE SET NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS leases (
    lease_id UUID PRIMARY KEY,
    resource_type TEXT NOT NULL CHECK (
        resource_type IN ('task_queue', 'worktree', 'merge', 'command', 'run')
    ),
    resource_id UUID NOT NULL,
    owner TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ NULL,
    released_at TIMESTAMPTZ NULL,
    CHECK (expires_at > acquired_at)
);

CREATE TABLE IF NOT EXISTS task_queue (
    queue_id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    priority INTEGER NOT NULL CHECK (priority >= 0 AND priority <= 100),
    available_at TIMESTAMPTZ NOT NULL,
    attempt_no INTEGER NOT NULL DEFAULT 1 CHECK (attempt_no >= 1),
    command_class TEXT NOT NULL CHECK (command_class IN ('io', 'cpu', 'git', 'tool')),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'leased', 'completed', 'failed', 'cancelled')
    ),
    lease_owner TEXT NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    last_heartbeat TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (task_id, run_id) REFERENCES tasks(task_id, run_id) ON DELETE CASCADE,
    CHECK (
        status <> 'leased'
        OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);
