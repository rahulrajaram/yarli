-- Allow drained runs to persist into the materialized runs table.

ALTER TABLE runs
    DROP CONSTRAINT IF EXISTS runs_state_check;

ALTER TABLE runs
    ADD CONSTRAINT runs_state_check
    CHECK (
        state IN (
            'RUN_OPEN',
            'RUN_ACTIVE',
            'RUN_VERIFYING',
            'RUN_BLOCKED',
            'RUN_FAILED',
            'RUN_COMPLETED',
            'RUN_COMPLETED_WITH_MERGE_FAILURE',
            'RUN_CANCELLED',
            'RUN_DRAINED'
        )
    );

ALTER TABLE runs
    DROP CONSTRAINT IF EXISTS runs_exit_reason_check;

ALTER TABLE runs
    ADD CONSTRAINT runs_exit_reason_check
    CHECK (
        exit_reason IS NULL
        OR exit_reason IN (
            'completed_all_gates',
            'blocked_open_tasks',
            'blocked_gate_failure',
            'merge_conflict',
            'failed_policy_denial',
            'failed_runtime_error',
            'completed_merge_teardown_failed',
            'cancelled_by_operator',
            'drained_by_operator',
            'timed_out',
            'stalled_no_progress'
        )
    );
