//! Tests for entity models: Run, Task, CommandExecution, Transition, WorktreeBinding, and MergeIntent.

#[cfg(test)]
mod run_tests {
    use crate::domain::{ExitReason, SafeMode};
    use crate::entities::Run;
    use crate::fsm::run::RunState;
    use uuid::Uuid;

    #[test]
    fn new_run_starts_in_open_state() {
        let run = Run::new("test objective", SafeMode::Execute);
        assert_eq!(run.state, RunState::RunOpen);
        assert_eq!(run.objective, "test objective");
        assert_eq!(run.safe_mode, SafeMode::Execute);
        assert!(run.exit_reason.is_none());
        assert!(run.task_ids.is_empty());
    }

    #[test]
    fn run_with_config_snapshot() {
        let config = serde_json::json!({"timeout": 30, "retries": 3});
        let run = Run::with_config("obj", SafeMode::Execute, config.clone());
        assert_eq!(run.config_snapshot, config);
    }

    #[test]
    fn run_add_task_deduplicates() {
        let mut run = Run::new("obj", SafeMode::Execute);
        let task_id = Uuid::now_v7();
        run.add_task(task_id);
        run.add_task(task_id); // duplicate
        assert_eq!(run.task_ids.len(), 1);
    }

    #[test]
    fn run_open_to_active() {
        let mut run = Run::new("obj", SafeMode::Execute);
        let t = run
            .transition(RunState::RunActive, "tasks ready", "system", None)
            .unwrap();
        assert_eq!(run.state, RunState::RunActive);
        assert_eq!(t.from_state, "RunOpen");
        assert_eq!(t.to_state, "RunActive");
        assert_eq!(t.entity_kind, "run");
        assert_eq!(t.reason, "tasks ready");
        assert_eq!(t.actor, "system");
    }

    #[test]
    fn run_full_happy_path() {
        let mut run = Run::new("build and test", SafeMode::Execute);

        // Open -> Active
        run.transition(RunState::RunActive, "starting", "system", None)
            .unwrap();
        assert_eq!(run.state, RunState::RunActive);

        // Active -> Verifying
        run.transition(RunState::RunVerifying, "all tasks done", "system", None)
            .unwrap();
        assert_eq!(run.state, RunState::RunVerifying);

        // Verifying -> Completed
        run.transition(RunState::RunCompleted, "gates passed", "system", None)
            .unwrap();
        assert_eq!(run.state, RunState::RunCompleted);
        assert_eq!(run.exit_reason, Some(ExitReason::CompletedAllGates));
    }

    #[test]
    fn run_blocked_and_recovery() {
        let mut run = Run::new("obj", SafeMode::Execute);
        run.transition(RunState::RunActive, "starting", "system", None)
            .unwrap();

        // Active -> Blocked
        run.transition(RunState::RunBlocked, "task failed", "system", None)
            .unwrap();
        assert_eq!(run.state, RunState::RunBlocked);

        // Blocked -> Active (unblocked)
        run.transition(RunState::RunActive, "task retried", "operator", None)
            .unwrap();
        assert_eq!(run.state, RunState::RunActive);
    }

    #[test]
    fn run_cancellation() {
        let mut run = Run::new("obj", SafeMode::Execute);
        run.transition(RunState::RunActive, "starting", "system", None)
            .unwrap();

        run.transition(RunState::RunCancelled, "user requested", "operator", None)
            .unwrap();
        assert_eq!(run.state, RunState::RunCancelled);
        assert_eq!(run.exit_reason, Some(ExitReason::CancelledByOperator));
    }

    #[test]
    fn run_failure_with_policy_reason() {
        let mut run = Run::new("obj", SafeMode::Execute);
        run.transition(RunState::RunActive, "starting", "system", None)
            .unwrap();

        run.transition(RunState::RunFailed, "policy denial", "policy_engine", None)
            .unwrap();
        assert_eq!(run.exit_reason, Some(ExitReason::FailedPolicyDenial));
    }

    #[test]
    fn run_failure_with_gate_reason() {
        let mut run = Run::new("obj", SafeMode::Execute);
        run.transition(RunState::RunActive, "starting", "system", None)
            .unwrap();

        run.transition(
            RunState::RunFailed,
            "2 gate(s) failed: evidence_present: missing; tests_passed: not run",
            "scheduler",
            None,
        )
        .unwrap();
        assert_eq!(run.exit_reason, Some(ExitReason::BlockedGateFailure));
    }

    #[test]
    fn run_terminal_state_rejects_transition() {
        let mut run = Run::new("obj", SafeMode::Execute);
        run.transition(RunState::RunActive, "starting", "system", None)
            .unwrap();
        run.transition(RunState::RunCancelled, "cancelled", "operator", None)
            .unwrap();

        let err = run
            .transition(RunState::RunActive, "try again", "system", None)
            .unwrap_err();
        assert!(format!("{err}").contains("terminal"));
    }

    #[test]
    fn run_invalid_transition_rejected() {
        let mut run = Run::new("obj", SafeMode::Execute);
        // Cannot go directly from Open to Completed
        let err = run
            .transition(RunState::RunCompleted, "skip", "system", None)
            .unwrap_err();
        assert!(format!("{err}").contains("invalid run transition"));
    }

    #[test]
    fn run_transition_has_correlation_id() {
        let mut run = Run::new("obj", SafeMode::Execute);
        let t = run
            .transition(RunState::RunActive, "start", "system", None)
            .unwrap();
        assert_eq!(t.correlation_id, run.correlation_id);
    }

    #[test]
    fn run_transition_with_causation_id() {
        let mut run = Run::new("obj", SafeMode::Execute);
        let cause = Uuid::now_v7();
        let t = run
            .transition(RunState::RunActive, "start", "system", Some(cause))
            .unwrap();
        assert_eq!(t.causation_id, Some(cause));
    }
}

#[cfg(test)]
mod task_tests {
    use crate::domain::CommandClass;
    use crate::entities::task::BlockerCode;
    use crate::entities::Task;
    use crate::fsm::task::TaskState;
    use uuid::Uuid;

    fn make_task() -> Task {
        Task::new(
            Uuid::now_v7(),
            "build",
            "compile the project",
            CommandClass::Cpu,
            Uuid::now_v7(),
        )
    }

    #[test]
    fn new_task_starts_in_open_state() {
        let task = make_task();
        assert_eq!(task.state, TaskState::TaskOpen);
        assert_eq!(task.task_key, "build");
        assert_eq!(task.attempt_no, 1);
        assert_eq!(task.max_attempts, 3);
        assert!(task.blocker.is_none());
    }

    #[test]
    fn task_full_happy_path() {
        let mut task = make_task();

        // Open -> Ready
        task.transition(TaskState::TaskReady, "deps met", "scheduler", None)
            .unwrap();
        assert_eq!(task.state, TaskState::TaskReady);

        // Ready -> Executing
        task.transition(TaskState::TaskExecuting, "worker claimed", "worker-1", None)
            .unwrap();
        assert_eq!(task.state, TaskState::TaskExecuting);

        // Executing -> Verifying
        task.transition(
            TaskState::TaskVerifying,
            "command exited 0",
            "worker-1",
            None,
        )
        .unwrap();
        assert_eq!(task.state, TaskState::TaskVerifying);

        // Verifying -> Complete
        task.transition(TaskState::TaskComplete, "gates passed", "gate_engine", None)
            .unwrap();
        assert_eq!(task.state, TaskState::TaskComplete);
    }

    #[test]
    fn task_block_with_blocker_code() {
        let mut task = make_task();
        task.transition(TaskState::TaskReady, "deps met", "scheduler", None)
            .unwrap();

        task.block(
            BlockerCode::DependencyPending,
            "waiting for lint",
            "scheduler",
            None,
        )
        .unwrap();
        assert_eq!(task.state, TaskState::TaskBlocked);
        assert_eq!(task.blocker, Some(BlockerCode::DependencyPending));
    }

    #[test]
    fn task_unblock_clears_blocker() {
        let mut task = make_task();
        task.transition(TaskState::TaskReady, "deps met", "scheduler", None)
            .unwrap();
        task.block(BlockerCode::DependencyPending, "waiting", "scheduler", None)
            .unwrap();

        // Blocked -> Ready (unblocked)
        task.transition(
            TaskState::TaskReady,
            "dependency resolved",
            "scheduler",
            None,
        )
        .unwrap();
        assert_eq!(task.state, TaskState::TaskReady);
        assert!(task.blocker.is_none());
    }

    #[test]
    fn task_retry_increments_attempt() {
        let mut task = make_task();
        assert_eq!(task.attempt_no, 1);

        // Open -> Ready -> Executing -> Failed
        task.transition(TaskState::TaskReady, "ready", "scheduler", None)
            .unwrap();
        task.transition(TaskState::TaskExecuting, "claimed", "worker-1", None)
            .unwrap();
        task.transition(TaskState::TaskFailed, "exit code 1", "worker-1", None)
            .unwrap();

        // Failed -> Ready (retry)
        task.transition(TaskState::TaskReady, "retry", "operator", None)
            .unwrap();
        assert_eq!(task.attempt_no, 2);
        assert_eq!(task.state, TaskState::TaskReady);
    }

    #[test]
    fn task_retry_fails_at_max_attempts() {
        let mut task = make_task().with_max_attempts(1);

        task.transition(TaskState::TaskReady, "ready", "scheduler", None)
            .unwrap();
        task.transition(TaskState::TaskExecuting, "claimed", "worker", None)
            .unwrap();
        task.transition(TaskState::TaskFailed, "error", "worker", None)
            .unwrap();

        // Should fail — max attempts reached (1 attempt used, max is 1)
        let err = task
            .transition(TaskState::TaskReady, "retry", "operator", None)
            .unwrap_err();
        assert!(format!("{err}").contains("max attempts"));
    }

    #[test]
    fn task_terminal_state_rejects_non_retry() {
        let mut task = make_task();
        task.transition(TaskState::TaskReady, "ready", "scheduler", None)
            .unwrap();
        task.transition(TaskState::TaskExecuting, "claimed", "worker", None)
            .unwrap();
        task.transition(TaskState::TaskVerifying, "exited", "worker", None)
            .unwrap();
        task.transition(TaskState::TaskComplete, "gates ok", "gate_engine", None)
            .unwrap();

        let err = task
            .transition(TaskState::TaskReady, "redo", "operator", None)
            .unwrap_err();
        assert!(format!("{err}").contains("terminal"));
    }

    #[test]
    fn task_dependencies_satisfied() {
        let mut task = make_task();
        let dep1 = Uuid::now_v7();
        let dep2 = Uuid::now_v7();
        task.depends_on(dep1);
        task.depends_on(dep2);

        // Only dep1 complete
        assert!(!task.dependencies_satisfied(|id| *id == dep1));

        // Both complete
        assert!(task.dependencies_satisfied(|_| true));

        // Neither complete
        assert!(!task.dependencies_satisfied(|_| false));
    }

    #[test]
    fn task_dependency_deduplication() {
        let mut task = make_task();
        let dep = Uuid::now_v7();
        task.depends_on(dep);
        task.depends_on(dep); // duplicate
        assert_eq!(task.depends_on.len(), 1);
    }

    #[test]
    fn task_evidence_attachment() {
        let mut task = make_task();
        let ev1 = Uuid::now_v7();
        let ev2 = Uuid::now_v7();
        task.attach_evidence(ev1);
        task.attach_evidence(ev2);
        task.attach_evidence(ev1); // duplicate
        assert_eq!(task.evidence_ids.len(), 2);
    }

    #[test]
    fn task_priority_and_max_attempts_builder() {
        let task = make_task().with_priority(1).with_max_attempts(5);
        assert_eq!(task.priority, 1);
        assert_eq!(task.max_attempts, 5);
    }

    #[test]
    fn task_invalid_transition_rejected() {
        let mut task = make_task();
        // Cannot go directly from Open to Complete
        let err = task
            .transition(TaskState::TaskComplete, "skip", "system", None)
            .unwrap_err();
        assert!(format!("{err}").contains("invalid task transition"));
    }

    #[test]
    fn task_transition_carries_correlation_id() {
        let mut task = make_task();
        let t = task
            .transition(TaskState::TaskReady, "ready", "scheduler", None)
            .unwrap();
        assert_eq!(t.correlation_id, task.correlation_id);
        assert_eq!(t.entity_kind, "task");
    }

    #[test]
    fn last_error_set_on_first_call_only() {
        let mut task = make_task();
        assert!(task.last_error.is_none());

        task.set_last_error("command exited with code 1");
        assert_eq!(
            task.last_error.as_deref(),
            Some("command exited with code 1")
        );

        // Second call should not overwrite.
        task.set_last_error("command killed");
        assert_eq!(
            task.last_error.as_deref(),
            Some("command exited with code 1")
        );
    }

    #[test]
    fn blocker_detail_set_and_clear() {
        let mut task = make_task();
        assert!(task.blocker_detail.is_none());

        task.set_blocker_detail("see blocker-001.md");
        assert_eq!(task.blocker_detail.as_deref(), Some("see blocker-001.md"));

        // Overwrite is allowed for blocker_detail.
        task.set_blocker_detail("see blocker-002.md");
        assert_eq!(task.blocker_detail.as_deref(), Some("see blocker-002.md"));

        task.clear_blocker_detail();
        assert!(task.blocker_detail.is_none());
    }

    #[test]
    fn new_task_has_no_last_error_or_blocker_detail() {
        let task = make_task();
        assert!(task.last_error.is_none());
        assert!(task.blocker_detail.is_none());
    }

    #[test]
    fn last_error_preserved_across_transitions() {
        let mut task = make_task();

        // Move to executing.
        task.transition(TaskState::TaskReady, "deps met", "scheduler", None)
            .unwrap();
        task.transition(TaskState::TaskExecuting, "claimed", "worker", None)
            .unwrap();

        // First failure sets last_error.
        task.set_last_error("command exited with code 1");
        task.transition(TaskState::TaskFailed, "nonzero exit", "worker", None)
            .unwrap();
        assert_eq!(
            task.last_error.as_deref(),
            Some("command exited with code 1")
        );

        // Retry and kill — last_error preserved.
        task.transition(TaskState::TaskReady, "retry", "scheduler", None)
            .unwrap();
        task.transition(TaskState::TaskExecuting, "claimed", "worker", None)
            .unwrap();
        task.set_last_error("command killed"); // Should NOT overwrite.
        task.transition(TaskState::TaskFailed, "killed", "worker", None)
            .unwrap();
        assert_eq!(
            task.last_error.as_deref(),
            Some("command exited with code 1")
        );
    }
}

#[cfg(test)]
mod command_execution_tests {
    use crate::domain::CommandClass;
    use crate::entities::CommandExecution;
    use crate::fsm::command::CommandState;
    use uuid::Uuid;

    fn make_cmd() -> CommandExecution {
        CommandExecution::new(
            Uuid::now_v7(),
            Uuid::now_v7(),
            "cargo test",
            "/home/user/project",
            CommandClass::Cpu,
            Uuid::now_v7(),
        )
    }

    #[test]
    fn new_command_starts_queued() {
        let cmd = make_cmd();
        assert_eq!(cmd.state, CommandState::CmdQueued);
        assert_eq!(cmd.command, "cargo test");
        assert!(cmd.started_at.is_none());
        assert!(cmd.ended_at.is_none());
        assert!(cmd.exit_code.is_none());
    }

    #[test]
    fn command_full_lifecycle() {
        let mut cmd = make_cmd();

        // Queued -> Started
        cmd.transition(CommandState::CmdStarted, "spawned", "worker-1", None)
            .unwrap();
        assert_eq!(cmd.state, CommandState::CmdStarted);
        assert!(cmd.started_at.is_some());

        // Started -> Streaming
        cmd.transition(
            CommandState::CmdStreaming,
            "output flowing",
            "worker-1",
            None,
        )
        .unwrap();
        assert_eq!(cmd.state, CommandState::CmdStreaming);

        // Streaming -> Exited
        cmd.exit(0, "worker-1", None).unwrap();
        assert_eq!(cmd.state, CommandState::CmdExited);
        assert_eq!(cmd.exit_code, Some(0));
        assert!(cmd.ended_at.is_some());
    }

    #[test]
    fn command_timeout() {
        let mut cmd = make_cmd();
        cmd.transition(CommandState::CmdStarted, "spawned", "worker-1", None)
            .unwrap();
        cmd.transition(CommandState::CmdStreaming, "output", "worker-1", None)
            .unwrap();

        cmd.transition(CommandState::CmdTimedOut, "30s timeout", "scheduler", None)
            .unwrap();
        assert_eq!(cmd.state, CommandState::CmdTimedOut);
        assert!(cmd.ended_at.is_some());
    }

    #[test]
    fn command_killed() {
        let mut cmd = make_cmd();
        cmd.transition(CommandState::CmdStarted, "spawned", "worker-1", None)
            .unwrap();

        cmd.transition(CommandState::CmdKilled, "shutdown signal", "system", None)
            .unwrap();
        assert_eq!(cmd.state, CommandState::CmdKilled);
        assert!(cmd.ended_at.is_some());
    }

    #[test]
    fn command_killed_from_queued() {
        let mut cmd = make_cmd();
        cmd.transition(CommandState::CmdKilled, "run cancelled", "system", None)
            .unwrap();
        assert_eq!(cmd.state, CommandState::CmdKilled);
    }

    #[test]
    fn command_terminal_state_rejects_transition() {
        let mut cmd = make_cmd();
        cmd.transition(CommandState::CmdStarted, "spawned", "worker", None)
            .unwrap();
        cmd.exit(0, "worker", None).unwrap();

        let err = cmd
            .transition(CommandState::CmdStarted, "restart", "worker", None)
            .unwrap_err();
        assert!(format!("{err}").contains("terminal"));
    }

    #[test]
    fn command_invalid_transition_rejected() {
        let mut cmd = make_cmd();
        // Cannot go from Queued to Streaming
        let err = cmd
            .transition(CommandState::CmdStreaming, "skip", "worker", None)
            .unwrap_err();
        assert!(format!("{err}").contains("invalid command transition"));
    }

    #[test]
    fn command_idempotency_key() {
        let cmd = make_cmd().with_idempotency_key("build-run-abc123");
        assert_eq!(cmd.idempotency_key.as_deref(), Some("build-run-abc123"));
    }

    #[test]
    fn command_duration() {
        let mut cmd = make_cmd();
        assert!(cmd.duration().is_none()); // not started yet

        cmd.transition(CommandState::CmdStarted, "spawned", "worker", None)
            .unwrap();
        assert!(cmd.duration().is_some()); // started, using now() as end

        cmd.exit(0, "worker", None).unwrap();
        assert!(cmd.duration().is_some()); // has both start and end
    }
}

#[cfg(test)]
mod transition_tests {
    use crate::entities::Transition;
    use uuid::Uuid;

    #[test]
    fn transition_new_populates_fields() {
        let entity_id = Uuid::now_v7();
        let corr_id = Uuid::now_v7();
        let cause_id = Uuid::now_v7();

        let t = Transition::new(
            "run",
            entity_id,
            "RunOpen",
            "RunActive",
            "tasks ready",
            "system",
            corr_id,
            Some(cause_id),
        );

        assert_eq!(t.entity_kind, "run");
        assert_eq!(t.entity_id, entity_id);
        assert_eq!(t.from_state, "RunOpen");
        assert_eq!(t.to_state, "RunActive");
        assert_eq!(t.reason, "tasks ready");
        assert_eq!(t.actor, "system");
        assert_eq!(t.correlation_id, corr_id);
        assert_eq!(t.causation_id, Some(cause_id));
    }

    #[test]
    fn transition_without_causation() {
        let t = Transition::new(
            "task",
            Uuid::now_v7(),
            "TaskOpen",
            "TaskReady",
            "deps met",
            "scheduler",
            Uuid::now_v7(),
            None,
        );
        assert!(t.causation_id.is_none());
    }
}

#[cfg(test)]
mod worktree_binding_tests {
    use crate::entities::worktree_binding::{SubmoduleMode, WorktreeBinding};
    use crate::fsm::worktree::WorktreeState;
    use uuid::Uuid;

    fn make_worktree() -> WorktreeBinding {
        WorktreeBinding::new(
            Uuid::now_v7(),
            "/home/user/repo",
            "yarl/abc123/build",
            "deadbeef1234",
            Uuid::now_v7(),
        )
    }

    #[test]
    fn new_worktree_starts_unbound() {
        let wt = make_worktree();
        assert_eq!(wt.state, WorktreeState::WtUnbound);
        assert_eq!(wt.base_ref, "deadbeef1234");
        assert_eq!(wt.head_ref, "deadbeef1234"); // head = base initially
        assert!(!wt.dirty);
        assert!(wt.submodule_state_hash.is_none());
        assert_eq!(wt.submodule_mode, SubmoduleMode::Locked);
        assert!(wt.lease_owner.is_none());
        assert!(wt.task_id.is_none());
    }

    #[test]
    fn worktree_with_task_binding() {
        let task_id = Uuid::now_v7();
        let wt = make_worktree().with_task(task_id);
        assert_eq!(wt.task_id, Some(task_id));
    }

    #[test]
    fn worktree_with_submodule_mode() {
        let wt = make_worktree().with_submodule_mode(SubmoduleMode::AllowAny);
        assert_eq!(wt.submodule_mode, SubmoduleMode::AllowAny);
    }

    #[test]
    fn worktree_full_happy_path() {
        let mut wt = make_worktree();

        // Unbound -> Creating
        wt.transition(
            WorktreeState::WtCreating,
            "creating worktree",
            "system",
            None,
        )
        .unwrap();
        assert_eq!(wt.state, WorktreeState::WtCreating);

        // Creating -> BoundHome
        wt.transition(
            WorktreeState::WtBoundHome,
            "worktree created",
            "system",
            None,
        )
        .unwrap();
        assert_eq!(wt.state, WorktreeState::WtBoundHome);

        // BoundHome -> CleanupPending
        wt.transition(
            WorktreeState::WtCleanupPending,
            "task complete",
            "scheduler",
            None,
        )
        .unwrap();
        assert_eq!(wt.state, WorktreeState::WtCleanupPending);

        // CleanupPending -> Closed
        wt.transition(WorktreeState::WtClosed, "cleaned up", "system", None)
            .unwrap();
        assert_eq!(wt.state, WorktreeState::WtClosed);
    }

    #[test]
    fn worktree_switch_branch() {
        let mut wt = make_worktree();
        wt.transition(WorktreeState::WtCreating, "creating", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtBoundHome, "created", "system", None)
            .unwrap();

        // BoundHome -> SwitchPending
        wt.transition(
            WorktreeState::WtSwitchPending,
            "switching branch",
            "system",
            None,
        )
        .unwrap();
        assert_eq!(wt.state, WorktreeState::WtSwitchPending);

        // SwitchPending -> BoundNonHome
        wt.transition(
            WorktreeState::WtBoundNonHome,
            "switched to feature branch",
            "system",
            None,
        )
        .unwrap();
        assert_eq!(wt.state, WorktreeState::WtBoundNonHome);
    }

    #[test]
    fn worktree_merge_flow() {
        let mut wt = make_worktree();
        wt.transition(WorktreeState::WtCreating, "creating", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtBoundHome, "created", "system", None)
            .unwrap();

        // BoundHome -> Merging
        wt.transition(WorktreeState::WtMerging, "merge requested", "system", None)
            .unwrap();
        assert_eq!(wt.state, WorktreeState::WtMerging);

        // Merging -> BoundHome (merge successful)
        wt.transition(WorktreeState::WtBoundHome, "merge complete", "system", None)
            .unwrap();
        assert_eq!(wt.state, WorktreeState::WtBoundHome);
    }

    #[test]
    fn worktree_merge_conflict_and_recovery() {
        let mut wt = make_worktree();
        wt.transition(WorktreeState::WtCreating, "creating", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtBoundHome, "created", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtMerging, "merge requested", "system", None)
            .unwrap();

        // Merging -> Conflict
        wt.transition(WorktreeState::WtConflict, "text conflict", "system", None)
            .unwrap();
        assert_eq!(wt.state, WorktreeState::WtConflict);

        // Conflict -> Recovering
        wt.transition(
            WorktreeState::WtRecovering,
            "aborting merge",
            "system",
            None,
        )
        .unwrap();
        assert_eq!(wt.state, WorktreeState::WtRecovering);

        // Recovering -> BoundHome
        wt.transition(WorktreeState::WtBoundHome, "recovered", "system", None)
            .unwrap();
        assert_eq!(wt.state, WorktreeState::WtBoundHome);
    }

    #[test]
    fn worktree_terminal_state_rejects_transition() {
        let mut wt = make_worktree();
        wt.transition(WorktreeState::WtCreating, "creating", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtBoundHome, "created", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtCleanupPending, "cleanup", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtClosed, "closed", "system", None)
            .unwrap();

        let err = wt
            .transition(WorktreeState::WtCreating, "try again", "system", None)
            .unwrap_err();
        assert!(format!("{err}").contains("terminal"));
    }

    #[test]
    fn worktree_invalid_transition_rejected() {
        let mut wt = make_worktree();
        // Cannot go directly from Unbound to BoundHome
        let err = wt
            .transition(WorktreeState::WtBoundHome, "skip", "system", None)
            .unwrap_err();
        assert!(format!("{err}").contains("invalid worktree transition"));
    }

    #[test]
    fn worktree_allows_mutations() {
        let mut wt = make_worktree();
        // Unbound does not allow mutations
        assert!(!wt.allows_mutations());

        wt.transition(WorktreeState::WtCreating, "creating", "system", None)
            .unwrap();
        assert!(wt.allows_mutations());

        wt.transition(WorktreeState::WtBoundHome, "created", "system", None)
            .unwrap();
        assert!(wt.allows_mutations());
    }

    #[test]
    fn worktree_update_head_ref() {
        let mut wt = make_worktree();
        assert_eq!(wt.head_ref, "deadbeef1234");

        wt.update_head_ref("cafebabe5678");
        assert_eq!(wt.head_ref, "cafebabe5678");
    }

    #[test]
    fn worktree_dirty_flag() {
        let mut wt = make_worktree();
        assert!(!wt.dirty);

        wt.set_dirty(true);
        assert!(wt.dirty);

        wt.set_dirty(false);
        assert!(!wt.dirty);
    }

    #[test]
    fn worktree_submodule_hash() {
        let mut wt = make_worktree();
        assert!(wt.submodule_state_hash.is_none());

        wt.update_submodule_hash("abc123");
        assert_eq!(wt.submodule_state_hash.as_deref(), Some("abc123"));
    }

    #[test]
    fn worktree_lease_owner() {
        let mut wt = make_worktree();
        assert!(wt.lease_owner.is_none());

        wt.set_lease_owner(Some("worker-1".into()));
        assert_eq!(wt.lease_owner.as_deref(), Some("worker-1"));

        wt.set_lease_owner(None);
        assert!(wt.lease_owner.is_none());
    }

    #[test]
    fn worktree_transition_has_correlation_id() {
        let mut wt = make_worktree();
        let t = wt
            .transition(WorktreeState::WtCreating, "creating", "system", None)
            .unwrap();
        assert_eq!(t.correlation_id, wt.correlation_id);
        assert_eq!(t.entity_kind, "worktree");
    }

    #[test]
    fn worktree_transition_with_causation_id() {
        let mut wt = make_worktree();
        let cause = Uuid::now_v7();
        let t = wt
            .transition(WorktreeState::WtCreating, "creating", "system", Some(cause))
            .unwrap();
        assert_eq!(t.causation_id, Some(cause));
    }

    #[test]
    fn worktree_set_path() {
        let mut wt = make_worktree();
        assert!(wt.worktree_path.as_os_str().is_empty());

        wt.set_worktree_path("/home/user/repo/.yarl/worktrees/abc123-def4");
        assert_eq!(
            wt.worktree_path.to_str().unwrap(),
            "/home/user/repo/.yarl/worktrees/abc123-def4"
        );
    }

    #[test]
    fn worktree_cleanup_pending_denies_mutations() {
        let mut wt = make_worktree();
        wt.transition(WorktreeState::WtCreating, "creating", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtBoundHome, "created", "system", None)
            .unwrap();
        wt.transition(WorktreeState::WtCleanupPending, "cleanup", "system", None)
            .unwrap();

        assert!(!wt.allows_mutations());
    }
}

#[cfg(test)]
mod merge_intent_tests {
    use crate::entities::merge_intent::{ConflictRecord, ConflictType, MergeIntent, MergeStrategy};
    use crate::fsm::merge::MergeState;
    use uuid::Uuid;

    fn make_merge() -> MergeIntent {
        MergeIntent::new(
            Uuid::now_v7(),
            Uuid::now_v7(),
            "feature/add-auth",
            "main",
            Uuid::now_v7(),
        )
    }

    #[test]
    fn new_merge_starts_requested() {
        let mi = make_merge();
        assert_eq!(mi.state, MergeState::MergeRequested);
        assert_eq!(mi.source_ref, "feature/add-auth");
        assert_eq!(mi.target_ref, "main");
        assert_eq!(mi.strategy, MergeStrategy::MergeNoFf); // default
        assert!(mi.source_sha.is_none());
        assert!(mi.target_sha.is_none());
        assert!(mi.result_sha.is_none());
        assert!(mi.conflicts.is_empty());
        assert!(mi.approval_token_id.is_none());
    }

    #[test]
    fn merge_with_strategy() {
        let mi = make_merge().with_strategy(MergeStrategy::SquashMerge);
        assert_eq!(mi.strategy, MergeStrategy::SquashMerge);
    }

    #[test]
    fn merge_with_approval() {
        let mi = make_merge().with_approval("token-abc123");
        assert_eq!(mi.approval_token_id.as_deref(), Some("token-abc123"));
    }

    #[test]
    fn merge_full_happy_path() {
        let mut mi = make_merge();

        // Requested -> Precheck
        mi.transition(
            MergeState::MergePrecheck,
            "starting precheck",
            "system",
            None,
        )
        .unwrap();
        assert_eq!(mi.state, MergeState::MergePrecheck);

        // Set precheck SHAs
        mi.set_precheck_shas("abc123", "def456");
        assert_eq!(mi.source_sha.as_deref(), Some("abc123"));
        assert_eq!(mi.target_sha.as_deref(), Some("def456"));

        // Precheck -> DryRun
        mi.transition(MergeState::MergeDryRun, "precheck passed", "system", None)
            .unwrap();
        assert_eq!(mi.state, MergeState::MergeDryRun);

        // DryRun -> Apply
        mi.transition(MergeState::MergeApply, "dry run clean", "system", None)
            .unwrap();
        assert_eq!(mi.state, MergeState::MergeApply);

        // Set result SHA
        mi.set_result_sha("merged123");
        assert_eq!(mi.result_sha.as_deref(), Some("merged123"));

        // Apply -> Verify
        mi.transition(MergeState::MergeVerify, "apply complete", "system", None)
            .unwrap();
        assert_eq!(mi.state, MergeState::MergeVerify);

        // Verify -> Done
        mi.transition(MergeState::MergeDone, "gates passed", "gate_engine", None)
            .unwrap();
        assert_eq!(mi.state, MergeState::MergeDone);
    }

    #[test]
    fn merge_conflict_during_dry_run() {
        let mut mi = make_merge();
        mi.transition(MergeState::MergePrecheck, "precheck", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeDryRun, "dry run", "system", None)
            .unwrap();

        // Record conflicts
        mi.set_conflicts(vec![
            ConflictRecord {
                path: "src/main.rs".into(),
                conflict_type: ConflictType::Text,
                marker_count: Some(3),
            },
            ConflictRecord {
                path: "config.toml".into(),
                conflict_type: ConflictType::DeleteModify,
                marker_count: None,
            },
        ]);
        assert!(mi.has_conflicts());
        assert_eq!(mi.conflicts.len(), 2);

        // DryRun -> Conflict
        mi.transition(
            MergeState::MergeConflict,
            "2 conflicts found",
            "system",
            None,
        )
        .unwrap();
        assert_eq!(mi.state, MergeState::MergeConflict);
    }

    #[test]
    fn merge_conflict_restart_from_precheck() {
        let mut mi = make_merge();
        mi.transition(MergeState::MergePrecheck, "precheck", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeDryRun, "dry run", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeConflict, "conflict", "system", None)
            .unwrap();

        // Conflict -> Precheck (restart after resolution)
        mi.transition(
            MergeState::MergePrecheck,
            "conflicts resolved, restarting",
            "operator",
            None,
        )
        .unwrap();
        assert_eq!(mi.state, MergeState::MergePrecheck);
    }

    #[test]
    fn merge_abort_from_requested() {
        let mut mi = make_merge();
        mi.transition(MergeState::MergeAborted, "cancelled", "operator", None)
            .unwrap();
        assert_eq!(mi.state, MergeState::MergeAborted);
    }

    #[test]
    fn merge_abort_from_precheck() {
        let mut mi = make_merge();
        mi.transition(MergeState::MergePrecheck, "precheck", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeAborted, "precheck failed", "system", None)
            .unwrap();
        assert_eq!(mi.state, MergeState::MergeAborted);
    }

    #[test]
    fn merge_abort_from_apply() {
        let mut mi = make_merge();
        mi.transition(MergeState::MergePrecheck, "precheck", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeDryRun, "dry run", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeApply, "apply", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeAborted, "runtime error", "system", None)
            .unwrap();
        assert_eq!(mi.state, MergeState::MergeAborted);
    }

    #[test]
    fn merge_no_direct_requested_to_apply() {
        let mut mi = make_merge();
        let err = mi
            .transition(MergeState::MergeApply, "skip", "system", None)
            .unwrap_err();
        assert!(format!("{err}").contains("invalid merge transition"));
    }

    #[test]
    fn merge_terminal_state_rejects_transition() {
        let mut mi = make_merge();
        mi.transition(MergeState::MergeAborted, "cancelled", "operator", None)
            .unwrap();

        let err = mi
            .transition(MergeState::MergePrecheck, "retry", "system", None)
            .unwrap_err();
        assert!(format!("{err}").contains("terminal"));
    }

    #[test]
    fn merge_done_is_terminal() {
        let mut mi = make_merge();
        mi.transition(MergeState::MergePrecheck, "precheck", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeDryRun, "dry run", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeApply, "apply", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeVerify, "verify", "system", None)
            .unwrap();
        mi.transition(MergeState::MergeDone, "done", "system", None)
            .unwrap();

        let err = mi
            .transition(MergeState::MergeAborted, "undo", "operator", None)
            .unwrap_err();
        assert!(format!("{err}").contains("terminal"));
    }

    #[test]
    fn merge_target_ref_stale() {
        let mut mi = make_merge();
        // No precheck yet — not stale
        assert!(!mi.target_ref_stale("anything"));

        mi.set_precheck_shas("abc123", "def456");
        assert!(!mi.target_ref_stale("def456")); // same SHA
        assert!(mi.target_ref_stale("changed789")); // different SHA
    }

    #[test]
    fn merge_has_no_conflicts_initially() {
        let mi = make_merge();
        assert!(!mi.has_conflicts());
    }

    #[test]
    fn merge_transition_has_correlation_id() {
        let mut mi = make_merge();
        let t = mi
            .transition(MergeState::MergePrecheck, "starting", "system", None)
            .unwrap();
        assert_eq!(t.correlation_id, mi.correlation_id);
        assert_eq!(t.entity_kind, "merge");
    }

    #[test]
    fn merge_transition_with_causation_id() {
        let mut mi = make_merge();
        let cause = Uuid::now_v7();
        let t = mi
            .transition(MergeState::MergePrecheck, "starting", "system", Some(cause))
            .unwrap();
        assert_eq!(t.causation_id, Some(cause));
    }

    #[test]
    fn merge_conflict_types_serde_roundtrip() {
        let record = ConflictRecord {
            path: "lib.rs".into(),
            conflict_type: ConflictType::SubmodulePointer,
            marker_count: None,
        };
        let json = serde_json::to_string(&record).unwrap();
        let parsed: ConflictRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.conflict_type, ConflictType::SubmodulePointer);
        assert_eq!(parsed.path, "lib.rs");
    }

    #[test]
    fn merge_strategy_serde_roundtrip() {
        let mi = make_merge().with_strategy(MergeStrategy::RebaseThenFf);
        let json = serde_json::to_string(&mi).unwrap();
        let parsed: MergeIntent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.strategy, MergeStrategy::RebaseThenFf);
    }
}
