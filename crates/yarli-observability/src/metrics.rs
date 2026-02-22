//! Prometheus metrics for YARLI (Section 15.2).
//!
//! All metrics use the `yarli_` prefix. Label types are defined locally so that
//! the observability crate does not depend on FSM internals — callers pass
//! string labels derived from FSM states.
//!
//! # Metric families
//!
//! | Metric | Type | Labels | Section |
//! |--------|------|--------|---------|
//! | `yarli_queue_depth` | Gauge | — | Queue |
//! | `yarli_queue_lease_timeouts_total`* | Counter | — | Queue |
//! | `yarli_runs_total`* | Counter | `state` | Run/Task |
//! | `yarli_tasks_total`* | Counter | `state`, `command_class` | Run/Task |
//! | `yarli_gate_failures_total`* | Counter | `gate` | Run/Task |
//! | `yarli_commands_total`* | Counter | `command_class`, `exit_reason` | Commands |
//! | `yarli_command_duration_seconds` | Histogram | `command_class` | Commands |
//! | `yarli_worktree_state_total`* | Counter | `state` | Git |
//! | `yarli_merge_attempts_total`* | Counter | `outcome` | Git |
//! | `yarli_merge_conflicts_total`* | Counter | `conflict_type` | Git |
//!
//! *`_total` suffix is auto-appended by prometheus-client for counter types.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
pub use prometheus_client::registry::Registry;

// ---------------------------------------------------------------------------
// Label types
// ---------------------------------------------------------------------------

/// Labels for run-level metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StateLabel {
    pub state: String,
}

/// Labels for task-level metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TaskLabel {
    pub state: String,
    pub command_class: String,
}

/// Labels for gate failure metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GateLabel {
    pub gate: String,
}

/// Labels for command metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CommandLabel {
    pub command_class: String,
    pub exit_reason: String,
}

/// Labels for command duration histograms (class only).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CommandClassLabel {
    pub command_class: String,
}

/// Labels for scheduler tick stages.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TickStageLabel {
    pub stage: String,
}

/// Labels for command overhead phases.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CommandOverheadLabel {
    pub command_class: String,
    pub phase: String,
}

/// Labels for store operations.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StoreOpLabel {
    pub operation: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RunMetricLabel {
    pub run_id: String,
    pub metric: String,
}

/// Labels for merge outcome metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MergeOutcomeLabel {
    pub outcome: String,
}

/// Labels for merge conflict type metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ConflictTypeLabel {
    pub conflict_type: String,
}

// ---------------------------------------------------------------------------
// Metrics registry
// ---------------------------------------------------------------------------

/// All YARLI Prometheus metrics, registered under a shared [`Registry`].
///
/// Clone this struct freely — all metric handles are internally `Arc`-based.
#[derive(Clone, Debug)]
pub struct YarliMetrics {
    // -- Queue --
    /// Current number of tasks in the queue.
    pub queue_depth: Gauge,
    /// Total lease timeouts observed.
    pub queue_lease_timeouts_total: Counter,

    // -- Scheduler --
    /// Scheduler tick duration in seconds (labelled by stage).
    pub scheduler_tick_duration_seconds: Family<TickStageLabel, Histogram>,

    // -- Runs --
    /// Total run state transitions (labelled by target state).
    pub runs_total: Family<StateLabel, Counter>,

    // -- Tasks --
    /// Total task state transitions (labelled by state + command class).
    pub tasks_total: Family<TaskLabel, Counter>,

    // -- Gates --
    /// Total gate evaluation failures (labelled by gate name).
    pub gate_failures_total: Family<GateLabel, Counter>,

    // -- Commands --
    /// Total command executions (labelled by class + exit reason).
    pub commands_total: Family<CommandLabel, Counter>,
    /// Command execution duration in seconds (labelled by class).
    pub command_duration_seconds: Family<CommandClassLabel, Histogram>,
    /// Command overhead duration in seconds (labelled by class + phase).
    pub command_overhead_duration_seconds: Family<CommandOverheadLabel, Histogram>,

    // -- Store --
    /// Store operation duration in seconds (labelled by operation).
    pub store_duration_seconds: Family<StoreOpLabel, Histogram>,
    /// Total slow queries observed (labelled by operation).
    pub store_slow_queries_total: Family<StoreOpLabel, Counter>,

    // -- Git: Worktree --
    /// Total worktree state transitions (labelled by target state).
    pub worktree_state_total: Family<StateLabel, Counter>,

    // -- Git: Merge --
    /// Total merge attempts (labelled by outcome: done/conflict/aborted).
    pub merge_attempts_total: Family<MergeOutcomeLabel, Counter>,
    /// Total merge conflicts (labelled by conflict type).
    pub merge_conflicts_total: Family<ConflictTypeLabel, Counter>,

    // -- Run metrics --
    /// Current run-level resource usage totals.
    pub run_resource_usage: Family<RunMetricLabel, Gauge>,
    /// Current run-level token usage totals.
    pub run_token_usage: Family<RunMetricLabel, Gauge>,
}

impl YarliMetrics {
    /// Create a new `YarliMetrics` and register all metrics in `registry`.
    pub fn new(registry: &mut Registry) -> Self {
        // -- Queue --
        let queue_depth = Gauge::default();
        registry.register(
            "yarli_queue_depth",
            "Current number of tasks in queue",
            queue_depth.clone(),
        );

        // NOTE: prometheus-client auto-appends `_total` suffix to counter names,
        // so we register without the `_total` suffix.
        let queue_lease_timeouts_total = Counter::default();
        registry.register(
            "yarli_queue_lease_timeouts",
            "Total number of lease timeouts",
            queue_lease_timeouts_total.clone(),
        );

        // -- Scheduler --
        let scheduler_tick_duration_seconds =
            Family::<TickStageLabel, Histogram>::new_with_constructor(|| {
                // Microsecond resolution up to ~1s
                Histogram::new(exponential_buckets(0.0001, 4.0, 8))
            });
        registry.register(
            "yarli_scheduler_tick_duration_seconds",
            "Scheduler tick duration in seconds by stage",
            scheduler_tick_duration_seconds.clone(),
        );

        // -- Runs --
        let runs_total = Family::<StateLabel, Counter>::default();
        registry.register(
            "yarli_runs",
            "Total run state transitions by target state",
            runs_total.clone(),
        );

        // -- Tasks --
        let tasks_total = Family::<TaskLabel, Counter>::default();
        registry.register(
            "yarli_tasks",
            "Total task state transitions by state and command class",
            tasks_total.clone(),
        );

        // -- Gates --
        let gate_failures_total = Family::<GateLabel, Counter>::default();
        registry.register(
            "yarli_gate_failures",
            "Total gate evaluation failures by gate name",
            gate_failures_total.clone(),
        );

        // -- Commands --
        let commands_total = Family::<CommandLabel, Counter>::default();
        registry.register(
            "yarli_commands",
            "Total command executions by class and exit reason",
            commands_total.clone(),
        );

        let command_duration_seconds =
            Family::<CommandClassLabel, Histogram>::new_with_constructor(|| {
                // Exponential buckets: 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6, 51.2
                Histogram::new(exponential_buckets(0.1, 2.0, 10))
            });
        registry.register(
            "yarli_command_duration_seconds",
            "Command execution duration in seconds",
            command_duration_seconds.clone(),
        );

        let command_overhead_duration_seconds =
            Family::<CommandOverheadLabel, Histogram>::new_with_constructor(|| {
                // Millisecond resolution up to ~10s
                Histogram::new(exponential_buckets(0.001, 4.0, 8))
            });
        registry.register(
            "yarli_command_overhead_duration_seconds",
            "Command overhead duration in seconds by phase",
            command_overhead_duration_seconds.clone(),
        );

        // -- Store --
        let store_duration_seconds =
            Family::<StoreOpLabel, Histogram>::new_with_constructor(|| {
                // Millisecond resolution up to ~10s
                Histogram::new(exponential_buckets(0.001, 4.0, 8))
            });
        registry.register(
            "yarli_store_duration_seconds",
            "Store operation duration in seconds by operation",
            store_duration_seconds.clone(),
        );

        let store_slow_queries_total = Family::<StoreOpLabel, Counter>::default();
        registry.register(
            "yarli_store_slow_queries",
            "Total slow queries observed by operation",
            store_slow_queries_total.clone(),
        );

        // -- Git: Worktree --
        let worktree_state_total = Family::<StateLabel, Counter>::default();
        registry.register(
            "yarli_worktree_state",
            "Total worktree state transitions by target state",
            worktree_state_total.clone(),
        );

        // -- Git: Merge --
        let merge_attempts_total = Family::<MergeOutcomeLabel, Counter>::default();
        registry.register(
            "yarli_merge_attempts",
            "Total merge attempts by outcome",
            merge_attempts_total.clone(),
        );

        let merge_conflicts_total = Family::<ConflictTypeLabel, Counter>::default();
        registry.register(
            "yarli_merge_conflicts",
            "Total merge conflicts by conflict type",
            merge_conflicts_total.clone(),
        );

        let run_resource_usage = Family::<RunMetricLabel, Gauge>::default();
        registry.register(
            "yarli_run_resource_usage",
            "Current run-level resource usage totals",
            run_resource_usage.clone(),
        );

        let run_token_usage = Family::<RunMetricLabel, Gauge>::default();
        registry.register(
            "yarli_run_token_usage",
            "Current run-level token usage totals",
            run_token_usage.clone(),
        );

        Self {
            queue_depth,
            queue_lease_timeouts_total,
            scheduler_tick_duration_seconds,
            runs_total,
            tasks_total,
            gate_failures_total,
            commands_total,
            command_duration_seconds,
            command_overhead_duration_seconds,
            store_duration_seconds,
            store_slow_queries_total,
            worktree_state_total,
            merge_attempts_total,
            merge_conflicts_total,
            run_resource_usage,
            run_token_usage,
        }
    }

    /// Set the queue depth gauge.
    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.set(depth as i64);
    }

    // -- Convenience helpers --

    /// Record a run state transition.
    pub fn record_run_transition(&self, state: &str) {
        self.runs_total
            .get_or_create(&StateLabel {
                state: state.to_string(),
            })
            .inc();
    }

    /// Record a task state transition.
    pub fn record_task_transition(&self, state: &str, command_class: &str) {
        self.tasks_total
            .get_or_create(&TaskLabel {
                state: state.to_string(),
                command_class: command_class.to_string(),
            })
            .inc();
    }

    /// Record a gate failure.
    pub fn record_gate_failure(&self, gate: &str) {
        self.gate_failures_total
            .get_or_create(&GateLabel {
                gate: gate.to_string(),
            })
            .inc();
    }

    /// Record a command execution.
    pub fn record_command(&self, command_class: &str, exit_reason: &str) {
        self.commands_total
            .get_or_create(&CommandLabel {
                command_class: command_class.to_string(),
                exit_reason: exit_reason.to_string(),
            })
            .inc();
    }

    /// Record a command duration.
    pub fn record_command_duration(&self, command_class: &str, duration_secs: f64) {
        self.command_duration_seconds
            .get_or_create(&CommandClassLabel {
                command_class: command_class.to_string(),
            })
            .observe(duration_secs);
    }

    /// Record a worktree state transition.
    pub fn record_worktree_transition(&self, state: &str) {
        self.worktree_state_total
            .get_or_create(&StateLabel {
                state: state.to_string(),
            })
            .inc();
    }

    /// Record a merge attempt outcome.
    pub fn record_merge_attempt(&self, outcome: &str) {
        self.merge_attempts_total
            .get_or_create(&MergeOutcomeLabel {
                outcome: outcome.to_string(),
            })
            .inc();
    }

    /// Record a merge conflict.
    pub fn record_merge_conflict(&self, conflict_type: &str) {
        self.merge_conflicts_total
            .get_or_create(&ConflictTypeLabel {
                conflict_type: conflict_type.to_string(),
            })
            .inc();
    }

    /// Record current run-level resource usage.
    pub fn set_run_resource_usage(&self, run_id: &str, metric: &str, value: u64) {
        self.run_resource_usage
            .get_or_create(&RunMetricLabel {
                run_id: run_id.to_string(),
                metric: metric.to_string(),
            })
            .set(value as i64);
    }

    /// Record current run-level token usage.
    pub fn set_run_token_usage(&self, run_id: &str, metric: &str, value: u64) {
        self.run_token_usage
            .get_or_create(&RunMetricLabel {
                run_id: run_id.to_string(),
                metric: metric.to_string(),
            })
            .set(value as i64);
    }

    /// Record scheduler tick duration.
    pub fn record_scheduler_tick_duration(&self, stage: &str, duration_secs: f64) {
        self.scheduler_tick_duration_seconds
            .get_or_create(&TickStageLabel {
                stage: stage.to_string(),
            })
            .observe(duration_secs);
    }

    /// Record command overhead duration.
    pub fn record_command_overhead_duration(
        &self,
        command_class: &str,
        phase: &str,
        duration_secs: f64,
    ) {
        self.command_overhead_duration_seconds
            .get_or_create(&CommandOverheadLabel {
                command_class: command_class.to_string(),
                phase: phase.to_string(),
            })
            .observe(duration_secs);
    }

    /// Record store operation duration.
    pub fn record_store_duration(&self, operation: &str, duration_secs: f64) {
        self.store_duration_seconds
            .get_or_create(&StoreOpLabel {
                operation: operation.to_string(),
            })
            .observe(duration_secs);
    }

    /// Record a slow store query.
    pub fn record_store_slow_query(&self, operation: &str) {
        self.store_slow_queries_total
            .get_or_create(&StoreOpLabel {
                operation: operation.to_string(),
            })
            .inc();
    }
}

/// Encode the registry contents in Prometheus text exposition format.
pub fn encode_metrics(registry: &Registry) -> String {
    let mut buf = String::new();
    prometheus_client::encoding::text::encode(&mut buf, registry)
        .expect("encoding to String never fails");
    buf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Registry, YarliMetrics) {
        let mut registry = Registry::default();
        let metrics = YarliMetrics::new(&mut registry);
        (registry, metrics)
    }

    // -- Registration --

    #[test]
    fn metrics_register_without_panic() {
        let (_registry, _metrics) = setup();
    }

    #[test]
    fn encode_empty_registry() {
        let (registry, _metrics) = setup();
        let output = encode_metrics(&registry);
        // Empty families still encode but no samples
        assert!(output.contains("yarli_queue_depth"));
        assert!(output.contains("yarli_queue_lease_timeouts_total"));
    }

    // -- Queue metrics --

    #[test]
    fn queue_depth_gauge() {
        let (registry, metrics) = setup();
        metrics.queue_depth.set(42);
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_queue_depth 42"));
    }

    #[test]
    fn queue_depth_inc_dec() {
        let (_registry, metrics) = setup();
        metrics.queue_depth.inc();
        metrics.queue_depth.inc();
        metrics.queue_depth.dec();
        assert_eq!(metrics.queue_depth.get(), 1);
    }

    #[test]
    fn queue_lease_timeouts_counter() {
        let (registry, metrics) = setup();
        metrics.queue_lease_timeouts_total.inc();
        metrics.queue_lease_timeouts_total.inc();
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_queue_lease_timeouts_total 2"));
    }

    // -- Run metrics --

    #[test]
    fn runs_total_counter() {
        let (registry, metrics) = setup();
        metrics.record_run_transition("RUN_ACTIVE");
        metrics.record_run_transition("RUN_ACTIVE");
        metrics.record_run_transition("RUN_COMPLETED");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_runs_total"));
        assert!(output.contains("RUN_ACTIVE"));
        assert!(output.contains("RUN_COMPLETED"));
    }

    // -- Task metrics --

    #[test]
    fn tasks_total_counter() {
        let (registry, metrics) = setup();
        metrics.record_task_transition("TASK_EXECUTING", "cpu");
        metrics.record_task_transition("TASK_COMPLETE", "io");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_tasks_total"));
        assert!(output.contains("TASK_EXECUTING"));
        assert!(output.contains("TASK_COMPLETE"));
    }

    // -- Gate metrics --

    #[test]
    fn gate_failures_counter() {
        let (registry, metrics) = setup();
        metrics.record_gate_failure("tests_passed");
        metrics.record_gate_failure("tests_passed");
        metrics.record_gate_failure("policy_clean");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_gate_failures_total"));
        assert!(output.contains("tests_passed"));
        assert!(output.contains("policy_clean"));
    }

    // -- Command metrics --

    #[test]
    fn commands_total_counter() {
        let (registry, metrics) = setup();
        metrics.record_command("io", "exited");
        metrics.record_command("cpu", "timed_out");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_commands_total"));
        assert!(output.contains("exited"));
        assert!(output.contains("timed_out"));
    }

    #[test]
    fn command_duration_histogram() {
        let (registry, metrics) = setup();
        metrics.record_command_duration("io", 0.5);
        metrics.record_command_duration("io", 1.5);
        metrics.record_command_duration("cpu", 10.0);
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_command_duration_seconds"));
    }

    // -- Worktree metrics --

    #[test]
    fn worktree_state_counter() {
        let (registry, metrics) = setup();
        metrics.record_worktree_transition("WT_BOUND_HOME");
        metrics.record_worktree_transition("WT_CLOSED");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_worktree_state_total"));
        assert!(output.contains("WT_BOUND_HOME"));
    }

    // -- Merge metrics --

    #[test]
    fn merge_attempts_counter() {
        let (registry, metrics) = setup();
        metrics.record_merge_attempt("done");
        metrics.record_merge_attempt("conflict");
        metrics.record_merge_attempt("aborted");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_merge_attempts_total"));
        assert!(output.contains("done"));
        assert!(output.contains("conflict"));
    }

    #[test]
    fn merge_conflicts_counter() {
        let (registry, metrics) = setup();
        metrics.record_merge_conflict("text");
        metrics.record_merge_conflict("rename_rename");
        metrics.record_merge_conflict("submodule_pointer");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_merge_conflicts_total"));
        assert!(output.contains("text"));
        assert!(output.contains("rename_rename"));
    }

    // -- Scheduler metrics --

    #[test]
    fn scheduler_tick_duration_histogram() {
        let (registry, metrics) = setup();
        metrics.record_scheduler_tick_duration("scan", 0.1);
        metrics.record_scheduler_tick_duration("claim", 0.05);
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_scheduler_tick_duration_seconds"));
        assert!(output.contains("scan"));
        assert!(output.contains("claim"));
    }

    // -- Command overhead metrics --

    #[test]
    fn command_overhead_duration_histogram() {
        let (registry, metrics) = setup();
        metrics.record_command_overhead_duration("io", "spawn", 0.01);
        metrics.record_command_overhead_duration("cpu", "capture", 0.02);
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_command_overhead_duration_seconds"));
        assert!(output.contains("spawn"));
        assert!(output.contains("capture"));
    }

    // -- Store metrics --

    #[test]
    fn store_metrics() {
        let (registry, metrics) = setup();
        metrics.record_store_duration("append", 0.05);
        metrics.record_store_slow_query("query");
        let output = encode_metrics(&registry);
        assert!(output.contains("yarli_store_duration_seconds"));
        assert!(output.contains("yarli_store_slow_queries"));
        assert!(output.contains("append"));
        assert!(output.contains("query"));
    }

    // -- Clone --

    #[test]
    fn metrics_are_cloneable_and_share_state() {
        let (_registry, metrics) = setup();
        let metrics2 = metrics.clone();
        metrics.queue_depth.set(99);
        assert_eq!(metrics2.queue_depth.get(), 99);
    }

    // -- Full encode --

    #[test]
    fn full_encode_includes_all_metric_names() {
        let (registry, metrics) = setup();
        // Touch each metric family at least once
        metrics.queue_depth.set(1);
        metrics.queue_lease_timeouts_total.inc();
        metrics.record_run_transition("RUN_OPEN");
        metrics.record_task_transition("TASK_OPEN", "io");
        metrics.record_gate_failure("tests_passed");
        metrics.record_command("io", "exited");
        metrics.record_command_duration("io", 1.0);
        metrics.record_worktree_transition("WT_CREATING");
        metrics.record_merge_attempt("done");
        metrics.record_merge_conflict("text");
        metrics.record_scheduler_tick_duration("scan", 0.001);
        metrics.record_command_overhead_duration("io", "spawn", 0.001);
        metrics.record_store_duration("query", 0.001);
        metrics.record_store_slow_query("append");

        let output = encode_metrics(&registry);

        let expected_names = [
            "yarli_queue_depth",
            "yarli_queue_lease_timeouts_total",
            "yarli_scheduler_tick_duration_seconds",
            "yarli_runs_total",
            "yarli_tasks_total",
            "yarli_gate_failures_total",
            "yarli_commands_total",
            "yarli_command_duration_seconds",
            "yarli_command_overhead_duration_seconds",
            "yarli_store_duration_seconds",
            "yarli_store_slow_queries",
            "yarli_worktree_state_total",
            "yarli_merge_attempts_total",
            "yarli_merge_conflicts_total",
            "yarli_run_resource_usage",
            "yarli_run_token_usage",
        ];
        for name in &expected_names {
            assert!(output.contains(name), "missing metric: {name}");
        }
    }
}
