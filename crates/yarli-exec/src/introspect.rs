//! LLM-driven process introspection for detecting stuck or unproductive processes.
//!
//! # Architecture (4 layers)
//!
//! 1. **Enriched Process Sampling** — extends existing `ProcessSample` with
//!    process state, syscall, thread count, fd count, consecutive sleep samples.
//! 2. **Output Pattern Analysis** — detects repetitive output, zero progress,
//!    and compaction signatures in command output.
//! 3. **Health Scoring** — combines signals into a 0.0–1.0 health score with
//!    weighted factors, published via `tokio::sync::watch`.
//! 4. **LLM Analysis Dispatch** — when health degrades to Stuck, assembles a
//!    diagnostic prompt and dispatches to an LLM via a pluggable `RouterSender`.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ─── Layer 1: Enriched Process Sampling ────────────────────────────────────

/// Extended process sample with additional state signals.
#[derive(Debug, Clone)]
pub struct EnrichedProcessSample {
    /// RSS in bytes (from /proc/{pid}/status VmRSS).
    pub rss_bytes: Option<u64>,
    /// CPU user ticks (from /proc/{pid}/stat field 14).
    pub cpu_user_ticks: Option<u64>,
    /// CPU system ticks (from /proc/{pid}/stat field 15).
    pub cpu_system_ticks: Option<u64>,
    /// I/O read bytes (from /proc/{pid}/io).
    pub io_read_bytes: Option<u64>,
    /// I/O write bytes (from /proc/{pid}/io).
    pub io_write_bytes: Option<u64>,
    /// Process state character from /proc/{pid}/stat field 3.
    /// R=Running, S=Sleeping, D=DiskSleep, Z=Zombie, T=Stopped.
    pub process_state: Option<char>,
    /// Current syscall number from /proc/{pid}/syscall (first field).
    pub current_syscall: Option<i64>,
    /// Thread count from /proc/{pid}/status Threads line.
    pub thread_count: Option<u32>,
    /// Number of open file descriptors (count of /proc/{pid}/fd/ entries).
    pub fd_count: Option<u32>,
    /// Timestamp when sample was taken.
    pub sampled_at: Instant,
}

impl Default for EnrichedProcessSample {
    fn default() -> Self {
        Self {
            rss_bytes: None,
            cpu_user_ticks: None,
            cpu_system_ticks: None,
            io_read_bytes: None,
            io_write_bytes: None,
            process_state: None,
            current_syscall: None,
            thread_count: None,
            fd_count: None,
            sampled_at: Instant::now(),
        }
    }
}

/// Read an enriched process sample from /proc on Linux.
#[cfg(target_os = "linux")]
pub fn read_enriched_sample(pid: u32) -> Option<EnrichedProcessSample> {
    use std::fs;

    let mut sample = EnrichedProcessSample {
        sampled_at: Instant::now(),
        ..Default::default()
    };

    // /proc/{pid}/status → VmRSS, Threads
    let status_path = format!("/proc/{pid}/status");
    if let Ok(status) = fs::read_to_string(&status_path) {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                sample.rss_bytes = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                    .map(|kb| kb.saturating_mul(1024));
            } else if let Some(rest) = line.strip_prefix("Threads:") {
                sample.thread_count = rest.trim().parse::<u32>().ok();
            }
        }
    } else {
        return None; // Process likely exited
    }

    // /proc/{pid}/stat → state, utime, stime
    let stat_path = format!("/proc/{pid}/stat");
    if let Ok(stat_text) = fs::read_to_string(&stat_path) {
        if let Some(end_comm) = stat_text.rfind(')') {
            let after = stat_text.get(end_comm + 2..).unwrap_or("");
            let fields: Vec<&str> = after.split_whitespace().collect();
            // Field 3 (after comm) is the state character
            sample.process_state = fields.first().and_then(|s| s.chars().next());
            // utime (#14) => idx 11, stime (#15) => idx 12 (relative to after-comm fields)
            sample.cpu_user_ticks = fields.get(11).and_then(|v| v.parse::<u64>().ok());
            sample.cpu_system_ticks = fields.get(12).and_then(|v| v.parse::<u64>().ok());
        }
    }

    // /proc/{pid}/io → read_bytes, write_bytes
    let io_path = format!("/proc/{pid}/io");
    if let Ok(io_text) = fs::read_to_string(&io_path) {
        for line in io_text.lines() {
            if let Some(v) = line.strip_prefix("read_bytes:") {
                sample.io_read_bytes = v.trim().parse::<u64>().ok();
            } else if let Some(v) = line.strip_prefix("write_bytes:") {
                sample.io_write_bytes = v.trim().parse::<u64>().ok();
            }
        }
    }

    // /proc/{pid}/syscall → current syscall number
    let syscall_path = format!("/proc/{pid}/syscall");
    if let Ok(syscall_text) = fs::read_to_string(&syscall_path) {
        sample.current_syscall = syscall_text
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<i64>().ok());
    }

    // /proc/{pid}/fd → count entries
    let fd_path = format!("/proc/{pid}/fd");
    if let Ok(entries) = fs::read_dir(&fd_path) {
        sample.fd_count = Some(entries.count() as u32);
    }

    Some(sample)
}

/// Stub for non-Linux platforms.
#[cfg(not(target_os = "linux"))]
pub fn read_enriched_sample(_pid: u32) -> Option<EnrichedProcessSample> {
    None
}

// ─── Layer 2: Output Pattern Analysis ──────────────────────────────────────

/// Detects repetitive patterns in command output using a sliding window of line hashes.
#[derive(Debug)]
pub struct RepetitionDetector {
    window: VecDeque<u64>,
    window_size: usize,
}

impl RepetitionDetector {
    pub fn new(window_size: usize) -> Self {
        Self {
            window: VecDeque::with_capacity(window_size),
            window_size,
        }
    }

    /// Feed a line of output and return the current repetition score (0.0–1.0).
    pub fn feed(&mut self, line: &str) -> f64 {
        let hash = Self::hash_line(line);
        if self.window.len() >= self.window_size {
            self.window.pop_front();
        }
        self.window.push_back(hash);
        self.score()
    }

    /// Current repetition score: ratio of most-frequent hash to total window size.
    pub fn score(&self) -> f64 {
        if self.window.len() < 2 {
            return 0.0;
        }
        let mut counts = std::collections::HashMap::new();
        for &h in &self.window {
            *counts.entry(h).or_insert(0u32) += 1;
        }
        let max_count = counts.values().copied().max().unwrap_or(0);
        max_count as f64 / self.window.len() as f64
    }

    fn hash_line(line: &str) -> u64 {
        // Simple FNV-1a hash for speed.
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in line.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

/// Detects true zero-progress: no output for an extended duration.
#[derive(Debug)]
pub struct ZeroProgressDetector {
    last_output_at: Option<Instant>,
    last_io_bytes: Option<u64>,
}

impl ZeroProgressDetector {
    pub fn new() -> Self {
        Self {
            last_output_at: None,
            last_io_bytes: None,
        }
    }

    /// Record a line of output.
    pub fn record_output(&mut self) {
        self.last_output_at = Some(Instant::now());
    }

    /// Record I/O bytes from a process sample.
    pub fn record_io_bytes(&mut self, total: u64) {
        self.last_io_bytes = Some(total);
    }

    /// Time since last output, or None if no output ever recorded.
    pub fn time_since_last_output(&self) -> Option<Duration> {
        self.last_output_at.map(|t| t.elapsed())
    }

    /// Check if I/O bytes have changed since last recording.
    pub fn io_bytes_changed(&self, current_total: u64) -> bool {
        match self.last_io_bytes {
            Some(prev) => current_total > prev,
            None => true,
        }
    }
}

impl Default for ZeroProgressDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Detects compaction/context-window signatures in output text.
#[derive(Debug)]
pub struct CompactionSignatureDetector {
    signatures: Vec<&'static str>,
    detected: Vec<String>,
}

impl CompactionSignatureDetector {
    pub fn new() -> Self {
        Self {
            signatures: vec![
                "context window",
                "compaction",
                "summarizing",
                "summarising",
                "context limit",
                "token limit",
                "truncat",
                "exceeds context",
            ],
            detected: Vec::new(),
        }
    }

    /// Feed a line and return whether any signature was detected.
    pub fn feed(&mut self, line: &str) -> bool {
        let lower = line.to_lowercase();
        for sig in &self.signatures {
            if lower.contains(sig) {
                self.detected.push(sig.to_string());
                return true;
            }
        }
        false
    }

    /// All detected signatures so far.
    pub fn detected_signatures(&self) -> &[String] {
        &self.detected
    }

    /// Clear detected signatures.
    pub fn clear(&mut self) {
        self.detected.clear();
    }
}

impl Default for CompactionSignatureDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined output analysis result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputAnalysis {
    /// Repetition score (0.0 = unique, 1.0 = all identical).
    pub repetition_score: f64,
    /// Seconds since last output (None if output never seen).
    pub seconds_since_output: Option<f64>,
    /// Whether I/O bytes are changing.
    pub io_progressing: bool,
    /// Detected compaction/context-window signatures.
    pub signatures: Vec<String>,
}

/// Combines all output pattern detectors.
#[derive(Debug)]
pub struct OutputAnalyzer {
    pub repetition: RepetitionDetector,
    pub zero_progress: ZeroProgressDetector,
    pub compaction: CompactionSignatureDetector,
}

impl OutputAnalyzer {
    pub fn new(repetition_window: usize) -> Self {
        Self {
            repetition: RepetitionDetector::new(repetition_window),
            zero_progress: ZeroProgressDetector::new(),
            compaction: CompactionSignatureDetector::new(),
        }
    }

    /// Feed a line of output to all detectors.
    pub fn feed_line(&mut self, line: &str) {
        self.repetition.feed(line);
        self.zero_progress.record_output();
        self.compaction.feed(line);
    }

    /// Feed a process sample's I/O bytes.
    pub fn feed_io_bytes(&mut self, total: u64) {
        let _changed = self.zero_progress.io_bytes_changed(total);
        self.zero_progress.record_io_bytes(total);
    }

    /// Produce the current analysis snapshot.
    pub fn analyze(&self, current_io_bytes: Option<u64>) -> OutputAnalysis {
        OutputAnalysis {
            repetition_score: self.repetition.score(),
            seconds_since_output: self
                .zero_progress
                .time_since_last_output()
                .map(|d| d.as_secs_f64()),
            io_progressing: current_io_bytes
                .map(|b| self.zero_progress.io_bytes_changed(b))
                .unwrap_or(true),
            signatures: self.compaction.detected_signatures().to_vec(),
        }
    }
}

// ─── Layer 3: Health Scoring ───────────────────────────────────────────────

/// Overall health level derived from the composite score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthLevel {
    /// Score >= 0.7: process is making progress.
    Healthy,
    /// Score 0.3–0.7: signs of degradation.
    Degraded,
    /// Score < 0.3: process appears stuck.
    Stuck,
}

impl HealthLevel {
    pub fn from_score(score: f64) -> Self {
        if score >= 0.7 {
            HealthLevel::Healthy
        } else if score >= 0.3 {
            HealthLevel::Degraded
        } else {
            HealthLevel::Stuck
        }
    }
}

impl std::fmt::Display for HealthLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthLevel::Healthy => write!(f, "healthy"),
            HealthLevel::Degraded => write!(f, "degraded"),
            HealthLevel::Stuck => write!(f, "stuck"),
        }
    }
}

/// Health report with score breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub score: f64,
    pub level: HealthLevel,
    pub factors: HealthFactors,
}

/// Individual factor scores that compose the health score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthFactors {
    /// Output recency factor (0.0–1.0). Weight: 0.3.
    pub output_recency: f64,
    /// I/O bytes delta factor (0.0–1.0). Weight: 0.2.
    pub io_delta: f64,
    /// CPU utilization factor (0.0–1.0). Weight: 0.15.
    pub cpu_utilization: f64,
    /// Repetition factor (0.0–1.0). Weight: 0.2.
    pub repetition: f64,
    /// Sleep ratio factor (0.0–1.0). Weight: 0.15.
    pub sleep_ratio: f64,
}

/// Computes a composite health score from process and output signals.
#[derive(Debug)]
pub struct HealthScorer {
    /// Consecutive samples where process state was 'S' (sleeping).
    consecutive_sleep_samples: u32,
    /// Total samples taken.
    total_samples: u32,
    /// Previous CPU ticks for delta calculation.
    prev_cpu_ticks: Option<u64>,
    /// Previous I/O bytes for delta calculation.
    prev_io_bytes: Option<u64>,
}

impl HealthScorer {
    pub fn new() -> Self {
        Self {
            consecutive_sleep_samples: 0,
            total_samples: 0,
            prev_cpu_ticks: None,
            prev_io_bytes: None,
        }
    }

    /// Score the current health from a process sample and output analysis.
    pub fn score(
        &mut self,
        sample: &EnrichedProcessSample,
        analysis: &OutputAnalysis,
    ) -> HealthReport {
        self.total_samples += 1;

        // Track sleep state
        if sample.process_state == Some('S') {
            self.consecutive_sleep_samples += 1;
        } else {
            self.consecutive_sleep_samples = 0;
        }

        // Factor 1: Output recency (weight 0.3)
        // Healthy: <5min, Degraded: 5-15min, Stuck: >15min
        let output_recency = match analysis.seconds_since_output {
            None => 0.5, // No output ever — ambiguous
            Some(secs) if secs < 300.0 => 1.0,
            Some(secs) if secs < 900.0 => {
                1.0 - (secs - 300.0) / 600.0 // Linear decay 1.0→0.0
            }
            Some(_) => 0.0,
        };

        // Factor 2: I/O bytes delta (weight 0.2)
        let io_delta = if analysis.io_progressing { 1.0 } else { 0.0 };

        // Update I/O tracking
        let current_io = sample
            .io_read_bytes
            .unwrap_or(0)
            .saturating_add(sample.io_write_bytes.unwrap_or(0));
        self.prev_io_bytes = Some(current_io);

        // Factor 3: CPU utilization (weight 0.15)
        // Compare current vs previous CPU ticks
        let current_cpu = sample
            .cpu_user_ticks
            .unwrap_or(0)
            .saturating_add(sample.cpu_system_ticks.unwrap_or(0));
        let cpu_utilization = match self.prev_cpu_ticks {
            Some(prev) => {
                let delta = current_cpu.saturating_sub(prev);
                if delta > 10 {
                    1.0
                } else if delta > 2 {
                    0.5
                } else {
                    0.0
                }
            }
            None => 0.5, // First sample, unknown
        };
        self.prev_cpu_ticks = Some(current_cpu);

        // Factor 4: Repetition (weight 0.2)
        // Healthy: <0.3, Degraded: 0.3-0.7, Stuck: >0.7
        let repetition = if analysis.repetition_score < 0.3 {
            1.0
        } else if analysis.repetition_score < 0.7 {
            1.0 - (analysis.repetition_score - 0.3) / 0.4
        } else {
            0.0
        };

        // Factor 5: Sleep ratio (weight 0.15)
        // Based on consecutive sleep samples vs total
        let sleep_ratio_raw = if self.total_samples > 0 {
            self.consecutive_sleep_samples as f64 / self.total_samples.min(20) as f64
        } else {
            0.0
        };
        let sleep_ratio = if sleep_ratio_raw < 0.5 {
            1.0
        } else if sleep_ratio_raw < 0.8 {
            1.0 - (sleep_ratio_raw - 0.5) / 0.3
        } else {
            0.0
        };

        // Weighted composite score
        let score = output_recency * 0.3
            + io_delta * 0.2
            + cpu_utilization * 0.15
            + repetition * 0.2
            + sleep_ratio * 0.15;

        let score = score.clamp(0.0, 1.0);
        let level = HealthLevel::from_score(score);

        HealthReport {
            score,
            level,
            factors: HealthFactors {
                output_recency,
                io_delta,
                cpu_utilization,
                repetition,
                sleep_ratio,
            },
        }
    }
}

impl Default for HealthScorer {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Layer 4: LLM Analysis Dispatch ────────────────────────────────────────

/// Verdict from LLM analysis of a stuck process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectionVerdict {
    /// LLM's assessment: "stuck", "degraded", "healthy", or "unknown".
    pub verdict: String,
    /// Confidence 0.0–1.0.
    pub confidence: f64,
    /// Human-readable reasoning.
    pub reasoning: String,
    /// Recommended action: "kill", "retry", "wait", "escalate".
    pub recommendation: String,
}

/// Diagnostic context assembled for LLM analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticContext {
    /// Last N lines of command output.
    pub recent_output: Vec<String>,
    /// Health report at the time of diagnosis.
    pub health_report: HealthReport,
    /// Output analysis at the time of diagnosis.
    pub output_analysis: OutputAnalysis,
    /// Task objective / description.
    pub task_objective: String,
    /// How long the command has been running.
    pub runtime_seconds: f64,
    /// Number of enriched samples taken.
    pub total_samples: u32,
    /// Detected compaction signatures.
    pub detected_signatures: Vec<String>,
}

impl DiagnosticContext {
    /// Build a prompt string for LLM analysis.
    pub fn to_prompt(&self) -> String {
        let output_section = if self.recent_output.is_empty() {
            "(no output captured)".to_string()
        } else {
            self.recent_output.join("\n")
        };

        format!(
            "Analyze this process for signs of being stuck or unproductive.\n\n\
             Task: {}\n\
             Runtime: {:.0}s\n\
             Health score: {:.2} ({})\n\
             Repetition score: {:.2}\n\
             Seconds since output: {}\n\
             I/O progressing: {}\n\
             Detected signatures: {:?}\n\n\
             Recent output (last {} lines):\n{}\n\n\
             Respond with JSON: {{\"verdict\": \"stuck\"|\"degraded\"|\"healthy\"|\"unknown\", \
             \"confidence\": 0.0-1.0, \"reasoning\": \"...\", \"recommendation\": \"kill\"|\"retry\"|\"wait\"|\"escalate\"}}",
            self.task_objective,
            self.runtime_seconds,
            self.health_report.score,
            self.health_report.level,
            self.output_analysis.repetition_score,
            self.output_analysis
                .seconds_since_output
                .map(|s| format!("{s:.0}"))
                .unwrap_or_else(|| "never".to_string()),
            self.output_analysis.io_progressing,
            self.detected_signatures,
            self.recent_output.len(),
            output_section,
        )
    }
}

/// Trait for dispatching LLM analysis requests.
/// Implemented by yarli-sw4rm's RouterSender or a mock for testing.
#[async_trait::async_trait]
pub trait IntrospectionRouter: Send + Sync {
    /// Send a diagnostic prompt and receive a verdict.
    async fn analyze(&self, context: &DiagnosticContext) -> Result<IntrospectionVerdict, String>;
}

/// Event emitted when the health level changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthTransitionEvent {
    /// Previous health level before the transition.
    pub previous_level: HealthLevel,
    /// New health level after the transition.
    pub new_level: HealthLevel,
    /// Health score at the time of transition.
    pub score: f64,
    /// Factor breakdown at the time of transition.
    pub factors: HealthFactors,
}

/// Controller that orchestrates introspection alongside command execution.
pub struct IntrospectionController {
    pid: u32,
    analyzer: OutputAnalyzer,
    scorer: HealthScorer,
    recent_output: VecDeque<String>,
    max_output_lines: usize,
    started_at: Instant,
    task_objective: String,
    health_tx: tokio::sync::watch::Sender<HealthReport>,
    health_rx: tokio::sync::watch::Receiver<HealthReport>,
    last_emitted_level: HealthLevel,
}

impl IntrospectionController {
    /// Create a new controller for the given process.
    pub fn new(pid: u32, task_objective: String) -> Self {
        let initial_report = HealthReport {
            score: 1.0,
            level: HealthLevel::Healthy,
            factors: HealthFactors {
                output_recency: 1.0,
                io_delta: 1.0,
                cpu_utilization: 0.5,
                repetition: 1.0,
                sleep_ratio: 1.0,
            },
        };
        let (health_tx, health_rx) = tokio::sync::watch::channel(initial_report);

        Self {
            pid,
            analyzer: OutputAnalyzer::new(50),
            scorer: HealthScorer::new(),
            recent_output: VecDeque::with_capacity(50),
            max_output_lines: 50,
            started_at: Instant::now(),
            task_objective,
            health_tx,
            health_rx,
            last_emitted_level: HealthLevel::Healthy,
        }
    }

    /// Get a receiver for health updates.
    pub fn health_receiver(&self) -> tokio::sync::watch::Receiver<HealthReport> {
        self.health_rx.clone()
    }

    /// Feed a line of output from the command.
    pub fn feed_output(&mut self, line: &str) {
        self.analyzer.feed_line(line);
        if self.recent_output.len() >= self.max_output_lines {
            self.recent_output.pop_front();
        }
        self.recent_output.push_back(line.to_string());
    }

    /// Take an enriched sample and update health score.
    pub fn sample(&mut self) -> Option<HealthReport> {
        let sample = read_enriched_sample(self.pid)?;

        // Feed I/O bytes
        let total_io = sample
            .io_read_bytes
            .unwrap_or(0)
            .saturating_add(sample.io_write_bytes.unwrap_or(0));
        self.analyzer.feed_io_bytes(total_io);

        let analysis = self.analyzer.analyze(Some(total_io));
        let report = self.scorer.score(&sample, &analysis);

        // Publish to watch channel
        let _ = self.health_tx.send(report.clone());

        debug!(
            pid = self.pid,
            score = report.score,
            level = %report.level,
            "introspection sample"
        );

        Some(report)
    }

    /// Build a diagnostic context for LLM analysis.
    pub fn build_diagnostic_context(&self) -> DiagnosticContext {
        let total_io = None; // We don't have current I/O without a fresh sample
        let analysis = self.analyzer.analyze(total_io);
        let report = self.health_rx.borrow().clone();

        DiagnosticContext {
            recent_output: self.recent_output.iter().cloned().collect(),
            health_report: report,
            output_analysis: analysis,
            task_objective: self.task_objective.clone(),
            runtime_seconds: self.started_at.elapsed().as_secs_f64(),
            total_samples: self.scorer.total_samples,
            detected_signatures: self.analyzer.compaction.detected_signatures().to_vec(),
        }
    }

    /// Check if the health level has changed since the last emission.
    ///
    /// Returns `Some(HealthTransitionEvent)` if the level changed, `None` otherwise.
    /// Calling this updates the internal `last_emitted_level` tracker.
    pub fn check_health_transition(&mut self) -> Option<HealthTransitionEvent> {
        let current = self.health_rx.borrow().clone();
        if current.level == self.last_emitted_level {
            return None;
        }
        let previous_level = self.last_emitted_level;
        self.last_emitted_level = current.level;
        Some(HealthTransitionEvent {
            previous_level,
            new_level: current.level,
            score: current.score,
            factors: current.factors,
        })
    }

    /// Update the in-memory health report without a new process sample.
    ///
    /// This is intended for adapters that replay externally-derived health
    /// observations back into transition tracking.
    pub fn set_health_report(&self, report: HealthReport) -> bool {
        self.health_tx.send(report).is_ok()
    }

    /// Run the introspection loop at the given sampling interval.
    /// Calls `on_degraded` when health drops to Degraded, and
    /// `on_stuck` when it drops to Stuck (with diagnostic context).
    pub async fn run_loop(
        &mut self,
        base_interval: Duration,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let mut interval = tokio::time::interval(base_interval);
        let mut current_level = HealthLevel::Healthy;

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    debug!(pid = self.pid, "introspection loop cancelled");
                    break;
                }
                _ = interval.tick() => {
                    let Some(report) = self.sample() else {
                        // Process exited
                        debug!(pid = self.pid, "process exited during introspection");
                        break;
                    };

                    match report.level {
                        HealthLevel::Degraded if current_level == HealthLevel::Healthy => {
                            info!(
                                pid = self.pid,
                                score = report.score,
                                "process health degraded, increasing sample rate"
                            );
                            // Increase sample rate to 2x
                            interval = tokio::time::interval(base_interval / 2);
                        }
                        HealthLevel::Stuck if current_level != HealthLevel::Stuck => {
                            warn!(
                                pid = self.pid,
                                score = report.score,
                                runtime_secs = self.started_at.elapsed().as_secs(),
                                "process appears stuck"
                            );
                        }
                        HealthLevel::Healthy if current_level != HealthLevel::Healthy => {
                            info!(
                                pid = self.pid,
                                score = report.score,
                                "process health recovered"
                            );
                            interval = tokio::time::interval(base_interval);
                        }
                        _ => {}
                    }
                    current_level = report.level;
                }
            }
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- Layer 2: RepetitionDetector ---

    #[test]
    fn repetition_detector_all_unique_lines_scores_low() {
        let mut det = RepetitionDetector::new(10);
        for i in 0..10 {
            det.feed(&format!("unique line {i}"));
        }
        assert!(det.score() < 0.3, "score={}", det.score());
    }

    #[test]
    fn repetition_detector_identical_lines_scores_high() {
        let mut det = RepetitionDetector::new(10);
        for _ in 0..10 {
            det.feed("identical output");
        }
        assert!(
            (det.score() - 1.0).abs() < f64::EPSILON,
            "score={}",
            det.score()
        );
    }

    #[test]
    fn repetition_detector_mixed_lines() {
        let mut det = RepetitionDetector::new(10);
        for _ in 0..5 {
            det.feed("repeated");
        }
        for i in 0..5 {
            det.feed(&format!("unique {i}"));
        }
        assert!(
            det.score() > 0.3 && det.score() < 0.8,
            "score={}",
            det.score()
        );
    }

    #[test]
    fn repetition_detector_empty_window_returns_zero() {
        let det = RepetitionDetector::new(10);
        assert_eq!(det.score(), 0.0);
    }

    #[test]
    fn repetition_detector_single_line_returns_zero() {
        let mut det = RepetitionDetector::new(10);
        det.feed("one line");
        assert_eq!(det.score(), 0.0);
    }

    #[test]
    fn repetition_detector_window_slides() {
        let mut det = RepetitionDetector::new(5);
        // Fill with repeated
        for _ in 0..5 {
            det.feed("repeated");
        }
        assert!((det.score() - 1.0).abs() < f64::EPSILON);
        // Push unique lines to slide out the repeated ones
        for i in 0..5 {
            det.feed(&format!("unique_{i}"));
        }
        assert!(det.score() < 0.4, "score={}", det.score());
    }

    // --- Layer 2: ZeroProgressDetector ---

    #[test]
    fn zero_progress_no_output_returns_none() {
        let det = ZeroProgressDetector::new();
        assert!(det.time_since_last_output().is_none());
    }

    #[test]
    fn zero_progress_after_output_returns_some() {
        let mut det = ZeroProgressDetector::new();
        det.record_output();
        let elapsed = det.time_since_last_output().unwrap();
        assert!(elapsed < Duration::from_millis(100));
    }

    #[test]
    fn zero_progress_io_bytes_initial_always_changed() {
        let det = ZeroProgressDetector::new();
        assert!(det.io_bytes_changed(100));
    }

    #[test]
    fn zero_progress_io_bytes_unchanged() {
        let mut det = ZeroProgressDetector::new();
        det.record_io_bytes(100);
        assert!(!det.io_bytes_changed(100));
    }

    #[test]
    fn zero_progress_io_bytes_changed() {
        let mut det = ZeroProgressDetector::new();
        det.record_io_bytes(100);
        assert!(det.io_bytes_changed(200));
    }

    // --- Layer 2: CompactionSignatureDetector ---

    #[test]
    fn compaction_detects_context_window() {
        let mut det = CompactionSignatureDetector::new();
        assert!(det.feed("Warning: approaching context window limit"));
        assert_eq!(det.detected_signatures().len(), 1);
    }

    #[test]
    fn compaction_detects_compaction() {
        let mut det = CompactionSignatureDetector::new();
        assert!(det.feed("Performing compaction of conversation history"));
        assert_eq!(det.detected_signatures().len(), 1);
    }

    #[test]
    fn compaction_case_insensitive() {
        let mut det = CompactionSignatureDetector::new();
        assert!(det.feed("CONTEXT WINDOW exceeded"));
    }

    #[test]
    fn compaction_no_match_returns_false() {
        let mut det = CompactionSignatureDetector::new();
        assert!(!det.feed("normal output line"));
        assert!(det.detected_signatures().is_empty());
    }

    // --- Layer 2: OutputAnalyzer ---

    #[test]
    fn output_analyzer_fresh_state() {
        let analyzer = OutputAnalyzer::new(10);
        let analysis = analyzer.analyze(None);
        assert_eq!(analysis.repetition_score, 0.0);
        assert!(analysis.seconds_since_output.is_none());
        assert!(analysis.io_progressing);
        assert!(analysis.signatures.is_empty());
    }

    #[test]
    fn output_analyzer_after_feeding() {
        let mut analyzer = OutputAnalyzer::new(10);
        analyzer.feed_line("hello world");
        analyzer.feed_line("hello world");
        let analysis = analyzer.analyze(None);
        assert!(analysis.repetition_score > 0.5);
        assert!(analysis.seconds_since_output.is_some());
    }

    // --- Layer 3: HealthLevel ---

    #[test]
    fn health_level_from_score_boundaries() {
        assert_eq!(HealthLevel::from_score(1.0), HealthLevel::Healthy);
        assert_eq!(HealthLevel::from_score(0.7), HealthLevel::Healthy);
        assert_eq!(HealthLevel::from_score(0.69), HealthLevel::Degraded);
        assert_eq!(HealthLevel::from_score(0.3), HealthLevel::Degraded);
        assert_eq!(HealthLevel::from_score(0.29), HealthLevel::Stuck);
        assert_eq!(HealthLevel::from_score(0.0), HealthLevel::Stuck);
    }

    // --- Layer 3: HealthScorer ---

    #[test]
    fn health_scorer_healthy_process() {
        let mut scorer = HealthScorer::new();
        let sample = EnrichedProcessSample {
            rss_bytes: Some(1024 * 1024),
            cpu_user_ticks: Some(100),
            cpu_system_ticks: Some(10),
            io_read_bytes: Some(5000),
            io_write_bytes: Some(3000),
            process_state: Some('R'),
            thread_count: Some(4),
            fd_count: Some(10),
            sampled_at: Instant::now(),
            ..Default::default()
        };
        let analysis = OutputAnalysis {
            repetition_score: 0.1,
            seconds_since_output: Some(5.0),
            io_progressing: true,
            signatures: vec![],
        };
        let report = scorer.score(&sample, &analysis);
        assert_eq!(report.level, HealthLevel::Healthy);
        assert!(report.score >= 0.7, "score={}", report.score);
    }

    #[test]
    fn health_scorer_stuck_process() {
        let mut scorer = HealthScorer::new();
        // Prime with initial CPU ticks so delta is 0
        let sample_init = EnrichedProcessSample {
            cpu_user_ticks: Some(100),
            cpu_system_ticks: Some(10),
            process_state: Some('S'),
            sampled_at: Instant::now(),
            ..Default::default()
        };
        let analysis_init = OutputAnalysis {
            repetition_score: 0.0,
            seconds_since_output: Some(0.0),
            io_progressing: true,
            signatures: vec![],
        };
        scorer.score(&sample_init, &analysis_init);

        // Now score with stuck indicators
        let sample = EnrichedProcessSample {
            cpu_user_ticks: Some(100), // No change
            cpu_system_ticks: Some(10),
            io_read_bytes: Some(0),
            io_write_bytes: Some(0),
            process_state: Some('S'),
            sampled_at: Instant::now(),
            ..Default::default()
        };
        let analysis = OutputAnalysis {
            repetition_score: 0.9,
            seconds_since_output: Some(1200.0), // 20 minutes
            io_progressing: false,
            signatures: vec!["context window".to_string()],
        };
        let report = scorer.score(&sample, &analysis);
        assert_eq!(report.level, HealthLevel::Stuck);
        assert!(report.score < 0.3, "score={}", report.score);
    }

    #[test]
    fn health_scorer_degraded_process() {
        let mut scorer = HealthScorer::new();
        let sample = EnrichedProcessSample {
            cpu_user_ticks: Some(50),
            cpu_system_ticks: Some(5),
            process_state: Some('S'),
            sampled_at: Instant::now(),
            ..Default::default()
        };
        let analysis = OutputAnalysis {
            repetition_score: 0.5,
            seconds_since_output: Some(600.0), // 10 minutes
            io_progressing: true,
            signatures: vec![],
        };
        let report = scorer.score(&sample, &analysis);
        assert_eq!(report.level, HealthLevel::Degraded);
    }

    // --- Layer 4: DiagnosticContext ---

    #[test]
    fn diagnostic_context_to_prompt_contains_task_info() {
        let ctx = DiagnosticContext {
            recent_output: vec!["line 1".to_string(), "line 2".to_string()],
            health_report: HealthReport {
                score: 0.2,
                level: HealthLevel::Stuck,
                factors: HealthFactors {
                    output_recency: 0.0,
                    io_delta: 0.0,
                    cpu_utilization: 0.0,
                    repetition: 0.0,
                    sleep_ratio: 0.0,
                },
            },
            output_analysis: OutputAnalysis {
                repetition_score: 0.9,
                seconds_since_output: Some(1200.0),
                io_progressing: false,
                signatures: vec!["context window".to_string()],
            },
            task_objective: "run tests".to_string(),
            runtime_seconds: 300.0,
            total_samples: 50,
            detected_signatures: vec!["context window".to_string()],
        };
        let prompt = ctx.to_prompt();
        assert!(prompt.contains("run tests"));
        assert!(prompt.contains("300s"));
        assert!(prompt.contains("0.20"));
        assert!(prompt.contains("stuck"));
        assert!(prompt.contains("line 1"));
        assert!(prompt.contains("context window"));
    }

    #[test]
    fn diagnostic_context_empty_output() {
        let ctx = DiagnosticContext {
            recent_output: vec![],
            health_report: HealthReport {
                score: 0.5,
                level: HealthLevel::Degraded,
                factors: HealthFactors {
                    output_recency: 0.5,
                    io_delta: 0.5,
                    cpu_utilization: 0.5,
                    repetition: 0.5,
                    sleep_ratio: 0.5,
                },
            },
            output_analysis: OutputAnalysis {
                repetition_score: 0.0,
                seconds_since_output: None,
                io_progressing: true,
                signatures: vec![],
            },
            task_objective: "build project".to_string(),
            runtime_seconds: 60.0,
            total_samples: 10,
            detected_signatures: vec![],
        };
        let prompt = ctx.to_prompt();
        assert!(prompt.contains("no output captured"));
    }

    // --- IntrospectionController ---

    #[test]
    fn controller_feed_output_updates_recent() {
        let mut ctrl = IntrospectionController::new(99999, "test task".to_string());
        for i in 0..60 {
            ctrl.feed_output(&format!("line {i}"));
        }
        // Should keep only last 50
        assert_eq!(ctrl.recent_output.len(), 50);
        assert!(ctrl.recent_output.front().unwrap().contains("10"));
    }

    #[test]
    fn controller_health_receiver_gets_initial_healthy() {
        let ctrl = IntrospectionController::new(99999, "test".to_string());
        let rx = ctrl.health_receiver();
        assert_eq!(rx.borrow().level, HealthLevel::Healthy);
    }

    #[test]
    fn controller_build_diagnostic_context() {
        let mut ctrl = IntrospectionController::new(99999, "run tests".to_string());
        ctrl.feed_output("test output");
        let ctx = ctrl.build_diagnostic_context();
        assert_eq!(ctx.task_objective, "run tests");
        assert_eq!(ctx.recent_output.len(), 1);
        assert!(ctx.runtime_seconds >= 0.0);
    }

    // --- HealthTransitionEvent ---

    #[test]
    fn health_transition_detected_on_level_change() {
        let mut ctrl = IntrospectionController::new(99999, "test".to_string());
        // Initially Healthy; simulate a degraded report via watch channel
        let degraded_report = HealthReport {
            score: 0.5,
            level: HealthLevel::Degraded,
            factors: HealthFactors {
                output_recency: 0.5,
                io_delta: 0.5,
                cpu_utilization: 0.5,
                repetition: 0.5,
                sleep_ratio: 0.5,
            },
        };
        ctrl.health_tx.send(degraded_report).unwrap();
        let transition = ctrl.check_health_transition();
        assert!(transition.is_some());
        let t = transition.unwrap();
        assert_eq!(t.previous_level, HealthLevel::Healthy);
        assert_eq!(t.new_level, HealthLevel::Degraded);
    }

    #[test]
    fn no_transition_when_level_unchanged() {
        let mut ctrl = IntrospectionController::new(99999, "test".to_string());
        // Health is Healthy initially; send another Healthy report
        let healthy_report = HealthReport {
            score: 0.9,
            level: HealthLevel::Healthy,
            factors: HealthFactors {
                output_recency: 1.0,
                io_delta: 1.0,
                cpu_utilization: 0.5,
                repetition: 1.0,
                sleep_ratio: 1.0,
            },
        };
        ctrl.health_tx.send(healthy_report).unwrap();
        assert!(ctrl.check_health_transition().is_none());
    }

    #[test]
    fn transition_back_to_healthy() {
        let mut ctrl = IntrospectionController::new(99999, "test".to_string());
        // Go to Degraded first
        let degraded = HealthReport {
            score: 0.5,
            level: HealthLevel::Degraded,
            factors: HealthFactors {
                output_recency: 0.5,
                io_delta: 0.5,
                cpu_utilization: 0.5,
                repetition: 0.5,
                sleep_ratio: 0.5,
            },
        };
        ctrl.health_tx.send(degraded).unwrap();
        let _ = ctrl.check_health_transition(); // consume Healthy→Degraded

        // Now go back to Healthy
        let healthy = HealthReport {
            score: 0.9,
            level: HealthLevel::Healthy,
            factors: HealthFactors {
                output_recency: 1.0,
                io_delta: 1.0,
                cpu_utilization: 0.5,
                repetition: 1.0,
                sleep_ratio: 1.0,
            },
        };
        ctrl.health_tx.send(healthy).unwrap();
        let transition = ctrl.check_health_transition();
        assert!(transition.is_some());
        let t = transition.unwrap();
        assert_eq!(t.previous_level, HealthLevel::Degraded);
        assert_eq!(t.new_level, HealthLevel::Healthy);
    }

    #[test]
    fn transition_tracks_previous_level() {
        let mut ctrl = IntrospectionController::new(99999, "test".to_string());
        // Healthy → Degraded
        ctrl.health_tx
            .send(HealthReport {
                score: 0.5,
                level: HealthLevel::Degraded,
                factors: HealthFactors {
                    output_recency: 0.5,
                    io_delta: 0.5,
                    cpu_utilization: 0.5,
                    repetition: 0.5,
                    sleep_ratio: 0.5,
                },
            })
            .unwrap();
        let t1 = ctrl.check_health_transition().unwrap();
        assert_eq!(t1.previous_level, HealthLevel::Healthy);
        assert_eq!(t1.new_level, HealthLevel::Degraded);

        // Degraded → Stuck
        ctrl.health_tx
            .send(HealthReport {
                score: 0.1,
                level: HealthLevel::Stuck,
                factors: HealthFactors {
                    output_recency: 0.0,
                    io_delta: 0.0,
                    cpu_utilization: 0.0,
                    repetition: 0.0,
                    sleep_ratio: 0.0,
                },
            })
            .unwrap();
        let t2 = ctrl.check_health_transition().unwrap();
        assert_eq!(t2.previous_level, HealthLevel::Degraded);
        assert_eq!(t2.new_level, HealthLevel::Stuck);
    }
}
