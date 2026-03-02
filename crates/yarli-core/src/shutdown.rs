//! Graceful shutdown infrastructure (Section 31).
//!
//! Two-stage interrupt protocol:
//! - First Ctrl+C: graceful shutdown — cancel running commands (SIGTERM children),
//!   persist state, clean up terminal, exit 130.
//! - Second Ctrl+C within 2s: force exit — skip cleanup, immediate process exit.
//!
//! `CancellationToken` from `tokio_util` is propagated to every async task/worker.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::domain::{
    CancellationActorKind, CancellationProvenance, CancellationSource, CancellationStage,
};

/// How long after the first Ctrl+C a second Ctrl+C triggers force exit.
const FORCE_EXIT_WINDOW: Duration = Duration::from_secs(2);

/// How long to wait after SIGTERM before escalating to SIGKILL.
const SIGKILL_ESCALATION: Duration = Duration::from_secs(5);

/// Current phase of shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShutdownPhase {
    /// Normal operation.
    Running = 0,
    /// Graceful shutdown in progress (first Ctrl+C received).
    Graceful = 1,
    /// Force exit requested (second Ctrl+C within window).
    Force = 2,
}

impl ShutdownPhase {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Running,
            1 => Self::Graceful,
            2 => Self::Force,
            _ => Self::Force,
        }
    }
}

/// Coordinates graceful shutdown across the entire application.
///
/// Clone-friendly: all clones share the same underlying state.
/// Propagate the `token()` to every async task so they can observe cancellation.
#[derive(Clone)]
pub struct ShutdownController {
    inner: Arc<ShutdownInner>,
}

impl std::fmt::Debug for ShutdownController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownController")
            .field("phase", &self.phase())
            .finish()
    }
}

struct ShutdownInner {
    /// CancellationToken propagated to all async tasks.
    token: CancellationToken,
    /// Current shutdown phase (atomic for signal-handler safety).
    phase: AtomicU8,
    /// Timestamp of first Ctrl+C (set once; 0 means not set).
    /// We use AtomicBool + Notify instead of storing Instant atomically.
    first_signal_received: AtomicBool,
    /// Source of the first graceful shutdown request.
    /// Encoded as u8; 0 means "unset".
    cancellation_source: AtomicU8,
    /// Best-effort cancellation provenance captured at graceful request time.
    cancellation_provenance: std::sync::Mutex<Option<CancellationProvenance>>,
    /// Notifies waiters that graceful shutdown has started.
    graceful_notify: Notify,
    /// Notifies waiters that force shutdown was requested.
    force_notify: Notify,
    /// Tracked child PIDs for SIGTERM/SIGKILL escalation.
    #[cfg(unix)]
    children: std::sync::Mutex<Vec<u32>>,
}

impl ShutdownController {
    const SOURCE_UNSET: u8 = 0;
    const SOURCE_OPERATOR: u8 = 1;
    const SOURCE_SIGINT: u8 = 2;
    const SOURCE_SIGTERM: u8 = 3;
    const SOURCE_SW4RM_PREEMPTION: u8 = 4;
    const SOURCE_UNKNOWN: u8 = 5;

    fn encode_source(source: CancellationSource) -> u8 {
        match source {
            CancellationSource::Operator => Self::SOURCE_OPERATOR,
            CancellationSource::Sigint => Self::SOURCE_SIGINT,
            CancellationSource::Sigterm => Self::SOURCE_SIGTERM,
            CancellationSource::Sw4rmPreemption => Self::SOURCE_SW4RM_PREEMPTION,
            CancellationSource::Unknown => Self::SOURCE_UNKNOWN,
        }
    }

    fn decode_source(raw: u8) -> Option<CancellationSource> {
        match raw {
            Self::SOURCE_OPERATOR => Some(CancellationSource::Operator),
            Self::SOURCE_SIGINT => Some(CancellationSource::Sigint),
            Self::SOURCE_SIGTERM => Some(CancellationSource::Sigterm),
            Self::SOURCE_SW4RM_PREEMPTION => Some(CancellationSource::Sw4rmPreemption),
            Self::SOURCE_UNKNOWN => Some(CancellationSource::Unknown),
            _ => None,
        }
    }

    /// Create a new controller in the `Running` phase.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownInner {
                token: CancellationToken::new(),
                phase: AtomicU8::new(ShutdownPhase::Running as u8),
                first_signal_received: AtomicBool::new(false),
                cancellation_source: AtomicU8::new(Self::SOURCE_UNSET),
                cancellation_provenance: std::sync::Mutex::new(None),
                graceful_notify: Notify::new(),
                force_notify: Notify::new(),
                #[cfg(unix)]
                children: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    /// The cancellation token to propagate to async tasks.
    ///
    /// Tasks should select on `token.cancelled()` to observe shutdown.
    pub fn token(&self) -> CancellationToken {
        self.inner.token.clone()
    }

    /// Current shutdown phase.
    pub fn phase(&self) -> ShutdownPhase {
        ShutdownPhase::from_u8(self.inner.phase.load(Ordering::SeqCst))
    }

    /// Whether graceful or force shutdown has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.phase() != ShutdownPhase::Running
    }

    /// Return the source that initiated graceful cancellation, if known.
    pub fn cancellation_source(&self) -> Option<CancellationSource> {
        Self::decode_source(self.inner.cancellation_source.load(Ordering::SeqCst))
    }

    /// Return structured provenance for the cancellation trigger, if captured.
    pub fn cancellation_provenance(&self) -> Option<CancellationProvenance> {
        self.inner
            .cancellation_provenance
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Initiate graceful shutdown (first signal).
    ///
    /// Returns `true` if this call transitioned from Running → Graceful.
    /// Returns `false` if already shutting down (caller should escalate to force).
    pub fn request_graceful_with_source(&self, source: CancellationSource) -> bool {
        self.request_graceful_with_provenance(source, None)
    }

    /// Initiate graceful shutdown and capture explicit provenance metadata.
    ///
    /// Returns `true` if this call transitioned from Running → Graceful.
    /// Returns `false` if already shutting down.
    pub fn request_graceful_with_provenance(
        &self,
        source: CancellationSource,
        provenance: Option<CancellationProvenance>,
    ) -> bool {
        let prev = self.inner.phase.compare_exchange(
            ShutdownPhase::Running as u8,
            ShutdownPhase::Graceful as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );

        if prev.is_ok() {
            self.inner
                .first_signal_received
                .store(true, Ordering::SeqCst);
            self.inner
                .cancellation_source
                .store(Self::encode_source(source), Ordering::SeqCst);
            if let Ok(mut slot) = self.inner.cancellation_provenance.lock() {
                let mut value = provenance.unwrap_or_else(|| fallback_provenance(source));
                value.stage = None;
                *slot = Some(value);
            }
            self.inner.token.cancel();
            self.inner.graceful_notify.notify_waiters();
            info!(source = %source, "graceful shutdown initiated");
            true
        } else {
            false
        }
    }

    /// Initiate graceful shutdown with unknown source (legacy behavior).
    pub fn request_graceful(&self) -> bool {
        self.request_graceful_with_source(CancellationSource::Unknown)
    }

    /// Escalate to force shutdown (second signal).
    ///
    /// Returns `true` if this call transitioned to Force phase.
    pub fn request_force(&self) -> bool {
        let prev = self
            .inner
            .phase
            .swap(ShutdownPhase::Force as u8, Ordering::SeqCst);
        if prev != ShutdownPhase::Force as u8 {
            // Ensure token is cancelled (might already be from graceful).
            self.inner.token.cancel();
            self.inner.force_notify.notify_waiters();
            warn!("force shutdown requested — skipping cleanup");
            true
        } else {
            false
        }
    }

    /// Handle a signal (Ctrl+C / SIGINT / SIGTERM).
    ///
    /// Implements the two-stage protocol:
    /// - First call → graceful shutdown.
    /// - Second call (while still in Graceful phase) → force exit.
    pub fn on_signal(&self) {
        let provenance = signal_provenance(CancellationSource::Sigint, "SIGINT", 2);
        if !self.request_graceful_with_provenance(CancellationSource::Sigint, Some(provenance)) {
            // Already in graceful or force — escalate.
            self.request_force();
        }
    }

    /// Wait until graceful shutdown is requested.
    pub async fn wait_for_graceful(&self) {
        if self.inner.phase.load(Ordering::SeqCst) >= ShutdownPhase::Graceful as u8 {
            return;
        }
        self.inner.graceful_notify.notified().await;
    }

    /// Wait until force shutdown is requested.
    pub async fn wait_for_force(&self) {
        if self.inner.phase.load(Ordering::SeqCst) >= ShutdownPhase::Force as u8 {
            return;
        }
        self.inner.force_notify.notified().await;
    }

    /// Register a child process PID for managed shutdown.
    #[cfg(unix)]
    pub fn track_child(&self, pid: u32) {
        if let Ok(mut children) = self.inner.children.lock() {
            children.push(pid);
        }
    }

    /// Remove a child process PID (e.g., after it exits normally).
    #[cfg(unix)]
    pub fn untrack_child(&self, pid: u32) {
        if let Ok(mut children) = self.inner.children.lock() {
            children.retain(|&p| p != pid);
        }
    }

    /// Send SIGTERM to all tracked children (and their process groups), then
    /// SIGKILL after `SIGKILL_ESCALATION`.
    ///
    /// Each tracked PID is also used as a process group ID (PGID) because the
    /// runner calls `setpgid(0,0)` before spawning, making each child its own
    /// process group leader. Signalling `-pgid` ensures descendant processes
    /// (e.g. codex backends) are also terminated.
    #[cfg(unix)]
    pub async fn terminate_children(&self) {
        use tokio::time::sleep;

        let pids: Vec<u32> = {
            let children = self
                .inner
                .children
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            children.clone()
        };

        if pids.is_empty() {
            return;
        }

        info!(count = pids.len(), "sending SIGTERM to tracked children");
        for &pid in &pids {
            let pgid = pid as i32;
            // Signal the process group first (negative PID), then the process
            // itself as a fallback in case it changed its own group.
            unsafe {
                libc::kill(-pgid, libc::SIGTERM);
                libc::kill(pgid, libc::SIGTERM);
            }
        }

        // Wait for escalation period, then SIGKILL any survivors.
        sleep(SIGKILL_ESCALATION).await;

        let remaining: Vec<u32> = {
            let children = self
                .inner
                .children
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            children.clone()
        };

        for &pid in &remaining {
            let pgid = pid as i32;
            warn!(pid, "escalating to SIGKILL");
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
                libc::kill(pgid, libc::SIGKILL);
            }
        }
    }

    /// Install a tokio signal handler that drives the two-stage protocol.
    ///
    /// This spawns a background task that listens for SIGINT (Ctrl+C) and SIGTERM,
    /// calling `on_signal()` for each received signal.
    ///
    /// # Panics
    /// Panics if called outside a tokio runtime.
    #[cfg(unix)]
    pub fn install_signal_handler(&self) {
        let controller = self.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigint =
                signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

            let mut first_signal_time: Option<Instant> = None;

            loop {
                let (source, signal_name, signal_number) = tokio::select! {
                    _ = sigint.recv() => (CancellationSource::Sigint, "SIGINT", libc::SIGINT),
                    _ = sigterm.recv() => (CancellationSource::Sigterm, "SIGTERM", libc::SIGTERM),
                };

                match controller.phase() {
                    ShutdownPhase::Running => {
                        let provenance = signal_provenance(source, signal_name, signal_number);
                        controller.request_graceful_with_provenance(source, Some(provenance));
                        first_signal_time = Some(Instant::now());
                    }
                    ShutdownPhase::Graceful => {
                        // Second signal: check if within the force-exit window.
                        let within_window = first_signal_time
                            .map(|t| t.elapsed() < FORCE_EXIT_WINDOW)
                            .unwrap_or(true);

                        if within_window {
                            controller.request_force();
                            // Force exit: don't wait for cleanup.
                            std::process::exit(130);
                        } else {
                            // Outside window — treat as another graceful reminder.
                            // Reset the window for a potential third signal.
                            first_signal_time = Some(Instant::now());
                            info!("shutdown already in progress, press Ctrl+C again within 2s to force exit");
                        }
                    }
                    ShutdownPhase::Force => {
                        // Already forcing — hard exit.
                        std::process::exit(130);
                    }
                }
            }
        });
    }

    /// Install signal handler (non-Unix stub).
    #[cfg(not(unix))]
    pub fn install_signal_handler(&self) {
        let controller = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install Ctrl+C handler");
                let provenance = fallback_provenance(CancellationSource::Sigint);
                if !controller
                    .request_graceful_with_provenance(CancellationSource::Sigint, Some(provenance))
                {
                    controller.request_force();
                }

                if controller.phase() == ShutdownPhase::Force {
                    std::process::exit(130);
                }
            }
        });
    }
}

fn signal_provenance(
    source: CancellationSource,
    signal_name: &str,
    signal_number: i32,
) -> CancellationProvenance {
    let mut provenance = fallback_provenance(source);
    provenance.signal_name = Some(signal_name.to_string());
    provenance.signal_number = Some(signal_number);
    provenance
}

fn fallback_provenance(source: CancellationSource) -> CancellationProvenance {
    let (receiver_pid, parent_pid, process_group_id, session_id, tty) = capture_process_context();
    let actor_kind = infer_actor_kind(source, parent_pid, tty.as_deref());
    let actor_detail = default_actor_detail(source, actor_kind);
    CancellationProvenance {
        cancellation_source: source,
        signal_name: None,
        signal_number: None,
        sender_pid: None,
        receiver_pid,
        parent_pid,
        process_group_id,
        session_id,
        tty,
        actor_kind: Some(actor_kind),
        actor_detail,
        stage: Some(CancellationStage::Unknown),
    }
}

fn infer_actor_kind(
    source: CancellationSource,
    parent_pid: Option<u32>,
    tty: Option<&str>,
) -> CancellationActorKind {
    match source {
        CancellationSource::Operator | CancellationSource::Sigint => {
            CancellationActorKind::Operator
        }
        CancellationSource::Sw4rmPreemption => CancellationActorKind::Supervisor,
        CancellationSource::Sigterm => {
            if tty.is_some() {
                CancellationActorKind::Operator
            } else if parent_pid == Some(1) {
                CancellationActorKind::Supervisor
            } else {
                CancellationActorKind::System
            }
        }
        CancellationSource::Unknown => CancellationActorKind::Unknown,
    }
}

fn default_actor_detail(
    source: CancellationSource,
    actor_kind: CancellationActorKind,
) -> Option<String> {
    match source {
        CancellationSource::Operator => {
            Some("operator control command requested cancellation".to_string())
        }
        CancellationSource::Sigint => Some("SIGINT observed by runtime signal handler".to_string()),
        CancellationSource::Sigterm => match actor_kind {
            CancellationActorKind::Supervisor => {
                Some("SIGTERM likely originated from a supervisor-managed parent".to_string())
            }
            CancellationActorKind::Operator => {
                Some("SIGTERM observed from interactive session context".to_string())
            }
            _ => Some("SIGTERM sender PID unavailable via tokio signal API".to_string()),
        },
        CancellationSource::Sw4rmPreemption => {
            Some("sw4rm preemption bridge requested graceful shutdown".to_string())
        }
        CancellationSource::Unknown => {
            Some("shutdown token cancellation source not attributed".to_string())
        }
    }
}

#[cfg(unix)]
#[allow(clippy::type_complexity)]
fn capture_process_context() -> (
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<String>,
) {
    let receiver_pid = Some(std::process::id());

    let parent_pid = {
        let raw = unsafe { libc::getppid() };
        if raw > 0 {
            Some(raw as u32)
        } else {
            None
        }
    };

    let process_group_id = {
        let raw = unsafe { libc::getpgrp() };
        if raw > 0 {
            Some(raw as u32)
        } else {
            None
        }
    };

    let session_id = {
        let raw = unsafe { libc::getsid(0) };
        if raw > 0 {
            Some(raw as u32)
        } else {
            None
        }
    };

    let tty = tty_name_for_fd(libc::STDIN_FILENO).or_else(|| tty_name_for_fd(libc::STDERR_FILENO));

    (receiver_pid, parent_pid, process_group_id, session_id, tty)
}

#[cfg(unix)]
fn tty_name_for_fd(fd: i32) -> Option<String> {
    unsafe {
        if libc::isatty(fd) != 1 {
            return None;
        }
        let mut buf = [0u8; 256];
        let rc = libc::ttyname_r(fd, buf.as_mut_ptr() as *mut libc::c_char, buf.len());
        if rc != 0 {
            return None;
        }
        let cstr = std::ffi::CStr::from_ptr(buf.as_ptr() as *const libc::c_char);
        cstr.to_str().ok().map(ToString::to_string)
    }
}

#[cfg(not(unix))]
fn capture_process_context() -> (
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<String>,
) {
    (Some(std::process::id()), None, None, None, None)
}

impl Default for ShutdownController {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for terminal state restoration (Section 31 checklist).
///
/// On drop (including panics), restores:
/// - Raw mode disabled
/// - Cursor shown
/// - Mouse capture disabled
/// - Alternate screen left
/// - Stdout flushed
///
/// This guard should be created **after** the signal handler is installed
/// (spec rule: signal handler installed before raw mode entry).
pub struct TerminalGuard {
    raw_mode_entered: bool,
    alternate_screen_entered: bool,
    mouse_capture_enabled: bool,
}

impl TerminalGuard {
    /// Create a new guard without entering any terminal mode.
    /// Call the `enter_*` methods to track which modes were activated.
    pub fn new() -> Self {
        Self {
            raw_mode_entered: false,
            alternate_screen_entered: false,
            mouse_capture_enabled: false,
        }
    }

    /// Record that raw mode was entered (will disable on drop).
    pub fn set_raw_mode(&mut self) {
        self.raw_mode_entered = true;
    }

    /// Record that alternate screen was entered (will leave on drop).
    pub fn set_alternate_screen(&mut self) {
        self.alternate_screen_entered = true;
    }

    /// Record that mouse capture was enabled (will disable on drop).
    pub fn set_mouse_capture(&mut self) {
        self.mouse_capture_enabled = true;
    }
}

impl Default for TerminalGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        use std::io::Write;

        // Best-effort cleanup — ignore errors since we may be panicking.
        let mut stdout = std::io::stdout();

        if self.mouse_capture_enabled {
            let _ = crossterm::execute!(stdout, crossterm::event::DisableMouseCapture);
        }

        if self.alternate_screen_entered {
            let _ = crossterm::execute!(stdout, crossterm::terminal::LeaveAlternateScreen);
        }

        let _ = crossterm::execute!(stdout, crossterm::cursor::Show);

        if self.raw_mode_entered {
            let _ = crossterm::terminal::disable_raw_mode();
        }

        let _ = stdout.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_controller_is_running() {
        let ctrl = ShutdownController::new();
        assert_eq!(ctrl.phase(), ShutdownPhase::Running);
        assert!(!ctrl.is_shutting_down());
    }

    #[test]
    fn request_graceful_transitions_to_graceful() {
        let ctrl = ShutdownController::new();
        assert!(ctrl.request_graceful());
        assert_eq!(ctrl.phase(), ShutdownPhase::Graceful);
        assert!(ctrl.is_shutting_down());
        assert!(ctrl.token().is_cancelled());
        assert_eq!(
            ctrl.cancellation_source(),
            Some(CancellationSource::Unknown)
        );
    }

    #[test]
    fn request_graceful_idempotent() {
        let ctrl = ShutdownController::new();
        assert!(ctrl.request_graceful());
        // Second call returns false — already in graceful.
        assert!(!ctrl.request_graceful());
        assert_eq!(ctrl.phase(), ShutdownPhase::Graceful);
    }

    #[test]
    fn request_force_transitions_to_force() {
        let ctrl = ShutdownController::new();
        ctrl.request_graceful();
        assert!(ctrl.request_force());
        assert_eq!(ctrl.phase(), ShutdownPhase::Force);
    }

    #[test]
    fn request_force_idempotent() {
        let ctrl = ShutdownController::new();
        ctrl.request_graceful();
        assert!(ctrl.request_force());
        assert!(!ctrl.request_force());
        assert_eq!(ctrl.phase(), ShutdownPhase::Force);
    }

    #[test]
    fn on_signal_first_goes_graceful() {
        let ctrl = ShutdownController::new();
        ctrl.on_signal();
        assert_eq!(ctrl.phase(), ShutdownPhase::Graceful);
        assert_eq!(ctrl.cancellation_source(), Some(CancellationSource::Sigint));
    }

    #[test]
    fn on_signal_second_goes_force() {
        let ctrl = ShutdownController::new();
        ctrl.on_signal();
        ctrl.on_signal();
        assert_eq!(ctrl.phase(), ShutdownPhase::Force);
    }

    #[test]
    fn token_is_cancelled_on_graceful() {
        let ctrl = ShutdownController::new();
        let token = ctrl.token();
        assert!(!token.is_cancelled());
        ctrl.request_graceful();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cloned_controller_shares_state() {
        let ctrl1 = ShutdownController::new();
        let ctrl2 = ctrl1.clone();
        ctrl1.request_graceful();
        assert_eq!(ctrl2.phase(), ShutdownPhase::Graceful);
        assert!(ctrl2.token().is_cancelled());
    }

    #[test]
    fn direct_force_from_running() {
        let ctrl = ShutdownController::new();
        // Can go directly to force (e.g., SIGKILL scenario).
        assert!(ctrl.request_force());
        assert_eq!(ctrl.phase(), ShutdownPhase::Force);
        assert!(ctrl.token().is_cancelled());
    }

    #[tokio::test]
    async fn wait_for_graceful_returns_immediately_if_already_shutting_down() {
        let ctrl = ShutdownController::new();
        ctrl.request_graceful();
        // Should return immediately, not hang.
        ctrl.wait_for_graceful().await;
    }

    #[tokio::test]
    async fn wait_for_force_returns_immediately_if_already_forced() {
        let ctrl = ShutdownController::new();
        ctrl.request_force();
        // Should return immediately, not hang.
        ctrl.wait_for_force().await;
    }

    #[cfg(unix)]
    #[test]
    fn track_and_untrack_children() {
        let ctrl = ShutdownController::new();
        ctrl.track_child(1234);
        ctrl.track_child(5678);
        {
            let children = ctrl.inner.children.lock().unwrap();
            assert_eq!(children.len(), 2);
        }
        ctrl.untrack_child(1234);
        {
            let children = ctrl.inner.children.lock().unwrap();
            assert_eq!(children.len(), 1);
            assert_eq!(children[0], 5678);
        }
    }

    #[test]
    fn terminal_guard_default() {
        // Just verify it can be created and dropped without panic.
        let _guard = TerminalGuard::new();
    }

    #[test]
    fn terminal_guard_with_modes() {
        let mut guard = TerminalGuard::new();
        guard.set_raw_mode();
        guard.set_alternate_screen();
        guard.set_mouse_capture();
        // Drop should not panic even though we never actually entered these modes.
        drop(guard);
    }

    #[test]
    fn phase_from_u8_covers_all_values() {
        assert_eq!(ShutdownPhase::from_u8(0), ShutdownPhase::Running);
        assert_eq!(ShutdownPhase::from_u8(1), ShutdownPhase::Graceful);
        assert_eq!(ShutdownPhase::from_u8(2), ShutdownPhase::Force);
        // Out of range defaults to Force (safe fallback).
        assert_eq!(ShutdownPhase::from_u8(255), ShutdownPhase::Force);
    }

    #[test]
    fn request_graceful_with_source_records_source() {
        let ctrl = ShutdownController::new();
        assert!(ctrl.request_graceful_with_source(CancellationSource::Sigterm));
        assert_eq!(
            ctrl.cancellation_source(),
            Some(CancellationSource::Sigterm)
        );
        let provenance = ctrl
            .cancellation_provenance()
            .expect("expected cancellation provenance");
        assert_eq!(provenance.cancellation_source, CancellationSource::Sigterm);
        assert!(provenance.receiver_pid.is_some());
    }
}
