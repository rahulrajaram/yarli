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

struct ShutdownInner {
    /// CancellationToken propagated to all async tasks.
    token: CancellationToken,
    /// Current shutdown phase (atomic for signal-handler safety).
    phase: AtomicU8,
    /// Timestamp of first Ctrl+C (set once; 0 means not set).
    /// We use AtomicBool + Notify instead of storing Instant atomically.
    first_signal_received: AtomicBool,
    /// Notifies waiters that graceful shutdown has started.
    graceful_notify: Notify,
    /// Notifies waiters that force shutdown was requested.
    force_notify: Notify,
    /// Tracked child PIDs for SIGTERM/SIGKILL escalation.
    #[cfg(unix)]
    children: std::sync::Mutex<Vec<u32>>,
}

impl ShutdownController {
    /// Create a new controller in the `Running` phase.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownInner {
                token: CancellationToken::new(),
                phase: AtomicU8::new(ShutdownPhase::Running as u8),
                first_signal_received: AtomicBool::new(false),
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

    /// Initiate graceful shutdown (first signal).
    ///
    /// Returns `true` if this call transitioned from Running → Graceful.
    /// Returns `false` if already shutting down (caller should escalate to force).
    pub fn request_graceful(&self) -> bool {
        let prev = self
            .inner
            .phase
            .compare_exchange(
                ShutdownPhase::Running as u8,
                ShutdownPhase::Graceful as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

        if prev.is_ok() {
            self.inner.first_signal_received.store(true, Ordering::SeqCst);
            self.inner.token.cancel();
            self.inner.graceful_notify.notify_waiters();
            info!("graceful shutdown initiated");
            true
        } else {
            false
        }
    }

    /// Escalate to force shutdown (second signal).
    ///
    /// Returns `true` if this call transitioned to Force phase.
    pub fn request_force(&self) -> bool {
        let prev = self.inner.phase.swap(ShutdownPhase::Force as u8, Ordering::SeqCst);
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
        if !self.request_graceful() {
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

    /// Send SIGTERM to all tracked children, then SIGKILL after `SIGKILL_ESCALATION`.
    ///
    /// This is called during graceful shutdown. The caller should await the returned future.
    #[cfg(unix)]
    pub async fn terminate_children(&self) {
        use tokio::time::sleep;

        let pids: Vec<u32> = {
            let children = self.inner.children.lock().unwrap_or_else(|e| e.into_inner());
            children.clone()
        };

        if pids.is_empty() {
            return;
        }

        info!(count = pids.len(), "sending SIGTERM to tracked children");
        for &pid in &pids {
            // Safety: sending signal to a known PID.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }

        // Wait for escalation period, then SIGKILL any survivors.
        sleep(SIGKILL_ESCALATION).await;

        let remaining: Vec<u32> = {
            let children = self.inner.children.lock().unwrap_or_else(|e| e.into_inner());
            children.clone()
        };

        for &pid in &remaining {
            warn!(pid, "escalating to SIGKILL");
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
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
                tokio::select! {
                    _ = sigint.recv() => {}
                    _ = sigterm.recv() => {}
                }

                match controller.phase() {
                    ShutdownPhase::Running => {
                        controller.request_graceful();
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
                controller.on_signal();

                if controller.phase() == ShutdownPhase::Force {
                    std::process::exit(130);
                }
            }
        });
    }
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
}
