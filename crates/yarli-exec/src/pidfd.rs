//! pidfd-based process handle for race-free signal delivery.
//!
//! On Linux 5.3+, `pidfd_open(2)` returns a file descriptor tied to a
//! specific process. Sending signals through the pidfd avoids PID-recycling
//! races that plague `kill(2)`.
//!
//! [`ProcessHandle`] tries pidfd first and falls back to raw PID when the
//! syscall is unavailable (older kernels, non-Linux).

#[cfg(unix)]
mod inner {
    use std::os::unix::io::RawFd;

    /// A file descriptor obtained from `pidfd_open(2)`, uniquely identifying a process.
    pub struct PidFd {
        fd: RawFd,
    }

    impl PidFd {
        /// Open a pidfd for the given PID.
        ///
        /// Returns `Err` if the syscall is unavailable or the PID is invalid.
        #[cfg(target_os = "linux")]
        pub fn open(pid: u32) -> std::io::Result<Self> {
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::c_int, 0) };
            if fd < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(Self { fd: fd as RawFd })
            }
        }

        #[cfg(not(target_os = "linux"))]
        pub fn open(_pid: u32) -> std::io::Result<Self> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "pidfd_open is only available on Linux 5.3+",
            ))
        }

        /// Send a signal to the process via `pidfd_send_signal(2)`.
        #[cfg(target_os = "linux")]
        pub fn send_signal(&self, sig: i32) -> std::io::Result<()> {
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    self.fd,
                    sig,
                    std::ptr::null::<libc::siginfo_t>(),
                    0u32,
                )
            };
            if rc == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        }

        #[cfg(not(target_os = "linux"))]
        pub fn send_signal(&self, _sig: i32) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "pidfd_send_signal is only available on Linux 5.3+",
            ))
        }

        /// Return the raw file descriptor.
        pub fn as_raw_fd(&self) -> RawFd {
            self.fd
        }
    }

    impl Drop for PidFd {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }

    /// Process handle that uses pidfd when available, falling back to raw PID.
    pub enum ProcessHandle {
        /// Race-free handle via pidfd.
        PidFd(PidFd),
        /// Fallback: raw PID (subject to PID recycling races).
        RawPid(u32),
    }

    impl ProcessHandle {
        /// Acquire a handle for the given PID. Tries pidfd first, falls back to raw PID.
        pub fn acquire(pid: u32) -> Self {
            match PidFd::open(pid) {
                Ok(fd) => ProcessHandle::PidFd(fd),
                Err(_) => ProcessHandle::RawPid(pid),
            }
        }

        /// Send a signal to the process.
        pub fn send_signal(&self, sig: i32) -> std::io::Result<()> {
            match self {
                ProcessHandle::PidFd(fd) => fd.send_signal(sig),
                ProcessHandle::RawPid(pid) => {
                    let rc = unsafe { libc::kill(*pid as i32, sig) };
                    if rc == 0 {
                        Ok(())
                    } else {
                        let err = std::io::Error::last_os_error();
                        // ESRCH means the process already exited — treat as success.
                        if matches!(err.raw_os_error(), Some(code) if code == libc::ESRCH) {
                            Ok(())
                        } else {
                            Err(err)
                        }
                    }
                }
            }
        }

        /// Returns `true` if this handle uses a pidfd (race-free).
        pub fn is_pidfd(&self) -> bool {
            matches!(self, ProcessHandle::PidFd(_))
        }
    }

    /// Send a signal to an entire process group via negative PID.
    ///
    /// This is separate from `ProcessHandle` because pidfd_send_signal doesn't
    /// support process groups — we always use `kill(-pgid, sig)` for group signals.
    pub fn signal_process_group(pgid: i32, signal: i32) -> std::io::Result<()> {
        let rc = unsafe { libc::kill(-pgid, signal) };
        if rc == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if matches!(err.raw_os_error(), Some(code) if code == libc::ESRCH) {
            return Ok(());
        }
        Err(err)
    }
}

// On non-unix platforms, provide stub types so the rest of the crate compiles.
#[cfg(not(unix))]
mod inner {
    /// Stub process handle for non-unix platforms.
    pub enum ProcessHandle {}

    impl ProcessHandle {
        pub fn acquire(_pid: u32) -> Option<Self> {
            None
        }

        pub fn send_signal(&self, _sig: i32) -> std::io::Result<()> {
            match *self {}
        }

        pub fn is_pidfd(&self) -> bool {
            match *self {}
        }
    }

    pub fn signal_process_group(_pgid: i32, _signal: i32) -> std::io::Result<()> {
        Ok(())
    }
}

pub use inner::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn pidfd_open_current_process() {
        let pid = std::process::id();
        let fd = PidFd::open(pid).expect("should open pidfd for own process");
        assert!(fd.as_raw_fd() >= 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pidfd_send_signal_zero() {
        let pid = std::process::id();
        let fd = PidFd::open(pid).expect("should open pidfd");
        // Signal 0 is a no-op probe — just checks if the process exists.
        fd.send_signal(0).expect("signal 0 to self should succeed");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pidfd_open_invalid_pid() {
        // PID 0 refers to the calling process in kill(), but pidfd_open
        // does not accept it. We try an unlikely high PID.
        let result = PidFd::open(u32::MAX);
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn process_handle_acquire_returns_some_variant() {
        let pid = std::process::id();
        let handle = ProcessHandle::acquire(pid);
        // On Linux 5.3+ this should be PidFd; on older kernels RawPid.
        // Either way, sending signal 0 should work.
        handle.send_signal(0).expect("signal 0 to self should work");
    }

    #[cfg(unix)]
    #[test]
    fn process_handle_send_signal_zero() {
        let pid = std::process::id();
        let handle = ProcessHandle::acquire(pid);
        handle
            .send_signal(0)
            .expect("signal probe via handle should succeed");
    }
}
