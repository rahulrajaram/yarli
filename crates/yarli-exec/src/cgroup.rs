//! cgroup v2 sandbox for process resource isolation.
//!
//! When available, creates a cgroup under `/sys/fs/cgroup/yarli/<run_id>/<task_id>/`
//! and writes resource limits (memory.max, cpu.max, pids.max) before adding the
//! child process. The sandbox is cleaned up on drop.
//!
//! Falls back gracefully when cgroup v2 is not mounted or the caller lacks write
//! access to the cgroup hierarchy.

use std::io;
use std::path::{Path, PathBuf};

use crate::yarli_exec::runner::ResourceLimits;

/// Check whether cgroup v2 is available on this system.
///
/// Returns `true` if `/sys/fs/cgroup/cgroup.controllers` exists, which is the
/// canonical indicator of a cgroup v2 unified hierarchy.
pub fn cgroup_v2_available() -> bool {
    Path::new("/sys/fs/cgroup/cgroup.controllers").exists()
}

/// A cgroup v2 sandbox created for a single task execution.
///
/// Writes resource limits to the cgroup control files and adds the child PID.
/// The cgroup directory is removed on drop (best-effort).
pub struct CgroupSandbox {
    /// Full path to the cgroup directory (e.g. `/sys/fs/cgroup/yarli/<run>/<task>/`).
    pub path: PathBuf,
}

impl CgroupSandbox {
    /// Create the cgroup directory structure.
    pub fn create(base: &Path, run_id: &str, task_id: &str) -> io::Result<Self> {
        let path = base.join("yarli").join(run_id).join(task_id);
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    /// Write resource limits to the cgroup control files.
    ///
    /// Only writes limits that have `Some` values:
    /// - `memory.max` for max_memory_bytes
    /// - `cpu.max` for max_cpu_seconds (converted to quota/period)
    /// - `pids.max` for max_pids
    pub fn write_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
        if let Some(bytes) = limits.max_memory_bytes {
            std::fs::write(self.path.join("memory.max"), bytes.to_string())?;
        }
        if let Some(cpu_secs) = limits.max_cpu_seconds {
            // cpu.max format: "$QUOTA $PERIOD" in microseconds.
            // Set period to 1 second (1_000_000 us), quota = cpu_secs * period.
            let period_us: u64 = 1_000_000;
            let quota_us = cpu_secs.saturating_mul(period_us);
            std::fs::write(self.path.join("cpu.max"), format!("{quota_us} {period_us}"))?;
        }
        if let Some(max_pids) = limits.max_pids {
            std::fs::write(self.path.join("pids.max"), max_pids.to_string())?;
        }
        Ok(())
    }

    /// Add a process to this cgroup by writing its PID to `cgroup.procs`.
    pub fn add_pid(&self, pid: u32) -> io::Result<()> {
        std::fs::write(self.path.join("cgroup.procs"), pid.to_string())
    }
}

impl Drop for CgroupSandbox {
    fn drop(&mut self) {
        // Best-effort cleanup: remove the task-level directory, then try the
        // run-level (will only succeed if empty).
        let _ = std::fs::remove_dir(&self.path);
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::remove_dir(parent);
        }
    }
}

/// Trait for creating cgroup sandboxes. Abstracted for testability.
pub trait CgroupManager: Send + Sync {
    /// Create a sandbox with the given limits. Returns `None` if cgroup v2
    /// is not available or creation fails.
    fn create_sandbox(
        &self,
        run_id: &str,
        task_id: &str,
        limits: &ResourceLimits,
    ) -> Option<CgroupSandbox>;
}

/// Production cgroup manager that writes to `/sys/fs/cgroup/`.
pub struct LocalCgroupManager;

impl CgroupManager for LocalCgroupManager {
    fn create_sandbox(
        &self,
        run_id: &str,
        task_id: &str,
        limits: &ResourceLimits,
    ) -> Option<CgroupSandbox> {
        if !cgroup_v2_available() {
            return None;
        }
        let base = Path::new("/sys/fs/cgroup");
        let sandbox = CgroupSandbox::create(base, run_id, task_id).ok()?;
        sandbox.write_limits(limits).ok()?;
        Some(sandbox)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_v2_available_returns_bool() {
        // Just verify it doesn't panic. The result depends on the host system.
        let _available = cgroup_v2_available();
    }

    #[test]
    fn cgroup_sandbox_write_limits_in_tempdir() {
        let tmp = tempfile::tempdir().unwrap();
        let sandbox = CgroupSandbox::create(tmp.path(), "run-1", "task-1").unwrap();
        assert!(sandbox.path.exists());

        let limits = ResourceLimits {
            max_memory_bytes: Some(1_073_741_824), // 1 GiB
            max_cpu_seconds: Some(60),
            max_open_files: None,
            max_pids: Some(100),
        };
        sandbox.write_limits(&limits).unwrap();

        let mem = std::fs::read_to_string(sandbox.path.join("memory.max")).unwrap();
        assert_eq!(mem, "1073741824");

        let cpu = std::fs::read_to_string(sandbox.path.join("cpu.max")).unwrap();
        assert_eq!(cpu, "60000000 1000000");

        let pids = std::fs::read_to_string(sandbox.path.join("pids.max")).unwrap();
        assert_eq!(pids, "100");

        // max_open_files has no cgroup equivalent, so no file should be written for it.
        assert!(!sandbox.path.join("nofile.max").exists());
    }

    #[test]
    fn cgroup_sandbox_drop_cleans_up() {
        let tmp = tempfile::tempdir().unwrap();
        let path;
        {
            let sandbox = CgroupSandbox::create(tmp.path(), "run-2", "task-2").unwrap();
            path = sandbox.path.clone();
            assert!(path.exists());
        }
        // After drop, the task-level directory should be removed.
        assert!(!path.exists());
    }

    #[test]
    fn cgroup_sandbox_add_pid_writes_procs() {
        let tmp = tempfile::tempdir().unwrap();
        let sandbox = CgroupSandbox::create(tmp.path(), "run-3", "task-3").unwrap();
        sandbox.add_pid(12345).unwrap();

        let contents = std::fs::read_to_string(sandbox.path.join("cgroup.procs")).unwrap();
        assert_eq!(contents, "12345");
    }
}
