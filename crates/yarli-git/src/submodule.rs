//! Submodule policy enforcement (Section 12.4).
//!
//! Provides detection of uninitialized/dirty submodules and policy-based
//! validation of submodule SHA changes across merges.

use yarli_core::entities::worktree_binding::SubmoduleMode;

use crate::constants::*;
use crate::error::GitError;

/// A single entry from `git submodule status` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmoduleEntry {
    /// Status prefix: ' ' = ok, '-' = uninit, '+' = modified, 'U' = conflict.
    pub status: SubmoduleStatus,
    /// Current SHA of the submodule.
    pub sha: String,
    /// Path of the submodule relative to repo root.
    pub path: String,
    /// Optional branch/descriptor in parentheses (may be empty).
    pub descriptor: Option<String>,
}

/// Status of a single submodule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmoduleStatus {
    /// Submodule is at the recorded SHA (space prefix or no prefix).
    Current,
    /// Submodule is not initialized (dash prefix).
    Uninitialized,
    /// Submodule has been modified / checked out to different SHA (plus prefix).
    Modified,
    /// Submodule has a merge conflict (U prefix).
    Conflict,
}

/// Parse `git submodule status` output into structured entries.
///
/// Format per line: `<prefix><sha> <path> (<descriptor>)`
/// Prefix is one of: ' ', '-', '+', 'U'
pub fn parse_submodule_status(stdout: &str) -> Vec<SubmoduleEntry> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            // The line starts with an optional status char, then 40-char SHA, then space, then path.
            // Examples:
            //  abc123...  path/to/sub (v1.0)
            // -abc123...  path/to/sub
            // +abc123...  path/to/sub (v1.0-dirty)
            // Uabc123...  path/to/sub
            let trimmed = line.trim_start();
            if trimmed.is_empty() {
                return None;
            }

            let first = trimmed.chars().next()?;
            let (status, rest) = match first {
                c if c == SUBMODULE_UNINIT_PREFIX => {
                    (SubmoduleStatus::Uninitialized, &trimmed[1..])
                }
                c if c == SUBMODULE_MODIFIED_PREFIX => (SubmoduleStatus::Modified, &trimmed[1..]),
                c if c == SUBMODULE_CONFLICT_PREFIX => (SubmoduleStatus::Conflict, &trimmed[1..]),
                c if c.is_ascii_hexdigit() => (SubmoduleStatus::Current, trimmed),
                ' ' => (SubmoduleStatus::Current, &trimmed[1..]),
                _ => return None,
            };

            // Extract SHA (up to first space).
            let sha_end = rest.find(' ')?;
            let sha = rest[..sha_end].to_string();
            let after_sha = rest[sha_end..].trim_start();

            // Extract path (up to optional parenthesized descriptor).
            let (path, descriptor) = if let Some(paren_start) = after_sha.find(" (") {
                let path = after_sha[..paren_start].to_string();
                let desc = after_sha[paren_start + 2..]
                    .trim_end_matches(')')
                    .to_string();
                (path, Some(desc))
            } else {
                (after_sha.to_string(), None)
            };

            Some(SubmoduleEntry {
                status,
                sha,
                path,
                descriptor,
            })
        })
        .collect()
}

/// Find submodules that are uninitialized.
pub fn find_uninitialized(entries: &[SubmoduleEntry]) -> Vec<&SubmoduleEntry> {
    entries
        .iter()
        .filter(|e| e.status == SubmoduleStatus::Uninitialized)
        .collect()
}

/// Find submodules that are dirty (modified or conflicted).
pub fn find_dirty(entries: &[SubmoduleEntry]) -> Vec<&SubmoduleEntry> {
    entries
        .iter()
        .filter(|e| e.status == SubmoduleStatus::Modified || e.status == SubmoduleStatus::Conflict)
        .collect()
}

/// Check submodule policy between before/after states.
///
/// Compares submodule SHAs before and after an operation and validates
/// against the configured policy mode.
pub fn check_policy(
    mode: SubmoduleMode,
    before: &[SubmoduleEntry],
    after: &[SubmoduleEntry],
) -> Result<(), GitError> {
    match mode {
        SubmoduleMode::AllowAny => Ok(()),
        SubmoduleMode::Locked => check_locked_policy(before, after),
        SubmoduleMode::AllowFastForward => {
            // AllowFastForward: SHAs may change but we can't verify FF without
            // git ancestry checks. At the submodule status level we just check
            // that no new uninitialized or conflicted submodules appeared.
            // The actual FF verification requires per-submodule `git merge-base --is-ancestor`.
            check_no_regressions(before, after)
        }
    }
}

/// Locked mode: no submodule SHAs may change.
fn check_locked_policy(
    before: &[SubmoduleEntry],
    after: &[SubmoduleEntry],
) -> Result<(), GitError> {
    // Build a map of path -> sha from before.
    let before_map: std::collections::HashMap<&str, &str> = before
        .iter()
        .map(|e| (e.path.as_str(), e.sha.as_str()))
        .collect();

    for entry in after {
        if let Some(&before_sha) = before_map.get(entry.path.as_str()) {
            if before_sha != entry.sha {
                return Err(GitError::SubmodulePolicyViolation {
                    path: entry.path.clone(),
                    reason: format!(
                        "SHA changed from {} to {} (Locked mode forbids changes)",
                        &before_sha[..8.min(before_sha.len())],
                        &entry.sha[..8.min(entry.sha.len())],
                    ),
                });
            }
        }
        // New submodules appearing is also a change.
        if !before_map.contains_key(entry.path.as_str()) {
            return Err(GitError::SubmodulePolicyViolation {
                path: entry.path.clone(),
                reason: "new submodule added (Locked mode forbids changes)".into(),
            });
        }
    }

    // Check for removed submodules.
    let after_paths: std::collections::HashSet<&str> =
        after.iter().map(|e| e.path.as_str()).collect();
    for entry in before {
        if !after_paths.contains(entry.path.as_str()) {
            return Err(GitError::SubmodulePolicyViolation {
                path: entry.path.clone(),
                reason: "submodule removed (Locked mode forbids changes)".into(),
            });
        }
    }

    Ok(())
}

/// Check that no submodules regressed to uninitialized or conflict state.
fn check_no_regressions(
    before: &[SubmoduleEntry],
    after: &[SubmoduleEntry],
) -> Result<(), GitError> {
    let before_map: std::collections::HashMap<&str, &SubmoduleEntry> =
        before.iter().map(|e| (e.path.as_str(), e)).collect();

    for entry in after {
        // A submodule that was initialized but is now uninitialized is a regression.
        if entry.status == SubmoduleStatus::Uninitialized {
            if let Some(before_entry) = before_map.get(entry.path.as_str()) {
                if before_entry.status != SubmoduleStatus::Uninitialized {
                    return Err(GitError::SubmodulePolicyViolation {
                        path: entry.path.clone(),
                        reason: "submodule became uninitialized".into(),
                    });
                }
            }
        }
        // Conflicts are always a violation.
        if entry.status == SubmoduleStatus::Conflict {
            return Err(GitError::SubmodulePolicyViolation {
                path: entry.path.clone(),
                reason: "submodule has merge conflict".into(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Parsing tests ────────────────────────────────────────────────

    #[test]
    fn parse_empty_output() {
        let entries = parse_submodule_status("");
        assert!(entries.is_empty());
    }

    #[test]
    fn parse_current_submodule() {
        let output = " abc123def456789012345678901234567890ab vendor/lib (v1.0)\n";
        let entries = parse_submodule_status(output);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, SubmoduleStatus::Current);
        assert_eq!(entries[0].sha, "abc123def456789012345678901234567890ab");
        assert_eq!(entries[0].path, "vendor/lib");
        assert_eq!(entries[0].descriptor.as_deref(), Some("v1.0"));
    }

    #[test]
    fn parse_uninitialized_submodule() {
        let output = "-abc123def456789012345678901234567890ab vendor/uninitialized\n";
        let entries = parse_submodule_status(output);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, SubmoduleStatus::Uninitialized);
        assert_eq!(entries[0].path, "vendor/uninitialized");
    }

    #[test]
    fn parse_modified_submodule() {
        let output = "+abc123def456789012345678901234567890ab vendor/dirty (v1.0-1-gabcdef)\n";
        let entries = parse_submodule_status(output);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, SubmoduleStatus::Modified);
        assert_eq!(entries[0].path, "vendor/dirty");
        assert_eq!(entries[0].descriptor.as_deref(), Some("v1.0-1-gabcdef"));
    }

    #[test]
    fn parse_conflict_submodule() {
        let output = "Uabc123def456789012345678901234567890ab vendor/conflict\n";
        let entries = parse_submodule_status(output);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, SubmoduleStatus::Conflict);
        assert_eq!(entries[0].path, "vendor/conflict");
    }

    #[test]
    fn parse_multiple_submodules() {
        let output = " abc123def456789012345678901234567890ab vendor/a (v1.0)\n\
                       -def456789012345678901234567890abcdef01 vendor/b\n\
                       +789012345678901234567890abcdef0123456789 vendor/c (v2.1-dirty)\n";
        let entries = parse_submodule_status(output);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].status, SubmoduleStatus::Current);
        assert_eq!(entries[1].status, SubmoduleStatus::Uninitialized);
        assert_eq!(entries[2].status, SubmoduleStatus::Modified);
    }

    #[test]
    fn parse_submodule_without_descriptor() {
        let output = " abc123def456789012345678901234567890ab vendor/lib\n";
        let entries = parse_submodule_status(output);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].descriptor.is_none());
    }

    // ── Find helpers ─────────────────────────────────────────────────

    #[test]
    fn find_uninitialized_returns_only_uninit() {
        let entries = vec![
            SubmoduleEntry {
                status: SubmoduleStatus::Current,
                sha: "a".repeat(40),
                path: "vendor/a".into(),
                descriptor: None,
            },
            SubmoduleEntry {
                status: SubmoduleStatus::Uninitialized,
                sha: "b".repeat(40),
                path: "vendor/b".into(),
                descriptor: None,
            },
            SubmoduleEntry {
                status: SubmoduleStatus::Modified,
                sha: "c".repeat(40),
                path: "vendor/c".into(),
                descriptor: None,
            },
        ];
        let uninit = find_uninitialized(&entries);
        assert_eq!(uninit.len(), 1);
        assert_eq!(uninit[0].path, "vendor/b");
    }

    #[test]
    fn find_dirty_returns_modified_and_conflict() {
        let entries = vec![
            SubmoduleEntry {
                status: SubmoduleStatus::Current,
                sha: "a".repeat(40),
                path: "vendor/a".into(),
                descriptor: None,
            },
            SubmoduleEntry {
                status: SubmoduleStatus::Modified,
                sha: "b".repeat(40),
                path: "vendor/b".into(),
                descriptor: None,
            },
            SubmoduleEntry {
                status: SubmoduleStatus::Conflict,
                sha: "c".repeat(40),
                path: "vendor/c".into(),
                descriptor: None,
            },
        ];
        let dirty = find_dirty(&entries);
        assert_eq!(dirty.len(), 2);
    }

    // ── Policy tests ─────────────────────────────────────────────────

    #[test]
    fn locked_policy_allows_no_changes() {
        let before = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let after = before.clone();
        assert!(check_policy(SubmoduleMode::Locked, &before, &after).is_ok());
    }

    #[test]
    fn locked_policy_rejects_sha_change() {
        let before = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let after = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "b".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let result = check_policy(SubmoduleMode::Locked, &before, &after);
        assert!(result.is_err());
        match result.unwrap_err() {
            GitError::SubmodulePolicyViolation { path, reason } => {
                assert_eq!(path, "vendor/lib");
                assert!(reason.contains("Locked mode"));
            }
            other => panic!("expected SubmodulePolicyViolation, got {other:?}"),
        }
    }

    #[test]
    fn locked_policy_rejects_new_submodule() {
        let before = vec![];
        let after = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/new".into(),
            descriptor: None,
        }];
        let result = check_policy(SubmoduleMode::Locked, &before, &after);
        assert!(result.is_err());
        match result.unwrap_err() {
            GitError::SubmodulePolicyViolation { path, reason } => {
                assert_eq!(path, "vendor/new");
                assert!(reason.contains("new submodule"));
            }
            other => panic!("expected SubmodulePolicyViolation, got {other:?}"),
        }
    }

    #[test]
    fn locked_policy_rejects_removed_submodule() {
        let before = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/gone".into(),
            descriptor: None,
        }];
        let after = vec![];
        let result = check_policy(SubmoduleMode::Locked, &before, &after);
        assert!(result.is_err());
        match result.unwrap_err() {
            GitError::SubmodulePolicyViolation { path, reason } => {
                assert_eq!(path, "vendor/gone");
                assert!(reason.contains("removed"));
            }
            other => panic!("expected SubmodulePolicyViolation, got {other:?}"),
        }
    }

    #[test]
    fn allow_any_accepts_all_changes() {
        let before = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let after = vec![SubmoduleEntry {
            status: SubmoduleStatus::Modified,
            sha: "b".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        assert!(check_policy(SubmoduleMode::AllowAny, &before, &after).is_ok());
    }

    #[test]
    fn allow_ff_rejects_conflict() {
        let before = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let after = vec![SubmoduleEntry {
            status: SubmoduleStatus::Conflict,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let result = check_policy(SubmoduleMode::AllowFastForward, &before, &after);
        assert!(result.is_err());
        match result.unwrap_err() {
            GitError::SubmodulePolicyViolation { path, reason } => {
                assert_eq!(path, "vendor/lib");
                assert!(reason.contains("conflict"));
            }
            other => panic!("expected SubmodulePolicyViolation, got {other:?}"),
        }
    }

    #[test]
    fn allow_ff_rejects_regression_to_uninit() {
        let before = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let after = vec![SubmoduleEntry {
            status: SubmoduleStatus::Uninitialized,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let result = check_policy(SubmoduleMode::AllowFastForward, &before, &after);
        assert!(result.is_err());
        match result.unwrap_err() {
            GitError::SubmodulePolicyViolation { path, reason } => {
                assert_eq!(path, "vendor/lib");
                assert!(reason.contains("uninitialized"));
            }
            other => panic!("expected SubmodulePolicyViolation, got {other:?}"),
        }
    }

    #[test]
    fn allow_ff_accepts_sha_change_without_regression() {
        let before = vec![SubmoduleEntry {
            status: SubmoduleStatus::Current,
            sha: "a".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        let after = vec![SubmoduleEntry {
            status: SubmoduleStatus::Modified,
            sha: "b".repeat(40),
            path: "vendor/lib".into(),
            descriptor: None,
        }];
        assert!(check_policy(SubmoduleMode::AllowFastForward, &before, &after).is_ok());
    }
}
