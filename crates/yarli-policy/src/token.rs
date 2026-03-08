//! Approval tokens: scoped, time-bound authorization for REQUIRE_APPROVAL decisions.
//!
//! Tokens are persisted and auditable. Each token has a scope constraining
//! what action, repo, branch, and run/task it covers (Section 13.2).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::yarli_core::domain::{RunId, TaskId};

/// Scope constraints for an approval token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenScope {
    /// Action type this token authorizes (e.g. "git_push", "merge", "branch_delete").
    pub action: String,
    /// Repository path (if scoped to a specific repo). None = any repo.
    pub repo_path: Option<String>,
    /// Branch pattern (if scoped to a specific branch). None = any branch.
    pub branch: Option<String>,
    /// Run ID (if scoped to a specific run). None = any run.
    pub run_id: Option<RunId>,
    /// Task ID (if scoped to a specific task). None = any task.
    pub task_id: Option<TaskId>,
}

impl TokenScope {
    /// Check if this scope covers the given request parameters.
    pub fn covers(
        &self,
        action: &str,
        repo: Option<&str>,
        branch: Option<&str>,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
    ) -> bool {
        if self.action != action {
            return false;
        }
        if let Some(ref scope_repo) = self.repo_path {
            match repo {
                Some(r) if r != scope_repo => return false,
                None => return false,
                _ => {}
            }
        }
        if let Some(ref scope_branch) = self.branch {
            match branch {
                Some(b) if b != scope_branch => return false,
                None => return false,
                _ => {}
            }
        }
        if let Some(scope_run) = self.run_id {
            match run_id {
                Some(r) if r != scope_run => return false,
                None => return false,
                _ => {}
            }
        }
        if let Some(scope_task) = self.task_id {
            match task_id {
                Some(t) if t != scope_task => return false,
                None => return false,
                _ => {}
            }
        }
        true
    }
}

/// A signed approval token authorizing a specific action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalToken {
    /// Unique token identifier.
    pub token_id: Uuid,
    /// Who issued this token.
    pub issuer: String,
    /// Scope constraints.
    pub scope: TokenScope,
    /// When the token was issued.
    pub issued_at: DateTime<Utc>,
    /// When the token expires.
    pub expires_at: DateTime<Utc>,
    /// Whether the token has been consumed (one-time use).
    pub consumed: bool,
    /// Reason for issuing the token.
    pub reason: String,
}

impl ApprovalToken {
    /// Create a new approval token.
    pub fn new(
        issuer: impl Into<String>,
        scope: TokenScope,
        expires_at: DateTime<Utc>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            token_id: Uuid::now_v7(),
            issuer: issuer.into(),
            scope,
            issued_at: Utc::now(),
            expires_at,
            consumed: false,
            reason: reason.into(),
        }
    }

    /// Check if the token is currently valid (not expired, not consumed).
    pub fn is_valid(&self) -> bool {
        !self.consumed && Utc::now() < self.expires_at
    }

    /// Check if the token covers the given action parameters.
    pub fn covers(
        &self,
        action: &str,
        repo: Option<&str>,
        branch: Option<&str>,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
    ) -> bool {
        self.is_valid() && self.scope.covers(action, repo, branch, run_id, task_id)
    }

    /// Mark the token as consumed.
    pub fn consume(&mut self) {
        self.consumed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn future_time() -> DateTime<Utc> {
        Utc::now() + Duration::hours(1)
    }

    fn past_time() -> DateTime<Utc> {
        Utc::now() - Duration::hours(1)
    }

    fn make_scope(action: &str) -> TokenScope {
        TokenScope {
            action: action.to_string(),
            repo_path: None,
            branch: None,
            run_id: None,
            task_id: None,
        }
    }

    #[test]
    fn token_is_valid_when_not_expired_and_not_consumed() {
        let token = ApprovalToken::new("admin", make_scope("git_push"), future_time(), "testing");
        assert!(token.is_valid());
    }

    #[test]
    fn token_invalid_when_expired() {
        let token = ApprovalToken::new("admin", make_scope("git_push"), past_time(), "testing");
        assert!(!token.is_valid());
    }

    #[test]
    fn token_invalid_when_consumed() {
        let mut token =
            ApprovalToken::new("admin", make_scope("git_push"), future_time(), "testing");
        token.consume();
        assert!(!token.is_valid());
    }

    #[test]
    fn scope_covers_matching_action() {
        let scope = make_scope("git_push");
        assert!(scope.covers("git_push", None, None, None, None));
    }

    #[test]
    fn scope_rejects_different_action() {
        let scope = make_scope("git_push");
        assert!(!scope.covers("merge", None, None, None, None));
    }

    #[test]
    fn scope_with_repo_constraint() {
        let scope = TokenScope {
            action: "git_push".to_string(),
            repo_path: Some("/repo/a".to_string()),
            branch: None,
            run_id: None,
            task_id: None,
        };
        assert!(scope.covers("git_push", Some("/repo/a"), None, None, None));
        assert!(!scope.covers("git_push", Some("/repo/b"), None, None, None));
        assert!(!scope.covers("git_push", None, None, None, None));
    }

    #[test]
    fn scope_with_branch_constraint() {
        let scope = TokenScope {
            action: "merge".to_string(),
            repo_path: None,
            branch: Some("main".to_string()),
            run_id: None,
            task_id: None,
        };
        assert!(scope.covers("merge", None, Some("main"), None, None));
        assert!(!scope.covers("merge", None, Some("develop"), None, None));
    }

    #[test]
    fn scope_with_run_id_constraint() {
        let run_id = Uuid::new_v4();
        let scope = TokenScope {
            action: "git_push".to_string(),
            repo_path: None,
            branch: None,
            run_id: Some(run_id),
            task_id: None,
        };
        assert!(scope.covers("git_push", None, None, Some(run_id), None));
        assert!(!scope.covers("git_push", None, None, Some(Uuid::new_v4()), None));
        assert!(!scope.covers("git_push", None, None, None, None));
    }

    #[test]
    fn scope_with_task_id_constraint() {
        let task_id = Uuid::new_v4();
        let scope = TokenScope {
            action: "git_push".to_string(),
            repo_path: None,
            branch: None,
            run_id: None,
            task_id: Some(task_id),
        };
        assert!(scope.covers("git_push", None, None, None, Some(task_id)));
        assert!(!scope.covers("git_push", None, None, None, Some(Uuid::new_v4())));
    }

    #[test]
    fn token_covers_checks_validity_and_scope() {
        let mut token = ApprovalToken::new("admin", make_scope("git_push"), future_time(), "test");
        assert!(token.covers("git_push", None, None, None, None));
        assert!(!token.covers("merge", None, None, None, None));
        token.consume();
        assert!(!token.covers("git_push", None, None, None, None));
    }

    #[test]
    fn token_serializes_roundtrip() {
        let token = ApprovalToken::new(
            "admin",
            make_scope("merge"),
            future_time(),
            "approved by lead",
        );
        let json = serde_json::to_string(&token).unwrap();
        let decoded: ApprovalToken = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.token_id, token.token_id);
        assert_eq!(decoded.issuer, "admin");
        assert_eq!(decoded.scope.action, "merge");
        assert_eq!(decoded.reason, "approved by lead");
    }

    #[test]
    fn scope_all_constraints_combined() {
        let run_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let scope = TokenScope {
            action: "merge".to_string(),
            repo_path: Some("/repo/x".to_string()),
            branch: Some("main".to_string()),
            run_id: Some(run_id),
            task_id: Some(task_id),
        };
        assert!(scope.covers(
            "merge",
            Some("/repo/x"),
            Some("main"),
            Some(run_id),
            Some(task_id)
        ));
        // Fail on any mismatch
        assert!(!scope.covers(
            "merge",
            Some("/repo/y"),
            Some("main"),
            Some(run_id),
            Some(task_id)
        ));
        assert!(!scope.covers(
            "merge",
            Some("/repo/x"),
            Some("dev"),
            Some(run_id),
            Some(task_id)
        ));
    }
}
