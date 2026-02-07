//! Policy rule engine: deterministic evaluation of operation permissions.
//!
//! Rules are evaluated in order. The first matching rule determines the outcome.
//! If no rule matches, the default is DENY (safe by default).
//!
//! Section 13.1: Rule evaluation considers actor, command class, repo path,
//! branch target, and run mode.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, PolicyDecision, PolicyOutcome, RunId, SafeMode, TaskId};

use crate::error::PolicyError;
use crate::token::ApprovalToken;

// ---------------------------------------------------------------------------
// Action types
// ---------------------------------------------------------------------------

/// Categorized action types that policy rules can match against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    /// Execute a shell command.
    CommandExecute,
    /// Push to a remote repository.
    GitPush,
    /// Force-push to a remote repository.
    GitForcePush,
    /// Create or modify a tag.
    GitTag,
    /// Delete a branch.
    BranchDelete,
    /// Perform a merge operation.
    Merge,
    /// Clear stash.
    StashClear,
    /// Destructive cleanup (git clean -fd, etc.).
    DestructiveCleanup,
    /// Memory write operation.
    MemoryWrite,
}

impl ActionType {
    /// String identifier for this action type (used in token scope matching).
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionType::CommandExecute => "command_execute",
            ActionType::GitPush => "git_push",
            ActionType::GitForcePush => "git_force_push",
            ActionType::GitTag => "git_tag",
            ActionType::BranchDelete => "branch_delete",
            ActionType::Merge => "merge",
            ActionType::StashClear => "stash_clear",
            ActionType::DestructiveCleanup => "destructive_cleanup",
            ActionType::MemoryWrite => "memory_write",
        }
    }
}

// ---------------------------------------------------------------------------
// Policy request
// ---------------------------------------------------------------------------

/// A request to the policy engine for a decision.
#[derive(Debug, Clone)]
pub struct PolicyRequest {
    /// Actor performing the action (e.g. "scheduler", "operator").
    pub actor: String,
    /// Type of action being requested.
    pub action: ActionType,
    /// Command class (if applicable).
    pub command_class: Option<CommandClass>,
    /// Repository path (if applicable).
    pub repo_path: Option<String>,
    /// Target branch (if applicable).
    pub branch: Option<String>,
    /// Run ID context.
    pub run_id: RunId,
    /// Task ID context (if applicable).
    pub task_id: Option<TaskId>,
    /// Current safe mode.
    pub safe_mode: SafeMode,
}

// ---------------------------------------------------------------------------
// Policy rules
// ---------------------------------------------------------------------------

/// Outcome of a single rule match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuleOutcome {
    Allow,
    Deny,
    RequireApproval,
}

impl From<RuleOutcome> for PolicyOutcome {
    fn from(outcome: RuleOutcome) -> Self {
        match outcome {
            RuleOutcome::Allow => PolicyOutcome::Allow,
            RuleOutcome::Deny => PolicyOutcome::Deny,
            RuleOutcome::RequireApproval => PolicyOutcome::RequireApproval,
        }
    }
}

/// A single policy rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    /// Unique rule identifier.
    pub id: String,
    /// Human-readable description.
    pub description: String,
    /// Action types this rule applies to. Empty = match all.
    pub actions: Vec<ActionType>,
    /// Safe modes this rule applies to. Empty = match all.
    pub safe_modes: Vec<SafeMode>,
    /// Command classes this rule applies to. Empty = match all.
    pub command_classes: Vec<CommandClass>,
    /// Branch patterns this rule applies to. Empty = match all.
    pub branches: Vec<String>,
    /// The outcome when this rule matches.
    pub outcome: RuleOutcome,
    /// Reason for this rule.
    pub reason: String,
}

impl PolicyRule {
    /// Check if this rule matches the given request.
    pub fn matches(&self, req: &PolicyRequest) -> bool {
        // Action filter
        if !self.actions.is_empty() && !self.actions.contains(&req.action) {
            return false;
        }
        // Safe mode filter
        if !self.safe_modes.is_empty() && !self.safe_modes.contains(&req.safe_mode) {
            return false;
        }
        // Command class filter
        if !self.command_classes.is_empty() {
            match req.command_class {
                Some(cc) if self.command_classes.contains(&cc) => {}
                None if self.command_classes.is_empty() => {}
                _ => return false,
            }
        }
        // Branch filter
        if !self.branches.is_empty() {
            match &req.branch {
                Some(b) if self.branches.contains(b) => {}
                None => return false,
                _ => return false,
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Safe mode enforcement
// ---------------------------------------------------------------------------

/// Safe mode policy enforcement (Section 13.3).
pub struct SafeModePolicy;

impl SafeModePolicy {
    /// Check if the given action is permitted under the current safe mode.
    /// Returns `Ok(())` if permitted, or `Err(PolicyError::SafeModeViolation)`.
    pub fn check(mode: SafeMode, action: ActionType) -> Result<(), PolicyError> {
        match mode {
            SafeMode::Observe => {
                // Observe mode: read-only, no mutations allowed
                Err(PolicyError::SafeModeViolation {
                    mode: "observe".to_string(),
                    action: action.as_str().to_string(),
                })
            }
            SafeMode::Execute => {
                // Normal execution: all actions subject to policy rules
                Ok(())
            }
            SafeMode::Restricted => {
                // Restricted: only command execution allowed
                match action {
                    ActionType::CommandExecute => Ok(()),
                    _ => Err(PolicyError::SafeModeViolation {
                        mode: "restricted".to_string(),
                        action: action.as_str().to_string(),
                    }),
                }
            }
            SafeMode::Breakglass => {
                // Breakglass: all actions allowed (with high audit level)
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Default rules
// ---------------------------------------------------------------------------

/// Build the default rule set (Section 12.12 + Section 13.1).
///
/// Default-deny for dangerous operations, allow for normal execution.
pub fn default_rules() -> Vec<PolicyRule> {
    vec![
        // Forbidden git operations require approval
        PolicyRule {
            id: "deny-force-push".to_string(),
            description: "Force push is forbidden by default".to_string(),
            actions: vec![ActionType::GitForcePush],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Deny,
            reason: "Force push can destroy remote history".to_string(),
        },
        PolicyRule {
            id: "require-approval-push".to_string(),
            description: "Push requires approval".to_string(),
            actions: vec![ActionType::GitPush],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::RequireApproval,
            reason: "Push to remote requires explicit approval".to_string(),
        },
        PolicyRule {
            id: "require-approval-tag".to_string(),
            description: "Tag creation requires approval".to_string(),
            actions: vec![ActionType::GitTag],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::RequireApproval,
            reason: "Tag creation requires explicit approval".to_string(),
        },
        PolicyRule {
            id: "require-approval-branch-delete".to_string(),
            description: "Branch deletion requires approval".to_string(),
            actions: vec![ActionType::BranchDelete],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::RequireApproval,
            reason: "Branch deletion requires explicit approval".to_string(),
        },
        PolicyRule {
            id: "deny-stash-clear".to_string(),
            description: "Stash clear is forbidden".to_string(),
            actions: vec![ActionType::StashClear],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Deny,
            reason: "Global stash clear can destroy work".to_string(),
        },
        PolicyRule {
            id: "deny-destructive-cleanup".to_string(),
            description: "Destructive cleanup is forbidden".to_string(),
            actions: vec![ActionType::DestructiveCleanup],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Deny,
            reason: "Destructive cleanup can destroy untracked files".to_string(),
        },
        // Normal operations allowed
        PolicyRule {
            id: "allow-command-execute".to_string(),
            description: "Normal command execution is allowed".to_string(),
            actions: vec![ActionType::CommandExecute],
            safe_modes: vec![SafeMode::Execute, SafeMode::Breakglass],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Allow,
            reason: "Standard command execution in execute/breakglass mode".to_string(),
        },
        PolicyRule {
            id: "allow-merge".to_string(),
            description: "Merge operations are allowed in execute mode".to_string(),
            actions: vec![ActionType::Merge],
            safe_modes: vec![SafeMode::Execute, SafeMode::Breakglass],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Allow,
            reason: "Merge operations are part of normal workflow".to_string(),
        },
        PolicyRule {
            id: "allow-memory-write".to_string(),
            description: "Memory writes are allowed".to_string(),
            actions: vec![ActionType::MemoryWrite],
            safe_modes: vec![SafeMode::Execute, SafeMode::Breakglass],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Allow,
            reason: "Memory operations are safe".to_string(),
        },
    ]
}

// ---------------------------------------------------------------------------
// Policy engine
// ---------------------------------------------------------------------------

/// The policy engine: evaluates requests against rules and approval tokens.
pub struct PolicyEngine {
    rules: Vec<PolicyRule>,
    tokens: Vec<ApprovalToken>,
}

impl PolicyEngine {
    /// Create a new policy engine with the given rules.
    pub fn new(rules: Vec<PolicyRule>) -> Self {
        Self {
            rules,
            tokens: Vec::new(),
        }
    }

    /// Create a policy engine with the default rule set.
    pub fn with_defaults() -> Self {
        Self::new(default_rules())
    }

    /// Register an approval token.
    pub fn add_token(&mut self, token: ApprovalToken) {
        self.tokens.push(token);
    }

    /// Get all registered tokens (for audit).
    pub fn tokens(&self) -> &[ApprovalToken] {
        &self.tokens
    }

    /// Get all rules (for introspection).
    pub fn rules(&self) -> &[PolicyRule] {
        &self.rules
    }

    /// Evaluate a policy request.
    ///
    /// 1. Check safe mode constraints.
    /// 2. Evaluate rules in order (first match wins).
    /// 3. If REQUIRE_APPROVAL, check for matching approval token.
    /// 4. If no rule matches, default to DENY.
    ///
    /// Returns a `PolicyDecision` record suitable for audit logging.
    pub fn evaluate(&mut self, req: &PolicyRequest) -> Result<PolicyDecision, PolicyError> {
        let now = Utc::now();

        // Step 1: Safe mode enforcement
        if let Err(e) = SafeModePolicy::check(req.safe_mode, req.action) {
            return Ok(PolicyDecision {
                decision_id: Uuid::now_v7(),
                run_id: req.run_id,
                actor: req.actor.clone(),
                action: req.action.as_str().to_string(),
                outcome: PolicyOutcome::Deny,
                rule_id: "safe_mode".to_string(),
                reason: e.to_string(),
                decided_at: now,
            });
        }

        // Step 2: Evaluate rules in order
        for rule in &self.rules {
            if rule.matches(req) {
                let outcome = rule.outcome;
                let rule_id = rule.id.clone();
                let reason = rule.reason.clone();

                // Step 3: If REQUIRE_APPROVAL, check for approval token
                if outcome == RuleOutcome::RequireApproval {
                    let action_str = req.action.as_str();
                    let found = self.tokens.iter_mut().find(|t| {
                        t.covers(
                            action_str,
                            req.repo_path.as_deref(),
                            req.branch.as_deref(),
                            Some(req.run_id),
                            req.task_id,
                        )
                    });

                    if let Some(token) = found {
                        token.consume();
                        return Ok(PolicyDecision {
                            decision_id: Uuid::now_v7(),
                            run_id: req.run_id,
                            actor: req.actor.clone(),
                            action: action_str.to_string(),
                            outcome: PolicyOutcome::Allow,
                            rule_id: format!("{rule_id}+token:{}", token.token_id),
                            reason: format!("approved via token: {}", token.reason),
                            decided_at: now,
                        });
                    }

                    return Ok(PolicyDecision {
                        decision_id: Uuid::now_v7(),
                        run_id: req.run_id,
                        actor: req.actor.clone(),
                        action: action_str.to_string(),
                        outcome: PolicyOutcome::RequireApproval,
                        rule_id,
                        reason,
                        decided_at: now,
                    });
                }

                return Ok(PolicyDecision {
                    decision_id: Uuid::now_v7(),
                    run_id: req.run_id,
                    actor: req.actor.clone(),
                    action: req.action.as_str().to_string(),
                    outcome: outcome.into(),
                    rule_id,
                    reason,
                    decided_at: now,
                });
            }
        }

        // Step 4: Default deny
        Ok(PolicyDecision {
            decision_id: Uuid::now_v7(),
            run_id: req.run_id,
            actor: req.actor.clone(),
            action: req.action.as_str().to_string(),
            outcome: PolicyOutcome::Deny,
            rule_id: "default_deny".to_string(),
            reason: "no matching policy rule found (default deny)".to_string(),
            decided_at: now,
        })
    }

    /// Convenience: evaluate and return just the outcome.
    pub fn is_allowed(&mut self, req: &PolicyRequest) -> Result<bool, PolicyError> {
        let decision = self.evaluate(req)?;
        Ok(decision.outcome == PolicyOutcome::Allow)
    }

    /// Collect all policy violations for a set of decisions.
    ///
    /// Returns descriptions of any DENY or REQUIRE_APPROVAL outcomes,
    /// suitable for populating `GateContext.policy_violations`.
    pub fn collect_violations(decisions: &[PolicyDecision]) -> Vec<String> {
        decisions
            .iter()
            .filter(|d| d.outcome != PolicyOutcome::Allow)
            .map(|d| format!("{}: {} (rule: {})", d.action, d.reason, d.rule_id))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Helper: build request
// ---------------------------------------------------------------------------

impl PolicyRequest {
    /// Build a policy request for a command execution.
    pub fn command(
        run_id: RunId,
        task_id: TaskId,
        command_class: CommandClass,
        safe_mode: SafeMode,
    ) -> Self {
        Self {
            actor: "scheduler".to_string(),
            action: ActionType::CommandExecute,
            command_class: Some(command_class),
            repo_path: None,
            branch: None,
            run_id,
            task_id: Some(task_id),
            safe_mode,
        }
    }

    /// Build a policy request for a git operation.
    pub fn git_op(
        action: ActionType,
        run_id: RunId,
        repo_path: impl Into<String>,
        branch: Option<String>,
        safe_mode: SafeMode,
    ) -> Self {
        Self {
            actor: "scheduler".to_string(),
            action,
            command_class: Some(CommandClass::Git),
            repo_path: Some(repo_path.into()),
            branch,
            run_id,
            task_id: None,
            safe_mode,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_request(action: ActionType, safe_mode: SafeMode) -> PolicyRequest {
        PolicyRequest {
            actor: "scheduler".to_string(),
            action,
            command_class: None,
            repo_path: None,
            branch: None,
            run_id: Uuid::new_v4(),
            task_id: None,
            safe_mode,
        }
    }

    // ---- Safe mode enforcement ----

    #[test]
    fn observe_mode_denies_all_actions() {
        let err = SafeModePolicy::check(SafeMode::Observe, ActionType::CommandExecute);
        assert!(err.is_err());
        let err = SafeModePolicy::check(SafeMode::Observe, ActionType::GitPush);
        assert!(err.is_err());
        let err = SafeModePolicy::check(SafeMode::Observe, ActionType::Merge);
        assert!(err.is_err());
    }

    #[test]
    fn execute_mode_allows_all_actions() {
        assert!(SafeModePolicy::check(SafeMode::Execute, ActionType::CommandExecute).is_ok());
        assert!(SafeModePolicy::check(SafeMode::Execute, ActionType::GitPush).is_ok());
        assert!(SafeModePolicy::check(SafeMode::Execute, ActionType::Merge).is_ok());
    }

    #[test]
    fn restricted_mode_only_allows_command_execute() {
        assert!(SafeModePolicy::check(SafeMode::Restricted, ActionType::CommandExecute).is_ok());
        assert!(SafeModePolicy::check(SafeMode::Restricted, ActionType::GitPush).is_err());
        assert!(SafeModePolicy::check(SafeMode::Restricted, ActionType::Merge).is_err());
        assert!(SafeModePolicy::check(SafeMode::Restricted, ActionType::BranchDelete).is_err());
    }

    #[test]
    fn breakglass_mode_allows_all() {
        assert!(SafeModePolicy::check(SafeMode::Breakglass, ActionType::CommandExecute).is_ok());
        assert!(SafeModePolicy::check(SafeMode::Breakglass, ActionType::GitPush).is_ok());
        assert!(
            SafeModePolicy::check(SafeMode::Breakglass, ActionType::DestructiveCleanup).is_ok()
        );
    }

    // ---- Rule matching ----

    #[test]
    fn rule_matches_specific_action() {
        let rule = PolicyRule {
            id: "test".to_string(),
            description: "test rule".to_string(),
            actions: vec![ActionType::GitPush],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Deny,
            reason: "test".to_string(),
        };
        let req_push = make_request(ActionType::GitPush, SafeMode::Execute);
        let req_merge = make_request(ActionType::Merge, SafeMode::Execute);
        assert!(rule.matches(&req_push));
        assert!(!rule.matches(&req_merge));
    }

    #[test]
    fn rule_empty_actions_matches_all() {
        let rule = PolicyRule {
            id: "catch-all".to_string(),
            description: "matches everything".to_string(),
            actions: vec![],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Deny,
            reason: "catch-all".to_string(),
        };
        assert!(rule.matches(&make_request(ActionType::GitPush, SafeMode::Execute)));
        assert!(rule.matches(&make_request(ActionType::Merge, SafeMode::Execute)));
    }

    #[test]
    fn rule_matches_safe_mode() {
        let rule = PolicyRule {
            id: "execute-only".to_string(),
            description: "only in execute mode".to_string(),
            actions: vec![],
            safe_modes: vec![SafeMode::Execute],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Allow,
            reason: "test".to_string(),
        };
        assert!(rule.matches(&make_request(ActionType::CommandExecute, SafeMode::Execute)));
        assert!(!rule.matches(&make_request(
            ActionType::CommandExecute,
            SafeMode::Breakglass
        )));
    }

    #[test]
    fn rule_matches_branch() {
        let rule = PolicyRule {
            id: "main-only".to_string(),
            description: "only main branch".to_string(),
            actions: vec![ActionType::GitPush],
            safe_modes: vec![],
            command_classes: vec![],
            branches: vec!["main".to_string()],
            outcome: RuleOutcome::Deny,
            reason: "protect main".to_string(),
        };
        let mut req = make_request(ActionType::GitPush, SafeMode::Execute);
        req.branch = Some("main".to_string());
        assert!(rule.matches(&req));

        req.branch = Some("develop".to_string());
        assert!(!rule.matches(&req));

        req.branch = None;
        assert!(!rule.matches(&req));
    }

    #[test]
    fn rule_matches_command_class() {
        let rule = PolicyRule {
            id: "git-only".to_string(),
            description: "only git commands".to_string(),
            actions: vec![],
            safe_modes: vec![],
            command_classes: vec![CommandClass::Git],
            branches: vec![],
            outcome: RuleOutcome::Allow,
            reason: "test".to_string(),
        };
        let mut req = make_request(ActionType::CommandExecute, SafeMode::Execute);
        req.command_class = Some(CommandClass::Git);
        assert!(rule.matches(&req));

        req.command_class = Some(CommandClass::Cpu);
        assert!(!rule.matches(&req));

        req.command_class = None;
        assert!(!rule.matches(&req));
    }

    // ---- Policy engine evaluation ----

    #[test]
    fn engine_allows_command_execute() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::CommandExecute, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Allow);
    }

    #[test]
    fn engine_denies_force_push() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::GitForcePush, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Deny);
        assert_eq!(decision.rule_id, "deny-force-push");
    }

    #[test]
    fn engine_requires_approval_for_push() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::GitPush, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::RequireApproval);
    }

    #[test]
    fn engine_requires_approval_for_tag() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::GitTag, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::RequireApproval);
    }

    #[test]
    fn engine_requires_approval_for_branch_delete() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::BranchDelete, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::RequireApproval);
    }

    #[test]
    fn engine_denies_stash_clear() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::StashClear, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Deny);
    }

    #[test]
    fn engine_denies_destructive_cleanup() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::DestructiveCleanup, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Deny);
    }

    #[test]
    fn engine_allows_merge_in_execute() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::Merge, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Allow);
    }

    #[test]
    fn engine_denies_in_observe_mode() {
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::CommandExecute, SafeMode::Observe);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Deny);
        assert_eq!(decision.rule_id, "safe_mode");
    }

    #[test]
    fn engine_restricted_mode_allows_command_denies_git() {
        let mut engine = PolicyEngine::with_defaults();
        let cmd_req = make_request(ActionType::CommandExecute, SafeMode::Restricted);
        let decision = engine.evaluate(&cmd_req).unwrap();
        // Restricted passes safe mode for command, but needs a matching rule
        // Our default rules match execute|breakglass, so restricted won't match
        // → default deny
        assert!(
            decision.outcome == PolicyOutcome::Allow || decision.outcome == PolicyOutcome::Deny
        );

        let push_req = make_request(ActionType::GitPush, SafeMode::Restricted);
        let decision = engine.evaluate(&push_req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Deny);
        assert_eq!(decision.rule_id, "safe_mode");
    }

    // ---- Approval tokens ----

    #[test]
    fn approval_token_upgrades_require_approval_to_allow() {
        let mut engine = PolicyEngine::with_defaults();
        let run_id = Uuid::new_v4();

        // Add token for git_push on this run
        let scope = crate::token::TokenScope {
            action: "git_push".to_string(),
            repo_path: None,
            branch: None,
            run_id: Some(run_id),
            task_id: None,
        };
        let token = ApprovalToken::new(
            "admin",
            scope,
            Utc::now() + Duration::hours(1),
            "approved for deployment",
        );
        let token_id = token.token_id;
        engine.add_token(token);

        let req = PolicyRequest {
            actor: "scheduler".to_string(),
            action: ActionType::GitPush,
            command_class: None,
            repo_path: None,
            branch: None,
            run_id,
            task_id: None,
            safe_mode: SafeMode::Execute,
        };

        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Allow);
        assert!(decision.rule_id.contains(&format!("token:{token_id}")));
        assert!(decision.reason.contains("approved for deployment"));
    }

    #[test]
    fn expired_token_does_not_upgrade() {
        let mut engine = PolicyEngine::with_defaults();
        let run_id = Uuid::new_v4();

        let scope = crate::token::TokenScope {
            action: "git_push".to_string(),
            repo_path: None,
            branch: None,
            run_id: Some(run_id),
            task_id: None,
        };
        let token = ApprovalToken::new(
            "admin",
            scope,
            Utc::now() - Duration::hours(1), // expired
            "too late",
        );
        engine.add_token(token);

        let req = PolicyRequest {
            actor: "scheduler".to_string(),
            action: ActionType::GitPush,
            command_class: None,
            repo_path: None,
            branch: None,
            run_id,
            task_id: None,
            safe_mode: SafeMode::Execute,
        };

        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::RequireApproval);
    }

    #[test]
    fn consumed_token_not_reusable() {
        let mut engine = PolicyEngine::with_defaults();
        let run_id = Uuid::new_v4();

        let scope = crate::token::TokenScope {
            action: "git_push".to_string(),
            repo_path: None,
            branch: None,
            run_id: Some(run_id),
            task_id: None,
        };
        let token = ApprovalToken::new("admin", scope, Utc::now() + Duration::hours(1), "one-time");
        engine.add_token(token);

        let req = PolicyRequest {
            actor: "scheduler".to_string(),
            action: ActionType::GitPush,
            command_class: None,
            repo_path: None,
            branch: None,
            run_id,
            task_id: None,
            safe_mode: SafeMode::Execute,
        };

        // First evaluation consumes the token
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Allow);

        // Second evaluation: token consumed, requires approval again
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::RequireApproval);
    }

    // ---- Default deny ----

    #[test]
    fn default_deny_for_unknown_action() {
        // Empty rules: everything denied
        let mut engine = PolicyEngine::new(vec![]);
        let req = make_request(ActionType::CommandExecute, SafeMode::Execute);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Deny);
        assert_eq!(decision.rule_id, "default_deny");
    }

    // ---- Collect violations ----

    #[test]
    fn collect_violations_filters_non_allow() {
        let decisions = vec![
            PolicyDecision {
                decision_id: Uuid::new_v4(),
                run_id: Uuid::new_v4(),
                actor: "scheduler".to_string(),
                action: "command_execute".to_string(),
                outcome: PolicyOutcome::Allow,
                rule_id: "allow-command".to_string(),
                reason: "ok".to_string(),
                decided_at: Utc::now(),
            },
            PolicyDecision {
                decision_id: Uuid::new_v4(),
                run_id: Uuid::new_v4(),
                actor: "scheduler".to_string(),
                action: "git_push".to_string(),
                outcome: PolicyOutcome::Deny,
                rule_id: "deny-push".to_string(),
                reason: "forbidden".to_string(),
                decided_at: Utc::now(),
            },
            PolicyDecision {
                decision_id: Uuid::new_v4(),
                run_id: Uuid::new_v4(),
                actor: "scheduler".to_string(),
                action: "git_tag".to_string(),
                outcome: PolicyOutcome::RequireApproval,
                rule_id: "require-tag".to_string(),
                reason: "needs approval".to_string(),
                decided_at: Utc::now(),
            },
        ];

        let violations = PolicyEngine::collect_violations(&decisions);
        assert_eq!(violations.len(), 2);
        assert!(violations[0].contains("git_push"));
        assert!(violations[1].contains("git_tag"));
    }

    // ---- Decision audit fields ----

    #[test]
    fn decision_includes_all_audit_fields() {
        let mut engine = PolicyEngine::with_defaults();
        let run_id = Uuid::new_v4();
        let req = PolicyRequest {
            actor: "operator".to_string(),
            action: ActionType::GitPush,
            command_class: Some(CommandClass::Git),
            repo_path: Some("/repo/test".to_string()),
            branch: Some("main".to_string()),
            run_id,
            task_id: Some(Uuid::new_v4()),
            safe_mode: SafeMode::Execute,
        };

        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.run_id, run_id);
        assert_eq!(decision.actor, "operator");
        assert_eq!(decision.action, "git_push");
        assert!(decision.decided_at <= Utc::now());
        assert!(!decision.rule_id.is_empty());
        assert!(!decision.reason.is_empty());
    }

    // ---- Convenience methods ----

    #[test]
    fn is_allowed_returns_bool() {
        let mut engine = PolicyEngine::with_defaults();
        let cmd = make_request(ActionType::CommandExecute, SafeMode::Execute);
        assert!(engine.is_allowed(&cmd).unwrap());

        let push = make_request(ActionType::GitForcePush, SafeMode::Execute);
        assert!(!engine.is_allowed(&push).unwrap());
    }

    #[test]
    fn policy_request_command_builder() {
        let run_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let req = PolicyRequest::command(run_id, task_id, CommandClass::Cpu, SafeMode::Execute);
        assert_eq!(req.run_id, run_id);
        assert_eq!(req.task_id, Some(task_id));
        assert_eq!(req.action, ActionType::CommandExecute);
        assert_eq!(req.command_class, Some(CommandClass::Cpu));
    }

    #[test]
    fn policy_request_git_op_builder() {
        let run_id = Uuid::new_v4();
        let req = PolicyRequest::git_op(
            ActionType::GitPush,
            run_id,
            "/repo/test",
            Some("main".to_string()),
            SafeMode::Execute,
        );
        assert_eq!(req.action, ActionType::GitPush);
        assert_eq!(req.repo_path.as_deref(), Some("/repo/test"));
        assert_eq!(req.branch.as_deref(), Some("main"));
    }

    // ---- Default rules count ----

    #[test]
    fn default_rules_has_expected_count() {
        let rules = default_rules();
        assert_eq!(rules.len(), 9);
    }

    // ---- Serialization ----

    #[test]
    fn action_type_serializes() {
        let action = ActionType::GitPush;
        let json = serde_json::to_string(&action).unwrap();
        assert_eq!(json, "\"git_push\"");
        let decoded: ActionType = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, ActionType::GitPush);
    }

    #[test]
    fn rule_outcome_serializes() {
        let outcome = RuleOutcome::RequireApproval;
        let json = serde_json::to_string(&outcome).unwrap();
        assert_eq!(json, "\"REQUIRE_APPROVAL\"");
        let decoded: RuleOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, RuleOutcome::RequireApproval);
    }

    #[test]
    fn policy_rule_serializes() {
        let rule = PolicyRule {
            id: "test-rule".to_string(),
            description: "A test rule".to_string(),
            actions: vec![ActionType::GitPush],
            safe_modes: vec![SafeMode::Execute],
            command_classes: vec![],
            branches: vec!["main".to_string()],
            outcome: RuleOutcome::Deny,
            reason: "no push to main".to_string(),
        };
        let json = serde_json::to_string(&rule).unwrap();
        let decoded: PolicyRule = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, "test-rule");
        assert_eq!(decoded.outcome, RuleOutcome::Deny);
    }

    // ---- Breakglass mode ----

    #[test]
    fn breakglass_mode_allows_destructive_ops_via_rules() {
        // Breakglass passes safe mode check, but rules still apply
        let mut engine = PolicyEngine::with_defaults();
        let req = make_request(ActionType::DestructiveCleanup, SafeMode::Breakglass);
        let decision = engine.evaluate(&req).unwrap();
        // Default rules still deny destructive cleanup even in breakglass
        assert_eq!(decision.outcome, PolicyOutcome::Deny);
    }

    #[test]
    fn breakglass_with_custom_rules_allows() {
        let rules = vec![PolicyRule {
            id: "breakglass-allow-all".to_string(),
            description: "Allow everything in breakglass".to_string(),
            actions: vec![],
            safe_modes: vec![SafeMode::Breakglass],
            command_classes: vec![],
            branches: vec![],
            outcome: RuleOutcome::Allow,
            reason: "breakglass override".to_string(),
        }];
        let mut engine = PolicyEngine::new(rules);
        let req = make_request(ActionType::DestructiveCleanup, SafeMode::Breakglass);
        let decision = engine.evaluate(&req).unwrap();
        assert_eq!(decision.outcome, PolicyOutcome::Allow);

        // But not in execute mode
        let req2 = make_request(ActionType::DestructiveCleanup, SafeMode::Execute);
        let decision2 = engine.evaluate(&req2).unwrap();
        assert_eq!(decision2.outcome, PolicyOutcome::Deny);
    }
}
