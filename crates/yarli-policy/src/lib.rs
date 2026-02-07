//! yarli-policy: Policy engine, rule evaluation, and approval tokens.
//!
//! The policy engine is a deterministic, rule-based evaluation system that
//! decides whether operations are ALLOW, DENY, or REQUIRE_APPROVAL based on
//! actor, command class, repo path, branch target, and run mode (Section 13).

pub mod error;
pub mod rules;
pub mod token;

pub use error::PolicyError;
pub use rules::{ActionType, PolicyEngine, PolicyRequest, PolicyRule, RuleOutcome, SafeModePolicy};
pub use token::{ApprovalToken, TokenScope};
