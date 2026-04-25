//! Run discovery and control target selection helpers.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use uuid::Uuid;

use crate::persistence::query_events;
use crate::projection::run_state_from_event;
use yarli_cli::yarli_core::domain::EntityType;
use yarli_cli::yarli_core::fsm::run::RunState;
use yarli_cli::yarli_store::event_store::EventQuery;
use yarli_cli::yarli_store::EventStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RunDiscoveryScope {
    AllRepos,
    CurrentScope { scope_root: PathBuf },
}

impl RunDiscoveryScope {
    pub(crate) fn description(&self) -> String {
        match self {
            Self::AllRepos => "all repos".to_string(),
            Self::CurrentScope { scope_root } => {
                format!("current repo scope {}", scope_root.display())
            }
        }
    }

    pub(crate) fn list_empty_message(&self) -> String {
        match self {
            Self::AllRepos => "No runs found in event store.".to_string(),
            Self::CurrentScope { scope_root } => format!(
                "No runs found in {}. Pass --all-repos to inspect runs across every repo.",
                scope_root.display()
            ),
        }
    }

    fn selection_hint(&self) -> Option<String> {
        match self {
            Self::AllRepos => None,
            Self::CurrentScope { scope_root } => Some(format!(
                "searched {}; pass --all-repos to search across every repo",
                scope_root.display()
            )),
        }
    }
}

pub(crate) fn cli_flag_requested_from_args<I, S>(args: I, flag: &str) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    args.into_iter().any(|arg| {
        let raw = arg.as_ref().to_string_lossy();
        raw == flag
            || raw
                .strip_prefix(flag)
                .and_then(|suffix| suffix.strip_prefix('='))
                .map(|value| matches!(value, "1" | "true" | "yes" | "on"))
                .unwrap_or(false)
    })
}

pub(crate) fn cli_all_repos_requested() -> bool {
    cli_flag_requested_from_args(std::env::args_os(), "--all-repos")
}

pub(crate) fn resolve_run_discovery_scope(all_repos: bool) -> Result<RunDiscoveryScope> {
    if all_repos {
        return Ok(RunDiscoveryScope::AllRepos);
    }

    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    let scope_root = crate::plan::find_repo_root(&cwd).unwrap_or(cwd);
    let scope_root = scope_root.canonicalize().unwrap_or(scope_root);
    Ok(RunDiscoveryScope::CurrentScope { scope_root })
}

fn scope_root_from_snapshot_payload(payload: &serde_json::Value) -> Option<PathBuf> {
    payload
        .get("config_snapshot")
        .and_then(|snapshot| snapshot.get("runtime"))
        .and_then(|runtime| runtime.get("scope_root"))
        .and_then(|value| value.as_str())
        .map(PathBuf::from)
}

fn working_dir_from_snapshot_payload(payload: &serde_json::Value) -> Option<PathBuf> {
    payload
        .get("config_snapshot")
        .and_then(|snapshot| snapshot.get("runtime"))
        .and_then(|runtime| runtime.get("working_dir"))
        .and_then(|value| value.as_str())
        .map(PathBuf::from)
}

pub(crate) fn run_matches_scope_from_payload(
    scope: &RunDiscoveryScope,
    payload: &serde_json::Value,
) -> bool {
    match scope {
        RunDiscoveryScope::AllRepos => true,
        RunDiscoveryScope::CurrentScope { scope_root } => {
            if let Some(run_scope_root) = scope_root_from_snapshot_payload(payload) {
                return run_scope_root == *scope_root;
            }

            working_dir_from_snapshot_payload(payload)
                .map(|working_dir| working_dir.starts_with(scope_root))
                .unwrap_or(false)
        }
    }
}

pub(crate) fn run_scope_for_run(
    store: &dyn EventStore,
    run_id: Uuid,
) -> Result<Option<RunDiscoveryScope>> {
    let run_events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Run, run_id.to_string()),
    )?;
    for event in run_events.iter().rev() {
        if event.event_type != "run.config_snapshot" {
            continue;
        }
        if let Some(scope_root) = scope_root_from_snapshot_payload(&event.payload) {
            return Ok(Some(RunDiscoveryScope::CurrentScope { scope_root }));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
pub(crate) fn list_runs_by_latest_state(
    store: &dyn EventStore,
) -> Result<BTreeMap<Uuid, RunState>> {
    list_runs_by_latest_state_scoped(store, &RunDiscoveryScope::AllRepos)
}

pub(crate) fn list_runs_by_latest_state_scoped(
    store: &dyn EventStore,
    scope: &RunDiscoveryScope,
) -> Result<BTreeMap<Uuid, RunState>> {
    let run_events = query_events(store, &EventQuery::by_entity_type(EntityType::Run))?;
    let mut runs: BTreeMap<Uuid, RunState> = BTreeMap::new();
    let mut scoped_run_ids: HashSet<Uuid> = HashSet::new();
    for event in run_events {
        let Ok(run_id) = Uuid::parse_str(&event.entity_id) else {
            continue;
        };
        let entry = runs.entry(run_id).or_insert(RunState::RunOpen);
        if let Some(state) = run_state_from_event(&event) {
            *entry = state;
        }
        if matches!(scope, RunDiscoveryScope::AllRepos)
            || (event.event_type == "run.config_snapshot"
                && run_matches_scope_from_payload(scope, &event.payload))
        {
            scoped_run_ids.insert(run_id);
        }
    }

    if matches!(scope, RunDiscoveryScope::AllRepos) {
        return Ok(runs);
    }

    Ok(runs
        .into_iter()
        .filter(|(run_id, _)| scoped_run_ids.contains(run_id))
        .collect())
}

pub(crate) fn compact_run_id(run_id: &str) -> String {
    if let Ok(parsed) = Uuid::parse_str(run_id) {
        parsed.simple().to_string()
    } else {
        run_id.chars().filter(|c| *c != '-').collect()
    }
}

pub(crate) fn unique_run_id_prefixes(
    run_ids: Vec<String>,
    min_len: usize,
) -> HashMap<String, String> {
    let compact: Vec<(String, String)> = run_ids
        .into_iter()
        .map(|run_id| {
            let compact = compact_run_id(&run_id);
            (run_id, compact)
        })
        .collect();
    let mut prefixes = HashMap::new();

    for (run_id, compact_id) in &compact {
        let mut chosen = compact_id.clone();
        let start = min_len.min(compact_id.len()).max(1);
        for len in start..=compact_id.len() {
            let prefix = &compact_id[..len];
            let is_unique = compact
                .iter()
                .filter(|(other_id, _)| other_id != run_id)
                .all(|(_, other_compact)| !other_compact.starts_with(prefix));
            if is_unique {
                chosen = prefix.to_string();
                break;
            }
        }
        prefixes.insert(run_id.clone(), chosen);
    }

    prefixes
}

pub(crate) fn render_run_candidates(run_ids: &[Uuid]) -> String {
    let strings = run_ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    let prefixes = unique_run_id_prefixes(strings.clone(), 10);
    strings
        .iter()
        .map(|id| {
            prefixes
                .get(id)
                .cloned()
                .unwrap_or_else(|| compact_run_id(id))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn select_run_targets_for_control(
    store: &dyn EventStore,
    run_id_input: Option<&str>,
    all_selected: bool,
    eligible_states: &[RunState],
    all_flag_name: &str,
    action_name: &str,
    scope: &RunDiscoveryScope,
) -> Result<Vec<Uuid>> {
    let runs = list_runs_by_latest_state_scoped(store, scope)?;
    if let Some(raw_run_id) = run_id_input {
        let run_id = resolve_run_id_input_scoped(store, raw_run_id, scope)?;
        let state = runs
            .get(&run_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Run {run_id} not found in persisted event log."))?;
        if !eligible_states.contains(&state) {
            bail!(
                "run {run_id} is {:?}; cannot {action_name}. Eligible states: {}",
                state,
                eligible_states
                    .iter()
                    .map(|s| format!("{s:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        return Ok(vec![run_id]);
    }

    let eligible = runs
        .iter()
        .filter_map(|(run_id, state)| eligible_states.contains(state).then_some(*run_id))
        .collect::<Vec<_>>();

    if all_selected {
        if eligible.is_empty() {
            if let Some(hint) = scope.selection_hint() {
                bail!("no eligible runs found for `{action_name}` ({hint})");
            }
            bail!("no eligible runs found for `{action_name}`");
        }
        return Ok(eligible);
    }

    match eligible.len() {
        0 => {
            if let Some(hint) = scope.selection_hint() {
                bail!("no eligible runs found for `{action_name}` ({hint})");
            }
            bail!("no eligible runs found for `{action_name}`");
        }
        1 => Ok(eligible),
        _ => {
            let base = format!(
                "multiple eligible runs found for `{action_name}`; pass <run-id> or --{all_flag_name}. Candidates: {}",
                render_run_candidates(&eligible)
            );
            if let Some(hint) = scope.selection_hint() {
                bail!("{base} ({hint})");
            }
            bail!("{base}");
        }
    }
}

fn resolve_run_id_input_from_candidates(run_id_input: &str, run_ids: &[Uuid]) -> Result<Uuid> {
    let trimmed = run_id_input.trim();
    if trimmed.is_empty() {
        bail!("invalid run ID (expected UUID or unique run-list prefix)");
    }

    if let Ok(parsed) = Uuid::parse_str(trimmed) {
        if run_ids.contains(&parsed) {
            return Ok(parsed);
        }
        bail!("run {parsed} not found in persisted event log");
    }

    let compact_input = trimmed
        .chars()
        .filter(|c| *c != '-')
        .collect::<String>()
        .to_ascii_lowercase();
    if compact_input.is_empty() {
        bail!("invalid run ID (expected UUID or unique run-list prefix)");
    }

    let matches: Vec<Uuid> = run_ids
        .iter()
        .copied()
        .filter(|run_id| run_id.simple().to_string().starts_with(&compact_input))
        .collect();

    match matches.len() {
        1 => Ok(matches[0]),
        0 => bail!(
            "invalid run ID {:?} (expected UUID or unique run-list prefix)",
            run_id_input
        ),
        _ => {
            let sample = matches
                .iter()
                .take(5)
                .map(|run_id| run_id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "ambiguous run ID prefix {:?}; matches multiple runs: {}",
                run_id_input,
                sample
            );
        }
    }
}

pub(crate) fn resolve_run_id_input(store: &dyn EventStore, run_id_input: &str) -> Result<Uuid> {
    let run_events = query_events(store, &EventQuery::by_entity_type(EntityType::Run))?;
    let mut unique = HashSet::new();
    let mut run_ids = Vec::new();
    for event in &run_events {
        let Ok(run_id) = Uuid::parse_str(&event.entity_id) else {
            continue;
        };
        if unique.insert(run_id) {
            run_ids.push(run_id);
        }
    }
    resolve_run_id_input_from_candidates(run_id_input, &run_ids)
}

pub(crate) fn resolve_run_id_input_scoped(
    store: &dyn EventStore,
    run_id_input: &str,
    scope: &RunDiscoveryScope,
) -> Result<Uuid> {
    let run_ids = list_runs_by_latest_state_scoped(store, scope)?
        .keys()
        .copied()
        .collect::<Vec<_>>();
    resolve_run_id_input_from_candidates(run_id_input, &run_ids).map_err(|err| {
        if let Some(hint) = scope.selection_hint() {
            err.context(hint)
        } else {
            err
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{make_event, with_current_dir};
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::process::Command;
    use tempfile::TempDir;
    use yarli_cli::yarli_store::InMemoryEventStore;

    fn run_git_expect_ok(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .expect("git command should run");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn append_scoped_run(
        store: &InMemoryEventStore,
        run_id: Uuid,
        correlation_id: Uuid,
        scope_root: &Path,
        objective: &str,
        event_type: &str,
    ) {
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                correlation_id,
                serde_json::json!({
                    "objective": objective,
                    "config_snapshot": {
                        "runtime": {
                            "working_dir": scope_root.display().to_string(),
                            "scope_root": scope_root.display().to_string(),
                        }
                    }
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                event_type,
                correlation_id,
                serde_json::json!({ "from": "RunOpen", "to": state_name_for_event(event_type) }),
            ))
            .unwrap();
    }

    fn state_name_for_event(event_type: &str) -> &'static str {
        match event_type {
            "run.activated" => "RunActive",
            "run.blocked" => "RunBlocked",
            "run.verifying" => "RunVerifying",
            "run.completed" => "RunCompleted",
            "run.cancelled" => "RunCancelled",
            "run.drained" => "RunDrained",
            _ => "RunOpen",
        }
    }

    fn sorted_ids(ids: impl IntoIterator<Item = Uuid>) -> BTreeSet<Uuid> {
        ids.into_iter().collect()
    }

    #[test]
    fn cli_flag_requested_from_args_detects_all_repos_flag() {
        assert!(cli_flag_requested_from_args(
            ["yarli", "run", "list", "--all-repos"],
            "--all-repos"
        ));
        assert!(cli_flag_requested_from_args(
            ["yarli", "run", "list", "--all-repos=true"],
            "--all-repos"
        ));
        assert!(!cli_flag_requested_from_args(
            ["yarli", "run", "list"],
            "--all-repos"
        ));
    }

    #[test]
    fn select_run_targets_for_control_limits_default_scope_to_current_repo() {
        let repo_a = TempDir::new().expect("repo a");
        let repo_b = TempDir::new().expect("repo b");
        run_git_expect_ok(repo_a.path(), &["init"]);
        run_git_expect_ok(repo_b.path(), &["init"]);

        let store = InMemoryEventStore::new();
        let run_a = Uuid::now_v7();
        let run_b = Uuid::now_v7();
        append_scoped_run(
            &store,
            run_a,
            Uuid::now_v7(),
            repo_a.path(),
            "local",
            "run.activated",
        );
        append_scoped_run(
            &store,
            run_b,
            Uuid::now_v7(),
            repo_b.path(),
            "remote",
            "run.activated",
        );

        let targets = with_current_dir(repo_a.path(), || {
            let scope = resolve_run_discovery_scope(false).unwrap();
            select_run_targets_for_control(
                &store,
                None,
                false,
                &[RunState::RunActive, RunState::RunVerifying],
                "all-active",
                "pause",
                &scope,
            )
            .unwrap()
        });

        assert_eq!(targets, vec![run_a]);
    }

    #[test]
    fn select_run_targets_for_resume_limits_default_scope_to_current_repo() {
        let repo_a = TempDir::new().expect("repo a");
        let repo_b = TempDir::new().expect("repo b");
        run_git_expect_ok(repo_a.path(), &["init"]);
        run_git_expect_ok(repo_b.path(), &["init"]);

        let store = InMemoryEventStore::new();
        let run_a = Uuid::now_v7();
        let run_b = Uuid::now_v7();
        append_scoped_run(
            &store,
            run_a,
            Uuid::now_v7(),
            repo_a.path(),
            "local paused",
            "run.blocked",
        );
        append_scoped_run(
            &store,
            run_b,
            Uuid::now_v7(),
            repo_b.path(),
            "remote paused",
            "run.blocked",
        );

        let targets = with_current_dir(repo_a.path(), || {
            let scope = resolve_run_discovery_scope(false).unwrap();
            select_run_targets_for_control(
                &store,
                None,
                true,
                &[RunState::RunBlocked],
                "all-paused",
                "resume",
                &scope,
            )
            .unwrap()
        });

        assert_eq!(targets, vec![run_a]);
    }

    #[test]
    fn run_list_scope_filters_two_repos_with_and_without_all_repos() {
        let repo_a = TempDir::new().expect("repo a");
        let repo_b = TempDir::new().expect("repo b");
        run_git_expect_ok(repo_a.path(), &["init"]);
        run_git_expect_ok(repo_b.path(), &["init"]);

        let store = InMemoryEventStore::new();
        let local_active = Uuid::now_v7();
        let local_blocked = Uuid::now_v7();
        let remote_active = Uuid::now_v7();
        let remote_blocked = Uuid::now_v7();

        append_scoped_run(
            &store,
            local_active,
            Uuid::now_v7(),
            repo_a.path(),
            "local active",
            "run.activated",
        );
        append_scoped_run(
            &store,
            local_blocked,
            Uuid::now_v7(),
            repo_a.path(),
            "local paused",
            "run.blocked",
        );
        append_scoped_run(
            &store,
            remote_active,
            Uuid::now_v7(),
            repo_b.path(),
            "remote active",
            "run.activated",
        );
        append_scoped_run(
            &store,
            remote_blocked,
            Uuid::now_v7(),
            repo_b.path(),
            "remote paused",
            "run.blocked",
        );

        let current_repo_runs = with_current_dir(repo_a.path(), || {
            let scope = resolve_run_discovery_scope(false).unwrap();
            list_runs_by_latest_state_scoped(&store, &scope).unwrap()
        });
        assert_eq!(
            sorted_ids(current_repo_runs.keys().copied()),
            sorted_ids([local_active, local_blocked])
        );

        let all_repo_runs =
            list_runs_by_latest_state_scoped(&store, &RunDiscoveryScope::AllRepos).unwrap();
        assert_eq!(
            sorted_ids(all_repo_runs.keys().copied()),
            sorted_ids([local_active, local_blocked, remote_active, remote_blocked])
        );
    }

    #[test]
    fn control_selection_filters_two_repos_for_pause_resume_drain_and_cancel() {
        let repo_a = TempDir::new().expect("repo a");
        let repo_b = TempDir::new().expect("repo b");
        run_git_expect_ok(repo_a.path(), &["init"]);
        run_git_expect_ok(repo_b.path(), &["init"]);

        let store = InMemoryEventStore::new();
        let local_active = Uuid::now_v7();
        let local_blocked = Uuid::now_v7();
        let remote_active = Uuid::now_v7();
        let remote_blocked = Uuid::now_v7();

        append_scoped_run(
            &store,
            local_active,
            Uuid::now_v7(),
            repo_a.path(),
            "local active",
            "run.activated",
        );
        append_scoped_run(
            &store,
            local_blocked,
            Uuid::now_v7(),
            repo_a.path(),
            "local paused",
            "run.blocked",
        );
        append_scoped_run(
            &store,
            remote_active,
            Uuid::now_v7(),
            repo_b.path(),
            "remote active",
            "run.activated",
        );
        append_scoped_run(
            &store,
            remote_blocked,
            Uuid::now_v7(),
            repo_b.path(),
            "remote paused",
            "run.blocked",
        );

        let current_scope = with_current_dir(repo_a.path(), || {
            resolve_run_discovery_scope(false).expect("current scope")
        });

        for (action, all_flag, eligible, local_expected, all_expected) in [
            (
                "pause",
                "all-active",
                &[RunState::RunActive, RunState::RunVerifying][..],
                local_active,
                sorted_ids([local_active, remote_active]),
            ),
            (
                "resume",
                "all-paused",
                &[RunState::RunBlocked][..],
                local_blocked,
                sorted_ids([local_blocked, remote_blocked]),
            ),
            (
                "drain",
                "all-active",
                &[RunState::RunActive, RunState::RunVerifying][..],
                local_active,
                sorted_ids([local_active, remote_active]),
            ),
            (
                "cancel",
                "all-active",
                &[RunState::RunActive, RunState::RunVerifying][..],
                local_active,
                sorted_ids([local_active, remote_active]),
            ),
        ] {
            let current_targets = select_run_targets_for_control(
                &store,
                None,
                false,
                eligible,
                all_flag,
                action,
                &current_scope,
            )
            .unwrap();
            assert_eq!(
                current_targets,
                vec![local_expected],
                "{action} should default to the current repo scope"
            );

            let all_repo_targets = select_run_targets_for_control(
                &store,
                None,
                true,
                eligible,
                all_flag,
                action,
                &RunDiscoveryScope::AllRepos,
            )
            .unwrap();
            assert_eq!(
                sorted_ids(all_repo_targets),
                all_expected,
                "{action} with --all-repos and --{all_flag} should include both repos"
            );
        }
    }

    #[test]
    fn resolve_run_id_input_scoped_reports_scope_hint_when_run_is_out_of_scope() {
        let repo_a = TempDir::new().expect("repo a");
        let repo_b = TempDir::new().expect("repo b");
        run_git_expect_ok(repo_a.path(), &["init"]);
        run_git_expect_ok(repo_b.path(), &["init"]);

        let store = InMemoryEventStore::new();
        let run_a = Uuid::now_v7();
        append_scoped_run(
            &store,
            run_a,
            Uuid::now_v7(),
            repo_a.path(),
            "remote",
            "run.activated",
        );

        let err = with_current_dir(repo_b.path(), || {
            let scope = resolve_run_discovery_scope(false).unwrap();
            resolve_run_id_input_scoped(&store, &run_a.simple().to_string()[..12], &scope)
                .unwrap_err()
        });
        let message = err.to_string();
        assert!(message.contains("pass --all-repos"));
        assert!(message.contains("searched"));
    }
}
