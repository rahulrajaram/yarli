use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::process;

use anyhow::{bail, Context, Result};
use tracing::{debug, info, warn};
use yarli_cli::prompt;

use crate::plan::{
    discover_plan_entries, looks_like_uuid, parse_plan_tranche_header_line,
    token_has_alpha_and_digit, verify_only_override_enabled, ImplementationPlanEntry, RunPlan,
    TrancheDefinition, TrancheStatus, TranchesFile,
};

#[derive(Debug, Clone)]
pub(crate) struct PlanGuardContext {
    pub(crate) target: String,
    pub(crate) mode: prompt::RunSpecPlanGuardMode,
    pub(crate) was_complete: bool,
    pub(crate) plan_path: PathBuf,
}

// ---------------------------------------------------------------------------
// Structured tranches file (.yarli/tranches.toml)
// ---------------------------------------------------------------------------

pub(crate) const TRANCHES_FILE: &str = ".yarli/tranches.toml";

pub(crate) fn tranches_file_path(base_dir: &Path) -> PathBuf {
    base_dir.join(TRANCHES_FILE)
}

fn byte_offset_to_line_col(content: &str, byte_offset: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut column = 1usize;
    let offset = byte_offset.min(content.len());
    for ch in content[..offset].chars() {
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

fn format_tranches_toml_parse_error(content: &str, err: &toml::de::Error) -> String {
    let mut detail = err.to_string();
    if let Some(span) = err.span() {
        let (line, column) = byte_offset_to_line_col(content, span.start);
        detail.push_str(&format!(" (line {line}, column {column})"));
    }
    detail
}

fn atomic_write(path: &Path, content: &str) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent directory", path.display()))?;
    let pid = process::id();
    let tmp_basename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tranches.toml");
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 0..64u32 {
        let tmp_path = parent.join(format!(".{tmp_basename}.tmp-{pid}-{attempt}"));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
        {
            Ok(mut file) => {
                if let Err(err) = file.write_all(content.as_bytes()) {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(err).with_context(|| {
                        format!("failed to write temporary file {}", tmp_path.display())
                    });
                }
                if let Err(err) = file.sync_all() {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(err).with_context(|| {
                        format!("failed to sync temporary file {}", tmp_path.display())
                    });
                }
                drop(file);
                fs::rename(&tmp_path, path).with_context(|| {
                    format!(
                        "failed to atomically replace {} with {}",
                        path.display(),
                        tmp_path.display()
                    )
                })?;
                return Ok(());
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                last_error = Some(err.into());
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to create temporary file for {}", path.display())
                });
            }
        }
    }

    let last_error = last_error.map(|err| format!(": {err}")).unwrap_or_default();
    bail!(
        "failed to create unique temporary file for {} after multiple attempts{}",
        path.display(),
        last_error
    )
}

pub(crate) fn validate_structured_tranches_preflight_for_prompt(
    entry_prompt_path: &Path,
) -> Result<Option<PathBuf>> {
    let plan_path = match plan_path_for_prompt_entry(entry_prompt_path) {
        Ok(path) => path,
        Err(_) => return Ok(None),
    };
    let tranches_base_dir = plan_path
        .parent()
        .context("plan file has no parent directory")?;
    let tranches_path = tranches_file_path(tranches_base_dir);
    if !tranches_path.exists() {
        return Ok(None);
    }

    read_tranches_file_in(tranches_base_dir).map_err(|err| {
        anyhow::anyhow!(
            "structured tranches preflight failed for {}.\n\
Run aborted to avoid continuation/plan state drift.\n\
Fix the file syntax and rerun (for example: edit the file, then run `yarli plan validate`).\n\
Parse details:\n{err:#}",
            tranches_path.display()
        )
    })?;
    Ok(Some(tranches_path))
}

pub(crate) fn read_tranches_file_in(base_dir: &Path) -> Result<Option<TranchesFile>> {
    let path = tranches_file_path(base_dir);
    if !path.exists() {
        return Ok(None);
    }
    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let tf: TranchesFile = toml::from_str(&content).map_err(|err| {
        let detail = format_tranches_toml_parse_error(&content, &err);
        anyhow::anyhow!("failed to parse {}: {}", path.display(), detail)
    })?;
    if tf.version != 1 {
        bail!(
            "{}: unsupported version {} (expected 1)",
            path.display(),
            tf.version
        );
    }
    Ok(Some(tf))
}

pub(crate) fn read_tranches_file() -> Result<Option<TranchesFile>> {
    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    read_tranches_file_in(&cwd)
}

pub(crate) fn write_tranches_file_in(base_dir: &Path, tf: &TranchesFile) -> Result<()> {
    let path = tranches_file_path(base_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let content = toml::to_string_pretty(tf).context("failed to serialize tranches file")?;
    atomic_write(&path, &content).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub(crate) fn write_tranches_file(tf: &TranchesFile) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    write_tranches_file_in(&cwd, tf)
}

pub(crate) fn discover_plan_dispatch_entries(plan_text: &str) -> Vec<ImplementationPlanEntry> {
    let mut in_next_work_tranches = false;
    let mut saw_next_work_tranches_header = false;
    let mut entries = Vec::new();

    for line in plan_text.lines() {
        let trimmed = line.trim();
        if !in_next_work_tranches {
            let header_text = trimmed.trim_start_matches('#').trim();
            if header_text.eq_ignore_ascii_case("Next Work Tranches")
                || header_text.eq_ignore_ascii_case("Next Priority Actions")
            {
                in_next_work_tranches = true;
                saw_next_work_tranches_header = true;
            }
            continue;
        }

        if trimmed.starts_with("## ") {
            break;
        }
        if let Some(entry) = parse_plan_tranche_header_line(line) {
            entries.push(entry);
        }
    }

    if saw_next_work_tranches_header {
        entries
    } else {
        discover_plan_entries(plan_text)
    }
}

pub(crate) fn plan_path_for_prompt_entry(entry_prompt_path: &Path) -> Result<PathBuf> {
    let mut current = entry_prompt_path
        .parent()
        .context("prompt file has no parent directory")?
        .to_path_buf();
    loop {
        let candidate = current.join("IMPLEMENTATION_PLAN.md");
        if candidate.exists() {
            return Ok(candidate);
        }
        if !current.pop() {
            bail!(
                "failed to resolve IMPLEMENTATION_PLAN.md for prompt {}",
                entry_prompt_path.display()
            );
        }
    }
}

pub(crate) fn maybe_mark_current_structured_tranche_complete(plan: &RunPlan) -> Result<bool> {
    let Some(current_index) = plan.current_tranche_index else {
        return Ok(false);
    };
    let Some(current_tranche) = plan.tranche_plan.get(current_index) else {
        return Ok(false);
    };
    if current_tranche.key.eq_ignore_ascii_case("verification")
        || current_tranche.key.eq_ignore_ascii_case("prompt")
    {
        return Ok(false);
    }

    let Some(prompt_snapshot) = plan.prompt_snapshot.as_ref() else {
        return Ok(false);
    };
    let prompt_entry_path = PathBuf::from(&prompt_snapshot.entry_path);
    let plan_path = plan_path_for_prompt_entry(&prompt_entry_path)?;
    let tranches_base_dir = plan_path
        .parent()
        .context("plan file has no parent directory")?;
    let tranches_path = tranches_file_path(tranches_base_dir);

    let Some(mut tf) = read_tranches_file_in(tranches_base_dir)? else {
        return Ok(false);
    };
    let Some(def) = tf
        .tranches
        .iter_mut()
        .find(|def| def.key.eq_ignore_ascii_case(&current_tranche.key))
    else {
        debug!(
            tranche_key = %current_tranche.key,
            tranches_file = %tranches_path.display(),
            "current tranche key not found in structured tranches file"
        );
        return Ok(false);
    };
    if def.status == TrancheStatus::Complete {
        return Ok(false);
    }

    def.status = TrancheStatus::Complete;
    let completed_key = def.key.clone();
    write_tranches_file_in(tranches_base_dir, &tf)?;
    info!(
        tranche_key = %completed_key,
        tranches_file = %tranches_path.display(),
        "auto-marked structured tranche as complete"
    );
    Ok(true)
}

/// Run the tranche's `verify` command (if any) and return whether it passed.
///
/// Returns `Ok(true)` (default pass) when:
/// - there is no current tranche index,
/// - the tranche key is a special key ("verification" / "prompt"),
/// - there is no prompt snapshot,
/// - no `.yarli/tranches.toml` exists,
/// - the key is not found in the file, or
/// - the tranche definition has no `verify` field.
///
/// On spawn failure the command is treated as failed (`Ok(false)`).
pub(crate) fn run_tranche_verify_command(
    plan: &RunPlan,
    workdir: &Path,
    timeout_secs: u64,
) -> Result<bool> {
    let Some(current_index) = plan.current_tranche_index else {
        return Ok(true);
    };
    let Some(current_tranche) = plan.tranche_plan.get(current_index) else {
        return Ok(true);
    };
    if current_tranche.key.eq_ignore_ascii_case("verification")
        || current_tranche.key.eq_ignore_ascii_case("prompt")
    {
        return Ok(true);
    }

    let Some(prompt_snapshot) = plan.prompt_snapshot.as_ref() else {
        return Ok(true);
    };
    let prompt_entry_path = PathBuf::from(&prompt_snapshot.entry_path);
    let plan_path = plan_path_for_prompt_entry(&prompt_entry_path)?;
    let tranches_base_dir = plan_path
        .parent()
        .context("plan file has no parent directory")?;

    let Some(tf) = read_tranches_file_in(tranches_base_dir)? else {
        return Ok(true);
    };
    let Some(def) = tf
        .tranches
        .iter()
        .find(|def| def.key.eq_ignore_ascii_case(&current_tranche.key))
    else {
        return Ok(true);
    };

    let Some(verify_cmd) = def.verify.as_deref() else {
        return Ok(true);
    };

    info!(
        tranche_key = %current_tranche.key,
        verify_cmd = %verify_cmd,
        workdir = %workdir.display(),
        "running tranche verify command"
    );

    let mut child = match process::Command::new("sh")
        .args(["-c", verify_cmd])
        .current_dir(workdir)
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(err) => {
            warn!(
                tranche_key = %current_tranche.key,
                error = %err,
                "failed to spawn tranche verify command"
            );
            return Ok(false);
        }
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let passed = status.success();
                info!(
                    tranche_key = %current_tranche.key,
                    exit_code = status.code(),
                    passed,
                    "tranche verify command finished"
                );
                return Ok(passed);
            }
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    warn!(
                        tranche_key = %current_tranche.key,
                        timeout_secs,
                        "tranche verify command timed out; killing"
                    );
                    let _ = child.kill();
                    let _ = child.wait();
                    return Ok(false);
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            Err(err) => {
                warn!(
                    tranche_key = %current_tranche.key,
                    error = %err,
                    "error polling tranche verify command"
                );
                let _ = child.kill();
                let _ = child.wait();
                return Ok(false);
            }
        }
    }
}

pub(crate) fn validate_tranche_definition(def: &TrancheDefinition) -> Result<()> {
    let key = def.key.trim();
    if key.is_empty() {
        bail!("tranche key must not be empty");
    }
    if looks_like_uuid(key) {
        bail!(
            "tranche key '{}' looks like a stale UUID — use a descriptive key instead",
            key
        );
    }
    let lower = key.to_ascii_lowercase();
    for prefix in &["run-", "app-", "target-"] {
        if lower.starts_with(prefix) {
            bail!(
                "tranche key '{}' uses reserved prefix '{}' — use a descriptive key instead",
                key,
                prefix.trim_end_matches('-')
            );
        }
    }
    if !token_has_alpha_and_digit(key) {
        bail!("tranche key '{}' must contain both letters and digits", key);
    }
    let summary = def.summary.trim();
    if summary.is_empty() {
        bail!("tranche summary must not be empty");
    }
    let word_count = summary.split_whitespace().count();
    if word_count < 3 {
        bail!(
            "tranche summary '{}' is too vague ({} word(s)) — use format: Verb + outcome + scope",
            summary,
            word_count
        );
    }
    let lower_summary = summary.to_ascii_lowercase();
    for pattern in PLACEHOLDER_PHRASES {
        if lower_summary.contains(pattern) {
            bail!(
                "tranche summary contains placeholder text '{}' — provide a specific work description",
                pattern
            );
        }
    }
    Ok(())
}

/// Phrases that indicate a placeholder/vague tranche summary.
const PLACEHOLDER_PHRASES: &[&str] = &[
    "details pending",
    "pending",
    "placeholder",
    "to be determined",
    "tbd",
    "todo",
    "fix later",
    "needs work",
    "fill in",
];

// ---------------------------------------------------------------------------
// `yarli plan` command handlers
// ---------------------------------------------------------------------------

pub(crate) fn cmd_plan_tranche_add(
    key: &str,
    summary: &str,
    group: Option<&str>,
    allowed_paths: &[String],
    verify: Option<&str>,
    done_when: Option<&str>,
    max_tokens: Option<u64>,
) -> Result<()> {
    let def = TrancheDefinition {
        key: key.to_string(),
        summary: summary.to_string(),
        status: TrancheStatus::Incomplete,
        group: group.map(|g| g.to_string()),
        allowed_paths: allowed_paths.to_vec(),
        verify: verify.map(|s| s.to_string()),
        done_when: done_when.map(|s| s.to_string()),
        max_tokens,
    };
    validate_tranche_definition(&def)?;

    let mut tf = read_tranches_file()?.unwrap_or(TranchesFile {
        version: 1,
        tranches: Vec::new(),
    });

    if tf.tranches.iter().any(|t| t.key == key) {
        bail!("tranche with key '{}' already exists", key);
    }

    tf.tranches.push(def);
    write_tranches_file(&tf)?;
    println!("Added tranche '{key}'");
    Ok(())
}

pub(crate) fn cmd_plan_tranche_complete(key: &str) -> Result<()> {
    let mut tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    let entry = tf
        .tranches
        .iter_mut()
        .find(|t| t.key == key)
        .ok_or_else(|| anyhow::anyhow!("tranche with key '{}' not found", key))?;

    entry.status = TrancheStatus::Complete;
    write_tranches_file(&tf)?;
    println!("Marked tranche '{key}' as complete");
    Ok(())
}

pub(crate) fn cmd_plan_tranche_list() -> Result<()> {
    let tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    if tf.tranches.is_empty() {
        println!("No tranches defined.");
        return Ok(());
    }

    for def in &tf.tranches {
        let marker = match def.status {
            TrancheStatus::Complete => "[x]",
            TrancheStatus::Incomplete => "[ ]",
            TrancheStatus::Blocked => "[~]",
        };
        let group_suffix = def
            .group
            .as_ref()
            .map(|g| format!("  (group: {g})"))
            .unwrap_or_default();
        println!("{marker} {}: {}{group_suffix}", def.key, def.summary);
    }
    Ok(())
}

pub(crate) fn cmd_plan_tranche_remove(key: &str) -> Result<()> {
    let mut tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    let before = tf.tranches.len();
    tf.tranches.retain(|t| t.key != key);
    if tf.tranches.len() == before {
        bail!("tranche with key '{}' not found", key);
    }

    write_tranches_file(&tf)?;
    println!("Removed tranche '{key}'");
    Ok(())
}

pub(crate) fn cmd_plan_validate() -> Result<()> {
    let tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    let mut errors = Vec::new();
    let mut seen_keys = HashSet::new();

    for (i, def) in tf.tranches.iter().enumerate() {
        if let Err(e) = validate_tranche_definition(def) {
            errors.push(format!("tranches[{}] (key='{}'): {}", i, def.key, e));
        }
        if !seen_keys.insert(&def.key) {
            errors.push(format!("tranches[{}]: duplicate key '{}'", i, def.key));
        }
    }

    if errors.is_empty() {
        println!(
            "Tranches file is valid ({} tranches defined).",
            tf.tranches.len()
        );
        Ok(())
    } else {
        for err in &errors {
            eprintln!("  error: {err}");
        }
        bail!("{TRANCHES_FILE} has {} validation error(s)", errors.len());
    }
}

// ---------------------------------------------------------------------------
// Plan-guard helpers
// ---------------------------------------------------------------------------

pub(crate) fn target_matches_entry_key(entry_key: &str, target: &str) -> bool {
    if entry_key.eq_ignore_ascii_case(target) {
        return true;
    }
    let Some(remainder) = entry_key.strip_prefix(target) else {
        return false;
    };
    match remainder.chars().next() {
        None => true,
        Some(next) => !(next.is_ascii_alphanumeric() || next == '_' || next == '-'),
    }
}

pub(crate) fn plan_target_completion_state(plan_text: &str, target: &str) -> Result<Option<bool>> {
    Ok(discover_plan_entries(plan_text)
        .into_iter()
        .find(|entry| target_matches_entry_key(&entry.key, target))
        .map(|entry| entry.is_complete))
}

fn run_spec_mentions_verification(run_spec: &prompt::RunSpec) -> bool {
    if run_spec
        .objective
        .as_ref()
        .map(|objective| objective.to_ascii_lowercase().contains("verif"))
        .unwrap_or(false)
    {
        return true;
    }

    run_spec
        .tranches
        .as_ref()
        .map(|tranches| {
            tranches.items.iter().any(|tranche| {
                tranche
                    .objective
                    .as_ref()
                    .map(|objective| objective.to_ascii_lowercase().contains("verif"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

pub(crate) fn run_spec_plan_guard_preflight_with_override(
    loaded: &prompt::LoadedPrompt,
    verify_only_override: bool,
) -> Result<Option<PlanGuardContext>> {
    let Some(plan_guard) = loaded.run_spec.plan_guard.as_ref() else {
        return Ok(None);
    };

    let plan_path = plan_path_for_prompt_entry(&loaded.entry_path)?;
    let plan_text = fs::read_to_string(&plan_path).with_context(|| {
        format!(
            "failed to read plan guard file at {}",
            plan_path.as_path().display()
        )
    })?;
    let is_complete =
        plan_target_completion_state(&plan_text, &plan_guard.target)?.ok_or_else(|| {
            anyhow::anyhow!(
                "plan guard target {} not found in IMPLEMENTATION_PLAN.md",
                plan_guard.target
            )
        })?;

    match plan_guard.mode {
        prompt::RunSpecPlanGuardMode::Implement => {
            if is_complete && !verify_only_override {
                info!(
                    target = %plan_guard.target,
                    "plan guard target already complete; proceeding with verification-only routing"
                );
            }
        }
        prompt::RunSpecPlanGuardMode::VerifyOnly => {
            if !is_complete {
                bail!(
                    "plan guard blocked run: target {} is not complete; verify-only mode is only allowed after completion",
                    plan_guard.target
                );
            }
            if !run_spec_mentions_verification(&loaded.run_spec) {
                bail!(
                    "plan guard blocked run: verify-only mode requires objective text to clearly indicate verification"
                );
            }
        }
    }

    if verify_only_override && plan_guard.mode == prompt::RunSpecPlanGuardMode::Implement {
        info!(
            target = %plan_guard.target,
            "VERIFY_ONLY override enabled for completed implement target"
        );
    }

    Ok(Some(PlanGuardContext {
        target: plan_guard.target.clone(),
        mode: plan_guard.mode,
        was_complete: is_complete,
        plan_path,
    }))
}

pub(crate) fn run_spec_plan_guard_preflight(
    loaded: &prompt::LoadedPrompt,
) -> Result<Option<PlanGuardContext>> {
    run_spec_plan_guard_preflight_with_override(loaded, verify_only_override_enabled())
}

pub(crate) fn enforce_plan_guard_post_run(
    _loaded: &prompt::LoadedPrompt,
    context: &PlanGuardContext,
) -> Result<()> {
    if context.mode != prompt::RunSpecPlanGuardMode::Implement {
        return Ok(());
    }

    let plan_text = fs::read_to_string(&context.plan_path).with_context(|| {
        format!(
            "failed to read plan guard file at {}",
            context.plan_path.as_path().display()
        )
    })?;
    let is_complete_now =
        plan_target_completion_state(&plan_text, &context.target)?.ok_or_else(|| {
            anyhow::anyhow!(
                "plan guard target {} disappeared from IMPLEMENTATION_PLAN.md",
                context.target
            )
        })?;

    if is_complete_now && !context.was_complete {
        info!(
            target = %context.target,
            "plan guard target transitioned to complete during run"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config,
        plan::{
            discover_plan_dispatch_entries, is_verification_only_dispatch,
            plan_target_completion_state, run_spec_plan_guard_preflight_with_override,
            should_auto_advance_planned_tranche, validate_tranche_keys, PlannedTranche,
        },
    };
    use chrono::Utc;
    use tempfile::TempDir;
    use uuid::Uuid;
    use yarli_cli::yarli_core::entities::continuation::{
        ContinuationPayload, ContinuationQualityGate, RunSummary, TaskHealthAction, TrancheKind,
        TrancheSpec,
    };
    use yarli_cli::yarli_core::explain::DeteriorationTrend;
    use yarli_cli::yarli_core::fsm::run::RunState;

    #[test]
    fn auto_advance_blocks_stable_quality_gate() {
        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 1,
                completed: 1,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            tranche_token_usage: Vec::new(),
            tranche_token_thresholds: None,
            next_tranche: Some(TrancheSpec {
                suggested_objective: "next".into(),
                kind: TrancheKind::PlannedNext,
                retry_task_keys: vec![],
                unfinished_task_keys: vec![],
                planned_task_keys: vec!["test".into()],
                planned_tranche_key: Some("full".into()),
                cursor: None,
                config_snapshot: serde_json::json!({}),
                interventions: Vec::new(),
            }),
            quality_gate: Some(ContinuationQualityGate {
                allow_auto_advance: false,
                reason: "stable".into(),
                trend: Some(DeteriorationTrend::Stable),
                score: Some(30.0),
                task_health_action: TaskHealthAction::Continue,
            }),
            retry_recommendation: None,
        };
        let (allow, reason) = should_auto_advance_planned_tranche(
            &payload,
            config::AutoAdvanceConfig {
                policy: crate::config::AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 0,
                task_health: crate::config::RunTaskHealthConfig::default(),
            },
            0,
        );
        assert!(!allow);
        assert_eq!(reason, "stable");
    }

    #[test]
    fn auto_advance_policy_always_overrides_quality_gate() {
        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 1,
                completed: 1,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            tranche_token_usage: Vec::new(),
            tranche_token_thresholds: None,
            next_tranche: Some(TrancheSpec {
                suggested_objective: "next".into(),
                kind: TrancheKind::PlannedNext,
                retry_task_keys: vec![],
                unfinished_task_keys: vec![],
                planned_task_keys: vec!["test".into()],
                planned_tranche_key: Some("full".into()),
                cursor: None,
                config_snapshot: serde_json::json!({}),
                interventions: Vec::new(),
            }),
            quality_gate: Some(ContinuationQualityGate {
                allow_auto_advance: false,
                reason: "stable".into(),
                trend: Some(DeteriorationTrend::Stable),
                score: Some(30.0),
                task_health_action: TaskHealthAction::Continue,
            }),
            retry_recommendation: None,
        };
        let (allow, reason) = should_auto_advance_planned_tranche(
            &payload,
            config::AutoAdvanceConfig {
                policy: crate::config::AutoAdvancePolicy::Always,
                max_tranches: 0,
                task_health: crate::config::RunTaskHealthConfig::default(),
            },
            0,
        );
        assert!(allow);
        assert_eq!(reason, "auto_advance_policy=always");
    }

    #[test]
    fn auto_advance_respects_max_tranche_cap() {
        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 1,
                completed: 1,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            tranche_token_usage: Vec::new(),
            tranche_token_thresholds: None,
            next_tranche: Some(TrancheSpec {
                suggested_objective: "next".into(),
                kind: TrancheKind::PlannedNext,
                retry_task_keys: vec![],
                unfinished_task_keys: vec![],
                planned_task_keys: vec!["test".into()],
                planned_tranche_key: Some("full".into()),
                cursor: None,
                config_snapshot: serde_json::json!({}),
                interventions: Vec::new(),
            }),
            quality_gate: Some(ContinuationQualityGate {
                allow_auto_advance: true,
                reason: "improving".into(),
                trend: Some(DeteriorationTrend::Improving),
                score: Some(5.0),
                task_health_action: TaskHealthAction::Continue,
            }),
            retry_recommendation: None,
        };

        let (allow, reason) = should_auto_advance_planned_tranche(
            &payload,
            config::AutoAdvanceConfig {
                policy: crate::config::AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 2,
                task_health: crate::config::RunTaskHealthConfig::default(),
            },
            2,
        );
        assert!(!allow);
        assert!(reason.contains("max_auto_advance_tranches=2"));
    }

    #[test]
    fn auto_advance_blocks_gate_retry_tranche() {
        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunFailed,
            exit_reason: Some(yarli_cli::yarli_core::domain::ExitReason::BlockedGateFailure),
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 2,
                completed: 2,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            tranche_token_usage: Vec::new(),
            tranche_token_thresholds: None,
            next_tranche: Some(TrancheSpec {
                suggested_objective: "Re-verify after gate failure".into(),
                kind: TrancheKind::GateRetry,
                retry_task_keys: vec!["build".into(), "test".into()],
                unfinished_task_keys: vec![],
                planned_task_keys: Vec::new(),
                planned_tranche_key: None,
                cursor: None,
                config_snapshot: serde_json::json!({}),
                interventions: Vec::new(),
            }),
            quality_gate: Some(ContinuationQualityGate {
                allow_auto_advance: true,
                reason: "improving".into(),
                trend: Some(DeteriorationTrend::Improving),
                score: Some(5.0),
                task_health_action: TaskHealthAction::Continue,
            }),
            retry_recommendation: None,
        };

        let (allow, reason) = should_auto_advance_planned_tranche(
            &payload,
            config::AutoAdvanceConfig {
                policy: crate::config::AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 0,
                task_health: crate::config::RunTaskHealthConfig::default(),
            },
            0,
        );
        assert!(!allow);
        assert!(reason.contains("retry/unfinished, not planned-next"));
    }

    #[test]
    fn verification_only_dispatch_detects_single_verification_tranche() {
        let plan = vec![PlannedTranche {
            key: "verification".to_string(),
            objective: "verify".to_string(),
            task_keys: vec!["verify-001".to_string()],
            tranche_group: None,
        }];
        assert!(is_verification_only_dispatch(&plan));
    }

    #[test]
    fn verification_only_dispatch_rejects_multi_tranche_plan() {
        let plan = vec![
            PlannedTranche {
                key: "I9".to_string(),
                objective: "i9".to_string(),
                task_keys: vec!["tranche-001-i9".to_string()],
                tranche_group: Some("runtime-contract".to_string()),
            },
            PlannedTranche {
                key: "verification".to_string(),
                objective: "verify".to_string(),
                task_keys: vec!["verify-002".to_string()],
                tranche_group: None,
            },
        ];
        assert!(!is_verification_only_dispatch(&plan));
    }

    #[test]
    fn validate_tranche_keys_rejects_uuid() {
        let tranches = vec![PlannedTranche {
            key: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            objective: "test".to_string(),
            task_keys: vec!["task-001".to_string()],
            tranche_group: None,
        }];
        let err = validate_tranche_keys(&tranches).unwrap_err();
        assert!(
            format!("{err}").contains("stale UUID"),
            "expected UUID rejection, got: {err}"
        );
    }

    #[test]
    fn validate_tranche_keys_rejects_run_prefix() {
        let tranches = vec![PlannedTranche {
            key: "run-abc-123".to_string(),
            objective: "test".to_string(),
            task_keys: vec!["task-001".to_string()],
            tranche_group: None,
        }];
        let err = validate_tranche_keys(&tranches).unwrap_err();
        assert!(
            format!("{err}").contains("reserved prefix"),
            "expected prefix rejection, got: {err}"
        );
    }

    #[test]
    fn validate_tranche_keys_rejects_app_prefix() {
        let tranches = vec![PlannedTranche {
            key: "app-main-deploy".to_string(),
            objective: "test".to_string(),
            task_keys: vec!["task-001".to_string()],
            tranche_group: None,
        }];
        assert!(validate_tranche_keys(&tranches).is_err());
    }

    #[test]
    fn validate_tranche_keys_rejects_target_prefix() {
        let tranches = vec![PlannedTranche {
            key: "Target-production".to_string(),
            objective: "test".to_string(),
            task_keys: vec!["task-001".to_string()],
            tranche_group: None,
        }];
        assert!(validate_tranche_keys(&tranches).is_err());
    }

    #[test]
    fn validate_tranche_keys_rejects_empty() {
        let tranches = vec![PlannedTranche {
            key: "  ".to_string(),
            objective: "test".to_string(),
            task_keys: vec!["task-001".to_string()],
            tranche_group: None,
        }];
        assert!(validate_tranche_keys(&tranches).is_err());
    }

    #[test]
    fn validate_tranche_keys_allows_valid_keys() {
        let tranches = vec![
            PlannedTranche {
                key: "build-and-test".to_string(),
                objective: "build".to_string(),
                task_keys: vec!["task-001".to_string()],
                tranche_group: None,
            },
            PlannedTranche {
                key: "verification".to_string(),
                objective: "verify".to_string(),
                task_keys: vec!["verify-001".to_string()],
                tranche_group: None,
            },
            PlannedTranche {
                key: "I9".to_string(),
                objective: "implement I9".to_string(),
                task_keys: vec!["tranche-001".to_string()],
                tranche_group: None,
            },
        ];
        assert!(validate_tranche_keys(&tranches).is_ok());
    }

    #[test]
    fn looks_like_uuid_detects_valid_uuids() {
        assert!(looks_like_uuid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(looks_like_uuid("ABCDEF00-1234-5678-9ABC-DEF012345678"));
        assert!(!looks_like_uuid("not-a-uuid"));
        assert!(!looks_like_uuid("build-and-test"));
        assert!(!looks_like_uuid("550e8400e29b41d4a716446655440000"));
    }

    #[test]
    fn plan_target_completion_state_detects_status() {
        let plan = "- [x] CARD-R8-01\n- [ ] CARD-R8-02\n";
        assert_eq!(
            plan_target_completion_state(plan, "CARD-R8-01").unwrap(),
            Some(true)
        );
        assert_eq!(
            plan_target_completion_state(plan, "CARD-R8-02").unwrap(),
            Some(false)
        );
        assert_eq!(
            plan_target_completion_state(plan, "CARD-R8-03").unwrap(),
            None
        );
    }

    #[test]
    fn plan_target_completion_state_supports_common_non_checkbox_format() {
        let plan = "1. I8A complete\n2. I8B incomplete\n";
        assert_eq!(
            plan_target_completion_state(plan, "I8A").unwrap(),
            Some(true)
        );
        assert_eq!(
            plan_target_completion_state(plan, "I8B").unwrap(),
            Some(false)
        );
    }

    #[test]
    fn discover_plan_entries_extracts_tranche_group_metadata() {
        let entries = crate::plan::discover_plan_entries(
            "- [ ] I8A first tranche_group=CORE\n- [x] I8B second [tranche_group=ui]\n",
        );
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "I8A");
        assert_eq!(entries[0].tranche_group.as_deref(), Some("core"));
        assert!(!entries[0].summary.contains("tranche_group"));
        assert_eq!(entries[1].tranche_group.as_deref(), Some("ui"));
    }

    #[test]
    fn discover_plan_dispatch_entries_prefers_next_work_tranches_section() {
        let plan = r#"
## Next Work Tranches
11. I9 `Runtime Contract`: complete. tranche_group=runtime-contract
12. I10 `Remediation`: incomplete. tranche_group=runtime-contract
    1. Evidence line references I11 and 019c5308-e73b-7a23-8b7a-c4acc8b95e52.

## Notes
1. YARLI_DETERIORATION_REPORT_V1 incomplete.
"#;
        let entries = discover_plan_dispatch_entries(plan);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "I9");
        assert!(entries[0].is_complete);
        assert_eq!(entries[1].key, "I10");
        assert!(!entries[1].is_complete);
    }

    #[test]
    fn discover_plan_dispatch_entries_falls_back_when_section_missing() {
        let plan = "- [ ] I8A first\n- [x] I8B second\n";
        let entries = discover_plan_dispatch_entries(plan);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "I8A");
        assert!(!entries[0].is_complete);
        assert_eq!(entries[1].key, "I8B");
        assert!(entries[1].is_complete);
    }

    #[test]
    fn discover_plan_dispatch_entries_parses_tp_keys_under_next_work_tranches() {
        let plan = r#"
## Next Work Tranches
1. TP-05 `Config loader`: incomplete.
2. TP-06 `Validation pass`: incomplete.
3. TP-07 `Finalize schema`: complete.
"#;
        let entries = discover_plan_dispatch_entries(plan);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, "TP-05");
        assert!(!entries[0].is_complete);
        assert_eq!(entries[1].key, "TP-06");
        assert!(!entries[1].is_complete);
        assert_eq!(entries[2].key, "TP-07");
        assert!(entries[2].is_complete);
    }

    #[test]
    fn discover_plan_dispatch_entries_recognizes_next_priority_actions_header() {
        let plan = r#"
### Next Priority Actions
1. TP-01 `Bootstrap runner`: incomplete.
2. TP-02 `Wire events`: incomplete.
"#;
        let entries = discover_plan_dispatch_entries(plan);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "TP-01");
        assert!(!entries[0].is_complete);
        assert_eq!(entries[1].key, "TP-02");
        assert!(!entries[1].is_complete);
    }

    #[test]
    fn discover_plan_dispatch_entries_mixed_complete_and_pending() {
        let plan = r#"
## Next Work Tranches
1. TP-10 `Done task`: complete.
2. TP-11 `Open task`: incomplete.
3. TP-12 `Blocked task`: blocked.
"#;
        let entries = discover_plan_dispatch_entries(plan);
        assert_eq!(entries.len(), 3);
        let open: Vec<_> = entries.iter().filter(|e| !e.is_complete).collect();
        assert_eq!(open.len(), 2);
        assert_eq!(open[0].key, "TP-11");
        assert_eq!(open[1].key, "TP-12");
    }

    #[test]
    fn discover_plan_dispatch_entries_i_prefix_backward_compat() {
        let plan = r#"
## Next Work Tranches
1. I5 `Legacy format`: incomplete.
2. I6 `Also legacy`: complete.
"#;
        let entries = discover_plan_dispatch_entries(plan);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "I5");
        assert!(!entries[0].is_complete);
        assert_eq!(entries[1].key, "I6");
        assert!(entries[1].is_complete);
    }

    #[test]
    fn parse_plan_tranche_header_line_rejects_uuid_keys() {
        let line = "1. 550e8400-e29b-41d4-a716-446655440000 `Some task`: incomplete.";
        assert!(crate::plan::parse_plan_tranche_header_line(line).is_none());
    }

    #[test]
    fn parse_plan_tranche_header_line_handles_colon_in_title() {
        let line = r#"62. I153 `Visual indicator in status bar: "PLAN MODE -- read-only"`: incomplete. tranche_group=tool-io"#;
        let entry = crate::plan::parse_plan_tranche_header_line(line).expect("should parse");
        assert_eq!(entry.key, "I153");
        assert!(!entry.is_complete);
        assert_eq!(entry.tranche_group.as_deref(), Some("tool-io"));

        // Multiple colons in title
        let line2 = r#"30. I121 `Search backend adapter: LSP: ripgrep`: blocked. tranche_group=tool-lsp-search"#;
        let entry2 = crate::plan::parse_plan_tranche_header_line(line2).expect("should parse");
        assert_eq!(entry2.key, "I121");
        assert!(!entry2.is_complete);
    }

    #[test]
    fn discover_plan_entries_extracts_allowed_paths_metadata() {
        let entries = crate::plan::discover_plan_entries(
            "- [ ] I8A first allowed_paths=src/main.rs,docs/CLI.md,../reject\n- [x] I8B second [allowed_paths=src/lib.rs,SRC/lib.rs,/reject]\n",
        );
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].allowed_paths,
            vec!["src/main.rs".to_string(), "docs/CLI.md".to_string()]
        );
        assert_eq!(entries[1].allowed_paths, vec!["src/lib.rs".to_string()]);
        assert!(!entries[0].summary.contains("allowed_paths"));
        assert!(!entries[1].summary.contains("allowed_paths"));
    }

    #[test]
    fn discover_plan_entries_extracts_contract_metadata() {
        let entries = crate::plan::discover_plan_entries(
            "- [ ] I8A first tranche_group=core verify=\"cargo test -p yarli tranche\" done_when=\"Status output shows tranche totals\" max_tokens=42000\n",
        );
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.key, "I8A");
        assert_eq!(entry.tranche_group.as_deref(), Some("core"));
        assert_eq!(entry.verify.as_deref(), Some("cargo test -p yarli tranche"));
        assert_eq!(
            entry.done_when.as_deref(),
            Some("Status output shows tranche totals")
        );
        assert_eq!(entry.max_tokens, Some(42_000));
        assert!(!entry.summary.contains("verify="));
        assert!(!entry.summary.contains("done_when="));
        assert!(!entry.summary.contains("max_tokens="));
    }

    #[test]
    fn parse_plan_tranche_header_line_extracts_contract_metadata() {
        let line = r#"5. NXT-042 `Record tranche tokens`: incomplete. tranche_group=core verify="cargo test -p yarli tranche_tokens" done_when="CLI shows per-tranche totals" max_tokens=70000 allowed_paths=crates/yarli-cli/src/plan.rs,crates/yarli-cli/src/render.rs"#;
        let entry = crate::plan::parse_plan_tranche_header_line(line).expect("should parse");
        assert_eq!(entry.key, "NXT-042");
        assert_eq!(entry.tranche_group.as_deref(), Some("core"));
        assert_eq!(
            entry.verify.as_deref(),
            Some("cargo test -p yarli tranche_tokens")
        );
        assert_eq!(
            entry.done_when.as_deref(),
            Some("CLI shows per-tranche totals")
        );
        assert_eq!(entry.max_tokens, Some(70_000));
        assert_eq!(
            entry.allowed_paths,
            vec![
                "crates/yarli-cli/src/plan.rs".to_string(),
                "crates/yarli-cli/src/render.rs".to_string()
            ]
        );
    }

    #[test]
    fn plan_guard_preflight_allows_completed_implement_target_for_auto_verify() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "implement"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();

        let context = run_spec_plan_guard_preflight_with_override(&loaded, false).unwrap();
        assert!(context.is_some());
    }

    #[test]
    fn plan_guard_preflight_allows_completed_target_with_verify_only_override() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "implement"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();

        let context = run_spec_plan_guard_preflight_with_override(&loaded, true)
            .expect("override should allow");
        assert!(context.is_some());
    }

    #[test]
    fn plan_guard_verify_only_requires_verification_objective_text() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "verify-only"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();

        let err = run_spec_plan_guard_preflight_with_override(&loaded, false).unwrap_err();
        assert!(err.to_string().contains("requires objective text"));
    }

    #[test]
    fn plan_guard_post_run_allows_same_prompt_after_completion() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "implement"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let context = run_spec_plan_guard_preflight_with_override(&loaded, false)
            .expect("preflight should pass");
        let context = context.expect("context should be present");

        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();

        enforce_plan_guard_post_run(&loaded, &context).unwrap();
    }

    // -----------------------------------------------------------------------
    // Structured tranches file tests
    // -----------------------------------------------------------------------

    #[test]
    fn tranches_file_toml_roundtrip() {
        let tf = TranchesFile {
            version: 1,
            tranches: vec![
                TrancheDefinition {
                    key: "TP-05".to_string(),
                    summary: "Config loader".to_string(),
                    status: TrancheStatus::Incomplete,
                    group: Some("runtime".to_string()),
                    allowed_paths: vec!["src/main.rs".to_string(), "docs/".to_string()],
                    verify: None,
                    done_when: None,
                    max_tokens: None,
                },
                TrancheDefinition {
                    key: "TP-06".to_string(),
                    summary: "Validation pass".to_string(),
                    status: TrancheStatus::Complete,
                    group: None,
                    allowed_paths: vec![],
                    verify: None,
                    done_when: None,
                    max_tokens: None,
                },
                TrancheDefinition {
                    key: "TP-07".to_string(),
                    summary: "Blocked work".to_string(),
                    status: TrancheStatus::Blocked,
                    group: None,
                    allowed_paths: vec![],
                    verify: None,
                    done_when: None,
                    max_tokens: None,
                },
            ],
        };

        let serialized = toml::to_string_pretty(&tf).unwrap();
        let deserialized: TranchesFile = toml::from_str(&serialized).unwrap();
        assert_eq!(tf, deserialized);
    }

    #[test]
    fn tranches_file_to_entries_conversion() {
        let tf = TranchesFile {
            version: 1,
            tranches: vec![
                TrancheDefinition {
                    key: "TP-05".to_string(),
                    summary: "Config loader".to_string(),
                    status: TrancheStatus::Incomplete,
                    group: Some("runtime".to_string()),
                    allowed_paths: vec!["src/".to_string()],
                    verify: None,
                    done_when: None,
                    max_tokens: None,
                },
                TrancheDefinition {
                    key: "TP-06".to_string(),
                    summary: "Validation pass".to_string(),
                    status: TrancheStatus::Complete,
                    group: None,
                    allowed_paths: vec![],
                    verify: None,
                    done_when: None,
                    max_tokens: None,
                },
                TrancheDefinition {
                    key: "TP-07".to_string(),
                    summary: "Blocked item".to_string(),
                    status: TrancheStatus::Blocked,
                    group: Some("infra".to_string()),
                    allowed_paths: vec![],
                    verify: None,
                    done_when: None,
                    max_tokens: None,
                },
            ],
        };

        let entries = tf.to_entries();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].key, "TP-05");
        assert!(!entries[0].is_complete);
        assert_eq!(entries[0].tranche_group, Some("runtime".to_string()));
        assert_eq!(entries[0].allowed_paths, vec!["src/".to_string()]);

        assert_eq!(entries[1].key, "TP-06");
        assert!(entries[1].is_complete);
        assert_eq!(entries[1].tranche_group, None);

        // Blocked maps to is_complete = false
        assert_eq!(entries[2].key, "TP-07");
        assert!(!entries[2].is_complete);
        assert_eq!(entries[2].tranche_group, Some("infra".to_string()));
    }

    #[test]
    fn validate_tranche_definition_rejects_uuid() {
        let def = TrancheDefinition {
            key: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            summary: "Implement query cache eviction".to_string(),
            status: TrancheStatus::Incomplete,
            group: None,
            allowed_paths: vec![],
            verify: None,
            done_when: None,
            max_tokens: None,
        };
        let err = validate_tranche_definition(&def).unwrap_err();
        assert!(format!("{err}").contains("UUID"), "error: {err}");
    }

    #[test]
    fn validate_tranche_definition_rejects_reserved_prefix() {
        for prefix in &["run-abc1", "app-test2", "target-foo3"] {
            let def = TrancheDefinition {
                key: prefix.to_string(),
                summary: "Implement query cache eviction".to_string(),
                status: TrancheStatus::Incomplete,
                group: None,
                allowed_paths: vec![],
                verify: None,
                done_when: None,
                max_tokens: None,
            };
            let err = validate_tranche_definition(&def).unwrap_err();
            assert!(
                format!("{err}").contains("reserved prefix"),
                "key={prefix}, error: {err}"
            );
        }
    }

    #[test]
    fn validate_tranche_definition_rejects_alpha_only() {
        let def = TrancheDefinition {
            key: "configloader".to_string(),
            summary: "Reject invalid digit-only keys".to_string(),
            status: TrancheStatus::Incomplete,
            group: None,
            allowed_paths: vec![],
            verify: None,
            done_when: None,
            max_tokens: None,
        };
        let err = validate_tranche_definition(&def).unwrap_err();
        assert!(
            format!("{err}").contains("letters and digits"),
            "error: {err}"
        );
    }

    #[test]
    fn validate_tranche_definition_accepts_valid_keys() {
        for key in &["TP-05", "M3A", "CARD-001", "step2b"] {
            let def = TrancheDefinition {
                key: key.to_string(),
                summary: "Valid tranche definition here".to_string(),
                status: TrancheStatus::Incomplete,
                group: None,
                allowed_paths: vec![],
                verify: None,
                done_when: None,
                max_tokens: None,
            };
            assert!(
                validate_tranche_definition(&def).is_ok(),
                "key={key} should be valid"
            );
        }
    }

    #[test]
    fn validate_tranche_definition_rejects_empty_summary() {
        let def = TrancheDefinition {
            key: "TP-01".to_string(),
            summary: "".to_string(),
            status: TrancheStatus::Incomplete,
            group: None,
            allowed_paths: vec![],
            verify: None,
            done_when: None,
            max_tokens: None,
        };
        let err = validate_tranche_definition(&def).unwrap_err();
        assert!(
            format!("{err}").contains("must not be empty"),
            "error: {err}"
        );
    }

    #[test]
    fn validate_tranche_definition_rejects_short_summary() {
        for summary in &["Fix", "Do thing"] {
            let def = TrancheDefinition {
                key: "TP-01".to_string(),
                summary: summary.to_string(),
                status: TrancheStatus::Incomplete,
                group: None,
                allowed_paths: vec![],
                verify: None,
                done_when: None,
                max_tokens: None,
            };
            let err = validate_tranche_definition(&def).unwrap_err();
            assert!(
                format!("{err}").contains("too vague"),
                "summary='{summary}', error: {err}"
            );
        }
    }

    #[test]
    fn validate_tranche_definition_rejects_placeholder_phrases() {
        for phrase in &[
            "Implement feature details pending",
            "TBD work on module",
            "Todo implement the cache",
            "Fix later the parser bug",
        ] {
            let def = TrancheDefinition {
                key: "TP-01".to_string(),
                summary: phrase.to_string(),
                status: TrancheStatus::Incomplete,
                group: None,
                allowed_paths: vec![],
                verify: None,
                done_when: None,
                max_tokens: None,
            };
            let err = validate_tranche_definition(&def).unwrap_err();
            assert!(
                format!("{err}").contains("placeholder text"),
                "summary='{phrase}', error: {err}"
            );
        }
    }

    #[test]
    fn validate_tranche_definition_accepts_good_summaries() {
        for summary in &[
            "Implement query cache eviction in src/query/cache.rs",
            "Add retry logic for transient failures",
            "Wire OpenTelemetry spans into scheduler",
        ] {
            let def = TrancheDefinition {
                key: "TP-01".to_string(),
                summary: summary.to_string(),
                status: TrancheStatus::Incomplete,
                group: None,
                allowed_paths: vec![],
                verify: None,
                done_when: None,
                max_tokens: None,
            };
            assert!(
                validate_tranche_definition(&def).is_ok(),
                "summary='{summary}' should be valid"
            );
        }
    }

    #[test]
    fn build_plan_driven_prefers_structured_file() {
        let temp = TempDir::new().unwrap();
        let yarli_dir = temp.path().join(".yarli");
        std::fs::create_dir_all(&yarli_dir).unwrap();

        // Write a structured tranches file with one incomplete tranche
        let tf = TranchesFile {
            version: 1,
            tranches: vec![TrancheDefinition {
                key: "ST-01".to_string(),
                summary: "Structured tranche".to_string(),
                status: TrancheStatus::Incomplete,
                group: None,
                allowed_paths: vec![],
                verify: None,
                done_when: None,
                max_tokens: None,
            }],
        };
        let toml_content = toml::to_string_pretty(&tf).unwrap();
        std::fs::write(yarli_dir.join("tranches.toml"), &toml_content).unwrap();

        // Write an IMPLEMENTATION_PLAN.md with a DIFFERENT tranche key
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n- [ ] MD-99 Markdown-only tranche\n",
        )
        .unwrap();

        // chdir into the temp directory so read_tranches_file() finds the file
        let original_dir = std::env::current_dir().unwrap_or_else(|_| std::env::temp_dir());
        std::env::set_current_dir(temp.path()).unwrap();

        let result = read_tranches_file();
        let _ = std::env::set_current_dir(&original_dir);

        let tf_read = result.unwrap().expect("should find tranches file");
        let entries: Vec<ImplementationPlanEntry> = tf_read
            .to_entries()
            .into_iter()
            .filter(|e| !e.is_complete)
            .collect();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "ST-01");
        assert_eq!(entries[0].summary, "Structured tranche");
    }

    #[test]
    fn structured_fallback_to_markdown() {
        // When no tranches file exists, read_tranches_file() returns None and
        // discover_plan_dispatch_entries should still work independently.
        let temp = TempDir::new().unwrap();
        let original_dir = std::env::current_dir().unwrap_or_else(|_| std::env::temp_dir());
        std::env::set_current_dir(temp.path()).unwrap();

        let result = read_tranches_file();
        let _ = std::env::set_current_dir(&original_dir);

        assert!(
            result.unwrap().is_none(),
            "should return None when file missing"
        );

        // Verify markdown parsing still works independently
        let plan = "## Next Work Tranches\n1. FB-01 `Fallback tranche`: incomplete.\n";
        let entries = discover_plan_dispatch_entries(plan);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "FB-01");
        assert!(!entries[0].is_complete);
    }

    #[test]
    fn validate_structured_tranches_preflight_reports_parse_details() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n1. ST-01 `Structured`: incomplete.\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1
[[tranches]]
key = "ST-01"
summary = "Structured tranche"
status = incomplete
"#,
        )
        .unwrap();

        let err = validate_structured_tranches_preflight_for_prompt(&repo.path().join("PROMPT.md"))
            .unwrap_err();
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("structured tranches preflight failed"),
            "expected preflight context, got: {err_text}"
        );
        assert!(
            err_text.contains("line"),
            "expected parse position details, got: {err_text}"
        );
    }

    #[test]
    fn write_tranches_file_in_round_trips_without_parse_failures() {
        let repo = TempDir::new().unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();

        for idx in 0..64 {
            let tf = TranchesFile {
                version: 1,
                tranches: vec![TrancheDefinition {
                    key: format!("ST-{idx:02}"),
                    summary: format!("Structured tranche {idx}"),
                    status: if idx % 2 == 0 {
                        TrancheStatus::Incomplete
                    } else {
                        TrancheStatus::Complete
                    },
                    group: Some("core".to_string()),
                    allowed_paths: vec!["src/lib.rs".to_string()],
                    verify: None,
                    done_when: None,
                    max_tokens: None,
                }],
            };
            write_tranches_file_in(repo.path(), &tf).unwrap();
            let parsed = read_tranches_file_in(repo.path())
                .unwrap()
                .expect("tranches file should exist");
            assert_eq!(parsed.version, 1);
            assert_eq!(parsed.tranches.len(), 1);
        }
    }

    #[test]
    fn auto_complete_structured_tranche_marks_matching_key_using_prompt_root() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n1. ST-01 `Structured`: incomplete.\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "ST-01"
summary = "Structured tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let decoy_cwd = TempDir::new().unwrap();
        std::fs::create_dir_all(decoy_cwd.path().join(".yarli")).unwrap();
        std::fs::write(
            decoy_cwd.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "ST-01"
summary = "Decoy tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let plan = RunPlan {
            objective: "implement active plan [ST-01]".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: ".".to_string(),
            timeout_secs: 1,
            pace: None,
            prompt_snapshot: Some(prompt::PromptSnapshot {
                entry_path: repo.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            }),
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "ST-01".to_string(),
                objective: "implement active plan [ST-01]".to_string(),
                task_keys: vec!["tranche-001-ST-01".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        // Use a stable path for restore — current_dir() may be invalidated by
        // concurrent tests that drop TempDirs, so fall back to a known-stable path.
        let restore_dir = std::env::current_dir().unwrap_or_else(|_| std::env::temp_dir());
        std::env::set_current_dir(decoy_cwd.path()).unwrap();
        let updated = maybe_mark_current_structured_tranche_complete(&plan).unwrap();
        let _ = std::env::set_current_dir(&restore_dir);

        assert!(updated, "structured tranche should be marked complete");

        let repo_tf = read_tranches_file_in(repo.path()).unwrap().unwrap();
        assert_eq!(repo_tf.tranches.len(), 1);
        assert_eq!(repo_tf.tranches[0].status, TrancheStatus::Complete);

        let decoy_tf = read_tranches_file_in(decoy_cwd.path()).unwrap().unwrap();
        assert_eq!(decoy_tf.tranches.len(), 1);
        assert_eq!(decoy_tf.tranches[0].status, TrancheStatus::Incomplete);
    }

    #[test]
    fn auto_complete_structured_tranche_skips_verification_tranche() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n1. ST-01 `Structured`: incomplete.\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "ST-01"
summary = "Structured tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let plan = RunPlan {
            objective: "verification".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: ".".to_string(),
            timeout_secs: 1,
            pace: None,
            prompt_snapshot: Some(prompt::PromptSnapshot {
                entry_path: repo.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            }),
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "verification".to_string(),
                objective: "verification".to_string(),
                task_keys: vec!["verification-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let updated = maybe_mark_current_structured_tranche_complete(&plan).unwrap();
        assert!(
            !updated,
            "verification tranche should not alter structured file"
        );

        let repo_tf = read_tranches_file_in(repo.path()).unwrap().unwrap();
        assert_eq!(repo_tf.tranches.len(), 1);
        assert_eq!(repo_tf.tranches[0].status, TrancheStatus::Incomplete);
    }

    #[test]
    fn auto_complete_structured_tranche_skips_plain_prompt_tranche() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "ST-01"
summary = "Structured tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let plan = RunPlan {
            objective: "yarli run [prompt]".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: ".".to_string(),
            timeout_secs: 1,
            pace: None,
            prompt_snapshot: Some(prompt::PromptSnapshot {
                entry_path: repo.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            }),
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "prompt".to_string(),
                objective: "yarli run [prompt]".to_string(),
                task_keys: vec!["prompt-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let updated = maybe_mark_current_structured_tranche_complete(&plan).unwrap();
        assert!(
            !updated,
            "plain prompt tranche should not alter structured file"
        );

        let repo_tf = read_tranches_file_in(repo.path()).unwrap().unwrap();
        assert_eq!(repo_tf.tranches.len(), 1);
        assert_eq!(repo_tf.tranches[0].status, TrancheStatus::Incomplete);
    }

    // ---- run_tranche_verify_command tests ----

    #[test]
    fn verify_command_passes_with_no_verify_field() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n1. VT-01 `Verify test`: incomplete.\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "VT-01"
summary = "Verify test tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let plan = RunPlan {
            objective: "verify test".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: repo.path().display().to_string(),
            timeout_secs: 10,
            pace: None,
            prompt_snapshot: Some(prompt::PromptSnapshot {
                entry_path: repo.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            }),
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "VT-01".to_string(),
                objective: "verify test".to_string(),
                task_keys: vec!["task-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let passed = run_tranche_verify_command(&plan, repo.path(), 10).unwrap();
        assert!(passed, "no verify field should default to pass");
    }

    #[test]
    fn verify_command_passes_when_exit_zero() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n1. VT-02 `Verify pass`: incomplete.\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "VT-02"
summary = "Verify pass tranche"
status = "incomplete"
verify = "true"
"#,
        )
        .unwrap();

        let plan = RunPlan {
            objective: "verify pass".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: repo.path().display().to_string(),
            timeout_secs: 10,
            pace: None,
            prompt_snapshot: Some(prompt::PromptSnapshot {
                entry_path: repo.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            }),
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "VT-02".to_string(),
                objective: "verify pass".to_string(),
                task_keys: vec!["task-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let passed = run_tranche_verify_command(&plan, repo.path(), 10).unwrap();
        assert!(passed, "exit 0 should pass verification");
    }

    #[test]
    fn verify_command_fails_when_exit_nonzero() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n1. VT-03 `Verify fail`: incomplete.\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "VT-03"
summary = "Verify fail tranche"
status = "incomplete"
verify = "false"
"#,
        )
        .unwrap();

        let plan = RunPlan {
            objective: "verify fail".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: repo.path().display().to_string(),
            timeout_secs: 10,
            pace: None,
            prompt_snapshot: Some(prompt::PromptSnapshot {
                entry_path: repo.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            }),
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "VT-03".to_string(),
                objective: "verify fail".to_string(),
                task_keys: vec!["task-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let passed = run_tranche_verify_command(&plan, repo.path(), 10).unwrap();
        assert!(!passed, "exit non-zero should fail verification");
    }

    #[test]
    fn verify_command_skips_for_prompt_key() {
        let plan = RunPlan {
            objective: "prompt run".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: ".".to_string(),
            timeout_secs: 10,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "prompt".to_string(),
                objective: "prompt run".to_string(),
                task_keys: vec!["task-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let passed = run_tranche_verify_command(&plan, Path::new("."), 10).unwrap();
        assert!(passed, "prompt key should skip and default to pass");
    }

    #[test]
    fn verify_command_skips_for_no_prompt_snapshot() {
        let plan = RunPlan {
            objective: "no snapshot".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: ".".to_string(),
            timeout_secs: 10,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "NXT-01".to_string(),
                objective: "no snapshot".to_string(),
                task_keys: vec!["task-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let passed = run_tranche_verify_command(&plan, Path::new("."), 10).unwrap();
        assert!(passed, "no prompt snapshot should default to pass");
    }

    #[test]
    fn verify_command_skips_when_no_tranches_file() {
        let repo = TempDir::new().unwrap();
        std::fs::write(repo.path().join("PROMPT.md"), "# prompt\n").unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "## Next Work Tranches\n1. VT-06 `No file`: incomplete.\n",
        )
        .unwrap();
        // Intentionally no .yarli/tranches.toml

        let plan = RunPlan {
            objective: "no tranches file".to_string(),
            tasks: Vec::new(),
            task_catalog: Vec::new(),
            workdir: repo.path().display().to_string(),
            timeout_secs: 10,
            pace: None,
            prompt_snapshot: Some(prompt::PromptSnapshot {
                entry_path: repo.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            }),
            run_spec: None,
            tranche_plan: vec![PlannedTranche {
                key: "VT-06".to_string(),
                objective: "no tranches file".to_string(),
                task_keys: vec!["task-001".to_string()],
                tranche_group: None,
            }],
            current_tranche_index: Some(0),
        };

        let passed = run_tranche_verify_command(&plan, repo.path(), 10).unwrap();
        assert!(passed, "missing tranches file should default to pass");
    }
}
