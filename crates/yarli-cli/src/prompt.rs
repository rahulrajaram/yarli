//! Prompt loading and run-spec parsing for `yarli run`.
//!
//! Opinionated rules:
//! - Default fallback entrypoint is `PROMPT.md` (resolved by walking up from CWD).
//! - `PROMPT.md` may contain `@include <path>` directives (repo-confined).
//! - A single fenced code block with info string `yarli-run` defines what to execute.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PROMPT_FILENAME: &str = "PROMPT.md";
const MAX_INCLUDE_DEPTH: usize = 8;
const MAX_EXPANDED_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSpec {
    pub version: u32,
    pub objective: Option<String>,
    #[serde(default)]
    pub tasks: RunSpecTasks,
    #[serde(default)]
    pub tranches: Option<RunSpecTranches>,
    #[serde(default)]
    pub plan_guard: Option<RunSpecPlanGuard>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RunSpecTasks {
    #[serde(default)]
    pub items: Vec<RunSpecTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSpecTask {
    pub key: String,
    pub cmd: String,
    #[serde(default)]
    pub class: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSpecTranches {
    pub items: Vec<RunSpecTranche>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSpecTranche {
    pub key: String,
    #[serde(default)]
    pub objective: Option<String>,
    pub task_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSpecPlanGuard {
    pub target: String,
    #[serde(default)]
    pub mode: RunSpecPlanGuardMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RunSpecPlanGuardMode {
    Implement,
    VerifyOnly,
}

impl Default for RunSpecPlanGuardMode {
    fn default() -> Self {
        Self::Implement
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptSnapshot {
    pub entry_path: String,
    pub expanded_sha256: String,
    pub included_files: Vec<PromptFileHash>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptFileHash {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct LoadedPrompt {
    pub entry_path: PathBuf,
    pub expanded_text: String,
    pub snapshot: PromptSnapshot,
    pub run_spec: RunSpec,
}

#[derive(Debug, Clone)]
pub struct LoadedPromptOptionalRunSpec {
    pub entry_path: PathBuf,
    pub expanded_text: String,
    pub snapshot: PromptSnapshot,
    pub run_spec: Option<RunSpec>,
}

pub fn load_prompt_and_run_spec_from_cwd() -> Result<LoadedPrompt> {
    let entry_path =
        find_prompt_upwards(std::env::current_dir()?).context("failed to resolve PROMPT.md")?;
    load_prompt_and_run_spec(&entry_path)
}

pub fn find_prompt_upwards(mut dir: PathBuf) -> Result<PathBuf> {
    loop {
        let candidate = dir.join(PROMPT_FILENAME);
        if candidate.exists() {
            return Ok(candidate);
        }
        if !dir.pop() {
            bail!(
                "{} not found (run from repo root or create PROMPT.md)",
                PROMPT_FILENAME
            );
        }
    }
}

pub fn load_prompt_and_run_spec(entry_prompt_path: &Path) -> Result<LoadedPrompt> {
    let loaded = load_prompt_with_optional_run_spec(entry_prompt_path)?;
    let Some(run_spec) = loaded.run_spec else {
        bail!("PROMPT.md must contain exactly one ```yarli-run fenced block (found 0)");
    };

    Ok(LoadedPrompt {
        entry_path: loaded.entry_path,
        expanded_text: loaded.expanded_text,
        snapshot: loaded.snapshot,
        run_spec,
    })
}

pub fn load_prompt_with_optional_run_spec(
    entry_prompt_path: &Path,
) -> Result<LoadedPromptOptionalRunSpec> {
    let (entry_prompt_path, expanded, snapshot) = load_expanded_prompt(entry_prompt_path)?;
    let run_spec = parse_run_spec_block(&expanded, false)?;
    Ok(LoadedPromptOptionalRunSpec {
        entry_path: entry_prompt_path,
        expanded_text: expanded,
        snapshot,
        run_spec,
    })
}

fn load_expanded_prompt(entry_prompt_path: &Path) -> Result<(PathBuf, String, PromptSnapshot)> {
    let entry_prompt_path = entry_prompt_path
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", entry_prompt_path.display()))?;
    let base_dir = entry_prompt_path
        .parent()
        .context("PROMPT.md has no parent directory")?
        .to_path_buf();

    let mut included_hashes: BTreeMap<PathBuf, String> = BTreeMap::new();
    let mut visiting: BTreeSet<PathBuf> = BTreeSet::new();
    let mut expanded = String::new();
    expand_file(
        &base_dir,
        &entry_prompt_path,
        0,
        &mut visiting,
        &mut included_hashes,
        &mut expanded,
    )?;

    if expanded.len() > MAX_EXPANDED_BYTES {
        bail!(
            "expanded prompt exceeds max size ({} > {} bytes)",
            expanded.len(),
            MAX_EXPANDED_BYTES
        );
    }

    let expanded_sha256 = sha256_hex(expanded.as_bytes());
    let entry_path_display = entry_prompt_path.display().to_string();
    let included_files = included_hashes
        .into_iter()
        .map(|(path, sha)| PromptFileHash {
            path: path.display().to_string(),
            sha256: sha,
        })
        .collect::<Vec<_>>();

    Ok((
        entry_prompt_path,
        expanded,
        PromptSnapshot {
            entry_path: entry_path_display,
            expanded_sha256,
            included_files,
        },
    ))
}

fn parse_run_spec_block(expanded: &str, require_block: bool) -> Result<Option<RunSpec>> {
    let run_spec_blocks = extract_fenced_blocks(expanded, "yarli-run");
    if run_spec_blocks.is_empty() {
        if require_block {
            bail!("PROMPT.md must contain exactly one ```yarli-run fenced block (found 0)");
        }
        return Ok(None);
    }
    if run_spec_blocks.len() != 1 {
        bail!(
            "PROMPT.md must contain exactly one ```yarli-run fenced block (found {})",
            run_spec_blocks.len()
        );
    }

    let run_spec: RunSpec = toml::from_str(&run_spec_blocks[0])
        .context("failed to parse TOML in ```yarli-run block")?;
    validate_run_spec(&run_spec)?;
    Ok(Some(run_spec))
}

pub fn validate_run_spec(run_spec: &RunSpec) -> Result<()> {
    // Validate uniqueness and required fields.
    if run_spec.version != 1 {
        bail!(
            "unsupported run spec version {} (expected 1)",
            run_spec.version
        );
    }
    let mut keys = BTreeSet::new();
    for task in &run_spec.tasks.items {
        if task.key.trim().is_empty() {
            bail!("run spec task.key must be non-empty");
        }
        if task.cmd.trim().is_empty() {
            bail!(
                "run spec task.cmd must be non-empty (task key {})",
                task.key
            );
        }
        if !keys.insert(task.key.clone()) {
            bail!("duplicate task key in run spec: {}", task.key);
        }
    }

    validate_run_spec_task_dependencies(&run_spec.tasks.items)?;

    if let Some(tranches) = run_spec.tranches.as_ref() {
        if tranches.items.is_empty() {
            bail!("run spec tranches.items must be non-empty when [tranches] is present");
        }
        let mut tranche_keys = BTreeSet::new();
        let mut referenced = BTreeSet::new();
        for tranche in &tranches.items {
            if tranche.key.trim().is_empty() {
                bail!("run spec tranche.key must be non-empty");
            }
            if !tranche_keys.insert(tranche.key.clone()) {
                bail!("duplicate tranche key in run spec: {}", tranche.key);
            }
            if tranche.task_keys.is_empty() {
                bail!(
                    "run spec tranche.task_keys must be non-empty (tranche key {})",
                    tranche.key
                );
            }
            for task_key in &tranche.task_keys {
                if !keys.contains(task_key) {
                    bail!(
                        "run spec tranche {} references unknown task key: {}",
                        tranche.key,
                        task_key
                    );
                }
                if !referenced.insert(task_key.clone()) {
                    bail!(
                        "run spec task key appears in multiple tranches: {}",
                        task_key
                    );
                }
            }
        }
    }

    if let Some(plan_guard) = run_spec.plan_guard.as_ref() {
        if plan_guard.target.trim().is_empty() {
            bail!("run spec plan_guard.target must be non-empty");
        }
    }
    Ok(())
}

fn validate_run_spec_task_dependencies(tasks: &[RunSpecTask]) -> Result<()> {
    let available_keys: BTreeSet<_> = tasks.iter().map(|task| task.key.as_str()).collect();
    let mut dependency_graph: HashMap<&str, Vec<&str>> = HashMap::new();

    for task in tasks {
        let task_key = task.key.trim();
        if task_key.is_empty() {
            continue;
        }

        let mut normalized_deps = Vec::new();
        for dep in &task.depends_on {
            let dep = dep.trim();
            if dep.is_empty() {
                bail!("run spec task {} has empty depends_on entry", task.key);
            }
            if dep == task_key {
                bail!("run spec task {} cannot depend on itself", task.key);
            }
            if !available_keys.contains(dep) {
                bail!(
                    "run spec task {} depends on unknown task key: {}",
                    task.key,
                    dep
                );
            }
            normalized_deps.push(dep);
        }
        dependency_graph.insert(task_key, normalized_deps);
    }

    let mut visited = BTreeSet::new();
    let mut visiting_stack = Vec::new();
    let mut visiting_lookup = BTreeSet::new();

    for task_key in available_keys {
        if !visited.contains(task_key) {
            detect_task_dependency_cycle(task_key, &dependency_graph, &mut visited, &mut visiting_stack, &mut visiting_lookup)?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn detect_task_dependency_cycle<'a>(
    task_key: &'a str,
    dependency_graph: &HashMap<&str, Vec<&'a str>>,
    visited: &mut BTreeSet<&'a str>,
    visiting_stack: &mut Vec<&'a str>,
    visiting_lookup: &mut BTreeSet<&'a str>,
) -> Result<()> {
    if visited.contains(task_key) {
        return Ok(());
    }
    if visiting_lookup.contains(task_key) {
        let cycle_start = visiting_stack
            .iter()
            .position(|value| *value == task_key)
            .unwrap_or(0);
        let cycle: Vec<&str> = visiting_stack[cycle_start..]
            .iter()
            .chain(std::iter::once(&task_key))
            .copied()
            .collect();
        bail!("run spec has cyclic task dependency: {}", cycle.join(" -> "));
    }

    visiting_lookup.insert(task_key);
    visiting_stack.push(task_key);

    for dep in dependency_graph.get(task_key).into_iter().flatten() {
        detect_task_dependency_cycle(dep, dependency_graph, visited, visiting_stack, visiting_lookup)?;
    }

    visiting_lookup.remove(task_key);
    visiting_stack.pop();
    visited.insert(task_key);
    Ok(())
}

fn expand_file(
    base_dir: &Path,
    file_path: &Path,
    depth: usize,
    visiting: &mut BTreeSet<PathBuf>,
    included_hashes: &mut BTreeMap<PathBuf, String>,
    out: &mut String,
) -> Result<()> {
    if depth > MAX_INCLUDE_DEPTH {
        bail!(
            "include depth exceeded (>{}) at {}",
            MAX_INCLUDE_DEPTH,
            file_path.display()
        );
    }

    let file_path = file_path
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", file_path.display()))?;
    ensure_repo_confined(base_dir, &file_path)?;

    if !visiting.insert(file_path.clone()) {
        bail!("include cycle detected at {}", file_path.display());
    }

    let raw = fs::read_to_string(&file_path)
        .with_context(|| format!("failed to read {}", file_path.display()))?;
    included_hashes.insert(file_path.clone(), sha256_hex(raw.as_bytes()));

    // Write a deterministic boundary marker so the expanded prompt is auditable.
    out.push_str(&format!(
        "\n<!-- BEGIN_INCLUDE path={} sha256={} -->\n",
        file_path.display(),
        sha256_hex(raw.as_bytes())
    ));

    for line in raw.lines() {
        if let Some(path) = parse_include_line(line) {
            let include_path = resolve_include_path(base_dir, &file_path, &path)?;
            expand_file(
                base_dir,
                &include_path,
                depth + 1,
                visiting,
                included_hashes,
                out,
            )?;
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }

    out.push_str(&format!(
        "<!-- END_INCLUDE path={} -->\n",
        file_path.display()
    ));

    visiting.remove(&file_path);
    Ok(())
}

fn parse_include_line(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("@include")?.trim();
    if rest.is_empty() {
        return None;
    }
    Some(rest.to_string())
}

fn resolve_include_path(base_dir: &Path, current_file: &Path, include: &str) -> Result<PathBuf> {
    let include_path = Path::new(include);
    if include_path.is_absolute() {
        bail!("absolute include paths are not allowed: {include}");
    }

    // Resolve relative to the directory of the including file.
    let parent_dir = current_file
        .parent()
        .context("including file has no parent directory")?;
    let candidate = parent_dir.join(include_path);

    // Reject obvious traversal segments even before canonicalize (friendlier errors).
    for comp in include_path.components() {
        if matches!(
            comp,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            bail!("include path escape is not allowed: {include}");
        }
    }

    let canonical = candidate.canonicalize().with_context(|| {
        format!(
            "failed to resolve include {include} from {}",
            current_file.display()
        )
    })?;
    ensure_repo_confined(base_dir, &canonical)?;
    Ok(canonical)
}

fn ensure_repo_confined(repo_root: &Path, candidate: &Path) -> Result<()> {
    // Confine includes to the directory containing the entry PROMPT.md and its descendants.
    let repo_root = repo_root
        .canonicalize()
        .with_context(|| format!("failed to canonicalize repo root {}", repo_root.display()))?;
    if !candidate.starts_with(&repo_root) {
        bail!(
            "include path escapes repo root ({}): {}",
            repo_root.display(),
            candidate.display()
        );
    }
    Ok(())
}

fn extract_fenced_blocks(markdown: &str, info: &str) -> Vec<String> {
    // Minimal, deterministic fence parser. Accepts only triple-backtick fences.
    let mut blocks = Vec::new();
    let mut in_block = false;
    let mut current = String::new();
    let mut matches_info = false;

    for line in markdown.lines() {
        if !in_block {
            if let Some(rest) = line.trim_start().strip_prefix("```") {
                let tag = rest.trim();
                in_block = true;
                matches_info = tag == info;
                current.clear();
            }
            continue;
        }

        if line.trim_start().starts_with("```") {
            if matches_info {
                blocks.push(current.clone());
            }
            in_block = false;
            matches_info = false;
            current.clear();
            continue;
        }

        if matches_info {
            current.push_str(line);
            current.push('\n');
        }
    }

    blocks
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn extracts_single_run_spec_block() {
        let md = r#"
hello
```yarli-run
version = 1
objective = "x"
[tasks]
items = [{ key = "a", cmd = "echo ok" }]
```
"#;
        let blocks = extract_fenced_blocks(md, "yarli-run");
        assert_eq!(blocks.len(), 1);
        let spec: RunSpec = toml::from_str(&blocks[0]).unwrap();
        assert_eq!(spec.version, 1);
        assert_eq!(spec.tasks.items.len(), 1);
    }

    #[test]
    fn include_expands_and_is_confined() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        fs::write(root.join("PROMPT.md"), "@include sub/plan.md\n").unwrap();
        fs::create_dir_all(root.join("sub")).unwrap();
        fs::write(
            root.join("sub/plan.md"),
            "```yarli-run\nversion = 1\n[tasks]\nitems=[{key=\"a\",cmd=\"echo ok\"}]\n```\n",
        )
        .unwrap();

        let loaded = load_prompt_and_run_spec(&root.join("PROMPT.md")).unwrap();
        assert!(loaded.expanded_text.contains("BEGIN_INCLUDE"));
        assert_eq!(loaded.run_spec.tasks.items[0].key, "a");
        assert_eq!(loaded.snapshot.included_files.len(), 2);
    }

    #[test]
    fn optional_loader_accepts_plain_prompt_without_run_spec_block() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        fs::write(root.join("PROMPT.md"), "# plain prompt\nDo the work.\n").unwrap();

        let loaded = load_prompt_with_optional_run_spec(&root.join("PROMPT.md")).unwrap();
        assert!(loaded.run_spec.is_none());
        assert!(loaded.expanded_text.contains("plain prompt"));
    }

    #[test]
    fn strict_loader_still_rejects_missing_run_spec_block() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        fs::write(root.join("PROMPT.md"), "# plain prompt\nNo fenced block.\n").unwrap();

        let err = load_prompt_and_run_spec(&root.join("PROMPT.md")).unwrap_err();
        assert!(err
            .to_string()
            .contains("must contain exactly one ```yarli-run fenced block (found 0)"));
    }

    #[test]
    fn rejects_multiple_run_spec_blocks() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        fs::write(
            root.join("PROMPT.md"),
            "```yarli-run\nversion=1\n[tasks]\nitems=[{key=\"a\",cmd=\"echo ok\"}]\n```\n```yarli-run\nversion=1\n[tasks]\nitems=[{key=\"b\",cmd=\"echo ok\"}]\n```\n",
        )
        .unwrap();
        let err = load_prompt_and_run_spec(&root.join("PROMPT.md")).unwrap_err();
        assert!(err.to_string().contains("exactly one"));
    }

    #[test]
    fn accepts_explicit_tranche_sequence() {
        let md = r#"
```yarli-run
version = 1
objective = "x"
[tasks]
items = [
  { key = "a", cmd = "echo a" },
  { key = "b", cmd = "echo b" },
]
[tranches]
items = [
  { key = "first", task_keys = ["a"] },
  { key = "second", objective = "second pass", task_keys = ["b"] },
]
```
"#;
        let blocks = extract_fenced_blocks(md, "yarli-run");
        let spec: RunSpec = toml::from_str(&blocks[0]).unwrap();
        assert_eq!(spec.tranches.as_ref().unwrap().items.len(), 2);
    }

    #[test]
    fn rejects_duplicate_tranche_task_keys() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        fs::write(
            root.join("PROMPT.md"),
            "```yarli-run\nversion=1\n[tasks]\nitems=[{key=\"a\",cmd=\"echo ok\"},{key=\"b\",cmd=\"echo ok\"}]\n[tranches]\nitems=[{key=\"t1\",task_keys=[\"a\"]},{key=\"t2\",task_keys=[\"a\",\"b\"]}]\n```\n",
        )
        .unwrap();
        let err = load_prompt_and_run_spec(&root.join("PROMPT.md")).unwrap_err();
        assert!(err.to_string().contains("appears in multiple tranches"));
    }

    #[test]
    fn parses_plan_guard_verify_only_mode() {
        let md = r#"
```yarli-run
version = 1
objective = "verification-only: CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "cargo test --workspace" }]
[plan_guard]
target = "CARD-R8-01"
mode = "verify-only"
```
"#;
        let blocks = extract_fenced_blocks(md, "yarli-run");
        let spec: RunSpec = toml::from_str(&blocks[0]).unwrap();
        let guard = spec.plan_guard.expect("plan_guard should parse");
        assert_eq!(guard.target, "CARD-R8-01");
        assert_eq!(guard.mode, RunSpecPlanGuardMode::VerifyOnly);
    }

    #[test]
    fn rejects_empty_plan_guard_target() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        fs::write(
            root.join("PROMPT.md"),
            "```yarli-run\nversion=1\n[tasks]\nitems=[{key=\"a\",cmd=\"echo ok\"}]\n[plan_guard]\ntarget=\"\"\n```\n",
        )
        .unwrap();
        let err = load_prompt_and_run_spec(&root.join("PROMPT.md")).unwrap_err();
        assert!(err.to_string().contains("plan_guard.target"));
    }

    #[test]
    fn accepts_minimal_run_spec_without_tasks_for_config_first_mode() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        fs::write(
            root.join("PROMPT.md"),
            "```yarli-run\nversion=1\nobjective=\"implement plan\"\n```\n",
        )
        .unwrap();

        let loaded = load_prompt_and_run_spec(&root.join("PROMPT.md")).unwrap();
        assert_eq!(loaded.run_spec.version, 1);
        assert!(loaded.run_spec.tasks.items.is_empty());
    }
}
