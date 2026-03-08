use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedCommitMessage {
    pub subject: String,
    pub body: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffSpec<'a> {
    Staged,
    Range { base: &'a str, head: &'a str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeStatus {
    Added,
    Modified,
    Deleted,
    Renamed,
    TypeChanged,
    Unmerged,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChangedPath {
    path: PathBuf,
    status: ChangeStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathCategory {
    State,
    Docs,
    Tests,
    Ci,
    Build,
    Config,
    Code,
    Unknown,
}

pub fn generate_commit_message(
    repo: &Path,
    diff: DiffSpec<'_>,
    metadata: &[(String, String)],
    fallback_subject: &str,
    fallback_scope: Option<&str>,
) -> GeneratedCommitMessage {
    let subject = collect_changed_paths(repo, diff)
        .ok()
        .and_then(|paths| infer_subject(&paths, fallback_scope))
        .unwrap_or_else(|| fallback_subject.to_string());

    let body = if metadata.is_empty() {
        None
    } else {
        Some(
            metadata
                .iter()
                .map(|(key, value)| format!("{key}: {value}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )
    };

    GeneratedCommitMessage { subject, body }
}

pub fn render_commit_message(message: &GeneratedCommitMessage) -> String {
    match message.body.as_deref() {
        Some(body) if !body.trim().is_empty() => format!("{}\n\n{}", message.subject, body),
        _ => message.subject.clone(),
    }
}

fn collect_changed_paths(repo: &Path, diff: DiffSpec<'_>) -> std::io::Result<Vec<ChangedPath>> {
    let mut args = vec![
        "diff".to_string(),
        "--name-status".to_string(),
        "--find-renames".to_string(),
        "--diff-filter=ACDMRTUXB".to_string(),
    ];
    match diff {
        DiffSpec::Staged => args.push("--cached".to_string()),
        DiffSpec::Range { base, head } => args.push(format!("{base}..{head}")),
    }

    let output = Command::new("git").args(&args).current_dir(repo).output()?;
    if !output.status.success() {
        return Ok(Vec::new());
    }

    Ok(parse_name_status(&String::from_utf8_lossy(&output.stdout)))
}

fn parse_name_status(text: &str) -> Vec<ChangedPath> {
    text.lines()
        .filter_map(|line| {
            let mut parts = line.split('\t');
            let status = parts.next()?;
            let status = parse_status(status);
            let first_path = parts.next()?;
            let path = if status == ChangeStatus::Renamed {
                parts.next().unwrap_or(first_path)
            } else {
                first_path
            };
            Some(ChangedPath {
                path: PathBuf::from(path),
                status,
            })
        })
        .collect()
}

fn parse_status(raw: &str) -> ChangeStatus {
    match raw.chars().next().unwrap_or('M') {
        'A' => ChangeStatus::Added,
        'M' => ChangeStatus::Modified,
        'D' => ChangeStatus::Deleted,
        'R' => ChangeStatus::Renamed,
        'T' => ChangeStatus::TypeChanged,
        'U' => ChangeStatus::Unmerged,
        _ => ChangeStatus::Unknown,
    }
}

fn infer_subject(paths: &[ChangedPath], fallback_scope: Option<&str>) -> Option<String> {
    if paths.is_empty() {
        return None;
    }

    if paths
        .iter()
        .all(|path| classify_path(&path.path) == PathCategory::State)
    {
        return Some("chore(state): checkpoint runtime state".to_string());
    }

    let scope = infer_scope(paths, fallback_scope);
    let categories: Vec<_> = paths.iter().map(|path| classify_path(&path.path)).collect();
    let all_docs = categories
        .iter()
        .all(|category| *category == PathCategory::Docs);
    let all_tests = categories
        .iter()
        .all(|category| *category == PathCategory::Tests);
    let all_ci = categories
        .iter()
        .all(|category| *category == PathCategory::Ci);
    let all_build = categories
        .iter()
        .all(|category| *category == PathCategory::Build);
    let all_config = categories
        .iter()
        .all(|category| *category == PathCategory::Config);
    let any_added = paths.iter().any(|path| path.status == ChangeStatus::Added);
    let only_deleted = paths
        .iter()
        .all(|path| path.status == ChangeStatus::Deleted);
    let only_renamed = paths
        .iter()
        .all(|path| path.status == ChangeStatus::Renamed);

    if all_docs {
        return Some(format!("docs({scope}): update documentation"));
    }
    if all_tests {
        return Some(format!("test({scope}): update coverage"));
    }
    if all_ci {
        return Some(format!("ci({scope}): update automation"));
    }
    if all_build {
        return Some(format!("build({scope}): update build tooling"));
    }
    if all_config {
        return Some(format!("chore({scope}): update configuration"));
    }

    let target = if paths.len() == 1 {
        describe_target(&paths[0].path)
    } else {
        humanize_token(&scope)
    };

    if only_deleted {
        return Some(format!("refactor({scope}): remove {target}"));
    }
    if only_renamed {
        return Some(format!("refactor({scope}): reorganize {target}"));
    }
    if any_added {
        let action = if paths.len() == 1 {
            format!("add {target}")
        } else {
            format!("update {target}")
        };
        return Some(format!("feat({scope}): {action}"));
    }

    Some(format!("fix({scope}): update {target}"))
}

fn infer_scope(paths: &[ChangedPath], fallback_scope: Option<&str>) -> String {
    let mut counts = BTreeMap::<String, usize>::new();
    for path in paths {
        let candidate = path_scope_candidate(&path.path);
        *counts.entry(candidate).or_default() += 1;
    }

    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(scope, _)| scope)
        .or_else(|| fallback_scope.map(ToString::to_string))
        .unwrap_or_else(|| "repo".to_string())
}

fn path_scope_candidate(path: &Path) -> String {
    let parts: Vec<_> = path
        .iter()
        .map(|segment| segment.to_string_lossy().to_string())
        .collect();

    if parts.is_empty() {
        return "repo".to_string();
    }

    if parts.first().is_some_and(|part| part == ".yarli") {
        return "state".to_string();
    }

    if parts.first().is_some_and(|part| part == "crates") {
        if parts.len() >= 4 {
            let file_stem = Path::new(&parts[3])
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("repo");
            if !is_generic_token(file_stem) {
                return sanitize_scope(file_stem);
            }
        }
        if parts.len() >= 2 {
            return sanitize_scope(parts[1].trim_start_matches("yarli-"));
        }
    }

    if parts.first().is_some_and(|part| part == "docs") {
        if parts.len() >= 2 {
            let stem = Path::new(&parts[1])
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("docs");
            if !is_generic_token(stem) {
                return sanitize_scope(stem);
            }
        }
        return "docs".to_string();
    }

    if parts.first().is_some_and(|part| part == "tests") {
        if parts.len() >= 3 {
            return sanitize_scope(&parts[2]);
        }
        return "tests".to_string();
    }

    if parts.first().is_some_and(|part| part == "scripts") {
        if parts.len() >= 2 {
            let stem = Path::new(&parts[1])
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("scripts");
            return sanitize_scope(stem);
        }
        return "scripts".to_string();
    }

    if parts.first().is_some_and(|part| part == ".github") {
        return "ci".to_string();
    }

    if let Some(file_name) = path.file_stem().and_then(|name| name.to_str()) {
        if !is_generic_token(file_name) {
            return sanitize_scope(file_name);
        }
    }

    sanitize_scope(&parts[0])
}

fn classify_path(path: &Path) -> PathCategory {
    let parts: Vec<_> = path
        .iter()
        .map(|segment| segment.to_string_lossy().to_string())
        .collect();

    if parts.is_empty() {
        return PathCategory::Unknown;
    }

    if parts.first().is_some_and(|part| part == ".yarli") {
        return PathCategory::State;
    }

    if parts.first().is_some_and(|part| part == "docs")
        || path.extension().and_then(|ext| ext.to_str()) == Some("md")
    {
        return PathCategory::Docs;
    }

    if parts
        .iter()
        .any(|part| part == "tests" || part == "__tests__")
        || parts
            .last()
            .is_some_and(|name| name.ends_with("_test.rs") || name.ends_with(".snap"))
    {
        return PathCategory::Tests;
    }

    if parts.first().is_some_and(|part| part == ".github")
        || parts.starts_with(&["docs".to_string(), "ci".to_string()])
    {
        return PathCategory::Ci;
    }

    if parts
        .last()
        .is_some_and(|name| matches!(name.as_str(), "Cargo.toml" | "Cargo.lock" | "Dockerfile"))
    {
        return PathCategory::Build;
    }

    if parts.last().is_some_and(|name| {
        name.ends_with(".toml") || name.ends_with(".yml") || name.ends_with(".yaml")
    }) {
        return PathCategory::Config;
    }

    if parts.first().is_some_and(|part| part == "scripts") {
        return PathCategory::Build;
    }

    PathCategory::Code
}

fn describe_target(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !is_generic_token(value))
        .map(humanize_token);
    if let Some(stem) = stem {
        return stem;
    }

    path.parent()
        .and_then(|parent| parent.file_name())
        .and_then(|value| value.to_str())
        .map(humanize_token)
        .unwrap_or_else(|| "changes".to_string())
}

fn sanitize_scope(raw: &str) -> String {
    let cleaned = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();

    cleaned
        .trim_matches('-')
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn humanize_token(raw: &str) -> String {
    raw.replace(['_', '-', '.'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn is_generic_token(value: &str) -> bool {
    matches!(value, "mod" | "lib" | "main" | "index" | "readme")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_only_changes_become_checkpoint_subject() {
        let paths = vec![ChangedPath {
            path: PathBuf::from(".yarli/tranches.toml"),
            status: ChangeStatus::Modified,
        }];
        assert_eq!(
            infer_subject(&paths, None).as_deref(),
            Some("chore(state): checkpoint runtime state")
        );
    }

    #[test]
    fn single_added_code_file_becomes_feature_subject() {
        let paths = vec![ChangedPath {
            path: PathBuf::from("crates/yarli-cli/src/workspace.rs"),
            status: ChangeStatus::Added,
        }];
        assert_eq!(
            infer_subject(&paths, None).as_deref(),
            Some("feat(workspace): add workspace")
        );
    }

    #[test]
    fn docs_changes_use_docs_type() {
        let paths = vec![ChangedPath {
            path: PathBuf::from("docs/API_CONTRACT.md"),
            status: ChangeStatus::Modified,
        }];
        assert_eq!(
            infer_subject(&paths, None).as_deref(),
            Some("docs(api-contract): update documentation")
        );
    }

    #[test]
    fn render_commit_message_preserves_body() {
        let rendered = render_commit_message(&GeneratedCommitMessage {
            subject: "feat(workspace): add workspace".to_string(),
            body: Some("yarli-run: run-123".to_string()),
        });
        assert_eq!(
            rendered,
            "feat(workspace): add workspace\n\nyarli-run: run-123"
        );
    }
}
