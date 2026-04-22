use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

pub(crate) const EVIDENCE_DIR: &str = ".yarli/evidence";
const EVIDENCE_FILE_PREFIX: &str = "I";
const EVIDENCE_SCHEMA_VERSION: u32 = 1;
const REQUIRED_BODY_HEADINGS: [&str; 3] = ["## Summary", "## Changes", "## Verification"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EvidenceTaskType {
    Implementation,
    Verification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EvidenceStatus {
    Pass,
    Fail,
    Partial,
}

#[derive(Debug, Deserialize)]
struct EvidenceFrontmatter {
    schema_version: u32,
    tranche: String,
    task_type: EvidenceTaskType,
    status: EvidenceStatus,
    summary: String,
    generated_at: String,
    prompt_file: String,
    plan_file: String,
    #[serde(default)]
    scope: Vec<String>,
    verification_commands: Vec<String>,
    #[serde(default)]
    done_when: Option<String>,
}

#[derive(Debug)]
struct EvidenceDocument {
    frontmatter: EvidenceFrontmatter,
    body: String,
}

pub(crate) fn evidence_file_format_instructions() -> String {
    format!(
        "Evidence file contract for `{EVIDENCE_DIR}/I<tranche-key>.md`:\n\
1. Start the file with TOML frontmatter delimited by `+++`.\n\
2. Required frontmatter keys: `schema_version = 1`, `tranche`, `task_type` (`implementation` or `verification`), `status` (`pass`, `fail`, or `partial`), `summary`, `generated_at` (`YYYY-MM-DD`), `prompt_file`, `plan_file`, `scope` (array; may be empty), and `verification_commands` (non-empty array). `done_when` is optional.\n\
3. After the frontmatter, use this markdown body shape exactly:\n\
```text\n\
+++\n\
schema_version = 1\n\
tranche = \"WIRE-GATE-01\"\n\
task_type = \"implementation\"\n\
status = \"pass\"\n\
summary = \"One-sentence outcome\"\n\
generated_at = \"2026-03-10\"\n\
prompt_file = \"PROMPT.md\"\n\
plan_file = \"IMPLEMENTATION_PLAN.md\"\n\
scope = [\"crates/yarli-cli/src/commands.rs\"]\n\
verification_commands = [\"cargo test --workspace\"]\n\
done_when = \"Optional copied completion criteria\"\n\
+++\n\
# Evidence: WIRE-GATE-01\n\
## Summary\n\
- What changed and why it satisfies the tranche.\n\
## Changes\n\
- Files touched, behaviors added, or verification-only note.\n\
## Verification\n\
- `cargo test --workspace`: PASS - concrete result details.\n\
```\n\
4. Record concrete PASS/FAIL/PARTIAL outcomes under `## Verification`; do not leave placeholders."
    )
}

/// Collect tranche keys from `.yarli/evidence` whose evidence file:
///  - parses cleanly as a schema-valid evidence document, AND
///  - declares `status = "pass"` in frontmatter.
///
/// Used by `yarli plan tranche reconcile-from-evidence` and the auto-reconcile
/// pass invoked at the top of `yarli run` / `yarli run --fresh-from-tranches`
/// to prevent re-dispatching tranches whose work is already landed on disk.
pub(crate) fn collect_passing_tranche_keys(evidence_dir: &Path) -> Result<Vec<String>> {
    if !evidence_dir.exists() {
        return Ok(Vec::new());
    }
    let files = collect_evidence_files(evidence_dir)?;
    let mut keys = Vec::new();
    for file in &files {
        if let Ok((key, EvidenceStatus::Pass)) = read_tranche_key_and_status(file) {
            keys.push(key);
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn read_tranche_key_and_status(path: &Path) -> Result<(String, EvidenceStatus)> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read evidence file {}", path.display()))?;
    let document = parse_evidence_document(&content)
        .with_context(|| format!("invalid evidence document {}", path.display()))?;
    validate_frontmatter(path, &document.frontmatter)?;
    validate_body(path, &document.frontmatter, &document.body)?;
    Ok((
        document.frontmatter.tranche.clone(),
        document.frontmatter.status,
    ))
}

pub(crate) fn cmd_evidence_validate(path: &Path) -> Result<()> {
    let files = collect_evidence_files(path)?;
    if files.is_empty() {
        bail!("no evidence markdown files found at {}", path.display());
    }

    let mut errors = Vec::new();
    for file in &files {
        if let Err(err) = validate_evidence_file(file) {
            errors.push(format!("{}: {err:#}", file.display()));
        }
    }

    if errors.is_empty() {
        println!(
            "Evidence files are valid ({} file(s) checked).",
            files.len()
        );
        Ok(())
    } else {
        for err in &errors {
            eprintln!("  error: {err}");
        }
        bail!(
            "{} has {} validation error(s) across {} file(s)",
            path.display(),
            errors.len(),
            files.len()
        );
    }
}

fn collect_evidence_files(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if !path.exists() {
        bail!("evidence path not found: {}", path.display());
    }
    if !path.is_dir() {
        bail!(
            "evidence path is neither a file nor directory: {}",
            path.display()
        );
    }

    let mut files = fs::read_dir(path)
        .with_context(|| format!("failed to read evidence directory {}", path.display()))?
        .filter_map(|entry| entry.ok().map(|item| item.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("md"))
        .collect::<Vec<_>>();
    files.sort();
    Ok(files)
}

fn validate_evidence_file(path: &Path) -> Result<()> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read evidence file {}", path.display()))?;
    let document = parse_evidence_document(&content)
        .with_context(|| format!("invalid evidence document {}", path.display()))?;

    validate_frontmatter(path, &document.frontmatter)?;
    validate_body(path, &document.frontmatter, &document.body)?;

    Ok(())
}

fn parse_evidence_document(content: &str) -> Result<EvidenceDocument> {
    let normalized = content.replace("\r\n", "\n");
    let rest = normalized
        .strip_prefix("+++\n")
        .ok_or_else(|| anyhow::anyhow!("expected TOML frontmatter starting with `+++`"))?;
    let (frontmatter_text, body) = rest
        .split_once("\n+++\n")
        .ok_or_else(|| anyhow::anyhow!("expected closing `+++` after frontmatter"))?;
    let frontmatter: EvidenceFrontmatter =
        toml::from_str(frontmatter_text).context("failed to parse TOML frontmatter")?;

    Ok(EvidenceDocument {
        frontmatter,
        body: body.to_string(),
    })
}

fn validate_frontmatter(path: &Path, frontmatter: &EvidenceFrontmatter) -> Result<()> {
    if frontmatter.schema_version != EVIDENCE_SCHEMA_VERSION {
        bail!(
            "unsupported schema_version {} (expected {})",
            frontmatter.schema_version,
            EVIDENCE_SCHEMA_VERSION
        );
    }

    ensure_non_empty("tranche", &frontmatter.tranche)?;
    ensure_non_empty("summary", &frontmatter.summary)?;
    ensure_non_empty("generated_at", &frontmatter.generated_at)?;
    ensure_non_empty("prompt_file", &frontmatter.prompt_file)?;
    ensure_non_empty("plan_file", &frontmatter.plan_file)?;
    validate_iso_date(&frontmatter.generated_at)?;

    if frontmatter.verification_commands.is_empty() {
        bail!("frontmatter key `verification_commands` must contain at least one command");
    }
    for command in &frontmatter.verification_commands {
        ensure_non_empty("verification_commands[]", command)?;
    }
    for scope_path in &frontmatter.scope {
        ensure_non_empty("scope[]", scope_path)?;
    }
    if let Some(done_when) = frontmatter.done_when.as_deref() {
        ensure_non_empty("done_when", done_when)?;
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("evidence file name is not valid UTF-8"))?;
    if !file_name.starts_with(EVIDENCE_FILE_PREFIX) || !file_name.ends_with(".md") {
        bail!("evidence file name must match I<tranche-key>.md");
    }

    let expected_tranche = file_name
        .strip_suffix(".md")
        .and_then(|stem| stem.strip_prefix(EVIDENCE_FILE_PREFIX))
        .ok_or_else(|| anyhow::anyhow!("evidence file name must match I<tranche-key>.md"))?;
    if frontmatter.tranche != expected_tranche {
        bail!(
            "frontmatter tranche {:?} does not match file name tranche {:?}",
            frontmatter.tranche,
            expected_tranche
        );
    }

    let _ = frontmatter.task_type;
    let _ = frontmatter.status;

    Ok(())
}

fn validate_body(path: &Path, frontmatter: &EvidenceFrontmatter, body: &str) -> Result<()> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        bail!("evidence body must not be empty");
    }

    let expected_title = format!("# Evidence: {}", frontmatter.tranche);
    if !trimmed.starts_with(&expected_title) {
        bail!(
            "evidence body must begin with {:?} in {}",
            expected_title,
            path.display()
        );
    }

    let mut last_index = 0usize;
    for heading in REQUIRED_BODY_HEADINGS {
        let relative = trimmed[last_index..]
            .find(heading)
            .ok_or_else(|| anyhow::anyhow!("missing required heading `{heading}`"))?;
        last_index += relative + heading.len();
    }

    Ok(())
}

fn ensure_non_empty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("frontmatter key `{field}` must not be empty");
    }
    Ok(())
}

fn validate_iso_date(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes[..4].iter().all(u8::is_ascii_digit)
        || !bytes[5..7].iter().all(u8::is_ascii_digit)
        || !bytes[8..10].iter().all(u8::is_ascii_digit)
    {
        bail!("generated_at must use YYYY-MM-DD format");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn validate_evidence_file_accepts_structured_document() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("IWIRE-GATE-01.md");
        fs::write(
            &path,
            r#"+++
schema_version = 1
tranche = "WIRE-GATE-01"
task_type = "implementation"
status = "partial"
summary = "Propagated gate failure names into observer shutdown reporting."
generated_at = "2026-03-10"
prompt_file = "PROMPT.md"
plan_file = "IMPLEMENTATION_PLAN.md"
scope = ["crates/yarli-cli/src/commands.rs"]
verification_commands = ["cargo test --workspace"]
done_when = "`observe_run_end` receives actual gate failure names."
+++
# Evidence: WIRE-GATE-01
## Summary
- Propagated gate failure names through scheduler shutdown handling.
## Changes
- Updated the command path and left observer internals unchanged.
## Verification
- `cargo test --workspace`: PARTIAL - target behavior is present, but one observer test is still red.
"#,
        )
        .unwrap();

        validate_evidence_file(&path).unwrap();
    }

    #[test]
    fn validate_evidence_file_rejects_missing_frontmatter() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("IWIRE-GATE-01.md");
        fs::write(
            &path,
            "# Evidence: WIRE-GATE-01\n## Summary\n- nope\n## Changes\n- nope\n## Verification\n- nope\n",
        )
        .unwrap();

        let err = validate_evidence_file(&path).unwrap_err();
        assert!(format!("{err:#}").contains("expected TOML frontmatter starting with `+++`"));
    }

    #[test]
    fn validate_evidence_file_rejects_filename_mismatch() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("IWIRE-GATE-01.md");
        fs::write(
            &path,
            r#"+++
schema_version = 1
tranche = "WIRE-AUDIT-04"
task_type = "implementation"
status = "pass"
summary = "Stored audit records."
generated_at = "2026-03-10"
prompt_file = "PROMPT.md"
plan_file = "IMPLEMENTATION_PLAN.md"
scope = []
verification_commands = ["cargo test --workspace"]
+++
# Evidence: WIRE-AUDIT-04
## Summary
- Stored audit records.
## Changes
- Added audit writes.
## Verification
- `cargo test --workspace`: PASS - green.
"#,
        )
        .unwrap();

        let err = validate_evidence_file(&path).unwrap_err();
        assert!(err.to_string().contains("does not match file name tranche"));
    }

    #[test]
    fn cmd_evidence_validate_accepts_directory() {
        let temp = TempDir::new().unwrap();
        let evidence_dir = temp.path().join(".yarli/evidence");
        fs::create_dir_all(&evidence_dir).unwrap();
        fs::write(
            evidence_dir.join("IWIRE-RETRY-03.md"),
            r#"+++
schema_version = 1
tranche = "WIRE-RETRY-03"
task_type = "implementation"
status = "pass"
summary = "Attached retry recommendations to continuation payloads."
generated_at = "2026-03-10"
prompt_file = "PROMPT.md"
plan_file = "IMPLEMENTATION_PLAN.md"
scope = []
verification_commands = ["cargo test --workspace"]
+++
# Evidence: WIRE-RETRY-03
## Summary
- Added retry scope recommendations to continuation payloads.
## Changes
- Implementation tranche only; no extra notes.
## Verification
- `cargo test --workspace`: PASS - workspace tests passed.
"#,
        )
        .unwrap();

        cmd_evidence_validate(&evidence_dir).unwrap();
    }
}
