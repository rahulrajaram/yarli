use std::env;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn unix_epoch_seconds() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-env-changed=PROFILE");

    let commit =
        git_output(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let commit_date = git_output(&["show", "-s", "--format=%cI", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string());

    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    let timestamp = env::var("SOURCE_DATE_EPOCH").unwrap_or_else(|_| unix_epoch_seconds());
    let build_id = format!("{profile}-{timestamp}");

    println!("cargo:rustc-env=YARLI_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=YARLI_BUILD_DATE={commit_date}");
    println!("cargo:rustc-env=YARLI_BUILD_ID={build_id}");
}
