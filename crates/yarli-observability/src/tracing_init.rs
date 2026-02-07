//! Tracing initialization for YARLI (Section 15.1, 15.3).
//!
//! Provides a composable tracing-subscriber setup with:
//! - JSON or human-readable formatting
//! - `RUST_LOG` environment filter (defaults to `info`)
//! - Correlation ID propagation via tracing spans
//!
//! OTLP exporter integration is deferred until `tracing-opentelemetry` is
//! added as a workspace dependency; the current setup outputs structured logs
//! that are compatible with log collectors (Loki, Fluentd, etc.).

use tracing::Level;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Configuration for the tracing subsystem.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Default log level when `RUST_LOG` is not set.
    pub default_level: Level,
    /// Emit logs as JSON lines (for machine consumption).
    pub json: bool,
    /// Include thread IDs in log output.
    pub thread_ids: bool,
    /// Include file/line in log output.
    pub file_info: bool,
    /// Include target (module path) in log output.
    pub target: bool,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            default_level: Level::INFO,
            json: false,
            thread_ids: false,
            file_info: false,
            target: true,
        }
    }
}

impl TracingConfig {
    /// Create a config for JSON-formatted logs (suitable for production).
    pub fn json() -> Self {
        Self {
            json: true,
            thread_ids: true,
            file_info: true,
            ..Default::default()
        }
    }
}

/// Initialize the global tracing subscriber.
///
/// This should be called once at application startup. It reads `RUST_LOG`
/// from the environment; if absent, it uses `config.default_level`.
///
/// # Errors
///
/// Returns an error if the subscriber has already been set (double init).
pub fn init_tracing(config: &TracingConfig) -> Result<(), TracingInitError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.default_level.as_str()));

    if config.json {
        let fmt_layer = fmt::layer()
            .json()
            .with_target(config.target)
            .with_thread_ids(config.thread_ids)
            .with_file(config.file_info)
            .with_line_number(config.file_info);

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()
            .map_err(|_| TracingInitError::AlreadyInitialized)?;
    } else {
        let fmt_layer = fmt::layer()
            .with_target(config.target)
            .with_thread_ids(config.thread_ids)
            .with_file(config.file_info)
            .with_line_number(config.file_info);

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()
            .map_err(|_| TracingInitError::AlreadyInitialized)?;
    }

    Ok(())
}

/// Errors from tracing initialization.
#[derive(Debug, thiserror::Error)]
pub enum TracingInitError {
    #[error("tracing subscriber already initialized")]
    AlreadyInitialized,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = TracingConfig::default();
        assert_eq!(config.default_level, Level::INFO);
        assert!(!config.json);
        assert!(!config.thread_ids);
        assert!(!config.file_info);
        assert!(config.target);
    }

    #[test]
    fn json_config_values() {
        let config = TracingConfig::json();
        assert!(config.json);
        assert!(config.thread_ids);
        assert!(config.file_info);
    }

    #[test]
    fn config_clone_and_debug() {
        let config = TracingConfig::default();
        let _clone = config.clone();
        let debug = format!("{:?}", config);
        assert!(debug.contains("TracingConfig"));
    }

    #[test]
    fn tracing_init_error_display() {
        let err = TracingInitError::AlreadyInitialized;
        assert_eq!(err.to_string(), "tracing subscriber already initialized");
    }
}
