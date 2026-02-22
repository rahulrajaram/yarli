//! Chaos engineering harness for YARLI.
//!
//! Provides traits and implementations for injecting faults into the runtime.

use anyhow::Result;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// A specific failure mode that can be injected.
#[async_trait]
pub trait Fault: Send + Sync + std::fmt::Debug {
    /// Name of the fault (e.g. "postgres_connection_drop").
    fn name(&self) -> &str;

    /// Should the fault trigger for this injection point?
    fn should_trigger(&self, point: &str) -> bool;

    /// Execute the fault logic (e.g. sleep, panic, error).
    async fn execute(&self) -> Result<()>;
}

/// Controller for managing active faults.
#[derive(Debug, Default, Clone)]
pub struct ChaosController {
    faults: Arc<Mutex<Vec<Arc<dyn Fault>>>>,
}

impl ChaosController {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_fault(&self, fault: Arc<dyn Fault>) {
        self.faults.lock().unwrap().push(fault);
    }

    pub fn clear(&self) {
        self.faults.lock().unwrap().clear();
    }

    /// Inject a potential fault at a named code point.
    pub async fn inject(&self, point: &str) -> Result<()> {
        let active_faults = {
            let faults = self.faults.lock().unwrap();
            faults
                .iter()
                .filter(|f| f.should_trigger(point))
                .cloned()
                .collect::<Vec<_>>()
        };

        for fault in active_faults {
            tracing::warn!(fault = fault.name(), point = point, "injecting fault");
            fault.execute().await?;
        }

        Ok(())
    }
}

// --- Common Faults ---

#[derive(Debug)]
pub struct CrashFault {
    pub point: String,
    pub probability: f64,
}

#[async_trait]
impl Fault for CrashFault {
    fn name(&self) -> &str {
        "crash_process"
    }

    fn should_trigger(&self, point: &str) -> bool {
        if point != self.point {
            return false;
        }
        rand::random::<f64>() < self.probability
    }

    async fn execute(&self) -> Result<()> {
        tracing::error!("CHAOS: Crashing process now!");
        std::process::exit(101); // Exit with a specific code
    }
}

#[derive(Debug)]
pub struct LatencyFault {
    pub point: String,
    pub duration: std::time::Duration,
    pub probability: f64,
}

#[async_trait]
impl Fault for LatencyFault {
    fn name(&self) -> &str {
        "inject_latency"
    }

    fn should_trigger(&self, point: &str) -> bool {
        if point != self.point {
            return false;
        }
        rand::random::<f64>() < self.probability
    }

    async fn execute(&self) -> Result<()> {
        tracing::warn!("CHAOS: Sleeping for {:?}", self.duration);
        tokio::time::sleep(self.duration).await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ErrorFault {
    pub point: String,
    pub message: String,
    pub probability: f64,
}

#[async_trait]
impl Fault for ErrorFault {
    fn name(&self) -> &str {
        "inject_error"
    }

    fn should_trigger(&self, point: &str) -> bool {
        if point != self.point {
            return false;
        }
        rand::random::<f64>() < self.probability
    }

    async fn execute(&self) -> Result<()> {
        tracing::warn!("CHAOS: Injecting error: {}", self.message);
        Err(anyhow::anyhow!("CHAOS: {}", self.message))
    }
}

#[derive(Debug)]
pub struct PanicFault {
    pub point: String,
    pub message: String,
    pub probability: f64,
}

#[async_trait]
impl Fault for PanicFault {
    fn name(&self) -> &str {
        "inject_panic"
    }

    fn should_trigger(&self, point: &str) -> bool {
        if point != self.point {
            return false;
        }
        rand::random::<f64>() < self.probability
    }

    async fn execute(&self) -> Result<()> {
        tracing::error!("CHAOS: Panicking now: {}", self.message);
        panic!("CHAOS: {}", self.message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_controller_filters_by_point() {
        let controller = ChaosController::new();
        let fault = Arc::new(ErrorFault {
            point: "point_a".to_string(),
            message: "boom".to_string(),
            probability: 1.0,
        });
        controller.add_fault(fault);

        // Should not trigger
        assert!(controller.inject("point_b").await.is_ok());

        // Should trigger
        let err = controller.inject("point_a").await.unwrap_err();
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn test_probability_filtering() {
        let controller = ChaosController::new();
        let fault = Arc::new(ErrorFault {
            point: "point_a".to_string(),
            message: "boom".to_string(),
            probability: 0.0, // Never trigger
        });
        controller.add_fault(fault);

        // Should not trigger
        assert!(controller.inject("point_a").await.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_faults() {
        let controller = ChaosController::new();
        controller.add_fault(Arc::new(LatencyFault {
            point: "point_a".to_string(),
            duration: std::time::Duration::from_millis(10),
            probability: 1.0,
        }));
        controller.add_fault(Arc::new(ErrorFault {
            point: "point_a".to_string(),
            message: "boom".to_string(),
            probability: 1.0,
        }));

        let start = std::time::Instant::now();
        let err = controller.inject("point_a").await.unwrap_err();
        assert!(start.elapsed() >= std::time::Duration::from_millis(10));
        assert!(err.to_string().contains("boom"));
    }
}
