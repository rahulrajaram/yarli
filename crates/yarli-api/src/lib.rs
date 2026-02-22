//! yarli-api: minimal HTTP service surface.

pub mod server;

pub use server::{
    router, serve, ApiServerError, ApiState, HealthResponse, RunStatusResponse, TaskStatusResponse,
    TaskStatusSummary,
};
#[cfg(feature = "debug-api")]
pub use server::{router_with_queue, serve_with_queue};
