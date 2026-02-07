//! yarli-api: minimal HTTP service surface.

pub mod server;

pub use server::{
    router, serve, ApiServerError, ApiState, HealthResponse, RunStatusResponse, TaskStatusSummary,
};
