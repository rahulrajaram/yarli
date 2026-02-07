//! yarli-store: Event store and repositories.
//!
//! Provides the [`EventStore`] trait and an in-memory implementation
//! for development and testing. A Postgres-backed implementation will
//! follow in a later milestone.

pub mod error;
pub mod event_store;
pub mod memory;

pub use error::StoreError;
pub use event_store::EventStore;
pub use memory::InMemoryEventStore;
