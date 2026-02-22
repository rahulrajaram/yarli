# CARD-R16-02 Verification Evidence

Date: 2026-02-22

Objective: wire `depends_on` DAG metadata from catalog events into discovery, projections, stream output, and dashboard/status surfaces.

Command outcomes:

- `cargo test --workspace`
  - Exit: `101`
  - Blockers:
    - `crates/yarli-api/src/server.rs`: missing `Request` generic, `map_err` signature mismatch, unknown `after` field usage, non-serializable `EventFilter`, unused import warnings (`HashSet`, `futures_util::SinkExt`).
    - `crates/yarli-cli/src/prompt.rs`: dependency validator expects `&[RunSpecTask]`, cycle detector stack type/lifetime mismatch.

Implementation file touchpoints:
- `crates/yarli-cli/src/stream/events.rs`
- `crates/yarli-cli/src/events.rs`
- `crates/yarli-cli/src/stream/renderer.rs`
- `crates/yarli-cli/src/dashboard/state.rs`
- `crates/yarli-cli/src/dashboard/renderer.rs`
- `crates/yarli-cli/src/stream/headless.rs`
- `crates/yarli-cli/src/projection.rs`
- `crates/yarli-cli/src/render.rs`
- `IMPLEMENTATION_PLAN.md`
