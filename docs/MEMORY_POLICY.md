# YARLI Memory Policy

This document defines when YARLI should store/query memories (beyond the durable event log).

## Goals

- Preserve reusable incident knowledge across runs (for humans and future agent context).
- Keep memory writes safe (no secrets/tokens) and small (summaries, not logs).
- Make memory behavior explicit and auditable via event-log observer events.

## Classes + Scopes

Classes (mirrors Memory-backend):

- `working`: transient run-local notes
- `episodic`: run timeline / incident narrative
- `semantic`: durable heuristics/lessons across runs

Scopes:

- `run/<run_id>`: run-scoped
- `task/<task_id>`: task-scoped
- `project/<project_id>`: project-scoped (cross-run)

## Storage Triggers (Minimum)

YARLI should store a memory when:

1. A task reaches a terminal failure:
- Trigger: `task.failed`
- Store: `semantic` memory in `project/<project_id>`
- Content: short summary (task key, reason bucket, exit-code bucket, minimal detail)
- Metadata: `run_id`, `task_id`, `event_id`, `reason`, `task_key`, plus any stable fingerprints

2. A task blocks and is likely to recur:
- Trigger: `task.blocked` (optional in early iterations)
- Store: `episodic` memory in `run/<run_id>` (avoid polluting project-level semantics)

3. Governance failures:
- Policy denial, budget exceeded, gate failure:
  - Prefer encoding these as `semantic` only when the signature is stable and reusable.

## Current Implementation (As Of 2026-02-11)

- Stored:
  - `task.failed` -> one `semantic` memory written to `project/<project_id>` (best-effort).
- Queried (hints only, no side effects):
  - Run start -> emits `run.observer.memory_hints` (best-effort).
  - Task failure or block -> emits `task.observer.memory_hints` (best-effort).

## Query/Hint Triggers

YARLI should query memories and emit hint events:

- At run start (`run.observer.memory_hints`), using objective/task keys as query text.
- When a task blocks or fails (`task.observer.memory_hints`), using reason bucket as query text.

Hints are *observer outputs* and should be surfaced in:

- `yarli run status <run-id>`
- `yarli run explain-exit <run-id>`

## Safety Rules (Hard Requirements)

- Redaction: if content matches secret patterns, do not store. Emit an explicit `*.memory_store_failed` observer event noting redaction required.
- Size: do not store raw command logs; store a summary and stable identifiers only.
- Best-effort: memory backend failures must never change scheduler semantics or budget fail-fast behavior.
