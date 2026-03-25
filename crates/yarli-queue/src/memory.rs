//! In-memory task queue for development and testing.
//!
//! Uses `RwLock<Vec<QueueEntry>>` with index maps for O(1) lookups,
//! mirroring the InMemoryEventStore pattern from yarli-store.

use std::collections::HashMap;
use std::sync::RwLock;

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::yarli_core::domain::{CommandClass, RunId, TaskId};

use crate::yarli_queue::error::QueueError;
use crate::yarli_queue::queue::{
    ClaimRequest, ConcurrencyConfig, QueueEntry, QueueStats, QueueStatus, TaskQueue,
};

const PRIORITY_AGING_WINDOW_SECONDS: i64 = 60;

fn priority_for_claim(priority: u32, available_at: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
    let age_seconds = (now - available_at).num_seconds().max(0);
    let age_boost = age_seconds / PRIORITY_AGING_WINDOW_SECONDS;
    priority as i64 + age_boost
}

/// Thread-safe in-memory task queue.
pub struct InMemoryTaskQueue {
    inner: RwLock<QueueInner>,
}

struct QueueInner {
    entries: Vec<QueueEntry>,
    /// queue_id → index in entries vec.
    id_index: HashMap<Uuid, usize>,
    /// task_id → queue_id for active (pending/leased) entries.
    active_tasks: HashMap<TaskId, Uuid>,
}

impl InMemoryTaskQueue {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(QueueInner {
                entries: Vec::new(),
                id_index: HashMap::new(),
                active_tasks: HashMap::new(),
            }),
        }
    }
}

impl Default for InMemoryTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueue for InMemoryTaskQueue {
    fn enqueue(
        &self,
        task_id: TaskId,
        run_id: RunId,
        priority: u32,
        command_class: CommandClass,
        available_at: Option<DateTime<Utc>>,
    ) -> Result<Uuid, QueueError> {
        let mut inner = self.inner.write().unwrap();

        // Check for duplicate active task.
        if inner.active_tasks.contains_key(&task_id) {
            return Err(QueueError::DuplicateTask(task_id));
        }

        let now = Utc::now();
        let queue_id = Uuid::now_v7();
        let entry = QueueEntry {
            queue_id,
            task_id,
            run_id,
            priority,
            available_at: available_at.unwrap_or(now),
            attempt_no: 1,
            command_class,
            status: QueueStatus::Pending,
            lease_owner: None,
            lease_expires_at: None,
            last_heartbeat: None,
            rehydration_tokens: None,
            created_at: now,
            updated_at: now,
        };

        let idx = inner.entries.len();
        inner.entries.push(entry);
        inner.id_index.insert(queue_id, idx);
        inner.active_tasks.insert(task_id, queue_id);

        Ok(queue_id)
    }

    fn claim(
        &self,
        request: &ClaimRequest,
        config: &ConcurrencyConfig,
    ) -> Result<Vec<QueueEntry>, QueueError> {
        let mut inner = self.inner.write().unwrap();
        let now = Utc::now();

        // Count current leased entries per run and per command class.
        let mut leased_by_run: HashMap<RunId, usize> = HashMap::new();
        let mut leased_by_class: HashMap<CommandClass, usize> = HashMap::new();
        for entry in &inner.entries {
            if entry.status == QueueStatus::Leased {
                *leased_by_run.entry(entry.run_id).or_default() += 1;
                *leased_by_class.entry(entry.command_class).or_default() += 1;
            }
        }

        // Collect indices of claimable entries, sorted by priority DESC with aging.
        let allowed = &request.allowed_run_ids;
        let mut candidates: Vec<usize> = inner
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                e.status == QueueStatus::Pending
                    && e.available_at <= now
                    && allowed.as_ref().map_or(true, |ids| ids.contains(&e.run_id))
            })
            .map(|(i, _)| i)
            .collect();

        candidates.sort_by(|&a, &b| {
            let ea = &inner.entries[a];
            let eb = &inner.entries[b];
            priority_for_claim(eb.priority, eb.available_at, now)
                .cmp(&priority_for_claim(ea.priority, ea.available_at, now))
                .then(eb.priority.cmp(&ea.priority))
                .then(ea.available_at.cmp(&eb.available_at))
                .then(ea.queue_id.cmp(&eb.queue_id))
        });

        let mut claimed = Vec::new();
        let lease_expires = now + request.lease_ttl;

        for idx in candidates {
            if claimed.len() >= request.limit {
                break;
            }

            let entry = &inner.entries[idx];

            // Check per-run cap.
            let run_count = leased_by_run.get(&entry.run_id).copied().unwrap_or(0);
            if run_count >= config.per_run_cap {
                continue;
            }

            // Check per-class cap.
            let class_count = leased_by_class
                .get(&entry.command_class)
                .copied()
                .unwrap_or(0);
            if class_count >= config.cap_for(entry.command_class) {
                continue;
            }

            // Claim it.
            let entry = &mut inner.entries[idx];
            entry.status = QueueStatus::Leased;
            entry.lease_owner = Some(request.worker_id.clone());
            entry.lease_expires_at = Some(lease_expires);
            entry.last_heartbeat = Some(now);
            entry.updated_at = now;

            *leased_by_run.entry(entry.run_id).or_default() += 1;
            *leased_by_class.entry(entry.command_class).or_default() += 1;

            claimed.push(entry.clone());
        }

        Ok(claimed)
    }

    fn heartbeat(
        &self,
        queue_id: Uuid,
        worker_id: &str,
        lease_ttl: Duration,
    ) -> Result<(), QueueError> {
        let mut inner = self.inner.write().unwrap();
        let idx = *inner
            .id_index
            .get(&queue_id)
            .ok_or(QueueError::NotFound(queue_id))?;

        let entry = &mut inner.entries[idx];

        if entry.status != QueueStatus::Leased {
            return Err(QueueError::InvalidStatus {
                entry_id: queue_id,
                expected: "leased",
                actual: format!("{:?}", entry.status),
            });
        }

        let owner = entry.lease_owner.as_deref().unwrap_or("");
        if owner != worker_id {
            return Err(QueueError::LeaseOwnerMismatch {
                entry_id: queue_id,
                expected: worker_id.to_string(),
                actual: owner.to_string(),
            });
        }

        // Check if lease already expired.
        let now = Utc::now();
        if let Some(expires) = entry.lease_expires_at {
            if now > expires {
                return Err(QueueError::LeaseExpired(queue_id));
            }
        }

        entry.lease_expires_at = Some(now + lease_ttl);
        entry.last_heartbeat = Some(now);
        entry.updated_at = now;

        Ok(())
    }

    fn complete(&self, queue_id: Uuid, worker_id: &str) -> Result<(), QueueError> {
        let mut inner = self.inner.write().unwrap();
        let idx = *inner
            .id_index
            .get(&queue_id)
            .ok_or(QueueError::NotFound(queue_id))?;

        let entry = &inner.entries[idx];

        if entry.status != QueueStatus::Leased {
            return Err(QueueError::InvalidStatus {
                entry_id: queue_id,
                expected: "leased",
                actual: format!("{:?}", entry.status),
            });
        }

        let owner = entry.lease_owner.as_deref().unwrap_or("");
        if owner != worker_id {
            return Err(QueueError::LeaseOwnerMismatch {
                entry_id: queue_id,
                expected: worker_id.to_string(),
                actual: owner.to_string(),
            });
        }

        let task_id = entry.task_id;
        let entry = &mut inner.entries[idx];
        entry.status = QueueStatus::Completed;
        entry.updated_at = Utc::now();

        // Remove from active tasks.
        inner.active_tasks.remove(&task_id);

        Ok(())
    }

    fn fail(&self, queue_id: Uuid, worker_id: &str) -> Result<(), QueueError> {
        let mut inner = self.inner.write().unwrap();
        let idx = *inner
            .id_index
            .get(&queue_id)
            .ok_or(QueueError::NotFound(queue_id))?;

        let entry = &inner.entries[idx];

        if entry.status != QueueStatus::Leased {
            return Err(QueueError::InvalidStatus {
                entry_id: queue_id,
                expected: "leased",
                actual: format!("{:?}", entry.status),
            });
        }

        let owner = entry.lease_owner.as_deref().unwrap_or("");
        if owner != worker_id {
            return Err(QueueError::LeaseOwnerMismatch {
                entry_id: queue_id,
                expected: worker_id.to_string(),
                actual: owner.to_string(),
            });
        }

        let task_id = entry.task_id;
        let entry = &mut inner.entries[idx];
        entry.status = QueueStatus::Failed;
        entry.updated_at = Utc::now();

        // Remove from active tasks so the task can be re-enqueued for retry.
        inner.active_tasks.remove(&task_id);

        Ok(())
    }

    fn override_priority(&self, task_id: TaskId, priority: u32) -> Result<(), QueueError> {
        let mut inner = self.inner.write().unwrap();
        let now = Utc::now();
        let mut found = false;

        for entry in &mut inner.entries {
            if entry.task_id == task_id {
                entry.priority = priority;
                entry.updated_at = now;
                found = true;
            }
        }

        if found {
            Ok(())
        } else {
            Err(QueueError::NotFound(task_id))
        }
    }

    fn cancel(&self, queue_id: Uuid) -> Result<(), QueueError> {
        let mut inner = self.inner.write().unwrap();
        let idx = *inner
            .id_index
            .get(&queue_id)
            .ok_or(QueueError::NotFound(queue_id))?;

        let status = inner.entries[idx].status;
        let task_id = inner.entries[idx].task_id;

        match status {
            QueueStatus::Pending | QueueStatus::Leased => {
                let entry = &mut inner.entries[idx];
                entry.status = QueueStatus::Cancelled;
                entry.updated_at = Utc::now();
                inner.active_tasks.remove(&task_id);
                Ok(())
            }
            _ => Err(QueueError::InvalidStatus {
                entry_id: queue_id,
                expected: "pending or leased",
                actual: format!("{status:?}"),
            }),
        }
    }

    fn entries(&self) -> Vec<QueueEntry> {
        let inner = self.inner.read().unwrap();
        inner.entries.clone()
    }

    fn reclaim_stale(&self, grace_period: Duration) -> Result<usize, QueueError> {
        let mut inner = self.inner.write().unwrap();
        let now = Utc::now();
        let mut reclaimed = 0;

        for entry in &mut inner.entries {
            if entry.status != QueueStatus::Leased {
                continue;
            }

            let expires = match entry.lease_expires_at {
                Some(t) => t,
                None => continue,
            };

            // Stale if lease_expires_at + grace < now.
            if expires + grace_period < now {
                entry.status = QueueStatus::Pending;
                entry.attempt_no += 1;
                entry.lease_owner = None;
                entry.lease_expires_at = None;
                entry.last_heartbeat = None;
                entry.updated_at = now;
                reclaimed += 1;
            }
        }

        Ok(reclaimed)
    }

    fn stats(&self) -> QueueStats {
        let inner = self.inner.read().unwrap();
        let mut stats = QueueStats::default();
        for entry in &inner.entries {
            match entry.status {
                QueueStatus::Pending => stats.pending += 1,
                QueueStatus::Leased => stats.leased += 1,
                QueueStatus::Completed => stats.completed += 1,
                QueueStatus::Failed => stats.failed += 1,
                QueueStatus::Cancelled => stats.cancelled += 1,
            }
        }
        stats
    }

    fn leased_count_for_run(&self, run_id: RunId) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .entries
            .iter()
            .filter(|e| e.status == QueueStatus::Leased && e.run_id == run_id)
            .count()
    }

    fn leased_count_for_class(&self, class: CommandClass) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .entries
            .iter()
            .filter(|e| e.status == QueueStatus::Leased && e.command_class == class)
            .count()
    }

    fn pending_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .entries
            .iter()
            .filter(|e| e.status == QueueStatus::Pending)
            .count()
    }

    fn cancel_for_run(&self, run_id: RunId) -> Result<usize, QueueError> {
        let mut inner = self.inner.write().unwrap();
        let now = Utc::now();
        let mut cancelled = 0;

        // Collect task_ids of entries we're about to cancel.
        let mut cancelled_task_ids = Vec::new();

        for entry in &mut inner.entries {
            if entry.run_id == run_id
                && matches!(entry.status, QueueStatus::Pending | QueueStatus::Leased)
            {
                cancelled_task_ids.push(entry.task_id);
                entry.status = QueueStatus::Cancelled;
                entry.updated_at = now;
                cancelled += 1;
            }
        }

        // Remove cancelled entries from active_tasks map.
        for task_id in &cancelled_task_ids {
            inner.active_tasks.remove(task_id);
        }

        Ok(cancelled)
    }

    fn cancel_stale_runs(&self, active_run_ids: &[RunId]) -> Result<usize, QueueError> {
        let mut inner = self.inner.write().unwrap();
        let now = Utc::now();
        let mut cancelled = 0;
        let mut cancelled_task_ids = Vec::new();

        for entry in &mut inner.entries {
            if matches!(entry.status, QueueStatus::Pending | QueueStatus::Leased)
                && !active_run_ids.contains(&entry.run_id)
            {
                cancelled_task_ids.push(entry.task_id);
                entry.status = QueueStatus::Cancelled;
                entry.updated_at = now;
                cancelled += 1;
            }
        }

        for task_id in &cancelled_task_ids {
            inner.active_tasks.remove(task_id);
        }

        Ok(cancelled)
    }
}

#[cfg(test)]
mod tests;
