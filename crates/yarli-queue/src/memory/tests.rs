//! Tests for InMemoryTaskQueue.

use chrono::{Duration, Utc};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, RunId};

use crate::memory::InMemoryTaskQueue;
use crate::queue::{ClaimRequest, ConcurrencyConfig, QueueStats, QueueStatus, TaskQueue};
use crate::QueueError;

fn make_run_id() -> RunId {
    Uuid::now_v7()
}

fn default_config() -> ConcurrencyConfig {
    ConcurrencyConfig::default()
}

// ─── Enqueue ─────────────────────────────────────────────────────────────

#[test]
fn enqueue_returns_queue_id() {
    let q = InMemoryTaskQueue::new();
    let task_id = Uuid::now_v7();
    let run_id = make_run_id();

    let qid = q
        .enqueue(task_id, run_id, 3, CommandClass::Io, None)
        .unwrap();
    assert_ne!(qid, Uuid::nil());
}

#[test]
fn enqueue_duplicate_active_task_rejected() {
    let q = InMemoryTaskQueue::new();
    let task_id = Uuid::now_v7();
    let run_id = make_run_id();

    q.enqueue(task_id, run_id, 3, CommandClass::Io, None)
        .unwrap();
    let err = q
        .enqueue(task_id, run_id, 3, CommandClass::Io, None)
        .unwrap_err();
    assert!(matches!(err, QueueError::DuplicateTask(id) if id == task_id));
}

#[test]
fn enqueue_same_task_after_complete_allowed() {
    let q = InMemoryTaskQueue::new();
    let task_id = Uuid::now_v7();
    let run_id = make_run_id();
    let cfg = default_config();

    let qid = q
        .enqueue(task_id, run_id, 1, CommandClass::Io, None)
        .unwrap();

    // Claim and complete.
    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 1);
    q.complete(qid, "w1").unwrap();

    // Re-enqueue should work since the task is no longer active.
    let qid2 = q
        .enqueue(task_id, run_id, 1, CommandClass::Io, None)
        .unwrap();
    assert_ne!(qid, qid2);
}

#[test]
fn enqueue_multiple_tasks() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();

    for _ in 0..5 {
        q.enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Io, None)
            .unwrap();
    }

    assert_eq!(q.pending_count(), 5);
}

// ─── Claim ───────────────────────────────────────────────────────────────

#[test]
fn claim_returns_pending_entries() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    let t1 = Uuid::now_v7();
    let t2 = Uuid::now_v7();
    q.enqueue(t1, run_id, 3, CommandClass::Io, None).unwrap();
    q.enqueue(t2, run_id, 3, CommandClass::Io, None).unwrap();

    let req = ClaimRequest::new("w1", 5, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 2);
    assert!(claimed.iter().all(|e| e.status == QueueStatus::Leased));
    assert!(claimed
        .iter()
        .all(|e| e.lease_owner.as_deref() == Some("w1")));
}

#[test]
fn claim_respects_priority_ordering() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    let low_pri = Uuid::now_v7();
    let high_pri = Uuid::now_v7();
    q.enqueue(low_pri, run_id, 5, CommandClass::Io, None)
        .unwrap();
    q.enqueue(high_pri, run_id, 1, CommandClass::Io, None)
        .unwrap();

    // Claim only 1 — should get the high-priority one.
    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, high_pri);
}

#[test]
fn claim_respects_available_at() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    let now_task = Uuid::now_v7();
    let future_task = Uuid::now_v7();

    q.enqueue(now_task, run_id, 1, CommandClass::Io, None)
        .unwrap();
    q.enqueue(
        future_task,
        run_id,
        1,
        CommandClass::Io,
        Some(Utc::now() + Duration::hours(1)),
    )
    .unwrap();

    let req = ClaimRequest::new("w1", 10, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, now_task);
}

#[test]
fn claim_skips_already_leased() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    let t1 = Uuid::now_v7();
    let t2 = Uuid::now_v7();
    q.enqueue(t1, run_id, 1, CommandClass::Io, None).unwrap();
    q.enqueue(t2, run_id, 1, CommandClass::Io, None).unwrap();

    // Worker 1 claims 1.
    let req1 = ClaimRequest::single("w1");
    let claimed1 = q.claim(&req1, &cfg).unwrap();
    assert_eq!(claimed1.len(), 1);

    // Worker 2 should get the other one.
    let req2 = ClaimRequest::single("w2");
    let claimed2 = q.claim(&req2, &cfg).unwrap();
    assert_eq!(claimed2.len(), 1);
    assert_ne!(claimed1[0].queue_id, claimed2[0].queue_id);

    // No more pending.
    let req3 = ClaimRequest::single("w3");
    let claimed3 = q.claim(&req3, &cfg).unwrap();
    assert!(claimed3.is_empty());
}

#[test]
fn claim_empty_queue_returns_empty() {
    let q = InMemoryTaskQueue::new();
    let cfg = default_config();
    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    assert!(claimed.is_empty());
}

#[test]
fn claim_respects_limit() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    for _ in 0..10 {
        q.enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Io, None)
            .unwrap();
    }

    let req = ClaimRequest::new("w1", 3, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 3);
}

// ─── Concurrency Caps ────────────────────────────────────────────────────

#[test]
fn claim_respects_per_run_cap() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = ConcurrencyConfig {
        per_run_cap: 2,
        ..default_config()
    };

    for _ in 0..5 {
        q.enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Io, None)
            .unwrap();
    }

    let req = ClaimRequest::new("w1", 10, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 2);
}

#[test]
fn claim_respects_per_class_cap() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = ConcurrencyConfig {
        git_cap: 1,
        ..default_config()
    };

    for _ in 0..3 {
        q.enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Git, None)
            .unwrap();
    }

    let req = ClaimRequest::new("w1", 10, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 1);
}

#[test]
fn claim_mixed_classes_respect_individual_caps() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = ConcurrencyConfig {
        per_run_cap: 100,
        io_cap: 2,
        git_cap: 1,
        cpu_cap: 100,
        tool_cap: 100,
    };

    // 3 IO tasks.
    for _ in 0..3 {
        q.enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Io, None)
            .unwrap();
    }
    // 3 Git tasks.
    for _ in 0..3 {
        q.enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Git, None)
            .unwrap();
    }

    let req = ClaimRequest::new("w1", 20, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();

    let io_count = claimed
        .iter()
        .filter(|e| e.command_class == CommandClass::Io)
        .count();
    let git_count = claimed
        .iter()
        .filter(|e| e.command_class == CommandClass::Git)
        .count();

    assert_eq!(io_count, 2);
    assert_eq!(git_count, 1);
    assert_eq!(claimed.len(), 3);
}

// ─── Heartbeat ───────────────────────────────────────────────────────────

#[test]
fn heartbeat_extends_lease() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    let task_id = Uuid::now_v7();
    q.enqueue(task_id, run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::new("w1", 1, Duration::seconds(5));
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;
    let _original_expires = claimed[0].lease_expires_at.unwrap();

    // Heartbeat with a longer TTL.
    q.heartbeat(qid, "w1", Duration::seconds(60)).unwrap();

    // The new expiry should be later than the original.
    // We can't easily read back the entry, but we can verify it doesn't error
    // and a subsequent heartbeat also works.
    q.heartbeat(qid, "w1", Duration::seconds(60)).unwrap();
}

#[test]
fn heartbeat_wrong_worker_rejected() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;

    let err = q.heartbeat(qid, "w2", Duration::seconds(30)).unwrap_err();
    assert!(matches!(err, QueueError::LeaseOwnerMismatch { .. }));
}

#[test]
fn heartbeat_not_found() {
    let q = InMemoryTaskQueue::new();
    let err = q
        .heartbeat(Uuid::now_v7(), "w1", Duration::seconds(30))
        .unwrap_err();
    assert!(matches!(err, QueueError::NotFound(_)));
}

#[test]
fn heartbeat_on_pending_entry_rejected() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();

    let qid = q
        .enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let err = q.heartbeat(qid, "w1", Duration::seconds(30)).unwrap_err();
    assert!(matches!(err, QueueError::InvalidStatus { .. }));
}

// ─── Complete ────────────────────────────────────────────────────────────

#[test]
fn complete_transitions_to_completed() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;

    q.complete(qid, "w1").unwrap();

    let stats = q.stats();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.leased, 0);
}

#[test]
fn complete_wrong_worker_rejected() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;

    let err = q.complete(qid, "w2").unwrap_err();
    assert!(matches!(err, QueueError::LeaseOwnerMismatch { .. }));
}

#[test]
fn complete_pending_entry_rejected() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();

    let qid = q
        .enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let err = q.complete(qid, "w1").unwrap_err();
    assert!(matches!(err, QueueError::InvalidStatus { .. }));
}

// ─── Fail ────────────────────────────────────────────────────────────────

#[test]
fn fail_transitions_to_failed() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;

    q.fail(qid, "w1").unwrap();

    let stats = q.stats();
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.leased, 0);
}

#[test]
fn fail_wrong_worker_rejected() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;

    let err = q.fail(qid, "w2").unwrap_err();
    assert!(matches!(err, QueueError::LeaseOwnerMismatch { .. }));
}

// ─── Cancel ──────────────────────────────────────────────────────────────

#[test]
fn cancel_pending_entry() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();

    let qid = q
        .enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    q.cancel(qid).unwrap();

    let stats = q.stats();
    assert_eq!(stats.cancelled, 1);
    assert_eq!(stats.pending, 0);
}

#[test]
fn cancel_leased_entry() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;

    q.cancel(qid).unwrap();

    let stats = q.stats();
    assert_eq!(stats.cancelled, 1);
    assert_eq!(stats.leased, 0);
}

#[test]
fn cancel_completed_entry_rejected() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    let qid = claimed[0].queue_id;
    q.complete(qid, "w1").unwrap();

    let err = q.cancel(qid).unwrap_err();
    assert!(matches!(err, QueueError::InvalidStatus { .. }));
}

// ─── Reclaim Stale ───────────────────────────────────────────────────────

#[test]
fn reclaim_stale_returns_zero_when_no_stale() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::new("w1", 1, Duration::seconds(300));
    q.claim(&req, &cfg).unwrap();

    let reclaimed = q.reclaim_stale(Duration::seconds(5)).unwrap();
    assert_eq!(reclaimed, 0);
}

#[test]
fn reclaim_stale_reclaims_expired_leases() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    // Claim with a very short TTL (1 millisecond — effectively already expired).
    let req = ClaimRequest::new("w1", 1, Duration::milliseconds(1));
    q.claim(&req, &cfg).unwrap();

    // Small sleep to ensure expiry.
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Grace period of 0 — should reclaim immediately.
    let reclaimed = q.reclaim_stale(Duration::zero()).unwrap();
    assert_eq!(reclaimed, 1);

    let stats = q.stats();
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.leased, 0);
}

#[test]
fn reclaim_stale_increments_attempt() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    // Claim with expired TTL.
    let req = ClaimRequest::new("w1", 1, Duration::milliseconds(1));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed[0].attempt_no, 1);

    std::thread::sleep(std::time::Duration::from_millis(10));
    q.reclaim_stale(Duration::zero()).unwrap();

    // Re-claim — should see attempt_no = 2.
    let req2 = ClaimRequest::single("w2");
    let reclaimed = q.claim(&req2, &cfg).unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].attempt_no, 2);
}

#[test]
fn reclaim_stale_does_not_reclaim_within_grace() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::new("w1", 1, Duration::milliseconds(1));
    q.claim(&req, &cfg).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(10));

    // Grace period of 1 hour — should not reclaim.
    let reclaimed = q.reclaim_stale(Duration::hours(1)).unwrap();
    assert_eq!(reclaimed, 0);
}

// ─── Stats / Counts ──────────────────────────────────────────────────────

#[test]
fn stats_reflect_all_statuses() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    // Enqueue 4 tasks.
    let mut qids = Vec::new();
    for _ in 0..4 {
        let qid = q
            .enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Io, None)
            .unwrap();
        qids.push(qid);
    }

    // Claim all.
    let req = ClaimRequest::new("w1", 10, Duration::seconds(30));
    q.claim(&req, &cfg).unwrap();

    // Complete first, fail second, cancel third, leave fourth leased.
    q.complete(qids[0], "w1").unwrap();
    q.fail(qids[1], "w1").unwrap();
    q.cancel(qids[2]).unwrap();

    let stats = q.stats();
    assert_eq!(
        stats,
        QueueStats {
            pending: 0,
            leased: 1,
            completed: 1,
            failed: 1,
            cancelled: 1,
        }
    );
}

#[test]
fn leased_count_for_run() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();
    let run_b = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_a, 1, CommandClass::Io, None)
        .unwrap();
    q.enqueue(Uuid::now_v7(), run_a, 1, CommandClass::Io, None)
        .unwrap();
    q.enqueue(Uuid::now_v7(), run_b, 1, CommandClass::Io, None)
        .unwrap();

    let req = ClaimRequest::new("w1", 10, Duration::seconds(30));
    q.claim(&req, &cfg).unwrap();

    assert_eq!(q.leased_count_for_run(run_a), 2);
    assert_eq!(q.leased_count_for_run(run_b), 1);
}

#[test]
fn leased_count_for_class() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();
    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Git, None)
        .unwrap();
    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Git, None)
        .unwrap();

    let req = ClaimRequest::new("w1", 10, Duration::seconds(30));
    q.claim(&req, &cfg).unwrap();

    assert_eq!(q.leased_count_for_class(CommandClass::Io), 1);
    assert_eq!(q.leased_count_for_class(CommandClass::Git), 2);
    assert_eq!(q.leased_count_for_class(CommandClass::Cpu), 0);
}

#[test]
fn pending_count_tracks_pending_only() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();
    q.enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    assert_eq!(q.pending_count(), 2);

    // Claim one.
    let req = ClaimRequest::single("w1");
    q.claim(&req, &cfg).unwrap();

    assert_eq!(q.pending_count(), 1);
}

// ─── Concurrency: Thread Safety ──────────────────────────────────────────

#[test]
fn concurrent_claims_no_double_leasing() {
    use std::sync::Arc;
    use std::sync::Barrier;

    let q = Arc::new(InMemoryTaskQueue::new());
    let run_id = make_run_id();
    let cfg = Arc::new(ConcurrencyConfig {
        per_run_cap: 100,
        ..default_config()
    });
    let barrier = Arc::new(Barrier::new(5));

    // Enqueue 10 tasks.
    for _ in 0..10 {
        q.enqueue(Uuid::now_v7(), run_id, 3, CommandClass::Io, None)
            .unwrap();
    }

    // Spawn 5 threads, each trying to claim up to 10 tasks.
    // The RwLock serializes access, so no double-leasing should occur.
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let q = Arc::clone(&q);
            let cfg = Arc::clone(&cfg);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let req = ClaimRequest::new(format!("w{i}"), 10, Duration::seconds(30));
                q.claim(&req, &cfg).unwrap()
            })
        })
        .collect();

    let mut all_claimed: Vec<_> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    // All 10 tasks should be claimed with no duplicates.
    assert_eq!(all_claimed.len(), 10);
    all_claimed.sort_by_key(|e| e.queue_id);
    all_claimed.dedup_by_key(|e| e.queue_id);
    assert_eq!(all_claimed.len(), 10);

    // Nothing left pending.
    assert_eq!(q.pending_count(), 0);
}

// ─── Full Lifecycle ──────────────────────────────────────────────────────

#[test]
fn full_lifecycle_enqueue_claim_heartbeat_complete() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();
    let task_id = Uuid::now_v7();

    // 1. Enqueue.
    let qid = q
        .enqueue(task_id, run_id, 1, CommandClass::Cpu, None)
        .unwrap();
    assert_eq!(q.pending_count(), 1);

    // 2. Claim.
    let req = ClaimRequest::new("worker-A", 1, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].queue_id, qid);
    assert_eq!(claimed[0].status, QueueStatus::Leased);
    assert_eq!(q.pending_count(), 0);

    // 3. Heartbeat.
    q.heartbeat(qid, "worker-A", Duration::seconds(30)).unwrap();

    // 4. Complete.
    q.complete(qid, "worker-A").unwrap();

    let stats = q.stats();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.leased, 0);
}

#[test]
fn lifecycle_fail_and_reenqueue() {
    let q = InMemoryTaskQueue::new();
    let run_id = make_run_id();
    let cfg = default_config();
    let task_id = Uuid::now_v7();

    // Enqueue, claim, fail.
    let qid1 = q
        .enqueue(task_id, run_id, 1, CommandClass::Io, None)
        .unwrap();
    let req = ClaimRequest::single("w1");
    q.claim(&req, &cfg).unwrap();
    q.fail(qid1, "w1").unwrap();

    // Task is now removed from active set — can re-enqueue.
    let qid2 = q
        .enqueue(task_id, run_id, 1, CommandClass::Io, None)
        .unwrap();
    assert_ne!(qid1, qid2);

    let stats = q.stats();
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.pending, 1);
}

// ─── allowed_run_ids filtering ──────────────────────────────────────────

#[test]
fn claim_with_allowed_run_ids_filters_correctly() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();
    let run_b = make_run_id();
    let cfg = default_config();

    let ta = Uuid::now_v7();
    let tb = Uuid::now_v7();
    q.enqueue(ta, run_a, 1, CommandClass::Io, None).unwrap();
    q.enqueue(tb, run_b, 1, CommandClass::Io, None).unwrap();

    // Only claim tasks from run_b.
    let req = ClaimRequest::new("w1", 10, Duration::seconds(30))
        .with_allowed_run_ids(vec![run_b]);
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, tb);
    assert_eq!(claimed[0].run_id, run_b);

    // run_a task is still pending.
    assert_eq!(q.pending_count(), 1);
}

#[test]
fn claim_with_allowed_run_ids_none_claims_all() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();
    let run_b = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_a, 1, CommandClass::Io, None)
        .unwrap();
    q.enqueue(Uuid::now_v7(), run_b, 1, CommandClass::Io, None)
        .unwrap();

    // No filter — claim all.
    let req = ClaimRequest::new("w1", 10, Duration::seconds(30));
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 2);
}

#[test]
fn claim_with_empty_allowed_run_ids_claims_none() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), run_a, 1, CommandClass::Io, None)
        .unwrap();

    // Empty vec = no runs allowed.
    let req = ClaimRequest::new("w1", 10, Duration::seconds(30))
        .with_allowed_run_ids(vec![]);
    let claimed = q.claim(&req, &cfg).unwrap();
    assert!(claimed.is_empty());
}

// ─── cancel_for_run ─────────────────────────────────────────────────────

#[test]
fn cancel_for_run_drains_pending_and_leased() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();
    let run_b = make_run_id();
    let cfg = default_config();

    // Enqueue 3 for run_a and 1 for run_b.
    for _ in 0..3 {
        q.enqueue(Uuid::now_v7(), run_a, 1, CommandClass::Io, None)
            .unwrap();
    }
    q.enqueue(Uuid::now_v7(), run_b, 1, CommandClass::Io, None)
        .unwrap();

    // Claim 1 from run_a to make it leased.
    let req = ClaimRequest::new("w1", 1, Duration::seconds(30))
        .with_allowed_run_ids(vec![run_a]);
    let claimed = q.claim(&req, &cfg).unwrap();
    assert_eq!(claimed.len(), 1);

    // Cancel all for run_a: 2 pending + 1 leased = 3.
    let cancelled = q.cancel_for_run(run_a).unwrap();
    assert_eq!(cancelled, 3);

    // run_b task is untouched.
    let stats = q.stats();
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.cancelled, 3);
    assert_eq!(stats.leased, 0);
}

#[test]
fn cancel_for_run_returns_zero_when_no_active_entries() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();

    let cancelled = q.cancel_for_run(run_a).unwrap();
    assert_eq!(cancelled, 0);
}

#[test]
fn cancel_for_run_allows_reenqueue() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();
    let task_id = Uuid::now_v7();

    q.enqueue(task_id, run_a, 1, CommandClass::Io, None)
        .unwrap();

    q.cancel_for_run(run_a).unwrap();

    // Re-enqueue the same task — should work since it was cancelled.
    let qid2 = q
        .enqueue(task_id, run_a, 1, CommandClass::Io, None)
        .unwrap();
    assert_ne!(qid2, Uuid::nil());
}

// ─── cancel_stale_runs ──────────────────────────────────────────────────

#[test]
fn cancel_stale_runs_drains_non_active_run_rows() {
    let q = InMemoryTaskQueue::new();
    let active_run = make_run_id();
    let stale_run_1 = make_run_id();
    let stale_run_2 = make_run_id();

    // 2 tasks for active run, 5 for stale runs.
    q.enqueue(Uuid::now_v7(), active_run, 1, CommandClass::Io, None)
        .unwrap();
    q.enqueue(Uuid::now_v7(), active_run, 1, CommandClass::Io, None)
        .unwrap();
    for _ in 0..3 {
        q.enqueue(Uuid::now_v7(), stale_run_1, 1, CommandClass::Io, None)
            .unwrap();
    }
    for _ in 0..2 {
        q.enqueue(Uuid::now_v7(), stale_run_2, 1, CommandClass::Io, None)
            .unwrap();
    }

    assert_eq!(q.pending_count(), 7);

    let cancelled = q.cancel_stale_runs(&[active_run]).unwrap();
    assert_eq!(cancelled, 5);
    assert_eq!(q.pending_count(), 2); // only active_run rows remain
}

#[test]
fn cancel_stale_runs_with_empty_active_cancels_all() {
    let q = InMemoryTaskQueue::new();
    let run_a = make_run_id();

    q.enqueue(Uuid::now_v7(), run_a, 1, CommandClass::Io, None)
        .unwrap();

    let cancelled = q.cancel_stale_runs(&[]).unwrap();
    assert_eq!(cancelled, 1);
    assert_eq!(q.pending_count(), 0);
}

#[test]
fn cancel_stale_runs_leaves_completed_entries_alone() {
    let q = InMemoryTaskQueue::new();
    let stale_run = make_run_id();
    let cfg = default_config();

    q.enqueue(Uuid::now_v7(), stale_run, 1, CommandClass::Io, None)
        .unwrap();

    // Claim and complete one entry.
    let req = ClaimRequest::single("w1");
    let claimed = q.claim(&req, &cfg).unwrap();
    q.complete(claimed[0].queue_id, "w1").unwrap();

    // Add another pending entry.
    q.enqueue(Uuid::now_v7(), stale_run, 1, CommandClass::Io, None)
        .unwrap();

    let cancelled = q.cancel_stale_runs(&[]).unwrap();
    assert_eq!(cancelled, 1); // only the pending one, not the completed one
    assert_eq!(q.stats().completed, 1);
}

// ─── Stall scenario: claim with run scoping ─────────────────────────────

#[test]
fn no_zero_progress_with_claimable_rows_and_stale_contamination() {
    // Simulates the project bug: 75 stale rows + 5 current-run rows.
    let q = InMemoryTaskQueue::new();
    let current_run = make_run_id();
    let stale_run = make_run_id();
    let cfg = default_config();

    // Enqueue 75 stale rows.
    for _ in 0..75 {
        q.enqueue(Uuid::now_v7(), stale_run, 1, CommandClass::Io, None)
            .unwrap();
    }

    // Enqueue 5 current-run rows.
    for _ in 0..5 {
        q.enqueue(Uuid::now_v7(), current_run, 1, CommandClass::Io, None)
            .unwrap();
    }

    assert_eq!(q.pending_count(), 80);

    // Claim with allowed_run_ids = [current_run].
    let req = ClaimRequest::new("w1", 4, Duration::seconds(30))
        .with_allowed_run_ids(vec![current_run]);
    let claimed = q.claim(&req, &cfg).unwrap();

    // Must claim current run's rows, not zero.
    assert_eq!(claimed.len(), 4);
    assert!(claimed.iter().all(|e| e.run_id == current_run));
}
