//! Integration tests: high-contention scheduler/queue behavior.
//!
//! Focused on concurrency invariants introduced in CARD-R13-03.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::timeout;
use uuid::Uuid;

use yarli_core::domain::CommandClass;
use yarli_queue::{ClaimRequest, ConcurrencyConfig, InMemoryTaskQueue, QueueStatus, TaskQueue};

fn assert_single_lease_per_task(queue: &InMemoryTaskQueue, context: &str) {
    let mut leased_task_ids = HashSet::new();
    for entry in queue.entries() {
        if entry.status == QueueStatus::Leased {
            assert!(
                leased_task_ids.insert(entry.task_id),
                "{context}: duplicate active lease for task {}",
                entry.task_id
            );
            assert!(
                entry.lease_owner.is_some(),
                "{context}: leased entry {} missing lease owner",
                entry.task_id
            );
        }
    }
}

#[tokio::test]
async fn concurrency_stress_multi_worker_claim_contention() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let worker_count = 12;
    let run_count = 8;
    let tasks_per_run = 15;
    let task_total = run_count * tasks_per_run;

    let run_ids: Vec<Uuid> = (0..run_count).map(|_| Uuid::now_v7()).collect();
    let config = ConcurrencyConfig {
        per_run_cap: 3,
        io_cap: 5,
        cpu_cap: 3,
        git_cap: 2,
        tool_cap: 4,
    };

    for task_index in 0..task_total {
        let run_id = run_ids[task_index % run_count];
        let command_class = match task_index % 4 {
            0 => CommandClass::Io,
            1 => CommandClass::Cpu,
            2 => CommandClass::Git,
            _ => CommandClass::Tool,
        };
        let priority = (task_index % 7) as u32 + 1;
        queue
            .enqueue(Uuid::now_v7(), run_id, priority, command_class, None)
            .unwrap();
    }

    let claimed_queue_ids = Arc::new(Mutex::new(HashSet::new()));
    let run_progress = Arc::new(Mutex::new(HashMap::<Uuid, usize>::new()));
    let completed = Arc::new(AtomicUsize::new(0));

    let mut workers = Vec::new();
    for worker_id in 0..worker_count {
        let queue = Arc::clone(&queue);
        let claimed_queue_ids = Arc::clone(&claimed_queue_ids);
        let run_progress = Arc::clone(&run_progress);
        let completed = Arc::clone(&completed);
        let config = config.clone();
        let worker_name = format!("worker-{worker_id}");

        workers.push(tokio::spawn(async move {
            loop {
                if completed.load(Ordering::Acquire) >= task_total {
                    break;
                }

                let claimed = queue
                    .claim(
                        &ClaimRequest::new(&worker_name, 2, chrono::Duration::milliseconds(600)),
                        &config,
                    )
                    .unwrap();

                if claimed.is_empty() {
                    if queue.pending_count() == 0 && queue.stats().leased == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }

                assert_single_lease_per_task(&queue, "multi-worker contention");

                for entry in claimed {
                    {
                        let mut held = claimed_queue_ids.lock().await;
                        assert!(
                            held.insert(entry.queue_id),
                            "duplicate live lease for queue entry {}",
                            entry.queue_id
                        );
                    }

                    assert!(
                        queue.leased_count_for_run(entry.run_id) <= config.per_run_cap,
                        "per-run lease cap breached for {}",
                        entry.run_id
                    );
                    assert!(
                        queue.leased_count_for_class(entry.command_class)
                            <= config.cap_for(entry.command_class),
                        "per-class lease cap breached for {:?}",
                        entry.command_class
                    );

                    queue
                        .heartbeat(
                            entry.queue_id,
                            &worker_name,
                            chrono::Duration::milliseconds(300),
                        )
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(3)).await;
                    queue.complete(entry.queue_id, &worker_name).unwrap();

                    {
                        let mut held = claimed_queue_ids.lock().await;
                        assert!(held.remove(&entry.queue_id));
                    }

                    {
                        let mut progress = run_progress.lock().await;
                        *progress.entry(entry.run_id).or_default() += 1;
                    }

                    completed.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    for worker in workers {
        worker.await.expect("worker task failed");
    }

    assert_eq!(completed.load(Ordering::Acquire), task_total);

    let progress = run_progress.lock().await;
    assert_eq!(progress.values().sum::<usize>(), task_total);
    for run_id in &run_ids {
        let completed_for_run = progress.get(run_id).copied().unwrap_or(0);
        assert!(
            completed_for_run > 0,
            "run {run_id} had no completed tasks (starvation)"
        );
    }

    let held = claimed_queue_ids.lock().await;
    assert!(held.is_empty());

    let stats = queue.stats();
    assert_eq!(
        stats.pending, 0,
        "no pending entries should remain after test"
    );
    assert_eq!(
        stats.leased, 0,
        "all leases should be released after completion"
    );
    assert_eq!(stats.completed, task_total);
    assert_eq!(
        stats.pending + stats.leased + stats.completed + stats.failed + stats.cancelled,
        task_total,
        "queue depth accounting should remain exact"
    );
}

#[tokio::test]
async fn concurrency_stress_heartbeat_reclaim_cancellation() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let config = ConcurrencyConfig {
        per_run_cap: 2,
        io_cap: 3,
        cpu_cap: 2,
        git_cap: 1,
        tool_cap: 2,
    };

    let run_id = Uuid::now_v7();
    let task_total = 90;
    let cancel_target = 12;
    for _ in 0..task_total {
        queue
            .enqueue(Uuid::now_v7(), run_id, 4, CommandClass::Io, None)
            .unwrap();
    }

    let completed = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let reclaimed = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let active_workers_ready = Arc::new(AtomicBool::new(false));

    let reclaimer = {
        let queue = Arc::clone(&queue);
        let stop = Arc::clone(&stop);
        let reclaimed = Arc::clone(&reclaimed);
        tokio::spawn(async move {
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let reclaimed_count = queue
                    .reclaim_stale(chrono::Duration::milliseconds(5))
                    .unwrap();
                reclaimed.fetch_add(reclaimed_count, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(8)).await;
            }
        })
    };

    let canceller = {
        let queue = Arc::clone(&queue);
        let cancelled = Arc::clone(&cancelled);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            while !stop.load(Ordering::Acquire) {
                let limit = cancelled.load(Ordering::Acquire);
                if limit >= cancel_target {
                    break;
                }
                let candidate = queue
                    .entries()
                    .into_iter()
                    .find(|entry| entry.status == QueueStatus::Pending);

                if let Some(entry) = candidate {
                    if queue.cancel(entry.queue_id).is_ok() {
                        cancelled.fetch_add(1, Ordering::SeqCst);
                    }
                }

                tokio::time::sleep(Duration::from_millis(12)).await;
            }
        })
    };

    let active_leases = Arc::new(Mutex::new(HashSet::new()));

    let mut workers = Vec::new();
    for worker_id in 0..6 {
        let queue = Arc::clone(&queue);
        let stop = Arc::clone(&stop);
        let active_workers_ready = Arc::clone(&active_workers_ready);
        let completed = Arc::clone(&completed);
        let active_leases = Arc::clone(&active_leases);
        let config = config.clone();
        let worker_name = format!("claim-worker-{worker_id}");

        workers.push(tokio::spawn(async move {
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }

                if !active_workers_ready.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }

                let claimed = queue
                    .claim(
                        &ClaimRequest::new(&worker_name, 2, chrono::Duration::milliseconds(120)),
                        &config,
                    )
                    .unwrap();

                if claimed.is_empty() {
                    if queue.stats().pending == 0 && queue.stats().leased == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }

                assert_single_lease_per_task(&queue, "heartbeat-reclaim");
                for entry in claimed {
                    {
                        let mut active = active_leases.lock().await;
                        assert!(
                            active.insert(entry.queue_id),
                            "duplicate concurrent lease for task {}",
                            entry.task_id
                        );
                    }

                    queue
                        .heartbeat(
                            entry.queue_id,
                            &worker_name,
                            chrono::Duration::milliseconds(150),
                        )
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(4)).await;
                    queue.complete(entry.queue_id, &worker_name).unwrap();

                    {
                        let mut active = active_leases.lock().await;
                        assert!(active.remove(&entry.queue_id));
                    }

                    completed.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    let stalled = {
        let queue = Arc::clone(&queue);
        let stop = Arc::clone(&stop);
        let config = config.clone();
        tokio::spawn(async move {
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }

                let claimed = queue
                    .claim(
                        &ClaimRequest::new("stalled-worker", 1, chrono::Duration::milliseconds(50)),
                        &config,
                    )
                    .unwrap();

                if claimed.is_empty() {
                    if queue.stats().pending == 0 && queue.stats().leased == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(3)).await;
                    continue;
                }

                // This worker intentionally does not heartbeat to force stale lease recovery.
                let entry = &claimed[0];
                let queue_id = entry.queue_id;
                tokio::time::sleep(Duration::from_millis(180)).await;
                let _ = queue.complete(queue_id, "stalled-worker");
            }
        })
    };

    timeout(Duration::from_secs(3), async {
        while reclaimed.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected at least one stale lease reclamation event from stalled worker");

    active_workers_ready.store(true, Ordering::Release);

    let finish = timeout(Duration::from_secs(8), async {
        loop {
            let done = completed.load(Ordering::Acquire) + cancelled.load(Ordering::Acquire);
            if done >= task_total {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    finish
        .await
        .expect("concurrency stress with reclaim/cancel must finish within timeout");

    stop.store(true, Ordering::Release);

    for worker in workers {
        worker
            .await
            .expect("worker task join failed in heartbeat/reclaim/cancel test");
    }
    reclaimer.await.expect("reclaimer task join failed");
    canceller.await.expect("canceller task join failed");
    stalled.await.expect("stalled worker task join failed");

    assert!(
        reclaimed.load(Ordering::Acquire) > 0,
        "expected at least one reclaim event from stale heartbeat paths"
    );

    let completed_count = completed.load(Ordering::Acquire);
    let cancelled_count = cancelled.load(Ordering::Acquire);
    assert_eq!(completed_count + cancelled_count, task_total);

    {
        let active = active_leases.lock().await;
        assert!(active.is_empty());
    }

    assert_single_lease_per_task(&queue, "final check");

    let stats = queue.stats();
    assert_eq!(
        stats.pending + stats.leased,
        0,
        "no unfinished leased/pending entries"
    );
    assert_eq!(stats.completed + stats.cancelled, task_total);
    assert_eq!(
        stats.completed + stats.cancelled + stats.pending + stats.leased + stats.failed,
        task_total
    );
}
