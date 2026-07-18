# Starvation Prevention & Queue Management

Under extreme ingestion loads (e.g., millions of metrics per second), background workers can experience loop starvation. This deep dive explains why starvation occurs and how `stat-pg` prevents it.

---

## The Starvation Scenario

In our multi-threaded architecture, client connection threads parse TCP messages and drop aggregated values into a channel (`std::sync::mpsc::channel`).
The main worker thread drains this channel in a loop:

```rust
// Legacy implementation (Vulnerable to starvation)
while let Ok(metric) = rx.try_recv() {
    cache.add(metric);
}
```

### The Infinite Loop Vulnerability
* If incoming metric rates are extremely high (e.g., 10M metrics/sec), the channel will accumulate items faster than the main thread can execute loop iterations.
* Because the channel is never empty, `rx.try_recv()` will continuously return `Ok(...)`.
* The thread becomes **trapped** inside the drainage loop indefinitely.
* Because it never exits the drainage loop, it never reaches the periodic flush logic:
  ```rust
  if elapsed.as_secs_f64() >= flush_sec {
      // Never reached!
  }
  ```
* **Consequences**: No data is written to the database (0 rows inserted), and memory usage spikes continuously as the cache grows without bound, ultimately causing an Out-Of-Memory (OOM) crash.

---

## The Solution: Capped Channel Drainage

To ensure the flusher regularly yields and executes transaction commits, we enforce a **fairness cap** on how many metrics can be pulled from the queue per iteration:

```rust
// Starvation-resistant implementation
while s.metrics_processed < 500_000 {
    if let Ok((metric_id, value, at)) = s.metric_rx.pop() {
        s.sliding_window.add(metric_id as usize, value, at, client)?;
        s.metrics_processed += 1;
    } else {
        break;
    }
}
```

### How it Works
1. **The Drainage Cap**: The loop pulls at most `500,000` items from the SPSC ring buffer.
2. **Exiting and Checking**: Once `metrics_processed` reaches `500,000`, the loop terminates. The thread proceeds immediately to the next steps:
    - Checks the flush timer.
    - Flushes aggregated metrics to PostgreSQL tables if `flush_interval` has passed.
    - Yields CPU execution to the operating system using `BackgroundWorker::wait_latch(Some(sleep_dur))` to let PostgreSQL handle internal interrupts and checkpoints.
3. **Looping**: On the next loop tick, the worker resumes draining the remaining queued channel items.

This threshold balances **high aggregate memory absorption** (so we drain the queue quickly) with **predictable write latency** (guaranteeing that SQL transactions commit on time).
