# Steps: Hierarchical Cascading Rollups (Candidate 1)

Detailed implementation plan for Phase 1.

---

## Step 1: DDL & External Functions in `src/lib.rs`
- Add DDL definition for `stat_pg_rollups`.
- Add DDL migration lines to modify `stat_pg_processed` (adding `total_latency_ms` and `merge_count` columns).
- Implement `stat_pg_udp_stats` external FFI function.

---

## Step 2: Implement Cascading Rollup inside `coordinator_merge`
- Add code to track current timestamp and initialize `stat_pg_rollups` entries if empty.
- Modify transaction to insert/merge only into `evt_secs` on every second.
- Implement conditional cascading checks to execute min/hour/day/30d rollups on boundary crossings.
- Wrap coordinator logic in duration tracker to compute elapsed time, writing totals to `stat_pg_processed`.

---

## Step 3: Update Benchmark Client `bench/src/main.rs`
- Update cleanup queries to truncate `stat_pg_rollups` and reset `stat_pg_processed`.
- Add startup and shutdown calls to `stat_pg_udp_stats()` to compute real kernel-level drops.
- Print average merge latency and kernel drops in the final summary report.
