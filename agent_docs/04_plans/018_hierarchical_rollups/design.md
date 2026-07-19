# Design: Hierarchical Cascading Rollups (Candidate 1)

This design outlines Phase 1 of our three-candidate optimization roadmap: **Hierarchical Cascading Rollups** to eliminate 80% of index write-amplification on the ingestion hot path, combined with automated **Database Latency Tracking** and **Native Kernel UDP Drop Reporting**.

---

## 1. Goal Description
The background worker coordinator thread currently merges data into all 5 rollup tables every second. This causes significant index contention and write overhead. We propose writing only to `evt_secs` every second, while lazily cascading rollups on time boundary crossings.

---

## 2. Proposed Changes

### Database Table Schemas
1. **Rollup Schedules**:
   ```sql
   CREATE TABLE IF NOT EXISTS stat_pg_rollups (
       rollup_type VARCHAR(10) PRIMARY KEY,
       last_aggregated_at BIGINT NOT NULL
   );
   ```
2. **Analytics Latency Columns** in `stat_pg_processed`:
   ```sql
   ALTER TABLE stat_pg_processed 
   ADD COLUMN IF NOT EXISTS total_latency_ms DOUBLE PRECISION DEFAULT 0.0,
   ADD COLUMN IF NOT EXISTS merge_count BIGINT DEFAULT 0;
   ```

### Cascading Rollup Logic
During `coordinator_merge`:
* Retrieve the current timestamp.
* If a minute boundary is crossed (`current_time / 60 > last_aggregated_minute`):
  * Aggregate from `evt_secs` to `evt_mins` for the interval.
  * Update `last_aggregated_minute`.
* If an hour boundary is crossed (`current_time / 3600 > last_aggregated_hour`):
  * Aggregate from `evt_mins` to `evt_hours` (cascading).
  * Update `last_aggregated_hour`.
* Follow similar logic for daily and 30-day boundaries.

### Native Kernel Drop Function
```rust
#[pg_extern]
fn stat_pg_udp_stats() -> TableIterator<'static, (name!(received, i64), name!(dropped, i64))> {
    // Reads /proc/net/snmp inside database container
}
```

---

## 3. Verification Plan
* Rebuild and execute unit tests.
* Execute the 8-thread benchmark and verify printed latency and drop figures.
