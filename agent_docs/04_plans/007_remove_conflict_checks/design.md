# COPY Protocol Ingestion Strategy Design

To scale metrics ingestion even further, we can bypass the SQL planner, executor, and B-Tree constraint checking completely by leveraging PostgreSQL's native bulk **`COPY`** protocol.

---

## Architectural Options

We have two primary options to implement `COPY` in our background worker system:

### Option A: Single-Worker Direct `COPY` (Deadlock-Free, Simple)
Revert the background worker process count back to **1**. Because a single worker process aggregates all incoming UDP packets in memory, it produces exactly one unique `(at, metric_id)` row per second.
* **Mechanism**: 
  1. The worker writes its active buffer to a CSV file `/workspace/scratch/flush_worker.csv` (takes <50µs for 1,000 metrics).
  2. The worker executes a single SPI command:
     `COPY evt_secs FROM '/workspace/scratch/flush_worker.csv' WITH (FORMAT csv);`
* **Pros**: 
  - Extremely fast (completely bypasses SQL parsing, planning, and index lookup on insert).
  - No database lock contention or staging tables required.
* **Cons**:
  - Does not utilize multiple worker processes (though a single worker is likely fast enough with `COPY` and `recvmmsg`).

### Option B: Multi-Worker Staging `COPY` (Scales to Multi-Core)
Retain the **8-worker process cluster** for maximum CPU parallelization.
* **Mechanism**:
  1. We create unindexed staging tables (e.g. `evt_secs_staging`) with **no primary key or unique constraints**.
  2. Each worker writes its aggregated metrics to a unique CSV `/workspace/scratch/flush_worker_{pid}.csv` and copies it into `evt_secs_staging`. Since there are no indexes, there is zero write contention.
  3. A single coordinator loop merges the staging table into the main indexed tables:
     ```sql
     INSERT INTO evt_secs (metric_id, at, value)
     SELECT metric_id, at, SUM(value) FROM evt_secs_staging GROUP BY metric_id, at
     ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_secs.value + EXCLUDED.value;
     TRUNCATE evt_secs_staging;
     ```
* **Pros**:
  - Full CPU scaling across 8 processes.
  - Zero lock contention during the high-speed UDP ingestion phase.
* **Cons**:
  - Higher database schema and query complexity.

---

## Proposed Changes

### Database Extension

#### [MODIFY] [lib.rs](file:///Users/jefflunt/code/stat-pg/src/lib.rs)
* Implement CSV writer helper function `write_flush_csv(pid, entries, ts)`.
* Implement the selected `COPY` execution logic inside `flush_slot`.

### Benchmark Tool

#### [MODIFY] [bench/src/main.rs](file:///Users/jefflunt/code/stat-pg/bench/src/main.rs)
* Add automatic `TRUNCATE ... CASCADE` and `VACUUM ANALYZE` to the startup block.
