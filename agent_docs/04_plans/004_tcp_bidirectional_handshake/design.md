# Plan: 004 - Bidirectional TCP Ingestion & ID-Based Aggregation

## Goal
Optimize ingestion performance and network utilization by switching from UDP to TCP and introducing a bi-directional metric name-to-integer ID registration handshake. This replaces string-based hashing with ultra-fast integer lookups in the database worker.

---

## Scope

### In-Scope
- **Schema Update**:
  - Introduce `stat_pg_metrics` lookup table mapping string names to sequential `id` integers.
  - Modify `evt_secs`, `evt_mins`, etc. to use `metric_id INT` instead of `code VARCHAR` as part of their primary key.
- **TCP Listener**:
  - Replace UDP socket with `TcpListener` on port 8125.
  - Multi-threaded handler spawning a reader thread per TCP client connection.
- **Bi-directional Handshake Protocol**:
  - **Type 0x01 (Register)**: Client requests ID for a string name; Server returns the 4-byte `metric_id`.
  - **Type 0x02 (Metric)**: Client sends `[0x02] [metric_id (4B)] [value (8B)] [timestamp (8B)]`.
- **Fast Aggregation**:
  - Use `HashMap<(i32, i64), f64>` with `FxHasher` or direct lookup arrays for aggregating metrics in memory before database flushes.

### Out-of-Scope
- Supporting concurrent UDP traffic (UDP listener will be fully replaced by TCP).

---

## Architecture

```
[stat-pg-bench (Client)]                         [PostgreSQL (stat-pg Server)]
   ├── Thread 1 ──(TCP Conn)──> [TcpListener] ──> [Reader Thread 1] ──
   ├── Thread 2 ──(TCP Conn)──> [TcpListener] ──> [Reader Thread 2] ──┼─> [MPSC Channel] ──> [Main Aggregator]
   └── Thread N ──(TCP Conn)──> [TcpListener] ──> [Reader Thread N] ──┘                           │ (Flushes to DB)
                                                                                                  v
                                                                                           [stat_pg_metrics]
                                                                                           [evt_secs, evt_mins, ...]
```

---

## Implementation Steps
Detailed in the `steps/` subdirectory:
- **T001**: Database schema migration script setup.
- **T002**: TCP server implementation and client registration handshake.
- **T003**: In-memory aggregation refactoring (using `(i32, i64)` keys).
- **T004**: Client benchmark tool update to support TCP and handshake mapping.
- **T005**: End-to-End verification and walkthrough report.

---

## Verification Plan
1. Rebuild and restart the container.
2. Run database migration tests to verify new schema and relationships.
3. Run the benchmark client tool:
   ```bash
   ./target/release/stat-pg-bench --threads 4 --duration 180 --metrics 1000
   ```
4. Confirm total throughput and check sum stability under dynamic monitoring.
