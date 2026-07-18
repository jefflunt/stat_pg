# Plan: 005 - UDP stats Ingestion & StatsD Client Proxy

## Goal
Simplify the metrics ingestion architecture. Replace the TCP-based background worker and bidirectional handshakes inside PostgreSQL with:
1. A stateless UDP listener on port 8125.
2. An array-based double-buffered accumulator inside the PG Background Worker.
3. Once-per-second database mapping refreshes to update index boundary limits and reclaim unused IDs.
4. A host-native StatsD UDP proxy client daemon that translates local StatsD traffic to the custom binary format and performs database metadata registration.

---

## Scope

### In-Scope
- **UDP Background Worker**:
  - Remove all TCP listener logic, client threads, and TCP stream parsing from `stat-pg`.
  - Bind to UDP port 8125.
  - Implement a lock-free bounds check inside the UDP thread against the currently allocated vector size.
- **Double-Buffering & Boundary Sync**:
  - Maintain a shared active buffer `Arc<Mutex<Vec<f64>>>`.
  - Every 1.0s, swap the active buffer with a pre-allocated clean buffer.
  - Refresh `max_valid_id` by executing a SQL query inside the SPI transaction.
  - Resize the flush buffer to `max_valid_id + 1` before making it available for the next cycle.
- **ID Assignment Reuse**:
  - Implement a gap-finding query when registering a new metric name to allocate the lowest available positive integer ID.
- **StatsD Proxy Client**:
  - Create a new binary client that reads UDP StatsD packets on local port 8125.
  - Standardize gauges/timers into counter sum increments.
  - Register new metric names via standard SQL queries to PostgreSQL.
  - Aggregate and stream binary ID-mapped tuples `[ID (4B)] [Value (8B)]` over UDP to the database.

### Out-of-Scope
- Storing or resolving timestamps on the client.
- Dynamic resizing of the active buffer in the UDP thread's hot path (new IDs are dropped for a maximum of 1s until the next flush sync).

---

## Architecture

```
  [StatsD Clients] ──(UDP StatsD)──> [Proxy Client Daemon (Rust)]
                                      ├── Registers names -> ID via libpq
                                      └── Flushes aggregates via UDP
                                                │
                                                ▼ (12-byte UDP updates)
                                      [stat-pg UDP BGWorker (Postgres)]
                                                │ (aggregates in memory)
                                                ▼ (swaps & flushes via SPI)
                                      [Postgres Event Tables]
```

---

## Implementation Steps
Detailed in the `steps/` subdirectory:
- **T001**: Implement the UDP aggregation worker and double-buffering in PostgreSQL.
- **T002**: Implement the StatsD UDP proxy client daemon in Rust.
- **T003**: Update the benchmark tool to support the new UDP protocol.
- **T004**: End-to-End verification and walkthrough report.

---

## Verification Plan
1. Rebuild the extension and restart the database.
2. Run database migration/setup verification.
3. Run the updated benchmark tool over UDP to verify raw speed and throughput.
4. Launch the StatsD proxy client, emit standard StatsD mock data, and verify they are correctly rolled up in the database tables.
