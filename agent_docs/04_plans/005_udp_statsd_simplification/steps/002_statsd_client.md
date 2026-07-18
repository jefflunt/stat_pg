# Step: 002 - StatsD Proxy Client Daemon

## Goal
Create a host-native client daemon written in Rust that accepts local StatsD UDP packets, parses them, handles name-to-id SQL registration, aggregates metrics, and streams the binary ID-mapped format to `stat-pg` via UDP.

---

## Tasks
1. **Initialize Project**:
   * Create a new cargo binary subproject `client/` inside the root workspace.
2. **StatsD Parser**:
   * Bind to local UDP port `8125` (configurable).
   * Parse StatsD messages (e.g. `name:value|c`, `name:value|g`, `name:value|ms`).
   * Standardize:
     * For timers (`|ms`), append `_ms` suffix to the name, parse the time value, and aggregate it.
     * Keep a local thread-safe aggregation map `Arc<Mutex<HashMap<String, f64>>>`.
3. **Database Client (libpq integration)**:
   * Maintain a database connection to fetch/assign metric IDs.
   * On encountering a new name, execute the gap-finding registration query:
     ```sql
     BEGIN;
     LOCK TABLE stat_pg_metrics IN EXCLUSIVE MODE;
     INSERT INTO stat_pg_metrics (id, name) VALUES (...) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id;
     COMMIT;
     ```
   * Cache `name -> id` in-memory.
4. **Flusher Loop**:
   * Every 1 second (configurable), extract all accumulated metric names and values.
   * Lookup IDs from cache (or query if missing).
   * Package them into MTU-stuffed binary UDP packets: `[ID (4B LE i32)] [Value (8B LE f64)]`.
   * Send the UDP packets to the configured `stat-pg` server host and port.
