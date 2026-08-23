# stat-pg User Guide

`stat-pg` is a high-performance, minimalist PostgreSQL extension written in Rust (using `pgrx`) that runs metric ingestion and hierarchical time-series rollups directly inside the PostgreSQL database engine.

This guide explains how `stat-pg` works, its architectural relationship to Etsy's `statsd`, why it adopts a radically minimal philosophy to achieve multi-million metrics/second ingestion, and how to configure, emit, and query metrics effectively.

---

## 1. Executive Overview & Architecture

Traditional time-series systems typically place a separate service (such as Prometheus, StatsD, Telegraf, or Carbon/Graphite) in front of storage. Metrics are buffered, scraped, or written over network APIs, adding context switches, serialization layers, and inter-service failure points.

`stat-pg` eliminates these layers by running as a **PostgreSQL Background Worker (`BGWorker`)**:

```
+-----------------------------------------------------------------------------------+
| Host Applications & Microservices                                                 |
| (Web Servers, Trading Engines, Workers, Daemons)                                  |
+-----------------------------------------------------------------------------------+
       │                                                         │
       │ (1. Optional StatsD Text lines)                         │ (2. High-speed Binary UDP)
       ▼                                                         │
+-----------------------------+                                  │
| stat-pg-client Proxy Daemon |                                  │
| - Ingests port 8126 (StatsD)|                                  │
| - Registers Name -> ID      |                                  │
| - Stuffs binary packets     |                                  │
+-----------------------------+                                  │
       │                                                         │
       └──────────────────────────┬──────────────────────────────┘
                                  │
                                  ▼ (Port 8125 UDP)
+───────────────────────────────────────────────────────────────────────────────────+
| PostgreSQL Engine (`stat-pg` Extension)                                           |
|                                                                                   |
|  [eBPF XDP Layer] (Optional Kernel Bypass: 13 GB/s direct NIC intercept)          |
|         │                                                                         |
|  [SO_REUSEPORT Multi-Process Sockets] (Worker 0 .. Worker N)                      |
|         │                                                                         |
|  [recvmmsg Batching + SIMD Vectorized Payload Parsing]                            |
|         │                                                                         |
|  [In-Memory Double-Buffered Array: `active_buffer` Vec<f64>]                      |
|         │                                                                         |
|  [Periodic 1.0s - 10.0s Flush: /dev/shm CSV COPY to `stat_pg_staging`]            |
|         │                                                                         |
|  [Coordinator Merge & Watermark-Synchronized Cascading Rollups]                   |
|         ├── stat_pg_secs   (1-second resolution)                                  |
|         ├── stat_pg_mins   (1-minute resolution)                                  |
|         ├── stat_pg_hours  (1-hour resolution)                                    |
|         ├── stat_pg_days   (1-day resolution)                                     |
|         └── stat_pg_30s    (30-day resolution)                                    |
+───────────────────────────────────────────────────────────────────────────────────+
                                  │
                                  ▼ (Standard SQL Queries / ActionCable WebSockets)
+───────────────────────────────────────────────────────────────────────────────────+
| Visualizations & Analytics (`stat_rails`, Dashboards, BI, Ad-hoc SQL)             |
+-----------------------------------------------------------------------------------+
```

---

## 2. Relationship to `statsd` & Design Philosophy

`stat-pg` was directly inspired by the core strengths of Etsy's `statsd`:
1. **Fire-and-forget over UDP**: Application threads must never block or experience latency spikes due to metric emission.
2. **Tolerance of minor packet loss**: For aggregated system monitoring, dropping transient datagrams during severe network partitions is preferable to crashing production services with backpressure.
3. **Packet stuffing & batching**: Packing multiple updates into single Ethernet MTUs (up to 1,440 bytes) to maximize network efficiency.

### Why `stat-pg` Takes a Radically Minimal Approach

While `statsd` implementations often maintain polymorphic metric types (timers, histograms, percentiles, sets, counters, gauges) and complex hash maps in memory, `stat-pg` achieves orders-of-magnitude higher throughput by stripping all polymorphic complexity out of the ingestion path:

| Feature / Concern | Traditional StatsD / Time-Series DBs | `stat-pg` Approach | Rationale for Extreme Performance |
|---|---|---|---|
| **Engine Data Types** | Polymorphic structs, Histograms, Sets, Gauges | **Single Flat Float (`f64`) Array** | Eliminates pointer dereferencing, dynamic dispatch, and branch mispredictions in the hot loop. |
| **Metric Identification** | Dynamic String keys in Hash Maps | **Pre-registered 2-byte Integer IDs** | Direct array index offset (`active_buffer[id] += val`). Zero hashing overhead per packet. |
| **Aggregation Semantics** | Calculated at ingestion time | **Deferred to Query Time in SQL** | Whether a metric represents a Counter (`SUM`), Gauge (`AVG`), or Min/Max is determined in PostgreSQL SQL queries. |
| **Network Protocol** | Variable-length text strings | **SIMD-parsed Packed Binary Datagrams** | 2 to 10 bytes per update. No ASCII float parsing (`atof`) in the critical ingestion path. |
| **Storage Pipeline** | Per-point row inserts / LSM trees | **Double-Buffered CSV COPY via `/dev/shm`** | Bypasses PostgreSQL WAL logging overhead using unlogged staging tables and sorted B-Tree sequential index insertions. |

---

## 3. Database Schema & Tables

When `CREATE EXTENSION stat_pg;` runs, the following tables are automatically initialized:

### 1. `stat_pg_metrics` (Metric Directory)
Maps human-readable metric names to 16-bit integer IDs.
```sql
CREATE TABLE stat_pg_metrics (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    type VARCHAR NOT NULL DEFAULT 'numeric'
);
```

### 2. Time-Resolution Rollup Tables
Metrics are consolidated into five discrete resolution tables with composite primary keys `(at, metric_id)`:
```sql
-- Seconds resolution: `at` is Unix epoch second
CREATE TABLE stat_pg_secs (
    metric_id INT NOT NULL,
    at BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (at, metric_id)
);

-- Minutes resolution: `at` is Unix epoch / 60
CREATE TABLE stat_pg_mins ( ... PRIMARY KEY (at, metric_id) );

-- Hours resolution: `at` is Unix epoch / 3600
CREATE TABLE stat_pg_hours ( ... PRIMARY KEY (at, metric_id) );

-- Days resolution: `at` is Unix epoch / 86400
CREATE TABLE stat_pg_days ( ... PRIMARY KEY (at, metric_id) );

-- 30-Day resolution: `at` is Unix epoch / 2592000
CREATE TABLE stat_pg_30s ( ... PRIMARY KEY (at, metric_id) );
```

---

## 4. Configuration Parameters (GUCs)

Configure `stat-pg` in `postgresql.conf` or via `ALTER SYSTEM`:

```ini
# postgresql.conf

# Required to load the background worker on startup
shared_preload_libraries = 'stat_pg'

# Target database and user
stat_pg.database = 'postgres'        # Database name where tables reside
stat_pg.user = 'postgres'            # Database user executing SPI transactions

# Network listener settings
stat_pg.bind_address = '0.0.0.0'     # UDP interface bind address
stat_pg.port = 8125                  # UDP port (default: 8125)

# Ingestion scaling & timing
stat_pg.workers = 4                  # Worker processes (1-16, default: 4, scales via SO_REUSEPORT)
stat_pg.threads_per_worker = 1       # UDP receiver threads per worker process (default: 1)
stat_pg.flush_interval = 10.0        # Seconds between memory-to-database flush cycles (default: 10.0)
```

> [!NOTE]
> Changes to `stat_pg.workers` require restarting the PostgreSQL service. Other parameters can be updated dynamically with `SELECT pg_reload_conf();`.

---

## 5. Emitting Metrics to `stat-pg`

You can send metrics to `stat-pg` using either of two methods:

### Method A: StatsD Text Format via `stat-pg-client` Proxy (Port `8126`)

For existing applications utilizing standard StatsD client libraries (Python, Node.js, Go, Ruby, Java):

1. Launch `stat-pg-client`:
   ```bash
   stat-pg-client --listen 127.0.0.1:8126 --server 127.0.0.1:8125 --db postgres://postgres:postgres@127.0.0.1:5432/postgres
   ```
2. Send standard StatsD lines over UDP to port `8126`:
   ```text
   api.requests:1|c
   database.query_time:45|ms
   orders.revenue:199.99|curr
   system.cpu.load:72.5|g
   ```
   `stat-pg-client` automatically registers unknown metric names in `stat_pg_metrics`, translates them to binary IDs, and batches updates to `stat-pg` on port `8125`.

### Method B: Native High-Performance Binary UDP Protocol (Port `8125`)

For high-throughput systems (e.g. trading engines or telemetry daemons):

1. **Pre-Register Metric**:
   Ensure metric name exists in `stat_pg_metrics` (e.g. `SELECT id FROM stat_pg_metrics WHERE name = 'api.requests';` yields `id = 42`).
2. **Pack UDP Datagram**:
   Format: `[num_segments (1B)]` followed by segments `[format_id (1B)] [num_updates (2B LE)] [updates...]`.
   - **Format 0 (Increment +1.0)**: `[metric_id (2B LE)]` (2 bytes per update)
   - **Format 1 (Decrement -1.0)**: `[metric_id (2B LE)]` (2 bytes per update)
   - **Format 2 (INT8 Value)**: `[metric_id (2B LE)] [value (1B i8)]` (3 bytes per update)
   - **Format 8 (INT32 Value)**: `[metric_id (2B LE)] [value (4B LE i32)]` (6 bytes per update)
   - **Format 14 (FLOAT64 Value)**: `[metric_id (2B LE)] [value (8B LE f64)]` (10 bytes per update)

---

## 6. Querying Metrics in PostgreSQL

Because `stat-pg` normalizes metric names into `stat_pg_metrics`, querying simply involves joining the desired resolution table:

### Recent Real-Time Activity (Seconds)
```sql
SELECT 
    m.name AS metric_name,
    to_timestamp(s.at) AS recorded_at,
    s.value
FROM stat_pg_secs s
JOIN stat_pg_metrics m ON s.metric_id = m.id
WHERE m.name = 'api.requests'
  AND s.at >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes')
ORDER BY s.at DESC;
```

### Hourly Trend / Gauge Aggregation (Hours)
```sql
SELECT 
    m.name AS metric_name,
    to_timestamp(h.at * 3600) AS hour_window,
    h.value AS total_or_sum
FROM stat_pg_hours h
JOIN stat_pg_metrics m ON h.metric_id = m.id
WHERE m.name = 'orders.revenue'
  AND h.at >= EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') / 3600
ORDER BY h.at ASC;
```

### Dynamic Aggregation Types (Counters vs. Gauges)
- **Counters / Cumulative Rates**: `SUM(s.value)`
- **Gauges / Resource Percentages**: `AVG(s.value)` or `s.value`
- **Peaks / Troughs**: `MAX(s.value)`, `MIN(s.value)`

---

## 7. Self-Monitoring & Health Diagnostics

`stat-pg` automatically tracks internal operational metrics under the `stat_pg.` namespace.

### Inspect System Health via SQL
```sql
SELECT * FROM stat_pg_self_metrics();
```

Output includes:
- `stat_pg.defined_metrics`: Total metric series registered.
- `stat_pg.bg_workers`: Active background worker processes.
- `stat_pg.cpu_consumed`: Current CPU utilization %.
- `stat_pg.ram_consumed`: Resident memory consumed in bytes.
- `stat_pg.disk_space_consumed`: Total database disk footprint.
- `stat_pg.udp_received` / `stat_pg.udp_dropped`: Kernel socket traffic and drop statistics.
- `stat_pg.udp_batch_size_avg`: Average `recvmmsg` batch efficiency.
- `stat_pg.flush_duration_ms` / `stat_pg.merge_duration_ms`: SPI pipeline latencies.

### Kernel Packet Counter Inspection
```sql
SELECT * FROM stat_pg_udp_stats();
```

---

## 8. Benchmarking & Stress Testing

`stat-pg` includes a dedicated benchmark utility in `bench/`:

```bash
# Build release benchmark binary
cargo build --release --manifest-path bench/Cargo.toml

# Run benchmark
./target/release/stat-pg-bench \
  --host 127.0.0.1 \
  --port 8125 \
  --db-port 5433 \
  --db-name postgres \
  --threads 8 \
  --readers 2 \
  --duration 60 \
  --metrics 1000 \
  --local-agg
```

- `--threads <u32>`: Parallel UDP sender threads.
- `--readers <u32>`: Concurrent SQL reader threads executing live aggregation queries.
- `--metrics <u32>`: Metric series cardinality (supports up to 64,000).
- `--local-agg`: Pre-aggregates metrics locally before transmission (enables multi-billion metric testing).
- `--db-port <u16>` / `--db-name <str>`: Target database metadata port and name.
