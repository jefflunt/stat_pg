# Domain Model

This document outlines the core data structures, table definitions, and binary network protocols used by `stat-pg`.

---

## Metric Resolutions

The aggregator computes and stores statistics at five distinct time resolutions:

| Table | Resolution | Key Formula (`at`) |
| :--- | :--- | :--- |
| `stat_pg_secs` | Second | `at` (Epoch timestamp) |
| `stat_pg_mins` | Minute | `at / 60` |
| `stat_pg_hours` | Hour | `at / 3600` |
| `stat_pg_days` | Day | `at / 86400` |
| `stat_pg_30s` | 30-Day | `at / 2592000` |

---

## PostgreSQL Database Schema

The extension interacts with the following database tables. The schema SQL is registered and automatically run by the extension on startup:

### 1. `stat_pg_metrics` (Lookup Table)
Maps unique text metric names to 32-bit integer IDs.
```sql
CREATE TABLE stat_pg_metrics (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE
);
```

### 2. `stat_pg_staging` (Unlogged Table)
A fast staging table used to receive database imports from individual worker files without WAL overhead.
```sql
CREATE UNLOGGED TABLE stat_pg_staging (
    metric_id INT NOT NULL,
    at BIGINT NOT NULL,
    value DECIMAL(20, 12) NOT NULL
);
```

### 3. Metric Resolution Tables (`stat_pg_secs`, `stat_pg_mins`, `stat_pg_hours`, `stat_pg_days`, `stat_pg_30s`)
Stores aggregated value data at various time granularities.
```sql
CREATE TABLE stat_pg_secs (
    metric_id INT NOT NULL,
    at BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (at, metric_id)
);
```
*(All other resolution tables `stat_pg_mins`, `stat_pg_hours`, `stat_pg_days`, and `stat_pg_30s` use the exact same schema structure).*

---

## UDP Protocol Format

To minimize packet payload size, `stat-pg` utilizes a variable-length binary packet encoding format using **2-byte metric IDs** (with mixed float and integer value encoding).

Each update block is parsed dynamically depending on the sign bit (bit 15) of the 2-byte ID:

### Format A: INT32 Update (6 Bytes total)
Used for integer values (typically counters) to save network bandwidth.
* **Condition**: First `i16` read is `< 0` (most significant bit is set).
* **Decoding**:
  - `metric_id` = `raw_id & 0x7FFF` (recovers the positive integer ID).
  - `value` = 4-byte little-endian `i32` integer (cast to `f64` in memory).
* **Length**: 6 bytes.

### Format B: FLOAT64 Update (10 Bytes total)
Used for arbitrary high-precision float values.
* **Condition**: First `i16` read is `>= 0` (most significant bit is not set).
* **Decoding**:
  - `metric_id` = `raw_id` (used as-is).
  - `value` = 8-byte little-endian `f64` float.
* **Length**: 10 bytes.

### MTU Saturation
Multiple updates are packed contiguously into a single UDP packet up to the MTU safety limit of **`1,440` bytes** (approximately `120` updates of 12-byte equivalents, or up to `240` updates of 6-byte integer format).

> [!WARNING]
> The current `stat_pg_client` proxy daemon still formats UDP packets using the old `12-byte` TCP protocol format (4-byte ID + 8-byte value) and is incompatible with the updated UDP variable-length parser in the extension. The high-performance `stat_pg_bench` stress-test client implements the new 2-byte mixed payload format.
