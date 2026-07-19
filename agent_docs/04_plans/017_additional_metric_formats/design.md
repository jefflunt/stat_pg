# Additional Metric Formats Design (Updated Column Names)

We will scale the binary network protocol to support 16 distinct format types. We will also migrate the database resolution tables to use CPU-native `DOUBLE PRECISION` and implement a client-side currency encoder in the StatsD UDP proxy client.

---

## 1. Database Schema Migration

We will modify the target metrics database tables schema in `src/lib.rs` to use `DOUBLE PRECISION` (alias `float8`) instead of `DECIMAL(20, 12)`, and add a `type` column to `stat_pg_metrics`:

```sql
CREATE TABLE IF NOT EXISTS stat_pg_metrics (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    type VARCHAR NOT NULL DEFAULT 'numeric'
);
CREATE TABLE IF NOT EXISTS evt_secs (
    metric_id INT NOT NULL,
    at BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (at, metric_id)
);
```

---

## 2. Ingestion Design (0% Overhead)

* Ingestion continues to be O(1) array updates in memory. The database UDP receiver doesn't query metadata or schemas during UDP packet consumption.
* Value parsing is driven by the format tag byte in each update:
  `[Format (1B)] [Metric ID (2B LE i16)] [Value (0B / 1B / 2B / 4B / 8B)]`

---

## 3. Client-Side Currency Encoding (10,000x scaling)

In `client/src/main.rs`:
* We parse StatsD lines with `curr` or `currency` modifier, e.g. `payment.amount:12.50|curr`.
* We multiply currency values by `10,000` to convert them to integers.
* The proxy client registers the metric name with `type = 'currency'` in `stat_pg_metrics`.
* Transmitted values are encoded in the most compact integer formats (e.g. `i32` or `i16`).
* When querying, values are divided by `10,000.0` (e.g. `value / 10000.0`).
