# Architecture

`stat-pg` is a custom PostgreSQL extension designed to bypass traditional application ingestion pipelines and database triggers to achieve ultra-fast statistical aggregation.

---

## High-Level Threads & Ingestion Flow

```mermaid
graph TD
    Client[Client Systems] -->|UDP Packets| Socket[UDP Port 8125]
    
    subgraph PostgreSQL Background Worker Process
        Socket -->|read| RecvThread["Receiver Thread (OS thread)"]
        RecvThread -->|parse metrics| Channel[Rust mpsc::channel]
        
        subgraph Main worker Thread (thread-local context)
            Channel -->|try_recv| QueueDrain[Queue Drainer]
            QueueDrain -->|aggregate| Cache[(MetricCache HashMap)]
            Cache -->|periodic flush| Tx[PG Transaction Context]
            Tx -->|SPI batch write| DB[(PostgreSQL Tables)]
        end
    end
```

---

## Detailed Execution Components

### 1. Decoupled Thread Architecture
PostgreSQL is single-threaded and expects all backend C APIs to run on the main process thread. To avoid thread-local storage corruption, `stat-pg` divides its work:
* **OS Receiver Thread**: Binds the UDP socket and calls `recv_from`. It is written using only pure Rust standard library APIs (no `pgrx` or `pg_sys` functions). It parses text payloads into structured `Metric` instances and sends them into a thread-safe channel.
* **Main Backend Thread**: Drains the channel, aggregates metrics into an in-memory accumulator map, checks the flush timer, and connect to Postgres internals.

### 2. In-Memory Aggregation (MetricCache)
Instead of executing a database write for every incoming packet, metrics are aggregated in-memory:
* Metrics are summed across five granularity resolutions: second (`evt_secs`), minute (`evt_mins`), hour (`evt_hours`), day (`evt_days`), and 30-day (`evt_30s`).
* A single metric code sent 100,000 times in a second only results in **1 cache entry update** in memory, reducing write amplification to the database tables.

### 3. Starvation Prevention
Under peak fires of UDP traffic, the channel can fill up faster than the main thread can aggregate metrics. To prevent the flusher from getting stuck in the queue drainage loop indefinitely, the worker drains a maximum of `100,000` metrics per iteration before yielding to check the timer and run the flush.

### 4. Transaction-Safe SPI Flush
Every `1.0` seconds (configurable), the worker swaps the metrics cache and initiates a database transaction using:
```rust
BackgroundWorker::transaction(|| {
    Spi::connect_mut(|client| { ... })
});
```
It flushes the aggregates into the tables using high-speed parameterized upserts:
```sql
INSERT INTO table (code, at, value) VALUES ($1, $2, $3)
ON CONFLICT (at, code) DO UPDATE SET value = table.value + EXCLUDED.value;
```
This guarantees that all metrics updates are written in transaction-safe batches.
