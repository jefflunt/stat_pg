# Architecture

`stat-pg` is a custom PostgreSQL extension designed to bypass traditional application ingestion pipelines and database triggers to achieve ultra-fast statistical aggregation.

---

## High-Level Threads & Ingestion Flow

```mermaid
graph TD
    Client[Client Systems] -->|TCP Streams| Listener[TCP Port 8125]
    
    subgraph PostgreSQL Background Worker Process
        Listener -->|accept| Connection["Client Stream Threads (Concurrent OS threads)"]
        Connection -->|1. RegRequest Handshake| RegChannel[mpsc Reg Channel]
        Connection -->|2. Stream 0x02 Packets| MetricChannel[mpsc Metric Channel]
        
        subgraph Main worker Thread (thread-local context)
            RegChannel -->|register| SPI_Reg[SPI Registration Transaction]
            SPI_Reg -->|insert/lookup| Cache[(shared name_to_id cache)]
            
            MetricChannel -->|try_recv| QueueDrain[Queue Drainer]
            QueueDrain -->|aggregate| SlidingWin[(SlidingWindow Circular Array)]
            SlidingWin -->|periodic flush| Tx[PG Transaction Context]
            Tx -->|SPI parameterized batch write| DB[(PostgreSQL Rollup Tables)]
        end
    end
```

---

## Detailed Execution Components

### 1. Decoupled Thread Architecture
PostgreSQL is single-threaded and expects all backend C APIs to run on the main process thread. To avoid thread-local storage corruption and satisfy compile-time UnwindSafe bounds, `stat-pg` divides its work:
* **Concurrent OS Stream Threads**: Binds a non-blocking TCP listener. For every incoming socket connection, it spawns a dedicated client reader thread. These threads read incoming buffers, execute name registration handshakes, and stream parsed metric updates into a thread-safe channel.
* **Main Backend Thread**: Drains the channel, processes registration queries, aggregates metric values into the sliding aggregation window, checks the flush timer, and connects to Postgres database internals.

### 2. In-Memory Aggregation (SlidingWindow)
Instead of executing a database write for every incoming packet, metrics are aggregated in-memory:
* Metrics are aggregated across a circular buffer array of size `buffer_window` (default `5` seconds).
* Slots in the array are directly indexed using `timestamp % window_size`, providing $O(1)$ constant-time lookup and aggregate updates without hash collisions.
* Metrics are flushed to five granularity resolutions: second (`evt_secs`), minute (`evt_mins`), hour (`evt_hours`), day (`evt_days`), and 30-day (`evt_30s`).

### 3. Starvation Prevention
Under peak fires of TCP traffic, the channels can fill up faster than the main thread can aggregate metrics. To prevent the flusher from getting stuck in the queue drainage loop indefinitely, the worker drains a maximum of `500,000` metrics per iteration before yielding to check the timer and run the flush.

### 4. Transaction-Safe SPI Flush
Every `1.0` seconds (configurable), the worker swaps the metrics cache and initiates a database transaction using:
* Parameterized batch prepared statements (chunks of `5000`, `500`, `1`) to avoid SegFaults by aligning datums to exact OIDs (`INT4OID`, `INT8OID`, `FLOAT8OID`).
* Upserts targeting integer foreign keys:
  ```sql
  INSERT INTO table (metric_id, at, value) VALUES ($1, $2, $3)
  ON CONFLICT (at, metric_id) DO UPDATE SET value = table.value + EXCLUDED.value;
  ```
