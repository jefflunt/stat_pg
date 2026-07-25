# Architecture

`stat-pg` is a custom PostgreSQL extension designed to bypass traditional application ingestion pipelines and database triggers to achieve ultra-fast statistical aggregation.

---

## High-Level Threads & Ingestion Flow

```mermaid
graph TD
    Client[Client / Benchmark Systems] -->|UDP Packets on port 8125| NIC[Network Interface: eth0]
    
    subgraph eBPF Kernel Network Bypass Layer
        NIC -->|Direct Driver Intercept| XDP["eBPF XDP Driver (stat_pg_xdp.bpf.c)"]
        XDP -->|Lock-free per-CPU Accumulation| Map["BPF_MAP_TYPE_PERCPU_ARRAY (metrics_map)"]
    end

    XDP -->|XDP_PASS| OS[OS Socket Layer]

    subgraph PostgreSQL Cluster Processes
        OS -->|Load Balance via SO_REUSEPORT| Worker0["Background Worker 0 (Coordinator)"]
        OS -->|Load Balance via SO_REUSEPORT| WorkerN["Background Worker N (Aggregator)"]
        
        subgraph Worker 0 (Coordinator)
            Worker0 -->|spawn| UDP0[UDP Listener Thread Pool]
            UDP0 -->|recvmmsg batch + SIMD parse| Socket0[UDP Socket]
            Socket0 -->|Parse & Update| MutexBuf0["active_buffer (Mutex<Vec<f64>>)"]
            
            Main0[Main SPI Thread] -->|1. Swap local buffer every 1.0s| MutexBuf0
            Main0 -->|2. Write CSV| CSV0["File: /tmp/flush_pid0.csv"]
            CSV0 -->|3. SQL COPY| Staging[(stat_pg_staging unlogged table)]
            
            Main0 -->|4. coordinator_merge| Staging
            Staging -->|Aggregate & ORDER BY at, metric_id| EvtTables[(stat_pg_secs, stat_pg_mins, stat_pg_hours, etc.)]
        end

        subgraph Worker N (Aggregator)
            WorkerN -->|spawn| UDPN[UDP Listener Thread Pool]
            UDPN -->|recvmmsg batch + SIMD parse| SocketN[UDP Socket]
            SocketN -->|Parse & Update| MutexBufN["active_buffer (Mutex<Vec<f64>>)"]
            
            MainN[Main SPI Thread] -->|1. Swap local buffer every 1.0s| MutexBufN
            MainN -->|2. Write CSV| CSVN["File: /tmp/flush_pidN.csv"]
            CSVN -->|3. SQL COPY| Staging
        end
    end
```

---

## Detailed Execution Components

### 1. Multi-Process Parallel Ingestion (`SO_REUSEPORT`)
Postgres is historically single-threaded. To bypass single-core CPU throughput constraints, `stat-pg` supports horizontal scaling across multiple background worker processes:
* The number of workers is configured via the GUC setting `stat_pg.workers` (up to `16`, default `4`).
* Each worker process runs `stat_pg_main` and binds a separate UDP socket to the same port (default `8125`) using the `SO_REUSEPORT` socket option.
* The Linux kernel automatically load-balances incoming UDP packets at the socket layer across these worker sockets, scaling ingestion linearly with available CPU cores.

### 2. Decoupled Thread Architecture & Double Buffering
Within each background worker process, execution is split into two parts to prevent blocking the network receiver during database writes:
* **UDP Listener Thread**: Spawns a dedicated OS thread that receives UDP packets. It decodes the custom binary payload and updates the shared `active_buffer` (`Mutex<Vec<f64>>`) directly. It validates that the parsed metric ID is within the atomic bounds of `max_valid_id` to avoid out-of-bounds errors.
* **Main SPI Thread**: Runs the main loop of the PostgreSQL background worker. Every `flush_interval` (default `1.0` seconds), it swaps the `active_buffer` with its own `local_flush_buffer` inside a brief mutex lock. The UDP thread is freed immediately to continue packet consumption while the main thread writes the flushed metrics.

### 3. Linux `recvmmsg` Batching
To minimize user-to-kernel context switching overhead:
* Linux environments utilize the `recvmmsg` system call to retrieve batches of up to `32` packets in a single context switch.
* After a blocking batch retrieval, a non-blocking loop drains the remaining queued socket buffers.
* Non-Linux environments gracefully fall back to a standard blocking `recv_from` call.

### 4. COPY-Based Staging Pipeline & Coordinator Merge
Writing directly to target resolution tables under high load triggers high write amplification and transaction lock contention:
* **COPY to Staging**: Each worker formats its `local_flush_buffer` as CSV and dumps it to `/tmp/flush_<pid>.csv`. It then executes a high-speed `COPY` statement to load the data into the unlogged `stat_pg_staging` table. Unlogged tables bypass PostgreSQL WAL logging, dramatically reducing disk I/O.
* **Coordinator Merge**: Worker index `0` acts as the coordinator. In addition to staging its own data, it periodically:
  1. Creates a temporary table `stat_pg_temp` by selecting all rows from `stat_pg_staging`.
  2. Instantly runs `TRUNCATE stat_pg_staging` to release lock contention for other workers.
  3. Inserts and aggregates metrics from `stat_pg_temp` into the final resolution tables (`stat_pg_secs`, `stat_pg_mins`, `stat_pg_hours`, `stat_pg_days`, `stat_pg_30s`).
  4. Drops `stat_pg_temp`.

### 5. B-Tree Sequential Insertion Optimization
When merging data from the temporary table, the queries use an `ORDER BY at, metric_id` clause. This ensures that the rows are inserted in sequential order. In PostgreSQL, inserting sorted data into B-Tree primary key indexes (`(at, metric_id)`) avoids random page lookups, prevents page-split fragmentation, and maximizes write throughput.

### 6. Sampling Profile (`pprof`)
The workers register a `pprof` profile handler. Upon shutdown (SIGTERM), the worker outputs a CPU profile SVG flamegraph to `/workspace/flamegraph_<pid>.svg` to simplify diagnostics.
