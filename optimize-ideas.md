# Ingestion & Parsing Optimization Ideas

This document outlines architectural, system-level, and software-level strategies to scale the metrics ingestion and parsing throughput of the `stat-pg` background worker.

---

## Already Implemented Optimizations

### 1. Batching System Calls (`recvmmsg` / `sendmmsg`)
*   **Status**: **IMPLEMENTED**
*   **Core Concept**: Reduces user-to-kernel space context-switching frequency by receiving/sending multiple UDP packets in a single system call.
*   **Mechanism**: Uses `recvmmsg` on the database receiver workers and `sendmmsg` on the benchmark client.

### 2. Horizontal Thread Scaling (`SO_REUSEPORT`)
*   **Status**: **IMPLEMENTED**
*   **Core Concept**: Distributes the packet-receiving load across multiple CPU cores.
*   **Mechanism**: Spawns multiple UDP receiver worker threads, each binding a separate socket to port `8125` with `SO_REUSEPORT`.

### 3. Hierarchical Cascading Rollups (Candidate 1)
*   **Status**: **IMPLEMENTED**
*   **Core Concept**: Reduces high-frequency database lock and transaction merge times.
*   **Mechanism**: The background worker flushes only to `evt_secs` every second. Conditional checks lazy-aggregate up the chronological hierarchy (`evt_mins -> evt_hours -> evt_days -> evt_30s`) upon minute/hour/day epoch transitions.
*   **Results**: Coordinator merge transaction latency dropped from **31.38 ms to 4.76 ms** (a **6x speedup**).

### 4. Grouped/Columnar Packet Format (Candidate 2)
*   **Status**: **IMPLEMENTED**
*   **Core Concept**: Eliminate CPU branch predictor misses in the parser loop and optimize network payloads.
*   **Mechanism**: Restructured UDP payloads to pack updates in format-specific segments. The receiver processes each segment in a dedicated, extremely tight branchless loop where format checking is completely factored out.
*   **Results**: 
    *   Ingestion throughput rose from **177.6M MPS to 2.88 Billion MPS** (a **16.2x raw speedup**).
    *   Network byte overhead reduced by **27.4%** per metric update (down to **2.49 bytes/update**).

### 5. Concurrent Multi-Worker Background Rollups (Candidate 3)
*   **Status**: **IMPLEMENTED**
*   **Core Concept**: Parallelize the background rollup execution using multiple background workers to prevent index/lock contention.
*   **Mechanism**: The background worker flushes only to `stat_pg_secs` every second. Conditional checks lazy-aggregate up the chronological hierarchy (`stat_pg_mins -> stat_pg_hours -> stat_pg_days -> stat_pg_30s`) upon minute/hour/day epoch transitions.
*   **Dedicated Worker Pipeline**: Assign specialized responsibilities to individual worker IDs:
    *   Worker 0: performs staging merges (`staging_merge`) into `stat_pg_secs`.
    *   Worker 1: performs minute rollups (`rollup_minutes`) into `stat_pg_mins`.
    *   Worker 2: performs hour rollups (`rollup_hours`) into `stat_pg_hours`.
    *   Worker 3: performs day/30d rollups (`rollup_days`) into `stat_pg_days` / `stat_pg_30s`.
    *   Watermark Synchronization: Workers check `stat_pg_rollups` metadata to ensure lower-tier tables are fully aggregated before performing the next rollup tier, preventing race conditions.
*   **Results**: A completely lock-free parallel write pipeline, ensuring zero write-write conflicts across tables.

---

## Retired Proposals

### 6. Memory Prefetching to Hide Cache Latency
*   **Status**: **RETIRED**
*   **Reason**: Attempting to prefetch memory addresses using assembly instructions (`prefetcht0` / `prfm`) inside the metric loop halved throughput (dropping ingestion speed to 83M MPS). In a loop performing back-to-back updates, prefetch instructions saturated execution load ports in the CPU's load/store pipeline, causing execution queues to stall.

---

## High-Cardinality Scaling Proposals

### 7. Embedded LSM-Tree Ingestion (Fjall / RocksDB Crate)
*   **Status**: **PROPOSED**
*   **Core Concept**: Completely bypass B-Tree index write-amplification and page splits in the database.
*   **Mechanism**: Add a pure-Rust embedded LSM engine (e.g., `fjall` crate) into the extension. The background worker writes incoming metrics directly to a local WAL/MemTable on disk. Expose the data to PostgreSQL queries via Rust Set-Returning Functions (UDFs).
*   **Benefits**: Disk writes become sequential append-only operations, keeping write throughput high regardless of cardinality.

### 8. Partitioning event tables by Time (`at`)
*   **Status**: **PROPOSED**
*   **Core Concept**: Slice database tables into chronological chunks (hourly/daily).
*   **Mechanism**: Use native PostgreSQL declarative partitioning. Indices are created per-partition, meaning active index leaf nodes stay small and fit entirely inside the database `shared_buffers` cache.

### 9. Index FillFactor Tuning
*   **Status**: **PROPOSED**
*   **Core Concept**: Reduce leaf page splitting frequency during high-cardinality merges.
*   **Mechanism**: Alter primary key indexes to set `FILLFACTOR = 80`. This reserves 20% free space on each index page to absorb chronological inserts, delaying B-Tree page splits.

---

## Proposed Optimizations

### 10. Inline Assembly Parser (Aarch64 / x86_64 ASM)
*   **Status**: **PROPOSED**
*   **Core Concept**: Bypass compiler intermediate representations and enforce optimal register layout.
*   **Mechanism**: Write the decoding loop in raw assembly using conditional selects (`CSEL` on ARM, `CMOV` on x86) to run loop updates in CPU registers.

### 11. Kernel Bypass (AF_XDP)
*   **Status**: **PROPOSED**
*   **Core Concept**: Avoid the Linux kernel network stack and socket layer entirely.
*   **Mechanism**: Maps the network card (NIC) receive queue ring buffers directly into user space via zero-copy.
*   **Trade-offs**: Requires modern Linux kernels and privileged container access (`CAP_NET_ADMIN`).
