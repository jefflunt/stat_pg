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

---

## Retired Proposals

### 5. Memory Prefetching to Hide Cache Latency
*   **Status**: **RETIRED**
*   **Reason**: Attempting to prefetch memory addresses using assembly instructions (`prefetcht0` / `prfm`) inside the metric loop halved throughput (dropping ingestion speed to 83M MPS). In a loop performing back-to-back updates, prefetch instructions saturated execution load ports in the CPU's load/store pipeline, causing execution queues to stall.

---

## Proposed Optimizations

### 6. Parallelized Multi-Worker Background Rollups (Candidate 3)
*   **Status**: **PROPOSED**
*   **Core Concept**: Parallelize the background rollup execution using multiple background workers to prevent index/lock contention.

### 7. Inline Assembly Parser (Aarch64 / x86_64 ASM)
*   **Status**: **PROPOSED**
*   **Core Concept**: Bypass compiler intermediate representations and enforce optimal register layout.
*   **Mechanism**: Write the decoding loop in raw assembly using conditional selects (`CSEL` on ARM, `CMOV` on x86) to run loop updates in CPU registers.

### 8. Kernel Bypass (AF_XDP)
*   **Status**: **PROPOSED**
*   **Core Concept**: Avoid the Linux kernel network stack and socket layer entirely.
*   **Mechanism**: Maps the network card (NIC) receive queue ring buffers directly into user space via zero-copy.
*   **Trade-offs**: Requires modern Linux kernels and privileged container access (`CAP_NET_ADMIN`).
