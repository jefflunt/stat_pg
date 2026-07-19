# Ingestion & Parsing Optimization Ideas

This document outlines architectural, system-level, and software-level strategies to scale the metrics ingestion and parsing throughput of the `stat-pg` background worker.

---

## Already Implemented Optimizations

### 1. Batching System Calls (`recvmmsg` / `sendmmsg`)
*   **Status**: **IMPLEMENTED** (Commit `c2a72d6` & `b1b4ca9`).
*   **Core Concept**: Reduces user-to-kernel space context-switching frequency by receiving/sending multiple UDP packets in a single system call.
*   **Mechanism**: Uses `recvmmsg` on the database receiver workers and `sendmmsg` on the benchmark client.

### 2. Horizontal Thread Scaling (`SO_REUSEPORT`)
*   **Status**: **IMPLEMENTED** (Commit `b1b4ca9`).
*   **Core Concept**: Distributes the packet-receiving load across multiple CPU cores.
*   **Mechanism**: Spawns multiple UDP receiver worker threads, each binding a separate socket to port `8125` with `SO_REUSEPORT`.

---

## Active & Proposed Optimizations

### 3. Branchless Lookup & Fast-Path Partitioning
*   **Status**: **PROPOSED (Implementing)**.
*   **Core Concept**: Reduce instruction count and eliminate branch predictor thrashing in the packet parsing hot loop.
*   **Mechanism**: Partition the loop into a branchless fast path for the most common formats (increments, decrements, 8-bit integers, which make up 90% of traffic) using arithmetic calculation instead of match/jump tables, and fall back to a match statement only for less common formats.
*   **Benefits**: Substantial reduction in CPU branch misses and instruction count per update.

### 4. Memory Prefetching to Hide Cache Latency
*   **Status**: **PROPOSED (Implementing)**.
*   **Core Concept**: Hide L1/L2 cache miss latency when accessing the shared `active` aggregation array.
*   **Mechanism**: Use CPU prefetch instructions (`core::arch::prefetch`) to load the cache line of the metric ID processed in the next packet loop iteration.
*   **Benefits**: Up to 1.5x throughput improvement when processing large sets of unique metrics.

### 5. Grouped/Columnar Packet Format (Protocol Redesign)
*   **Status**: **PROPOSED**.
*   **Core Concept**: Remove format matching inside the parsing loop entirely.
*   **Mechanism**: Group updates of the same format together within packet segments. The receiver processes each segment in a dedicated, extremely tight branchless loop.
*   **Benefits**: Highly vectorizable loops (compiler auto-vectorization using SIMD).

### 6. Inline Assembly Parser (Aarch64 / x86_64 ASM)
*   **Status**: **PROPOSED**.
*   **Core Concept**: Bypass compiler intermediate representations and enforce optimal register layout.
*   **Mechanism**: Write the decoding loop in raw assembly using `core::arch::asm!` using conditional selects (`CSEL` on ARM, `CMOV` on x86) to run loop updates in CPU registers.

### 7. Kernel Bypass (AF_XDP)
*   **Status**: **PROPOSED**.
*   **Core Concept**: Avoid the Linux kernel network stack and socket layer entirely.
*   **Mechanism**: Address Address Family XDP (AF_XDP) maps the network card (NIC) receive queue ring buffers directly into user space via zero-copy.
*   **Trade-offs**: Requires modern Linux kernels and privileged container access (`CAP_NET_ADMIN`).
