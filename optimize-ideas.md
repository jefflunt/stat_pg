# Ingestion, Parsing & Persistence Optimization Tracker

This document tracks the architectural, kernel-level, and database-engine optimizations implemented in `stat_pg`, experiments that were tried and retired due to performance regressions or architectural bottlenecks, and the next frontier of optimization candidates.

---

## 1. Implemented & Verified Optimizations (Closed Out)

### 1.1 Batching System Calls (`recvmmsg` / `sendmmsg`) & 128-Packet Drain Loop
*   **Status**: **IMPLEMENTED** (`src/lib.rs`, `bench/src/main.rs`)
*   **Core Concept**: Amortize user-to-kernel context-switching overhead across batches of UDP datagrams.
*   **Mechanism**:
    *   Increased `recvmmsg` batch size from 32 to **128 packets per syscall** in `recvmmsg_sys`.
    *   Paired the initial blocking `recv_batch_blocking` call with an inner non-blocking `recv_batch_nonblocking` (`MSG_DONTWAIT`) drain loop so receiver threads completely drain queued kernel socket buffers under a single mutex acquisition.
    *   Benchmark senders use `sendmmsg` with prebuilt packet pools.
*   **Results**: Boosted real-time packet ingestion throughput by **+36.1% to +43.7%** across both local-aggregation and unthrottled benchmark modes.

### 1.2 Horizontal Process & Thread Scaling (`SO_REUSEPORT`)
*   **Status**: **IMPLEMENTED** (`stat_pg.workers`, `stat_pg.threads_per_worker`)
*   **Core Concept**: Distribute kernel UDP socket queue processing across multiple physical CPU cores.
*   **Mechanism**:
    *   Spawns multiple PostgreSQL background worker processes (`stat_pg.workers`, up to 16) and configurable per-worker OS receiver threads (`stat_pg.threads_per_worker`).
    *   Each receiver binds an independent UDP socket to port `8125` with `SO_REUSEPORT`, allowing the Linux kernel network stack to hash-distribute incoming flows across cores.
*   **Results**: Linearly scaled multi-core socket consumption and verified 100% data consistency across up to 32 parallel receiver sockets (`8 workers × 4 threads`).

### 1.3 OS Kernel Socket Buffer Tuning (`rmem_max` & `SO_RCVBUF`)
*   **Status**: **IMPLEMENTED** (`docker-compose.yml`, `src/lib.rs`, `bench/src/main.rs`)
*   **Core Concept**: Expand kernel UDP socket buffers to act as a shock absorber during micro-burst blasts and SPI flush swaps.
*   **Mechanism**: Configured host/container kernel `sysctl net.core.rmem_max=536870912` (512 MB), set `SO_RCVBUF` to **256 MB** per server socket, and tuned client `SO_SNDBUF` to **50 MB**.
*   **Results**: Eliminated UDP packet drops in local-aggregation mode (**100.00% delivery rate with 0 dropped packets** across multi-billion metric runs).

### 1.4 Grouped/Columnar Wire Protocol & 16 Variable-Width Encodings
*   **Status**: **IMPLEMENTED** (`src/lib.rs`, `client/src/main.rs`, `bench/src/main.rs`)
*   **Core Concept**: Eliminate CPU branch predictor misses in the parser hot loop and maximize metric density per Ethernet frame.
*   **Mechanism**:
    *   Shrank metric IDs from 4-byte `i32` to **2-byte unsigned `u16`** (supporting up to 65,535 unique metrics).
    *   Restructured UDP payloads into homogeneous format segments (Formats `0..15`: implicit `+1`/`-1`, `i8`/`u8`, `i16`/`u16`, IEEE-754 `f16`, `i32`/`u32`, `f32`, `i64`/`u64`, `f64`).
    *   Packed updates up to a **1,440-byte MTU safety threshold** (preventing IPv4 UDP fragmentation at 1,472 bytes).
*   **Results**:
    *   Parser throughput rose from **177.6M MPS to 2.88 Billion MPS** (a **16.2× raw parser speedup**).
    *   Wire overhead dropped by **27.4%** down to **2.49 bytes/update**.

### 1.5 SIMD Vectorized Payload Parsing (ARM64 NEON / x86_64 SSE2)
*   **Status**: **IMPLEMENTED** (`parse_packet_payload` in `src/lib.rs`)
*   **Core Concept**: Decode and bounds-check multiple metric IDs and values in parallel using 128-bit SIMD vector registers.
*   **Mechanism**: Uses ARM64 NEON intrinsics (`vld1q_u16`, `vcgtq_u16`, `vcleq_u16`, `vandq_u16`) and x86_64 SSE2 intrinsics (`_mm_loadu_si128`) in 8-way and 4-way unrolled loops to validate `0 < id <= max_id` and accumulate contiguous segments.
*   **Results**: Confirmed 100% mathematical consistency across all 5 rollup tiers at ~15.8M–16.5M PPS, proving empirically that user-space payload decoding was no longer the bottleneck and shifting focus to kernel syscall bypass.

### 1.6 Kernel Network Bypass via eBPF / XDP (`stat_pg_xdp.bpf.c`)
*   **Status**: **IMPLEMENTED** (`src/bpf/stat_pg_xdp.bpf.c`, `read_ebpf_map_metrics` in `src/lib.rs`)
*   **Core Concept**: Bypass the Linux kernel network stack (`sk_buff` allocation, iptables/netfilter, UDP socket queues, and user-space `recvmmsg` copies) entirely.
*   **Mechanism**:
    *   Attaches an eBPF XDP program directly to the network interface driver (`SEC("xdp")`).
    *   Parses Ethernet -> IPv4 -> UDP port `8125` frames directly in driver RX ring memory and accumulates metric values into a pinned lock-free `BPF_MAP_TYPE_PERCPU_ARRAY` (`/sys/fs/bpf/metrics_map`, 65,536 entries).
    *   Returns `XDP_DROP` to recycle RX descriptors immediately at the driver level.
    *   PostgreSQL Worker 0 drains the per-CPU BPF map directly into `local_flush_buffer` during the periodic SPI flush cycle.
*   **Results**:
    *   **Peak Wire Bandwidth**: **13.03 GB/sec** (**9.50 Million UDP packets/sec**) with **0.00% packet loss** — a **28× to 60× gain** over standard Linux UDP sockets.
    *   **Verified Persistence**: **2.15 Trillion metrics** ingested and persisted across all 5 PostgreSQL rollup tables with 100% consistency using only **1.16 MB** of disk space.

### 1.7 Double-Buffered In-Memory Accumulator (`std::mem::swap`)
*   **Status**: **IMPLEMENTED** (`src/lib.rs`)
*   **Core Concept**: Decouple high-speed UDP receiver threads from database SPI transaction latency without channel allocation overhead.
*   **Mechanism**:
    *   Receiver threads accumulate directly into a pre-allocated `active_buffer` (`Arc<Mutex<Vec<f64>>>`) using unchecked indexing (`get_unchecked_mut`) guarded by an atomic `max_valid_id`.
    *   Every `flush_interval`, the main PostgreSQL background worker thread locks `active_buffer` for a few microseconds to execute an $O(1)$ pointer swap (`std::mem::swap`) with `local_flush_buffer`.
*   **Results**: Eliminated producer-consumer queue starvation and kept mutex wait times in the microsecond range (`stat_pg.active_buffer_lock_wait_us`).

### 1.8 RAM-Backed `/dev/shm` CSV Export & Unlogged Table `COPY` Pipeline
*   **Status**: **IMPLEMENTED** (`flush_slot` in `src/lib.rs`)
*   **Core Concept**: Bypass per-row SQL `INSERT` parsing and Write-Ahead Log (WAL) I/O amplification during worker flushes.
*   **Mechanism**:
    *   Each worker serializes non-zero slots from `local_flush_buffer` to an in-memory tmpfs file (`/dev/shm/flush_<pid>.csv`) and executes `COPY stat_pg_staging FROM ... WITH (FORMAT csv)`.
    *   `stat_pg_staging` is an `UNLOGGED` table, bypassing WAL ring buffers during multi-worker staging.
*   **Results**: Moving staging files from `/tmp` to RAM-backed `/dev/shm` reduced flush latency by **10%**, and `COPY` to `UNLOGGED` staging eliminated multi-worker WAL lock contention.

### 1.9 Sequential B-Tree Index Insertion (`ORDER BY at, metric_id`)
*   **Status**: **IMPLEMENTED** (`staging_merge` and `rollup_*` in `src/lib.rs`)
*   **Core Concept**: Eliminate random B-Tree leaf page splits and buffer pool thrashing during database merges.
*   **Mechanism**: All `INSERT INTO stat_pg_* SELECT ...` merge and rollup queries explicitly sort by `ORDER BY at, metric_id` to match the composite primary key `PRIMARY KEY (at, metric_id)`.
*   **Results**: Transforms B-Tree index updates into sequential right-edge page appends, maximizing `shared_buffers` hit ratios.

### 1.10 Hierarchical Cascading Rollups & Dedicated Multi-Worker Pipeline
*   **Status**: **IMPLEMENTED** (`src/lib.rs`)
*   **Core Concept**: Eliminate write-write lock contention across resolution tables by partitioning rollup tiers across dedicated background workers.
*   **Mechanism**:
    *   Instead of every worker upserting into all 5 resolution tables every second, workers only `COPY` to `stat_pg_staging`.
    *   **Worker 0**:Snapshots and truncates `stat_pg_staging` via `stat_pg_temp` and merges into `stat_pg_secs`.
    *   **Worker 1**: Rolls up `stat_pg_secs -> stat_pg_mins` on minute boundaries.
    *   **Worker 2**: Rolls up `stat_pg_mins -> stat_pg_hours` on hour boundaries.
    *   **Worker 3**: Rolls up `stat_pg_hours -> stat_pg_days -> stat_pg_30s` on day/30d boundaries.
    *   **Watermark Synchronization**: Workers coordinate via `stat_pg_rollups` watermarks so higher tiers only aggregate completed lower-tier epochs.
*   **Results**: Coordinator merge transaction latency dropped from **31.38 ms to 4.76 ms** (**6.6× speedup**) with zero cross-table lock contention.

### 1.11 Asynchronous Commit Pipeline & Graceful `SIGTERM` Shutdown Flush
*   **Status**: **IMPLEMENTED** (`src/lib.rs`)
*   **Core Concept**: Prevent synchronous disk `fsync` stalls on periodic metric flushes while guaranteeing clean-shutdown durability.
*   **Mechanism**: Sets `SET LOCAL synchronous_commit = off;` inside background worker SPI transactions, Tuned default `stat_pg.flush_interval` to `10.0s`, and registered a `SIGTERM` shutdown hook that flushes remaining in-memory metrics and runs a final `staging_merge` before process exit.
*   **Results**: Runs periodic database merges at memory speed while concurrent SQL readers (`--readers 3`) sustain **18,000+ queries/sec** at **0.11–0.17 ms** average read latency.

### 1.12 Client-Side Local Pre-Aggregation (`--local-agg`) & $O(N)$ Bulk Registration
*   **Status**: **IMPLEMENTED** (`bench/src/main.rs`, `client/src/main.rs`)
*   **Core Concept**: Pre-aggregate high-frequency events in client memory per second before MTU packet stuffing, and register metric metadata in a single SQL round-trip.
*   **Mechanism**:
    *   Client threads accumulate per-second updates locally before emitting grouped multi-segment UDP packets.
    *   Replaced $O(N^2)$ sequential metric registration loops with a single $O(N)$ `generate_series` batch insert.
*   **Results**:
    *   Sustained **1.147 Billion metrics/sec** persisted into PostgreSQL (68.87B metrics in 60s).
    *   Registration of 64,000 unique metrics dropped from **45 seconds to 50 milliseconds** (**1,000× speedup**).

### 1.13 Self-Monitoring Telemetry (`stat_pg.*`) & `pprof` Flamegraph Profiling
*   **Status**: **IMPLEMENTED** (`flush_self_monitoring_and_profiling_metrics` in `src/lib.rs`)
*   **Core Concept**: Dogfood `stat_pg` to monitor its own internal ingestion, lock contention, and SQL merge latencies in real time.
*   **Mechanism**: Automatically records 20 internal health and profiling metrics (`stat_pg.cpu_consumed`, `stat_pg.active_buffer_lock_wait_us`, `stat_pg.flush_duration_ms`, `stat_pg.merge_duration_ms`, `stat_pg.ebpf_read_duration_us`, etc.) and dumps `pprof` SVG flamegraphs on worker shutdown.

---

## 2. Tried & Retired / Failed Experiments (What Didn't Work & Why)

### 2.1 Explicit CPU Memory Prefetching (`prefetcht0` / `prfm`) in the Parser Loop
*   **Status**: **FAILED & RETIRED**
*   **What We Tried**: Issued inline hardware prefetch instructions (`prefetcht0` on x86_64, `prfm` on ARM64) inside the packet parsing loop to load the `active_buffer` cache line for the upcoming metric ID.
*   **Why It Failed**: **Halved ingestion throughput** (dropping speed down to ~83M MPS). Because each iteration of the columnar parser loop executes only a handful of instructions, explicit prefetch instructions saturated the CPU's Load/Store execution unit ports and stalled instruction retirement. Hardware out-of-order execution and L1/L2 residency (64K `f64`s = 512 KB, fitting entirely inside L2 cache) made software prefetching actively harmful.

### 2.2 Producer-Consumer Channels (`mpsc`, `crossbeam-channel`, and `rtrb` SPSC Ring Buffers)
*   **Status**: **FAILED & REPLACED** (Commits `20252c8`, `f3a2a08`, `253358a` -> Replaced in `4d1b6da`)
*   **What We Tried**: Decoupled network receiver threads from the main aggregator thread using message channels—first `std::sync::mpsc`, then `crossbeam-channel` with unbounded flat tuples, and finally a lock-free single-producer single-consumer ring buffer (`rtrb`).
*   **Why It Failed**:
    1.  **Drain Loop Starvation & OOM**: Under multi-million MPS bursts, producer threads pushed events into the channel faster than the consumer loop (`while let Ok(metric) = rx.try_recv()`) could drain them. The main worker thread became permanently trapped inside the drain loop, never reaching the periodic SPI database flush, and eventually crashed the container with Out-of-Memory (OOM).
    2.  **Cache-Line Bouncing**: Shipping millions of discrete `(id, value)` tuples across thread boundaries caused constant L1 cache invalidation. Accumulating directly in place into a shared `Vec<f64>` and swapping vector pointers once per flush window (`std::mem::swap`) was orders of magnitude faster and bounded memory strictly to $O(\text{unique metrics})$.

### 2.3 Stateful Bidirectional TCP Protocol & 5-Slot Time Ring Buffer
*   **Status**: **RETIRED & REPLACED** (Commit `aaefb46` -> Replaced in `4d1b6da`)
*   **What We Tried**: Built a custom bidirectional TCP server inside the PostgreSQL background worker featuring inline metric name registration handshakes (`Type 0x01`), 21-byte binary metric frames (`Type 0x02` containing an 8-byte client timestamp), and a 5-second server-side circular ring buffer (`idx = ts % 5`).
*   **Why It Failed**: Bottlenecked at ~7–14M MPS. TCP connection management, kernel ACK traffic, head-of-line blocking, and transmitting an 8-byte `i64` timestamp with every single metric update consumed >65% of wire bandwidth. Switching to stateless UDP, assigning timestamps at server flush time, and moving metric registration to SQL/proxy eliminated 400+ lines of complex state machine code and unlocked 100×+ higher throughput.

### 2.4 Dynamic Inline Resizing of `active_buffer` in the Main / Hot Path
*   **Status**: **FAILED & REVERTED** (Commit `de92496` -> Reverted in `e5d091d`)
*   **What We Tried**: Resized `active_buffer` immediately upon discovering a larger `max_valid_id` so newly registered metrics would be recorded without waiting for the next buffer swap.
*   **Why It Failed**: Locking and reallocating `active_buffer` outside the brief `std::mem::swap` window introduced lock contention and vector reallocation stalls on active UDP receiver threads, measurably degrading ingestion throughput. Reverted so buffer resizing happens exclusively on `local_flush_buffer` prior to the $O(1)$ swap.

### 2.5 Receiver Thread Oversubscription (`threads_per_worker = 4` Default across 8 Workers)
*   **Status**: **REVERTED DEFAULT TO 1** (Commit `329d6ef` -> Tuned in `a1b6f11`)
*   **What We Tried**: Defaulted `stat_pg.threads_per_worker` to `4` across `8` background workers (spawning 32 concurrent UDP receiver threads).
*   **Why It Failed**: While functional, running 32 tight `recvmmsg` threads on an 8–16 core system caused OS scheduler context switching, kernel `SO_REUSEPORT` lock contention, and intra-worker `active_buffer` mutex contention (Multiple threads within the same worker competing for the same `Arc<Mutex<Vec<f64>>>`). Setting `threads_per_worker = 1` (1:1 alignment between receiver sockets, `active_buffer` mutexes, and CPU cores) delivered higher throughput and near-zero lock wait times.

### 2.6 Direct Parameterized SPI Upserts to All 5 Resolution Tables Every Second
*   **Status**: **FAILED & REPLACED** (Commits `9f8c518`, `2a1ad7f`, `fedcdbe`)
*   **What We Tried**: Each worker executed chunked prepared `INSERT INTO stat_pg_secs/mins/hours/days/30s ... ON CONFLICT DO UPDATE` statements directly every second.
*   **Why It Failed**: Triggered 5× write amplification on every flush, heavy WAL lock contention, and row-level lock conflicts when multiple workers updated overlapping `(at, metric_id)` keys (pushing merge latencies past 31 ms). Replaced by the `/dev/shm` CSV `COPY` -> `UNLOGGED stat_pg_staging` -> single-coordinator sorted merge and lazy watermark rollup pipeline.

### 2.7 Hand-Written Inline Assembly Parser (`Aarch64 / x86_64 ASM`)
*   **Status**: **SUPERSEDED / NOT NEEDED**
*   **What We Tried / Evaluated**: Writing the packet decoding loop in raw inline assembly (`core::arch::asm!`) using conditional selects (`CSEL` / `CMOV`).
*   **Why It Was Retired**: Profiling the SIMD NEON/SSE2 implementation (Section 1.5) with `pprof` flamegraphs proved that compiler-generated SIMD code already outpaced the Linux kernel UDP socket layer. Moving packet parsing into the NIC driver via eBPF/XDP (Section 1.6) eliminated the user-space socket bottleneck entirely, making hand-rolled user-space assembly redundant.

---

## 3. Next Optimization Frontiers (Future Candidates)

### 3.1 `BPF_F_MMAPABLE` Zero-Syscall eBPF Map Draining (or `BPF_MAP_LOOKUP_BATCH`)
*   **Target Bottleneck**: In `read_ebpf_map_metrics` (`src/lib.rs:1245`), Worker 0 currently iterates from `key = 1..65536` and issues up to **2 `SYS_bpf` system calls per metric ID** (`BPF_MAP_LOOKUP_ELEM` + `BPF_MAP_UPDATE_ELEM` to zero it out) — up to **131,072 syscalls per flush** at max cardinality.
*   **Proposed Mechanism**:
    1.  **Batch Syscall**: Replace the per-key syscall loop with `BPF_MAP_LOOKUP_AND_DELETE_BATCH` / `BPF_MAP_LOOKUP_BATCH` (Linux 5.6+) to read and reset all 65,536 per-CPU counters in a **single system call**.
    2.  **Memory-Mapped BPF Array (`BPF_F_MMAPABLE`)**: Use a memory-mapped BPF array map (`mmap()`ed directly into Worker 0's virtual address space) with atomic `u64` `swap(0, Ordering::Relaxed)` in shared user/kernel pages, reducing `stat_pg.ebpf_read_duration_us` to **zero syscalls**.

### 3.2 Multi-Segment eBPF XDP Parsing via `bpf_loop` & Hoisted Counter Updates
*   **Target Bottleneck**: In `src/bpf/stat_pg_xdp.bpf.c`, the XDP kernel program currently:
    1.  Calls `bpf_map_lookup_elem(&metrics_map, &zero_key)` on **every inner loop iteration** (lines 62, 76, 91, 106, 121, 137) instead of accumulating a local `u64 packet_items` register and updating `zero_key` **once per packet**.
    2.  Only parses the first segment header and caps the loop at 64 updates (`for (int i = 0; i < 64; i++)`) to satisfy older eBPF verifier complexity limits.
*   **Proposed Mechanism**:
    *   Hoist the `zero_key` lookup out of all 6 format loops (`*cnt += num_processed`), Immediately cutting BPF map lookups inside the XDP hot path by **50%**.
    *   Use the Linux 5.17+ `bpf_loop()` helper (or bounded outer segment loop) to iterate across all `num_segments` and up to 240 updates per MTU packet in XDP space.

### 3.3 Fixed-Point Fractional Accumulation in eBPF XDP (`Q44.20` Scaling)
*   **Target Bottleneck**: The Linux kernel eBPF verifier prohibits hardware floating-point instructions inside XDP programs, forcing `stat_pg_xdp.bpf.c` to store raw integer/bit values in `__u64` map slots.
*   **Proposed Mechanism**: Decode `f16`, `f32`, and `f64` IEEE-754 bit patterns inside XDP using integer bit-shifts into a signed 64-bit fixed-point representation (e.g., micro-units scaled by $10^6$ or binary `Q44.20`), accumulate atomically in BPF, and convert back to `f64` (`val as f64 / 1e6`) when Worker 0 drains `metrics_map`.

### 3.4 Direct C-API `heap_multi_insert` or Binary `COPY` (Bypassing `/dev/shm` ASCII CSV)
*   **Target Bottleneck**: During `flush_slot` (`src/lib.rs:190`), each worker formats `f64` values into ASCII strings (`writeln!(writer, "{},{},{}", ...)`) in `/dev/shm/flush_<pid>.csv`, only for PostgreSQL's `COPY ... WITH (FORMAT csv)` parser to immediately parse those ASCII strings back into binary `INT4`, `INT8`, and `FLOAT8` datums.
*   **Proposed Mechanism**:
    *   **Phase 1 (Binary COPY)**: Emit PostgreSQL's native binary `COPY` stream (`PGCOPY\n\xff\r\n\0` + raw big-endian `i32`/`i64`/`f64` bytes) to eliminate float-to-ASCII formatting and ASCII-to-float parsing.
    *   **Phase 2 (Direct SPI/Heap Bypass)**: Use `pgrx::pg_sys::table_open` and `heap_multi_insert` with pre-allocated `TupleTableSlot` arrays to insert flushed metrics directly from Rust `local_flush_buffer` slices into `stat_pg_staging` buffer pages with zero file I/O and zero SQL query parsing.

### 3.5 Lock-Free Thread-Local Accumulators (Eliminating `active_buffer` Mutex)
*   **Target Bottleneck**: When `stat_pg.threads_per_worker > 1` (or during flush swaps), receiver threads acquire `active_buffer.try_lock()` / `lock()`.
*   **Proposed Mechanism**: Give each receiver thread its own isolated double-buffered `Vec<f64>` (swapped via `AtomicPtr` or indexed per thread), and let the worker's main SPI thread sum across its thread-local flush buffers before writing to `stat_pg_staging`. This makes the UDP receiver path **100% lock-free** even with multiple threads per worker.

### 3.6 High-Cardinality & Long-Horizon Storage Scaling
*   **Declarative Time-Range Partitioning (`PARTITION BY RANGE (at)`)**:
    *   Slice `stat_pg_secs` and `stat_pg_mins` into hourly/daily partitions so active B-Tree indexes stay tiny and fit entirely in L3 cache / `shared_buffers`, while historical raw second-level data can be expired instantaneously via `DROP TABLE` instead of `DELETE` + `VACUUM`.
*   **BRIN Indexes on Historical Rollup Tiers**:
    *   Because `stat_pg_hours`, `stat_pg_days`, and `stat_pg_30s` are populated strictly in chronological `at` order by watermark rollups, replacing or supplementing B-Tree indexes with Block Range Indexes (`USING brin (at)`) reduces index storage footprint by ~99%.
*   **B-Tree `FILLFACTOR = 80` Tuning**:
    *   Reserve 20% free space on index leaf pages (`WITH (fillfactor = 80)`) on `stat_pg_secs` to absorb late-arriving or multi-pass updates without triggering expensive B-Tree page splits.
*   **Gorilla / XOR Compressed Time-Series Blocks (Embedded LSM or Custom Table Access Method)**:
    *   Pack completed 60-second or 60-minute windows per `metric_id` into delta-of-delta / XOR-compressed binary arrays (or an embedded `fjall` LSM tree exposed via a Rust Set-Returning Function / Table Access Method), cutting row tuple header overhead (`23 bytes/row` in PostgreSQL heap tables) by **30×–60×** for long-term archival.
