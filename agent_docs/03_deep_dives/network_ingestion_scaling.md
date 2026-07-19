# Network Ingestion Scaling Strategies

To achieve ultra-fast statistical aggregation, `stat-pg` scales network ingestion capacity past standard single-threaded OS socket limits (~14.5M metrics/second) through a series of system-level and network-level optimizations.

---

## 1. Horizontal Process Scaling via `SO_REUSEPORT`

PostgreSQL is a process-per-connection architecture. Spawning a single ingestion thread binds the network socket to a single CPU core, bottlenecking performance.

### Mechanism
* `stat-pg` registers multiple background worker processes based on the GUC setting `stat_pg.workers` (default `4`, up to `16`).
* Each worker process runs `stat_pg_main` and binds a separate UDP socket to the same local port (`8125`) using the `SO_REUSEPORT` socket option.
* The Linux kernel automatically distributes incoming UDP packets across the available worker sockets using a hash of the packet's source IP and port.
* This allows network ingestion and memory aggregation to scale linearly across multiple physical CPU cores.

---

## 2. System Call Batching (`recvmmsg` and `sendmmsg`)

Context switching between user space and kernel space to read/write individual packets is extremely expensive. At 1M+ packets/second, system calls consume the majority of CPU cycles.

### Server-Side (`recvmmsg`)
* On Linux hosts, `stat-pg` utilizes the `recvmmsg` system call to pull up to `32` packets from the kernel socket queue in a single context switch.
* Non-Linux systems fall back to standard `recv_from` calls.

### Client-Side (`sendmmsg`)
* The benchmark client `stat-pg-bench` uses the `sendmmsg` system call on Linux to blast batches of `32` prebuilt packets (up to `3,840` metric updates) in a single system call.
* This drops benchmark CPU utilization and allows client systems to fully saturate the server's input capacity.

---

## 3. Socket Buffer Tuning

Linux default UDP socket receive and send buffers are relatively small (usually `208 KB`), causing rapid packet drops during transient spikes in database flush operations.

* **Server Ingestion Sockets**: Tuned using `SO_RCVBUF` to **`256 MB`** in `src/lib.rs`. This provides a massive kernel-level memory buffer to store incoming packets while the background worker is writing data to staging tables.
* **Client Sender Sockets**: Tuned using `SO_SNDBUF` to **`50 MB`** in `bench/src/main.rs` to prevent client-side network queue blockages during high-speed traffic generation.

---

## 4. MTU Saturation and Packet Packing

Standard Ethernet networks enforce an Maximum Transmission Unit (MTU) of `1,500` bytes. If a UDP packet exceeds this limit, the IP layer splits it into multiple fragments, increasing packet loss rates and router processing overhead.

* **Packet Sizing**: `stat-pg` packs updates up to a safety threshold of **`1,440` bytes** (well under the `1,472` byte UDP payload limit) to guarantee no IP fragmentation.
* **Mixed Variable-Length Format**: Updates use 2-byte IDs with mixed sizes (6-byte Format A for integers, 10-byte Format B for floats) to increase packing density. A single UDP packet can contain between `144` and `240` individual metric updates.

---

## 5. Profiling & Diagnostics (`pprof`)

To verify and troubleshoot hot-path bottlenecks, the extension integrates the `pprof` sampling profiler:
* The profiler is initialized on worker startup with a sample frequency of `99Hz`.
* On SIGTERM / database shutdown, each active worker dumps its sampling profile as an SVG flamegraph to `/workspace/flamegraph_<pid>.svg`.
* This allows developers to analyze locks, memory allocations, and parsing overhead under full production-level ingestion loads.
