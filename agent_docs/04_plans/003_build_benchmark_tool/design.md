# Plan: 003 - Build Benchmark Tool

## Goal
Build a high-performance, multi-threaded Rust CLI benchmark tool inside `bench/` to stress-test the `stat-pg` UDP extension, allowing users to measure ingestion limits on any hardware.

## Scope
- **In-Scope**:
  - A standalone Rust binary crate in `bench/`.
  - Multi-threaded UDP sender architecture using blocking `std::net::UdpSocket` for maximum throughput.
  - CLI argument parsing (target host, port, threads, duration, rate limits, metric count).
  - High-performance, zero-allocation serialization in the sender loop.
  - Real-time or summary statistics reporting (Total packets, duration, average Packets-Per-Second (PPS), throughput in MB/s).
- **Out-of-Scope**:
  - Verifying the database side in real-time during the benchmark (we will query Postgres manually after the run).

## Architecture

```
[stat-pg-bench (Host)]
 ├── Thread 1 ──> [UdpSocket 1] ──┐
 ├── Thread 2 ──> [UdpSocket 2] ──┼──> UDP Port 8125 ──> [PostgreSQL (stat-pg)]
 └── Thread N ──> [UdpSocket N] ──┘
```

Each thread spawns an independent UDP socket and loops, sending pre-allocated metric strings as fast as possible to the target address, bypassing locking and socket contention.

## Implementation Steps
Detailed in the `steps/` subdirectory:
- **T001**: Set up the `bench` crate structure and dependencies.
- **T002**: Implement CLI parser and multi-threaded sender engine.
- **T003**: Verify benchmark tool execution against the docker container.

## Verification Plan
1. Start the Docker Postgres server.
2. Run the benchmark tool with various thread counts:
   ```bash
   cd bench && cargo run --release -- -d 5 -t 4
   ```
3. Query the database to see the counts and verify that the extension processed the metrics stably under stress.
