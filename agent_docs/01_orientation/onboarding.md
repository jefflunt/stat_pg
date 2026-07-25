# Onboarding and Setup

This document describes how to set up, compile, test, and run the `stat-pg` PostgreSQL extension and its benchmarking client.

---

## Environment Dependencies

To run the development environment, the host system only needs:
1. **Docker & Docker Compose**

All other developer toolchains (Rust stable, `cargo-pgrx`, LLVM, Clang, PostgreSQL developer headers) are pre-packaged inside our development container.

---

## Step-by-Step Instructions

### 1. Build and Start the Container
Start the containerized PostgreSQL 16 instance:
```bash
docker compose up --build -d
```
* **PostgreSQL** is bound to host port **`5433`** to prevent conflicts with local database installations.
* **UDP Socket Listener** binds to host port **`8125`**.
* The container automatically preloads the compiled `stat_pg` extension on startup using `shared_preload_libraries`.

### 2. Run the Test Suite
The codebase includes unit tests and live database integration tests (`#[pg_test]`):
```bash
docker compose exec db cargo test
```

### 3. Load the Extension & Attach eBPF XDP Bypass
To run DDL schema migrations (creating `stat_pg_metrics`, `stat_pg_staging`, `stat_pg_secs`, `stat_pg_mins`, `stat_pg_hours`, `stat_pg_days`, and `stat_pg_30s`), load the extension in PostgreSQL:
```bash
docker compose exec db psql -U postgres -c "CREATE EXTENSION stat_pg;"
```

To enable eBPF XDP kernel network bypass:
```bash
# 1. Compile eBPF program
docker compose exec db clang -g -O2 -target bpf -I/usr/include/aarch64-linux-gnu -c /workspace/src/bpf/stat_pg_xdp.bpf.c -o /workspace/src/bpf/stat_pg_xdp.bpf.o

# 2. Load eBPF program into kernel and pin maps
docker compose exec --user root db bpftool prog load /workspace/src/bpf/stat_pg_xdp.bpf.o /sys/fs/bpf/stat_pg_xdp type xdp pinmaps /sys/fs/bpf/
docker compose exec --user root db chmod 777 /sys/fs/bpf /sys/fs/bpf/metrics_map /sys/fs/bpf/stat_pg_xdp

# 3. Attach eBPF XDP driver hook to eth0
docker compose exec --user root db bpftool net attach xdpgeneric pinned /sys/fs/bpf/stat_pg_xdp dev eth0
```

### 4. Run the Ingestion Benchmark
Compile and run the host-native benchmark client to stress-test ingestion limits:
```bash
cargo run --release --manifest-path bench/Cargo.toml -- --host 127.0.0.1 --port 8125 --threads 14 --readers 2 --duration 60 --metrics 1000 --local-agg
```
#### Benchmark Options
- `--threads <u32>`: Number of parallel UDP metric sender threads.
- `--readers <u32>`: Number of concurrent SQL reader threads executing random metric aggregation queries.
- `--metrics <u32>`: Number of unique metric time series (supports up to `64000`).
- `--local-agg`: Enables client-side local aggregation mode for multi-billion metric persistence density.
- `--duration <u32>`: Duration of test in seconds.

#### Benchmark Execution Flow
When `stat-pg-bench` starts, it automatically:
1. Truncates all event and staging tables, and performs a database `VACUUM ANALYZE`.
2. Registers simulated metric names (`bench.metric.[0-N]`) using an $O(N)$ bulk `generate_series` query (~50ms for 64,000 metrics).
3. Warms up for `2.0` seconds to allow background workers to synchronize `max_valid_id` boundaries.
4. Blasts 16 mixed payload formats using `sendmmsg` UDP batching.
5. Performs normalized database consistency validation across all 5 resolution tables.

---

## Configuration GUC Parameters

The extension registers custom settings (GUCs) in `postgresql.conf`:

```ini
shared_preload_libraries = 'stat_pg'

stat_pg.database = 'postgres'        # Ingestion target database name
stat_pg.user = 'postgres'            # Database connection username
stat_pg.bind_address = '0.0.0.0'     # UDP interface bind address
stat_pg.port = 8125                  # UDP port to listen on
stat_pg.flush_interval = 1.0         # Seconds interval to dump CSV and load to staging
stat_pg.buffer_window = 5            # Aggregator sliding window size (unused in current UDP logic)
stat_pg.workers = 4                  # Number of BGWorker processes to spawn (up to 16, default 4)
```

> [!NOTE]
> Changes to `stat_pg.workers` require restarting the PostgreSQL server because background workers are loaded during postmaster startup. Other parameters can be reloaded using `SIGHUP` (`SELECT pg_reload_conf();`).
