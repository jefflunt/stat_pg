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

### 3. Load the Extension
To run the DDL schema migrations (creating the tables `stat_pg_metrics`, `stat_pg_staging`, `stat_pg_secs`, `stat_pg_mins`, `stat_pg_hours`, `stat_pg_days`, and `stat_pg_30s`), load the extension in PostgreSQL:
```bash
docker compose exec db psql -U postgres -c "CREATE EXTENSION stat_pg;"
```
*(If updating, you can run `DROP EXTENSION stat_pg CASCADE; CREATE EXTENSION stat_pg;` to reload).*

### 4. Run the Ingestion Benchmark
Compile and run the host-native benchmark client to stress-test the ingestion limits:
```bash
cd bench
cargo build --release
./target/release/stat-pg-bench --host 127.0.0.1 --port 8125 --threads 4 --duration 10 --metrics 1000
```
#### Benchmark Operations
When `stat-pg-bench` starts, it automatically:
1. Truncates all event and staging tables, and performs a database `VACUUM ANALYZE` to clear indexes and optimize tables.
2. Registers simulated metric names (`bench.metric.[0-999]`) in the database to fetch their generated IDs.
3. Warms up for `2.0` seconds to allow background workers to query the database and synchronize their `max_valid_id` buffer boundaries.
4. Spawns threads to send packets containing 120 updates per UDP packet (mixed Format A/B). On Linux, it uses the optimized `sendmmsg` batch system call.
5. Performs a final database consistency validation to verify that total values match perfectly across all 5 resolution tables.

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
