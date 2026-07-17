# Onboarding and Setup

This document describes how to set up, compile, test, and run the `stat-pg` PostgreSQL extension and its benchmarking client.

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
* **UDP Socket Listener** is bound to host port **`8125`**.
* The container automatically preloads the compiled `stat_pg` extension on startup using `shared_preload_libraries`.

### 2. Run the Test Suite
The codebase includes unit tests (for packet parsing) and live database integration tests (`#[pg_test]`):
```bash
docker compose exec db cargo test
```

### 3. Load the Extension
To run the DDL schema migrations (creating the tables `evt_secs`, `evt_mins`, `evt_hours`, `evt_days`, and `evt_30s`), load the extension in PostgreSQL:
```bash
docker compose exec db psql -U postgres -c "CREATE EXTENSION stat_pg;"
```
*(If updating, you can run `DROP EXTENSION stat_pg; CREATE EXTENSION stat_pg;` to reload).*

### 4. Run the Ingestion Benchmark
Compile and run the host-native benchmark client to stress-test the ingestion limits:
```bash
cd bench
cargo build --release
./target/release/stat-pg-bench --threads 4 --duration 5 --metrics 1000 --batch 20
```

---

## Configuration GUC Parameters

The extension registers custom settings (GUCs) in `postgresql.conf`:

```ini
shared_preload_libraries = 'stat_pg'

stat_pg.database = 'postgres'        # Ingestion target database
stat_pg.user = 'postgres'            # Database connection user
stat_pg.bind_address = '0.0.0.0'     # UDP interface bind address
stat_pg.port = 8125                  # UDP port to listen on
stat_pg.flush_interval = 1.0         # Seconds interval to commit to tables
```
