# stat-pg

A minimalist, high-performance PostgreSQL extension for statistical and log event ingestion and aggregation, written in Rust using the `pgrx` framework.

`stat-pg` runs a high-speed UDP socket listener directly inside the PostgreSQL database engine as a background worker process, aggregates metrics in-memory, and writes them to aggregate tables in periodic, transaction-safe batches.

---

## Architecture

- **Background Worker**: Spawns on Postgres boot to listen on UDP port `8125` (configurable).
- **In-Memory Accumulator**: Aggregates metric sums across five resolutions (`secs`, `mins`, `hours`, `days`, `30s`) in memory, avoiding row-level database locking and trigger write-amplification.
- **Transaction-Safe SPI Flush**: Flushes memory caches to tables in a single SQL transaction every `1.0` seconds (configurable).

---

## Configuration (GUCs)

You can customize the extension inside `postgresql.conf` using the following configuration parameters:

```ini
# postgresql.conf
shared_preload_libraries = 'stat_pg' # Required to load the worker on startup

stat_pg.database = 'postgres'        # The database to connect to and flush metrics into
stat_pg.user = 'postgres'            # The database user to run the worker as
stat_pg.bind_address = '0.0.0.0'     # The IP address to bind the UDP listener to
stat_pg.port = 8125                  # The UDP port to listen on
stat_pg.flush_interval = 1.0         # Interval in seconds to flush aggregated data to tables
```

### Can this be installed on an existing database?
**Yes!** Since it is a standard PostgreSQL extension, you can install it on any existing database cluster (such as the database co-located with your Rails application). 
* **Note**: Because a single background worker process binds to the UDP port, it flushes metrics into the single database configured by `stat_pg.database` (defaulting to `'postgres'`). It is recommended to use a dedicated database or dedicated tables for metrics to isolate the data, but running it on your main application's PG instance is fully supported.

---

## Local Development & Testing

We build and run `stat-pg` inside an isolated Docker container to avoid polluting or crashing your host system's PostgreSQL installation.

### 1. Start the Environment
Spin up the containerized database (Postgres 16) with Rust compile libraries:
```bash
docker compose up --build -d
```
* Binds PostgreSQL on host port **`5433`** (to avoid conflicts with local servers).
* Exposes UDP metrics port on host port **`8125`**.

### 2. Run the Test Suite
`stat-pg` includes Rust unit tests for parsing, and live database integration tests using `#[pg_test]`:
```bash
docker compose exec db cargo test
```

### 3. Manual Ingestion Verification (UDP)
You can verify the UDP pipeline manually from your host machine (macOS/Linux):

1. **Verify tables are empty**:
   ```bash
   docker compose exec db psql -U postgres -c "SELECT * FROM evt_secs;"
   ```
2. **Send a UDP packet** containing `[metric_name] [value] [optional_timestamp]`:
   ```bash
   # Format: code value timestamp
   echo "api.request.count 12.5 1721237340" | nc -u -w1 127.0.0.1 8125
   ```
   *If you omit the timestamp, the background worker automatically defaults to the server's current system time.*
3. **Wait 1 second** (to trigger the flush interval) and query the seconds resolution table:
   ```bash
   docker compose exec db psql -U postgres -c "SELECT * FROM evt_secs;"
   ```
   **Output**:
   ```text
          code        |     at     |      value      
   -------------------+------------+-----------------
    api.request.count | 1721237340 | 12.500000000000
   (1 row)
   ```
4. **Query other time resolutions** (e.g. minutes):
   ```bash
   docker compose exec db psql -U postgres -c "SELECT * FROM evt_mins;"
   ```
   **Output**:
   ```text
          code        |    at    |      value      
   -------------------+----------+-----------------
    api.request.count | 28687289 | 12.500000000000
   (1 row)
   ```
