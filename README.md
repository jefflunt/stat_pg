# stat-pg

A minimalist, high-performance PostgreSQL extension for statistical and log event ingestion and aggregation, written in Rust using the `pgrx` framework.

`stat-pg` runs a high-speed TCP socket listener directly inside the PostgreSQL database engine as a background worker process, aggregates metrics in-memory, and writes them to aggregate tables in periodic, transaction-safe batches.

---

## Architecture

- **Background Worker**: Spawns on Postgres boot to listen on TCP port `8125` (configurable).
- **In-Memory Accumulator**: Aggregates metric sums across five resolutions (`secs`, `mins`, `hours`, `days`, `30s`) in memory, avoiding row-level database locking and trigger write-amplification.
- **Transaction-Safe SPI Flush**: Flushes memory caches to tables in a single SQL transaction every `1.0` seconds (configurable).

### Network Protocol Format

`stat-pg` communicates over a custom bidirectional binary TCP protocol:

1. **Metric Registration Handshake (Type `0x01`)**
   - **Client Request**: `[0x01 (1B)] [Name Length (2B BE)] [Name String (UTF-8)]`
   - **Server Response**: `[0x01 (1B)] [Name Length (2B BE)] [Name String (UTF-8)] [Metric ID (4B LE i32)]`
   - Once a metric is registered, the client uses the returned 4-byte `Metric ID` for subsequent metric updates to avoid parsing overhead.

2. **Metric Update (Type `0x02`)**
   - **Client Payload**: `[0x02 (1B)] [Metric ID (4B LE i32)] [Value (8B LE f64)] [Timestamp (8B LE i64)]`
   - **Server Processing**: Parsed metrics are placed into a lock-free Single-Producer Single-Consumer (SPSC) ring buffer to be processed by the background aggregator thread.

---

## Configuration (GUCs)

You can customize the extension inside `postgresql.conf` using the following configuration parameters:

```ini
# postgresql.conf
shared_preload_libraries = 'stat_pg' # Required to load the worker on startup

stat_pg.database = 'postgres'        # The database to connect to and flush metrics into
stat_pg.user = 'postgres'            # The database user to run the worker as
stat_pg.bind_address = '0.0.0.0'     # The IP address to bind the TCP listener to
stat_pg.port = 8125                  # The TCP port to listen on
stat_pg.flush_interval = 1.0         # Interval in seconds to flush aggregated data to tables
stat_pg.buffer_window = 5            # Sliding window size in seconds to buffer metrics (default: 5)
```

### Can this be installed on an existing database?
**Yes!** Since it is a standard PostgreSQL extension, you can install it on any existing database cluster. 
* **Note**: Because a single background worker process binds to the TCP port, it flushes metrics into the single database configured by `stat_pg.database` (defaulting to `'postgres'`). It is recommended to use a dedicated database or dedicated tables for metrics to isolate the data.

---

## Local Development & Testing

We build and run `stat-pg` inside an isolated Docker container to avoid polluting or crashing your host system's PostgreSQL installation.

### 1. Start the Environment
Spin up the containerized database (Postgres 16) with Rust compile libraries:
```bash
docker compose up --build -d
```
* Binds PostgreSQL on host port **`5433`** (to avoid conflicts with local servers).
* Exposes TCP metrics port on host port **`8125`**.

### 2. Run the Test Suite
`stat-pg` includes Rust unit tests for parsing, and live database integration tests using `#[pg_test]`:
```bash
docker compose exec db cargo test
```

### 3. Manual Ingestion Verification (TCP)
You can verify the TCP pipeline manually by executing a Python one-liner on your host machine to register a metric name and send an update:

1. **Verify tables are empty**:
   ```bash
   docker compose exec db psql -U postgres -c "SELECT * FROM evt_secs;"
   ```
2. **Send binary packets over TCP** using a Python one-liner:
   ```bash
   python3 -c '
   import socket, struct
   s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   s.connect(("127.0.0.1", 8125))
   name = b"api.request.count"
   # Send Registration (0x01)
   s.sendall(b"\x01" + struct.pack(">H", len(name)) + name)
   resp = s.recv(1024)
   metric_id = struct.unpack("<i", resp[-4:])[0]
   # Send Metric Update (0x02) - ID, value=12.5, timestamp=1721237340
   s.sendall(b"\x02" + struct.pack("<i d q", metric_id, 12.5, 1721237340))
   s.close()
   '
   ```
3. **Wait 1 second** (to trigger the flush interval) and query the seconds resolution table. Since metric names are normalized to IDs internally, join `evt_secs` with the lookup table:
   ```bash
   docker compose exec db psql -U postgres -c "
   SELECT m.name as code, e.at, e.value 
   FROM evt_secs e 
   JOIN stat_pg_metrics m ON e.metric_id = m.id;
   "
   ```
   **Output**:
   ```text
          code        |     at     |      value      
   -------------------+------------+-----------------
    api.request.count | 1721237340 | 12.500000000000
   (1 row)
   ```

---

## Benchmarking

We have included a high-speed, multi-threaded TCP stress test benchmark tool inside `bench/` to check your environment's ingestion limits.

### 1. Compile the Benchmark Tool
On your host machine, compile the tool in release mode:
```bash
cd bench
cargo build --release
```

### 2. Run the Benchmark
Run the stress test against the local database container:
```bash
./target/release/stat-pg-bench [options]
```

**Options**:
- `--host <ip>` (default: `127.0.0.1`)
- `--port <port>` (default: `8125`)
- `--threads <num>` (default: CPU cores)
- `--duration <secs>` (default: 180)
- `--metrics <num>` (default: 1000)

**Example**:
```bash
./target/release/stat-pg-bench --threads 4 --duration 5 --metrics 1000
```

Console Output Example:
```text
==================================================
stat-pg-bench Starting Bidirectional TCP Stress Test
Target Address : 127.0.0.1:8125
Sender Threads : 4
Run Duration   : 5 seconds (0.1 minutes)
Unique Metrics : 1000
Protocol Format: Bidirectional TCP (ID Handshake)
==================================================
Blasting TCP metric updates to target server...
==================================================
Benchmark Complete!
Elapsed Time   : 5.13s
Total Packets  : 717,255
Total Metrics  : 71,725,500
Total Bytes    : 1436.46 MB
Average PPS    : 139,862.11 Batches/sec (100 updates/batch)
Average MPS    : 14,345,100 Metrics/sec (Ingested)
Throughput     : 280.10 MB/sec
==================================================
```
This demonstrates blasting **14.3 million metrics/sec** over a **280 MB/sec** network throughput, with the database background worker successfully parsing, aggregating in-memory, and flushing directly inside PostgreSQL.
