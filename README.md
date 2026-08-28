# stat-pg

A minimalist PostgreSQL extension for metric ingestion and aggregation.

`stat-pg` runs a TCP socket listener directly inside the PostgreSQL database engine as a background worker process, aggregates metrics in-memory, and writes them to aggregate tables in periodic, transaction-safe batches. By doing this inside the database engine, millions of metrics per second can be aggregated while keeping the data queryable.

---

## Design

- **Background Worker**: Spawns on Postgres boot to listen on TCP port `8125` (configurable).
- **In-Memory Accumulator**: Aggregates metric sums across five resolutions (`secs`, `mins`, `hours`, `days`, `30s`)
- **Transaction-Safe SPI Flush**: Flushes memory caches to tables in a single SQL transaction every `1.0` seconds (configurable).


### Super simplified:

* A client sends a metric with:
  * A metric name (i.e. `api.request.count`)
  * A 64-bit integer representing the current UTC second
  * A value for the metric (i.e. a count, `1` - integers and floats supported)
* A client-side batcher collects metrics locally before sending to the server, in order to send less total traffic to the server. So, multiple `api.request.count` metrics per second are accumulated locally and sent as `api.request.count 1784378411 7`.
* The server receives the metrics from multiple connected clients, aggregates then in-memory, then flushes the aggregate values to the database once per second (default, configurable).

### One layer deeper: binary encoding, packet stuffing, and metric name -> ID map, and the metrics ring buffer

#### Binary encoding

* In its simplest form the string `api.request.count 1784378411 7` is a valid metric emission. However, that format is very poor for performance.
* The metrics are instead transmitted in a binary format that doesn't require string parsing on the receiving end.

#### Packet stuffing

Networks transmit data via packets, and packets have a maximum size. A simplified MTU for this is 1,500 bytes, and of that about 1,492 are usable to transmit payload data. The client takes this into account, and stuffs as many client-side aggregated metrics into a single packet as it can without exceeding the MTU in order to minimize network traffic. When the client _either_ has a full packet's worth of data, or it's been 1s since the last packet sent, we send the data we have. In this way data is transmitted at least once per second (for low metric traffic), but can be transmitted much faster for higher volumes.

#### Metric name -> ID mapping

* Sending text metric names such as `api.request.count` for every mtric, and having that matched up to an accumulator metric on the receiving side in the database is very inefficient, because you would need a hash of `String -> accumulator` such that on every metric received there's a Hash lookup. Hashes are quick, but not nearly as fast as Arrays.
* So, we pay a small RAM footprint penalty to have an array of accumulators on the receiving end (but for, say, 1,000 unique metrics this buffer is still under 64MB -- 10,000 unique metrics would be approximately 10x the RAM, so about 650MB).
* The first time that a client sends a metric it sends the text-based form. The server then either looks up or assigns an integer ID to that metric name, and responds with this ID so that future metrics sent from the client can use the ID instead of the name. This allows the server-side to index incoming metrics directly into an Array for accumulation/aggregation. So, we trade a slightly slower initial metric emission for a much faster emission thereafter.

#### Metrics ring buffer

On the server side a 5-second aggregation ring buffer is keps in RAM in order to allow some slack time in the arrival of metrics (+/- 2 seconds around the current second). If the connection between client and server is (a) reliable, and (b) a short network distance, and if all system clocks are updated and synchronized to a time server, then this +/- 2 second buffer should be more than sufficient to get at or near 100% metric retention. Metrics outside of this +/- 2 second sliding window are discarded, since this is technically a time configuration issue for the clients, or a network connection problem. In any case, `stat-pg` is designed to provide both very fast real-time metric ingestion, but like StatsD by which it was inspired, some packet loss is considered okay.


```
========================================================================================
                                    CLIENT / SERVER INTERACTION
========================================================================================

    CLIENT                                                    PostgreSQL BGWORKER
  (Sends Metrics)                                            (stat-pg TCP Listener)
        |                                                                 |
        |------ 1. Handshake (Type 0x01): Metric Name Registration ------>|
        |<----- 2. Response (Type 0x01): Assigned ID (4-byte i32) ------- |
        |                                                                 |
        |                                                                 |
        |====== 3. Blast Ingestion Updates (Type 0x02): Binary Payload ==>|
        |          [Type 0x02] [ID] [Value] [Timestamp]                   |
        v                                                                 v

========================================================================================
                                     PACKET STRUCTURES
========================================================================================

1. REGISTRATION HANDSHAKE (Type 0x01)
   Request:
   +-----------+--------------------+----------------------------------+
   | Type (1B) | Name Length (2B)   | Metric Name String (UTF-8, Var)  |
   |   0x01    | e.g. 0x0011 (17)   | "api.request.count"              |
   +-----------+--------------------+----------------------------------+
   Response:
   +-----------+--------------------+----------------------------------+-----------------+
   | Type (1B) | Name Length (2B)   | Metric Name String (UTF-8, Var)  | Metric ID (4B)  |
   |   0x01    | e.g. 0x0011 (17)   | "api.request.count"              | 1042 (LE i32)   |
   +-----------+--------------------+----------------------------------+-----------------+

2. INGESTION UPDATE (Type 0x02)
   +-----------+-----------------+-------------------+---------------------+
   | Type (1B) | Metric ID (4B)  | Value (8B f64)    | Timestamp (8B i64)  |
   |   0x02    |  1042 (LE i32)  |   12.5 (LE float) | 1721237340 (LE int) |
   +-----------+-----------------+-------------------+---------------------+

========================================================================================
                             IN-MEMORY SLIDING WINDOW (5-Slot Array)
========================================================================================

  Incoming stream timestamp = 105
  Window slots mapped using: idx = ts % 5 (window_size)

      Slot 0          Slot 1          Slot 2          Slot 3          Slot 4
   +------------+  +------------+  +------------+  +------------+  +------------+
   |  ts: 100   |  |  ts: 101   |  |  ts: 102   |  |  ts: 103   |  |  ts: 104   |
   |  data:     |  |  data:     |  |  data:     |  |  data:     |  |  data:     |
   |  [id1: v1] |  |  [id1: v2] |  |  [id1: v3] |  |  [id1: v4] |  |  [id1: v5] |
   |  [id2: v1] |  |  [id2: v2] |  |  [id2: v3] |  |  [id2: v4] |  |  [id2: v5] |
   +------------+  +------------+  +------------+  +------------+  +------------+
         |
         | (Incoming packet at 105 arrives: idx = 0)
         v
  [ EVICT & FLUSH ]  <-- Slot 0 (ts 100) is evicted & overwritten with ts 105
         |
         v
========================================================================================
                             DATABASE INGESTION (SPI Flush)
========================================================================================

         |
         +--> [ Convert Slot Data to Active Entries Array: (metric_id, aggregated_value) ]
         |
         +--> [ SPI Parameterized Upsert Transactions ]
         |
         +--> [ DB ROLLUP TABLES ]
              ├── stat_pg_secs (at = 100)
              ├── stat_pg_mins (at = 100 / 60)
              ├── stat_pg_hours (at = 100 / 3600)
              ├── stat_pg_days (at = 100 / 86400)
              └── stat_pg_30s (at = 100 / 2592000)
```

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
* **Note**: Because a single background worker process binds to the TCP port, it flushes metrics into the single database configured by `stat_pg.database` (defaulting to `'postgres'`). It is recommended to use a dedicated database or dedicated tables for metrics to isolate the data, especially if you're dealing with 100,000+ metric events per second -- at that point you're going to need a dedicated server for aggregation and reporting. In some tests, depending on the hardware you're running it on, servers with 4 vCPUs can ingest up to 10M metrics per second. Due to the current design of `stat-pg` there is no real benefit to using more cores than this, because all in-memory aggregation results in a single database writer, which means that part of the system is effectively single-threaded. 10M metrics/second of ingestion is still very fast, however.

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
   docker compose exec db psql -U postgres -c "SELECT * FROM stat_pg_secs;"
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
3. **Wait 1 second** (to trigger the flush interval) and query the seconds resolution table. Since metric names are normalized to IDs internally, join `stat_pg_secs` with the lookup table:
   ```bash
   docker compose exec db psql -U postgres -c "
   SELECT m.name as code, e.at, e.value 
   FROM stat_pg_secs e 
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

Included is a high-speed, multi-threaded TCP stress test benchmark tool inside `bench/` to check your environment's ingestion limits.

### 1. Compile the Benchmark Tool
On your host machine, compile the tool in release mode:
```bash
cd bench
cargo build --release
```

### 2. Run the Benchmark
Run the stress test against the local database container:
```bash
./target/release/stat_pg_bench [options]
```

**Options**:
- `--host <ip>` (default: `127.0.0.1`)
- `--port <port>` (default: `8125`)
- `--threads <num>` (default: CPU cores, though typical tests should limit this to 1-4 threads)
- `--duration <secs>` (default: 180)
- `--metrics <num>` (default: 1000, the cardinality of the incoming data -- i.e. 1,000 uniquely named metrics)

**Example**:
```bash
./target/release/stat_pg_bench --threads 4 --duration 5 --metrics 1000
```

Console Output Example:
```text
==================================================
stat_pg_bench Starting Bidirectional TCP Stress Test
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
