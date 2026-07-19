# Step 1: Implement Additional Metric Formats & Currency Encoder

We will implement the expanded 16-format decoding, migrate database schema to `DOUBLE PRECISION`, update the benchmark tool to emit the randomized distribution, and implement client-side currency encoding in the StatsD proxy client.

---

## Code Tasks

1. **Modify `src/lib.rs`**:
   - Update `extension_sql!` DDL schema to use `DOUBLE PRECISION` for all metric value columns.
   - Add `type VARCHAR NOT NULL DEFAULT 'numeric'` to `stat_pg_metrics`.
   - Implement `f16_to_f64` for IEEE-754 half-precision float decoding.
   - Extract decoding logic into `parse_packet_payload`.
   - Refactor the first-batch and drain-loop UDP parsing loops to invoke `parse_packet_payload`.
   - Add unit test `test_udp_payload_parsing` inside `mod tests` verifying all 16 formats are correctly parsed.

2. **Modify `bench/src/main.rs`**:
   - Implement `SimpleRng` pseudo-random generator.
   - Refactor the prebuilt packet logic to implement the new layouts and the 60%/15%/15%/8%/2% distribution.
   - Update log/terminal display math to show the expected 366 updates per batch.

3. **Modify `client/src/main.rs`**:
   - Update `parse_statsd_line` to extract the metric type (`currency` or `numeric`).
   - Introduce thread-safe `metric_types` cache in `main` to track parsed metric types.
   - Update `register_metric` SQL query to save the parsed `type` (currency/numeric) in `stat_pg_metrics`.
   - In the sender loop, scale values of type `"currency"` by `10,000x`.
   - Dynamically choose the most compact binary format representing the scaled values and write updates.

---

## Verification

1. **Compile and Run Tests**:
   Verify correct compilation and run integration tests inside the container:
   ```bash
   docker compose exec db cargo test
   ```

2. **Database Schema check**:
   Verify column types are indeed `double precision` using `psql`:
   ```bash
   docker compose exec db psql -U postgres -d postgres -c "\d evt_secs"
   ```

3. **Benchmark**:
   Run the stress test benchmark:
   ```bash
   cd bench && cargo build --release
   ./target/release/stat-pg-bench --threads 4 --duration 15 --metrics 1000
   ```
