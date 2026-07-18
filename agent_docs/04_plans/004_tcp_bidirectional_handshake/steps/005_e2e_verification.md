# Step: 005 - End-to-End Verification

## Goal
Verify the entire TCP bi-directional handshake and ID-based ingestion pipeline.

---

## Tasks
1. Rebuild and restart the container.
2. Truncate all database tables.
3. Run cargo tests in the container to verify functional correctness.
4. Run the benchmark stress test:
   ```bash
   ./target/release/stat-pg-bench --threads 4 --duration 180 --metrics 1000
   ```
5. Verify:
   * String name mappings are successfully persisted inside `stat_pg_metrics`.
   * High-speed ingestion completes without dropping packets (since TCP has built-in congestion and flow control, packet loss will be **0%**!).
   * Queue drains extremely fast.
6. Verify query execution speed and sum values.
