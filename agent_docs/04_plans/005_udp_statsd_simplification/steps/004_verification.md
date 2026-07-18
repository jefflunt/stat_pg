# Step: 004 - End-to-End Verification

## Goal
Verify all components are fully functional and consistent under high stress.

---

## Tasks
1. **Recompile and Run**:
   * Compile and test the PostgreSQL extension inside Docker.
   * Start the database container.
2. **Database Schema Verification**:
   * Confirm extension can be created and that `stat_pg_metrics`, `evt_secs`, etc., are present.
3. **Execute Benchmark**:
   * Run the stress-test benchmark:
     ```bash
     ./target/release/stat-pg-bench --threads 4 --duration 10 --metrics 1000
     ```
   * Verify aggregate results are 100% correct and stable in the database.
4. **StatsD Client E2E Check**:
   * Start the StatsD proxy client locally.
   * Use a mock shell loop to emit UDP StatsD metrics:
     ```bash
     echo "mock.api.requests:1|c" > /dev/udp/127.0.0.1/8125
     ```
   * Confirm the proxy client registers the name, receives the ID, and successfully pushes updates over UDP to the database.
   * Query the database to verify the mock metrics are rolled up correctly.
