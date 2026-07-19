# Step 1: Remove Conflict Checks and Automate Cleanup

We will modify the SQL query generation, revert worker count to 1, and add automatic database cleanup to the benchmark.

---

## Code Tasks

1. **Remove ON CONFLICT clause**:
   In `src/lib.rs` modify `make_batch_insert_query`:
   ```rust
   fn make_batch_insert_query(table_name: &str, batch_size: usize) -> String {
       let mut sql = format!("INSERT INTO {} (metric_id, at, value) VALUES ", table_name);
       for i in 0..batch_size {
           if i > 0 {
               sql.push_str(", ");
           }
           let base = i * 3;
           sql.push_str(&format!("(${}, ${}, ${})", base + 1, base + 2, base + 3));
       }
       sql.push_str(";");
       sql
   }
   ```

2. **Revert background worker count to 1**:
   In `src/lib.rs` inside `_PG_init`, change the registration loop back to a single worker registration:
   ```rust
   BackgroundWorkerBuilder::new("stat_pg_aggregator")
       .set_function("stat_pg_main")
       .set_library("stat_pg")
       .set_start_time(BgWorkerStartTime::PostmasterStart)
       .set_restart_time(Some(Duration::from_secs(10)))
       .enable_spi_access()
       .load();
   ```

3. **Add Database Cleanup to Benchmark**:
   In `bench/src/main.rs` inside `main()` right before calling `register_metrics`:
   ```rust
   // Automatically truncate and vacuum database tables before registering metrics
   println!("Database cleanup: Truncating event tables and vacuuming...");
   if let Ok(mut client) = postgres::Client::connect(&db_url, postgres::NoTls) {
       let _ = client.execute("TRUNCATE TABLE stat_pg_metrics, evt_secs, evt_mins, evt_hours, evt_days, evt_30s RESTART IDENTITY CASCADE;", &[]);
       let _ = client.execute("VACUUM ANALYZE;", &[]);
   }
   ```

---

## Verification

1. **Compile and Run Database Tests**:
   Ensure compile succeeds:
   `docker compose exec db cargo test`

2. **Run Ingestion Stress Test**:
   Verify delivery rate:
   `./target/release/stat-pg-bench --threads 1 --duration 60 --metrics 1000`
