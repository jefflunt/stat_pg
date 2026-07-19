# Step 1: Implementation of SO_REUSEPORT Multi-process Scaling

We will modify the library initialization and socket creation to enable multi-process scaling.

---

## Code Tasks

1. **Enable SO_REUSEPORT on Socket Creation**:
   In `src/lib.rs` inside `stat_pg_main`'s socket initialization:
   ```rust
   let _ = socket.set_reuse_address(true);
   let _ = socket.set_reuse_port(true); // Enable SO_REUSEPORT
   ```

2. **Register 4 Background Workers**:
   In `src/lib.rs` inside `_PG_init`:
   ```rust
   for i in 0..4 {
       BackgroundWorkerBuilder::new(&format!("stat_pg_aggregator_{}", i))
           .set_function("stat_pg_main")
           .set_library("stat_pg")
           .set_start_time(BgWorkerStartTime::PostmasterStart)
           .set_restart_time(Some(Duration::from_secs(10)))
           .enable_spi_access()
           .load();
   }
   ```

---

## Verification

1. **Compile and Run Database Tests**:
   Ensure compilation succeeds and tests pass:
   `docker compose exec db cargo test`
   
2. **Verify 4 Worker Processes Started**:
   List active processes in the database container and confirm 4 worker processes are running:
   `docker compose exec db ps aux | grep stat_pg`

3. **Ingestion Benchmarking**:
   Verify delivery rate gains under parallel load using 2 threads:
   `./target/release/stat-pg-bench --threads 2 --duration 60 --metrics 1000`
