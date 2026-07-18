# Step: 001 - UDP Aggregation Worker

## Goal
Implement the stateless UDP listener and the double-buffered aggregation logic in the PostgreSQL background worker.

---

## Tasks
1. **Modify GUCs**:
   * Change `stat_pg.port` and `stat_pg.bind_address` descriptions to reference UDP.
2. **Remove TCP Code**:
   * Delete `TcpListener`, `handle_client`, `RegRequest`, and `SlidingWindow` structures.
3. **Double-Buffering Setup**:
   * Define an atomic `max_valid_id: Arc<AtomicI32>` on the worker.
   * Define two double-buffered vector structures protected by a `Mutex<Vec<f64>>`.
4. **UDP Reader Thread**:
   * Spawn a thread that binds a UDP socket to `bind_address:port`.
   * Read incoming 12-byte updates: `[ID (4B LE i32)] [Value (8B LE f64)]`.
   * For each update, perform a bounds check:
     ```rust
     let max_id = max_valid_id.load(Ordering::Relaxed) as usize;
     if id > 0 && id <= max_id {
         let mut active = active_buffer.lock().unwrap();
         if id < active.len() {
             active[id] += value;
         }
     }
     ```
5. **Periodic Main Thread Flush & Bounds Sync**:
   * Every `flush_interval` (default 1.0s), swap the active buffer with the flush buffer inside a brief lock.
   * Start a transaction, execute `SELECT COALESCE(MAX(id), 0) FROM stat_pg_metrics;` to refresh `max_valid_id`.
   * Iterate over the flush buffer, perform SPI batch prepared upserts to the resolution tables, and commit the transaction.
   * Resize the flush buffer to `max_valid_id + 1` and clear it to `0.0` for the next cycle.
