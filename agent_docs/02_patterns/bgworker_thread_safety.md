# PostgreSQL BGWorker Thread Safety Pattern

When writing PostgreSQL background workers in Rust using `pgrx`, you must adhere to strict thread safety boundaries to prevent segmentation faults.

---

## The Problem: PostgreSQL Thread-Local Storage

PostgreSQL is historically a single-threaded process architecture. It relies heavily on global variables and thread-local pointers (e.g. `MyLatch`, `CurrentMemoryContext`, error state frames) to maintain transaction status and memory layouts.
* When you spawn a standard OS thread (`std::thread::spawn`), the new thread **does not** inherit or initialize these PostgreSQL thread-local values.
* If a spawned thread calls any `pgrx` or `pg_sys` APIs (including `pgrx::log!`, `BackgroundWorker::worker_continue()`, or SPI functions), it will dereference null pointers and trigger a **segmentation fault (Signal 11)**, crashing the entire database cluster.

---

## The Pattern: Decoupled UDP Listener & Main Database Threads

To leverage multi-core concurrency (handling high-speed UDP ingestion sockets without blocking on database writes), you must decouple network packet reading from database writing.

```
[Spawned UDP Thread (OS)]           [Shared Buffer]         [Main Thread (PG)]
  Read UDP socket (recvmmsg)                                  Check GUC parameters
  Parse variable payload                                      Periodically (1.0s):
  Check atomic max_valid_id   =======================>          Mutex buffer swap
  Mutex-locked updates             active_buffer                CSV export & COPY
  (Pure Rust StdLib)            (Mutex<Vec<f64>>)               coordinator_merge
                                                                SPI table inserts
```

### 1. The OS Thread Constraint (Pure Rust Execution)
All spawned threads (the UDP listener socket receiver and parser threads) must remain in a **pure Rust execution environment**:
* They can bind sockets, accept data, read buffer arrays, and perform memory updates.
* **CRITICAL**: Spawned threads must never call `pgrx::log!`, `BackgroundWorker::worker_continue()`, or execute any SPI/FFI functions.
* **FFI Warning**: Calling `pgrx::log!` from a spawned thread will cause a panic: `postgres FFI may not be called from multiple threads`. Use standard Rust `eprintln!` or `println!` instead, which safely redirects to stderr and is output to PostgreSQL server logs.

### 2. Coordination via Mutex-Double Buffering
* Instead of lock-free channel queues, the UDP listener thread and the main worker thread share `active_buffer` (`Arc<Mutex<Vec<f64>>>`) to aggregate values in-memory.
* Every `flush_interval` (default 1s), the main thread enters a brief lock block, swaps the `active_buffer` with a blank `local_flush_buffer` vector, and exits the lock. This minimizes lock contention, allowing the UDP thread to resume updates immediately.

### 3. Memory Access and Atomic Safety Boundaries
* For maximum performance in the UDP hot loop, metric updates in the `active_buffer` vector are done using `get_unchecked_mut`.
* **Index Out-of-Bounds prevention**: To prevent memory corruption or segmentation faults from out-of-bound writes:
  - The UDP thread reads an atomic `max_valid_id: Arc<AtomicI32>` value.
  - It only applies updates if `id > 0 && id <= max_valid_id && id < active_buffer.len()`.
  - The main thread updates `max_valid_id` during its periodic SPI flush sequence after querying the database to find the new maximum ID in `stat_pg_metrics`.
  - Active and local buffers are periodically resized to `max_valid_id` during buffer swap cycles, keeping the lock overhead in the ingestion path minimal.

### 4. SPI Connection Boundaries
The main thread alone manages connections to PostgreSQL database internals:
* Call `BackgroundWorker::connect_worker_to_spi` from the main thread.
* Only the main thread initiates transactions, writes staging tables, and runs merge/rollup queries.
* Wrap all SPI updates in a proper transaction context:
  ```rust
  BackgroundWorker::transaction(|| {
      Spi::connect_mut(|client| {
          // Perform SQL updates safely
      })
  });
  ```
