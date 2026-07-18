# PostgreSQL BGWorker Thread Safety Pattern

When writing PostgreSQL background workers in Rust using `pgrx`, you must adhere to strict thread safety boundaries to prevent segmentation faults.

---

## The Problem: PostgreSQL Thread-Local Storage

PostgreSQL is historically a single-threaded process architecture. It relies heavily on global variables and thread-local pointers (e.g. `MyLatch`, `CurrentMemoryContext`, error state frames) to maintain transaction status and memory layouts.
* When you spawn a standard OS thread (`std::thread::spawn`), the new thread **does not** inherit or initialize these PostgreSQL thread-local values.
* If a spawned thread calls any `pgrx` or `pg_sys` APIs (including `pgrx::log!`, `BackgroundWorker::worker_continue()`, or SPI functions), it will dereference null pointers and trigger a **segmentation fault (Signal 11)**, crashing the entire database cluster.

---

## The Pattern: Decoupled OS Socket & Database Threads

To leverage multi-core concurrency (e.g., to handle high-speed TCP streaming sockets without blocking on database writes), you must decouple network reading from database writing.

```
[Spawned Client Threads (OS)]      [SPSC Ring Buffer]        [Main Thread (PG)]
  Read TCP Stream                    reg_tx.send()             reg_rx.try_recv()
  Parse Handshake Handshake     =======================>       SPI name registration
  Parse 0x02 Updates                 metric_tx.push()          metric_rx.pop()
  (Pure Rust StdLib)            =======================>       SlidingWindow aggregate
                                                               SPI parameterized upserts
```

### 1. The OS Thread constraint
All spawned threads (both the listener thread and connection stream worker threads) must remain a **pure Rust execution environment**:
* They can bind sockets, accept connections, read stream buffers, and perform string parsing/allocations.
* **CRITICAL**: Spawned threads must never call `pgrx::log!`, `BackgroundWorker::worker_continue()`, or execute any SPI/FFI functions.
* **FFI Warning**: Calling `pgrx::log!` from a spawned thread will cause a panic with: `postgres FFI may not be called from multiple threads`. Use standard Rust `println!` or `eprintln!` instead, which safely redirects to stderr/Postgres logs.

### 2. Coordination via queues and atomics
* Use a high-performance, lock-free SPSC queue (`rtrb`) for metrics to prevent multi-producer atomic write contention.
* Store the SPSC Producer in a shared `Arc<Mutex<Option<Producer>>>` container. Connection streams take the producer at connection start and return it at connection end, enabling seamless reconnects without hot path locking.
* Use `Arc<AtomicBool>` to signal the threads when the BGWorker process is terminating, allowing them to cleanly exit their loops.

### 3. SPI Connection boundaries
The main thread alone manages connections to Postgres database internals:
* Call `BackgroundWorker::connect_worker_to_spi` from the main thread.
* Drain the channels in the main loop, run registrations, and perform memory aggregation.
* Wrap all SPI updates in a proper transaction context:
  ```rust
  BackgroundWorker::transaction(|| {
      Spi::connect_mut(|client| {
          // Perform SQL updates safely
      })
  });
  ```

### 4. Unwind Safety in Rust 2021
* The SPSC consumer (`Consumer`) contains interior mutability (`UnsafeCell`) and is not `UnwindSafe` / `RefUnwindSafe` by default.
* In the Rust 2021 edition, closures capture individual fields. Passing references to fields of a non-unwind-safe struct causes compiler failures on `BackgroundWorker::transaction(|| ...)` closures.
* To bypass this, wrap raw worker pointers in a manual `UnwindSafe` / `RefUnwindSafe` newtype (`SafeWorkerStatePtr`) and invoke methods on it directly to force Rust to capture the wrapper itself rather than its inner raw fields.
