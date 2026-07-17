# PostgreSQL BGWorker Thread Safety Pattern

When writing PostgreSQL background workers in Rust using `pgrx`, you must adhere to strict thread safety boundaries to prevent segmentation faults.

---

## The Problem: PostgreSQL Thread-Local Storage

PostgreSQL is historically a single-threaded process architecture. It relies heavily on global variables and thread-local pointers (e.g. `MyLatch`, `CurrentMemoryContext`, error state frames) to maintain transaction status and memory layouts.
* When you spawn a standard OS thread (`std::thread::spawn`), the new thread **does not** inherit or initialize these PostgreSQL thread-local values.
* If a spawned thread calls any `pgrx` or `pg_sys` APIs (including `log!`, `worker_continue()`, or SPI functions), it will dereference null pointers and trigger a **segmentation fault (Signal 11)**, crashing the entire database cluster.

---

## The Pattern: Decoupled OS Socket & Database Threads

To leverage multi-core concurrency (e.g., to handle high-speed UDP sockets without blocking on database writes), you must decouple network reading from database writing.

```
[Spawned Thread (OS)]           [mpsc Channel]           [Main Thread (PG)]
  Read UDP socket                    tx.send()             rx.try_recv()
  Parse metric line          ====================>      Add to MetricCache
  (Pure Rust StdLib)                                    Run transaction & SPI
```

### 1. The OS Thread constraint
The spawned thread must remain a **pure Rust execution environment**:
* It can bind sockets, read bytes, perform string parsing, and allocations.
* **CRITICAL**: It must never call `pgrx::log!`, `BackgroundWorker::worker_continue()`, or make SPI database calls.
* Use stdlib standard printing (`eprintln!`) if output is needed (which redirects safely to stderr/Postgres logs) or omit logging entirely inside the loop.

### 2. Coordination via channels and atomics
* Use `std::sync::mpsc::channel` to pass structured metrics from the OS thread to the main thread.
* Use `Arc<AtomicBool>` to signal the OS thread when the BGWorker process is terminating, allowing the thread to join and clean up resource handles.

### 3. SPI Connection boundaries
The main thread alone manages connections to Postgres database internals:
* Call `BackgroundWorker::connect_worker_to_spi` from the main thread.
* Drain the channel in the main loop and perform memory aggregation.
* Wrap all SPI updates in a proper transaction context:
  ```rust
  BackgroundWorker::transaction(|| {
      Spi::connect_mut(|client| {
          // Perform SQL updates safely
      })
  });
  ```
