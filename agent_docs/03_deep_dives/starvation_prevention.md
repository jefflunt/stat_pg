# Starvation Prevention & Socket Queue Management

Under extreme ingestion loads (e.g., millions of metrics per second), network ingestion processes can experience queue starvation or packet loss. This deep dive explains why starvation occurs and how `stat-pg` prevents it.

---

## The Starvation Scenario (TCP Stream Queue Ingestion)

In the legacy TCP-based architecture, client connection threads parsed incoming TCP streams and dropped metric updates into a single SPSC ring buffer. The main worker thread was responsible for draining this channel in a loop:

```rust
// Legacy implementation (Vulnerable to starvation and OOM)
while let Ok(metric) = rx.try_recv() {
    cache.add(metric);
}
```

### The Infinite Loop Vulnerability
* If incoming metric rates were extremely high, the channel accumulated items faster than the main thread could execute loop iterations.
* Because the channel was never empty, the thread became **trapped** inside the drainage loop.
* Because it never exited the drainage loop, it never reached the periodic database flush logic, growing the cache without bound and crashing with an Out-of-Memory (OOM) error.

---

## The Solution: Decoupled In-Memory Aggregation & Non-Blocking Socket Draining

To solve this, `stat-pg` decouples network buffer consumption from database transactions.

```
Incoming Network Blasts
       │
       ▼ (120 updates per UDP packet)
┌──────────────────────────────────────┐
│  Linux Socket Buffer (256MB tuned)   │
└──────────────────┬───────────────────┘
                   │
                   ▼ (1. blocking recvmmsg batch retrieval)
┌──────────────────────────────────────┐
│       UDP Listener Thread            │◄───┐
│  Decodes 2-byte variable payloads    │    │ (2. non-blocking recv_batch_nonblocking loop)
│  Updates Mutex-locked active_buffer  ├────┘
└──────────────────────────────────────┘
                   ▲
                   │ (3. fast reference swap every 1.0 second)
┌──────────────────┴───────────────────┐
│         Main SPI Thread              │
│  Runs SQL COPY of swapped buffer     │
│  Latching sleep (50ms duration)      │
└──────────────────────────────────────┘
```

### 1. Multi-Threaded In-Memory Decoupling
Because metrics are parsed and aggregated *in-memory* directly by the spawned UDP thread into a shared `active_buffer` (`Mutex<Vec<f64>>`), the main PostgreSQL thread is completely freed from channel draining:
* The UDP thread updates the active buffer hot-path under a mutex lock.
* The main thread only acquires the mutex once per `flush_interval` (default `1.0s`) to swap the buffer vector with a local flush buffer.
* The swap operation is near-instantaneous ($O(1)$ pointer swap), holding the lock for only a few microseconds. The UDP thread never experiences lock starvation.

### 2. Double-Buffered `recvmmsg` Queue Drainage
To prevent the OS socket buffer from overflowing and dropping UDP packets (which do not have flow control like TCP) under peak traffic bursts, the UDP thread implements a two-stage drainage process:
* **Blocking Batch Retrieval**: The thread calls `recv_batch_blocking` using `recvmmsg` (on Linux) to retrieve up to `32` packets. This blocks and yields CPU when no traffic is present, preventing high idle CPU usage.
* **Non-Blocking Hot Loop**: Once packets arrive, the thread processes them and immediately enters a tight inner loop calling `recv_batch_nonblocking` (`MSG_DONTWAIT` flag) to drain any other pending packets in the kernel's socket queue.
* **Socket Buffer Tuning**: The UDP socket's receive buffer size is set to `256MB` (`SO_RCVBUF`). This acts as a massive shock absorber to hold backlogged packets while the UDP thread is processing updates.

### 3. CPU Yielding via Latch Wait
The main worker thread checks for shutdown and handles database flushes. Instead of spinning, it sleeps for 50ms intervals using `BackgroundWorker::wait_latch(Some(Duration::from_millis(50)))`. This yields CPU cycles to the host operating system and other PostgreSQL background processes, ensuring the database server remains responsive.
