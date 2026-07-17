# Step: 002 - Benchmark CLI Implementation

## Goal
Implement the multi-threaded UDP sending logic with speed reporting.

## Tasks
- [ ] Parse arguments: host, port, threads, duration, rate limits, metric count.
- [ ] Pre-format metric templates in memory to avoid heap allocations in the sender loop.
- [ ] Spawn N worker threads where each thread binds a UDP socket and sends packets in a loop using `std::net::UdpSocket::send_to`.
- [ ] Implement atomic counters (`AtomicU64`) to track total packets sent in real-time.
- [ ] Implement a timer that stops all threads after the specified duration.
- [ ] Print summary stats (duration, total packets sent, packets/sec, MB/sec).

## Verification
- Compile the benchmark tool in release mode: `cargo build --release`.
