# Optimize UDP Ingestion Benchmark Client Writer

Now that we have removed string formatting and hashmap lookups, the client is CPU-bound by the vector allocation/clearing loop, byte copying, and the single-packet system call (`sendto`) overhead.

We propose two optimization phases to achieve maximum writer performance:

---

## Phase 1: Prebuilt UDP Packet Buffers (Platform-Independent)

Instead of building each UDP packet dynamically inside the hot loop, we will prebuilt a list of packets at thread startup. Since the metric IDs and values (`1.0f64`) follow a deterministic sequence, we can pre-generate this sequence in memory.

### Mechanism
- At thread startup, we allocate a circular queue of `1,000` prebuilt packets:
  ```rust
  let mut prebuilt_packets = Vec::with_capacity(1000);
  for _ in 0..1000 {
      let mut packet = Vec::with_capacity(1200);
      for _ in 0..100 {
          let metric_id = metric_ids_clone[batch_idx % unique_metrics];
          packet.extend_from_slice(&metric_id.to_le_bytes());
          packet.extend_from_slice(&1.0f64.to_le_bytes());
          batch_idx += threads;
      }
      prebuilt_packets.push(packet);
  }
  ```
- Inside the hot loop, we increment a packet index and send the prebuilt slice:
  ```rust
  let packet_bytes = &prebuilt_packets[packet_idx];
  socket.send(packet_bytes);
  packet_idx = (packet_idx + 1) % 1000;
  ```
- **Pros**: Completely removes loops, vector allocations, and memory copies from the user-space loop.

---

## Phase 2: Batch Sending via `sendmmsg` (Linux Native)

To reduce the user-to-kernel boundary crossing overhead (system calls), we can batch-send multiple UDP packets using the Linux-native `sendmmsg` system call.

### Mechanism
- Configure an array of `libc::mmsghdr` structs pointing to `32` prebuilt packets.
- Call `sendmmsg` in a single system call to transmit all 32 packets (3,200 metric updates) at once.
- **Pros**: Reduces system call overhead by up to **32x**, unlocking raw socket line rate.

---

## Proposed Changes

### Benchmark Tool

#### [MODIFY] [bench/src/main.rs](file:///Users/jefflunt/code/stat_pg/bench/src/main.rs)
- Implement Phase 1 (prebuilt packet circular buffer).
- Implement Phase 2 (`sendmmsg` batching on Linux).
