# Step: 004 - Benchmark Client TCP & Handshake Upgrade

## Goal
Update the benchmark client tool `bench/src/main.rs` to connect over TCP, perform name-registration handshakes, cache the assigned metric IDs, and transmit only metric IDs in hot traffic.

---

## Tasks
1. Upgrade benchmark network sockets to TCP (`std::net::TcpStream`).
2. Implement local mapping cache `name_to_id: HashMap<String, i32>` per sender thread.
3. At client startup or first execution:
   * Perform registration handshake for the 1,000 (or N) unique metrics.
   * Send Type 0x01 packets; wait for Type 0x01 response containing the ID.
   * Update the local mapping cache.
4. During active blasting:
   * Loop and serialize Type 0x02 packets: `[0x02] [ID (4 bytes)] [Value (8 bytes)] [Timestamp (8 bytes)]` (only 21 bytes per metric!).
   * Pack multiple Type 0x02 packets into the TCP socket write buffer (using buffered writes or direct TCP stream write) to maximize throughput.
