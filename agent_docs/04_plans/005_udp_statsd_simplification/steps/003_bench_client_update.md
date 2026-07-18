# Step: 003 - Benchmark Client Upgrade

## Goal
Update the release stress test tool `stat-pg-bench` to support the new UDP protocol and standard DB registration process.

---

## Tasks
1. **Update `bench/src/main.rs`**:
   * Replace the TCP connection loop with standard PostgreSQL database client (`postgres` crate) to do name-to-id registration.
   * Run the gap-reclamation SQL transaction for registering mock metrics on startup:
     ```rust
     // Fetch/assign all mock metric IDs via standard SQL queries
     ```
   * Replace the `TcpStream` writing thread logic with a `UdpSocket` sending loop.
   * Format packets as raw arrays of `[ID (4B LE)] [Value (8B LE)]` (12 bytes per update).
   * Pack exactly 100 updates per UDP packet (1200 bytes) to stay under the 1472-byte MTU payload limit.
2. **Cargo Dependencies**:
   * Add the `postgres` driver crate to `bench/Cargo.toml` to support the registration connection.
3. **Verify Execution**:
   * Compile and test locally.
