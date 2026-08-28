# Client Write Socket Tuning & Zero-Copy Ingestion Analysis

We will tune the client UDP socket buffer sizes to prevent packet drops and thread blocking under high workloads. We also analyze the options for zero-copy packet receiving and eBPF socket redirection.

---

## 1. Socket Buffer Tuning (SO_SNDBUF)

Currently, the client UDP socket uses the OS default write buffer size (212 KB). When the network stack loopback interface is saturated, the sender threads block.
- **Proposed Change**: Configure a **50 MB** write buffer size (`SO_SNDBUF`) on each client socket:
  ```rust
  let _ = socket.set_send_buffer_size(50 * 1024 * 1024);
  ```

---

## 2. Ingestion-Side Zero-Copy (`PACKET_RX_RING` / `AF_XDP`)

To avoid copying packet bytes from kernel space to user space on the database receiver side, we could use Linux **`PACKET_RX_RING`** or **`AF_XDP`**.
- **Trade-off Analysis**:
  - **Raw Packet Parsing**: Bypassing the kernel's UDP stack means we must manually parse Ethernet frames, IP headers, and UDP headers in Rust.
  - **Security Privileges**: Raw packet sockets (`PF_PACKET`) require the database process (`postgres`) to have `CAP_NET_RAW` privileges. Inside standard Docker containers, this is blocked by default and requires running the containers with `--cap-add=NET_RAW` or `--privileged`.
  - **Portability**: This binds the database extension specifically to Linux, breaking macOS test setups.
- **Recommendation**: Retain the current standard `recvmmsg` blocking reader which already maps kernel buffers to user space in batches, and focus on socket buffer sizing.

---

## 3. MSG_ZEROCOPY on the Writer

Linux supports `MSG_ZEROCOPY` for UDP sockets to avoid user-to-kernel copies.
- **Trade-off Analysis**:
  - `MSG_ZEROCOPY` requires reading tx completion notifications from the socket error queue (`MSG_ERRQUEUE`).
  - For small packets (1.2 KB in our case), the CPU overhead of kernel page pinning and processing completion events exceeds the cost of a simple CPU cache copy.
- **Recommendation**: Avoid `MSG_ZEROCOPY` as it would decrease performance for 1.2 KB packets.

---

## Proposed Changes

### Benchmark Tool

#### [MODIFY] [bench/src/main.rs](file:///Users/jefflunt/code/stat_pg/bench/src/main.rs)
- Set socket send buffer size (`set_send_buffer_size`) to 50 MB on startup for all threads.
