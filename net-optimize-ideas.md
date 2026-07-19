# Network Ingestion Optimization Ideas

This document outlines architectural and system-level strategies to scale the UDP metrics ingestion throughput of the `stat-pg` background worker past the single-threaded CPU boundary (~14.5M metrics/second).

---

## 1. Batching System Calls (`recvmmsg`)
* **Core Concept**: Reduce the user-to-kernel space context-switching frequency by receiving multiple UDP packets in a single system call.
* **Mechanism**: Use the Linux-specific `recvmmsg` system call. The application passes an array of buffers (e.g., size 32 or 64) to the kernel. In a single execution, the kernel drains the socket's receive queue into these buffers and returns.
* **Benefits**: 
  - Reduces system call overhead by $32\times$ or $64\times$.
  - Portable across Linux Docker environments.
  - No special host permissions or kernel configuration changes required.

---

## 2. Horizontal Thread Scaling (`SO_REUSEPORT`)
* **Core Concept**: Distribute the packet-receiving load across multiple CPU cores.
* **Mechanism**: Spawn multiple UDP receiver threads in the background worker, each binding a separate socket to the same port (`8125`) using the `SO_REUSEPORT` socket option.
* **Benefits**:
  - The Linux kernel load-balances incoming UDP packets at the socket layer across multiple threads.
  - Context switches and `recv_from` processing are distributed across multiple cores.

---

## 3. Kernel Bypass (AF_XDP)
* **Core Concept**: Avoid the Linux kernel network stack and socket layer entirely.
* **Mechanism**: Address Family XDP (AF_XDP) maps the network card (NIC) receive queue ring buffers directly into a user-space memory region (UMEM) via zero-copy. An eBPF program at the driver level redirects packet data directly into the user-space ring.
* **Benefits**:
  - **Zero-copy** packet retrieval with zero kernel context switches.
  - Able to handle 10M+ packets/sec on a single core.
* **Trade-offs**: Requires modern Linux kernel support and privileged container access (`CAP_NET_ADMIN`).

---

## 4. Packet Payload Optimization (Jumbo Frames)
* **Core Concept**: Increase the amount of data processed per system call by sending larger packets.
* **Mechanism**: Configure the network infrastructure to support 9,000-byte Jumbo Frames. The StatsD client proxy can then pack up to ~750 metrics into a single UDP packet instead of splitting at the standard 1,500-byte MTU limit.
* **Benefits**:
  - Drops packet rate and system call counts by $6\times$ for the same ingestion volume.

---

## 5. Linux Zero-Copy Packet Rings (PACKET_TX_RING / PACKET_RX_RING)
* **Core Concept**: Zero-copy network writes/reads using memory-mapped ring buffers shared between user-space and kernel.
* **Mechanism**: Use `PF_PACKET` sockets with `PACKET_TX_RING` on the writer and `PACKET_RX_RING` on the aggregator. 
* **Benefits**: Bypasses memory copies for UDP transmission and reception while still sending actual network packets.
* **Trade-offs / Security Concerns**:
  - **Security Privileges**: Requires `CAP_NET_RAW` capabilities. In Docker, this requires running containers with `--cap-add=NET_RAW` or `--privileged`, reducing container isolation.
  - **Payload Construction**: Bypasses the kernel's protocol stack, requiring user space to manually construct/parse Ethernet, IP, and UDP headers for every network packet.
  - **Portability**: Linux-specific APIs that break compatibility with macOS local test suites.

---

## 6. MSG_ZEROCOPY on Standard Sockets
* **Core Concept**: Page pinning to avoid user-to-kernel copy on standard UDP sockets without raw socket overhead.
* **Mechanism**: Enable `SO_ZEROCOPY` on the socket and pass `MSG_ZEROCOPY` to the socket send calls.
* **Trade-offs**:
  - Requires polling transmission completion notifications from the socket error queue (`MSG_ERRQUEUE`).
  - For small packets (like our 1.2 KB updates), page pinning and error queue polling overhead exceeds standard CPU cache copying cost. Only beneficial for packets larger than 10 KB.

---

## 7. eBPF Socket Redirection (sockmap)
* **Core Concept**: Bypass loopback TCP/IP IP routing layers inside the kernel.
* **Mechanism**: Attach an eBPF program to the sockets. When the writer socket sends data, eBPF redirects the network buffers directly to the receiver socket's queue inside the kernel.
* **Benefits**: 
  - 100% transparent to user-space code (requires zero modifications to client or database binaries).
  - Significantly reduces loopback CPU overhead.
* **Trade-offs**: Requires container host administration privileges and kernel eBPF tool chains to load the eBPF program.

