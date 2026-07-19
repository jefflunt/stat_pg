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
