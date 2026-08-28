# 02 Patterns

This directory holds coding standards, conventions, and architectural patterns used throughout this repository.

## Documents

* [bgworker_thread_safety.md](file:///Users/jefflunt/code/stat_pg/agent_docs/02_patterns/bgworker_thread_safety.md): Explains the decoupled multi-process UDP listener thread design and memory access safety bounds checking to prevent PostgreSQL crash segmentations.
* [ebpf_xdp_kernel_bypass.md](file:///Users/jefflunt/code/stat_pg/agent_docs/02_patterns/ebpf_xdp_kernel_bypass.md): Describes lock-free kernel-space packet parsing and metric aggregation using eBPF XDP driver hooks and per-CPU maps.
