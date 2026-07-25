# eBPF XDP Kernel Network Bypass Pattern

This pattern describes the design and implementation of kernel-space network bypass metric aggregation using eBPF (Extended Berkeley Packet Filter) and XDP (eXpress Data Path) hooks.

---

## Architecture & Data Flow

```mermaid
graph TD
    NIC[Network Interface Card: eth0] -->|Raw Ethernet Frames| XDP[XDP Driver Hook: stat_pg_xdp.bpf.c]
    
    subgraph eBPF Kernel Execution
        XDP -->|1. Parse Ethernet / IP / UDP| Parser[Header & Stride Decoder]
        Parser -->|2. Extract format & metric_id| Accumulator[Atomic Adder]
        Accumulator -->|3. Update key-value pairs| Map[BPF_MAP_TYPE_PERCPU_ARRAY: metrics_map]
    end
    
    XDP -->|4. Return XDP_PASS| Stack[Standard Linux Network Stack]
    
    subgraph PostgreSQL User Space
        SPI[PostgreSQL Extension: src/lib.rs] -->|Read Map via bpf_map_lookup_elem| Map
        SPI -->|stat_pg_udp_stats()| SQL[SQL Query Interface]
    end
```

---

## Key Components

### 1. eBPF XDP Program (`src/bpf/stat_pg_xdp.bpf.c`)
- **Hook Point**: `SEC("xdp")` hook running directly inside the network driver before Linux network stack buffer (`sk_buff`) allocation.
- **Payload Stride Parsing**: Decodes 16 binary metric payload formats (Formats 0–15) carried over UDP packets on port `8125`.
- **Item & Value Counters**: Increments per-CPU array counters in `metrics_map`:
  - `Key 0`: Total wire metric items received.
  - `Key 1..N`: Accumulated float value units for metric ID $N$.

### 2. Lock-Free Shared Memory Map (`metrics_map`)
- **Type**: `BPF_MAP_TYPE_PERCPU_ARRAY` with `max_entries = 65536`.
- **Pinned Path**: `/sys/fs/bpf/metrics_map`.
- **Lock-Free Aggregation**: Each CPU core updates its own isolated array slice without atomic lock contention or memory barrier overhead.

### 3. SQL User Interface (`src/lib.rs`)
- `stat_pg_udp_stats()`: Sums across all per-CPU map entries and returns real-time kernel ingestion statistics:
  - `ebpf_items`: Total wire update items processed in kernel space.
  - `ebpf_packets`: Total UDP packets received.
  - `ebpf_drops`: Total UDP packets dropped by socket queue.
- `stat_pg_ebpf_reset()`: Clears map entries for benchmark reset.

---

## Deployment & Lifecycle

1. **Compilation**:
   ```bash
   clang -g -O2 -target bpf -I/usr/include/aarch64-linux-gnu -c src/bpf/stat_pg_xdp.bpf.c -o src/bpf/stat_pg_xdp.bpf.o
   ```
2. **Kernel Loading & Pinning**:
   ```bash
   bpftool prog load src/bpf/stat_pg_xdp.bpf.o /sys/fs/bpf/stat_pg_xdp type xdp pinmaps /sys/fs/bpf/
   chmod 777 /sys/fs/bpf /sys/fs/bpf/metrics_map /sys/fs/bpf/stat_pg_xdp
   ```
3. **Driver Attachment**:
   ```bash
   bpftool net attach xdpgeneric pinned /sys/fs/bpf/stat_pg_xdp dev eth0
   ```
4. **Detachment**:
   ```bash
   bpftool net detach xdpgeneric dev eth0
   rm -f /sys/fs/bpf/stat_pg_xdp /sys/fs/bpf/metrics_map
   ```
