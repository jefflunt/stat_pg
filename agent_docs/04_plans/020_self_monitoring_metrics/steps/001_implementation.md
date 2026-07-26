# Step 001: Implement Self-Monitoring and Subsystem Profiling Metrics

## Overview
Implement automatic system resource tracking, hot loop latency timers, batch size metrics, lock wait metrics, SQL helper functions, and SPI flush loop integration in `src/lib.rs`.

---

## Technical Details

1. **Subsystem Profiling Accumulators in `src/lib.rs`**:
   - `UDP_BATCH_COUNT`: Atomic counter for `recvmmsg` calls.
   - `UDP_PACKET_COUNT`: Atomic counter for total received UDP packets.
   - `UDP_PARSE_ERRORS`: Atomic counter for invalid packet payload parses.
   - `LOCK_WAIT_MICROS`: Atomic counter for active buffer lock acquisition wait time.
   - `EBPF_READ_MICROS`: Atomic counter for eBPF map read latency.

2. **System & Pipeline Metric Samplers**:
   - `get_system_cpu_pct(last_total: &mut u64, last_idle: &mut u64) -> f64`: `/proc/stat` CPU percentage.
   - `get_process_ram_bytes() -> f64`: `/proc/self/status` VmRSS memory in bytes.
   - `get_network_rx_bytes() -> u64`: `/proc/net/dev` network interface RX bytes.
   - `get_udp_counters() -> (u64, u64)`: `/proc/net/snmp` InDatagrams & RcvbufErrors.

3. **SPI Transaction Loop Integration**:
   - Time `flush_slot` CSV writing and SQL `COPY` execution (`stat_pg.flush_duration_ms`).
   - Time `staging_merge` staging table merge transaction (`stat_pg.merge_duration_ms`).
   - Time `rollup_minutes`, `rollup_hours`, `rollup_days`, and `rollup_30s` execution.
   - Inject self-monitoring and profiling metrics into `stat_pg_staging` during each flush cycle.

4. **`#[pg_extern]` SQL Diagnostic Function**:
   - `stat_pg_self_metrics()`: Exposes live diagnostic view returning `(metric_name, value, unit)`.

5. **Integration Tests**:
   - `#[pg_test]` test `test_self_monitoring_metrics`: Asserts that `stat_pg.` metrics exist in `stat_pg_metrics` and record non-zero measurements in `stat_pg_secs`.
