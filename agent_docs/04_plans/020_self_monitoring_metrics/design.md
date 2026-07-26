# Design: Self-Monitoring & Internal Subsystem Profiling Metrics (STAT-82)

This document specifies the design for internal self-monitoring and subsystem performance profiling metrics in `stat-pg` under the `stat_pg.` metric namespace.

---

## 1. User Story & Goal
As a database administrator / system operator using `stat-pg`, I want the extension to automatically monitor and record system resource usage and subsystem profiling metrics (batch sizes, pipeline latencies, lock wait times, rollup durations) so that I can diagnose bottlenecks, tune performance, and monitor system health natively within `stat-pg` tables (`stat_pg_secs`, `stat_pg_mins`, etc.).

---

## 2. Requirements & Namespace
All metrics will be automatically registered in `stat_pg_metrics` with `type = 'numeric'` under the `stat_pg.` namespace:

### System & Infrastructure Metrics
| Metric Name | Type | Description | Source |
|---|---|---|---|
| `stat_pg.defined_metrics` | numeric | Total count of metrics registered | `SELECT COUNT(*) FROM stat_pg_metrics` |
| `stat_pg.bg_workers` | numeric | Number of background worker processes | `WORKERS.get()` |
| `stat_pg.cpu_consumed` | numeric | CPU utilization percentage | `/proc/stat` delta |
| `stat_pg.ram_consumed` | numeric | Resident memory consumed (VmRSS) in bytes | `/proc/self/status` |
| `stat_pg.disk_space_consumed` | numeric | Total database disk space in bytes | `pg_database_size(current_database())` |
| `stat_pg.network_bandwidth` | numeric | Network bandwidth throughput in bits/sec | `/proc/net/dev` rx bytes delta * 8 / interval |
| `stat_pg.udp_received` | numeric | Total UDP datagrams received | `/proc/net/snmp` InDatagrams |
| `stat_pg.udp_dropped` | numeric | Total UDP datagrams dropped due to buffer errors | `/proc/net/snmp` RcvbufErrors |

### Subsystem & Hot Loop Profiling Metrics
| Metric Name | Type | Description | Unit / Measurement |
|---|---|---|---|
| `stat_pg.udp_packets_parsed_sec` | numeric | UDP packets parsed per second | count/sec |
| `stat_pg.udp_batch_size_avg` | numeric | Average packet batch size per `recvmmsg` call | packets/batch (0 - 128) |
| `stat_pg.udp_parse_errors` | numeric | Total malformed packet parse errors | count |
| `stat_pg.flush_duration_ms` | numeric | Time spent formatting CSV and executing COPY to staging | milliseconds |
| `stat_pg.flushed_rows_count` | numeric | Count of non-zero metric values written per flush cycle | count |
| `stat_pg.merge_duration_ms` | numeric | Time spent in `staging_merge` SQL transaction | milliseconds |
| `stat_pg.merged_rows_count` | numeric | Count of aggregated metric rows merged into `stat_pg_secs` | count |
| `stat_pg.rollup_minute_duration_ms` | numeric | Time spent performing minute rollups (`stat_pg_mins`) | milliseconds |
| `stat_pg.rollup_hour_duration_ms` | numeric | Time spent performing hour rollups (`stat_pg_hours`) | milliseconds |
| `stat_pg.rollup_day_duration_ms` | numeric | Time spent performing day rollups (`stat_pg_days`) | milliseconds |
| `stat_pg.rollup_30d_duration_ms` | numeric | Time spent performing 30-day rollups (`stat_pg_30s`) | milliseconds |
| `stat_pg.ebpf_read_duration_us` | numeric | Time spent reading and zeroing per-CPU BPF map entries | microseconds |
| `stat_pg.active_buffer_lock_wait_us` | numeric | Microseconds spent acquiring `active_buffer` mutex lock | microseconds |

---

## 3. Architecture & Execution
During execution:
1. **Thread-Local / Atomic Counters**: UDP threads and background worker flush loops update atomic/thread-local accumulators for batch sizes, parse errors, lock wait times, and step durations.
2. **Periodic Metric Flush**: Worker 0 aggregates system metrics and subsystem profiling metrics during each `flush_interval`.
3. **Cascading Rollups**: Standard `staging_merge` and rollup routines (`rollup_minutes`, `rollup_hours`, `rollup_days`, `rollup_30s`) aggregate self-monitoring and profiling data seamlessly.
4. **SQL On-Demand Query Function**: Expose `stat_pg_self_metrics()` as a `#[pg_extern]` returning `(metric_name, value, unit)`.

---

## 4. Verification Plan
- Unit tests (`#[pg_test]`): Verify auto-registration of self-monitoring and profiling metrics in `stat_pg_metrics`, and verify rows inserted into `stat_pg_secs`.
- Manual Verification: Call `SELECT * FROM stat_pg_self_metrics();` in `psql` to view real-time latency and throughput stats.
