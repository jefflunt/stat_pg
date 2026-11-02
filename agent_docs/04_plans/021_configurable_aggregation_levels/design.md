# Design: Configurable Aggregation Levels (Issue #1)

This document specifies the design for allowing users to toggle individual aggregation resolution levels (`secs`, `mins`, `hours`, `days`, `30s`) in `stat-pg`.

---

## 1. User Story & Goal
As a PostgreSQL administrator operating `stat_pg`, I may not need all resolution tables (for example, fine-grained per-second stats in `stat_pg_secs` or long-term 30-day aggregates in `stat_pg_30s`), and storing unneeded resolutions wastes storage I/O, disk space, and coordinator CPU time.

I want GUC configuration flags to disable unwanted aggregation levels, and have any higher enabled rollup levels dynamically adapt and aggregate from the nearest lower active resolution or directly from `stat_pg_temp`.

---

## 2. Configuration GUCs

The following boolean GUCs are registered with SIGHUP context (reloadable without restart):

| GUC Parameter | Default | Resolution Table | Description |
|---|---|---|---|
| `stat_pg.aggregate_secs` | `true` (`on`) | `stat_pg_secs` | Enables per-second metric aggregation |
| `stat_pg.aggregate_mins` | `true` (`on`) | `stat_pg_mins` | Enables per-minute metric aggregation |
| `stat_pg.aggregate_hours` | `true` (`on`) | `stat_pg_hours` | Enables hourly metric aggregation |
| `stat_pg.aggregate_days` | `true` (`on`) | `stat_pg_days` | Enables daily metric aggregation |
| `stat_pg.aggregate_30s` | `true` (`on`) | `stat_pg_30s` | Enables 30-day metric aggregation |

---

## 3. Implementation Details

1. **GUC Registration**:
   Registered via `GucRegistry::define_bool_guc` with `GucContext::Sighup` in `_PG_init()`.

2. **Coordinator Merge Adaptation (Option A)**:
   - If `stat_pg.aggregate_secs` is enabled, staging data from `stat_pg_temp` is aggregated and merged into `stat_pg_secs`. If disabled, insertion into `stat_pg_secs` is skipped.
   - For minute rollups (`stat_pg_mins`):
     - If `stat_pg.aggregate_secs` is enabled, rollup pulls from `stat_pg_secs`.
     - If `stat_pg.aggregate_secs` is disabled, rollup pulls directly from `stat_pg_temp` (or staging data).
   - For hour rollups (`stat_pg_hours`):
     - Pulls from `stat_pg_mins` if enabled; otherwise falls back to `stat_pg_secs` if enabled; otherwise falls back to `stat_pg_temp`.
   - Subsequent levels (`days`, `30s`) follow the same cascading fallback mechanism.
   - If all aggregation levels are disabled, raw staging data is consumed and cleared without writing to any resolution table.

3. **Force Rollups Function**:
   `stat_pg_force_rollups()` checks the active GUC flags and only runs rollups for the enabled resolution tiers, using the same fallback cascading logic.

---

## 4. Verification

- `pg_test_skip_aggregate_secs`: Verifies disabling `stat_pg.aggregate_secs` skips `stat_pg_secs` while higher rollups continue to function.
- `pg_test_skip_aggregate_secs_and_mins`: Verifies disabling both second and minute tiers correctly routes rollups to `stat_pg_hours`.
- `pg_test_skip_all_aggregations`: Verifies no resolution tables are populated when all flags are `off`.
- `pg_test_force_rollups_with_skipped_levels`: Verifies manual `stat_pg_force_rollups()` runs safely when intermediate levels are disabled.
