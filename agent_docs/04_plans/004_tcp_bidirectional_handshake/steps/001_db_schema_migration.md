# Step: 001 - Database Schema Migration

## Goal
Update the PostgreSQL extension schema to support metric name-to-integer ID mapping and integer-based event tables.

---

## Tasks
1. Define the registration table `stat_pg_metrics`:
   ```sql
   CREATE TABLE IF NOT EXISTS stat_pg_metrics (
       id SERIAL PRIMARY KEY,
       name VARCHAR NOT NULL UNIQUE
   );
   ```
2. Update event tables (`evt_secs`, `evt_mins`, `evt_hours`, `evt_days`, `evt_30s`):
   * Replace `code VARCHAR` with `metric_id INT`.
   * Primary key becomes `(at, metric_id)`.
3. Update `tests` mod in `src/lib.rs` to reflect the new table schemas.
