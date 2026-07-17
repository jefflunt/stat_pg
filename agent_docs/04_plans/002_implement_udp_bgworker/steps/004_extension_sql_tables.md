# Step: 004 - SQL Table Creation

## Goal
Configure the tables creation script using pgrx `extension_sql!` macro.

## Tasks
- [ ] Define the schemas for `evt_secs`, `evt_mins`, `evt_hours`, `evt_days`, and `evt_30s`.
- [ ] Embed the table creation statements in the library file using `extension_sql!`. Ensure they are marked with `IF NOT EXISTS` to be safe across reloads.
- [ ] Fix the 30-day interval calculation bug (`2592000` seconds instead of `3592000`).

## Verification
- Confirm tables are created automatically on `CREATE EXTENSION stat_pg;`.
