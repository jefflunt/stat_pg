# Step: 001 - GUC and Registration

## Goal
Implement GUC configuration settings and register the background worker.

## Tasks
- [ ] Define static configuration GUCs:
  - `stat_pg.database` (default: `"postgres"`)
  - `stat_pg.user` (default: `"postgres"`)
  - `stat_pg.bind_address` (default: `"0.0.0.0"`)
  - `stat_pg.port` (default: `8125`)
  - `stat_pg.flush_interval` (default: `1.0`)
- [ ] Implement `_PG_init()` to define these GUCs and load the background worker with SPI and Shared Memory access enabled.

## Verification
- Compile the code inside the container: `cargo check`.
