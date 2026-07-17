# Step: 005 - End-to-End Verification

## Goal
Verify the entire system end-to-end using UDP traffic inside the Docker container.

## Tasks
- [ ] Configure `postgresql.conf` in the container to load the extension by adding `shared_preload_libraries = 'stat_pg'`.
- [ ] Send UDP traffic using `nc -u` or a simple script from the host.
- [ ] Query the database tables (`evt_secs`, `evt_mins`, etc.) and verify stats are aggregated.

## Verification
- Successful aggregation of stats inside all time resolution tables.
