# Step: 003 - End-to-End Benchmark Test

## Goal
Verify the benchmark tool successfully exercises the dockerized database under high traffic.

## Tasks
- [ ] Reset database tables.
- [ ] Run the benchmark tool for 5 seconds targeting `127.0.0.1:8125`.
- [ ] Check Postgres logs to confirm the background worker stayed stable.
- [ ] Query Postgres tables (`evt_secs`, etc.) to confirm metrics were successfully aggregated.

## Verification
- Confirmed stable runs and metrics accumulated in Postgres tables.
