# Step: 003 - In-memory Aggregation and SPI Flush

## Goal
Implement the core in-memory aggregation maps and the SPI flush routine.

## Tasks
- [ ] Implement data structures to aggregate metrics across the five resolutions in memory: seconds, minutes, hours, days, and 30-days.
- [ ] Connect the background worker to the SPI database during initialization using `connect_worker_to_spi`.
- [ ] Implement a flush handler that swaps active memory maps and runs batch SQL upserts using `Spi::connect` every `flush_interval` seconds.

## Verification
- Verify database writes occur periodically and handle connection failures gracefully.
