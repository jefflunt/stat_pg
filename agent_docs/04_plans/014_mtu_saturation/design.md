# Phase 1: MTU Saturation Ingestion Optimization

We will increase the number of updates packed into each UDP packet from 100 to 120. This pushes the payload size to 1,440 bytes, which fits perfectly under the standard 1,500-byte network MTU limit without fragmentation. This will reduce overall packet rates and system call overhead by 20%.

---

## Technical Feasibility

- **Database Extension**: The receiver parsing loop in `src/lib.rs` dynamically reads the UDP packet payload until the buffer length is exhausted (`while offset + 12 <= n`). Therefore, the database extension requires **zero code changes** to support different batch sizes.
- **Benchmark Client**: We must update `bench/src/main.rs` to prebuild packets containing 120 updates (1,440 bytes) and adjust metric counters accordingly.

---

## Proposed Changes

### Benchmark Tool

#### [MODIFY] [bench/src/main.rs](file:///Users/jefflunt/code/stat-pg/bench/src/main.rs)
- Update updates per packet from 100 to 120.
- Update packet byte size constant from 1200 to 1440.
