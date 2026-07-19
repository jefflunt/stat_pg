# Step 1: Fixed-Width 2-Byte Metric IDs

We will implement 2-byte ID parsing on the database side and randomized generation on the client side.

---

## Code Tasks

1. **Modify `src/lib.rs`**:
   Refactor first-batch and drain-loop parsing in `stat_pg_main_inner` to read 2-byte FFI IDs.
2. **Modify `bench/src/main.rs`**:
   Update prebuilding packet logic to pack 2-byte IDs and check remaining space.

---

## Verification

1. **Compile and Test**:
   Verify correct compilation and run integration tests inside the container.
2. **Benchmark**:
   Run the 12-thread stress test to measure aggregator throughput and CPU consumption.
