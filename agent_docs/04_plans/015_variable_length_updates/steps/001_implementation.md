# Step 1: Variable-Length Mixed Format Updates

We will implement the mixed format parsing on the database side and the randomized generation on the client side.

---

## Code Tasks

1. **Modify `src/lib.rs`**:
   Refactor first-batch and drain-loop parsing in `stat_pg_main_inner` to read sign-extended variable-width formats.
2. **Modify `bench/src/main.rs`**:
   Update prebuilding packet logic to alternate formats and count metrics per packet, and update throughput calculations.

---

## Verification

1. **Compile and Test**:
   Verify correct compilation and run integration tests inside the container.
2. **Benchmark**:
   Run the 12-thread stress test to measure aggregator throughput and CPU consumption.
