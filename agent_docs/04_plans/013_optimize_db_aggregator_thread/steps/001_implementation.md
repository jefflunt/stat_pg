# Step 1: Optimize Database Aggregator Thread

We will optimize the database aggregator thread's network reading and packet parsing.

---

## Code Tasks

1. **Modify `src/lib.rs`**:
   - Reimplement `recvmmsg_sys` using stack-allocated structures.
   - Refactor parsing loops in `stat_pg_main_inner` to use `read_unaligned`.

---

## Verification

1. **Build and Test**:
   Verify correct compilation and run integration tests inside the container.
2. **Benchmark**:
   Run the 12-thread stress test to measure aggregator throughput and CPU consumption improvements.
