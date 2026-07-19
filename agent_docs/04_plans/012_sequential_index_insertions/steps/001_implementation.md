# Step 1: Sequential Index Insertion Implementation

We will append sorting clauses to the SQL rollup queries in `src/lib.rs`.

---

## Code Tasks

1. **Modify `src/lib.rs`**:
   Update `coordinator_merge` to add `ORDER BY` clauses to all 5 database merge statements.

---

## Verification

1. **Build and Test**:
   Run the database tests to ensure correctness.
2. **Benchmark**:
   Run the 12-threaded benchmark to verify the performance gains on the database side.
