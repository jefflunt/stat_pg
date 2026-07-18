# Step: 003 - Metric Aggregation Refactoring

## Goal
Modify the aggregation cache inside the background worker to aggregate based on `metric_id` instead of string `code` names, accelerating lookups and eliminating string hashing.

---

## Tasks
1. Refactor `MetricCache` structures to store `(i32, i64)` as key instead of `(Arc<str>, i64)`.
2. Refactor `Metric` struct to carry `metric_id: i32` instead of `code: Arc<str>`.
3. Use a high-performance non-cryptographic hasher (such as `FxHasher` from `rustc-hash` or a custom two-level fast hasher) for the integer-key maps.
4. Refactor bulk prepared statements:
   * Map `metric_id` (i32) parameter type to `INT4OID` (Oid 23).
   * Update type check: `TEXTOID` becomes `INT4OID`.
