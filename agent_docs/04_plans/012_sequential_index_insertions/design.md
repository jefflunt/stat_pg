# Sequential B-Tree Index Insertions Design

We identified that `ExecCheckIndexConstraints` (23.91% CPU) and `index_getnext_slot` (19.57% CPU) are the dominant bottlenecks during the coordinator merge phase. This is because bulk inserts into B-Tree indexed tables in unsorted order cause random page lookups, leaf page splits, and buffer pool cache thrashing.

---

## Optimization Strategy: Sorted Sequential Inserts

If input rows are sorted by the index keys `(at, metric_id)` before they are inserted, PostgreSQL can optimize the B-Tree writes. 
- **Sequential Write Path**: Postgres detects that new keys are strictly increasing and inserts them directly into the rightmost leaf page of the B-Tree, completely bypassing random page lookups and page splits.
- **Cache Locality**: Only the hot rightmost page of the index is modified in memory, minimizing cache misses.

We will append `ORDER BY` clauses matching the table primary keys to the `SELECT` statements in all 5 rollup merge queries.

### Query Updates

1. **`evt_secs`**:
   ```sql
   INSERT INTO evt_secs (metric_id, at, value)
   SELECT metric_id, at, SUM(value) FROM stat_pg_temp GROUP BY metric_id, at
   ORDER BY at, metric_id
   ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_secs.value + EXCLUDED.value;
   ```

2. **`evt_mins`**:
   ```sql
   INSERT INTO evt_mins (metric_id, at, value)
   SELECT metric_id, at / 60, SUM(value) FROM stat_pg_temp GROUP BY metric_id, at / 60
   ORDER BY at / 60, metric_id
   ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_mins.value + EXCLUDED.value;
   ```

3. **`evt_hours`**:
   ```sql
   ORDER BY at / 3600, metric_id
   ```

4. **`evt_days`**:
   ```sql
   ORDER BY at / 86400, metric_id
   ```

5. **`evt_30s`**:
   ```sql
   ORDER BY at / 2592000, metric_id
   ```

---

## Proposed Changes

### Database Extension

#### [MODIFY] [lib.rs](file:///Users/jefflunt/code/stat_pg/src/lib.rs)
- Modify the 5 SQL merge queries in `coordinator_merge` to add the sorting clauses.
