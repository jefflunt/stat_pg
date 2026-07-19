# Step 1: Optimize Benchmark Sender Loop

We will cache the registered metric IDs in a flat `Vec` before spawning threads, and replace formatting/lookups in the hot loop with direct array lookups.

---

## Code Tasks

1. **Modify `bench/src/main.rs`**:
   Cache metric IDs in a flat list:
   ```rust
   let mut metric_ids = Vec::with_capacity(unique_metrics as usize);
   for idx in 0..unique_metrics {
       let name = format!("bench.metric.{}", idx);
       let id = *name_to_id.get(&name).expect("Registered metric not found");
       metric_ids.push(id);
   }
   let metric_ids = Arc::new(metric_ids);
   ```

2. **Update Sender Thread Loop**:
   Clone the `metric_ids` Arc and index it inside the loops.

---

## Verification

1. **Build and Run**:
   Compile the benchmark tool and run a 1-thread, 60-second test to compare the ingestion throughput.
