# Optimize Benchmark Sender Hot Loop Design

We identified that the benchmark writer thread is CPU-bound and unable to saturate the database ingestion because it performs expensive heap allocations (`format!`) and hashmap lookups (`get`) inside the hot send loop.

---

## Analysis of the Bottleneck

Inside `bench/src/main.rs`'s UDP transmission loop, the following operations run for every single metric update in a batch of 100:
```rust
let metric_idx = batch_idx % unique_metrics as usize;
let metric_name = format!("bench.metric.{}", metric_idx); // Heap allocation (Slow!)
let &metric_id = name_to_id_clone.get(&metric_name).unwrap(); // Hashing & Map Lookup (Slow!)
```

At millions of packets per second, these dynamic heap allocations and hash lookups completely peg the CPU cores of the benchmark sender.

---

## Optimization Proposal

We will pre-compile the metric IDs into a contiguous, flat `Vec<i32>` ordered by metric index `0..unique_metrics`. 

1. **Pre-Cache Array**:
   ```rust
   let mut metric_ids = Vec::with_capacity(unique_metrics as usize);
   for idx in 0..unique_metrics {
       let name = format!("bench.metric.{}", idx);
       let id = *name_to_id.get(&name).expect("Registered metric not found");
       metric_ids.push(id);
   }
   let metric_ids = Arc::new(metric_ids);
   ```

2. **Hot Loop Direct Indexing**:
   Inside the sender loop, we replace formatting and lookup with a direct array index access:
   ```rust
   let metric_idx = batch_idx % unique_metrics as usize;
   let metric_id = metric_ids_clone[metric_idx]; // O(1) Index Access (Extremely Fast!)
   ```

This will completely remove memory allocation and map lookup overhead, allowing the sender threads to generate packets at maximum wire-speed.
