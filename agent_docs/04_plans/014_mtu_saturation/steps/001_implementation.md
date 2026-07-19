# Step 1: MTU Saturation (120 updates/packet)

We will update the benchmark client to use 120 updates per packet.

---

## Code Tasks

1. **Modify `bench/src/main.rs`**:
   - Change `for _ in 0..100` to `for _ in 0..120` inside the packet prebuilding loop.
   - Update `Vec::with_capacity(1200)` to `Vec::with_capacity(1440)`.
   - Update `local_metrics += (sent_count * 100)` to `local_metrics += (sent_count * 120)`.
   - Update `local_bytes += (sent_count * 1200)` to `local_bytes += (sent_count * 1440)`.

---

## Verification

1. **Build and Run**:
   Compile the benchmark client and run a 12-threaded, 60-second test to measure the impact of 120 updates/packet.
