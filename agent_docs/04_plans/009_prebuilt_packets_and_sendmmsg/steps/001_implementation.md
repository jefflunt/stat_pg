# Step 1: Prebuilt Packets and sendmmsg Implementation

We will implement the prebuilt packet circular buffer in the benchmark tool, along with raw `sendmmsg` batching for Linux.

---

## Code Tasks

1. **Modify `bench/src/main.rs`**:
   - Implement Phase 1: Pre-generate a list of 1,000 packets per sender thread at startup.
   - Implement Phase 2: On Linux, pre-allocate `mmsghdr` and `iovec` arrays pointing to slices of 32 prebuilt packets, and execute raw `sendmmsg` system calls inside the sender loop.

---

## Verification

1. **Build and Run**:
   Compile the benchmark tool and run a 1-thread, 60-second test to verify throughput and CPU load.
