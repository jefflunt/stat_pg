# Phase 2: Variable-Length Mixed Format Updates Design

We will implement a variable-length update encoding format that mixes `FLOAT64` and `INT32` values. 

---

## 1. Bit-Flag Protocol Design (User-Space)

To represent the format of each metric update without adding payload overhead, we will hijack the **highest bit (sign bit)** of the 4-byte `metric_id`:
- **FLOAT64 Format (`bit 31 = 0`)**: The raw ID is positive ($id > 0$). The value is encoded as an 8-byte `f64` float. (Update size = 12 bytes).
- **INT32 Format (`bit 31 = 1`)**: The raw ID is negative ($id < 0$). The value is encoded as a 4-byte `i32` integer. (Update size = 8 bytes).

### Database Receiver Parsing
Inside the database receiver thread (`stat_pg_main_inner`):
```rust
let mut offset = 0;
while offset + 8 <= n {
    let ptr = unsafe { bufs[i].as_ptr().add(offset) };
    let raw_id = unsafe { std::ptr::read_unaligned(ptr as *const i32) };
    if raw_id < 0 {
        // Sign bit is set -> INT32 format (8 bytes total)
        let id = raw_id & 0x7FFFFFFF; // Clear the sign bit
        let val = unsafe { std::ptr::read_unaligned(ptr.add(4) as *const i32) } as f64;
        if id > 0 && (id as usize) <= max_id && (id as usize) < active.len() {
            unsafe { *active.get_unchecked_mut(id as usize) += val; }
        }
        offset += 8;
    } else {
        // Sign bit is 0 -> FLOAT64 format (12 bytes total)
        if offset + 12 > n {
            break; // Malformed boundary protection
        }
        let id = raw_id;
        let val = unsafe { std::ptr::read_unaligned(ptr.add(4) as *const f64) };
        if id > 0 && (id as usize) <= max_id && (id as usize) < active.len() {
            unsafe { *active.get_unchecked_mut(id as usize) += val; }
        }
        offset += 12;
    }
}
```

---

## 2. Benchmark Client Packet Generation

During packet prebuilding, we will randomize the format (50% chance for `INT32`, 50% chance for `FLOAT64`) for each update, and pack updates into each packet until the total size is as close to 1,440 bytes as possible.
- Each prebuilt packet will store:
  1. The raw packet byte vector (`Vec<u8>`).
  2. The number of updates packed inside it (`u32`).

---

## Proposed Changes

### Database Extension
#### [MODIFY] [src/lib.rs](file:///Users/jefflunt/code/stat_pg/src/lib.rs)
- Refactor the packet parsing loops in `stat_pg_main_inner` to decode both sign-extended `INT32` (8-byte) and `FLOAT64` (12-byte) formats.

### Benchmark Tool
#### [MODIFY] [bench/src/main.rs](file:///Users/jefflunt/code/stat_pg/bench/src/main.rs)
- Refactor packet prebuilding to randomly alternate format types.
- Saturate packets to 1,440 bytes and track individual packet update counts.
- Modify the sender loop to accumulate metrics using the packet-specific counts.
