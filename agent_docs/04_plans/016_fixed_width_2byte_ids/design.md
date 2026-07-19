# Fixed-Width 2-Byte Metric IDs Design (Option A)

We will scale down the network metric ID representation from 4 bytes to 2 bytes (`i16`). By using the highest bit as the type format flag, we support up to 32,767 unique registered metrics, which fits most single-node database installations.

---

## 1. Network Protocol Format

Every update in the UDP payload will have a 2-byte metric ID followed by a 4-byte or 8-byte value:
- **INT32 Format (`bit 15 = 1`)**: 2-byte ID + 4-byte value = **6 bytes** per update.
- **FLOAT64 Format (`bit 15 = 0`)**: 2-byte ID + 8-byte value = **10 bytes** per update.

### Database Receiver Parsing
Inside the database receiver thread (`stat_pg_main_inner`), we read `i16` IDs:
```rust
let mut offset = 0;
while offset + 6 <= n {
    let ptr = unsafe { bufs[i].as_ptr().add(offset) };
    let raw_id = unsafe { std::ptr::read_unaligned(ptr as *const i16) };
    if raw_id < 0 {
        // INT32 format (6 bytes total)
        let id = (raw_id & 0x7FFF) as i32;
        let value = unsafe { std::ptr::read_unaligned(ptr.add(2) as *const i32) } as f64;
        if id > 0 && (id as usize) <= max_id && (id as usize) < active.len() {
            unsafe { *active.get_unchecked_mut(id as usize) += value; }
        }
        offset += 6;
    } else {
        // FLOAT64 format (10 bytes total)
        if offset + 10 > n {
            break;
        }
        let id = raw_id as i32;
        let value = unsafe { std::ptr::read_unaligned(ptr.add(2) as *const f64) };
        if id > 0 && (id as usize) <= max_id && (id as usize) < active.len() {
            unsafe { *active.get_unchecked_mut(id as usize) += value; }
        }
        offset += 10;
    }
}
```

---

## 2. Benchmark Client Packet Generation

We will pack updates until the total size is as close to 1,440 bytes as possible.
- Minimum remaining space to insert another update: **10 bytes** (the size of a FLOAT64 update).
- Updates will pack to exactly under 1,440 bytes, yielding an average of **180 updates per packet** for a 50/50 mix.

---

## Proposed Changes

### Database Extension
#### [MODIFY] [src/lib.rs](file:///Users/jefflunt/code/stat-pg/src/lib.rs)
- Refactor the first-batch and drain-loop parsing logic in `stat_pg_main_inner` to read 2-byte FFI IDs.

### Benchmark Tool
#### [MODIFY] [bench/src/main.rs](file:///Users/jefflunt/code/stat-pg/bench/src/main.rs)
- Refactor packet prebuilding to serialize 2-byte IDs (`i16`) and check remaining MTU capacity against 10 bytes.
- Align loops and console display configurations to show 180 updates/batch averages.
