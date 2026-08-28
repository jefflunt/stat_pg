# Optimize Database Aggregator Thread

We will optimize the database aggregator thread by removing two key CPU bottlenecks:
1. **Dynamic heap allocations** inside the `recvmmsg_sys` system call wrapper.
2. **Runtime slicing/assertions** during packet byte parsing.

---

## 1. Eliminate Heap Allocations in `recvmmsg_sys`

Currently, `recvmmsg_sys` maps and collects vectors of `libc::iovec` and `libc::mmsghdr` dynamically on every socket read:
```rust
let mut iovecs: Vec<libc::iovec> = bufs.iter_mut().map(...).collect();
let mut mmsg: Vec<libc::mmsghdr> = iovecs.iter_mut().map(...).collect();
```
This forces two memory allocation/deallocation calls (malloc/free) per read call.
- **Proposed Optimization**: Pre-allocate fixed-size stack arrays of size 32 (matching the receiver batch size) and populate them inline, bypassing heap allocation entirely.

---

## 2. Eliminate Slice/Assertion Overhead in Parsing

Currently, packet bytes are extracted using slice indexing and `try_into().unwrap()`, which compiles to bounds checking and runtime panic branches:
```rust
let id_bytes = &bufs[i][offset..offset + 4];
let val_bytes = &bufs[i][offset + 4..offset + 12];
let id = i32::from_le_bytes(id_bytes.try_into().unwrap());
let value = f64::from_le_bytes(val_bytes.try_into().unwrap());
```
- **Proposed Optimization**: Use direct pointer offset reads via `std::ptr::read_unaligned` to compile down to raw unaligned memory load instructions, completely removing runtime slicing overhead:
  ```rust
  let ptr = unsafe { bufs[i].as_ptr().add(offset) };
  let id = unsafe { std::ptr::read_unaligned(ptr as *const i32) };
  let value = unsafe { std::ptr::read_unaligned(ptr.add(4) as *const f64) };
  ```

---

## Proposed Changes

### Database Extension

#### [MODIFY] [lib.rs](file:///Users/jefflunt/code/stat_pg/src/lib.rs)
- Reimplement `recvmmsg_sys` using stack-allocated arrays of size 32.
- Refactor the packet parsing logic in `stat_pg_main_inner` (both first batch and drain loop) to use pointer-based `read_unaligned`.
