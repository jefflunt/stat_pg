# Step 1: Client Socket Buffer Sizing

We will update the benchmark tool to set the UDP socket send buffer size to 50 MB.

---

## Code Tasks

1. **Modify `bench/src/main.rs`**:
   Before connecting the UDP socket, set the send buffer size:
   ```rust
   let socket = match UdpSocket::bind("0.0.0.0:0") {
       Ok(s) => s,
       Err(e) => { ... }
   };
   let _ = socket.set_send_buffer_size(50 * 1024 * 1024);
   ```

---

## Verification

1. **Build and Run**:
   Compile the benchmark tool and run the benchmark to verify.
