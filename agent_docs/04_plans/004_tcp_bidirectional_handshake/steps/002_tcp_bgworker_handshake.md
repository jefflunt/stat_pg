# Step: 002 - TCP Server & Client Registration Handshake

## Goal
Implement a TCP listener server inside the background worker to handle client connections and process the bidirectional name-registration handshake.

---

## Tasks
1. Replace UDP socket with `TcpListener` on port 8125.
2. At startup, load all existing metric names and IDs from `stat_pg_metrics` into an in-memory cache (`HashMap<String, i32>`).
3. Spawn a reader thread for each accepted TCP client socket connection.
4. Implement TCP message parser for registration (0x01) and metric data (0x02).
5. For registration:
   * Look up the string name in the cache.
   * If not found, run a database transaction to insert the name and return the new ID, updating the in-memory cache.
   * Return the OID/ID response to the TCP client.
