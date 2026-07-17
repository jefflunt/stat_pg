# Step: 002 - UDP Listener and Parser

## Goal
Implement the UDP socket binding and packet parsing.

## Tasks
- [ ] Implement socket creation binding to the configured bind address and port.
- [ ] Configure a read timeout on the socket (e.g. 100ms) to allow the main loop to check signal states.
- [ ] Implement parsing logic to parse UDP payloads formatted as `code value timestamp`. Handle malformed input gracefully by logging a warning and skipping the packet.

## Verification
- Compile and run unit tests for the parser.
