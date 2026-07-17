# Step: 001 - Bench Crate Setup

## Goal
Set up the standalone Cargo package inside `bench/`.

## Tasks
- [ ] Create directory `bench/` and run `cargo init` inside it.
- [ ] Configure `bench/Cargo.toml` with standard high-performance dependencies (e.g. `clap` for args, `indicatif` or helper for rendering if needed, or keep it dependency-free to compile instantly).
  *Note: To minimize build times and keep compile times extremely fast, we can use standard library `std::env` for argument parsing or keep dependencies to a minimum.*

## Verification
- Run `cargo check` inside `bench/` to verify a successful clean environment.
