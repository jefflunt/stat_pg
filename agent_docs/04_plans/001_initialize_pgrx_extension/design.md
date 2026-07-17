# Plan: 001 - Initialize PGRX Extension in Docker

## Goal
Set up a containerized development environment for compiling and testing the `stat-pg` PostgreSQL extension using Rust and `pgrx`, without polluting the local host machine.

## Scope
- **In-Scope**:
  - Docker environment setup (Dockerfile, Docker Compose).
  - Initialization of the Rust project template with `cargo-pgrx`.
  - Verification that the compiled extension can be loaded into the running PostgreSQL container.
- **Out-of-Scope**:
  - Implementing the actual UDP background worker socket listener (to be handled in a follow-up plan).
  - Writing the database tables schema in production (just testing the extension loading).

## Architecture

We will run a containerized PostgreSQL instance (`postgres:16`) equipped with Rust compile dependencies. 
We will bind the host workspace directory to the container. By compiling inside the container using `cargo-pgrx`, the output libraries are placed directly in the container's PG instance directories, making live reload and testing clean and isolated.

## Implementation Steps

We will execute the following steps (detailed in the `steps/` subdirectory):
- **T001**: Docker Setup (Dockerfile.dev and docker-compose.yml)
- **T002**: Initialize pgrx Rust Template
- **T003**: Verify Extension Build & Run inside Docker

## Verification Plan
1. Start the container using `docker compose up --build`.
2. Connect to the PostgreSQL container: `docker compose exec db psql -U postgres`.
3. Run `CREATE EXTENSION stat_pg;` to verify it successfully loads.
4. Verify the extension is listed in `\dx`.
