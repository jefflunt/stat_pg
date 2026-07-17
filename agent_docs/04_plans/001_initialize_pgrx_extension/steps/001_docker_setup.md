# Step: 001 - Docker Setup

## Goal
Create `Dockerfile.dev` and `docker-compose.yml` to define the containerized Postgres + Rust environment.

## Tasks
- [ ] Create `Dockerfile.dev` based on `postgres:16` that installs Rust and build tools.
- [ ] Create `docker-compose.yml` defining a `db` service with port forwards for Postgres (5432) and UDP metrics (8125).

## Verification
- Run `docker compose config` to verify the configuration syntax is correct.
