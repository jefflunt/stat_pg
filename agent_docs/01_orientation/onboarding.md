# Onboarding and Setup

This document describes how to set up your environment to develop, compile, test, and run the `stat-pg` PostgreSQL extension using Docker.

## Environment Dependencies

To run the development environment, the host system only needs:
1. **Docker & Docker Compose**

All other toolchains (Rust, `cargo-pgrx`, PostgreSQL developer libraries) are pre-packaged inside the development container.

## Step-by-Step Instructions

### 1. Build and Start the Container
Start the containerized PostgreSQL instance and development environment:
```bash
docker compose up --build -d
```
This binds:
- PostgreSQL on host port **`5433`** (avoiding conflict with local PG installations).
- UDP Ingestion socket on host port **`8125`**.

### 2. Enter the Development Shell
To run Cargo commands or access the container's shell:
```bash
docker compose exec db bash
```

### 3. Initialize/Create the Extension Template (First Time)
To create a new extension project named `stat_pg`:
```bash
docker compose exec db cargo pgrx new stat_pg
```

### 4. Build and Install the Extension
To compile the extension and copy the `.so` and `.control` files directly into the container's PostgreSQL system folders:
```bash
docker compose exec db cargo pgrx install
```

### 5. Load and Test the Extension
Connect to the database via client on host port `5433` or inside the container:
```bash
docker compose exec db psql -U postgres
```
Inside the `psql` console, run:
```sql
CREATE EXTENSION stat_pg;
```
Verify it is loaded:
```sql
SELECT hello_stat_pg();
```
