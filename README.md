# stat-pg

A minimalist, high-performance PostgreSQL extension for statistical and log event ingestion and aggregation.

## Overview

`stat-pg` implements a direct UDP ingestion port inside the PostgreSQL database engine. Rather than routing events through application-level queues, aggregators, and database triggers, `stat-pg` listens directly for incoming UDP packets, aggregates metrics in-memory, and flushes them to database tables in periodic batches.

This project is built to be extremely fast, minimalist, and highly reliable.

## Architecture

- **PostgreSQL Background Worker (BGWorker)**: Spawns directly inside the database process to bind and listen to a configured UDP socket.
- **In-Memory Accumulators**: Aggregates metrics (counts, sums) across multiple time resolutions (seconds, minutes, hours, days, 30-days) in-memory to prevent database lock contention.
- **SPI Batched Writes**: Every second, the worker flushes aggregated data directly to target tables (`evt_secs`, `evt_mins`, `evt_hours`, `evt_days`, `evt_30s`) in bulk using `INSERT ... ON CONFLICT DO UPDATE`.

## Documentation

This project uses the `agent_docs` framework under [agent_docs/](file:///Users/jefflunt/code/stat-pg/agent_docs/) for AI-assisted development, continuous alignment, and progressive disclosure. Start with [agent_docs/01_orientation/README.md](file:///Users/jefflunt/code/stat-pg/agent_docs/01_orientation/README.md) to understand the project structure and onboarding instructions.
