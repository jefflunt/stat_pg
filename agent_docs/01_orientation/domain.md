# Domain Model

This document outlines the core data structures and table definitions used by `stat-pg`.

## Metric Resolutions

The aggregator computes and stores statistics at five distinct time resolutions:

| Table | Resolution | Key Formula (`at`) |
| :--- | :--- | :--- |
| `evt_secs` | Second | `at` (Epoch timestamp) |
| `evt_mins` | Minute | `at / 60` |
| `evt_hours` | Hour | `at / 3600` |
| `evt_days` | Day | `at / 86400` |
| `evt_30s` | 30-Day | `at / 2592000` |

## PostgreSQL Database Schema

The extension interacts directly with the following tables. If they do not exist in the database, the extension's SQL installation script creates them:

### `evt_secs`
```sql
CREATE TABLE evt_secs (
    code VARCHAR NOT NULL,
    at BIGINT NOT NULL,
    value DECIMAL(20, 12) NOT NULL,
    PRIMARY KEY (at, code)
);
```

### `evt_mins`
```sql
CREATE TABLE evt_mins (
    code VARCHAR NOT NULL,
    at BIGINT NOT NULL,
    value DECIMAL(20, 12) NOT NULL,
    PRIMARY KEY (at, code)
);
```

### `evt_hours`
```sql
CREATE TABLE evt_hours (
    code VARCHAR NOT NULL,
    at BIGINT NOT NULL,
    value DECIMAL(20, 12) NOT NULL,
    PRIMARY KEY (at, code)
);
```

### `evt_days`
```sql
CREATE TABLE evt_days (
    code VARCHAR NOT NULL,
    at BIGINT NOT NULL,
    value DECIMAL(20, 12) NOT NULL,
    PRIMARY KEY (at, code)
);
```

### `evt_30s`
```sql
CREATE TABLE evt_30s (
    code VARCHAR NOT NULL,
    at BIGINT NOT NULL,
    value DECIMAL(20, 12) NOT NULL,
    PRIMARY KEY (at, code)
);
```
