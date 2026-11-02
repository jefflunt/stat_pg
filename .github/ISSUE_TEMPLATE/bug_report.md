---
name: Bug Report
about: Create a report to help us fix a bug or unexpected behavior in stat_pg
title: '[BUG]: '
labels: bug
assignees: ''
---

## Description
<!-- A clear and concise description of what the bug is. -->

## Steps to Reproduce
1. Start PostgreSQL with `stat_pg` loaded: `...`
2. Send metrics via TCP or execute SQL: `...`
3. Observe unexpected behavior or error: `...`

## Expected Behavior
<!-- A clear and concise description of what you expected to happen. -->

## Actual Behavior
<!-- A clear and concise description of what actually happened, including error messages or crashes. -->

## Environment & Configuration
- **OS / Distribution**: [e.g. Linux Ubuntu 22.04, macOS Sonoma]
- **PostgreSQL Version**: [e.g. 16.2]
- **stat_pg Version / Commit**: [e.g. main / abc1234]
- **Relevant `postgresql.conf` Settings**:
  ```ini
  # shared_preload_libraries, stat_pg.* settings
  ```

## Relevant Logs / Backtraces
<!-- Paste PostgreSQL server log output, backtraces, or psql output here. -->
```
```
