# 03 Deep Dives

This directory stores in-depth analyses, subsystem breakdowns, and progressive disclosure targets.

## Documents

* [starvation_prevention.md](file:///Users/jefflunt/code/stat_pg/agent_docs/03_deep_dives/starvation_prevention.md): Explains how decoupled threads, latch waiting, and two-stage UDP socket draining prevent loop starvation and OS buffer overflows.
* [network_ingestion_scaling.md](file:///Users/jefflunt/code/stat_pg/agent_docs/03_deep_dives/network_ingestion_scaling.md): Explains multi-process `SO_REUSEPORT` scaling, context-switch reduction via batch system calls, MTU packing, and `pprof` flamegraphs.
