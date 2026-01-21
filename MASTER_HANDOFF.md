# MASTER HANDOFF — USC (Unified State Codec)

## What USC is
A domain-specific compression codec for structured logs and AI agent traces.
It uses template mining, typed parameter channels, and bitpacking to beat general compressors.

## Current Best Mode
- USC-cold: strongest compression (lossless)

## Key Outcomes Achieved
- Restored typed slot detection (INT/IP/HEX/DICT/RAW) + safe INT handling
- Fixed LogHub `<*>` wildcard extraction so params are preserved (lossless)
- Real LogHub baseline (200k lines) saved in results/bench_loghub_all.json
- USC-cold beats zstd-19 on BGL / Zookeeper / HDFS / Apache / Android

## Known Limits / TODO
- hot/hot-lite still labeled INDEX-ONLY until row-order H1M2 is fully wired for mixed unknown/event rows
- stream mode is slower, mainly for baseline comparison

## How to run benchmarks
USC_SUITE_LINES=200000 PYTHONPATH=src python3 scripts/bench_loghub_all.py

Output:
results/bench_loghub_all.json

## Next step
Implement H1M2 row-order mask into hot/hot-lite encoder so query modes are fully lossless.
