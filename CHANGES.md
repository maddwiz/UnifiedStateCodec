# USC Changes Log

## 2026-01-21
- Added LogHub 200k baseline results (results/bench_loghub_all.json)
- USC-cold dominates zstd-19 on multiple datasets:
  - BGL ~83×, Zookeeper ~81×, HDFS ~56×, Apache ~35×, Android ~21×
- Fixed LogHub template wildcard parsing `<*>` so params are preserved (lossless)
- Restored strong typed slot selection (INT/IP/HEX/DICT/RAW) with safe INT fallback
- README created with quickstart + benchmark table + roadmap
