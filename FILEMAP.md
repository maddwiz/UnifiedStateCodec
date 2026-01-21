# USC File Map

## Core
- src/usc/
  - api/                      # codecs + format writers
  - mem/                      # template parse + PF1/PFQ logic
  - cli/ (if present)         # CLI entrypoints
  - __init__.py

## Bench + Tools
- scripts/bench_loghub_all.py # runs gzip/zstd/brotli + USC modes on LogHub
- scripts/mine_templates_like_hdfs.py # template mining helper
- scripts/debug_*.py          # debugging helpers (coverage, unknown rate)

## Data
- data/loghub/*.log           # LogHub logs (local datasets)

## Outputs
- results/bench_loghub_all.json  # canonical baseline results
- results/__tmp_*                # temporary bundles created during bench
