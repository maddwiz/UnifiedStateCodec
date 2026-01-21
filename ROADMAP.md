# ROADMAP — Unified State Codec (USC)

## Goal
Build a domain-aware compression system for AI agent logs + structured traces that:
1) Beats general compressors (gzip/zstd) in ratio for structured data
2) Supports queryable compressed memory (FAST recall + fallback)
3) Supports archive mode (max compression)

---

## Phase 0 (Done ✅)
- PF1: template/event recall index
- PFQ1: token bloom index fallback search
- COLD: TPLv1M bundle archive mode (~55x on HDFS 200k)
- CLI:
  - encode: hot / hot-lite / hot-lazy / cold
  - query: FAST-first, optional upgrade to PFQ1
  - bench: scoreboard table vs gzip/zstd

---

## Phase 1 (Next)
- Prove PFQ1 catches queries FAST misses (value-only tokens)
- Add "upgrade cache" so PFQ1 build uses pre-parsed events (avoid reread/parse)
- Add real log datasets beyond HDFS:
  - Open-source agent traces
  - Tool-call logs (JSON)
- Add packaging:
  - pip installable CLI
  - versioned releases
  - minimal examples folder

---

## Phase 2 (Product-level)
- Persistent global dictionaries across files (stream compression)
- Drain3 template miner integration for unknown log formats
- Selective decode indexing (query returns offsets without full decode)
- Optional lossy memory modes (utility-based gisting)

---

## Phase 3 (Publish / traction)
- Public benchmarks: datasets, scripts, reproducible results
- Blog + arXiv-style writeup with plots and ablations
