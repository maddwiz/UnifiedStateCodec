# ROADMAP — Unified State Codec (USC)

## Phase 0 — Proof of Reality ✅
- [x] Template mining + parameter extraction
- [x] Multi-mode compress (stream / hot-lite / hot / cold)
- [x] Real dataset suite (LogHub baseline)
- [x] PF3(H1M2) lossless rehydration verified (200k lines)
- [x] hot-lite-full mode to store restorable PF3 payload

## Phase 1 — Baseline + Publishing (NOW)
- [ ] Run full LogHub suite @ 200k lines on all datasets
- [ ] README scoreboard (sizes, ratios for gzip/zstd/USC modes)
- [ ] Add encode/decode timing + query timing
- [ ] Add “auto fallback”: if low repetition → use zstd; else USC modes

## Phase 2 — Query Productization
- [ ] Standard query interface: keyword search + template search
- [ ] Bloom/packet index profiling and tuning
- [ ] Partial decode (only packets matching query)
- [ ] CLI polish (`usc encode/query/decode/bench` stable)

## Phase 3 — Competitor Benchmarks
- [ ] CLP head-to-head (ratio + query latency)
- [ ] LogZip/LogShrink/others when feasible
- [ ] Publish reproducible scripts and results table

## Phase 4 — Packaging + Adoption
- [ ] PyPI package
- [ ] One-command quickstart demo
- [ ] “Agent memory store” adapter (LangGraph/LangChain compatible)

