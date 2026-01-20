# CHANGES — USC (Unified State Codec)

This is the running change log. Update after major milestones.

---

## 2026-01-19 — USC-LSP v3: DICT + DATA protocol ✅
### What changed
- Added stateful streaming protocol split into:
  - DICT packet (templates + arity once)
  - DATA packet (MTF positions + delta-only values forever)
- Added stream bench suite and chunk sweep testing
- Added zstd backend integration

### Why it matters
This enables:
- long-lived compression that improves over time
- tiny steady-state packets (ideal for agent memory + recall)

### Results (toy VARIED big log)
- RAW: 13587
- GZIP: 1495
- CANZ batch: 1545
- DICT+DATA first: 1537
- DICT+DATA steady: 189

---

## 2026-01-19 — Stream window v2 (remove nvals) ✅
### What changed
- Stored template arity once
- Removed per-chunk `nvals` varints

### Impact
- Slight reduction in stream tax (example: ~10 bytes saved on toy test)

---

## 2026-01-19 — Bench sweep added ✅
### What changed
- Added chunk-size sweep bench: 10 / 25 / 50 / 100 / 200

### Findings
- 25 lines per chunk is best for VARIED dataset
- Too small = overhead dominates
- Too large = template explosion + value drift

