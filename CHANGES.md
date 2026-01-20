# CHANGES — USC (Unified State Codec)

This is the running change log. Update after major milestones.

---

## 2026-01-19 — ✅ USC-LSP v3b beats gzip losslessly (FIRST RUN)
### What changed
- Added **DICT Shrink v3b**:
  - DICT stores templates only (tid implied by order)
  - removed stored arity (receiver infers robustly)
- Fixed lossless correctness by switching to:
  - **ints-only slot extraction** (prevents ASCII letter → int conversion)
- Added and verified roundtrip test

### Why it matters
USC now:
- is truly lossless on benchmark traces
- beats gzip on FIRST RUN (not just steady-state)
- remains stateful and streaming-friendly

### Results (toy VARIED big log)
- RAW: 13587
- GZIP: 1495  (ratio 9.09x)
- v3 FIRST: 1537
- ✅ v3b FIRST: 1420  (ratio 9.57x)  ✅ beats gzip by 75 bytes

---

## 2026-01-19 — USC-LSP v3: DICT + DATA protocol ✅
### What changed
- Added stateful streaming protocol split into:
  - DICT packet (templates once)
  - DATA packets (MTF positions + delta-only values forever)

### Results (toy VARIED big log)
- RAW: 13587
- GZIP: 1495
- CANZ batch: 1545
- DICT+DATA first: 1537
- STEADY: 189

---

## 2026-01-19 — Stream window v2 (remove nvals) ✅
### What changed
- Stored template arity once
- Removed per-chunk `nvals` varints

---

## 2026-01-19 — Bench sweep added ✅
### What changed
- Added chunk-size sweep bench

