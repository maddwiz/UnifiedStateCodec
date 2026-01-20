# USC — Unified State Codec (USC-LSP)
A compression + memory protocol built for AI agent traces, logs, and structured recall.

## Mission
Build the best AI-native compression system by combining:
- **Structure extraction** (templates + values)
- **Streaming memory** (stateful, long-lived dictionaries)
- **Delta-only encoding** (changes over time)
- **Entropy backend** (zstd)  
So we can beat generic compressors *in the real AI setting*: long-running agent traces.

---

## Current Status (✅ Working Today)
### Best known results (toy benchmarks)
VARIED big log:
- RAW: 13,587 bytes
- GZIP: 1,495 bytes
- **CANZ batch @25 lines**: 1,545 bytes  (only +50 vs gzip)
- **DICT+DATA first run**: 1,537 bytes  (only +42 vs gzip)
- **DICT+DATA steady-state**: 189 bytes (!!)

REPEAT-heavy big log:
- GZIP: 527 bytes
- CANZ: 613 bytes
- META repack zstd: 601 bytes

---

## How USC Works (mental model)
USC converts text logs into:
1) **Templates** (the repeating structure)
2) **Values** (numbers / parameters)
Then stores:
- Template order efficiently (MTF + bitpacking)
- Values efficiently (delta-only)
- Then zstd compresses the packet

### USC-LSP v3 (stateful protocol)
We split the stream into 2 packet types:
- **DICT packet (warmup)**: templates + arity once
- **DATA packets (forever)**: only MTF positions + deltas

This is the big innovation: compression gets cheaper over time.

---

## Roadmap (Start → Finish)

### Phase 0 — Foundation ✅ (done)
- [x] Basic template extraction + value encoding
- [x] MTF ordering
- [x] Delta-only values
- [x] Canonicalization experiments
- [x] zstd integration
- [x] Bench suite (toy)

### Phase 1 — Streaming Protocol ✅ (done)
- [x] Stream windows
- [x] Stream tax reduction (remove per-chunk nvals using template arity)
- [x] **DICT + DATA split (USC-LSP v3)**

### Phase 2 — Beat GZIP on VARIED (next target)
Goal: FIRST RUN < gzip (1495) on varied logs

Upgrades:
- [ ] Shrink DICT packet:
  - template string tokenization (dict within dict)
  - normalized whitespace/punctuation
  - shared fragments across templates
- [ ] Optional: zstd dictionary training for DICT
- [ ] Reduce framing bytes (smaller headers, smaller counters)

### Phase 3 — Real Template Mining (high ROI)
- [ ] Integrate Drain3-style online template mining
- [ ] Compare against current extractor on varied datasets
- [ ] Add slot typing: int / float / hex / uuid / timestamp

### Phase 4 — Persistent Dictionaries (true long-lived memory)
- [ ] Global string dictionary (keys + values)
- [ ] Per-template slot dictionaries
- [ ] Cross-window value prediction (more than delta-only)

### Phase 5 — AI Features (USC becomes a product)
- [ ] Partial decode (query → decode only matching events)
- [ ] Selective recall (importance weights)
- [ ] Lossy modes (utility-based gisting)
- [ ] Index packet (fast search)

### Phase 6 — Packaging + Release
- [ ] Stable file format versioning
- [ ] CLI polish (`usc encode`, `usc decode`, `usc stream`)
- [ ] Public benchmark datasets + charts
- [ ] First “paper-style” writeup + repo README

---

## Success Criteria (what “world class” means)
USC wins if we can do one of these:
1) **Lossless**: beat gzip/zstd on real agent traces consistently  
2) **Practical**: match gzip but add unique value:
   - partial decode
   - streaming state
   - selective recall
   - memory indexing
3) **Compression at scale**: steady-state packets stay tiny as agents run for hours/days

---

## Operator Notes (rules for development)
- Always keep benchmarks updated.
- Always write full-file patches (no line edits).
- Each milestone update:
  - ROADMAP.md
  - FILEMAP.md
  - MASTER_HANDOFF.md
  - CHANGES.md

