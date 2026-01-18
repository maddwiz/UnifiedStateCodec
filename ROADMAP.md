# USC Roadmap

USC = Unified State Codec
Compresses AI state across:
- Agent memory (logs / experiences)
- KV cache (inference memory)
- Weights (model storage)

Core principle:
Predict → store truth spine → store residual surprises → verify → commit.

---

## Phase 0 — Foundation (M0) ✅ DONE
Goal: repo exists, tests run, baseline benchmarks exist.

Delivered:
- Repo scaffold created
- CLI works: `usc bench --toy`
- Bench harness prints size metrics
- Basic tests wired with pytest

Exit criteria met:
- `pytest` passes
- `usc bench --toy` runs

---

## Phase 1 — USC-MEM (Agent Memory Codec)

### M1 — USC-MEM v0.7 ✅ DONE
Built a tiered, verified, self-healing memory codec.

Delivered:
- Tiering:
  - Tier 3 = lossless
  - Tier 0 = tiny utility memory
- Light ECC (truth spine verification)
- Fingerprint (behavior-id verification)
- Probes + confidence scoring
- Confidence gate (refuse silent hallucination)
- Auto-tier escalation (Tier 0 → Tier 3 if needed)
- Commit loop (writes known-good decode to `usc_commits.jsonl`)
- Tests: 5 passed

Exit criteria met:
- Tier 3 roundtrip exact
- Tier 0 compresses strongly
- Auto-tier successfully upgrades when confidence is low
- Commit loop writes and loads records

### M2 — USC-MEM v1.0 (Next)
Goal: real structure + real compression wins.

Planned upgrades:
- Chunking + rolling windows (long logs)
- Compact binary packet format (not JSON)
- Dictionary + entropy coding backend
- Better witnesses (entities/goals/timestamps)
- Multi-decode arbitration (two decoders, compare)
- Semantic ECC v2 (constraints across events)
- Probe suite expansion (utility-based)

---

## Phase 2 — USC-KV (KV Cache Codec)

### M3 — KV v0
- Layer budgets + anchor retention
- Basic importance scoring
- Measure memory reduction vs accuracy

### M4 — KV v1
- Concept-indexed KV
- Shared dictionaries across layers
- Tiered KV precision + fallback

### M5 — KV v2
- Cross-layer latent KV field
- Shared residual coder with USC-MEM

---

## Phase 3 — USC-W (Weights Codec)

### M6 — Weights v0
- Transform + predictor + residual coding
- Eval harness vs perplexity

### M7 — Weights v1
- Shared dictionaries + progressive decode
- Fingerprint + probe evaluation

### M8 — Weights v2
- Training-aware compression
- Utility-distortion tuning

---

## Phase 4 — Unified USC Platform

### M9 — One Codec System
- Shared residual coder backend across memory/KV/weights
- Unified probes + fingerprint framework
- Single CLI: `usc mem`, `usc kv`, `usc weights`

---

## Phase 5 — Publish + Integrate

### M10 — Release
- Benchmark suite
- Paper draft
- Integration hooks (vLLM / llama.cpp style)
