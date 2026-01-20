# MASTER_HANDOFF — USC (Unified State Codec / USC-LSP)

If you open a new chat window: start here.

---

## Project Goal
Create an AI-native compression protocol that:
- matches or beats gzip/zstd on real agent traces
- becomes dramatically better over time (stateful streaming)
- supports partial recall + future memory features

---

## What Works Right Now (✅ Verified)
### Best batch codec
- **TMTFDO_CANZ** (canonical + zstd) is best known batch.
- Best chunking found by sweep = **25 lines per chunk**

### Major breakthrough: USC-LSP v3
We split streaming into 2 packet types:
- **DICT packet** (warmup once)
- **DATA packets** (forever)

Results on VARIED big log (toy):
- RAW: 13,587 bytes
- GZIP: 1,495 bytes
- CANZ batch @25: 1,545 bytes
- **DICT+DATA FIRST RUN: 1,537 bytes**
- **DICT+DATA STEADY: 189 bytes**

This is the novelty territory: long-lived compression.

---

## How to Run Benchmarks (copy/paste)
### Compile check
```bash
python -m py_compile src/usc/bench/runner.py
python -m pytest -q
-Main bench
usc bench --toy
-Chunk sweep
python -m usc.bench.sweep
-Streaming DICT+DATA bench
python -m usc.bench.stream_bench5

Current Best Settings
	•	VARIED bench: 25 lines per chunk
	•	Goal: beat gzip on FIRST RUN (currently only 42 bytes behind)

⸻

Biggest Next Upgrades (Do these next)

1) Shrink DICT packet size (highest ROI)

Goal: FIRST RUN < gzip (1495 bytes)

Ideas:
	•	tokenize templates inside DICT (dictionary-of-template-tokens)
	•	normalize whitespace/punctuation
	•	share common fragments across templates
	•	possibly zstd dict training for DICT

2) Better template mining (Drain3)

Replace toy template extraction with Drain3-style log parsing for real traces.

3) Persistent dictionaries

Global string dict + per-slot value dict + type-aware encoding (uuid/time/hex).

⸻

What To Never Forget
	•	Chunking has a sweet spot (10 too small, 25 best, larger gets worse due to template explosion)
	•	Streaming must use windows (micro-packets kill ratio)
	•	USC’s main power is: state persists, gzip can’t do that.

⸻

Milestones
	•	✅ USC-LSP v3 created: DICT+DATA protocol
	•	✅ CANZ + best chunk size discovered (25)
	•	✅ Bench suite built + reproducible results

