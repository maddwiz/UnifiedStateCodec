# USC — Unified State Codec

USC is a **domain-specific compression codec** for structured / semi-structured logs (including AI agent traces).
It targets two goals:

1) **Higher compression than gzip/zstd** on repetitive structured logs  
2) **Search/query on compressed data** (without full decompression)

This repo currently includes a full benchmark runner against **LogHub datasets** and multiple USC modes.

---

## Why USC is different

General-purpose compressors (gzip/zstd/brotli) treat logs as raw bytes.

USC learns structure by using:
- **Template mining / templated event IDs**
- **Typed slot channels** (int / ip / hex / dict / raw)
- **Bitpacking + delta coding**
- Optional **query packets** for fast searching

---

## Modes (current)

### ✅ `cold` (best compression, lossless)
Best for storage and offline processing.

### ✅ `stream` (simple baseline)
Lossless streaming encode. Slower + lower compression than `cold`.

### ⚠️ `hot-lite` and `hot`
Queryable/searchable packet formats.
These are currently marked **INDEX-ONLY** in the benchmark output until full row-order lossless reconstruction is finalized for all mixed logs.

> NOTE: On datasets with perfect template coverage (ex: HDFS), hot/hot-lite are effectively lossless already.
> Next milestone: make query modes fully lossless across mixed unknown/event rows (H1M2 row-mask).

---

## Quickstart

### 1) Setup
From repo root:

```bash

python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

2) Run the full LogHub suite (200k lines each)
USC_SUITE_LINES=200000 PYTHONPATH=src python3 scripts/bench_loghub_all.py
-Results are written to:
results/bench_loghub_all.json

Benchmarks (LogHub @ 200k lines)

USC currently beats zstd-19 on multiple real LogHub datasets:

Dataset	gzip	zstd-19	USC-cold
BGL	16.24×	18.62×	83.29×
Zookeeper	25.80×	38.80×	81.13×
HDFS	11.53×	16.81×	56.66×
Apache	21.33×	28.88×	35.74×
Android	7.39×	20.93×	21.42×

Exact results: results/bench_loghub_all.json

Repo Layout
	•	src/usc/ — USC core library + codecs
	•	scripts/bench_loghub_all.py — benchmark runner (LogHub suite)
	•	data/loghub/ — LogHub datasets (local)
	•	results/ — output artifacts (json + temp bundles)

⸻

Roadmap (next milestones)

Milestone A — Query modes become fully lossless (H1M2)
	•	Add row-order mask so unknown raw rows can interleave with template rows safely
	•	Promote hot-lite/hot from INDEX-ONLY → full compression modes

Milestone B — Competitor comparisons
	•	Add CLP / Logzip / LogShrink baselines where feasible
	•	Publish full suite results table

Milestone C — Packaging + adoption
	•	Clean CLI docs
	•	PyPI package
	•	Example integrations (agent memory store / log search backend)

⸻

License

TBD (choose before release)
