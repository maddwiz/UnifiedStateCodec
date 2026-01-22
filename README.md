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


<!-- USC_SCOREBOARD_START -->

# USC Scoreboard (200k lines)

| Dataset | RAW | gzip | zstd-19 | CLP | USC-stream | USC-hot-lite | USC-hot-lite-full | USC-hot | USC-cold |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Android | — | 3.18 MB (7.39×) | 1.12 MB (20.93×) | — | 3.63 MB (6.47×) | 1.99 MB (11.78×) | 1.99 MB (11.78×) | 3.07 MB (7.65×) | 1.11 MB (21.15×) |
| Apache | — | 235.19 KB (21.33×) | 173.64 KB (28.88×) | — | 622.64 KB (8.06×) | 213.59 KB (23.48×) | 213.59 KB (23.48×) | 361.74 KB (13.87×) | 140.03 KB (35.82×) |
| BGL | — | 2.36 MB (10.84×) | 1.37 MB (18.62×) | — | 3.06 MB (8.36×) | 2.01 MB (12.73×) | 2.01 MB (12.73×) | 1.79 MB (14.31×) | 314.39 KB (83.30×) |
| HDFS | — | 2.32 MB (11.53×) | 1.59 MB (16.81×) | — | 3.09 MB (8.65×) | 2.06 MB (12.98×) | 2.06 MB (12.98×) | 2.39 MB (11.18×) | 482.79 KB (56.66×) |
| Zookeeper | — | 391.84 KB (25.80×) | 260.58 KB (38.80×) | — | 592.47 KB (17.06×) | 313.26 KB (32.27×) | 313.26 KB (32.27×) | 349.86 KB (28.90×) | 125.18 KB (80.76×) |

<!-- USC_SCOREBOARD_END -->






<!-- USC_COMPETITOR_START -->

# USC vs CLP — Competitor Report (200k lines)

This report compares USC to CLP on the same 5 LogHub datasets (200,000 lines each).

## Compression ratio (higher is better)

| Dataset | CLP | USC-hot-lite-full | USC-cold | Winner (Queryable) | Winner (Max) |
|---|---:|---:|---:|---|---|
| Android | 15.96× | 11.78× | 21.15× | CLP | USC |
| Apache | 15.33× | 23.48× | 35.82× | USC | USC |
| BGL | 9.12× | 12.73× | 83.30× | USC | USC |
| HDFS | 13.92× | 12.98× | 56.66× | CLP | USC |
| Zookeeper | 34.55× | 32.27× | 80.76× | CLP | USC |

## CLP search time (lower is better)

Average time over 4 queries: `ERROR`, `Exception`, `WARN`, `INFO`.

| Dataset | Avg CLP search time |
|---|---:|
| Android | 0.627s |
| Apache | 0.473s |
| BGL | 0.875s |
| HDFS | 0.642s |
| Zookeeper | 0.541s |

<!-- USC_COMPETITOR_END -->


