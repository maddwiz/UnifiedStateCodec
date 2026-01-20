# USC — Repo File Map

This file explains where everything lives.

---

## Root
- `ROADMAP.md` — project plan + phases
- `FILEMAP.md` — this map
- `MASTER_HANDOFF.md` — handoff summary (what works, how to run)
- `CHANGES.md` — milestone log + what changed each time

---

## Core Code
### `src/usc/mem/`
Compression modules (the heart of USC)

Key files:
- `templatepack.py`
  - `_extract_template(...)` template + values extractor used across codecs
- `templatemtf_bits_deltaonly_canon.py`
  - "CAN" codec (canonicalized)
- `templatemtf_bits_deltaonly_canon_zstd.py`
  - "CANZ" codec (CAN + zstd backend) ✅ best batch codec
- `zstd_codec.py`
  - zstd compress/decompress helper
- `stream_window_canz.py`
  - stream-window prototype (v1)
- `stream_window_canz_v2.py`
  - stream-window with arity stored once (removes nvals) ✅ improved stream window
- `stream_proto_canz_v3.py`
  - ✅ DICT + DATA protocol (USC-LSP v3) **major breakthrough**
- `canonicalize_lossless.py`
  - lossless canonicalization experiments
- `canonicalize_typed_lossless.py`
  - typed canonicalization experiments

---

## Benchmarks
### `src/usc/bench/`
Benchmark scripts used to test progress

Key files:
- `runner.py`
  - main CLI bench logic (`usc bench --toy`) ✅ default chunking set to 25
- `sweep.py`
  - chunk size sweep test (10 / 25 / 50 / 100 / 200)
- `stream_bench.py` / `stream_bench2.py` / `stream_bench3.py` / `stream_bench4.py`
  - streaming experiments and progression
- `stream_bench5.py`
  - ✅ DICT+DATA protocol benchmark (FIRST RUN vs STEADY)

---

## CLI (entry points)
Depends on current repo structure. The bench command used:
- `usc bench --toy`

---

## Naming
- `CAN`  = canonicalized batch encoding
- `CANZ` = CAN + zstd backend
- `USC-LSP v3` = DICT+DATA streaming protocol

