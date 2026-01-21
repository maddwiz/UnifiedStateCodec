# FILEMAP — Unified State Codec (USC)

## src/usc/cli/
- app.py
  - encode: hot / hot-lite / hot-lazy / cold
  - query: FAST-first + optional upgrade to PFQ1
  - bench: gzip/zstd vs USC scoreboard

## src/usc/mem/
- hdfs_templates_v0.py
  - template bank loading + parsing
- tpl_pf1_recall_v1.py
  - PF1 builder (FAST recall index)
- tpl_pfq1_query_v1.py
  - PFQ1 builder (bloom token index)
- tpl_fast_query_v1.py
  - FAST query against PF1
- tpl_query_router_v1.py
  - router: FAST → PFQ1 fallback

## src/usc/api/
- hdfs_template_codec_v1m_bundle.py
  - cold bundle encoder (max ratio archive)

## docs/
- USC_CLI.md
- USC_HOT_LAZY.md
- USC_MODES.md (if present)
