# CHANGES — Unified State Codec (USC)

## 2026-01-21 — Milestone: HOT-LAZY shipped ✅
- Added HOT-LITE mode (PF1-only) for fast build + ~22x compression.
- Added BENCH scoreboard (gzip/zstd vs USC hot/cold).
- Added HOT-LAZY mode: encode PF1-only, then upgrade to PFQ1 fallback on-demand during query.
- Verified:
  - HOT-LITE: ~22.65x, FAST query ~86ms
  - COLD: ~55.64x on HDFS 200k lines
  - PFQ1 builds once (~11s) then persists in same .usch file
