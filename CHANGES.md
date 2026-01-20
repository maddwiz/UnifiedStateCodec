# CHANGES — Unified State Codec (USC)

## 2026-01-19 — Major ODC Milestone
- Bench19: OuterStream (USC packet stream + outer zstd) achieved ~7.00x vs gzip ~6.25x.
- Bench20: OuterStream framed + trained zstd dict achieved ~7.18x.
- Established flagship mode name: USC-ODC (Outer Dictionary Codec).
- Added/validated components:
  - outerstream framing module
  - trained dictionary support
  - ODC benchmark

## 2026-01-19 — ODC API + Roundtrip Safety
- Added `usc.api.codec_odc`:
  - encode packets -> ODC blob
  - decode ODC blob -> packets
- Added Bench21 roundtrip validation:
  - ensures packet byte-identical restoration
- Updated docs:
  - ROADMAP.md
  - FILEMAP.md
  - MASTER_HANDOFF.md
  - CHANGES.md
