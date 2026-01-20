# MASTER_HANDOFF — Unified State Codec (USC)

## What we’re building
USC is an AI-native codec designed for agent traces / tool logs / structured memory streams.

## Current flagship mode: USC-ODC
**USC-ODC** = USC packetization (v3b) + OuterStream framing + ZSTD trained-dictionary outer compression.

### Confirmed wins
- USC v3b stream beats gzip on real traces.
- OuterStream improves further by compressing cross-packet redundancy.
- OuterStream + trained dict hits ~7.18x on real trace (Bench20).

## Why this matters
General compressors don’t understand agent log structure.
USC extracts structure first (templates/ids/params) then a second stage crushes the byte motifs across packets.

## Next build steps
1) ODC API encode/decode (done now)
2) CLI wrapper (encode/decode commands)
3) Real dataset bench suite
4) Template mining upgrade (Drain-style)
5) Selective replay + indexing

## Ground rules
- Full-block pastes only (no line edits)
- Keep benchmarks reproducible
- Always update:
  - ROADMAP.md
  - FILEMAP.md
  - MASTER_HANDOFF.md
  - CHANGES.md
