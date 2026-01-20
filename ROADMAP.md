# Unified State Codec (USC) — Roadmap

## Mission
Build an AI-native compression system that consistently beats general compressors on agent traces / tool logs,
and evolves into selective replay + long-context memory infrastructure.

## Current Status (as of Bench20)
We achieved a major milestone:
- USC packetization (v3b) beats gzip on real traces.
- OuterStream framing + ZSTD dict hits ~7.18x on real agent trace data (Bench20).
This stack is now called: **USC-ODC** (Outer Dictionary Codec).

## Milestones

### ✅ M0 — Baseline USC Packet Stream
- v3b stream packets (DICT + DATA packets)
- Chunking + windowing experiments
- Bench14–Bench18

### ✅ M1 — OuterStream Wrapper
- Frame packets as a single stream
- Compress as one blob (OuterStream)
- Bench19 proved cross-packet redundancy gains

### ✅ M2 — OuterStream + Trained Dictionary (ODC)
- OuterStream framed bytes + trained zstd dict
- Bench20 hit ~7.18x on real trace

### ✅ M3 — ODC Encode/Decode API (NOW)
- Implement `usc.api.codec_odc`
- Add bench21 roundtrip validation

### 🔜 M4 — CLI + “Drop-in SDK”
- `usc encode --mode odc`
- `usc decode --mode odc`
- Output stats + verify mode

### 🔜 M5 — Real Dataset Bench Suite
- Public log datasets
- Agent trace captures
- Compare: gzip / zstd / brotli / msgpack+c / dict zstd trained

### 🔜 M6 — Selective Replay + Indexing
- Packet-level index
- Decode only matching ranges
- Optional query keyword index / template index

### 🔜 M7 — Template Mining Upgrade (Drain-style)
- Stronger template extraction (Drain3-style)
- Persistent dictionaries
- Session-level compression wins

### 🔜 M8 — “Agent Memory Product Mode”
- Lossless mode (always exact)
- Utility mode (optional lossy gisting)
- Retrieval friendly storage format
