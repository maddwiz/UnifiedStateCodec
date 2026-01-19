# USC — MASTER HANDOFF (v0.2)

## What this repo is
USC (Unified State Codec) is a compression system designed for AI memory logs and agent traces.
It prioritizes:
- Structured compression
- Future selective recall + partial decoding
- AI-native extensions beyond gzip

---

## Current state (as of this commit)
### ✅ Best compressor on VARIED benchmark
- Best custom packer: **TMTFDO_CAN**
  - Canonicalize input (lossy placeholders v0)
  - Template extraction
  - MTF ordering of template IDs
  - Bitpacked MTF positions
  - Delta-only values after first occurrence per template stream

### ✅ MetaPack upgraded
MetaPack now includes TMTFDO_CAN and auto-selects best method.

---

## Latest benchmark snapshot
From: `usc bench --toy`

### REPEAT-HEAVY
- GZIP bytes: 527
- DICTPACK bytes: 575
- METAPACK bytes: 577
- TMTFDO bytes: 665
- **TMTFDO_CAN bytes: 657**

### VARIED (fair USC test)
- GZIP bytes: 1495
- TMTFDO bytes: 1651
- **TMTFDO_CAN bytes: 1643**
- **METAPACK bytes: 1645**

---

## What matters
- Canonicalization is a confirmed “big jump” direction (beats baseline TMTFDO).
- Current canonicalization is **lossy** and must become **lossless** to be a real memory codec.

---

## Next task (immediate)
### ✅ Make canonicalization lossless
We will:
- Replace UUID/TS/HEX/INT with placeholders in the text stream
- Store original stripped values into a compact side-stream:
  - Per-type dictionary + delta-only encoding
- Reinflate perfectly on decode

This should improve compression (more repetition) without losing information.
