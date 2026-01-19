# USC — MASTER HANDOFF (v0.1)

## What this repo is
USC (Unified State Codec) is a compression system designed for AI memory logs and agent traces.
It prioritizes:
- Structured compression
- Future selective recall + partial decoding
- AI-native extensions beyond gzip

---

## Current state (as of this commit)
### ✅ New best method on VARIED benchmark
- Best custom packer: **TMTFDO**
  - Template extraction
  - MTF ordering of template IDs
  - Bitpacked MTF positions
  - Delta-only values after first occurrence (per template ID stream)

### ✅ MetaPack upgraded
MetaPack now includes TMTFDO and correctly selects the best method.

---

## Latest benchmark snapshot
From `usc bench --toy`

### REPEAT-HEAVY
- GZIP bytes: 527
- DICTPACK bytes: 575
- METAPACK bytes: 577
- TMTFDO bytes: 665

### VARIED (fair USC test)
- GZIP bytes: 1495
- TMTFB bytes: 1652
- **TMTFDO bytes: 1651**
- **METAPACK bytes: 1653**

---

## What matters
- We are now in the “micro-optimizations near ceiling” zone.
- Small byte improvements are expected until we introduce a new compression class:
  - canonicalization
  - persistent dictionaries
  - semantic/event object encoding
  - utility-based lossy compression

---

## Next task (immediate)
Add a canonicalization pass BEFORE templating:
- normalize whitespace
- timestamp normalization
- UUID shortening
- consistent key ordering for JSON-ish lines

Then re-run `usc bench --toy` to measure.

