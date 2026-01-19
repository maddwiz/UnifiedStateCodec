# USC — FILEMAP (v0.2)

## Root
- ROADMAP.md
  - Project build plan + milestones
- FILEMAP.md
  - Repo structure map
- MASTER_HANDOFF.md
  - "Where we are right now" handoff for new chat windows
- CHANGES.md
  - Human-readable change log per milestone

---

## src/usc/bench/
- runner.py
  - Main toy benchmark runner for comparing packers
- datasets.py
  - Toy log generators (repeat-heavy + varied)
- metrics.py
  - gzip baseline helper and ratios

---

## src/usc/mem/
Core compression library packers + utilities.

### Preprocessing
- canonicalize.py
  - Canonicalization layer (v0 lossy placeholders)

### Pack utilities
- varint.py
  - Unsigned varint encode/decode used across packet formats

### Pack methods
- dictpack.py
  - Table + references (good on repeat-heavy)
- tokenpack.py
  - Token table approach
- deltapack.py
  - Delta compression for line changes
- templatepack.py
  - Template extraction + format slots
- templatedelta.py
  - Template-aware deltas
- templaterle.py
  - Run-length encoding for template ids
- templatemtf.py
  - Move-to-front encoding for template ids
- templatemtf_bits.py
  - TMTF + bitpacked MTF positions
- templatemtf_bits_deltaonly.py
  - TMTFB + delta-only values after first occurrence (TMTFDO)
- templatemtf_bits_deltaonly_canon.py
  - TMTFDO + canonicalization preprocessing (TMTFDO_CAN)
- templatemtf_huff.py
  - TemplateMTF + Huffman attempt (not currently winning)
- templatemtf_bits_vals.py
  - Value bitpacking attempt (not currently winning)
- templatemtf_bits_tdelta.py
  - Adaptive abs/delta attempt (not currently winning)
- hybridpack.py
  - Hybrid packer combining techniques
- metapack.py
  - Auto-selects best compressor among candidates

---

## src/usc/
- cli.py
  - CLI entry points
- (other modules)
  - USC tiered memory codec + decode fallback system
