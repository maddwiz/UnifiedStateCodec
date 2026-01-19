# USC — CHANGES

## v0.7 — Canonicalization + MetaPack upgrade
- Added `canonicalize.py` (lossy v0 placeholder normalization)
- Added `templatemtf_bits_deltaonly_canon.py` (TMTFDO_CAN)
- Bench results improved:
  - VARIED: TMTFDO 1651 → **TMTFDO_CAN 1643**
  - REPEAT: TMTFDO 665 → **TMTFDO_CAN 657**
- MetaPack upgraded to include canonicalized variant:
  - VARIED: METAPACK now **1645** (auto-selects best method)

## v0.6 — MetaPack + TMTFDO milestone
- Added TMTFDO: TemplateMTFBitPack + DeltaOnly value encoding
- Updated bench runner to print TMTFDO results
- Updated MetaPack to include and select TMTFDO
- Best on VARIED before canon: TMTFDO = 1651 bytes

## v0.5 — Position bitpacking milestone
- Added TMTFB: TemplateMTF + bitpacked MTF positions
- Improved VARIED from 1654 → 1652

## Experiments (non-winning, kept for research)
- TMH: Huffman attempt (overhead too high for small streams)
- TMTFBV: value bitpack attempt (header/bitwidth tax dominated)
- TMTFBD: adaptive abs/delta attempt (did not beat TMTFDO_CAN)
