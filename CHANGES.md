# USC — CHANGES

## v0.6 — MetaPack + TMTFDO milestone
- Added TMTFDO: TemplateMTFBitPack + DeltaOnly value encoding
- Updated bench runner to print TMTFDO results
- Updated MetaPack to include and select TMTFDO
- Current best on VARIED dataset: TMTFDO = 1651 bytes
- MetaPack now achieves 1653 bytes on VARIED by selecting TMTFDO

## v0.5 — Position bitpacking milestone
- Added TMTFB: TemplateMTF + bitpacked MTF positions
- Improved VARIED from 1654 → 1652

## Experiments (non-winning, kept for research)
- TMH: Huffman attempt (overhead too high for small streams)
- TMTFBV: value bitpack attempt (header/bitwidth tax dominated)
- TMTFBD: adaptive abs/delta attempt (did not beat TMTFDO)

