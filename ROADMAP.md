# USC — Unified State Codec
## ROADMAP (v0.2)

USC is an AI-native compression stack focused on:
- Agent memory logs + tool outputs
- Structured recall (not just “smallest bytes”)
- Selective decode + future utility-based lossy modes

---

## ✅ Milestone 0 — Repo baseline (DONE)
- Working CLI: `usc bench --toy`
- Chunking + tiers (Tier0 / Tier3)
- Multiple packers + MetaPack auto-selection

---

## ✅ Milestone 1 — Template family compression (DONE)
### Completed packers
- TEMPLATEPACK
- TMTF (Template + MTF ordering)
- TMTFB (TMTF + bitpacked positions)
- TMTFDO (TMTFB + delta-only values after first appearance)

---

## ✅ Milestone 2 — Canonicalization Layer (DONE - v0)
### What we added
- Canonicalize logs BEFORE templating to increase repetition:
  - timestamps → `<TS>`
  - UUIDs → `<UUID>`
  - long hex → `<HEX>`
  - long ints → `<INT>`
  - whitespace normalization

### New best packer
- **TMTFDO_CAN** (TMTFDO + Canonicalization)

### Result highlights (`usc bench --toy`)
VARIED benchmark best custom packer:
- **TMTFDO_CAN = 1643 bytes**
MetaPack after upgrade:
- **METAPACK = 1645 bytes** (auto-selects best method)

⚠️ Note: Canonicalization v0 is **lossy** (placeholders replace original values).
Next milestone is to make canonicalization **lossless** by storing the stripped values
in a compact side-stream + dictionary.

---

## 🎯 Milestone 3 — Bigger leaps (NEXT)
Goal: stop “micro-wins” and consistently beat gzip/zstd on real agent traces.

Planned upgrades:
1) **Lossless canonicalization**
   - Store stripped UUID/TS/HEX/INT values in a side-stream
   - Dict + delta-only encoding per type stream
2) Slot typing per template (ints, enums, small strings)
3) Persistent dictionaries across runs (streaming)
4) Random access / partial decode blocks
5) Optional “utility lossy” modes (agent memory usefulness)

---

## 🚀 Milestone 4 — AI-native memory features (FUTURE)
- Utility-scored lossy compression modes (keep meaning, drop fluff)
- Retrieval-friendly memory objects (events/entities/decisions)
- Streaming codec mode
- KV-cache memory compression layer (separate module)

---

## 🧪 Milestone 5 — Real-world benchmarks (FUTURE)
Add datasets:
- Tool call logs
- Multi-agent planner traces
- JSON structured events
- Mixed text + JSON hybrid

Target: match or beat gzip/zstd on real agent datasets, while providing extra USC features gzip cannot.
