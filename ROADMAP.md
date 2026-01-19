# USC — Unified State Codec
## ROADMAP (v0.1)

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
- TMTFDO (TMTFB + “delta-only values after first appearance”)

### Result highlights
VARIED benchmark best custom packer:
- TMTFDO = 1651 bytes
MetaPack after upgrade:
- METAPACK = 1653 bytes (chooses best method)

---

## 🎯 Milestone 2 — Bigger leaps (NEXT)
Goal: bigger-than-1-byte improvements.

Planned upgrades:
1) Canonicalization pass (normalize timestamps, UUIDs, whitespace)
2) Per-template slot typing (int ranges, small enums)
3) Residual dictionary for rare tokens (strings that don't template well)
4) Persistent template tables across runs (cross-log reuse)
5) Random access / partial decode blocks

---

## 🚀 Milestone 3 — “AI-native” memory features (FUTURE)
- Utility-scored lossy compression modes (keep meaning, drop fluff)
- Retrieval-friendly memory objects (events/entities/decisions)
- Streaming codec mode
- KV-cache memory compression layer (separate module)

---

## 🧪 Milestone 4 — Real-world benchmarks (FUTURE)
Add datasets:
- Tool call logs
- Multi-agent planner traces
- JSON structured events
- Mixed text + JSON hybrid

Target: match or beat gzip on at least one real agent dataset, while providing extra USC features gzip cannot.

