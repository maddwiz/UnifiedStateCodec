# MASTER HANDOFF — USC (Unified State Codec)

## What USC is
USC compresses AI “state” across:
1) Agent memory (logs / experiences)
2) KV-cache (inference memory)
3) Weights (static model storage)

Core idea:
Predict → store truth spine → store residual surprises → verify → commit.
Decode loop:
Verify → (Repair / Upgrade Tier) → Commit (never silently fails).

---

## Current milestone
✅ M1 complete: USC-MEM v0.7 (auto-tier + commit loop)

---

## What works right now (CONFIRMED)
### USC-MEM v0.7:
- Tier 3 (lossless): roundtrip exact ✅
- Tier 0 (tiny): compresses strongly ✅
- Light ECC: truth spine verification ✅
- Fingerprint: behavior-id verification ✅
- Probes + confidence scoring ✅
- Confidence gate: refuses silent hallucination ✅
- Auto-tier escalation:
  - Try Tier 0 first
  - Upgrade to Tier 3 if confidence too low ✅
- Commit loop:
  - Writes committed decode to `usc_commits.jsonl` ✅
  - Can load last commit ✅

Tests:
- `pytest` passes (5 tests)

Bench:
- `usc bench --toy` prints sizes + confidence + used tier + commit info

---

## Known limitations (EXPECTED)
- Packets are still JSON+gzip (not optimal)
- Tier 3 doesn’t beat gzip on small logs yet
- Tier 0 is not lossless (by design)
- No chunking for long logs yet

---

## Next steps (5-year-old mode roadmap)
M2 — USC-MEM v1.0 upgrades:
1) Add chunking (long logs)
2) Replace JSON with compact binary packet format
3) Add dictionary coding backend (better compression)
4) Expand witnesses into structured fields (entities/goals/timestamps)
5) Add multi-decode arbitration (two decoders compare)
6) Add more probes (utility-based)
7) Add “commit upgrade” (once Tier 3 decoded, store smaller stabilized Tier 0+patch)

---

## Commands you should use
- Run tests:
  `python -m pytest -q`
- Run bench:
  `usc bench --toy`
- View commits:
  `tail -n 5 usc_commits.jsonl`
