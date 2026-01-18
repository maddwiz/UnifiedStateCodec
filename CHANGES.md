# USC Changes Log

## Milestone M0 — Repo scaffold created ✅
- Added ROADMAP.md
- Added FILEMAP.md
- Added MASTER_HANDOFF.md
- Added CHANGES.md
- Added Python package scaffold under src/usc/
- Added CLI entry: `usc bench --toy`
- Added basic pytest wiring

---

## Milestone M1 — USC-MEM v0.7 (Verified Tiering + Auto-Tier + Commit Loop) ✅
Added a tiered memory codec with verification, confidence gating, self-healing escalation, and commit storage.

### Added / Updated
- src/usc/mem/skeleton.py
- src/usc/mem/witnesses.py
- src/usc/mem/residuals.py
- src/usc/mem/ecc.py (Light ECC checksum)
- src/usc/mem/fingerprint.py
- src/usc/mem/probes.py (probe checks + confidence scoring)
- src/usc/mem/codec.py
  - tiers (0,3)
  - ECC + fingerprint verification
  - probes + confidence gate
  - auto-tier escalation helper
- src/usc/mem/commit.py
  - commit_memory() writes jsonl record
  - load_last_commit() reads last record
- src/usc/bench/runner.py
  - prints tier sizes + confidence + auto-tier results
  - writes commit records to usc_commits.jsonl
- tests/test_mem_roundtrip.py
  - lossless roundtrip test
  - tier0 decode validity
  - auto-tier escalation behavior
  - commit store read/write

### Result
- `pytest` passes (5 tests)
- `usc bench --toy` runs with confidence + auto-tier + commit loop
