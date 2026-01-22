# CHANGES

## 2026-01-21 — Milestone: hot-lite-full (lossless PF3 payload)
- Added `hot-lite-full` encode mode to write full PF3 payload blobs (restorable), while `hot-lite` remains index-only for fast query.
- Verified PF3(H1M2) roundtrip correctness at 200,000 lines.
- Updated real dataset manifest + refreshed LogHub benchmark JSON.
- Repo hygiene: improved .gitignore to keep artifacts out while tracking benchmark results.

Next:
- Run full LogHub suite at 200k with all USC modes.
- Publish README scoreboard + baseline comparisons.
- Add decode CLI support for hot-lite-full payloads (if not already integrated cleanly).
