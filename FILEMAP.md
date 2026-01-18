# USC File Map

## Root Files
- README.md: project overview + quickstart
- ROADMAP.md: milestones from start to finish
- FILEMAP.md: what each folder/file does
- MASTER_HANDOFF.md: current state + what works + what’s next
- CHANGES.md: milestone-by-milestone log
- usc_commits.jsonl: commit log created by bench runs (local file)

---

## src/usc/
Top-level Python package.

### src/usc/cli.py
Command line interface:
- `usc bench --toy`

### src/usc/common/
Shared utilities (future):
- types.py, hashing.py, logging.py (placeholders)

---

## src/usc/mem/  ✅ USC-MEM ACTIVE MODULE
Agent memory codec modules:

- skeleton.py
  Extracts minimal skeleton (header/goal)

- witnesses.py
  Extracts key truth pins (Decision/Note lines)

- residuals.py
  Stores remaining text (Tier 3 lossless)

- ecc.py
  Light ECC checksum over truth spine

- fingerprint.py
  Behavior-id fingerprint over truth spine

- probes.py
  Probe checks + confidence scoring

- codec.py
  Core encode/decode:
  - tiers (0,3)
  - ECC + fingerprint verification
  - probes + confidence gate
  - auto-tier escalation helper

- commit.py
  Commit loop storage:
  - writes CommitRecord to jsonl
  - reads last commit

---

## src/usc/kv/
KV-cache codec (planned):
- budgets.py, anchors.py, quant.py, codec.py, probes.py

## src/usc/weights/
Weights codec (planned):
- transforms.py, predictor.py, residuals.py, codec.py, benchmarks.py

---

## src/usc/bench/
Bench harness + toy data:
- datasets.py: toy logs
- metrics.py: gzip + size reporting
- runner.py: runs USC-MEM benchmarks (tiers + confidence + commit loop)

---

## tests/
Pytest suite:
- test_cli_smoke.py
- test_mem_roundtrip.py (includes commit loop + auto-tier behavior)

## tools/
Helper scripts (future)
