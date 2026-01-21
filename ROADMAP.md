# USC Roadmap

## Current Status (Baseline)
- USC-cold beats zstd-19 on multiple LogHub datasets
- HOT/HOT-LITE provide query framing but currently labeled INDEX-ONLY in suite output

## Milestone 1 — Make HOT/HOT-LITE fully lossless (H1M2 Row Mask)
Goal: searchable + lossless + compressive across mixed unknown/event rows.

Tasks:
- Implement H1M2 row mask codec in encoder/decoder pipeline
- Add decode/roundtrip verifier to benchmark suite
- Remove INDEX-ONLY label once verified

## Milestone 2 — Full competitor bakeoff
Add baselines:
- CLP (y-scope)
- Logzip / LogShrink (if runnable)
- zstd w/ dictionary training

Deliverables:
- README table with ratios + encode time + query time

## Milestone 3 — Production polish
- Stable CLI
- PyPI package
- Better docs + examples
- Optional Rust hotpath rewrite

## Milestone 4 — Agent memory integration
- USC-backed memory store adapter for agent frameworks
- Query by entity/tool/error/time ranges
