# MASTER_HANDOFF — USC (Unified State Codec / USC-LSP)

If you open a new chat window: start here.

---

## Project Goal
Create an AI-native compression protocol that:
- matches or beats gzip/zstd on real agent traces
- becomes better over time (stateful streaming)
- supports partial recall + future memory features

---

## ✅ Current Champion: USC-LSP v3b (lossless + beats gzip)
USC-LSP v3b is the best known version:
- lossless roundtrip verified ✅
- beats gzip on VARIED toy log on FIRST RUN ✅

### Bench7 (VARIED big log)
- RAW: 13587
- GZIP: 1495
- v3 FIRST: 1537
- ✅ v3b FIRST: 1420 (beats gzip by 75 bytes)

---

## How USC Works
USC turns text logs into:
1) Templates (structure)
2) Values (parameters)
Then stores:
- template ordering efficiently (MTF + bitpacking)
- values efficiently (delta-only)
- zstd compresses packets

### USC-LSP Protocol
Two packet types:
- **DICT packet**: templates once
- **DATA packets**: only MTF positions + deltas

v3b improves DICT by removing redundant fields:
- tid implied by position
- arity inferred on receiver
And ensures losslessness:
- ints-only slot extraction (prevents ASCII letter → int bug)

---

## How to Run (copy/paste)
### Main bench
```bash
usc bench --toy
-Streaming DICT+DATA comparison
python -m usc.bench.stream_bench7
-Lossless proof
python -m usc.bench.stream_roundtrip_test_v3b
Expected output:
	•	ROUNDTRIP OK: True

⸻

Best Settings
	•	Chunking: 25 lines per chunk
	•	Goal: continue improving VARIED, then move to real-world logs

⸻

Next Upgrades (highest ROI)
	1.	Typed slots (lossless):
	•	int / float / hex / uuid / timestamp / small strings
	2.	Drain3 template mining
	•	better templates on real logs (massive real-world win)
	3.	Persistent global dictionaries
	•	shared keys/values across long sessions

