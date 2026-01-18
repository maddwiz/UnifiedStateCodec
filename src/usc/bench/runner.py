from usc.bench.datasets import toy_agent_log
from usc.bench.metrics import gzip_compress
from usc.mem.codec import mem_encode, mem_decode, mem_decode_with_fallback
from usc.mem.commit import commit_memory, load_last_commit


def run_toy_bench():
    raw = toy_agent_log()
    raw_bytes = raw.encode("utf-8")
    gz = gzip_compress(raw_bytes)

    # Make both packets:
    pkt_t3 = mem_encode(raw, tier=3)
    pkt_t0 = mem_encode(raw, tier=0)

    # Normal decode (shows confidence)
    decoded_t3, conf_t3 = mem_decode(pkt_t3, min_conf=0.60)
    decoded_t0, conf_t0 = mem_decode(pkt_t0, min_conf=0.60)

    # Auto-tier decode (self-healing)
    decoded_best, conf_best, used_tier = mem_decode_with_fallback(
        packets_low_to_high=[pkt_t0, pkt_t3],
        min_conf=0.80,  # force upgrade unless tier0 is truly confident
    )

    # COMMIT LOOP: save the known-good decode
    store_path = "usc_commits.jsonl"
    rec = commit_memory(
        store_path=store_path,
        packet_version="mem-v0.7",
        used_tier=used_tier,
        confidence=conf_best,
        decoded_text=decoded_best,
    )

    last = load_last_commit(store_path)

    print("USC Toy Bench — MEM v0.7 (AUTO-TIER + COMMIT LOOP)")
    print("----------------------------------------")
    print(f"RAW bytes : {len(raw_bytes)}")
    print(f"GZIP bytes: {len(gz)}  (ratio {len(raw_bytes)/max(1,len(gz)):.2f}x)")
    print("----------------------------------------")
    print("TIER 3 (LOSSLESS)")
    print(f"USC bytes : {len(pkt_t3)}  (ratio {len(raw_bytes)/max(1,len(pkt_t3)):.2f}x)")
    print(f"Roundtrip exact: {decoded_t3 == raw}")
    print(f"Confidence: {conf_t3:.2f}")
    print("----------------------------------------")
    print("TIER 0 (TINY, NOT LOSSLESS)")
    print(f"USC bytes : {len(pkt_t0)}  (ratio {len(raw_bytes)/max(1,len(pkt_t0)):.2f}x)")
    print(f"Roundtrip exact: {decoded_t0 == raw}")
    print(f"Confidence: {conf_t0:.2f}")
    print("----------------------------------------")
    print("AUTO-TIER (TRY 0 -> UPGRADE TO 3 IF NEEDED)")
    print(f"Used tier : {used_tier}")
    print(f"Confidence: {conf_best:.2f}")
    print(f"Exact     : {decoded_best == raw}")
    print("----------------------------------------")
    print("COMMIT LOOP")
    print(f"Commit file: {store_path}")
    print(f"Commit fp  : {rec.text_fingerprint}")
    if last:
        print(f"Last commit tier: {last.used_tier}  conf: {last.confidence:.2f}")
    print("----------------------------------------")
