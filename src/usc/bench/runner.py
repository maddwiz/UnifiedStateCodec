from usc.bench.datasets import toy_agent_log, toy_big_agent_log, toy_big_agent_log_varied
from usc.bench.metrics import gzip_compress
from usc.mem.codec import mem_encode, mem_decode_with_fallback
from usc.mem.commit import commit_memory, load_last_commit
from usc.mem.chunking import chunk_by_lines

from usc.mem.dictpack import encode_chunks_with_table
from usc.mem.tokenpack import encode_chunks_with_tokentable
from usc.mem.deltapack import encode_chunks_with_line_deltas
from usc.mem.templatepack import encode_chunks_with_templates
from usc.mem.templatedelta import encode_chunks_with_template_deltas
from usc.mem.templaterle import encode_chunks_with_template_rle
from usc.mem.templatemtf import encode_chunks_with_template_mtf
from usc.mem.templatemtf_bits import encode_chunks_with_template_mtf_bits
from usc.mem.templatemtf_bits_deltaonly import encode_chunks_with_template_mtf_bits_deltaonly
from usc.mem.templatemtf_bits_vals import encode_chunks_with_template_mtf_bits_vals
from usc.mem.templatemtf_huff import encode_chunks_with_template_mtf_huff
from usc.mem.templatemtf_bits_tdelta import encode_chunks_with_template_mtf_bits_tdelta

from usc.mem.hybridpack import encode_chunks_hybrid
from usc.mem.metapack import encode_chunks_metapack


def _ratio(raw: int, comp: int) -> float:
    return raw / max(1, comp)


def _is_important_chunk(text: str) -> bool:
    markers = ["Decision:", "Note:"]
    return any(m in text for m in markers)


def _bench_big(name: str, raw_big: str):
    raw_big_bytes = raw_big.encode("utf-8")
    gz_big = gzip_compress(raw_big_bytes)

    chunks = [c.text for c in chunk_by_lines(raw_big, max_lines=10)]

    pkt0_total = 0
    pkt3_total = 0
    used_tier_counts = {0: 0, 3: 0}
    important_count = 0
    boring_count = 0

    for text in chunks:
        pkt0 = mem_encode(text, tier=0)
        pkt3 = mem_encode(text, tier=3)

        pkt0_total += len(pkt0)
        pkt3_total += len(pkt3)

        important = _is_important_chunk(text)
        if important:
            important_count += 1
            min_conf = 0.80
        else:
            boring_count += 1
            min_conf = 0.60

        _, conf, used_tier = mem_decode_with_fallback(
            packets_low_to_high=[pkt0, pkt3],
            min_conf=min_conf,
        )
        used_tier_counts[used_tier] += 1

    dictpack_bytes = encode_chunks_with_table(chunks)
    tokenpack_bytes = encode_chunks_with_tokentable(chunks)
    deltapack_bytes = encode_chunks_with_line_deltas(chunks)
    templatepack_bytes = encode_chunks_with_templates(chunks)
    tdelta_bytes = encode_chunks_with_template_deltas(chunks)
    trle_bytes = encode_chunks_with_template_rle(chunks)
    tmtf_bytes = encode_chunks_with_template_mtf(chunks)
    tmtfb_bytes = encode_chunks_with_template_mtf_bits(chunks)
    tmtdo_bytes = encode_chunks_with_template_mtf_bits_deltaonly(chunks)
    tmtfbv_bytes = encode_chunks_with_template_mtf_bits_vals(chunks)
    tmh_bytes = encode_chunks_with_template_mtf_huff(chunks)
    tmtfbd_bytes = encode_chunks_with_template_mtf_bits_tdelta(chunks)
    hybridpack_bytes = encode_chunks_hybrid(chunks)
    metapack_bytes = encode_chunks_metapack(chunks)

    print(f"USC Bench — BIG LOG ({name})")
    print("----------------------------------------")
    print(f"RAW bytes           : {len(raw_big_bytes)}")
    print(f"GZIP bytes          : {len(gz_big)}  (ratio {_ratio(len(raw_big_bytes), len(gz_big)):.2f}x)")
    print(f"Chunks              : {len(chunks)}")
    print(f"Important chunks    : {important_count}")
    print(f"Boring chunks       : {boring_count}")
    print(f"USC Tier0 total     : {pkt0_total}  (ratio {_ratio(len(raw_big_bytes), pkt0_total):.2f}x)")
    print(f"USC Tier3 total     : {pkt3_total}  (ratio {_ratio(len(raw_big_bytes), pkt3_total):.2f}x)")
    print(f"USC Auto-tier       : Tier0={used_tier_counts[0]}  Tier3={used_tier_counts[3]}")
    print(f"DICTPACK bytes      : {len(dictpack_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(dictpack_bytes)):.2f}x)")
    print(f"TOKENPACK bytes     : {len(tokenpack_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tokenpack_bytes)):.2f}x)")
    print(f"DELTAPACK bytes     : {len(deltapack_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(deltapack_bytes)):.2f}x)")
    print(f"TEMPLATEPACK bytes  : {len(templatepack_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(templatepack_bytes)):.2f}x)")
    print(f"TDELTA bytes        : {len(tdelta_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tdelta_bytes)):.2f}x)")
    print(f"TRLE bytes          : {len(trle_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(trle_bytes)):.2f}x)")
    print(f"TMTF bytes          : {len(tmtf_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tmtf_bytes)):.2f}x)")
    print(f"TMTFB bytes         : {len(tmtfb_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tmtfb_bytes)):.2f}x)")
    print(f"TMTFDO bytes        : {len(tmtdo_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tmtdo_bytes)):.2f}x)")
    print(f"TMTFBV bytes        : {len(tmtfbv_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tmtfbv_bytes)):.2f}x)")
    print(f"TMH bytes           : {len(tmh_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tmh_bytes)):.2f}x)")
    print(f"TMTFBD bytes        : {len(tmtfbd_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(tmtfbd_bytes)):.2f}x)")
    print(f"HYBRIDPACK bytes    : {len(hybridpack_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(hybridpack_bytes)):.2f}x)")
    print(f"METAPACK bytes      : {len(metapack_bytes)}  (ratio {_ratio(len(raw_big_bytes), len(metapack_bytes)):.2f}x)")
    print("----------------------------------------")
    print()


def run_toy_bench():
    store_path = "usc_commits.jsonl"

    raw_small = toy_agent_log()
    raw_small_bytes = raw_small.encode("utf-8")
    gz_small = gzip_compress(raw_small_bytes)

    pkt3_small = mem_encode(raw_small, tier=3)
    pkt0_small = mem_encode(raw_small, tier=0)

    decoded_best_small, conf_small, used_tier_small = mem_decode_with_fallback(
        packets_low_to_high=[pkt0_small, pkt3_small],
        min_conf=0.80,
    )

    rec = commit_memory(
        store_path=store_path,
        packet_version="mem-v0.7",
        used_tier=used_tier_small,
        confidence=conf_small,
        decoded_text=decoded_best_small,
    )

    last = load_last_commit(store_path)

    print("USC Bench — SMALL LOG (AUTO-TIER + COMMIT)")
    print("----------------------------------------")
    print(f"RAW bytes : {len(raw_small_bytes)}")
    print(f"GZIP bytes: {len(gz_small)}  (ratio {_ratio(len(raw_small_bytes), len(gz_small)):.2f}x)")
    print(f"TIER0 pkt : {len(pkt0_small)}  (ratio {_ratio(len(raw_small_bytes), len(pkt0_small)):.2f}x)")
    print(f"TIER3 pkt : {len(pkt3_small)}  (ratio {_ratio(len(raw_small_bytes), len(pkt3_small)):.2f}x)")
    print(f"AUTO used : tier {used_tier_small}  conf {conf_small:.2f}")
    print(f"Commit fp : {rec.text_fingerprint}")
    if last:
        print(f"Last tier : {last.used_tier}  conf {last.confidence:.2f}")
    print("----------------------------------------")
    print()

    raw_big_repeat = toy_big_agent_log(repeats=30)
    raw_big_varied = toy_big_agent_log_varied(loops=30)

    _bench_big("REPEAT-HEAVY (gzip showcase)", raw_big_repeat)
    _bench_big("VARIED (fair USC test)", raw_big_varied)
