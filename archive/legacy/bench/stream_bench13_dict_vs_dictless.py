from usc.bench.metrics import gzip_compress
from usc.mem.chunking import chunk_by_lines

from usc.bench.datasets_real_agent_trace import real_agent_trace

# Normal v3b (dict + stream data)
from usc.mem.stream_proto_canz_v3b import (
    StreamStateV3B,
    build_dict_state_from_chunks as build_v3b,
    encode_dict_packet as dict_v3b,
    apply_dict_packet as apply_v3b,
    encode_data_packet as data_v3b,
)

# Dictless per-packet v3bSC
from usc.mem.stream_proto_canz_v3b_selfcontained import (
    StreamStateV3BSC,
    encode_dict_packet as dict_sc,
    encode_data_packet as data_sc,
)

def _ratio(raw: int, comp: int) -> float:
    return raw / max(1, comp)

def _windows(items, win):
    for i in range(0, len(items), win):
        yield items[i:i+win]

def run():
    # Use the best discovered settings for "real traces"
    max_lines_per_chunk = 60
    window_chunks = 20

    # Compare across session sizes
    sessions = [40, 80, 150, 250, 400]

    print("USC Bench13 — v3b (DICT+stream) vs v3bSC (dictless/self-contained)")
    print("Settings:")
    print(f"  lines/chunk={max_lines_per_chunk}, chunks/packet={window_chunks}")
    print("------------------------------------------------------------")

    for loops in sessions:
        raw_big = real_agent_trace(loops=loops, seed=7)
        raw_bytes = raw_big.encode("utf-8")
        gz = gzip_compress(raw_bytes)

        chunks = [c.text for c in chunk_by_lines(raw_big, max_lines=max_lines_per_chunk)]

        # -------- normal v3b
        st_build = StreamStateV3B()
        build_v3b(chunks, state=st_build)
        pkt_dict = dict_v3b(st_build, level=10)

        st_send = StreamStateV3B()
        apply_v3b(pkt_dict, state=st_send)

        total_v3b = len(pkt_dict)
        for w in _windows(chunks, window_chunks):
            pkt = data_v3b(w, st_send, level=10)
            total_v3b += len(pkt)

        # -------- dictless v3bSC (no upfront dict; dict embedded per packet)
        st_sc = StreamStateV3BSC()
        pkt_sc_dict = dict_sc(st_sc, level=10)  # always empty
        total_sc = len(pkt_sc_dict)
        for w in _windows(chunks, window_chunks):
            pkt = data_sc(w, st_sc, level=10)
            total_sc += len(pkt)

        print(f"loops={loops:>3} | RAW={len(raw_bytes):>7} | GZIP={len(gz):>6} ({_ratio(len(raw_bytes), len(gz)):.2f}x)")
        print(f"          v3b : {total_v3b:>6}  ({_ratio(len(raw_bytes), total_v3b):.2f}x)   dict={len(pkt_dict)}")
        print(f"          v3bSC: {total_sc:>6}  ({_ratio(len(raw_bytes), total_sc):.2f}x)   dict=0 (embedded)")
        print("------------------------------------------------------------")

if __name__ == "__main__":
    run()
