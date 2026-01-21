from usc.bench.datasets_real_agent_trace import real_agent_trace
from usc.bench.metrics import gzip_compress
from usc.mem.chunking import chunk_by_lines

from usc.mem.stream_proto_canz_v3b import (
    StreamStateV3B,
    build_dict_state_from_chunks as build_v3b,
    encode_dict_packet as dict_v3b,
    apply_dict_packet as apply_v3b,
    encode_data_packet as data_v3b,
)

from usc.mem.zstd_trained_dict import (
    train_dict,
    compress_plain,
    compress_with_dict,
    decompress_plain,
    decompress_with_dict,
)

def _ratio(raw: int, comp: int) -> float:
    return raw / max(1, comp)

def _windows(items, win):
    for i in range(0, len(items), win):
        yield items[i:i+win]

def _chunks(data: bytes, chunk_size: int = 4096):
    out = []
    for i in range(0, len(data), chunk_size):
        out.append(data[i:i+chunk_size])
    return out

def run():
    loops = 400
    max_lines_per_chunk = 60
    window_chunks = 20

    raw_text = real_agent_trace(loops=loops, seed=7)
    raw = raw_text.encode("utf-8")

    gz = gzip_compress(raw)

    # -------- USC v3b stream
    chunks = [c.text for c in chunk_by_lines(raw_text, max_lines=max_lines_per_chunk)]

    st_build = StreamStateV3B()
    build_v3b(chunks, state=st_build)
    pkt_dict = dict_v3b(st_build, level=10)

    st_send = StreamStateV3B()
    apply_v3b(pkt_dict, state=st_send)

    packets = [pkt_dict]
    for w in _windows(chunks, window_chunks):
        packets.append(data_v3b(w, st_send, level=10))

    usc_stream = b"".join(packets)

    # -------- Outer ZSTD (plain)
    z_plain = compress_plain(usc_stream, level=10)

    # Train dict on USC stream bytes (small corpus -> safe trainer will shrink dict)
    samples = _chunks(usc_stream, chunk_size=2048)  # smaller chunk size = more samples
    bundle = train_dict(samples, dict_size=8192)

    z_dict = compress_with_dict(usc_stream, bundle, level=10)

    # sanity: roundtrip
    assert decompress_plain(z_plain) == usc_stream
    assert decompress_with_dict(z_dict, bundle) == usc_stream

    print("USC Bench16 — USC(v3b) stream + outer zstd dict")
    print("------------------------------------------------------------")
    print(f"RAW bytes               : {len(raw)}")
    print(f"GZIP bytes              : {len(gz):>7} ({_ratio(len(raw), len(gz)):.2f}x)")
    print("------------------------------------------------------------")
    print(f"USC v3b total (packets) : {len(usc_stream):>7} ({_ratio(len(raw), len(usc_stream)):.2f}x)")
    print(f"USC stream + ZSTD plain : {len(z_plain):>7} ({_ratio(len(raw), len(z_plain)):.2f}x)")
    print(f"USC stream + ZSTD dict  : {len(z_dict):>7} ({_ratio(len(raw), len(z_dict)):.2f}x)")
    print("------------------------------------------------------------")
    print(f"Trained dict bytes      : {len(bundle.dict_bytes)}")
    print("------------------------------------------------------------")

if __name__ == "__main__":
    run()
