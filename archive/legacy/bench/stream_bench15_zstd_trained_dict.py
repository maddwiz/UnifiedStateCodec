from usc.bench.metrics import gzip_compress
from usc.bench.datasets_real_agent_trace import real_agent_trace

from usc.mem.zstd_trained_dict import (
    train_dict,
    compress_plain,
    compress_with_dict,
    decompress_plain,
    decompress_with_dict,
)

def _ratio(raw: int, comp: int) -> float:
    return raw / max(1, comp)

def _chunks(data: bytes, chunk_size: int = 4096):
    out = []
    for i in range(0, len(data), chunk_size):
        out.append(data[i:i+chunk_size])
    return out

def run():
    loops = 400
    raw = real_agent_trace(loops=loops, seed=7).encode("utf-8")

    gz = gzip_compress(raw)
    zstd_plain = compress_plain(raw, level=10)

    # Train dict on chunks of the SAME distribution
    samples = _chunks(raw, chunk_size=4096)[:256]  # 256 samples is plenty
    bundle = train_dict(samples, dict_size=8192)

    zstd_dict = compress_with_dict(raw, bundle, level=10)

    # sanity: roundtrip
    assert decompress_plain(zstd_plain) == raw
    assert decompress_with_dict(zstd_dict, bundle) == raw

    print("USC Bench15 — ZSTD trained dictionary vs baselines (REAL trace)")
    print("------------------------------------------------------------")
    print(f"RAW bytes        : {len(raw)}")
    print(f"GZIP bytes       : {len(gz):>7} (ratio {_ratio(len(raw), len(gz)):.2f}x)")
    print(f"ZSTD plain bytes : {len(zstd_plain):>7} (ratio {_ratio(len(raw), len(zstd_plain)):.2f}x)")
    print(f"ZSTD dict bytes  : {len(zstd_dict):>7} (ratio {_ratio(len(raw), len(zstd_dict)):.2f}x)")
    print("------------------------------------------------------------")
    print(f"Dict size bytes  : {len(bundle.dict_bytes)}")
    print("------------------------------------------------------------")

if __name__ == "__main__":
    run()
