from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import zstandard as zstd


@dataclass
class ZstdDictBundle:
    dict_bytes: bytes
    cdict: zstd.ZstdCompressionDict
    ddict: zstd.ZstdCompressionDict


def _clean_samples(samples: List[bytes]) -> List[bytes]:
    out: List[bytes] = []
    for s in samples:
        if isinstance(s, (bytes, bytearray)) and len(s) > 0:
            out.append(bytes(s))
    return out


def _split_bytes(blob: bytes, chunk_size: int = 512) -> List[bytes]:
    if not blob:
        return []
    return [blob[i:i + chunk_size] for i in range(0, len(blob), chunk_size)]


def _candidate_sizes(target: int, total_src: int) -> List[int]:
    """
    Conservative dict sizing for small corpora.

    zstd dict training can fail if dict is too big relative to total src.
    We'll try a descending list of safe sizes.
    """
    if total_src <= 0:
        return []

    # keep dict <= 1/8 of source, and <= target
    cap = max(256, min(target, total_src // 8))

    # also try smaller ones if needed
    sizes = []
    s = cap
    while s >= 256:
        sizes.append(s)
        s //= 2

    # always ensure uniqueness
    uniq = []
    seen = set()
    for x in sizes:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


def train_dict(samples: List[bytes], dict_size: int = 8192) -> ZstdDictBundle:
    """
    Safe dictionary trainer:
      - Cleans samples
      - Tries multiple dict sizes
      - If training fails at all sizes -> raises RuntimeError
    """
    samples = _clean_samples(samples)
    if not samples:
        raise ValueError("train_dict: samples is empty")

    total_src = sum(len(s) for s in samples)
    sizes = _candidate_sizes(dict_size, total_src)
    if not sizes:
        raise RuntimeError("train_dict: not enough source bytes")

    last_err: Optional[Exception] = None
    for size in sizes:
        try:
            d = zstd.train_dictionary(size, samples)
            dict_bytes = d.as_bytes()
            cdict = zstd.ZstdCompressionDict(dict_bytes)
            ddict = zstd.ZstdCompressionDict(dict_bytes)
            return ZstdDictBundle(dict_bytes=dict_bytes, cdict=cdict, ddict=ddict)
        except Exception as e:
            last_err = e

    raise RuntimeError(f"train_dict failed: {last_err}")


def compress_plain(data: bytes, level: int = 10) -> bytes:
    cctx = zstd.ZstdCompressor(level=level)
    return cctx.compress(data)


def decompress_plain(data: bytes) -> bytes:
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(data)


def compress_with_dict(data: bytes, bundle: ZstdDictBundle, level: int = 10) -> bytes:
    cctx = zstd.ZstdCompressor(level=level, dict_data=bundle.cdict)
    return cctx.compress(data)


def decompress_with_dict(data: bytes, bundle: ZstdDictBundle) -> bytes:
    dctx = zstd.ZstdDecompressor(dict_data=bundle.ddict)
    return dctx.decompress(data)
