from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import struct

from usc.mem.block_bloom_index_v0 import BlockBloomIndex


MAGIC = b"BBI0"  # Block Bloom Index v0


def _u16(x: int) -> bytes:
    return struct.pack("<H", x)


def _u32(x: int) -> bytes:
    return struct.pack("<I", x)


def _p_u16(b: bytes, off: int) -> tuple[int, int]:
    return struct.unpack_from("<H", b, off)[0], off + 2


def _p_u32(b: bytes, off: int) -> tuple[int, int]:
    return struct.unpack_from("<I", b, off)[0], off + 4


def append_block_bloom_footer(blob: bytes, bbi: BlockBloomIndex, block_count: int) -> bytes:
    """
    Appends a portable block-bloom table to the end of blob.

    Layout:
      payload:
        u16 m_bits
        u16 bloom_bytes
        u16 k_hashes
        u16 group_size
        u32 block_count
        [block_count * bloom_bytes] blooms (missing block => zeros)
      then:
        MAGIC (4 bytes)
        u32 payload_len

    Decode can ignore footer safely.
    """
    bloom_bytes = bbi.m_bits // 8

    payload = bytearray()
    payload += _u16(int(bbi.m_bits))
    payload += _u16(int(bloom_bytes))
    payload += _u16(int(bbi.k_hashes))
    payload += _u16(int(bbi.group_size))
    payload += _u32(int(block_count))

    # emit blooms in order 0..block_count-1
    for bid in range(block_count):
        bb = bbi.block_blooms.get(bid)
        if bb is None:
            payload += bytes(bloom_bytes)
        else:
            if len(bb) != bloom_bytes:
                raise ValueError("Bloom length mismatch")
            payload += bb

    out = blob + bytes(payload) + MAGIC + _u32(len(payload))
    return out


def read_block_bloom_footer(blob: bytes) -> Optional[BlockBloomIndex]:
    """
    If footer exists, returns BlockBloomIndex.
    Otherwise returns None.
    """
    if len(blob) < 8:
        return None

    magic = blob[-8:-4]
    if magic != MAGIC:
        return None

    payload_len = struct.unpack_from("<I", blob, len(blob) - 4)[0]
    start = len(blob) - 8 - payload_len
    if start < 0:
        return None

    payload = blob[start : start + payload_len]

    off = 0
    m_bits, off = _p_u16(payload, off)
    bloom_bytes, off = _p_u16(payload, off)
    k_hashes, off = _p_u16(payload, off)
    group_size, off = _p_u16(payload, off)
    block_count, off = _p_u32(payload, off)

    expected = 2 + 2 + 2 + 2 + 4 + (block_count * bloom_bytes)
    if len(payload) != expected:
        return None

    block_blooms: Dict[int, bytes] = {}
    for bid in range(block_count):
        bb = payload[off : off + bloom_bytes]
        off += bloom_bytes
        block_blooms[bid] = bytes(bb)

    return BlockBloomIndex(
        m_bits=int(m_bits),
        k_hashes=int(k_hashes),
        group_size=int(group_size),
        block_blooms=block_blooms,
    )
