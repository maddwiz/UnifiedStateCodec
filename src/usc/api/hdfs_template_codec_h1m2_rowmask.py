from __future__ import annotations

from typing import List, Optional, Tuple
import struct

from usc.api.hdfs_template_codec_v1_channels_mask import (
    encode_template_channels_v1_mask,
)

MAGIC = b"H1M2"
VERSION = 1


def _uvarint_encode(x: int) -> bytes:
    out = bytearray()
    n = int(x)
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _pack_rowmask_is_event(rows: List[Optional[Tuple[int, List[str]]]]) -> bytes:
    """
    bit=1 => row is EVENT
    bit=0 => row is UNKNOWN
    """
    n = len(rows)
    nbytes = (n + 7) // 8
    buf = bytearray(nbytes)
    for i, r in enumerate(rows):
        if r is not None:
            buf[i >> 3] |= (1 << (i & 7))
    return bytes(buf)


def extract_events_from_rows(rows: List[Optional[Tuple[int, List[str]]]]) -> List[Tuple[int, List[str]]]:
    return [r for r in rows if r is not None]


def encode_h1m2_rowmask_blob(
    rows: List[Optional[Tuple[int, List[str]]]],
    unknown_lines: List[str],
) -> bytes:
    """
    H1M2 wrapper:
      MAGIC 'H1M2'
      u32 VERSION
      uvarint row_count
      uvarint rowmask_len
      rowmask_bytes
      inner_payload = encode_template_channels_v1_mask(events, unknown_lines)
      uvarint inner_len
      inner_bytes
    """
    rowmask = _pack_rowmask_is_event(rows)
    events = extract_events_from_rows(rows)

    inner = encode_template_channels_v1_mask(events, unknown_lines)

    out = bytearray()
    out += MAGIC
    out += struct.pack("<I", VERSION)
    out += _uvarint_encode(len(rows))
    out += _uvarint_encode(len(rowmask))
    out += rowmask
    out += _uvarint_encode(len(inner))
    out += inner
    return bytes(out)
