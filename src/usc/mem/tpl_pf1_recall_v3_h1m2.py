from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
import struct

from usc.api.hdfs_template_codec_h1m2_rowmask import encode_h1m2_rowmask_blob

try:
    import zstandard as zstd
except Exception:
    zstd = None

MAGIC = b"TPF3"
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


def _bytes_encode(b: bytes) -> bytes:
    return _uvarint_encode(len(b)) + b


def _zstd_compress(buf: bytes, level: int = 10) -> bytes:
    if zstd is None:
        raise RuntimeError("zstandard missing (pip install zstandard)")
    cctx = zstd.ZstdCompressor(level=level)
    return cctx.compress(buf)


def _encode_eidset(eids: List[int]) -> bytes:
    # delta varint
    out = bytearray()
    out += _uvarint_encode(len(eids))
    prev = 0
    for x in eids:
        d = int(x) - prev
        prev = int(x)
        out += _uvarint_encode(d)
    return bytes(out)


@dataclass
class PF3Meta:
    packet_events: int
    zstd_level: int
    templates_csv: str
    row_count: int
    unknown_count: int


def build_tpl_pf3_blob_h1m2(
    rows: List[Optional[Tuple[int, List[str]]]],
    unknown_lines: List[str],
    template_csv_text: str,
    packet_events: int = 32768,
    zstd_level: int = 10,
) -> Tuple[bytes, PF3Meta]:
    """
    Layout:
      MAGIC 'TPF3'
      u32 VERSION
      u32 zstd_level
      u32 packet_events
      u32 template_csv_len
      template_csv_bytes
      uvarint row_count
      uvarint unknown_count
      uvarint packet_count
      For each packet:
        u32 offset
        u32 length
        eidset_bytes (len+payload)
      packet_bytes...

    Packet 0 raw_struct = H1M2 blob (contains rowmask + unknown_lines + first event chunk)
    Packets 1..N raw_struct = H1M2 blob with EMPTY unknown_lines but includes event chunk.
    """
    tpl_bytes = template_csv_text.encode("utf-8", errors="replace")

    # Extract events in order (rows preserve event order already)
    events: List[Tuple[int, List[str]]] = [r for r in rows if r is not None]

    packets: List[bytes] = []
    eidsets: List[bytes] = []

    i = 0
    n = len(events)
    pkt_idx = 0

    while i < n:
        chunk = events[i:i + packet_events]
        i += packet_events

        # only packet0 carries rowmask + unknown lines for full reconstruction
        ul = unknown_lines if pkt_idx == 0 else []
        # Build a “rows view” for the wrapper:
        # For packet0 we include the REAL rows list so rowmask is present.
        # For later packets we can pass a fake rows list sized 0 (rowmask omitted).
        if pkt_idx == 0:
            # rows are whole-file rows
            raw_struct = encode_h1m2_rowmask_blob(rows, ul)
        else:
            # we encode just the chunk as event-only rows (no unknowns)
            fake_rows: List[Optional[Tuple[int, List[str]]]] = [(eid, p) for (eid, p) in chunk]
            raw_struct = encode_h1m2_rowmask_blob(fake_rows, ul)

        comp = _zstd_compress(raw_struct, level=zstd_level)
        packets.append(comp)

        eids = [eid for eid, _p in chunk]
        eidsets.append(_encode_eidset(eids))

        pkt_idx += 1

    out = bytearray()
    out += MAGIC
    out += struct.pack("<I", VERSION)
    out += struct.pack("<I", int(zstd_level))
    out += struct.pack("<I", int(packet_events))
    out += struct.pack("<I", len(tpl_bytes))
    out += tpl_bytes

    out += _uvarint_encode(len(rows))
    out += _uvarint_encode(len(unknown_lines))

    out += _uvarint_encode(len(packets))

    # table
    for pkt, eidset in zip(packets, eidsets):
        out += struct.pack("<I", 0)
        out += struct.pack("<I", len(pkt))
        out += _bytes_encode(eidset)

    # packets
    offsets: List[int] = []
    for pkt in packets:
        offsets.append(len(out))
        out += pkt

    # fill offsets
    table_start = (
        len(MAGIC)
        + 4 + 4 + 4
        + 4 + len(tpl_bytes)
        + len(_uvarint_encode(len(rows)))
        + len(_uvarint_encode(len(unknown_lines)))
        + len(_uvarint_encode(len(packets)))
    )

    off = table_start
    for o in offsets:
        out[off:off+4] = struct.pack("<I", int(o))
        off += 4 + 4
        # skip eidset (uvarint len + bytes)
        # decode length quickly:
        # (since it's in our own output, we can walk it by reading uvarint)
        # but simplest: recompute by encoding same eidset again
        eidset = eidsets[(off - (table_start + 8)) // 0]  # never used; keep simple
        # We can't safely compute without parsing, so just walk uvarint now:
        # parse uvarint length from out starting at 'off'
        j = off
        shift = 0
        val = 0
        while True:
            b = out[j]
            j += 1
            val |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        off = j + val

    meta = PF3Meta(
        packet_events=packet_events,
        zstd_level=zstd_level,
        templates_csv=template_csv_text,
        row_count=len(rows),
        unknown_count=len(unknown_lines),
    )
    return bytes(out), meta
