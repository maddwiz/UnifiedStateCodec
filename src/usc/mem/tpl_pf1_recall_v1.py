import struct
from dataclasses import dataclass
from typing import Dict, List, Tuple

try:
    import zstandard as zstd
except Exception:
    zstd = None

from usc.api.hdfs_template_codec_v1_channels_mask import encode_template_channels_v1_mask


MAGIC = b"TPF1"
VERSION = 1


# ----------------------------
# uvarint helpers
# ----------------------------

def _uvarint_encode(x: int) -> bytes:
    out = bytearray()
    x = int(x)
    while True:
        b = x & 0x7F
        x >>= 7
        if x:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _uvarint_decode(buf: bytes, off: int) -> Tuple[int, int]:
    x = 0
    shift = 0
    while True:
        if off >= len(buf):
            raise ValueError("uvarint overflow")
        b = buf[off]
        off += 1
        x |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return x, off


def _bytes_encode(b: bytes) -> bytes:
    return _uvarint_encode(len(b)) + b


def _bytes_decode(buf: bytes, off: int) -> Tuple[bytes, int]:
    n, off = _uvarint_decode(buf, off)
    if off + n > len(buf):
        raise ValueError("bytes decode overflow")
    return buf[off:off+n], off+n


# ----------------------------
# template reconstruction
# ----------------------------

def render_template(template: str, params: List[str]) -> str:
    out = template
    for p in params:
        out = out.replace("[*]", p, 1)
    return out


def load_template_map_from_csv_text(csv_text: str) -> Dict[int, str]:
    import csv
    from io import StringIO
    reader = csv.DictReader(StringIO(csv_text))
    tmap: Dict[int, str] = {}
    for row in reader:
        eid_raw = (row.get("EventId") or "").strip()
        tpl = (row.get("EventTemplate") or "").strip()
        if not eid_raw or not tpl:
            continue
        eid = int(eid_raw[1:]) if eid_raw.startswith("E") else int(eid_raw)
        tmap[eid] = tpl
    return tmap


# ----------------------------
# decode H1M1 raw_struct (minimal)
# ----------------------------

def _zigzag_decode(u: int) -> int:
    return (u >> 1) ^ (-(u & 1))


def _decode_int_stream(buf: bytes, off: int) -> Tuple[List[int], int]:
    n, off = _uvarint_decode(buf, off)
    out: List[int] = []
    if n == 0:
        return out, off
    first_u, off = _uvarint_decode(buf, off)
    out.append(_zigzag_decode(first_u))
    prev = out[0]
    for _ in range(n - 1):
        du, off = _uvarint_decode(buf, off)
        prev = prev + _zigzag_decode(du)
        out.append(prev)
    return out, off


def _decode_str_stream(buf: bytes, off: int) -> Tuple[List[str], int]:
    n, off = _uvarint_decode(buf, off)
    out: List[str] = []
    for _ in range(n):
        b, off = _bytes_decode(buf, off)
        out.append(b.decode("utf-8", errors="replace"))
    return out, off


def _decode_dict_stream(buf: bytes, off: int) -> Tuple[List[str], int]:
    nvals, off = _uvarint_decode(buf, off)
    vocab_n, off = _uvarint_decode(buf, off)
    vocab: List[str] = []
    for _ in range(vocab_n):
        b, off = _bytes_decode(buf, off)
        vocab.append(b.decode("utf-8", errors="replace"))
    out: List[str] = []
    for _ in range(nvals):
        idx, off = _uvarint_decode(buf, off)
        if vocab:
            out.append(vocab[min(idx, len(vocab)-1)])
        else:
            out.append("")
    return out, off


def decode_h1m1_raw_struct(raw: bytes) -> Tuple[List[Tuple[int, List[str]]], List[str]]:
    if raw[:4] != b"H1M1":
        raise ValueError("bad H1M1 magic")
    off = 4
    _ver = struct.unpack("<I", raw[off:off+4])[0]
    off += 4

    n_events, off = _uvarint_decode(raw, off)
    n_unknown, off = _uvarint_decode(raw, off)
    max_params, off = _uvarint_decode(raw, off)

    eids: List[int] = []
    for _ in range(n_events):
        eid, off = _uvarint_decode(raw, off)
        eids.append(int(eid))

    params_mat: List[List[str]] = [[""] * max_params for _ in range(n_events)]

    for chan in range(max_params):
        mask_len, off = _uvarint_decode(raw, off)
        mask = raw[off:off+mask_len]
        off += mask_len

        ctype, off = _uvarint_decode(raw, off)
        payload, off = _bytes_decode(raw, off)

        poff = 0
        if ctype == 1:
            vals_i, _ = _decode_int_stream(payload, poff)
            vals = [str(x) for x in vals_i]
        elif ctype == 4:
            vals, _ = _decode_dict_stream(payload, poff)
        else:
            vals, _ = _decode_str_stream(payload, poff)

        vi = 0
        for row in range(n_events):
            if mask[row // 8] & (1 << (row % 8)):
                if vi < len(vals):
                    params_mat[row][chan] = vals[vi]
                vi += 1

    events = [(eids[i], params_mat[i]) for i in range(n_events)]

    unknown: List[str] = []
    for _ in range(n_unknown):
        b, off = _bytes_decode(raw, off)
        unknown.append(b.decode("utf-8", errors="replace"))

    return events, unknown


# ----------------------------
# PF1 v1 packet format
# ----------------------------

@dataclass
class PF1Meta:
    blob_bytes: int
    packet_count: int
    packet_events: int
    template_bytes: int


def _zstd_compress(b: bytes, level: int) -> bytes:
    if zstd is None:
        raise RuntimeError("zstandard missing")
    return zstd.ZstdCompressor(level=level).compress(b)


def _zstd_decompress(b: bytes) -> bytes:
    if zstd is None:
        raise RuntimeError("zstandard missing")
    return zstd.ZstdDecompressor().decompress(b)


def _encode_eidset(eids: List[int]) -> bytes:
    # delta-encoded sorted unique
    s = sorted(set(int(x) for x in eids))
    out = bytearray()
    out += _uvarint_encode(len(s))
    prev = 0
    for x in s:
        out += _uvarint_encode(x - prev)
        prev = x
    return bytes(out)


def _decode_eidset(buf: bytes) -> List[int]:
    off = 0
    n, off = _uvarint_decode(buf, off)
    out: List[int] = []
    prev = 0
    for _ in range(n):
        d, off = _uvarint_decode(buf, off)
        prev += int(d)
        out.append(prev)
    return out


def build_tpl_pf1_blob(
    events: List[Tuple[int, List[str]]],
    unknown_lines: List[str],
    template_csv_text: str,
    packet_events: int = 32768,
    zstd_level: int = 10,
) -> Tuple[bytes, PF1Meta]:
    """
    PF1 v1:
      - packetized chunks (bigger default)
      - exact EventID set per packet (no bloom false positives)

    Layout:
      MAGIC 'TPF1'
      u32 VERSION=1
      u32 zstd_level
      u32 packet_events
      u32 template_csv_len
      template_csv_bytes
      uvarint packet_count
      For each packet:
        u32 offset
        u32 length
        eidset_len (uvarint) + eidset_bytes
      packet_bytes...
    """
    tpl_bytes = template_csv_text.encode("utf-8", errors="replace")

    packets: List[bytes] = []
    eidsets: List[bytes] = []

    i = 0
    n = len(events)
    while i < n:
        chunk = events[i:i+packet_events]
        i += packet_events

        ul = unknown_lines if not packets else []
        raw_struct = encode_template_channels_v1_mask(chunk, ul)
        comp = _zstd_compress(raw_struct, level=zstd_level)
        packets.append(comp)

        eids = [eid for eid, _p in chunk]
        eidsets.append(_encode_eidset(eids))

    out = bytearray()
    out += MAGIC
    out += struct.pack("<I", VERSION)
    out += struct.pack("<I", int(zstd_level))
    out += struct.pack("<I", int(packet_events))
    out += struct.pack("<I", len(tpl_bytes))
    out += tpl_bytes
    out += _uvarint_encode(len(packets))

    table_start = len(out)
    for pkt, eidset in zip(packets, eidsets):
        out += struct.pack("<I", 0)         # offset placeholder
        out += struct.pack("<I", len(pkt))  # length
        out += _bytes_encode(eidset)        # eidset payload

    offsets: List[int] = []
    for pkt in packets:
        offsets.append(len(out))
        out += pkt

    # patch offsets
    off = table_start
    for po in offsets:
        out[off:off+4] = struct.pack("<I", po)
        off += 4
        _plen = struct.unpack("<I", out[off:off+4])[0]
        off += 4
        _eidset, off = _bytes_decode(out, off)

    meta = PF1Meta(
        blob_bytes=len(out),
        packet_count=len(packets),
        packet_events=packet_events,
        template_bytes=len(tpl_bytes),
    )
    return bytes(out), meta


def recall_event_id(blob: bytes, event_id: int, limit: int = 50) -> List[str]:
    if blob[:4] != MAGIC:
        raise ValueError("bad PF1 magic")

    off = 4
    ver = struct.unpack("<I", blob[off:off+4])[0]
    off += 4
    if ver != 1:
        raise ValueError("wrong PF1 version")

    _zlvl = struct.unpack("<I", blob[off:off+4])[0]
    off += 4
    _pkt_events = struct.unpack("<I", blob[off:off+4])[0]
    off += 4

    tpl_len = struct.unpack("<I", blob[off:off+4])[0]
    off += 4
    tpl_bytes = blob[off:off+tpl_len]
    off += tpl_len

    packet_count, off = _uvarint_decode(blob, off)

    tmap = load_template_map_from_csv_text(tpl_bytes.decode("utf-8", errors="replace"))

    table: List[Tuple[int, int, List[int]]] = []
    for _ in range(packet_count):
        pkt_off = struct.unpack("<I", blob[off:off+4])[0]
        off += 4
        pkt_len = struct.unpack("<I", blob[off:off+4])[0]
        off += 4
        eidset_bytes, off = _bytes_decode(blob, off)
        eids = _decode_eidset(eidset_bytes)
        table.append((pkt_off, pkt_len, eids))

    hits: List[str] = []
    eid = int(event_id)

    for pkt_off, pkt_len, eids in table:
        if eid not in eids:
            continue
        comp = blob[pkt_off:pkt_off+pkt_len]
        raw = _zstd_decompress(comp)
        events, _unknown = decode_h1m1_raw_struct(raw)

        for peid, params in events:
            if peid == eid:
                tpl = tmap.get(peid, "")
                if tpl:
                    hits.append(render_template(tpl, params))
                else:
                    hits.append(f"E{peid} " + " | ".join(params))
                if len(hits) >= limit:
                    return hits

    return hits
