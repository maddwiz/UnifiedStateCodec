from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import re
import zstandard as zstd
from datetime import datetime


MAGIC_SAS_DICT = b"USC_SASD1"  # dict packet v1
MAGIC_SAS_DATA = b"USC_SASA2"  # data packet v2 (fixed absolute timestamp base)


# -----------------------------
# Varint helpers
# -----------------------------
def _uvarint_encode(x: int) -> bytes:
    x = int(x)
    if x < 0:
        raise ValueError("uvarint cannot encode negative")
    out = bytearray()
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
            raise ValueError("uvarint decode overflow")
        b = buf[off]
        off += 1
        x |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return x, off
        shift += 7
        if shift > 63:
            raise ValueError("uvarint too large")


def _zigzag_encode(n: int) -> int:
    n = int(n)
    return (n << 1) ^ (n >> 63)


def _zigzag_decode(z: int) -> int:
    z = int(z)
    return (z >> 1) ^ (-(z & 1))


def _svarint_encode(n: int) -> bytes:
    return _uvarint_encode(_zigzag_encode(n))


def _svarint_decode(buf: bytes, off: int) -> Tuple[int, int]:
    z, off = _uvarint_decode(buf, off)
    return _zigzag_decode(z), off


def _bstr_encode(b: bytes) -> bytes:
    return _uvarint_encode(len(b)) + b


def _bstr_decode(buf: bytes, off: int) -> Tuple[bytes, int]:
    n, off = _uvarint_decode(buf, off)
    if off + n > len(buf):
        raise ValueError("bstr decode overflow")
    return buf[off:off + n], off + n


# -----------------------------
# SAS line parse
# -----------------------------
_HEAVY_KEYS = ("payload=", "args=", "json=", "data=")

_TS_RE = re.compile(r"^\[([^\]]+)\]\s*(.*)$")


def _parse_iso_to_us(ts: str) -> Optional[int]:
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    return int(round(dt.timestamp() * 1_000_000))


def _format_us_to_iso(us: int) -> str:
    sec = us / 1_000_000
    dt = datetime.utcfromtimestamp(sec)
    ms = int((us % 1_000_000) / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}+00:00"


def _split_timestamp(line: str) -> Tuple[Optional[str], str]:
    s = line.rstrip("\n")
    m = _TS_RE.match(s)
    if not m:
        return None, s
    return m.group(1), m.group(2)


def _parse_line_fields(line: str) -> Tuple[Optional[int], str, List[Tuple[str, str]]]:
    ts_str, rest = _split_timestamp(line)
    ts_us = _parse_iso_to_us(ts_str) if ts_str else None

    s = rest

    # heavy payload capture: keep it whole
    for hk in _HEAVY_KEYS:
        idx = s.find(hk)
        if idx != -1:
            head = s[:idx].rstrip()
            val = s[idx + len(hk):].strip()
            key = hk[:-1]
            pairs: List[Tuple[str, str]] = [(key, val)]
            return ts_us, head, pairs

    # general key=value capture
    it = list(re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)=", s))
    if not it:
        return ts_us, s, []

    head = s[:it[0].start()].rstrip()
    pairs: List[Tuple[str, str]] = []

    for i, m in enumerate(it):
        key = m.group(1)
        vstart = m.end()
        vend = it[i + 1].start() if i + 1 < len(it) else len(s)
        value = s[vstart:vend].strip()
        pairs.append((key, value))

    return ts_us, head, pairs


# -----------------------------
# Dict building
# -----------------------------
@dataclass
class SASDict:
    heads: List[str]
    keys: List[str]
    head_to_id: Dict[str, int]
    key_to_id: Dict[str, int]


def _build_dict(lines: List[str]) -> SASDict:
    heads_set = set()
    keys_set = set()

    for line in lines:
        _ts_us, head, pairs = _parse_line_fields(line)
        heads_set.add(head)
        for k, _v in pairs:
            keys_set.add(k)

    heads = sorted(heads_set)
    keys = sorted(keys_set)

    head_to_id = {h: i for i, h in enumerate(heads)}
    key_to_id = {k: i for i, k in enumerate(keys)}

    return SASDict(heads=heads, keys=keys, head_to_id=head_to_id, key_to_id=key_to_id)


def _encode_dict_packet(d: SASDict, level: int = 10) -> bytes:
    raw = bytearray()

    raw += _uvarint_encode(len(d.heads))
    for h in d.heads:
        raw += _bstr_encode(h.encode("utf-8", errors="replace"))

    raw += _uvarint_encode(len(d.keys))
    for k in d.keys:
        raw += _bstr_encode(k.encode("utf-8", errors="replace"))

    comp = zstd.ZstdCompressor(level=level).compress(bytes(raw))

    out = bytearray()
    out += MAGIC_SAS_DICT
    out += _uvarint_encode(len(raw))
    out += comp
    return bytes(out)


def _decode_dict_packet(pkt: bytes) -> SASDict:
    if not pkt.startswith(MAGIC_SAS_DICT):
        raise ValueError("not a SAS dict packet v1")
    off = len(MAGIC_SAS_DICT)

    raw_len, off = _uvarint_decode(pkt, off)
    comp = pkt[off:]
    raw = zstd.ZstdDecompressor().decompress(comp, max_output_size=int(raw_len))

    off2 = 0
    n_heads, off2 = _uvarint_decode(raw, off2)
    heads: List[str] = []
    for _ in range(n_heads):
        b, off2 = _bstr_decode(raw, off2)
        heads.append(b.decode("utf-8", errors="replace"))

    n_keys, off2 = _uvarint_decode(raw, off2)
    keys: List[str] = []
    for _ in range(n_keys):
        b, off2 = _bstr_decode(raw, off2)
        keys.append(b.decode("utf-8", errors="replace"))

    head_to_id = {h: i for i, h in enumerate(heads)}
    key_to_id = {k: i for i, k in enumerate(keys)}

    return SASDict(heads=heads, keys=keys, head_to_id=head_to_id, key_to_id=key_to_id)


# -----------------------------
# Data packets v2 (absolute first timestamp)
# -----------------------------
def _encode_data_packet(d: SASDict, lines: List[str]) -> bytes:
    """
    Data packet v2:
      MAGIC_SAS_DATA
      n_lines

      For each line:
        ts_flag uvarint (0/1)
        if ts_flag==1:
          if first_ts_in_packet: abs_ts_us uvarint
          else: dt_us svarint

        head_id uvarint
        n_pairs uvarint
        (key_id uvarint, value bstr)*
    """
    out = bytearray()
    out += MAGIC_SAS_DATA
    out += _uvarint_encode(len(lines))

    prev_ts: Optional[int] = None

    for line in lines:
        ts_us, head, pairs = _parse_line_fields(line)

        if ts_us is None:
            out += _uvarint_encode(0)
        else:
            out += _uvarint_encode(1)
            if prev_ts is None:
                # store absolute base once
                out += _uvarint_encode(int(ts_us))
            else:
                out += _svarint_encode(int(ts_us - prev_ts))
            prev_ts = ts_us

        hid = d.head_to_id.get(head, 0)
        out += _uvarint_encode(hid)

        out += _uvarint_encode(len(pairs))
        for k, v in pairs:
            kid = d.key_to_id.get(k, 0)
            out += _uvarint_encode(kid)
            out += _bstr_encode(v.encode("utf-8", errors="replace"))

    return bytes(out)


def _decode_data_packet(pkt: bytes) -> List[Tuple[Optional[int], int, List[Tuple[int, str]]]]:
    if not pkt.startswith(MAGIC_SAS_DATA):
        raise ValueError("not a SAS data packet v2")
    off = len(MAGIC_SAS_DATA)

    n, off = _uvarint_decode(pkt, off)
    rows: List[Tuple[Optional[int], int, List[Tuple[int, str]]]] = []

    prev_ts: Optional[int] = None

    for _ in range(n):
        ts_flag, off = _uvarint_decode(pkt, off)
        ts_us: Optional[int] = None

        if ts_flag == 0:
            ts_us = None
        else:
            if prev_ts is None:
                abs_ts, off = _uvarint_decode(pkt, off)
                ts_us = int(abs_ts)
                prev_ts = ts_us
            else:
                dt, off = _svarint_decode(pkt, off)
                ts_us = prev_ts + int(dt)
                prev_ts = ts_us

        hid, off = _uvarint_decode(pkt, off)
        np, off = _uvarint_decode(pkt, off)

        pairs: List[Tuple[int, str]] = []
        for _ in range(np):
            kid, off = _uvarint_decode(pkt, off)
            vb, off = _bstr_decode(pkt, off)
            pairs.append((int(kid), vb.decode("utf-8", errors="replace")))

        rows.append((ts_us, int(hid), pairs))

    return rows


def build_sas_packets_from_text(text: str, max_lines_per_packet: int = 60) -> List[bytes]:
    lines = text.splitlines()
    d = _build_dict(lines)

    packets: List[bytes] = []
    packets.append(_encode_dict_packet(d, level=10))

    for i in range(0, len(lines), max_lines_per_packet):
        chunk = lines[i:i + max_lines_per_packet]
        packets.append(_encode_data_packet(d, chunk))

    return packets


def decode_sas_packets_to_lines(packets: List[bytes]) -> List[str]:
    if not packets:
        return []

    d = _decode_dict_packet(packets[0])
    out_lines: List[str] = []

    for pkt in packets[1:]:
        rows = _decode_data_packet(pkt)
        for ts_us, hid, pairs in rows:
            head = d.heads[hid] if 0 <= hid < len(d.heads) else ""
            parts = [head] if head else []
            for kid, v in pairs:
                key = d.keys[kid] if 0 <= kid < len(d.keys) else "k"
                parts.append(f"{key}={v}")
            body = " ".join(parts).strip()

            if ts_us is None:
                out_lines.append(body)
            else:
                out_lines.append(f"[{_format_us_to_iso(ts_us)}] {body}".strip())

    return out_lines
