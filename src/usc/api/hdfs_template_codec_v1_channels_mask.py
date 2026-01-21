import struct
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

try:
    import zstandard as zstd
except Exception:
    zstd = None


@dataclass
class EncodedTemplateChannelsV1M:
    raw_structured_bytes: int
    compressed_bytes: int
    event_count: int
    unknown_count: int
    channel_count: int


# -------------------------
# basic encoders
# -------------------------

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


def _zigzag_encode(n: int) -> int:
    return (n << 1) ^ (n >> 63)


def _svarint_encode(n: int) -> bytes:
    return _uvarint_encode(_zigzag_encode(int(n)))


def _encode_bytes(b: bytes) -> bytes:
    return _uvarint_encode(len(b)) + b


def _encode_str(s: str) -> bytes:
    b = s.encode("utf-8", errors="replace")
    return _encode_bytes(b)


# -------------------------
# type detection
# -------------------------

_INT_RE = re.compile(r"^[+-]?\d+$")
_HEX_RE = re.compile(r"^(0x)?[0-9a-fA-F]{8,}$")
_IP_RE  = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


def _is_int(s: str) -> bool:
    return bool(_INT_RE.match(s.strip()))


def _is_hex(s: str) -> bool:
    s = s.strip()
    return bool(_HEX_RE.match(s))


def _is_ip(s: str) -> bool:
    s = s.strip()
    if not _IP_RE.match(s):
        return False
    parts = s.split(".")
    try:
        return all(0 <= int(p) <= 255 for p in parts)
    except Exception:
        return False


def _hex_to_bytes(s: str) -> bytes:
    s = s.strip()
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]
    if len(s) % 2 == 1:
        s = "0" + s
    return bytes.fromhex(s)


# -------------------------
# bitmask helpers
# -------------------------

def _build_nonempty_mask(values: List[str]) -> Tuple[bytes, List[str]]:
    """
    Returns (mask_bytes, nonempty_values_in_order)
    mask bit = 1 if values[i] != ""
    """
    n = len(values)
    mask_len = (n + 7) // 8
    mask = bytearray(mask_len)
    nonempty = []
    for i, v in enumerate(values):
        if v != "":
            mask[i // 8] |= (1 << (i % 8))
            nonempty.append(v)
    return bytes(mask), nonempty


# -------------------------
# channel encoders (NONEMPTY ONLY)
# -------------------------

def _encode_int_nonempty(values: List[str]) -> bytes:
    ints = [int(v.strip()) for v in values]
    out = bytearray()
    out += _uvarint_encode(len(ints))
    if not ints:
        return bytes(out)
    out += _svarint_encode(ints[0])
    prev = ints[0]
    for x in ints[1:]:
        out += _svarint_encode(x - prev)
        prev = x
    return bytes(out)


def _encode_ip_nonempty(values: List[str]) -> bytes:
    out = bytearray()
    out += _uvarint_encode(len(values))
    for v in values:
        parts = [int(p) for p in v.strip().split(".")]
        out += bytes(parts)
    return bytes(out)


def _encode_hex_nonempty(values: List[str]) -> bytes:
    out = bytearray()
    out += _uvarint_encode(len(values))
    for v in values:
        b = _hex_to_bytes(v)
        out += _encode_bytes(b)
    return bytes(out)


def _encode_raw_nonempty(values: List[str]) -> bytes:
    out = bytearray()
    out += _uvarint_encode(len(values))
    for v in values:
        out += _encode_str(v)
    return bytes(out)


def _encode_dict_nonempty(values: List[str], max_dict: int = 4096) -> bytes:
    from collections import Counter

    counts = Counter(values)
    vocab = [s for (s, _) in counts.most_common(max_dict)]
    idx = {s: i for i, s in enumerate(vocab)}

    out = bytearray()
    out += _uvarint_encode(len(values))

    out += _uvarint_encode(len(vocab))
    for s in vocab:
        out += _encode_str(s)

    for v in values:
        out += _uvarint_encode(idx.get(v, 0))
    return bytes(out)


# type tags
# 0 raw, 1 int, 2 hex, 3 ip, 4 dict
def _choose_type(values_nonempty: List[str], dict_threshold: int = 12) -> int:
    if not values_nonempty:
        return 0

    sample = values_nonempty[:256]

    if all(_is_int(v) for v in sample):
        return 1
    if all(_is_ip(v) for v in sample):
        return 3
    if all(_is_hex(v) for v in sample):
        return 2

    uniq = len(set(sample))
    repeats_heavy = (len(sample) - uniq) >= dict_threshold
    if repeats_heavy:
        return 4

    return 0


def _encode_by_type(values_nonempty: List[str], t: int, max_dict: int = 4096) -> bytes:
    if t == 1:
        return _encode_int_nonempty(values_nonempty)
    if t == 2:
        return _encode_hex_nonempty(values_nonempty)
    if t == 3:
        return _encode_ip_nonempty(values_nonempty)
    if t == 4:
        return _encode_dict_nonempty(values_nonempty, max_dict=max_dict)
    return _encode_raw_nonempty(values_nonempty)


def encode_template_channels_v1_mask(
    events: List[Tuple[int, List[str]]],
    unknown_lines: List[str],
    dict_threshold: int = 12,
    max_dict: int = 4096,
) -> bytes:
    """
    V1-MASK: Same as V1 channels, but each channel stores:
      - bitmask of which rows have a value (non-empty)
      - then encodes ONLY those values (typed or dict/raw)

    Header:
      magic 'H1M1'
      version u32 (1)
      event_count uvarint
      unknown_count uvarint
      max_params uvarint

    Body:
      event_id stream (uvarint per row)
      For each channel:
         mask_len uvarint
         mask_bytes
         type uvarint
         payload bytes (len + payload of nonempty values)
      unknown lines (len+utf8)
    """
    out = bytearray()
    out += b"H1M1"
    out += struct.pack("<I", 1)

    n = len(events)
    out += _uvarint_encode(n)
    out += _uvarint_encode(len(unknown_lines))

    max_params = 0
    for _, p in events:
        if len(p) > max_params:
            max_params = len(p)
    out += _uvarint_encode(max_params)

    # event_ids stream
    for eid, _ in events:
        out += _uvarint_encode(int(eid))

    # build channels
    channels: List[List[str]] = [[] for _ in range(max_params)]
    for _, params in events:
        for i in range(max_params):
            channels[i].append(params[i] if i < len(params) else "")

    # encode each channel with mask + nonempty values
    for vals in channels:
        mask_bytes, nonempty_vals = _build_nonempty_mask(vals)
        out += _uvarint_encode(len(mask_bytes))
        out += mask_bytes

        t = _choose_type(nonempty_vals, dict_threshold=dict_threshold)
        payload = _encode_by_type(nonempty_vals, t, max_dict=max_dict)

        out += _uvarint_encode(t)
        out += _encode_bytes(payload)

    for ln in unknown_lines:
        out += _encode_str(ln)

    return bytes(out)


def compress_bytes(b: bytes, level: int = 10) -> bytes:
    if zstd is None:
        raise RuntimeError("zstandard not installed. Install with: pip install zstandard")
    return zstd.ZstdCompressor(level=level).compress(b)


def encode_and_compress_v1m(
    events: List[Tuple[int, List[str]]],
    unknown_lines: List[str],
    zstd_level: int = 10,
    dict_threshold: int = 12,
    max_dict: int = 4096,
):
    raw_struct = encode_template_channels_v1_mask(
        events,
        unknown_lines,
        dict_threshold=dict_threshold,
        max_dict=max_dict,
    )
    comp = compress_bytes(raw_struct, level=zstd_level)

    meta = EncodedTemplateChannelsV1M(
        raw_structured_bytes=len(raw_struct),
        compressed_bytes=len(comp),
        event_count=len(events),
        unknown_count=len(unknown_lines),
        channel_count=max((len(p) for _, p in events), default=0),
    )
    return comp, meta
