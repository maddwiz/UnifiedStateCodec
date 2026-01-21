from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import re
import zstandard as zstd
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig


MAGIC_DICT = b"USC_D3D2"   # Dict packet v2 (zstd-compressed templates)
MAGIC_DATA = b"USC_D3A2"   # Data packet v2 (tid + params from regex)
MAGIC_PATCH = b"USC_D3P2"  # Patch packet v2 (prefix/suffix + middle bytes)


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


def _bstr_encode(b: bytes) -> bytes:
    return _uvarint_encode(len(b)) + b


def _bstr_decode(buf: bytes, off: int) -> Tuple[bytes, int]:
    n, off = _uvarint_decode(buf, off)
    if off + n > len(buf):
        raise ValueError("bstr decode overflow")
    return buf[off:off + n], off + n


# -----------------------------
# Drain3 mining
# -----------------------------
def _make_miner() -> TemplateMiner:
    cfg = TemplateMinerConfig()
    cfg.drain_depth = 4
    cfg.sim_th = 0.4
    cfg.max_children = 100
    cfg.max_clusters = 8000
    cfg.parametrize_numeric_tokens = True
    return TemplateMiner(config=cfg)


@dataclass
class D3Line:
    tid: int
    params: List[str]


def _extract_params_regex(template: str, line: str) -> List[str]:
    """
    Extract params by turning Drain3 template into a regex.
    This captures real payloads/JSON instead of token-splitting.
    """
    if "<*>" not in template:
        return []

    segs = template.split("<*>")
    # Match full line, capture between literal segments
    pattern = "^" + "(.*?)".join(re.escape(s) for s in segs) + "$"
    m = re.match(pattern, line, flags=re.DOTALL)
    if not m:
        return []
    return [g for g in m.groups()]


def _reconstruct_from_template(template: str, params: List[str]) -> str:
    """
    Reconstruct by interleaving template segments + params.
    Preserves punctuation/spaces exactly as template.
    """
    if "<*>" not in template:
        return template

    segs = template.split("<*>")
    out = [segs[0]]
    for i in range(len(segs) - 1):
        out.append(params[i] if i < len(params) else "<*>")
        out.append(segs[i + 1])
    return "".join(out)


def mine_lines(lines: List[str]) -> Tuple[List[str], List[D3Line]]:
    miner = _make_miner()

    cluster_tpl: Dict[int, str] = {}
    mined_raw: List[Tuple[int, List[str]]] = []

    for line in lines:
        line = line.rstrip("\n")
        res = miner.add_log_message(line)

        cid = int(res.get("cluster_id"))
        tpl = str(res.get("template_mined"))
        cluster_tpl[cid] = tpl

        params = _extract_params_regex(tpl, line)
        mined_raw.append((cid, params))

    # remap cluster ids -> 0..n-1
    cids = sorted(cluster_tpl.keys())
    cid_to_tid = {cid: i for i, cid in enumerate(cids)}

    templates = [cluster_tpl[cid] for cid in cids]

    mined: List[D3Line] = []
    for cid, params in mined_raw:
        mined.append(D3Line(tid=cid_to_tid[cid], params=params))

    return templates, mined


# -----------------------------
# Dict packet
# -----------------------------
def _encode_templates_uncompressed(templates: List[str]) -> bytes:
    out = bytearray()
    out += _uvarint_encode(len(templates))
    for tpl in templates:
        tb = tpl.encode("utf-8", errors="replace")
        out += _bstr_encode(tb)
    return bytes(out)


def _decode_templates_uncompressed(buf: bytes) -> List[str]:
    off = 0
    n, off = _uvarint_decode(buf, off)
    tpls: List[str] = []
    for _ in range(n):
        tb, off = _bstr_decode(buf, off)
        tpls.append(tb.decode("utf-8", errors="replace"))
    return tpls


def encode_dict_packet(templates: List[str], level: int = 10) -> bytes:
    raw = _encode_templates_uncompressed(templates)
    comp = zstd.ZstdCompressor(level=level).compress(raw)

    out = bytearray()
    out += MAGIC_DICT
    out += _uvarint_encode(len(raw))
    out += comp
    return bytes(out)


def decode_dict_packet(pkt: bytes) -> List[str]:
    if not pkt.startswith(MAGIC_DICT):
        raise ValueError("not a D3 dict packet v2")
    off = len(MAGIC_DICT)

    raw_len, off = _uvarint_decode(pkt, off)
    comp = pkt[off:]
    raw = zstd.ZstdDecompressor().decompress(comp, max_output_size=int(raw_len))
    return _decode_templates_uncompressed(raw)


# -----------------------------
# Data packet
# -----------------------------
def encode_data_packet(d3lines: List[D3Line]) -> bytes:
    out = bytearray()
    out += MAGIC_DATA
    out += _uvarint_encode(len(d3lines))

    for dl in d3lines:
        out += _uvarint_encode(int(dl.tid))
        out += _uvarint_encode(len(dl.params))
        for p in dl.params:
            pb = str(p).encode("utf-8", errors="replace")
            out += _bstr_encode(pb)

    return bytes(out)


def decode_data_packet(pkt: bytes) -> List[Tuple[int, List[str]]]:
    if not pkt.startswith(MAGIC_DATA):
        raise ValueError("not a D3 data packet v2")
    off = len(MAGIC_DATA)

    n, off = _uvarint_decode(pkt, off)
    out: List[Tuple[int, List[str]]] = []

    for _ in range(n):
        tid, off = _uvarint_decode(pkt, off)
        np, off = _uvarint_decode(pkt, off)
        params: List[str] = []
        for _ in range(np):
            pb, off = _bstr_decode(pkt, off)
            params.append(pb.decode("utf-8", errors="replace"))
        out.append((int(tid), params))

    return out


# -----------------------------
# Patch packet (prefix/suffix + middle)
# -----------------------------
def _lcp(a: str, b: str) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return i


def _lcs(a: str, b: str) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and a[-1 - i] == b[-1 - i]:
        i += 1
    return i


def encode_patch_packet(patches: List[Tuple[int, int, int, str]], level: int = 10) -> bytes:
    """
    Patch entry:
      (line_idx, pre_len, suf_len, middle_str)
    Rebuild:
      pred[:pre_len] + middle + pred[len(pred)-suf_len:]
    """
    raw = bytearray()
    raw += _uvarint_encode(len(patches))
    for idx, pre, suf, mid in patches:
        raw += _uvarint_encode(int(idx))
        raw += _uvarint_encode(int(pre))
        raw += _uvarint_encode(int(suf))
        raw += _bstr_encode(mid.encode("utf-8", errors="replace"))

    comp = zstd.ZstdCompressor(level=level).compress(bytes(raw))

    out = bytearray()
    out += MAGIC_PATCH
    out += _uvarint_encode(len(raw))
    out += comp
    return bytes(out)


def decode_patch_packet(pkt: bytes) -> List[Tuple[int, int, int, str]]:
    if not pkt.startswith(MAGIC_PATCH):
        raise ValueError("not a D3 patch packet v2")
    off = len(MAGIC_PATCH)

    raw_len, off = _uvarint_decode(pkt, off)
    comp = pkt[off:]
    raw = zstd.ZstdDecompressor().decompress(comp, max_output_size=int(raw_len))

    off2 = 0
    n, off2 = _uvarint_decode(raw, off2)

    patches: List[Tuple[int, int, int, str]] = []
    for _ in range(n):
        idx, off2 = _uvarint_decode(raw, off2)
        pre, off2 = _uvarint_decode(raw, off2)
        suf, off2 = _uvarint_decode(raw, off2)
        mb, off2 = _bstr_decode(raw, off2)
        patches.append((int(idx), int(pre), int(suf), mb.decode("utf-8", errors="replace")))

    return patches


# -----------------------------
# Public helpers (lossless packets)
# -----------------------------
def build_d3_packets_from_text(text: str, max_lines_per_packet: int = 60) -> List[bytes]:
    lines = text.splitlines()
    templates, mined = mine_lines(lines)

    packets: List[bytes] = []
    packets.append(encode_dict_packet(templates, level=10))

    # data packets
    for i in range(0, len(mined), max_lines_per_packet):
        packets.append(encode_data_packet(mined[i:i + max_lines_per_packet]))

    # predict before patch
    predicted = decode_d3_packets_to_lines(packets)

    patches: List[Tuple[int, int, int, str]] = []
    n = min(len(lines), len(predicted))

    for i in range(n):
        if lines[i] != predicted[i]:
            pre = _lcp(lines[i], predicted[i])
            suf = _lcs(lines[i], predicted[i])

            # prevent overlap
            max_suf = max(0, min(suf, len(lines[i]) - pre, len(predicted[i]) - pre))
            suf = max_suf

            mid = lines[i][pre:len(lines[i]) - suf] if suf > 0 else lines[i][pre:]
            patches.append((i, pre, suf, mid))

    if patches:
        packets.append(encode_patch_packet(patches, level=10))

    return packets


def decode_d3_packets_to_lines(packets: List[bytes]) -> List[str]:
    if not packets:
        return []

    templates = decode_dict_packet(packets[0])

    patch_entries: List[Tuple[int, int, int, str]] = []
    data_packets = packets[1:]

    # optional patch at end
    if data_packets and data_packets[-1].startswith(MAGIC_PATCH):
        patch_entries = decode_patch_packet(data_packets[-1])
        data_packets = data_packets[:-1]

    # decode prediction
    out_lines: List[str] = []
    for pkt in data_packets:
        rows = decode_data_packet(pkt)
        for tid, params in rows:
            tpl = templates[tid] if 0 <= tid < len(templates) else "<*>"
            out_lines.append(_reconstruct_from_template(tpl, params))

    # apply patches
    for idx, pre, suf, mid in patch_entries:
        if 0 <= idx < len(out_lines):
            pred = out_lines[idx]
            left = pred[:pre]
            right = pred[len(pred) - suf:] if suf > 0 else ""
            out_lines[idx] = left + mid + right

    return out_lines
