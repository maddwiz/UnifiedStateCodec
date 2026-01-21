from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

from usc.mem.varint import encode_uvarint, decode_uvarint
from usc.mem.zstd_codec import zstd_compress, zstd_decompress

# Drain3 (encoder-side only)
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

from usc.mem.template_miner_drain3 import _extract_params_from_template


MAGIC_DICT = b"USDICT3D"   # templates only
MAGIC_DATA = b"USDAT3D6"   # template refresh + string refresh + MTF bitpacks (persistent miner)


def _pack_string(s: str) -> bytes:
    b = s.encode("utf-8")
    return encode_uvarint(len(b)) + b


def _unpack_string(data: bytes, offset: int) -> Tuple[str, int]:
    n, off = decode_uvarint(data, offset)
    b = data[off:off + n]
    off += n
    return b.decode("utf-8"), off


def _bitpack(values: List[int], bits: int) -> bytes:
    if bits <= 0:
        return b""
    out = bytearray()
    acc = 0
    acc_bits = 0
    mask = (1 << bits) - 1

    for v in values:
        v &= mask
        acc |= (v << acc_bits)
        acc_bits += bits
        while acc_bits >= 8:
            out.append(acc & 0xFF)
            acc >>= 8
            acc_bits -= 8

    if acc_bits > 0:
        out.append(acc & 0xFF)

    return bytes(out)


def _bitunpack(data: bytes, count: int, bits: int) -> List[int]:
    if bits <= 0:
        return [0] * count

    out: List[int] = []
    acc = 0
    acc_bits = 0
    idx = 0
    mask = (1 << bits) - 1

    for _ in range(count):
        while acc_bits < bits:
            if idx >= len(data):
                raise ValueError("bitunpack ran out of bytes")
            acc |= data[idx] << acc_bits
            idx += 1
            acc_bits += 8

        out.append(acc & mask)
        acc >>= bits
        acc_bits -= bits

    return out


def _bits_needed(n: int) -> int:
    if n <= 1:
        return 1
    b = 0
    x = n - 1
    while x > 0:
        b += 1
        x >>= 1
    return max(1, b)


def _mtf_pos_and_move(mtf: List[int], idx: int) -> int:
    pos = mtf.index(idx)
    mtf.pop(pos)
    mtf.insert(0, idx)
    return pos


def _mtf_get_and_move(mtf: List[int], pos: int) -> int:
    idx = mtf[pos]
    mtf.pop(pos)
    mtf.insert(0, idx)
    return idx


def _drain_to_format(template_mined: str) -> str:
    return template_mined.replace("<*>", "{}")


def _make_miner() -> TemplateMiner:
    cfg = TemplateMinerConfig()
    cfg.profiling_enabled = False
    return TemplateMiner(config=cfg)


def _mine_chunks_stateful(miner: TemplateMiner, chunks: List[str]) -> Tuple[List[str], List[List[str]]]:
    chunk_templates: List[str] = []
    chunk_params: List[List[str]] = []

    for ch in chunks:
        ends_with_newline = ch.endswith("\n")
        lines = ch.splitlines(keepends=False)

        line_templates: List[str] = []
        all_params: List[str] = []

        for line in lines:
            res = miner.add_log_message(line)
            tmpl = res["template_mined"]
            params = _extract_params_from_template(line, tmpl)

            line_templates.append(tmpl)
            all_params.extend(params)

        rebuilt = "\n".join(line_templates)
        if ends_with_newline:
            rebuilt += "\n"

        chunk_templates.append(rebuilt)
        chunk_params.append(all_params)

    return chunk_templates, chunk_params


@dataclass
class StreamStateV3D:
    templates: List[str] = field(default_factory=list)
    temp_index: Dict[str, int] = field(default_factory=dict)
    mtf: List[int] = field(default_factory=list)

    strings: List[str] = field(default_factory=list)
    str_index: Dict[str, int] = field(default_factory=dict)
    str_mtf: List[int] = field(default_factory=list)

    miner: Optional[TemplateMiner] = None


def build_dict_state_from_chunks(chunks: List[str], state: StreamStateV3D) -> None:
    if state.miner is None:
        state.miner = _make_miner()

    mined_templates, _ = _mine_chunks_stateful(state.miner, chunks)

    for tmpl_mined in mined_templates:
        fmt_t = _drain_to_format(tmpl_mined)
        if fmt_t not in state.temp_index:
            tid = len(state.templates)
            state.templates.append(fmt_t)
            state.temp_index[fmt_t] = tid

    state.mtf = list(range(len(state.templates)))


def encode_dict_packet(state: StreamStateV3D, level: int = 10) -> bytes:
    out = bytearray()
    out += MAGIC_DICT
    out += encode_uvarint(len(state.templates))
    for t in state.templates:
        out += _pack_string(t)
    return zstd_compress(bytes(out), level=level)


def apply_dict_packet(packet: bytes, state: StreamStateV3D) -> None:
    raw = zstd_decompress(packet)
    if not raw.startswith(MAGIC_DICT):
        raise ValueError("not a v3d DICT packet")

    off = len(MAGIC_DICT)
    ntemp, off = decode_uvarint(raw, off)

    state.templates = []
    state.temp_index = {}
    for _ in range(ntemp):
        t, off = _unpack_string(raw, off)
        tid = len(state.templates)
        state.templates.append(t)
        state.temp_index[t] = tid

    state.mtf = list(range(len(state.templates)))

    state.strings = []
    state.str_index = {}
    state.str_mtf = []

    if state.miner is None:
        state.miner = _make_miner()


def encode_data_packet(chunks: List[str], state: StreamStateV3D, level: int = 10) -> bytes:
    if state.miner is None:
        state.miner = _make_miner()

    mined_templates, mined_params = _mine_chunks_stateful(state.miner, chunks)

    new_templates: List[str] = []
    for tmpl_mined in mined_templates:
        fmt_t = _drain_to_format(tmpl_mined)
        if fmt_t not in state.temp_index:
            tid = len(state.templates)
            state.templates.append(fmt_t)
            state.temp_index[fmt_t] = tid
            new_templates.append(fmt_t)

    if not state.mtf:
        state.mtf = list(range(len(state.templates)))
    else:
        cur = len(state.mtf)
        if len(state.templates) > cur:
            state.mtf.extend(range(cur, len(state.templates)))

    tid_positions: List[int] = []
    arities: List[int] = []

    for tmpl_mined in mined_templates:
        fmt_t = _drain_to_format(tmpl_mined)
        tid = state.temp_index[fmt_t]
        tid_positions.append(_mtf_pos_and_move(state.mtf, tid))
        arities.append(fmt_t.count("{}"))

    t_bits = _bits_needed(max(1, len(state.templates)))
    tpos_bytes = _bitpack(tid_positions, t_bits)

    flat_params: List[str] = []
    for tmpl_mined, params in zip(mined_templates, mined_params):
        fmt_t = _drain_to_format(tmpl_mined)
        arity = fmt_t.count("{}")

        if len(params) < arity:
            params = params + [""] * (arity - len(params))
        elif len(params) > arity:
            params = params[:arity]

        flat_params.extend(params)

    new_strings: List[str] = []
    for p in flat_params:
        if p not in state.str_index:
            sid = len(state.strings)
            state.strings.append(p)
            state.str_index[p] = sid
            new_strings.append(p)

    if not state.str_mtf:
        state.str_mtf = list(range(len(state.strings)))
    else:
        cur = len(state.str_mtf)
        if len(state.strings) > cur:
            state.str_mtf.extend(range(cur, len(state.strings)))

    spos: List[int] = []
    for p in flat_params:
        sid = state.str_index[p]
        spos.append(_mtf_pos_and_move(state.str_mtf, sid))

    s_bits = _bits_needed(max(1, len(state.strings)))
    spos_bytes = _bitpack(spos, s_bits)

    out = bytearray()
    out += MAGIC_DATA
    out += encode_uvarint(len(chunks))

    out += encode_uvarint(len(new_templates))
    for t in new_templates:
        out += _pack_string(t)

    out += encode_uvarint(t_bits)
    out += encode_uvarint(len(tpos_bytes))
    out += tpos_bytes

    out += encode_uvarint(len(arities))
    for a in arities:
        out += encode_uvarint(a)

    out += encode_uvarint(len(new_strings))
    for s in new_strings:
        out += _pack_string(s)

    out += encode_uvarint(s_bits)
    out += encode_uvarint(len(spos_bytes))
    out += spos_bytes

    return zstd_compress(bytes(out), level=level)


def decode_data_packet(packet: bytes, state: StreamStateV3D) -> List[str]:
    raw = zstd_decompress(packet)
    if not raw.startswith(MAGIC_DATA):
        raise ValueError("not a v3d6 DATA packet")

    off = len(MAGIC_DATA)
    n_chunks, off = decode_uvarint(raw, off)

    nnewt, off = decode_uvarint(raw, off)
    for _ in range(nnewt):
        t, off = _unpack_string(raw, off)
        if t not in state.temp_index:
            tid = len(state.templates)
            state.templates.append(t)
            state.temp_index[t] = tid

    if not state.mtf:
        state.mtf = list(range(len(state.templates)))
    else:
        cur = len(state.mtf)
        if len(state.templates) > cur:
            state.mtf.extend(range(cur, len(state.templates)))

    t_bits, off = decode_uvarint(raw, off)
    tpos_len, off = decode_uvarint(raw, off)
    tpos_bytes = raw[off:off + tpos_len]
    off += tpos_len
    tid_positions = _bitunpack(tpos_bytes, n_chunks, t_bits)

    n_ar, off = decode_uvarint(raw, off)
    if n_ar != n_chunks:
        raise ValueError("arity mismatch")
    arities: List[int] = []
    for _ in range(n_ar):
        a, off = decode_uvarint(raw, off)
        arities.append(a)

    nnews, off = decode_uvarint(raw, off)
    for _ in range(nnews):
        s, off = _unpack_string(raw, off)
        if s not in state.str_index:
            sid = len(state.strings)
            state.strings.append(s)
            state.str_index[s] = sid

    if not state.str_mtf:
        state.str_mtf = list(range(len(state.strings)))
    else:
        cur = len(state.str_mtf)
        if len(state.strings) > cur:
            state.str_mtf.extend(range(cur, len(state.strings)))

    s_bits, off = decode_uvarint(raw, off)
    spos_len, off = decode_uvarint(raw, off)
    spos_bytes = raw[off:off + spos_len]
    off += spos_len

    total_params = sum(arities)
    spos = _bitunpack(spos_bytes, total_params, s_bits)

    out_chunks: List[str] = []
    pidx = 0

    for i in range(n_chunks):
        tid = _mtf_get_and_move(state.mtf, tid_positions[i])
        template = state.templates[tid]
        arity = arities[i]

        vals: List[str] = []
        for _ in range(arity):
            sid = _mtf_get_and_move(state.str_mtf, spos[pidx])
            pidx += 1
            vals.append(state.strings[sid])

        out_chunks.append(template.format(*vals))

    return out_chunks
