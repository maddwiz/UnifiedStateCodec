from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import hashlib
import re

from usc.mem.sas_dict_token_v1 import (
    MAGIC_SASD,
    MAGIC_SASA,
    _decode_dict_packet,
    _decode_data_packet,
)

# Tokenizer: words >= 3 chars (letters/numbers/_), lowercase
RE_WORD = re.compile(r"[A-Za-z0-9_]{3,}")


def _hash64(s: str) -> int:
    """
    Stable 64-bit hash using blake2b.
    """
    h = hashlib.blake2b(s.encode("utf-8", errors="replace"), digest_size=8).digest()
    return int.from_bytes(h, "little", signed=False)


def _k_hashes(h64: int, k: int, m_bits: int) -> List[int]:
    """
    Derive k bloom positions from a base 64-bit hash.
    Uses a simple mixing sequence.
    """
    out: List[int] = []
    x = h64 & ((1 << 64) - 1)
    for i in range(k):
        x = (x * 0x9E3779B97F4A7C15 + (i + 1) * 0xD1B54A32D192ED03) & ((1 << 64) - 1)
        out.append(int(x % m_bits))
    return out


def _set_bit(bits: bytearray, pos: int) -> None:
    byte_i = pos >> 3
    bit_i = pos & 7
    bits[byte_i] |= (1 << bit_i)


def _get_bit(bits: bytes, pos: int) -> bool:
    byte_i = pos >> 3
    bit_i = pos & 7
    return (bits[byte_i] >> bit_i) & 1 == 1


def _tokenize_text(s: str) -> List[str]:
    return [w.lower() for w in RE_WORD.findall(s)]


def _stem_lite(word: str) -> str:
    """
    Very cheap stemming:
      evaluating -> evaluate
      decided -> decid
      errors -> error
    Not linguistically perfect, but good enough for logs.
    """
    w = word.lower()
    if len(w) <= 3:
        return w

    # common suffixes
    for suf in ("ing", "edly", "edly", "edly", "edly", "edly"):
        if w.endswith(suf) and len(w) > len(suf) + 2:
            return w[: -len(suf)]

    for suf in ("ing", "ed", "es", "s"):
        if w.endswith(suf) and len(w) > len(suf) + 2:
            return w[: -len(suf)]

    return w


def _variants_for_keyword(kw: str, enable_stem: bool, prefix_len: int) -> Set[str]:
    """
    Generate matching variants:
      - original
      - stemmed (optional)
      - prefix token (optional), like "pref:evalu"
    """
    out = set()
    k = kw.strip().lower()
    if not k:
        return out

    out.add(k)

    if enable_stem:
        out.add(_stem_lite(k))

    if prefix_len and len(k) >= prefix_len:
        out.add(f"pref:{k[:prefix_len]}")

        if enable_stem:
            ks = _stem_lite(k)
            if len(ks) >= prefix_len:
                out.add(f"pref:{ks[:prefix_len]}")

    return out


@dataclass
class SASKeywordIndex:
    """
    Bloom filter per data packet (packet index 1..N-1).

    packet_blooms[j] corresponds to packets index (j+1)

    keyword_df: approximate document frequency (#packets where keyword variant appears).
      - used for rarest-first planning
      - variants include pref:* tokens too
    """
    m_bits: int
    k_hashes: int
    packet_blooms: List[bytes]
    total_packets: int
    keyword_df: Dict[str, int]


def build_keyword_index(
    packets: List[bytes],
    m_bits: int = 2048,
    k_hashes: int = 4,
    include_tool_names: bool = True,
    include_raw_lines: bool = True,
    enable_stem: bool = True,
    prefix_len: int = 5,
) -> SASKeywordIndex:
    """
    Build a Bloom filter index over packets without fully decoding everything.

    What we index:
      - Raw line words if include_raw_lines
      - Tool name tokens "tool:web.search_query" if include_tool_names
      - Optional:
          stem-lites for words
          prefix tokens pref:xxxxx (helps 'evaluate' match 'evaluating')
    """
    if not packets:
        return SASKeywordIndex(m_bits=m_bits, k_hashes=k_hashes, packet_blooms=[], total_packets=0, keyword_df={})

    if not packets[0].startswith(MAGIC_SASD):
        raise ValueError("Not a SAS dict-token v1 stream")

    d = _decode_dict_packet(packets[0])

    if m_bits % 8 != 0:
        raise ValueError("m_bits must be multiple of 8")

    bloom_bytes = m_bits // 8
    packet_blooms: List[bytes] = []
    keyword_df: Dict[str, int] = {}

    # data packets start at index 1
    for pi in range(1, len(packets)):
        pkt = packets[pi]
        if not pkt.startswith(MAGIC_SASA):
            packet_blooms.append(bytes(bloom_bytes))
            continue

        bits = bytearray(bloom_bytes)
        entries = _decode_data_packet(pkt)

        seen_in_packet: Set[str] = set()

        for (_dt_us, _ts_style, tool_id, _rid16, raw_body, _payload_tok) in entries:
            if tool_id == 0:
                if include_raw_lines:
                    text = raw_body.decode("utf-8", errors="replace")
                    for w in _tokenize_text(text):
                        for v in _variants_for_keyword(w, enable_stem=enable_stem, prefix_len=prefix_len):
                            seen_in_packet.add(v)

                continue

            if include_tool_names:
                tid = int(tool_id)
                tool_name = "UNKNOWN"
                if 1 <= tid <= len(d.id_to_tool):
                    tool_name = d.id_to_tool[tid - 1]
                kw = f"tool:{tool_name}".lower()
                seen_in_packet.add(kw)

        # set bloom bits and update DF
        for v in seen_in_packet:
            h64 = _hash64(v)
            for pos in _k_hashes(h64, k_hashes, m_bits):
                _set_bit(bits, pos)
            keyword_df[v] = keyword_df.get(v, 0) + 1

        packet_blooms.append(bytes(bits))

    return SASKeywordIndex(
        m_bits=int(m_bits),
        k_hashes=int(k_hashes),
        packet_blooms=packet_blooms,
        total_packets=len(packets),
        keyword_df=keyword_df,
    )


def query_packets_for_keywords(
    kwi: SASKeywordIndex,
    packets: List[bytes],
    keywords: Set[str],
    enable_stem: bool = True,
    prefix_len: int = 5,
    require_all: bool = True,
) -> Set[int]:
    """
    Return packet indices (in the packets list) that might contain the keywords.
    Uses Bloom filter membership test.

    If require_all=True:
      - packet must match ALL keywords (Bloom AND)
    else:
      - match ANY keyword (Bloom OR)
    """
    if not keywords:
        return set()

    if not packets:
        return set()

    # normalize -> variants per keyword
    kw_groups: List[Set[str]] = []
    for k in keywords:
        vs = _variants_for_keyword(k, enable_stem=enable_stem, prefix_len=prefix_len)
        if vs:
            kw_groups.append(vs)

    if not kw_groups:
        return set()

    # precompute positions for every variant
    variant_positions: Dict[str, List[int]] = {}
    for group in kw_groups:
        for v in group:
            if v not in variant_positions:
                variant_positions[v] = _k_hashes(_hash64(v), kwi.k_hashes, kwi.m_bits)

    out: Set[int] = set()

    # packet_blooms[j] corresponds to packet index (j+1)
    for j, bits in enumerate(kwi.packet_blooms):
        pi = j + 1
        if pi >= len(packets):
            break

        if require_all:
            ok = True
            for group in kw_groups:
                # group matches if ANY variant matches
                g_ok = False
                for v in group:
                    pos_list = variant_positions[v]
                    if all(_get_bit(bits, pos) for pos in pos_list):
                        g_ok = True
                        break
                if not g_ok:
                    ok = False
                    break
            if ok:
                out.add(pi)
        else:
            # OR mode
            hit = False
            for group in kw_groups:
                for v in group:
                    pos_list = variant_positions[v]
                    if all(_get_bit(bits, pos) for pos in pos_list):
                        hit = True
                        break
                if hit:
                    break
            if hit:
                out.add(pi)

    return out


def keywords_to_tool_keyword(tool_name: str) -> str:
    return f"tool:{tool_name}".lower()
