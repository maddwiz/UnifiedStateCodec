from __future__ import annotations

from dataclasses import dataclass
from typing import List, Set, Optional

from usc.mem.sas_dict_token_v1 import decode_sas_packets_to_lines
from usc.mem.sas_keyword_index_v0 import (
    SASKeywordIndex,
    build_keyword_index,
    query_packets_for_keywords,
    _stem_lite,  # ok to import internal helper for consistent behavior
)

from usc.api.odc2_sharded_v0 import (
    odc2s_decode_selected_blocks,
    packet_indices_to_block_ids,
)


@dataclass
class RecallResult:
    matched_lines: List[str]
    selected_packets: int
    selected_blocks: int
    total_blocks: int


def _normalize_keywords(keywords: Set[str]) -> Set[str]:
    return {k.strip().lower() for k in keywords if k.strip()}


def _keyword_match_terms(k: str, prefix_len: int = 5) -> List[str]:
    """
    Expand keyword into match substrings that can appear in decoded text.
    - exact keyword
    - stem-lite keyword
    - prefix substrings (NOT 'pref:xxxxx' token; actual substring)
    """
    out: List[str] = []
    k = k.strip().lower()
    if not k:
        return out

    out.append(k)

    ks = _stem_lite(k)
    if ks and ks not in out:
        out.append(ks)

    if prefix_len and len(k) >= prefix_len:
        p = k[:prefix_len]
        if p not in out:
            out.append(p)

    if prefix_len and len(ks) >= prefix_len:
        ps = ks[:prefix_len]
        if ps not in out:
            out.append(ps)

    return out


def _expand_match_terms(kws: Set[str], prefix_len: int = 5) -> List[List[str]]:
    """
    For each keyword, return a list of acceptable substring match terms.

    - tool:NAME keyword:
        ["tool:name", "tool_call::name"]
      (tool:name won't exist in decoded lines, but tool_call::name will)

    - normal keyword:
        ["kw", "stem", "prefix", "stem_prefix"]
    """
    out: List[List[str]] = []
    for k in kws:
        if k.startswith("tool:"):
            name = k.split("tool:", 1)[1].strip()
            if name:
                out.append([k, f"tool_call::{name}"])
            else:
                out.append([k])
        else:
            out.append(_keyword_match_terms(k, prefix_len=prefix_len))
    return out


def recall_from_odc2s(
    blob: bytes,
    packets_all: List[bytes],
    keywords: Set[str],
    kwi: Optional[SASKeywordIndex] = None,
    group_size: int = 2,
    require_all: bool = True,
    prefix_len: int = 5,
) -> RecallResult:
    """
    Recall lines from a sharded ODC2S stream using Bloom keyword index.

    Includes:
      - dict block always
      - tool:* support
      - stemming/prefix in BOTH:
          Bloom candidate selection
          AND final post-filter
    """
    kws = _normalize_keywords(keywords)
    if not kws or not packets_all:
        return RecallResult(matched_lines=[], selected_packets=0, selected_blocks=0, total_blocks=0)

    if kwi is None:
        kwi = build_keyword_index(
            packets_all,
            m_bits=2048,
            k_hashes=4,
            include_tool_names=True,
            include_raw_lines=True,
            enable_stem=True,
            prefix_len=prefix_len,
        )

    # Candidate packets using Bloom
    want_packet_indices = query_packets_for_keywords(
        kwi,
        packets_all,
        kws,
        enable_stem=True,
        prefix_len=prefix_len,
        require_all=require_all,
    )

    # Packets -> blocks
    block_ids = packet_indices_to_block_ids(want_packet_indices, group_size)

    # Always include dict block
    dict_block = next(iter(packet_indices_to_block_ids({0}, group_size)))
    block_ids.add(dict_block)

    # Decode selected blocks
    packets_part, meta = odc2s_decode_selected_blocks(blob, block_ids=block_ids)

    # Ensure dict packet is present first
    if not packets_part:
        packets_part = [packets_all[0]]
    else:
        if packets_part[0] != packets_all[0]:
            packets_part = [packets_all[0]] + packets_part

    # Decode lines
    lines = decode_sas_packets_to_lines(packets_part)

    # Expand match terms for post-filter
    match_terms = _expand_match_terms(kws, prefix_len=prefix_len)

    # Post-filter
    out: List[str] = []
    for ln in lines:
        lnl = ln.lower()

        if require_all:
            ok = True
            for term_group in match_terms:
                if not any(t in lnl for t in term_group):
                    ok = False
                    break
            if ok:
                out.append(ln)
        else:
            hit_any = False
            for term_group in match_terms:
                if any(t in lnl for t in term_group):
                    hit_any = True
                    break
            if hit_any:
                out.append(ln)

    return RecallResult(
        matched_lines=out,
        selected_packets=len(want_packet_indices),
        selected_blocks=len(block_ids),
        total_blocks=meta.block_count,
    )
