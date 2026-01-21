from __future__ import annotations

from dataclasses import dataclass
from typing import List, Set, Optional

from usc.mem.sas_dict_token_v1 import decode_sas_packets_to_lines
from usc.mem.sas_keyword_index_v0 import (
    SASKeywordIndex,
    build_keyword_index,
    query_packets_for_keywords,
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


def _expand_match_terms(kws: Set[str]) -> List[List[str]]:
    """
    For each keyword, return a list of acceptable match terms.
    - Normal keyword: ["keyword"]
    - tool:NAME keyword: ["tool:name", "tool_call::name"]
      so user can query with tool:web.open but we match decoded output.
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
            out.append([k])
    return out


def recall_from_odc2s(
    blob: bytes,
    packets_all: List[bytes],
    keywords: Set[str],
    kwi: Optional[SASKeywordIndex] = None,
    group_size: int = 2,
    require_all: bool = True,
) -> RecallResult:
    """
    Recall lines from a sharded ODC2S stream using Bloom keyword index.

    Strategy:
      1) Bloom -> candidate packet indices
      2) packet indices -> block IDs
      3) ALWAYS include dict packet block (packet 0)
      4) Decode only those blocks
      5) Decode lines and post-filter for correctness
    """
    kws = _normalize_keywords(keywords)
    if not kws or not packets_all:
        return RecallResult(matched_lines=[], selected_packets=0, selected_blocks=0, total_blocks=0)

    # Build index if needed
    if kwi is None:
        kwi = build_keyword_index(
            packets_all,
            m_bits=2048,
            k_hashes=4,
            include_tool_names=True,
            include_raw_lines=True,
        )

    # Candidate packets (Bloom test)
    want_packet_indices = query_packets_for_keywords(kwi, packets_all, kws)

    # Convert packets -> blocks
    block_ids = packet_indices_to_block_ids(want_packet_indices, group_size)

    # ✅ ALWAYS include dict packet block (packet index 0)
    dict_block = next(iter(packet_indices_to_block_ids({0}, group_size)))
    block_ids.add(dict_block)

    # Decode only selected blocks
    packets_part, meta = odc2s_decode_selected_blocks(blob, block_ids=block_ids)

    # ✅ Ensure dict packet is present at front
    if not packets_part:
        packets_part = [packets_all[0]]
    else:
        if packets_part[0] != packets_all[0]:
            packets_part = [packets_all[0]] + packets_part

    # Decode to lines
    lines = decode_sas_packets_to_lines(packets_part)

    # Expand match terms (fixes tool:* queries)
    match_terms = _expand_match_terms(kws)

    # Post-filter for exact correctness
    out: List[str] = []
    for ln in lines:
        lnl = ln.lower()

        if require_all:
            ok = True
            for term_group in match_terms:
                # each keyword group matches if ANY of its terms appear
                if not any(t in lnl for t in term_group):
                    ok = False
                    break
            if ok:
                out.append(ln)
        else:
            # OR mode: match if ANY keyword group matches
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
