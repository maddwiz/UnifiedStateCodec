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
      3) ALWAYS include the dict packet block (packet 0)
      4) Decode only those blocks
      5) Decode lines and post-filter for correctness
    """
    kws = _normalize_keywords(keywords)
    if not kws:
        return RecallResult(matched_lines=[], selected_packets=0, selected_blocks=0, total_blocks=0)

    if not packets_all:
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

    # Candidate packets (1..N-1 usually)
    want_packet_indices = query_packets_for_keywords(kwi, packets_all, kws)

    # Convert packets -> blocks
    block_ids = packet_indices_to_block_ids(want_packet_indices, group_size)

    # ✅ ALWAYS include dict packet block (packet index 0)
    dict_block = next(iter(packet_indices_to_block_ids({0}, group_size)))
    block_ids.add(dict_block)

    # Decode only selected blocks
    packets_part, meta = odc2s_decode_selected_blocks(blob, block_ids=block_ids)

    # ✅ Safety: ensure dict packet is present at front
    # If the dict packet is missing or not first, prepend it.
    if not packets_part:
        packets_part = [packets_all[0]]
    else:
        # dict packet should be packets_all[0]
        if packets_part[0] != packets_all[0]:
            packets_part = [packets_all[0]] + packets_part

    # Decode to lines
    lines = decode_sas_packets_to_lines(packets_part)

    # Post-filter for exact correctness (Bloom can false-positive)
    out: List[str] = []
    for ln in lines:
        lnl = ln.lower()
        if require_all:
            if all(k in lnl for k in kws):
                out.append(ln)
        else:
            if any(k in lnl for k in kws):
                out.append(ln)

    return RecallResult(
        matched_lines=out,
        selected_packets=len(want_packet_indices),
        selected_blocks=len(block_ids),
        total_blocks=meta.block_count,
    )
