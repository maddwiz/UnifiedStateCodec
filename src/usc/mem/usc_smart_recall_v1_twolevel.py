from __future__ import annotations

from typing import List, Optional, Set, Tuple, Dict

from usc.api.odc2s_bloom_footer_v0 import read_block_bloom_footer
from usc.mem.block_bloom_index_v0 import query_blocks_for_keywords
from usc.mem.sas_keyword_index_v0 import SASKeywordIndex, query_packets_for_keywords
from usc.api.odc2_sharded_v0 import odc2s_decode_selected_blocks
from usc.mem.usc_recall_v0 import recall_from_packets_decoded, RecallResult


def _packet_index_to_block(packet_index: int, group_size: int) -> int:
    return packet_index // group_size


def _infer_packet_indices_for_decoded_blocks(
    packets_part: List[bytes],
    block_ids_sorted: List[int],
    group_size: int,
) -> List[Tuple[int, bytes]]:
    """
    Best-effort mapping:
      odc2s_decode_selected_blocks returns packets in block order.
      Each block contains group_size packets in sequence.
    This lets us attach approximate packet indices so we can filter.
    """
    out: List[Tuple[int, bytes]] = []

    if not packets_part:
        return out

    # packets_part includes dict packet at [0] often, but not guaranteed.
    # We don't assign index to dict packet here.
    data_packets = packets_part[1:] if packets_part[0].startswith(b"SASD") else packets_part

    p = 0
    for bid in block_ids_sorted:
        for j in range(group_size):
            if p >= len(data_packets):
                break
            packet_index = bid * group_size + j
            out.append((packet_index, data_packets[p]))
            p += 1

    return out


def smart_recall_twolevel_from_blob(
    blob: bytes,
    packets_all: List[bytes],
    keywords: Set[str],
    kwi: Optional[SASKeywordIndex],
    require_all: bool = True,
    prefix_len: int = 5,
) -> RecallResult:
    """
    Two-level selective recall:
      1) Block bloom footer chooses candidate blocks
      2) Packet bloom chooses candidate packets
      3) Keep only packets that lie inside chosen blocks
      4) Decode those blocks once
      5) Filter only selected packets (plus dict packet)
    """
    if kwi is None:
        # Fallback: without packet blooms we can't do level-2 refinement.
        # Just run normal recall.
        from usc.mem.usc_recall_v0 import recall_from_odc2s
        return recall_from_odc2s(blob, packets_all, keywords, kwi=None, group_size=2, require_all=require_all, prefix_len=prefix_len)

    bbi = read_block_bloom_footer(blob)
    if bbi is None:
        # No footer, fallback to normal recall
        from usc.mem.usc_recall_v0 import recall_from_odc2s
        return recall_from_odc2s(blob, packets_all, keywords, kwi=kwi, group_size=2, require_all=require_all, prefix_len=prefix_len)

    group_size = bbi.group_size

    # Level 1: blocks
    block_ids = query_blocks_for_keywords(
        bbi,
        keywords,
        enable_stem=True,
        prefix_len=prefix_len,
        require_all=require_all,
    )

    # Always include dict block (block of packet index 0)
    dict_block = _packet_index_to_block(0, group_size)
    block_ids.add(dict_block)

    # Level 2: packets (global packet blooms)
    pkt_ids = query_packets_for_keywords(
        kwi,
        packets_all,
        keywords,
        enable_stem=True,
        prefix_len=prefix_len,
        require_all=require_all,
    )

    # Keep only packets that fall into selected blocks
    pkt_ids_in_blocks = {pi for pi in pkt_ids if _packet_index_to_block(pi, group_size) in block_ids}

    # Decode selected blocks once
    block_ids_sorted = sorted(block_ids)
    packets_part, meta = odc2s_decode_selected_blocks(blob, block_ids=set(block_ids_sorted))

    # Ensure dict packet at front
    if not packets_part:
        packets_part = [packets_all[0]]
    else:
        if not packets_part[0].startswith(b"SASD"):
            packets_part = [packets_all[0]] + packets_part

    # Attach inferred packet indices to decoded packets
    pairs = _infer_packet_indices_for_decoded_blocks(packets_part, block_ids_sorted, group_size)

    # Rebuild packets_part_filtered:
    # Keep dict + only packets whose inferred index is in pkt_ids_in_blocks
    filtered: List[bytes] = [packets_part[0]]
    for pi, pkt in pairs:
        if pi in pkt_ids_in_blocks:
            filtered.append(pkt)

    return recall_from_packets_decoded(
        packets_part=filtered,
        keywords=keywords,
        require_all=require_all,
        prefix_len=prefix_len,
        selected_packets=len(pkt_ids_in_blocks),
        selected_blocks=len(block_ids),
        total_blocks=meta.block_count,
    )
