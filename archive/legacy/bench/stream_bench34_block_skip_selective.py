import time

from usc.bench.datasets_real_agent_trace import real_agent_trace
from usc.mem.sas_dict_token_v1 import build_sas_packets_from_text, decode_sas_packets_to_lines
from usc.mem.sas_index_v0 import build_index, selective_decode_lines

from usc.api.odc2_sharded_v0 import (
    odc2s_encode_packets,
    odc2s_decode_all,
    odc2s_decode_selected_blocks,
    packet_indices_to_block_ids,
)

from usc.mem.sas_dict_token_v1 import _decode_dict_packet


def run():
    loops = 900
    text = real_agent_trace(loops=loops, seed=7)

    # Build SAS packets (dict-token v1)
    packets = build_sas_packets_from_text(
        text,
        max_lines_per_packet=60,
        tok_top_k=0,
    )

    # Decode dict so we can map tool name -> tool_id
    d = _decode_dict_packet(packets[0])

    # Build index to find packet indices containing each tool_id
    t0 = time.perf_counter()
    idx = build_index(packets)
    t1 = time.perf_counter()

    want_tool = "web.search_query"
    want_tool_id = d.tool_to_id.get(want_tool, 0)

    if want_tool_id == 0:
        print("ERROR: tool not found in dict:", want_tool)
        return

    # These are PACKET indices in the packet list that contain this tool_id
    want_packet_indices = set(idx.tool_to_packets.get(want_tool_id, []))

    # Encode to ODC2S blocks
    t2 = time.perf_counter()
    blob, meta = odc2s_encode_packets(
        packets,
        group_size=8,
        dict_target_size=8192,
        zstd_level=10,
        sample_blocks=64,
    )
    t3 = time.perf_counter()

    # Full decode (all blocks)
    t4 = time.perf_counter()
    packets_full = odc2s_decode_all(blob)
    t5 = time.perf_counter()

    # Convert wanted packet indices -> wanted block ids
    # IMPORTANT: packet_indices are 0-based relative to 'packets' list
    block_ids = packet_indices_to_block_ids(want_packet_indices, meta.group_size)

    # Selective block decode
    t6 = time.perf_counter()
    packets_part, _meta2 = odc2s_decode_selected_blocks(blob, block_ids=block_ids)
    t7 = time.perf_counter()

    # Now selective decode only the tool we care about
    t8 = time.perf_counter()
    lines_sel = selective_decode_lines(packets_part, include_tools={want_tool}, include_raw_lines=False)
    t9 = time.perf_counter()

    # Full decode -> lines (for reference)
    t10 = time.perf_counter()
    lines_full = decode_sas_packets_to_lines(packets_full)
    t11 = time.perf_counter()

    print("USC Bench34 — Block-skip selective decode (ODC2S sharded v0)")
    print("------------------------------------------------------------")
    print("Want tool:", want_tool, "| tool_id:", want_tool_id)
    print("SAS packets:", len(packets))
    print("Wanted packets:", len(want_packet_indices))
    print("ODC2S bytes :", len(blob))
    print("Meta        :", f"blocks={meta.block_count} group={meta.group_size} dict={meta.dict_bytes}")
    print("Block IDs selected:", len(block_ids), "of", meta.block_count)
    print("------------------------------------------------------------")
    print("Index build time        (ms):", round((t1 - t0) * 1000, 2))
    print("Encode ODC2S time       (ms):", round((t3 - t2) * 1000, 2))
    print("Full decode ALL blocks  (ms):", round((t5 - t4) * 1000, 2))
    print("Decode SELECT blocks    (ms):", round((t7 - t6) * 1000, 2))
    print("Selective filter time   (ms):", round((t9 - t8) * 1000, 2))
    print("Full decode -> lines    (ms):", round((t11 - t10) * 1000, 2))
    print("------------------------------------------------------------")
    print("Full lines:", len(lines_full))
    print("Selective lines:", len(lines_sel))
    print("Examples (first 5 selective):")
    for ln in lines_sel[:5]:
        print("  ", ln[:120])


if __name__ == "__main__":
    run()
