from usc.bench.datasets_mixed_tool_trace import mixed_tool_trace
from usc.mem.sas_dict_token_v1 import build_sas_packets_from_text, _decode_dict_packet
from usc.mem.sas_keyword_index_v0 import build_keyword_index, query_packets_for_keywords, keywords_to_tool_keyword

from usc.api.odc2_sharded_v0 import (
    odc2s_encode_packets,
    packet_indices_to_block_ids,
)


def run():
    steps = 1200
    text = mixed_tool_trace(steps=steps, seed=7)

    # Query-mode preset
    max_lines = 10
    group_size = 2

    packets = build_sas_packets_from_text(
        text,
        max_lines_per_packet=max_lines,
        tok_top_k=0,
    )

    # Build keyword index
    kwi = build_keyword_index(
        packets,
        m_bits=2048,
        k_hashes=4,
        include_tool_names=True,
        include_raw_lines=True,
    )

    # Sharded compression (for block mapping)
    blob, meta = odc2s_encode_packets(
        packets,
        group_size=group_size,
        dict_target_size=8192,
        zstd_level=10,
        sample_blocks=64,
    )

    # Keywords to test
    tests = [
        {"warning"},
        {"decision"},
        {"thinking"},
        {"memory"},
        {"trace"},
        {"evaluate"},
        {"refine"},
        {keywords_to_tool_keyword("web.search_query")},
        {keywords_to_tool_keyword("web.open")},
        {keywords_to_tool_keyword("web.click")},
        {keywords_to_tool_keyword("web.screenshot")},
        {keywords_to_tool_keyword("finance")},
        {keywords_to_tool_keyword("weather")},
    ]

    print("USC Bench38 — Keyword selectivity (Bloom index -> blocks)")
    print(f"Preset: max_lines={max_lines} | group={group_size}")
    print("Packets:", len(packets), "| Blocks:", meta.block_count, "| Bytes:", len(blob))
    print("Bloom  :", f"m_bits={kwi.m_bits} k={kwi.k_hashes}")
    print("------------------------------------------------------------")

    for kws in tests:
        want_packet_indices = query_packets_for_keywords(kwi, packets, kws)
        block_ids = packet_indices_to_block_ids(want_packet_indices, meta.group_size)

        sel = len(block_ids)
        total = meta.block_count
        pct = 100.0 * sel / max(1, total)

        label = "+".join(sorted(kws))
        print(
            f"{label:28s} | want_pkts={len(want_packet_indices):3d} | sel_blocks={sel:3d}/{total:3d} ({pct:5.1f}%)"
        )

    print("------------------------------------------------------------")


if __name__ == "__main__":
    run()
