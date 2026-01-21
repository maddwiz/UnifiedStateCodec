from usc.bench.datasets_real_agent_trace import real_agent_trace
from usc.api.codec_odc import build_v3b_packets_from_text
from usc.api.codec_odc2_indexed import (
    odc2_encode_packets,
    odc2_decode_all_packets,
    odc2_decode_packet_range,
)


def run():
    text = real_agent_trace(loops=900, seed=7)
    packets = build_v3b_packets_from_text(
        text,
        max_lines_per_chunk=60,
        window_chunks=1,
        level=10,
    )

    blob, meta = odc2_encode_packets(
        packets,
        level=10,
        dict_target_size=8192,
        sample_chunk_size=1024,
        group_size=4,
    )

    all_back = odc2_decode_all_packets(blob)

    start = 3
    end = min(12, len(packets))
    sub_back = odc2_decode_packet_range(blob, start, end)

    ok_all = (all_back == packets)
    ok_sub = (sub_back == packets[start:end])

    print("USC Bench23 — ODC2 selective decode correctness")
    print("------------------------------------------------------------")
    print("packets           :", len(packets))
    print("group_size        :", meta.group_size)
    print("blocks            :", meta.block_count)
    print("dict_bytes        :", meta.dict_bytes)
    print("blob_bytes        :", len(blob))
    print("decode_all_OK     :", ok_all)
    print(f"decode_range_OK   : {ok_sub}  range=[{start}:{end}]  returned={len(sub_back)}")
    print("------------------------------------------------------------")


if __name__ == "__main__":
    run()
