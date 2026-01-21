from usc.bench.datasets_real_agent_trace import real_agent_trace
from usc.mem.stream_proto_d3_native_v0 import (
    build_d3_packets_from_text,
    decode_d3_packets_to_lines,
)
from usc.api.codec_odc2_indexed import odc2_encode_packets


def _ratio(raw: int, comp: int) -> float:
    return raw / max(1, comp)


def run():
    text = real_agent_trace(loops=900, seed=7)
    raw = text.encode("utf-8")

    packets = build_d3_packets_from_text(text, max_lines_per_packet=60)

    blob, meta = odc2_encode_packets(
        packets,
        level=10,
        dict_target_size=8192,
        sample_chunk_size=1024,
        group_size=8,
    )

    lines_in = text.splitlines()
    lines_out = decode_d3_packets_to_lines(packets)

    n = min(len(lines_in), len(lines_out))
    same = 0
    for i in range(n):
        if lines_in[i] == lines_out[i]:
            same += 1

    print("USC Bench27 — D3 + delta patches (lossless check)")
    print("------------------------------------------------------------")
    print("raw_bytes     :", len(raw))
    print("blob_bytes    :", len(blob), f"({_ratio(len(raw), len(blob)):.2f}x)")
    print("packets       :", len(packets))
    print("dict_bytes    :", meta.dict_bytes)
    print("blocks        :", meta.block_count)
    print("exact_match   :", f"{same}/{n} ({same/n*100:.2f}%)")
    print("------------------------------------------------------------")


if __name__ == "__main__":
    run()
