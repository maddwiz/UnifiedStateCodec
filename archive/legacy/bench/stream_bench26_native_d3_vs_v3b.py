from usc.bench.datasets_real_agent_trace import real_agent_trace
from usc.bench.metrics import gzip_compress
import zstandard as zstd

from usc.api.codec_odc import build_v3b_packets_from_text
from usc.api.codec_odc2_indexed import odc2_encode_packets

from usc.mem.stream_proto_d3_native_v0 import build_d3_packets_from_text


def _ratio(raw: int, comp: int) -> float:
    return raw / max(1, comp)


def run():
    loops = 900
    text = real_agent_trace(loops=loops, seed=7)
    raw = text.encode("utf-8")

    gz = gzip_compress(raw)
    zstd_plain = zstd.ZstdCompressor(level=10).compress(raw)

    # ---- USC v3b packets ----
    packets_v3b = build_v3b_packets_from_text(
        text,
        max_lines_per_chunk=60,
        window_chunks=1,
        level=10,
    )

    blob_v3b, meta_v3b = odc2_encode_packets(
        packets_v3b,
        level=10,
        dict_target_size=8192,
        sample_chunk_size=1024,
        group_size=8,
    )

    # ---- USC-D3 native packets ----
    packets_d3 = build_d3_packets_from_text(
        text,
        max_lines_per_packet=60,
    )

    blob_d3, meta_d3 = odc2_encode_packets(
        packets_d3,
        level=10,
        dict_target_size=8192,
        sample_chunk_size=1024,
        group_size=8,
    )

    print("USC Bench26 — Native Drain3 packets vs USC v3b (ODC2 gs=8)")
    print("------------------------------------------------------------")
    print(f"RAW bytes            : {len(raw)}")
    print(f"GZIP bytes           : {len(gz):>7} ({_ratio(len(raw), len(gz)):.2f}x)")
    print(f"ZSTD plain bytes     : {len(zstd_plain):>7} ({_ratio(len(raw), len(zstd_plain)):.2f}x)")
    print("------------------------------------------------------------")
    print(f"ODC2 v3b blob bytes   : {len(blob_v3b):>7} ({_ratio(len(raw), len(blob_v3b)):.2f}x) blocks={meta_v3b.block_count} dict={meta_v3b.dict_bytes} mode={meta_v3b.used_mode}")
    print(f"ODC2 D3  blob bytes   : {len(blob_d3):>7} ({_ratio(len(raw), len(blob_d3)):.2f}x) blocks={meta_d3.block_count} dict={meta_d3.dict_bytes} mode={meta_d3.used_mode}")
    print("------------------------------------------------------------")
    print(f"Packets v3b: {len(packets_v3b)}")
    print(f"Packets D3 : {len(packets_d3)}")
    print("------------------------------------------------------------")


if __name__ == "__main__":
    run()
