from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

# We reuse v3b and v3d6 as sub-codecs (ENCODE ONLY for v3d6 dictless)
from usc.mem.stream_proto_canz_v3b import (
    StreamStateV3B,
    build_dict_state_from_chunks as build_v3b,
    encode_dict_packet as dict_v3b,
    apply_dict_packet as apply_v3b,
    encode_data_packet as data_v3b,
)

from usc.mem.stream_proto_canz_v3d_drain3 import (
    StreamStateV3D,
    build_dict_state_from_chunks as build_v3d6,
    encode_data_packet as data_v3d6,
)

MAGIC_DICT = b"USAUTOD2"
MAGIC_DATA = b"USAUTOP2"

MODE_V3B = 0
MODE_V3D6 = 1


@dataclass
class StreamStateV3AUTO:
    st_v3b: StreamStateV3B = field(default_factory=StreamStateV3B)
    st_v3d6: StreamStateV3D = field(default_factory=StreamStateV3D)


def build_dict_state_from_chunks(chunks: List[str], state: StreamStateV3AUTO) -> None:
    # build both encoder-side, but only transmit v3b dict
    build_v3b(chunks, state=state.st_v3b)
    build_v3d6(chunks, state=state.st_v3d6)


def encode_dict_packet(state: StreamStateV3AUTO, level: int = 10) -> bytes:
    # ONLY v3b dict is transmitted
    pkt_b = dict_v3b(state.st_v3b, level=level)

    out = bytearray()
    out += MAGIC_DICT
    out += len(pkt_b).to_bytes(4, "little")
    out += pkt_b

    # NOTE: no outer zstd wrapper. v3b dict is already zstd.
    return bytes(out)


def apply_dict_packet(packet: bytes, state: StreamStateV3AUTO) -> None:
    if not packet.startswith(MAGIC_DICT):
        raise ValueError("not an AUTO v2 dict packet")

    off = len(MAGIC_DICT)
    nb = int.from_bytes(packet[off:off + 4], "little")
    off += 4
    pkt_b = packet[off:off + nb]
    off += nb

    apply_v3b(pkt_b, state=state.st_v3b)

    # v3d6 dictless mode:
    # start empty and let DATA packets refresh templates over time.
    state.st_v3d6.templates = []
    state.st_v3d6.temp_index = {}
    state.st_v3d6.mtf = []
    state.st_v3d6.strings = []
    state.st_v3d6.str_index = {}
    state.st_v3d6.str_mtf = []
    state.st_v3d6.miner = None


def encode_data_packet(chunks: List[str], state: StreamStateV3AUTO, level: int = 10) -> bytes:
    pkt_b = data_v3b(chunks, state.st_v3b, level=level)
    pkt_d = data_v3d6(chunks, state.st_v3d6, level=level)

    if len(pkt_b) <= len(pkt_d):
        mode = MODE_V3B
        payload = pkt_b
    else:
        mode = MODE_V3D6
        payload = pkt_d

    out = bytearray()
    out += MAGIC_DATA
    out += bytes([mode])
    out += len(payload).to_bytes(4, "little")
    out += payload

    # NOTE: no outer zstd wrapper. payload already zstd.
    return bytes(out)
