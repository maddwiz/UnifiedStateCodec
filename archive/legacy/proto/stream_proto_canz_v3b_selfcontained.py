from __future__ import annotations

from dataclasses import dataclass
from typing import List

from usc.mem.stream_proto_canz_v3b import (
    StreamStateV3B,
    build_dict_state_from_chunks as build_v3b,
    encode_dict_packet as dict_v3b,
    apply_dict_packet as apply_v3b,
    encode_data_packet as data_v3b,
)

MAGIC_SC = b"US3BSC00"


@dataclass
class StreamStateV3BSC:
    """
    Dictless/self-contained v3b.

    - No dict packet is sent out-of-band.
    - Each data packet contains (dict + data) built ONLY for that packet's chunks.
    - Great for short sessions / bursty agent memory.
    - Not intended to beat normal v3b on huge sessions.
    """
    pass


def build_dict_state_from_chunks(chunks: List[str], state: StreamStateV3BSC) -> None:
    # no-op (we do everything per packet)
    return


def encode_dict_packet(state: StreamStateV3BSC, level: int = 10) -> bytes:
    # dictless mode: no standalone dict packet
    return b""


def apply_dict_packet(packet: bytes, state: StreamStateV3BSC) -> None:
    # dictless mode: nothing to apply
    return


def encode_data_packet(chunks: List[str], state: StreamStateV3BSC, level: int = 10) -> bytes:
    """
    Create a self-contained packet:
    [MAGIC][len(dict)][dict_bytes][len(data)][data_bytes]

    dict_bytes and data_bytes are already zstd'd by v3b, so we DO NOT wrap again.
    """
    st_build = StreamStateV3B()
    build_v3b(chunks, state=st_build)
    pkt_dict = dict_v3b(st_build, level=level)

    st_send = StreamStateV3B()
    apply_v3b(pkt_dict, state=st_send)
    pkt_data = data_v3b(chunks, st_send, level=level)

    out = bytearray()
    out += MAGIC_SC
    out += len(pkt_dict).to_bytes(4, "little")
    out += pkt_dict
    out += len(pkt_data).to_bytes(4, "little")
    out += pkt_data
    return bytes(out)
