from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # Find build_pfq1(...) definition
    m = re.search(r"def build_pfq1\s*\(.*?\)\s*->\s*Tuple\[bytes,\s*PFQ1Meta\]\s*:\s*", s, re.DOTALL)
    if not m:
        die("could not find build_pfq1(...)")

    fn_start = m.start()
    m2 = re.search(r"\ndef\s+\w+\s*\(", s[m.end():])
    if not m2:
        die("could not find end of build_pfq1 (next def)")

    fn_end = m.end() + m2.start()

    # Replace entire build_pfq1 with a version that ALWAYS indexes unknown_lines
    new_fn = r'''def build_pfq1(
    events: List[Tuple[int, List[str]]],
    unknown_lines: List[str],
    template_csv_text: str,
    packet_events: int = 32768,
    zstd_level: int = 10,
    bloom_bits: int = 8192,
    bloom_k: int = 4,
) -> Tuple[bytes, PFQ1Meta]:
    """
    PFQ1 (TPQ1) packet-bloom index.

    ✅ Always includes unknown_lines in bloom-building + raw_struct payload,
    so HOT remains queryable even when templates/events are missing.
    """
    from usc.mem.tpl_pfq1_query_v1 import (
        MAGIC as TPQ1_MAGIC,
        VERSION as TPQ1_VERSION,
        bloom_make,
        bloom_add,
        tokenize_line,
    )
    import struct
    import zlib
    try:
        import zstandard as zstd
    except Exception:
        zstd = None

    # Load templates map (if any)
    templates_map: Dict[int, str] = {}
    if template_csv_text and template_csv_text.strip():
        try:
            templates_map = load_template_map_from_csv_text(template_csv_text)
        except Exception:
            templates_map = {}

    # Helper: encode a minimal H1M1 raw_struct carrying events+unknown_lines
    def _encode_h1m1(events_local: List[Tuple[int, List[str]]], unknown_local: List[str]) -> bytes:
        out = bytearray()
        out += b"H1M1"
        out += struct.pack("<I", 1)

        n_events = len(events_local)
        n_unknown = len(unknown_local)
        max_params = 0
        for _eid, params in events_local:
            if params:
                max_params = max(max_params, len(params))

        out += _uvarint_encode(n_events)
        out += _uvarint_encode(n_unknown)
        out += _uvarint_encode(max_params)

        # event ids
        for eid, _params in events_local:
            out += _uvarint_encode(int(eid))

        # channels (empty masks/payloads if no events)
        for chan in range(max_params):
            # mask bits for which rows have a value in this chan
            mask = bytearray((n_events + 7) // 8)
            vals: List[str] = []
            for i, (_eid, params) in enumerate(events_local):
                if chan < len(params) and params[chan] != "":
                    mask[i // 8] |= (1 << (i % 8))
                    vals.append(str(params[chan]))

            out += _uvarint_encode(len(mask))
            out += bytes(mask)

            # encode as raw stream (ctype=0)
            out += _uvarint_encode(0)
            payload = bytearray()
            payload += _uvarint_encode(len(vals))
            for v in vals:
                b = v.encode("utf-8", errors="replace")
                payload += _bytes_encode(b)
            out += _bytes_encode(bytes(payload))

        # unknown lines
        for ln in unknown_local:
            out += _bytes_encode(ln.encode("utf-8", errors="replace"))

        return bytes(out)

    # Packetize: we index BOTH rendered templates (from events) + unknown_lines
    # We put unknown_lines into their own packets as well.
    packets: List[Tuple[bytes, bytes, List[int]]] = []  # (bloom_bytes, raw_struct_z, eids_sorted)

    # 1) Build packets from events in chunks of packet_events
    if events:
        cur: List[Tuple[int, List[str]]] = []
        for (eid, params) in events:
            cur.append((eid, params))
            if len(cur) >= packet_events:
                raw_struct = _encode_h1m1(cur, [])
                bloom = bloom_make(bloom_bits)
                # render template lines if possible; else index params raw
                for _eid, _p in cur:
                    # best-effort string to tokenize
                    if templates_map and _eid in templates_map:
                        line = render_template(templates_map[_eid], _p)
                    else:
                        line = " ".join([str(_eid)] + [str(x) for x in _p if x])
                    for tok in tokenize_line(line):
                        bloom_add(bloom, bloom_bits, bloom_k, tok)
                if zstd is not None:
                    comp = zstd.ZstdCompressor(level=zstd_level).compress(raw_struct)
                else:
                    comp = zlib.compress(raw_struct, level=9)
                eids_sorted = sorted([x[0] for x in cur])
                packets.append((bytes(bloom), comp, eids_sorted))
                cur = []

        if cur:
            raw_struct = _encode_h1m1(cur, [])
            bloom = bloom_make(bloom_bits)
            for _eid, _p in cur:
                if templates_map and _eid in templates_map:
                    line = render_template(templates_map[_eid], _p)
                else:
                    line = " ".join([str(_eid)] + [str(x) for x in _p if x])
                for tok in tokenize_line(line):
                    bloom_add(bloom, bloom_bits, bloom_k, tok)
            if zstd is not None:
                comp = zstd.ZstdCompressor(level=zstd_level).compress(raw_struct)
            else:
                comp = zlib.compress(raw_struct, level=9)
            eids_sorted = sorted([x[0] for x in cur])
            packets.append((bytes(bloom), comp, eids_sorted))

    # 2) Build packets from unknown_lines (this is what fixes HOT = empty shell)
    if unknown_lines:
        cur_u: List[str] = []
        for ln in unknown_lines:
            cur_u.append(ln)
            if len(cur_u) >= packet_events:
                raw_struct = _encode_h1m1([], cur_u)
                bloom = bloom_make(bloom_bits)
                for line in cur_u:
                    for tok in tokenize_line(line):
                        bloom_add(bloom, bloom_bits, bloom_k, tok)
                if zstd is not None:
                    comp = zstd.ZstdCompressor(level=zstd_level).compress(raw_struct)
                else:
                    comp = zlib.compress(raw_struct, level=9)
                packets.append((bytes(bloom), comp, []))
                cur_u = []

        if cur_u:
            raw_struct = _encode_h1m1([], cur_u)
            bloom = bloom_make(bloom_bits)
            for line in cur_u:
                for tok in tokenize_line(line):
                    bloom_add(bloom, bloom_bits, bloom_k, tok)
            if zstd is not None:
                comp = zstd.ZstdCompressor(level=zstd_level).compress(raw_struct)
            else:
                comp = zlib.compress(raw_struct, level=9)
            packets.append((bytes(bloom), comp, []))

    # Write TPQ1 blob
    out = bytearray()
    out += TPQ1_MAGIC
    out += struct.pack("<I", TPQ1_VERSION)

    # meta
    out += _uvarint_encode(bloom_bits)
    out += _uvarint_encode(bloom_k)

    # templates map
    out += _uvarint_encode(len(templates_map))
    for eid, tpl in templates_map.items():
        out += _uvarint_encode(int(eid))
        out += _bytes_encode(tpl.encode("utf-8", errors="replace"))

    # packets
    out += _uvarint_encode(len(packets))
    for bloom_bytes, comp_struct, eids_sorted in packets:
        out += _uvarint_encode(len(eids_sorted))
        for eid in eids_sorted:
            out += _uvarint_encode(int(eid))
        out += _uvarint_encode(len(bloom_bytes))
        out += bloom_bytes
        out += _bytes_encode(comp_struct)

    meta = PFQ1Meta(
        templates_map=templates_map,
        bloom_bits=bloom_bits,
        bloom_k=bloom_k,
        packets=len(packets),
    )
    return bytes(out), meta
'''
    s2 = s[:fn_start] + new_fn + s[fn_end:]
    APP.write_text(s2, encoding="utf-8")
    print("✅ patched build_pfq1(): unknown_lines always indexed into PFQ1 packets")

if __name__ == "__main__":
    main()
