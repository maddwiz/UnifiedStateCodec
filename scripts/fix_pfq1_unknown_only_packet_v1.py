from __future__ import annotations
from pathlib import Path

FILE = Path("src/usc/mem/tpl_pfq1_query_v1.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not FILE.exists():
        die(f"missing: {FILE}")

    s = FILE.read_text(encoding="utf-8", errors="replace")

    marker = "    while i < n:\n"
    if marker not in s:
        die("could not find while i < n loop marker")

    if "UNKNOWN_ONLY_PACKET_MODE" in s:
        print("✅ already patched (skip)")
        return

    inject = """
    # UNKNOWN_ONLY_PACKET_MODE:
    # If events are empty but unknown_lines exist, we still build ONE packet
    # so HOT remains queryable on real logs without template coverage.
    if n == 0 and unknown_lines:
        raw_struct = encode_template_channels_v1_mask([], unknown_lines)
        comp = _zstd_compress(raw_struct, level=zstd_level)

        packets_comp.append(comp)
        eidsets.append(_encode_eidset([]))

        b = bloom_make(bloom_bits)
        for ln in unknown_lines:
            for tok in tokenize_line(ln):
                bloom_add(b, bloom_bits, bloom_k, tok)
        blooms.append(bytes(b))

        n = 1  # force table build + offsets patching
"""

    s = s.replace(marker, inject + "\n" + marker, 1)

    FILE.write_text(s, encoding="utf-8")
    print(f"✅ patched: {FILE}")
    print("✅ PFQ1 now supports UNKNOWN-ONLY HOT packets (no tpl required)")

if __name__ == "__main__":
    main()
