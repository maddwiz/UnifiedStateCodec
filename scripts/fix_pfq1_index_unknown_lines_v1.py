from __future__ import annotations
from pathlib import Path

FILE = Path("src/usc/mem/tpl_pfq1_query_v1.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not FILE.exists():
        die(f"missing: {FILE}")

    s = FILE.read_text(encoding="utf-8", errors="replace")

    # ------------------------------------------------------------
    # 1) Add unknown_lines into bloom building inside build_pfq1_blob()
    # ------------------------------------------------------------
    needle1 = "        # keyword bloom built from rendered lines (fast + good enough)\n"
    if needle1 not in s:
        die("could not find bloom build marker in build_pfq1_blob()")

    if "unknown_lines ALSO indexed into bloom" not in s:
        inject1 = (
            needle1 +
            "        # ✅ unknown_lines ALSO indexed into bloom (critical for real logs)\n"
            "        # Many real datasets have the important text in unknown_lines, not templates.\n"
        )
        s = s.replace(needle1, inject1, 1)

    # Insert loop right before blooms.append(bytes(b))
    needle2 = "        blooms.append(bytes(b))\n"
    if needle2 not in s:
        die("could not find blooms.append(bytes(b))")

    if "for ln in ul:" not in s:
        add_unknown_loop = (
            "        # add unknown_lines tokens into bloom (only present in pkt_idx==0)\n"
            "        for ln in ul:\n"
            "            for tok in tokenize_line(ln):\n"
            "                bloom_add(b, bloom_bits, bloom_k, tok)\n\n"
            + needle2
        )
        s = s.replace(needle2, add_unknown_loop, 1)

    # ------------------------------------------------------------
    # 2) Query: scan unknown_lines too after decoding candidate packets
    # ------------------------------------------------------------
    needle3 = "        events, _unknown = decode_h1m1_all_events(raw)\n"
    if needle3 not in s:
        die("could not find decode_h1m1_all_events call in query_keywords()")

    # Replace that line to keep unknown list around
    if "events, unknown_lines = decode_h1m1_all_events(raw)" not in s:
        s = s.replace(needle3, "        events, unknown_lines = decode_h1m1_all_events(raw)\n", 1)

    # After the events loop ends, we add unknown scan.
    # Anchor on the return hits block.
    anchor = "    return hits\n"
    if anchor not in s:
        die("could not find return hits")

    if "scan unknown_lines for matches" not in s:
        # Find a safe insertion point: right before final 'return hits' inside query_keywords
        idx = s.rfind(anchor)
        if idx == -1:
            die("return hits not found at end")

        unknown_scan_block = """
        # ✅ scan unknown_lines for matches too (critical for real logs)
        for ln in unknown_lines:
            s2 = ln.lower()
            if require_all_terms:
                ok2 = all(t in s2 for t in terms_lc)
            else:
                ok2 = any(t in s2 for t in terms_lc)

            if ok2:
                hits.append(ln)
                if len(hits) >= limit:
                    return hits

"""
        s = s[:idx] + unknown_scan_block + s[idx:]

    FILE.write_text(s, encoding="utf-8")
    print(f"✅ patched: {FILE}")
    print("✅ PFQ1 now indexes + searches unknown_lines too")

if __name__ == "__main__":
    main()
