from __future__ import annotations
from pathlib import Path
import re

P = Path("src/usc/cli/app.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# ----------------------------
# Remove old packet-events blocks (V2/V3/V6 etc) if present
# ----------------------------
s = re.sub(
    r"\n\s*#\s*USC_PACKET_EVENTS_QUERY.*?\n\s*return\s*\n",
    "\n",
    s,
    flags=re.DOTALL
)

# ----------------------------
# Insert a NEW robust handler at top of cmd_query
# ----------------------------
m = re.search(r"(?m)^def\s+cmd_query\s*\(.*\)\s*:\s*$", s)
if not m:
    print("❌ cannot find cmd_query")
    raise SystemExit(1)

# find newline after the def line
nl = s.find("\n", m.end())
if nl == -1:
    raise SystemExit("❌ unexpected cmd_query format")

marker = "USC_QUERY_PACKET_EVENTS_V7"
if marker not in s:
    insert_block = f"""
    # {marker}
    # Priority 1: if --packet_events is provided, query TPF3 packet-events blob directly and exit.
    pe_val = getattr(args, "packet_events", None)
    if isinstance(pe_val, str) and pe_val:
        from pathlib import Path
        import time

        pe_path = Path(pe_val)
        if not pe_path.exists():
            raise FileNotFoundError(f"packet_events not found: {pe_path}")

        blob_pe = pe_path.read_bytes()
        q = (args.q or "").encode("utf-8", errors="ignore")
        limit = int(getattr(args, "limit", 0) or 0)

        t0 = time.time()
        if limit == 0:
            hits = blob_pe.count(q) if q else 0
        else:
            txt2 = blob_pe.decode("utf-8", errors="ignore")
            hits = 0
            for ln2 in txt2.splitlines():
                if args.q in ln2:
                    print(ln2)
                    hits += 1
                    if hits >= limit:
                        break
        dt = time.time() - t0
        print(f"[packet_events] hits={{hits}} time={{dt:.6f}}s file={{pe_path.name}}")
        return

    # Priority 2: hot query requires --hot
    if not getattr(args, "hot", None):
        raise SystemExit("usc query: must provide --hot <USCH> or --packet_events <TPF3>")
"""

    s = s[:nl+1] + insert_block + s[nl+1:]

# ----------------------------
# Ensure hot path ALWAYS sets blob before hot_unpack(blob)
# ----------------------------
# Replace the hot_unpack(blob) call line with a safe sequence.
s = re.sub(
    r'(?m)^\s*pf1_blob\s*,\s*pfq1_blob\s*=\s*hot_unpack\(blob\)\s*$',
    '    blob = Path(args.hot).read_bytes()\n    pf1_blob, pfq1_blob = hot_unpack(blob)',
    s,
    count=1
)

P.write_text(s, encoding="utf-8")
print("✅ patched cmd_query with packet_events priority + hot blob guard (V7)")
