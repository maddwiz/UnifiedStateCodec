from __future__ import annotations
from pathlib import Path
import re

P = Path("src/usc/cli/app.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# ------------------------------------------------------------
# 1) Ensure cmd_query has a packet_events early return
# ------------------------------------------------------------
m = re.search(r"(?m)^def\s+cmd_query\s*\(.*\)\s*:\s*$", s)
if not m:
    raise SystemExit("❌ Could not find cmd_query() definition")

fn_start = m.start()
fn_line_end = s.find("\n", m.end())
if fn_line_end == -1:
    raise SystemExit("❌ cmd_query def line malformed")

marker = "USC_QUERY_PACKET_EVENTS_V7"

if marker not in s:
    insert = f"""
    # {marker}
    # If --packet_events is provided, query the TPF3 blob directly and exit.
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

"""
    s = s[:fn_line_end+1] + insert + s[fn_line_end+1:]
    print("✅ inserted packet_events handler into cmd_query")

# ------------------------------------------------------------
# 2) FIX hot_unpack(blob) call so blob is always initialized
# ------------------------------------------------------------
# Replace:
#   pf1_blob, pfq1_blob = hot_unpack(blob)
# with:
#   blob = Path(args.hot).read_bytes()
#   pf1_blob, pfq1_blob = hot_unpack(blob)

repl_pat = r'(?m)^\s*pf1_blob\s*,\s*pfq1_blob\s*=\s*hot_unpack\(blob\)\s*$'
if re.search(repl_pat, s):
    s = re.sub(
        repl_pat,
        '    blob = Path(args.hot).read_bytes()\n    pf1_blob, pfq1_blob = hot_unpack(blob)',
        s,
        count=1
    )
    print("✅ patched hot_unpack(blob) to load blob from args.hot")
else:
    print("⚠️ did not find hot_unpack(blob) line to patch (maybe already changed?)")

# ------------------------------------------------------------
# 3) Add a guard: if no --hot and no --packet_events, exit cleanly
# ------------------------------------------------------------
# We insert this guard just before the blob read (if not already present).
guard = '    if not getattr(args, "hot", None):\n        raise SystemExit("usc query: must provide --hot <USCH> or --packet_events <TPF3>")\n'
if 'must provide --hot <USCH>' not in s:
    # Insert before the blob-read line we just added
    s = s.replace(
        '    blob = Path(args.hot).read_bytes()',
        guard + '    blob = Path(args.hot).read_bytes()',
        1
    )
    print("✅ inserted --hot/--packet_events guard")

P.write_text(s, encoding="utf-8")
print("✅ wrote:", P)
