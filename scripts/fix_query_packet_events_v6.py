from __future__ import annotations
from pathlib import Path
import re

P = Path("src/usc/cli/app.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# ---------------------------------------------------------
# 1) Fix qry.add_argument("--packet_events", ...) for QUERY
# Make it: type=str, default=None
# ---------------------------------------------------------
# This replaces the entire argument line if it exists in one line.
# If your add_argument spans lines, we still handle it later.
pat = r'(qry\.add_argument\(\s*["\']--packet_events["\']\s*,[^)]*\))'
m = re.search(pat, s)
if m:
    old = m.group(1)
    # If it's a one-liner, rewrite it safely
    new = re.sub(r'\btype\s*=\s*int\b', 'type=str', old)
    new = new.replace("type=int", "type=str")
    # Replace default=32768 or any int default with None
    new = re.sub(r'\bdefault\s*=\s*\d+\b', 'default=None', new)
    s = s.replace(old, new)
else:
    # Multi-line fallback: patch type + default within nearby region
    lines = s.splitlines(True)
    for i, ln in enumerate(lines):
        if "qry.add_argument" in ln and "--packet_events" in ln:
            # patch next 0..12 lines
            for j in range(i, min(i+14, len(lines))):
                lines[j] = re.sub(r'\btype\s*=\s*int\b', 'type=str', lines[j])
                lines[j] = lines[j].replace("type=int", "type=str")
                lines[j] = re.sub(r'\bdefault\s*=\s*\d+\b', 'default=None', lines[j])
            s = "".join(lines)
            break

# ---------------------------------------------------------
# 2) Make the cmd_query packet_events handler only run
#    if args.packet_events is a real string path.
# ---------------------------------------------------------
# Replace the start of our handler if present.
# We look for the marker and rewrite that "if getattr(...)" line.
marker = "USC_PACKET_EVENTS_QUERY_V3"
if marker in s:
    # Replace the condition line to be type-safe
    s = re.sub(
        r'if\s+getattr\(args,\s*"packet_events",\s*None\)\s*:',
        'pe_val = getattr(args, "packet_events", None)\n    if isinstance(pe_val, str) and pe_val:',
        s,
        count=1
    )
    # And change uses of args.packet_events -> pe_val inside that block’s first Path() call
    s = s.replace("pe_path = Path(args.packet_events)", "pe_path = Path(pe_val)")

P.write_text(s, encoding="utf-8")
print("✅ fixed query --packet_events to be a path (str) with default=None")
