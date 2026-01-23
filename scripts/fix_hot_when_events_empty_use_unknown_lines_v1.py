from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # Find the line where PFQ1 is built (we patch right before it)
    needle = "pfq1_blob, pfq1_meta = build_pfq1_blob("
    if needle not in s:
        die("❌ could not find PFQ1 build call in app.py")

    if "EVENTS_EMPTY_FALLBACK_UNKNOWN_LINES" in s:
        print("✅ already patched")
        return

    patch = """
    # EVENTS_EMPTY_FALLBACK_UNKNOWN_LINES
    # If we extracted zero events, we MUST still make HOT queryable.
    # Fallback: treat every raw line as unknown_lines so PFQ1 builds at least 1 packet.
    if len(events) == 0 and len(raw_lines) > 0:
        unknown_lines = list(raw_lines)
"""

    s = s.replace(needle, patch + "\n" + needle, 1)

    APP.write_text(s, encoding="utf-8")
    print("✅ patched app.py: if events empty -> unknown_lines = raw_lines")

if __name__ == "__main__":
    main()
