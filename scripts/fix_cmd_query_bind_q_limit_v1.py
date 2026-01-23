from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # Find cmd_query definition block (best-effort) and inject q/limit binding once
    # We inject right after the line that prints the separator line of dashes,
    # because by that point args exists and prints are done.
    marker = 'print("------------------------------------------------------------")'
    if marker not in s:
        raise SystemExit("❌ could not find cmd_query separator marker line")

    # If already bound, skip
    if re.search(r"\n\s*q\s*=\s*args\.q\s*\n", s) and re.search(r"\n\s*limit\s*=\s*int\(args\.limit\)\s*\n", s):
        print("✅ q/limit already bound (skipping)")
        return

    inject = (
        marker +
        "\n    # bind query params once (prevents UnboundLocalError)\n"
        "    q = args.q\n"
        "    limit = int(args.limit)\n"
    )

    s2 = s.replace(marker, inject, 1)

    APP.write_text(s2, encoding="utf-8")
    print(f"✅ patched: {APP}")
    print("✅ injected: q=args.q and limit=int(args.limit)")

if __name__ == "__main__":
    main()
