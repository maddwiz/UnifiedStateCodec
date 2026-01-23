from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # Insert guard right at the top of cmd_query (after def line)
    needle = "def cmd_query"
    i = s.find(needle)
    if i == -1:
        die("❌ could not find cmd_query definition")

    # Find end of def line
    j = s.find("\n", i)
    if j == -1:
        die("❌ malformed cmd_query def line")

    guard = """
    # enforce required inputs by mode
    if getattr(args, "mode", "hot") != "hot-lite-full":
        if not getattr(args, "hot", None):
            raise SystemExit("❌ query --mode hot requires --hot <blob>")
"""

    # If already inserted, skip
    if "enforce required inputs by mode" in s:
        print("✅ cmd_query guard already present (skip)")
        return

    s = s[:j+1] + guard + s[j+1:]

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")
    print("✅ cmd_query now requires --hot only for mode=hot")

if __name__ == "__main__":
    main()
