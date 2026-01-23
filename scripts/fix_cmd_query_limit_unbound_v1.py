from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    before = 'print(f"limit: {limit}")'
    after  = 'print(f"limit: {args.limit}")'

    if before not in s:
        # fallback: replace the inner f-string pattern
        if "limit: {limit}" not in s:
            raise SystemExit("❌ did not find limit debug print in app.py (unexpected)")
        s = s.replace("limit: {limit}", "limit: {args.limit}")
        print("✅ patched limit debug print (fallback replace)")
    else:
        s = s.replace(before, after)
        print("✅ patched limit debug print")

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
