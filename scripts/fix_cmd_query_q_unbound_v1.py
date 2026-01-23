from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    before = 'print(f"q:     {q!r}")'
    after  = 'print(f"q:     {args.q!r}")'

    if before not in s:
        if "q:     {q!r}" not in s:
            raise SystemExit("❌ did not find q debug print in app.py (unexpected)")
        s = s.replace("q:     {q!r}", "q:     {args.q!r}")
        print("✅ patched q debug print (fallback replace)")
    else:
        s = s.replace(before, after)
        print("✅ patched q debug print")

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
