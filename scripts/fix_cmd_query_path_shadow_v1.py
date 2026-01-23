from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # Ensure we have import pathlib
    if "import pathlib" not in s:
        # Insert near the top after first import block
        lines = s.splitlines()
        inserted = False
        for i, line in enumerate(lines[:80]):
            if line.startswith("from __future__"):
                continue
            if line.startswith("import ") or line.startswith("from "):
                continue
            # First non-import line => insert just before it
            lines.insert(i, "import pathlib")
            inserted = True
            break
        if not inserted:
            lines.insert(0, "import pathlib")
        s = "\n".join(lines)

    # Patch the hot-path query read
    before = "blob = Path(args.hot).read_bytes()"
    after  = "blob = pathlib.Path(args.hot).read_bytes()"

    if before in s:
        s = s.replace(before, after)
        print("✅ replaced Path(args.hot) read with pathlib.Path(args.hot)")
    else:
        # fallback: replace any Path(args.hot) usage (safest)
        if "Path(args.hot)" in s:
            s = s.replace("Path(args.hot)", "pathlib.Path(args.hot)")
            print("✅ replaced all Path(args.hot) with pathlib.Path(args.hot)")
        else:
            print("⚠️ did not find Path(args.hot) in app.py (maybe already patched?)")

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
