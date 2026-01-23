from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    before = 'print(f"hot:   {hot_path}")'
    after  = 'print(f"hot:   {args.hot}")'

    if before not in s:
        # fallback: handle spacing variants
        if "hot_path" not in s:
            raise SystemExit("❌ did not find hot_path in app.py (unexpected)")
        print("⚠️ exact print line not found, doing safe replace for hot_path usage in hot print")
        s = s.replace("hot:   {hot_path}", "hot:   {args.hot}")
    else:
        s = s.replace(before, after)

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
