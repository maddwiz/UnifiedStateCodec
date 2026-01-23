from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    before = "hits_fast, _cands = query_fast_pf1(pf1_blob, q, limit=limit)"
    after  = "hits_fast, _cands = query_fast_pf1(pf1_blob, args.q, limit=int(args.limit))"

    if before not in s:
        raise SystemExit("❌ could not find the query_fast_pf1(...) line (it may have changed)")

    s = s.replace(before, after)

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")
    print("✅ replaced q/limit with args.q / int(args.limit)")

if __name__ == '__main__':
    main()
