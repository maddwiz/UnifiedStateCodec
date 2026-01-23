from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    before = "hits2, mode2 = query_router_v1(pf1_blob, pfq1_new, q, limit=limit)"
    after  = "hits2, mode2 = query_router_v1(pf1_blob, pfq1_new, args.q, limit=int(args.limit))"

    if before not in s:
        raise SystemExit("❌ could not find the hits2 query_router_v1(...) line (maybe already fixed?)")

    s = s.replace(before, after)

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")
    print("✅ replaced remaining hits2 query_router_v1 q/limit with args.q / int(args.limit)")

if __name__ == '__main__':
    main()
