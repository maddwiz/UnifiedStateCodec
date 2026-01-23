from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # Already injected?
    if re.search(r"^\s*q\s*=\s*args\.q\s*$", s, re.MULTILINE) and re.search(r"^\s*limit\s*=\s*int\(args\.limit\)\s*$", s, re.MULTILINE):
        print("✅ q/limit already bound somewhere (skipping)")
        return

    # Find cmd_query function start
    m = re.search(r"^def\s+cmd_query\s*\(\s*args\s*\)\s*:\s*$", s, re.MULTILINE)
    if not m:
        raise SystemExit("❌ could not find: def cmd_query(args):")

    start = m.end()

    # Take the function body substring (best effort: until next top-level def)
    m2 = re.search(r"^\ndef\s+\w+\s*\(", s[start:], re.MULTILINE)
    end = start + (m2.start() if m2 else len(s) - start)
    func = s[start:end]

    # Find the first dashed-line print inside cmd_query (10+ dashes)
    dash_print = re.search(r'^\s*print\(\s*["\']-{10,}["\']\s*\)\s*$', func, re.MULTILINE)

    inject = (
        "\n    # bind query params once (prevents UnboundLocalError)\n"
        "    q = args.q\n"
        "    limit = int(args.limit)\n"
    )

    if dash_print:
        insert_at = dash_print.end()
        func2 = func[:insert_at] + inject + func[insert_at:]
        print("✅ injected q/limit right after dashed-line print")
    else:
        # Fallback: inject right after limit print (works on your current output)
        lim_print = re.search(r'^\s*print\(\s*f["\']limit:\s*\{args\.limit\}["\']\s*\)\s*$', func, re.MULTILINE)
        if lim_print:
            insert_at = lim_print.end()
            func2 = func[:insert_at] + inject + func[insert_at:]
            print("✅ injected q/limit right after limit print")
        else:
            # Final fallback: inject at top of function body
            func2 = inject + func
            print("⚠️ dashed-line + limit print not found — injected at start of cmd_query body")

    s2 = s[:start] + func2 + s[end:]

    APP.write_text(s2, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
