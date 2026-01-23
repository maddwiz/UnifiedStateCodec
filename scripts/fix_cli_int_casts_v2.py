from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

FIELDS = [
    ("packet_events", 512),
    ("zstd", 19),
    ("chunk_lines", 25),
    ("lines", 20000),
]

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8")

    # 1) ensure helper exists
    if "def _normalize_int_args(" not in s:
        helper = """
def _normalize_int_args(args):
    # Normalize numeric args if argparse hands them in as strings
    for name, default in [
        ("packet_events", 512),
        ("zstd", 19),
        ("chunk_lines", 25),
        ("lines", 20000),
    ]:
        if hasattr(args, name):
            v = getattr(args, name)
            if isinstance(v, str):
                try:
                    setattr(args, name, int(v))
                except Exception:
                    setattr(args, name, default)
    return args

"""
        # Insert helper after imports (best-effort)
        insert_pos = 0
        m = re.search(r"\n\n", s)
        if m:
            insert_pos = m.end()
        s = s[:insert_pos] + helper + s[insert_pos:]

    # 2) call helper in main() after parse_args
    # Find parse_args line
    parse_pat = re.search(r"args\s*=\s*parser\.parse_args\(\)", s)
    if not parse_pat:
        raise SystemExit("❌ could not find: args = parser.parse_args()")

    # Check if already normalized
    after = s[parse_pat.end():parse_pat.end()+200]
    if "_normalize_int_args(args)" not in after:
        inject = "\n    args = _normalize_int_args(args)\n"
        s = s[:parse_pat.end()] + inject + s[parse_pat.end():]

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
