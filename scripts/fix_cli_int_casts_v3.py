from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8")

    # 1) add helper once
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
        # insert helper after imports/docstring block (best effort)
        m = re.search(r"\n\n", s)
        insert_pos = m.end() if m else 0
        s = s[:insert_pos] + helper + s[insert_pos:]

    # 2) find ANY "X = Y.parse_args(...)" line (more flexible)
    # Supports: args = parser.parse_args() OR a = p.parse_args(sys.argv[1:]) etc
    pat = re.compile(r"^\s*(\w+)\s*=\s*.*?\.parse_args\s*\(.*?\)\s*$", re.MULTILINE)
    m = pat.search(s)
    if not m:
        raise SystemExit("❌ could not find any 'X = something.parse_args(...)' line in app.py")

    var = m.group(1)

    # 3) inject normalization right after that parse_args assignment
    # Avoid double insert
    after_pos = m.end()
    tail = s[after_pos:after_pos + 400]
    inject_line = f"\n    {var} = _normalize_int_args({var})\n"
    if f"{var} = _normalize_int_args({var})" not in tail:
        s = s[:after_pos] + inject_line + s[after_pos:]

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")
    print(f"✅ normalized numeric args for: {var}")

if __name__ == "__main__":
    main()
