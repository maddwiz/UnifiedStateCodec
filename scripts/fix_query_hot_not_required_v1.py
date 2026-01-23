from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # Find the query parser block
    qpos = s.find('qry = sub.add_parser("query"')
    if qpos == -1:
        die('❌ could not find: qry = sub.add_parser("query"')

    # Find the qry.add_argument("--hot"...) call (single or multiline)
    m = re.search(r'qry\.add_argument\(\s*["\']--hot["\'].*?\)\s*', s[qpos:], re.DOTALL)
    if not m:
        die("❌ could not find qry.add_argument('--hot' ...)")

    block = m.group(0)

    # If it contains required=True, remove it
    if "required=True" in block:
        block2 = re.sub(r",\s*required\s*=\s*True\s*", "", block)
        block2 = re.sub(r"required\s*=\s*True\s*,\s*", "", block2)
        block2 = re.sub(r"required\s*=\s*True\s*", "", block2)
        s = s[:qpos] + s[qpos:].replace(block, block2, 1)
        print("✅ removed required=True from --hot")
    else:
        print("✅ --hot is already not required (no change)")

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
