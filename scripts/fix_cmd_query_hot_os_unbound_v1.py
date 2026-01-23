from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    before = "if not os.path.exists(hot_path):"
    after  = "if not Path(hot_path).exists():"

    if before not in s:
        die("❌ could not find: if not os.path.exists(hot_path):")

    # ensure Path is available in file (it already is in many files, but safe)
    if "from pathlib import Path" not in s:
        # insert near top
        lines = s.splitlines()
        inserted = False
        for i, line in enumerate(lines[:120]):
            if line.startswith("import ") or line.startswith("from "):
                continue
            lines.insert(i, "from pathlib import Path")
            inserted = True
            break
        if not inserted:
            lines.insert(0, "from pathlib import Path")
        s = "\n".join(lines)

    s = s.replace(before, after, 1)

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")
    print("✅ cmd_query HOT now uses Path(hot_path).exists() (no os needed)")

if __name__ == "__main__":
    main()
