from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # 1) ensure "import time as _time" exists somewhere (safe to add once)
    if "import time as _time" not in s:
        # add near the top with other imports
        lines = s.splitlines()
        inserted = False
        for i, line in enumerate(lines[:120]):
            if line.startswith("import ") or line.startswith("from "):
                continue
            # first non-import line => insert before it
            lines.insert(i, "import time as _time")
            inserted = True
            break
        if not inserted:
            lines.insert(0, "import time as _time")
        s = "\n".join(lines)

    # 2) replace time.perf_counter() with _time.perf_counter()
    if "time.perf_counter()" in s:
        s = s.replace("time.perf_counter()", "_time.perf_counter()")
        print("✅ replaced time.perf_counter() -> _time.perf_counter()")
    else:
        print("⚠️ no time.perf_counter() occurrences found (maybe already patched?)")

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
