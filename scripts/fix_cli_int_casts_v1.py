from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def main():
    if not APP.exists():
        raise SystemExit(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8")

    if "def _as_int(" in s:
        print("✅ _as_int already present (skipping)")
        return

    marker = "def cmd_encode(args):"
    i = s.find(marker)
    if i < 0:
        raise SystemExit("❌ could not find cmd_encode")

    insert_at = s.find("\n", i) + 1
    inject = """
def _as_int(x, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default

"""
    s = s[:insert_at] + inject + s[insert_at:]

    # Inside cmd_encode, normalize args once (packet_events/zstd/chunk_lines)
    # We try to inject right after the doc/first lines of cmd_encode.
    needle = "def cmd_encode(args):"
    j = s.find(needle)
    if j < 0:
        raise SystemExit("❌ cmd_encode vanished")

    # find first line after cmd_encode signature
    k = s.find("\n", j) + 1

    # don't double insert
    if "packet_events = _as_int(" in s[k:k+400]:
        print("✅ cmd_encode already normalizes ints (skipping)")
        APP.write_text(s, encoding="utf-8")
        return

    normalize = """    # normalize numeric args (argparse sometimes hands them in as str)
    args.packet_events = _as_int(getattr(args, "packet_events", None), 512)
    args.zstd = _as_int(getattr(args, "zstd", None), 19)
    args.chunk_lines = _as_int(getattr(args, "chunk_lines", None), 25)

"""
    s = s[:k] + normalize + s[k:]

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")

if __name__ == "__main__":
    main()
