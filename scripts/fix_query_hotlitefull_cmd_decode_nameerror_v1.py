from __future__ import annotations
from pathlib import Path

APP = Path("src/usc/cli/app.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    needle = "cmd_decode(dec_args)"
    if needle not in s:
        die("❌ could not find cmd_decode(dec_args) in app.py (maybe already fixed?)")

    replacement = """import subprocess as _sp
    from pathlib import Path as _P

    _tmp_out = _P("/tmp/usc_query_hotlitefull_tmp.log")
    _in_path = args.input or args.hot

    _sp.run([
        "python3", "-m", "usc.cli.app", "decode",
        "--mode", "hot-lite-full",
        "--input", str(_in_path),
        "--out", str(_tmp_out),
    ], check=True)

    # scan decoded lines (case-insensitive substring match)
    ql = str(args.q).lower()
    hits = 0
    with _tmp_out.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if ql in line.lower():
                print(line.rstrip("\\n"))
                hits += 1
                if hits >= int(args.limit):
                    break

    print(f"hits: {hits}   (hot-lite-full decode+scan)")
    return
"""

    # Keep indentation exactly like the original line
    s = s.replace(needle, replacement)

    APP.write_text(s, encoding="utf-8")
    print(f"✅ patched: {APP}")
    print("✅ query hot-lite-full now uses subprocess decode+scan (no cmd_decode dependency)")

if __name__ == "__main__":
    main()
