from __future__ import annotations
from pathlib import Path
import hashlib
import subprocess
import sys

def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def count_lines(p: Path) -> int:
    n = 0
    with p.open("r", encoding="utf-8", errors="replace") as f:
        for _ in f:
            n += 1
    return n

def main():
    if len(sys.argv) != 3:
        print("usage: python3 scripts/verify_hotlitefull_lossless_v1.py <raw_log> <hotlitefull_bin>")
        raise SystemExit(2)

    raw_log = Path(sys.argv[1])
    bin_path = Path(sys.argv[2])

    if not raw_log.exists():
        raise SystemExit(f"❌ missing raw: {raw_log}")
    if not bin_path.exists():
        raise SystemExit(f"❌ missing bin: {bin_path}")

    out = Path("/tmp/usc_verify_hotlitefull_decoded.log")
    if out.exists():
        out.unlink()

    subprocess.run([
        "python3","-m","usc.cli.app","decode",
        "--mode","hot-lite-full",
        "--input",str(bin_path),
        "--out",str(out),
    ], check=True)

    raw_hash = sha256(raw_log)
    out_hash = sha256(out)

    raw_lines = count_lines(raw_log)
    out_lines = count_lines(out)

    print("RAW :", raw_log, "bytes=", raw_log.stat().st_size, "lines=", raw_lines, "sha256=", raw_hash)
    print("DEC :", out,     "bytes=", out.stat().st_size,     "lines=", out_lines, "sha256=", out_hash)

    if raw_hash == out_hash:
        print("✅ LOSSLESS VERIFIED (byte-for-byte identical)")
    else:
        print("❌ NOT IDENTICAL (lossless failed)")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
