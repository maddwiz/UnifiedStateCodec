from __future__ import annotations
from pathlib import Path
import hashlib
import subprocess
import sys

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def main():
    if len(sys.argv) != 3:
        print("usage: python3 scripts/verify_hotlitefull_lossless_v2.py <raw_log> <hotlitefull_bin>")
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

    raw_b = raw_log.read_bytes()
    dec_b = out.read_bytes()

    raw_hash = sha256_bytes(raw_b)
    dec_hash = sha256_bytes(dec_b)

    print("RAW bytes:", len(raw_b), "sha256:", raw_hash)
    print("DEC bytes:", len(dec_b), "sha256:", dec_hash)

    if raw_b == dec_b:
        print("✅ BYTE-FOR-BYTE IDENTICAL (true lossless)")
        return

    # allow only 1-byte newline difference at EOF
    if (len(dec_b) == len(raw_b) + 1) and dec_b.endswith(b"\n") and not raw_b.endswith(b"\n"):
        if dec_b[:-1] == raw_b:
            print("✅ CONTENT LOSSLESS (only EOF newline added by decoder)")
            return

    print("❌ NOT LOSSLESS (content differs)")
    raise SystemExit(1)

if __name__ == "__main__":
    main()
