from __future__ import annotations

from pathlib import Path

CLP_DIR = Path("results/clp/logs")
OUTDIR = Path("results/raw_all_200k")
OUTDIR.mkdir(parents=True, exist_ok=True)

def main():
    if not CLP_DIR.exists():
        raise SystemExit("❌ results/clp/logs not found")

    files = sorted(CLP_DIR.glob("*_200k.log"))
    if not files:
        raise SystemExit("❌ no *_200k.log files found in results/clp/logs")

    n = 0
    for src in files:
        ds = src.name.replace("_200k.log", "")
        dst = OUTDIR / f"{ds}_200k.log"
        dst.write_bytes(src.read_bytes())
        n += 1

    print(f"✅ copied {n} datasets into {OUTDIR}")

if __name__ == "__main__":
    main()
