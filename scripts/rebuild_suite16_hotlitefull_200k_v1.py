from __future__ import annotations
from pathlib import Path
import subprocess

RAW_DIR = Path("results/raw_real_suite16_200k")
OUT_DIR = Path("results/suite16_200k")
OUT_DIR.mkdir(parents=True, exist_ok=True)

logs = sorted(RAW_DIR.glob("*_200000.log"))
print(f"Found {len(logs)} raw logs")

for p in logs:
    ds = p.name.replace("_200000.log", "")
    out = OUT_DIR / f"{ds}_hotlitefull.bin"
    print(f"\n== HOT-LITE-FULL {ds} ==")
    subprocess.run([
        "python3", "-m", "usc.cli.app", "encode",
        "--mode", "hot-lite-full",
        "--log", str(p),
        "--lines", "200000",
        "--out", str(out),
    ], check=True)

    print("wrote:", out, "size:", out.stat().st_size)

print("\nDONE ✅")
