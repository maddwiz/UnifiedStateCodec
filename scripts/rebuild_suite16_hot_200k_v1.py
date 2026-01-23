from __future__ import annotations
from pathlib import Path
import subprocess

RAW_DIR = Path("results/raw_real_suite16_200k")
TPL_ROOT = Path("data/loghub_full")
OUT_DIR = Path("results/suite16_200k")
OUT_DIR.mkdir(parents=True, exist_ok=True)

logs = sorted(RAW_DIR.glob("*_200000.log"))
print(f"Found {len(logs)} raw logs")

def find_tpl_csv(ds: str) -> Path | None:
    # examples: Windows_2k.log_templates.csv
    cand = TPL_ROOT / ds / f"{ds}_2k.log_templates.csv"
    if cand.exists():
        return cand
    # fallback: any templates.csv in folder
    d = TPL_ROOT / ds
    if d.exists():
        xs = list(d.glob("*templates.csv"))
        if xs:
            return xs[0]
    return None

for p in logs:
    ds = p.name.replace("_200000.log", "")
    tpl = find_tpl_csv(ds)
    if tpl is None:
        print(f"⚠️  no template CSV found for {ds} — skipping HOT")
        continue

    out = OUT_DIR / f"{ds}_hot.bin"
    print(f"\n== HOT {ds} ==")
    subprocess.run([
        "python3", "-m", "usc.cli.app", "encode",
        "--mode", "hot",
        "--log", str(p),
        "--tpl", str(tpl),
        "--lines", "200000",
        "--out", str(out),
    ], check=True)

    print("wrote:", out, "size:", out.stat().st_size)

print("\nDONE ✅")
