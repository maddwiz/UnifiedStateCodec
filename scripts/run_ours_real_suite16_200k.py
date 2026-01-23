from __future__ import annotations

import subprocess
from pathlib import Path
import re
import time

REAL_DIR = Path("results/raw_real_suite16_200k")
RAW_ALIAS = Path("results/raw_all_200k")
OUTDIR = Path("results/real_suite16_runs")
OUTDIR.mkdir(parents=True, exist_ok=True)

def run(cmd: list[str], log_path: Path) -> int:
    print("\n>>>", " ".join(cmd))
    with log_path.open("w", encoding="utf-8") as f:
        p = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    return p.returncode

def main():
    if not REAL_DIR.exists():
        raise SystemExit("❌ results/raw_real_suite16_200k missing")

    # point benches at the real suite
    if RAW_ALIAS.exists() or RAW_ALIAS.is_symlink():
        try:
            RAW_ALIAS.unlink()
        except Exception:
            subprocess.run(["rm", "-rf", str(RAW_ALIAS)])
    RAW_ALIAS.symlink_to(REAL_DIR.name)  # relative inside results/
    print(f"✅ mapped {RAW_ALIAS} -> {REAL_DIR}")

    scripts_dir = Path("scripts")
    scripts = sorted([p for p in scripts_dir.glob("*.py")])

    # pick likely "ours" scripts:
    # - has "usc" in name OR "codec" OR "pf0" OR "query"
    # - exclude competitor baseline builders
    patt = re.compile(r"(usc|pf0|codec|query|scoreboard|report|bench)", re.I)
    exclude = re.compile(r"(competitor|baseline|brotli|zstd|gzip|xz|bzip2|lz4)", re.I)

    candidates = []
    for p in scripts:
        name = p.name
        if patt.search(name) and not exclude.search(name):
            candidates.append(p)

    # move "report/scoreboard" to the end
    def rank(p: Path) -> int:
        n = p.name.lower()
        if "score" in n or "report" in n:
            return 99
        if "query" in n:
            return 50
        return 10

    candidates.sort(key=lambda p: (rank(p), p.name.lower()))

    print("\n=== running our bench scripts ===")
    for p in candidates:
        print(" -", p.name)

    if not candidates:
        raise SystemExit("❌ No bench scripts found. (scripts folder has none matching USC/bench/query)")

    results = []
    for p in candidates:
        stamp = int(time.time())
        log_path = OUTDIR / f"{p.stem}_{stamp}.log"
        rc = run(["python3", str(p)], log_path)
        results.append((p.name, rc, str(log_path)))
        print(f"✅ {p.name} rc={rc} log={log_path}")

    # write index
    idx = OUTDIR / "INDEX.txt"
    idx.write_text("\n".join([f"{n}\trc={rc}\t{lp}" for n, rc, lp in results]) + "\n", encoding="utf-8")
    print(f"\n✅ wrote run index: {idx}")

if __name__ == "__main__":
    main()
