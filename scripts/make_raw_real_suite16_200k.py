from __future__ import annotations
from pathlib import Path

SRCROOT = Path("data/loghub_real_v8_extracted")
OUTDIR = Path("results/raw_real_suite16_200k")
OUTDIR.mkdir(parents=True, exist_ok=True)

N_LINES = 200_000

CAND_EXT = {".log", ".txt", ".csv"}

def pick_best_file(folder: Path) -> Path | None:
    # pick the biggest candidate log-ish file
    cands = []
    for p in folder.rglob("*"):
        if p.is_file() and p.suffix.lower() in CAND_EXT:
            cands.append(p)
    if not cands:
        return None
    cands.sort(key=lambda x: x.stat().st_size, reverse=True)
    return cands[0]

def head_n_lines(src: Path, n: int) -> list[str]:
    out = []
    with src.open("r", encoding="utf-8", errors="replace") as f:
        for _ in range(n):
            line = f.readline()
            if not line:
                break
            out.append(line)
    return out

def main():
    if not SRCROOT.exists():
        raise SystemExit(f"❌ missing {SRCROOT}. Run extract script first.")

    folders = sorted([p for p in SRCROOT.iterdir() if p.is_dir()])
    if not folders:
        raise SystemExit(f"❌ no dataset folders found under {SRCROOT}")

    wrote = 0
    for ds_folder in folders:
        ds = ds_folder.name

        best = pick_best_file(ds_folder)
        if best is None:
            print(f"⚠️ {ds}: no .log/.txt/.csv found, skipping")
            continue

        lines = head_n_lines(best, N_LINES)
        if not lines:
            print(f"⚠️ {ds}: empty file? skipping")
            continue

        outp = OUTDIR / f"{ds}_{N_LINES}.log"
        outp.write_text("".join(lines), encoding="utf-8")
        wrote += 1

        print(f"✅ {ds:<14} src={best.name:<30} took={len(lines):>7}  size={outp.stat().st_size/1024/1024:.2f} MB")

    print(f"\n✅ wrote {wrote} datasets into {OUTDIR}")

if __name__ == "__main__":
    main()
