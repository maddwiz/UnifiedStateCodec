from __future__ import annotations
from pathlib import Path
import subprocess
import sys

def grep_count(raw: Path, needle: str) -> int:
    p = subprocess.run(
        ["bash","-lc", f"grep -a -i -o {needle!r} {str(raw)!r} | wc -l"],
        capture_output=True, text=True
    )
    try:
        return int(p.stdout.strip())
    except Exception:
        return -1

def run_hot_query(hotbin: Path, needle: str) -> list[str]:
    p = subprocess.run(
        ["python3","-m","usc.cli.app","query",
         "--mode","hot",
         "--hot",str(hotbin),
         "--q",needle,
         "--limit","10"],
        capture_output=True, text=True
    )
    lines = [ln.rstrip("\n") for ln in p.stdout.splitlines() if ln.strip()]

    hits = []
    for ln in lines:
        if ln.startswith(("USC QUERY", "mode:", "hits:", "time=", "hot:", "q:", "limit:", "------------------------------------------------------------", "DONE ✅")):
            continue
        # filter hit lines like "01) ..."
        if ln[0:2].isdigit() and ") " in ln:
            hits.append(ln.split(") ", 1)[1])
        else:
            # sometimes CLI prints plain lines; keep them too
            hits.append(ln)
    return hits

def main():
    if len(sys.argv) != 4:
        print("usage: python3 scripts/verify_hot_query_legit_v1.py <raw_log> <hot_bin> <needle>")
        raise SystemExit(2)

    raw = Path(sys.argv[1])
    hotbin = Path(sys.argv[2])
    needle = sys.argv[3]

    if not raw.exists():
        raise SystemExit(f"❌ missing raw: {raw}")
    if not hotbin.exists():
        raise SystemExit(f"❌ missing hot: {hotbin}")

    true_ct = grep_count(raw, needle)
    hits = run_hot_query(hotbin, needle)

    print(f"RAW grep count (case-insensitive) for '{needle}':", true_ct)
    print(f"HOT returned {len(hits)} lines (limit 10):")
    for h in hits:
        print("  ", h)

    raw_text = raw.read_text(encoding="utf-8", errors="replace")
    ok = sum(1 for h in hits if h in raw_text)

    print(f"Lines that exist verbatim in RAW: {ok}/{len(hits)}")

    if len(hits) > 0 and ok == len(hits):
        print("✅ HOT QUERY LEGIT (returned lines are truly in the raw log)")
    else:
        print("⚠️ HOT QUERY SUSPICIOUS (no hits or lines not found in raw)")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
