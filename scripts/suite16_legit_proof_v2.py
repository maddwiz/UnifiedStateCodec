from __future__ import annotations
from pathlib import Path
import re
import subprocess

RAW_DIR = Path("results/raw_real_suite16_200k")
HOT_DIR = Path("results/suite16_200k")

DATASETS = [
    "Apache","Windows","HDFS_v2","Hadoop","HPC","Linux","Mac","OpenSSH",
    "OpenStack","Spark","BGL","Zookeeper","HealthApp","Android","Proxifier","Thunderbird"
]

def run(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def pick_probe_token(raw_path: Path) -> str | None:
    # pick a decent token from first ~100 lines
    tokfreq = {}
    try:
        with raw_path.open("r", encoding="utf-8", errors="replace") as f:
            for _ in range(100):
                ln = f.readline()
                if not ln:
                    break
                for t in re.findall(r"[A-Za-z_]{4,}", ln):
                    t2 = t.lower()
                    if t2 in ("this","that","with","from","have","will","could","then","when","where"):
                        continue
                    tokfreq[t2] = tokfreq.get(t2, 0) + 1
    except Exception:
        return None

    if not tokfreq:
        return None

    # choose most frequent token (but output original-ish case)
    best = max(tokfreq.items(), key=lambda kv: kv[1])[0]
    return best

def hot_query(hot_path: Path, q: str):
    r = run(["python3","-m","usc.cli.app","query","--mode","hot","--hot",str(hot_path),"--q",q,"--limit","3"])
    txt = r.stdout
    # Parse hits line
    hits = None
    for ln in txt.splitlines():
        if ln.strip().startswith("hits:"):
            try:
                hits = int(ln.strip().split()[1])
            except Exception:
                hits = 0
            break
    return r.returncode, hits, txt

def main():
    ok = 0
    bad = 0

    for name in DATASETS:
        raw = RAW_DIR / f"{name}_200000.log"
        hot = HOT_DIR / f"{name}_hot.bin"

        if not raw.exists() or not hot.exists():
            print(f"SKIP {name:<12} missing raw/hot")
            continue

        probe = pick_probe_token(raw)
        if not probe:
            print(f"SKIP {name:<12} could not pick probe token")
            continue

        # confirm probe exists in raw
        g = run(["grep","-a","-i","-m","1",probe,str(raw)])
        raw_has = (g.returncode == 0)

        rc, hits, out = hot_query(hot, probe)

        hot_ok = (rc == 0 and hits is not None and hits > 0)

        if raw_has and hot_ok:
            print(f"✅ {name:<12} probe='{probe}' PASS   hits={hits}")
            ok += 1
        else:
            print(f"❌ {name:<12} probe='{probe}' FAIL   raw_has={raw_has} rc={rc} hits={hits}")
            # show 1st useful line of error/output
            preview = "\n".join(out.splitlines()[:8])
            print(preview)
            print("-"*40)
            bad += 1

    print("="*60)
    print(f"TOTAL PASS={ok}  FAIL={bad}")

if __name__ == "__main__":
    main()
