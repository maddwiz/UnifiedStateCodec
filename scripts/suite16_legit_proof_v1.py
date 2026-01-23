from __future__ import annotations
from pathlib import Path
import subprocess
import sys

# Dataset -> keyword guaranteed to exist
PROBES = {
    "Apache": "INFO",
    "Windows": "TrustedInstaller",
    "HDFS_v2": "addStoredBlock",
    "Hadoop": "INFO",
    "HPC": "INFO",
    "Linux": "kernel",
    "Mac": "launchd",
    "OpenSSH": "sshd",
    "OpenStack": "INFO",
    "Spark": "INFO",
    "BGL": "ERROR",
    "Zookeeper": "INFO",
    "HealthApp": "INFO",
    "Android": "ActivityManager",
    "Proxifier": "Proxy",
    "Thunderbird": "INFO",
}

RAW_DIR = Path("results/raw_real_suite16_200k")
HOT_DIR = Path("results/suite16_200k")

def run(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def main():
    ok = 0
    bad = 0

    for name, q in PROBES.items():
        raw = RAW_DIR / f"{name}_200000.log"
        hot = HOT_DIR / f"{name}_hot.bin"

        if not raw.exists() or not hot.exists():
            print(f"SKIP {name}: missing raw/hot")
            continue

        # confirm keyword exists in raw
        g = run(["grep", "-a", "-i", "-m", "1", q, str(raw)])
        raw_has = (g.returncode == 0)

        # query hot
        r = run(["python3", "-m", "usc.cli.app", "query",
                 "--mode", "hot", "--hot", str(hot),
                 "--q", q, "--limit", "3"])

        hit_line = ""
        for ln in r.stdout.splitlines():
            if ln.strip().startswith("hits:"):
                hit_line = ln.strip()
                break

        hot_hits = ("hits:" in r.stdout) and ("hits: 0" not in r.stdout)

        if raw_has and hot_hits:
            print(f"✅ {name:<12} probe='{q}'  PASS   {hit_line}")
            ok += 1
        else:
            print(f"❌ {name:<12} probe='{q}'  FAIL   raw_has={raw_has} hot_hits={hot_hits}")
            bad += 1

    print("-" * 60)
    print(f"TOTAL PASS={ok}  FAIL={bad}")
    if bad:
        sys.exit(1)

if __name__ == "__main__":
    main()
