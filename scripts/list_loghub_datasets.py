from __future__ import annotations
from pathlib import Path

CLP_DIR = Path("results/clp/logs")

def main():
    if not CLP_DIR.exists():
        print("❌ results/clp/logs not found")
        return

    ds = []
    for p in sorted(CLP_DIR.glob("*_200k.log")):
        name = p.name.replace("_200k.log", "")
        ds.append(name)

    print("✅ datasets found:", len(ds))
    for x in ds:
        print(x)

if __name__ == "__main__":
    main()
