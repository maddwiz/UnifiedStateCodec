from pathlib import Path
from usc.mem.hdfs_templates_v0 import HDFSTemplateBank, parse_hdfs_lines

LOG = Path("data/loghub/HDFS.log")
TPL = Path("data/loghub/preprocessed/HDFS.log_templates.csv")
LINES = 200000

def main():
    bank = HDFSTemplateBank.from_csv(TPL)
    print("templates_loaded:", len(bank.compiled))

    raw = []
    with LOG.open("r", encoding="utf-8", errors="ignore") as f:
        for i, ln in enumerate(f):
            if i >= LINES:
                break
            raw.append(ln.rstrip("\n"))

    events, unknown = parse_hdfs_lines(raw, bank)
    print("lines:", len(raw))
    print("events:", len(events))
    print("unknown:", len(unknown))
    if raw:
        print("unknown_pct:", f"{100.0*len(unknown)/len(raw):.2f}%")

if __name__ == "__main__":
    main()
