from pathlib import Path
import re

p = Path("src/usc/cli/app.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) ensure parse_hdfs_lines_rows imported
s = s.replace(
    "from usc.mem.hdfs_templates_v0 import HDFSTemplateBank, parse_hdfs_lines",
    "from usc.mem.hdfs_templates_v0 import HDFSTemplateBank, parse_hdfs_lines, parse_hdfs_lines_rows",
)

# 2) import new builder
if "tpl_pf1_recall_v3_h1m2" not in s:
    s = s.replace(
        "from usc.mem.tpl_pf1_recall_v1 import build_tpl_pf1_blob as build_pf1",
        "from usc.mem.tpl_pf1_recall_v1 import build_tpl_pf1_blob as build_pf1\nfrom usc.mem.tpl_pf1_recall_v3_h1m2 import build_tpl_pf3_blob_h1m2 as build_pf3_h1m2",
    )

# 3) patch hot-lite encode section:
# find "events, unknown = parse_hdfs_lines(raw_lines, bank)" inside cmd_encode and replace only for hot-lite branch
pattern = r"(if\s+args\.mode\s*==\s*['\"]hot-lite['\"]:\s*\n)([ \t]+)(events,\s*unknown\s*=\s*parse_hdfs_lines\(raw_lines,\s*bank\))"
m = re.search(pattern, s)
if not m:
    raise SystemExit("Could not find hot-lite parse block to patch.")

indent = m.group(2)
replacement = (
    f"{m.group(1)}"
    f"{indent}rows, unknown = parse_hdfs_lines_rows(raw_lines, bank)\n"
    f"{indent}tpl_text = Path(tpl_path).read_text(encoding='utf-8', errors='ignore')\n"
    f"{indent}pf1_blob, _meta = build_pf3_h1m2(rows, unknown, tpl_text, packet_events=args.packet_events, zstd_level=args.zstd)\n"
    f"{indent}Path(args.out).write_bytes(pf1_blob)\n"
    f"{indent}print('USCH:', f\"{len(pf1_blob)/1024.0:.2f} KB\", ' saved ✅ (HOT-LITE H1M2)')\n"
    f"{indent}return\n"
)
s = re.sub(pattern, replacement, s, count=1)

p.write_text(s, encoding="utf-8")
print("✅ patched hot-lite -> PF3(H1M2)")
